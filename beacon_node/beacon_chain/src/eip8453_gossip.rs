//! EIP-8453 gossip validation: the p2p spec's per-topic validation conditions for the
//! single-object topics (`head_vote`, `safe_vote`, `attestation_{subnet_id}`, the EIP-8453
//! `attester_slashing`) and the aggregate topics (`head_vote_aggregate`, `safe_vote_aggregate`,
//! `attestation_aggregate`), with the spec's `Seen` dedup caches.
//!
//! Spec: `specs/_features/eip8453/p2p-interface.md` (`validate_head_vote_gossip`,
//! `validate_safe_vote_gossip`, `validate_attestation_gossip`, and the three
//! `validate_*_aggregate_gossip` functions).
//!
//! The verifying state is the head snapshot's post-state (the spec reads
//! `store.block_states[get_head(store).root]`), and block/ancestry/payload facts come from the
//! fg fork-choice store. Prototype deviations, in line with the fork-choice STUBs:
//! - "block failed validation" cannot be distinguished from "block unknown" (invalid blocks are
//!   never indexed), so the spec's REJECT collapses into the unknown-block IGNORE.
//! - The payload-invalidated REJECT collapses into the payload-verified check (the prototype
//!   tracks payload verification, not EL invalidation — the existing payload-status STUB).

use std::collections::{BTreeMap, HashMap, HashSet};

use fork_choice::fg_fork_choice::FgForkChoiceStore;
use parking_lot::Mutex;
use slot_clock::SlotClock;
use state_processing::per_block_processing::eip8453::{
    get_attesting_indices_eip8453, is_valid_attestation_data, process_attester_slashing_eip8453,
};
use state_processing::state_advance::partial_state_advance;
use state_processing::{ConsensusContext, VerifySignatures};
use tree_hash::TreeHash;
use types::consts::gloas::{
    PAYLOAD_STATUS_EMPTY, PAYLOAD_STATUS_FULL, PAYLOAD_STATUS_PENDING, PayloadStatus,
};
use types::eip8453::{
    HeadVoteData, HeadVoteMessage, Round, SafeVoteMessage, SignedAttestationAggregateAndProof,
    SignedHeadVoteAggregateAndProof, SignedSafeVoteAggregateAndProof,
    compute_attestation_committee_slot, compute_round_at_slot, compute_start_slot_at_round,
    compute_subnet_for_fg_attestation, head_vote_aggregator_selection_root, is_selected_aggregator,
    round_aggregator_selection_root,
};
use types::{Domain, EthSpec, Hash256, Slot, eip8453};

use crate::{BeaconChain, BeaconChainTypes};

/// Why an EIP-8453 gossip object failed validation. Grouped by the spec's propagation verdicts;
/// `verdict()` gives the mapping.
#[derive(Debug)]
pub enum Eip8453GossipError {
    // IGNORE class.
    /// A head vote from this validator for this slot was already seen.
    PriorHeadVoteKnown { validator_index: u64, slot: Slot },
    /// The vote is not for the current slot (within clock disparity).
    NotCurrentSlot { vote_slot: Slot, current_slot: Slot },
    /// The safe vote is not for the current round (within clock disparity).
    NotCurrentRound {
        vote_round: Round,
        current_round: Round,
    },
    /// This exact safe vote was already seen.
    DuplicateSafeVote { validator_index: u64, round: Round },
    /// Two distinct safe votes from this validator this round already propagated (the second is
    /// equivocation evidence; a third adds nothing).
    SafeVoteEquivocationLimit { validator_index: u64, round: Round },
    /// An FG attestation from this validator for this round was already seen.
    PriorAttestationKnown { validator_index: u64, round: Round },
    /// The attestation's epoch is not the epoch of its committee's slot.
    EpochMismatch {
        attestation_epoch: types::Epoch,
        committee_epoch: types::Epoch,
    },
    /// The referenced block is not known to the fork choice (may be queued for lookup).
    UnknownBlock(Hash256),
    /// The finalized block is not an ancestor of the referenced block.
    NotFinalizedDescendant(Hash256),
    /// A FULL head vote names a payload we have not seen verified.
    PayloadNotVerified(Hash256),
    /// Every doubly-attesting validator in the slashing is already slashed.
    NoNewSlashableIndices,
    /// The FG aggregate's data is not valid for the pulled-up head state.
    ///
    /// Deliberate deviation from the spec's [REJECT]: an honest attester builds its data from
    /// `get_fg_target_source` (the confirmed source's view), which legitimately trails the head
    /// state's target whenever confirmation lags and no finality pair bridges the gap — e.g. in
    /// the first rounds after genesis. Rejecting would penalize honest aggregators, so this is
    /// IGNORE-class (spec friction #16).
    StaleAggregateData(String),
    /// An internal error (missing caches, clock not started, ...): do not penalize the peer.
    Internal(String),
    // REJECT class.
    /// The vote's slot or round is before the EIP-8453 fork.
    PreFork,
    /// The referenced block is from a later slot than the vote.
    BlockFromFuture { block_slot: Slot, vote_slot: Slot },
    /// The payload status breaks the spec's consistency rules.
    InvalidPayloadStatus(PayloadStatus),
    /// The validator is not on the relevant committee.
    NotInCommittee { validator_index: u64 },
    /// The validator index is unknown.
    UnknownValidator(u64),
    /// The validator is not active in the vote's epoch.
    InactiveValidator(u64),
    /// The signature does not verify.
    InvalidSignature(u64),
    /// The attestation's committee index is out of range.
    CommitteeIndexOutOfRange(u64),
    /// The attestation arrived on the wrong subnet.
    WrongSubnet { expected: u64, actual: u64 },
    /// The attestation data is not well formed.
    MalformedData,
    /// The attester slashing is invalid (not slashable, bad indices, or bad signatures).
    InvalidSlashing(String),
    /// An aggregate from this aggregator for this slot/round (and data) was already seen.
    PriorAggregateKnown { aggregator_index: u64 },
    /// The aggregate names no voters.
    EmptyAggregate,
    /// The aggregate's voters are not sorted and unique.
    UnsortedVoters,
    /// The selection proof does not select the aggregator.
    NotSelectedAggregator(u64),
    /// The selection proof signature does not verify.
    InvalidSelectionProof(u64),
    /// The aggregate signature does not verify against the voters.
    InvalidAggregateSignature,
    /// The FG aggregate's data or aggregation bits are invalid for the head state.
    InvalidAggregate(String),
}

/// The spec's propagation verdicts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Eip8453GossipVerdict {
    Ignore,
    Reject,
}

impl Eip8453GossipError {
    pub fn verdict(&self) -> Eip8453GossipVerdict {
        match self {
            Eip8453GossipError::PriorHeadVoteKnown { .. }
            | Eip8453GossipError::NotCurrentSlot { .. }
            | Eip8453GossipError::NotCurrentRound { .. }
            | Eip8453GossipError::DuplicateSafeVote { .. }
            | Eip8453GossipError::SafeVoteEquivocationLimit { .. }
            | Eip8453GossipError::PriorAttestationKnown { .. }
            | Eip8453GossipError::EpochMismatch { .. }
            | Eip8453GossipError::UnknownBlock(_)
            | Eip8453GossipError::NotFinalizedDescendant(_)
            | Eip8453GossipError::PayloadNotVerified(_)
            | Eip8453GossipError::NoNewSlashableIndices
            | Eip8453GossipError::PriorAggregateKnown { .. }
            | Eip8453GossipError::StaleAggregateData(_)
            | Eip8453GossipError::Internal(_) => Eip8453GossipVerdict::Ignore,
            Eip8453GossipError::PreFork
            | Eip8453GossipError::BlockFromFuture { .. }
            | Eip8453GossipError::InvalidPayloadStatus(_)
            | Eip8453GossipError::NotInCommittee { .. }
            | Eip8453GossipError::UnknownValidator(_)
            | Eip8453GossipError::InactiveValidator(_)
            | Eip8453GossipError::InvalidSignature(_)
            | Eip8453GossipError::CommitteeIndexOutOfRange(_)
            | Eip8453GossipError::WrongSubnet { .. }
            | Eip8453GossipError::MalformedData
            | Eip8453GossipError::InvalidSlashing(_)
            | Eip8453GossipError::EmptyAggregate
            | Eip8453GossipError::UnsortedVoters
            | Eip8453GossipError::NotSelectedAggregator(_)
            | Eip8453GossipError::InvalidSelectionProof(_)
            | Eip8453GossipError::InvalidAggregateSignature
            | Eip8453GossipError::InvalidAggregate(_) => Eip8453GossipVerdict::Reject,
        }
    }
}

fn internal(message: impl Into<String>) -> Eip8453GossipError {
    Eip8453GossipError::Internal(message.into())
}

/// The spec's `Seen` caches for the single-object EIP-8453 topics, keyed so stale entries prune
/// by slot/round. Only objects that pass full validation are marked, per the spec's
/// mark-at-the-end ordering.
#[derive(Default)]
pub struct Eip8453ObservedGossip {
    /// `seen.head_vote_validator_slots`: `(slot, validator)`.
    head_votes: Mutex<BTreeMap<Slot, HashSet<u64>>>,
    /// `seen.safe_vote_data_roots` and `seen.safe_vote_distinct_counts` in one map: the distinct
    /// data roots seen per `(round, validator)` (the spec caps the vec at two).
    safe_votes: Mutex<BTreeMap<Round, HashMap<u64, Vec<Hash256>>>>,
    /// `seen.attestation_validator_rounds`: `(round, validator)`.
    fg_attestations: Mutex<BTreeMap<Round, HashSet<u64>>>,
    /// `seen.head_vote_aggregator_slots`: `(slot, aggregator)`.
    head_vote_aggregators: Mutex<BTreeMap<Slot, HashSet<u64>>>,
    /// `seen.safe_vote_aggregator_rounds`: `(round, aggregator, data_root)`.
    safe_vote_aggregators: Mutex<BTreeMap<Round, HashSet<(u64, Hash256)>>>,
    /// `seen.attestation_aggregator_rounds`: `(round, aggregator, data_root)`.
    fg_attestation_aggregators: Mutex<BTreeMap<Round, HashSet<(u64, Hash256)>>>,
}

/// How many slots/rounds of seen-keys to retain behind the current one. Gossip only accepts
/// current-slot/round objects, so one step of history absorbs boundary races.
const SEEN_RETENTION: u64 = 2;

impl Eip8453ObservedGossip {
    fn head_vote_seen(&self, slot: Slot, validator_index: u64) -> bool {
        self.head_votes
            .lock()
            .get(&slot)
            .is_some_and(|validators| validators.contains(&validator_index))
    }

    fn mark_head_vote(&self, slot: Slot, validator_index: u64) {
        let mut cache = self.head_votes.lock();
        let cutoff = Slot::new(slot.as_u64().saturating_sub(SEEN_RETENTION));
        *cache = cache.split_off(&cutoff);
        cache.entry(slot).or_default().insert(validator_index);
    }

    /// The distinct safe-vote data roots seen from `(round, validator)`.
    fn safe_vote_roots(&self, round: Round, validator_index: u64) -> Vec<Hash256> {
        self.safe_votes
            .lock()
            .get(&round)
            .and_then(|validators| validators.get(&validator_index))
            .cloned()
            .unwrap_or_default()
    }

    fn mark_safe_vote(&self, round: Round, validator_index: u64, data_root: Hash256) {
        let mut cache = self.safe_votes.lock();
        let cutoff = round.saturating_sub(SEEN_RETENTION);
        *cache = cache.split_off(&cutoff);
        let roots = cache
            .entry(round)
            .or_default()
            .entry(validator_index)
            .or_default();
        if !roots.contains(&data_root) {
            roots.push(data_root);
        }
    }

    fn fg_attestation_seen(&self, round: Round, validator_index: u64) -> bool {
        self.fg_attestations
            .lock()
            .get(&round)
            .is_some_and(|validators| validators.contains(&validator_index))
    }

    fn mark_fg_attestation(&self, round: Round, validator_index: u64) {
        let mut cache = self.fg_attestations.lock();
        let cutoff = round.saturating_sub(SEEN_RETENTION);
        *cache = cache.split_off(&cutoff);
        cache.entry(round).or_default().insert(validator_index);
    }

    fn head_vote_aggregator_seen(&self, slot: Slot, aggregator_index: u64) -> bool {
        self.head_vote_aggregators
            .lock()
            .get(&slot)
            .is_some_and(|aggregators| aggregators.contains(&aggregator_index))
    }

    fn mark_head_vote_aggregator(&self, slot: Slot, aggregator_index: u64) {
        let mut cache = self.head_vote_aggregators.lock();
        let cutoff = Slot::new(slot.as_u64().saturating_sub(SEEN_RETENTION));
        *cache = cache.split_off(&cutoff);
        cache.entry(slot).or_default().insert(aggregator_index);
    }

    fn safe_vote_aggregator_seen(
        &self,
        round: Round,
        aggregator_index: u64,
        data_root: Hash256,
    ) -> bool {
        self.safe_vote_aggregators
            .lock()
            .get(&round)
            .is_some_and(|aggregators| aggregators.contains(&(aggregator_index, data_root)))
    }

    fn mark_safe_vote_aggregator(&self, round: Round, aggregator_index: u64, data_root: Hash256) {
        let mut cache = self.safe_vote_aggregators.lock();
        let cutoff = round.saturating_sub(SEEN_RETENTION);
        *cache = cache.split_off(&cutoff);
        cache
            .entry(round)
            .or_default()
            .insert((aggregator_index, data_root));
    }

    fn fg_attestation_aggregator_seen(
        &self,
        round: Round,
        aggregator_index: u64,
        data_root: Hash256,
    ) -> bool {
        self.fg_attestation_aggregators
            .lock()
            .get(&round)
            .is_some_and(|aggregators| aggregators.contains(&(aggregator_index, data_root)))
    }

    fn mark_fg_attestation_aggregator(
        &self,
        round: Round,
        aggregator_index: u64,
        data_root: Hash256,
    ) {
        let mut cache = self.fg_attestation_aggregators.lock();
        let cutoff = round.saturating_sub(SEEN_RETENTION);
        *cache = cache.split_off(&cutoff);
        cache
            .entry(round)
            .or_default()
            .insert((aggregator_index, data_root));
    }
}

/// `verify_head_vote_payload_status` from the p2p spec: the payload status is consistent with
/// the block being voted for.
///
/// The spec's separate optimistic/invalidated verdicts collapse into the payload-verified
/// check — see the module docs.
fn check_head_vote_payload_status<E: EthSpec>(
    fg: &FgForkChoiceStore<E>,
    data: &HeadVoteData,
    block_slot: Slot,
) -> Result<(), Eip8453GossipError> {
    if block_slot == data.slot && data.payload_status != PAYLOAD_STATUS_PENDING {
        // [REJECT] A same-slot vote has a pending payload status.
        return Err(Eip8453GossipError::InvalidPayloadStatus(
            data.payload_status,
        ));
    }
    let is_payload_decision =
        data.payload_status == PAYLOAD_STATUS_EMPTY || data.payload_status == PAYLOAD_STATUS_FULL;
    if block_slot < data.slot && !is_payload_decision {
        // [REJECT] A vote for an earlier block is a payload decision.
        return Err(Eip8453GossipError::InvalidPayloadStatus(
            data.payload_status,
        ));
    }
    if data.payload_status == PAYLOAD_STATUS_FULL
        && !fg.is_payload_verified(&data.beacon_block_root)
    {
        // [IGNORE] The voted payload has been seen and verified.
        return Err(Eip8453GossipError::PayloadNotVerified(
            data.beacon_block_root,
        ));
    }
    Ok(())
}

/// [REJECT] The voters are sorted, unique and nonempty.
fn check_voter_indices(voters: &[u64]) -> Result<(), Eip8453GossipError> {
    if voters.is_empty() {
        return Err(Eip8453GossipError::EmptyAggregate);
    }
    if !voters.windows(2).all(|pair| pair[0] < pair[1]) {
        return Err(Eip8453GossipError::UnsortedVoters);
    }
    Ok(())
}

impl<T: BeaconChainTypes> BeaconChain<T> {
    /// The slot range the gossip clock accepts as "current", per the spec's `is_current_slot`
    /// with `MAXIMUM_GOSSIP_CLOCK_DISPARITY` allowance.
    fn eip8453_gossip_slot_range(&self) -> Result<(Slot, Slot), Eip8453GossipError> {
        let now = self
            .slot_clock
            .now_duration()
            .ok_or_else(|| internal("slot clock not started"))?;
        let disparity = self.spec.maximum_gossip_clock_disparity();
        let earliest = self
            .slot_clock
            .slot_of(now.saturating_sub(disparity))
            .ok_or_else(|| internal("pre-genesis clock"))?;
        let latest = self
            .slot_clock
            .slot_of(now.saturating_add(disparity))
            .ok_or_else(|| internal("pre-genesis clock"))?;
        Ok((earliest, latest))
    }

    /// `validate_head_vote_gossip` from the p2p spec. On success the vote is marked seen; feed it
    /// with [`BeaconChain::eip8453_import_verified_head_vote`].
    pub fn eip8453_verify_head_vote_for_gossip(
        &self,
        message: &HeadVoteMessage,
    ) -> Result<(), Eip8453GossipError> {
        let data = &message.data;
        let validator_index = message.validator_index;

        // [REJECT] The head vote's slot is at or after the EIP-8453 fork.
        if !self
            .spec
            .fork_name_at_slot::<T::EthSpec>(data.slot)
            .eip8453_enabled()
        {
            return Err(Eip8453GossipError::PreFork);
        }

        // [IGNORE] This is the first valid head vote from this validator for the slot.
        if self
            .eip8453_observed_gossip
            .head_vote_seen(data.slot, validator_index)
        {
            return Err(Eip8453GossipError::PriorHeadVoteKnown {
                validator_index,
                slot: data.slot,
            });
        }

        // [IGNORE] The head vote is for the current slot (with clock disparity).
        let (earliest, latest) = self.eip8453_gossip_slot_range()?;
        if data.slot < earliest || data.slot > latest {
            return Err(Eip8453GossipError::NotCurrentSlot {
                vote_slot: data.slot,
                current_slot: latest,
            });
        }

        // Keep the fork-choice clock fresh so the committee window and payload maps are current.
        self.eip8453_tick_fg_to_now()
            .map_err(|e| internal(format!("fg tick: {e:?}")))?;

        {
            let fork_choice = self.canonical_head.fork_choice_read_lock();
            let fg = fork_choice
                .fg()
                .ok_or_else(|| internal("fg fork choice not active"))?;

            // [IGNORE] The block being voted for has been seen (MAY be queued until retrieved).
            // The spec's separate "failed validation" REJECT collapses here: invalid blocks are
            // never indexed, so they are indistinguishable from unseen ones.
            let block_slot = fg
                .block_slot(&data.beacon_block_root)
                .ok_or(Eip8453GossipError::UnknownBlock(data.beacon_block_root))?;

            // [REJECT] The block being voted for is not from a later slot than the head vote.
            if block_slot > data.slot {
                return Err(Eip8453GossipError::BlockFromFuture {
                    block_slot,
                    vote_slot: data.slot,
                });
            }

            // [IGNORE] The finalized block is an ancestor of the block being voted for.
            let finalized_root = fg.finalized_checkpoint.root;
            if !fg
                .is_ancestor_root(finalized_root, data.beacon_block_root)
                .unwrap_or(false)
            {
                return Err(Eip8453GossipError::NotFinalizedDescendant(
                    data.beacon_block_root,
                ));
            }

            // The payload status is consistent with the block being voted for.
            check_head_vote_payload_status(fg, data, block_slot)?;

            // [REJECT] The validator is a member of the head vote committee.
            let on_committee = fg
                .committee_at_slot(data.slot)
                .map_err(|e| internal(format!("head vote committee: {e:?}")))?
                .contains(&validator_index);
            if !on_committee {
                return Err(Eip8453GossipError::NotInCommittee { validator_index });
            }
        }

        // [REJECT] The signature is valid (against the head state, like every verifier).
        let head = self.head_snapshot();
        let signing_root = data.compute_signing_root::<T::EthSpec>(
            &head.beacon_state.fork(),
            head.beacon_state.genesis_validators_root(),
            &self.spec,
        );
        self.eip8453_verify_gossip_signature(validator_index, |pk| {
            message.signature.verify(pk, signing_root)
        })?;

        self.eip8453_observed_gossip
            .mark_head_vote(data.slot, validator_index);
        Ok(())
    }

    /// `validate_safe_vote_gossip` from the p2p spec. Up to two distinct votes per
    /// `(round, validator)` pass: the second is equivocation evidence for the stabilization rule.
    pub fn eip8453_verify_safe_vote_for_gossip(
        &self,
        message: &SafeVoteMessage,
    ) -> Result<(), Eip8453GossipError> {
        let data = &message.data;
        let validator_index = message.validator_index;
        let round_start_slot = compute_start_slot_at_round::<T::EthSpec>(data.round);

        // [REJECT] The safe vote's round is at or after the EIP-8453 fork.
        if !self
            .spec
            .fork_name_at_slot::<T::EthSpec>(round_start_slot)
            .eip8453_enabled()
        {
            return Err(Eip8453GossipError::PreFork);
        }

        // [REJECT] The payload status is a voteable status.
        if data.payload_status != PAYLOAD_STATUS_EMPTY
            && data.payload_status != PAYLOAD_STATUS_FULL
            && data.payload_status != PAYLOAD_STATUS_PENDING
        {
            return Err(Eip8453GossipError::InvalidPayloadStatus(
                data.payload_status,
            ));
        }

        // [IGNORE] The safe vote is for the current round (with clock disparity).
        let (earliest_slot, latest_slot) = self.eip8453_gossip_slot_range()?;
        let earliest_round = compute_round_at_slot::<T::EthSpec>(earliest_slot);
        let latest_round = compute_round_at_slot::<T::EthSpec>(latest_slot);
        if data.round < earliest_round || data.round > latest_round {
            return Err(Eip8453GossipError::NotCurrentRound {
                vote_round: data.round,
                current_round: latest_round,
            });
        }

        // [IGNORE] This exact safe vote has not been seen; at most two distinct ones propagate.
        let data_root = data.tree_hash_root();
        let seen_roots = self
            .eip8453_observed_gossip
            .safe_vote_roots(data.round, validator_index);
        if seen_roots.contains(&data_root) {
            return Err(Eip8453GossipError::DuplicateSafeVote {
                validator_index,
                round: data.round,
            });
        }
        if seen_roots.len() >= 2 {
            return Err(Eip8453GossipError::SafeVoteEquivocationLimit {
                validator_index,
                round: data.round,
            });
        }

        self.eip8453_tick_fg_to_now()
            .map_err(|e| internal(format!("fg tick: {e:?}")))?;

        {
            let fork_choice = self.canonical_head.fork_choice_read_lock();
            let fg = fork_choice
                .fg()
                .ok_or_else(|| internal("fg fork choice not active"))?;

            // [IGNORE] The block being voted for has been seen (MAY be queued until retrieved).
            if fg.block_slot(&data.beacon_block_root).is_none() {
                return Err(Eip8453GossipError::UnknownBlock(data.beacon_block_root));
            }

            // [IGNORE] The finalized block is an ancestor of the block being voted for.
            let finalized_root = fg.finalized_checkpoint.root;
            if !fg
                .is_ancestor_root(finalized_root, data.beacon_block_root)
                .unwrap_or(false)
            {
                return Err(Eip8453GossipError::NotFinalizedDescendant(
                    data.beacon_block_root,
                ));
            }
        }

        // [REJECT] The validator is active in the round's epoch.
        let head = self.head_snapshot();
        let round_epoch = round_start_slot.epoch(T::EthSpec::slots_per_epoch());
        let is_active = head
            .beacon_state
            .validators()
            .get(validator_index as usize)
            .ok_or(Eip8453GossipError::UnknownValidator(validator_index))?
            .is_active_at(round_epoch);
        if !is_active {
            return Err(Eip8453GossipError::InactiveValidator(validator_index));
        }

        // [REJECT] The signature is valid.
        let signing_root = data.compute_signing_root::<T::EthSpec>(
            &head.beacon_state.fork(),
            head.beacon_state.genesis_validators_root(),
            &self.spec,
        );
        self.eip8453_verify_gossip_signature(validator_index, |pk| {
            message.signature.verify(pk, signing_root)
        })?;

        self.eip8453_observed_gossip
            .mark_safe_vote(data.round, validator_index, data_root);
        Ok(())
    }

    /// `validate_attestation_gossip` from the p2p spec, for an FG attestation single on a subnet.
    /// FG attestations carry no slot: the committee's latest started slot fixes the round and
    /// epoch (`get_latest_attestation_committee_slot`).
    pub fn eip8453_verify_fg_attestation_for_gossip(
        &self,
        attestation: &eip8453::SingleAttestation,
        subnet_id: u64,
    ) -> Result<(), Eip8453GossipError> {
        let data = &attestation.data;
        let committee_index = attestation.committee_index;
        let attester_index = attestation.attester_index;

        // [REJECT] The committee index is within the expected range.
        if committee_index >= T::EthSpec::attestation_committees_per_round() as u64 {
            return Err(Eip8453GossipError::CommitteeIndexOutOfRange(
                committee_index,
            ));
        }

        // [REJECT] The attestation is for the correct subnet.
        let expected_subnet =
            compute_subnet_for_fg_attestation(committee_index, self.spec.attestation_subnet_count);
        if expected_subnet != subnet_id {
            return Err(Eip8453GossipError::WrongSubnet {
                expected: expected_subnet,
                actual: subnet_id,
            });
        }

        // `get_latest_attestation_committee_slot`: the committee's latest started slot.
        let (_, latest_slot) = self.eip8453_gossip_slot_range()?;
        let current_round = compute_round_at_slot::<T::EthSpec>(latest_slot);
        let mut committee_slot =
            compute_attestation_committee_slot::<T::EthSpec>(current_round, committee_index);
        if committee_slot > latest_slot {
            committee_slot = compute_attestation_committee_slot::<T::EthSpec>(
                current_round.saturating_sub(1),
                committee_index,
            );
        }
        let committee_round = compute_round_at_slot::<T::EthSpec>(committee_slot);

        // [IGNORE] No other valid attestation seen for this round and validator.
        if self
            .eip8453_observed_gossip
            .fg_attestation_seen(committee_round, attester_index)
        {
            return Err(Eip8453GossipError::PriorAttestationKnown {
                validator_index: attester_index,
                round: committee_round,
            });
        }

        // [IGNORE] The attestation's epoch is the epoch of its committee's slot.
        let committee_epoch = committee_slot.epoch(T::EthSpec::slots_per_epoch());
        if attestation.epoch != committee_epoch {
            return Err(Eip8453GossipError::EpochMismatch {
                attestation_epoch: attestation.epoch,
                committee_epoch,
            });
        }

        // [REJECT] The attestation data is well formed.
        if !data.is_well_formed() {
            return Err(Eip8453GossipError::MalformedData);
        }

        self.eip8453_tick_fg_to_now()
            .map_err(|e| internal(format!("fg tick: {e:?}")))?;

        {
            let fork_choice = self.canonical_head.fork_choice_read_lock();
            let fg = fork_choice
                .fg()
                .ok_or_else(|| internal("fg fork choice not active"))?;
            let finalized_root = fg.finalized_checkpoint.root;

            // [IGNORE] The target block has been seen. An attestation's target may be the
            // finalized block itself, which survives pruning as the fork-choice anchor.
            if fg.block_slot(&data.target.root).is_none() && data.target.root != finalized_root {
                return Err(Eip8453GossipError::UnknownBlock(data.target.root));
            }

            // [IGNORE] The finalized block is an ancestor of the target block.
            if !fg
                .is_ancestor_root(finalized_root, data.target.root)
                .unwrap_or(false)
            {
                return Err(Eip8453GossipError::NotFinalizedDescendant(data.target.root));
            }
        }

        // [REJECT] The attester is a member of the committee.
        let head = self.head_snapshot();
        let committee = head
            .beacon_state
            .get_attestation_committee(attestation.epoch, committee_index, &self.spec)
            .map_err(|e| internal(format!("attestation committee: {e:?}")))?;
        if !committee.contains(&(attester_index as usize)) {
            return Err(Eip8453GossipError::NotInCommittee {
                validator_index: attester_index,
            });
        }

        // [REJECT] The attestation signature is valid.
        let signing_root = data.signing_root_at_epoch(
            attestation.epoch,
            &head.beacon_state.fork(),
            head.beacon_state.genesis_validators_root(),
            &self.spec,
        );
        self.eip8453_verify_gossip_signature(attester_index, |pk| {
            attestation
                .signature
                .fast_aggregate_verify(signing_root, &[pk])
        })?;

        self.eip8453_observed_gossip
            .mark_fg_attestation(committee_round, attester_index);
        Ok(())
    }

    /// Gossip validation for an EIP-8453 `attester_slashing` ("the validation logic is
    /// unchanged"): slashability, at least one not-yet-slashed target, and full verification —
    /// including both aggregate signatures — by running the state transition's
    /// `process_attester_slashing` on a throwaway copy of the head state.
    pub fn eip8453_verify_attester_slashing_for_gossip(
        &self,
        slashing: &eip8453::AttesterSlashing<T::EthSpec>,
    ) -> Result<(), Eip8453GossipError> {
        let attestation_1 = &slashing.attestation_1;
        let attestation_2 = &slashing.attestation_2;

        // [REJECT] The attestations are slashable.
        if !eip8453::is_slashable_attestation_data(&attestation_1.data, &attestation_2.data) {
            return Err(Eip8453GossipError::InvalidSlashing(
                "attestations are not slashable".into(),
            ));
        }

        let head = self.head_snapshot();
        let state = &head.beacon_state;

        // [IGNORE] At least one doubly-attesting validator is not already slashed.
        let indices_2: HashSet<u64> = attestation_2.attesting_indices.iter().copied().collect();
        let current_epoch = state.current_epoch();
        let any_slashable = attestation_1
            .attesting_indices
            .iter()
            .filter(|index| indices_2.contains(index))
            .any(|&index| {
                state
                    .validators()
                    .get(index as usize)
                    .is_some_and(|validator| validator.is_slashable_at(current_epoch))
            });
        if !any_slashable {
            return Err(Eip8453GossipError::NoNewSlashableIndices);
        }

        // [REJECT] Everything else (index ordering, both signatures, ...): run the state
        // transition's own check on a throwaway copy of the head state.
        let mut state = state.clone();
        state
            .build_slashings_cache()
            .map_err(|e| internal(format!("slashings cache: {e:?}")))?;
        let mut ctxt = ConsensusContext::new(state.slot());
        process_attester_slashing_eip8453(
            &mut state,
            slashing,
            VerifySignatures::True,
            &mut ctxt,
            &self.spec,
        )
        .map_err(|e| Eip8453GossipError::InvalidSlashing(format!("{e:?}")))?;

        Ok(())
    }

    /// `validate_head_vote_aggregate_gossip` from the p2p spec.
    pub fn eip8453_verify_head_vote_aggregate_for_gossip(
        &self,
        signed_aggregate: &SignedHeadVoteAggregateAndProof<T::EthSpec>,
    ) -> Result<(), Eip8453GossipError> {
        let message = &signed_aggregate.message;
        let aggregate = &message.aggregate;
        let data = &aggregate.data;
        let aggregator_index = message.aggregator_index;
        let voters: Vec<u64> = aggregate.voter_indices.iter().copied().collect();

        // [IGNORE] This is the first aggregate from this aggregator for the slot.
        if self
            .eip8453_observed_gossip
            .head_vote_aggregator_seen(data.slot, aggregator_index)
        {
            return Err(Eip8453GossipError::PriorAggregateKnown { aggregator_index });
        }

        // [IGNORE] The aggregate is for the current slot (with clock disparity).
        let (earliest, latest) = self.eip8453_gossip_slot_range()?;
        if data.slot < earliest || data.slot > latest {
            return Err(Eip8453GossipError::NotCurrentSlot {
                vote_slot: data.slot,
                current_slot: latest,
            });
        }

        self.eip8453_tick_fg_to_now()
            .map_err(|e| internal(format!("fg tick: {e:?}")))?;

        {
            let fork_choice = self.canonical_head.fork_choice_read_lock();
            let fg = fork_choice
                .fg()
                .ok_or_else(|| internal("fg fork choice not active"))?;

            // [IGNORE] The block being voted for has been seen.
            let block_slot = fg
                .block_slot(&data.beacon_block_root)
                .ok_or(Eip8453GossipError::UnknownBlock(data.beacon_block_root))?;

            // [REJECT] The block being voted for is not from a later slot than the vote.
            if block_slot > data.slot {
                return Err(Eip8453GossipError::BlockFromFuture {
                    block_slot,
                    vote_slot: data.slot,
                });
            }

            // The payload status is consistent with the block being voted for.
            check_head_vote_payload_status(fg, data, block_slot)?;

            // [REJECT] The voters are sorted, unique, nonempty committee members, and the
            // aggregator is a member of the committee.
            let committee = fg
                .committee_at_slot(data.slot)
                .map_err(|e| internal(format!("head vote committee: {e:?}")))?;
            check_voter_indices(&voters)?;
            for &validator_index in &voters {
                if !committee.contains(&validator_index) {
                    return Err(Eip8453GossipError::NotInCommittee { validator_index });
                }
            }
            if !committee.contains(&aggregator_index) {
                return Err(Eip8453GossipError::NotInCommittee {
                    validator_index: aggregator_index,
                });
            }
        }

        // [REJECT] The selection proof selects the aggregator and verifies.
        if !is_selected_aggregator(
            &message.selection_proof,
            T::EthSpec::head_vote_committee_size() as u64,
            self.spec.target_aggregators_per_committee,
        ) {
            return Err(Eip8453GossipError::NotSelectedAggregator(aggregator_index));
        }
        let head = self.head_snapshot();
        let fork = head.beacon_state.fork();
        let genesis_validators_root = head.beacon_state.genesis_validators_root();
        let selection_root = head_vote_aggregator_selection_root::<T::EthSpec>(
            data.slot,
            &fork,
            genesis_validators_root,
            &self.spec,
        );
        self.eip8453_verify_gossip_signature(aggregator_index, |pk| {
            message.selection_proof.verify(pk, selection_root)
        })
        .map_err(|_| Eip8453GossipError::InvalidSelectionProof(aggregator_index))?;

        // [REJECT] The wrapper signature is valid.
        let wrapper_root = message.compute_signing_root(&fork, genesis_validators_root, &self.spec);
        self.eip8453_verify_gossip_signature(aggregator_index, |pk| {
            signed_aggregate.signature.verify(pk, wrapper_root)
        })?;

        // [REJECT] The aggregate signature is valid.
        let aggregate_root =
            data.compute_signing_root::<T::EthSpec>(&fork, genesis_validators_root, &self.spec);
        self.eip8453_verify_aggregate_signature(&voters, aggregate_root, &aggregate.signature)?;

        self.eip8453_observed_gossip
            .mark_head_vote_aggregator(data.slot, aggregator_index);
        Ok(())
    }

    /// `validate_safe_vote_aggregate_gossip` from the p2p spec. One aggregate per distinct
    /// `SafeVoteData` per aggregator propagates, so equivocation evidence aggregates too.
    pub fn eip8453_verify_safe_vote_aggregate_for_gossip(
        &self,
        signed_aggregate: &SignedSafeVoteAggregateAndProof<T::EthSpec>,
    ) -> Result<(), Eip8453GossipError> {
        let message = &signed_aggregate.message;
        let aggregate = &message.aggregate;
        let data = &aggregate.data;
        let aggregator_index = message.aggregator_index;
        let voters: Vec<u64> = aggregate.voter_indices.iter().copied().collect();
        let round_start_slot = compute_start_slot_at_round::<T::EthSpec>(data.round);
        let round_epoch = round_start_slot.epoch(T::EthSpec::slots_per_epoch());
        let data_root = data.tree_hash_root();

        // [IGNORE] This is the first aggregate from this aggregator for the data.
        if self.eip8453_observed_gossip.safe_vote_aggregator_seen(
            data.round,
            aggregator_index,
            data_root,
        ) {
            return Err(Eip8453GossipError::PriorAggregateKnown { aggregator_index });
        }

        // [IGNORE] The safe vote is for the current round (with clock disparity).
        let (earliest_slot, latest_slot) = self.eip8453_gossip_slot_range()?;
        let earliest_round = compute_round_at_slot::<T::EthSpec>(earliest_slot);
        let latest_round = compute_round_at_slot::<T::EthSpec>(latest_slot);
        if data.round < earliest_round || data.round > latest_round {
            return Err(Eip8453GossipError::NotCurrentRound {
                vote_round: data.round,
                current_round: latest_round,
            });
        }

        self.eip8453_tick_fg_to_now()
            .map_err(|e| internal(format!("fg tick: {e:?}")))?;

        {
            let fork_choice = self.canonical_head.fork_choice_read_lock();
            let fg = fork_choice
                .fg()
                .ok_or_else(|| internal("fg fork choice not active"))?;

            // [IGNORE] The block being voted for has been seen.
            if fg.block_slot(&data.beacon_block_root).is_none() {
                return Err(Eip8453GossipError::UnknownBlock(data.beacon_block_root));
            }
        }

        // [REJECT] The voters are sorted, unique, nonempty active validators; the aggregator is
        // active and selected by its proof.
        check_voter_indices(&voters)?;
        let head = self.head_snapshot();
        let state = &head.beacon_state;
        for &validator_index in voters.iter().chain(std::iter::once(&aggregator_index)) {
            let is_active = state
                .validators()
                .get(validator_index as usize)
                .ok_or(Eip8453GossipError::UnknownValidator(validator_index))?
                .is_active_at(round_epoch);
            if !is_active {
                return Err(Eip8453GossipError::InactiveValidator(validator_index));
            }
        }
        let active_count = state
            .get_active_validator_indices(round_epoch, &self.spec)
            .map_err(|e| internal(format!("active validators: {e:?}")))?
            .len() as u64;
        let members = active_count
            .checked_div(T::EthSpec::slots_per_round().saturating_sub(2))
            .unwrap_or(0);
        if !is_selected_aggregator(
            &message.selection_proof,
            members,
            self.spec.target_aggregators_per_committee,
        ) {
            return Err(Eip8453GossipError::NotSelectedAggregator(aggregator_index));
        }
        let fork = state.fork();
        let genesis_validators_root = state.genesis_validators_root();
        let selection_root = round_aggregator_selection_root::<T::EthSpec>(
            data.round,
            Domain::SafeVoteAggregator,
            &fork,
            genesis_validators_root,
            &self.spec,
        );
        self.eip8453_verify_gossip_signature(aggregator_index, |pk| {
            message.selection_proof.verify(pk, selection_root)
        })
        .map_err(|_| Eip8453GossipError::InvalidSelectionProof(aggregator_index))?;

        // [REJECT] The wrapper signature is valid.
        let wrapper_root = message.compute_signing_root(&fork, genesis_validators_root, &self.spec);
        self.eip8453_verify_gossip_signature(aggregator_index, |pk| {
            signed_aggregate.signature.verify(pk, wrapper_root)
        })?;

        // [REJECT] The aggregate signature is valid.
        let aggregate_root =
            data.compute_signing_root::<T::EthSpec>(&fork, genesis_validators_root, &self.spec);
        self.eip8453_verify_aggregate_signature(&voters, aggregate_root, &aggregate.signature)?;

        self.eip8453_observed_gossip.mark_safe_vote_aggregator(
            data.round,
            aggregator_index,
            data_root,
        );
        Ok(())
    }

    /// `validate_attestation_aggregate_gossip` from the p2p spec. Dedup uses the data and
    /// aggregator, since an FG aggregate's epoch is not signed; the selection proof signs the
    /// current round (a clock fact — FG attestations carry no slot).
    pub fn eip8453_verify_attestation_aggregate_for_gossip(
        &self,
        signed_aggregate: &SignedAttestationAggregateAndProof<T::EthSpec>,
    ) -> Result<(), Eip8453GossipError> {
        let message = &signed_aggregate.message;
        let aggregate = &message.aggregate;
        let aggregator_index = message.aggregator_index;
        let data_root = aggregate.data.tree_hash_root();

        let (_, latest_slot) = self.eip8453_gossip_slot_range()?;
        let current_round = compute_round_at_slot::<T::EthSpec>(latest_slot);
        let aggregation_epoch = compute_start_slot_at_round::<T::EthSpec>(current_round)
            .epoch(T::EthSpec::slots_per_epoch());

        // [IGNORE] This is the first aggregate from this aggregator for the data.
        if self.eip8453_observed_gossip.fg_attestation_aggregator_seen(
            current_round,
            aggregator_index,
            data_root,
        ) {
            return Err(Eip8453GossipError::PriorAggregateKnown { aggregator_index });
        }

        // [REJECT] The attestation data is well formed and valid for the state.
        if !aggregate.data.is_well_formed() {
            return Err(Eip8453GossipError::MalformedData);
        }
        // The spec verifies against the PULLED-UP head state: `target_checkpoint` back-fills at
        // slot boundaries, so an honest aggregate built from the current slot's view can be
        // invalid against the unadvanced head post-state.
        let head = self.head_snapshot();
        let mut pulled_up;
        let state = if head.beacon_state.slot() < latest_slot {
            pulled_up = head.beacon_state.clone();
            partial_state_advance(
                &mut pulled_up,
                Some(head.beacon_state_root()),
                latest_slot,
                None,
                &self.spec,
            )
            .map_err(|e| internal(format!("pull up head state: {e:?}")))?;
            &pulled_up
        } else {
            &head.beacon_state
        };
        if !is_valid_attestation_data(state, &aggregate.data)
            .map_err(|e| internal(format!("attestation data: {e:?}")))?
        {
            return Err(Eip8453GossipError::StaleAggregateData(format!(
                "attestation data {:?} is not valid for the head state (slot {:?}, target {:?})",
                aggregate.data,
                state.slot(),
                state.target_checkpoint(),
            )));
        }

        // [REJECT] The aggregation bits match the committees of the epoch.
        let attesting_indices = get_attesting_indices_eip8453(state, aggregate, &self.spec)
            .map_err(|e| Eip8453GossipError::InvalidAggregate(format!("{e:?}")))?;
        if attesting_indices.is_empty() {
            return Err(Eip8453GossipError::EmptyAggregate);
        }

        // [REJECT] The aggregator sits on a committee marked in the aggregate and its proof
        // selects it.
        let mut aggregator_committee_len = None;
        for committee_index in 0..T::EthSpec::attestation_committees_per_round() as u64 {
            if !aggregate
                .committee_bits
                .get(committee_index as usize)
                .unwrap_or(false)
            {
                continue;
            }
            let committee = state
                .get_attestation_committee(aggregate.epoch, committee_index, &self.spec)
                .map_err(|e| internal(format!("attestation committee: {e:?}")))?;
            if committee.contains(&(aggregator_index as usize)) {
                aggregator_committee_len = Some(committee.len() as u64);
                break;
            }
        }
        let Some(members) = aggregator_committee_len else {
            return Err(Eip8453GossipError::NotInCommittee {
                validator_index: aggregator_index,
            });
        };
        if !is_selected_aggregator(
            &message.selection_proof,
            members,
            self.spec.target_aggregators_per_committee,
        ) {
            return Err(Eip8453GossipError::NotSelectedAggregator(aggregator_index));
        }
        let fork = state.fork();
        let genesis_validators_root = state.genesis_validators_root();
        let selection_root = round_aggregator_selection_root::<T::EthSpec>(
            current_round,
            Domain::AttestationAggregator,
            &fork,
            genesis_validators_root,
            &self.spec,
        );
        self.eip8453_verify_gossip_signature(aggregator_index, |pk| {
            message.selection_proof.verify(pk, selection_root)
        })
        .map_err(|_| Eip8453GossipError::InvalidSelectionProof(aggregator_index))?;

        // [REJECT] The wrapper signature is valid.
        let wrapper_root = message.compute_signing_root(
            aggregation_epoch,
            &fork,
            genesis_validators_root,
            &self.spec,
        );
        self.eip8453_verify_gossip_signature(aggregator_index, |pk| {
            signed_aggregate.signature.verify(pk, wrapper_root)
        })?;

        // [REJECT] The aggregate attestation signature is valid.
        let aggregate_root = aggregate.data.signing_root_at_epoch(
            aggregate.epoch,
            &fork,
            genesis_validators_root,
            &self.spec,
        );
        let attesting: Vec<u64> = attesting_indices
            .iter()
            .map(|&index| index as u64)
            .collect();
        self.eip8453_verify_aggregate_signature(&attesting, aggregate_root, &aggregate.signature)?;

        self.eip8453_observed_gossip.mark_fg_attestation_aggregator(
            current_round,
            aggregator_index,
            data_root,
        );
        Ok(())
    }

    /// [REJECT] A `FastAggregateVerify` of `signature` over the voters' pubkeys.
    fn eip8453_verify_aggregate_signature(
        &self,
        voters: &[u64],
        signing_root: Hash256,
        signature: &bls::AggregateSignature,
    ) -> Result<(), Eip8453GossipError> {
        let pubkey_cache = self.validator_pubkey_cache.read();
        let mut pubkeys = Vec::with_capacity(voters.len());
        for &validator_index in voters {
            pubkeys.push(
                pubkey_cache
                    .get(validator_index as usize)
                    .ok_or(Eip8453GossipError::UnknownValidator(validator_index))?,
            );
        }
        if !signature.fast_aggregate_verify(signing_root, &pubkeys) {
            return Err(Eip8453GossipError::InvalidAggregateSignature);
        }
        Ok(())
    }

    fn eip8453_verify_gossip_signature(
        &self,
        validator_index: u64,
        verify: impl FnOnce(&bls::PublicKey) -> bool,
    ) -> Result<(), Eip8453GossipError> {
        let pubkey_cache = self.validator_pubkey_cache.read();
        let pubkey = pubkey_cache
            .get(validator_index as usize)
            .ok_or(Eip8453GossipError::UnknownValidator(validator_index))?;
        if !verify(pubkey) {
            return Err(Eip8453GossipError::InvalidSignature(validator_index));
        }
        Ok(())
    }
}
