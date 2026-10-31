//! EIP-8453 validator duty support: duty computation, fork-choice duty reads, and ingestion of
//! validator-client-produced votes and FG attestations.
//!
//! Prototype notes (greppable STUB markers):
//! - EIP8453-STUB: attestation committee assignments are computed from the head state; the spec
//!   reads assignments from the latest finalized block's state. The reported `dependent_root` is
//!   the finalized block root per spec, so the VC's re-poll triggers are spec-shaped even though
//!   the assignment source is not.
//! - Ingested messages are verified individually (no batching, no observed-message caches).

use std::collections::HashMap;

use bls::{AggregateSignature, PublicKey, PublicKeyBytes};
use eth2::types::{FgAttestationComponents, FgAttesterDuty, HeadVoteDuty};
use ssz::{BitVector, ProgressiveBitList, TryFromIter};
use ssz_types::{ProgressiveVariableList, VariableList};
use types::consts::gloas::{PAYLOAD_STATUS_PENDING, PayloadStatus};
use types::eip8453::{
    HeadVoteData, HeadVoteMessage, SafeVoteData, SafeVoteMessage,
    compute_attestation_committee_slot, compute_round_at_slot,
};
use types::{BeaconState, Epoch, EthSpec, Hash256, Slot, eip8453};

use crate::errors::BeaconChainError as Error;
use crate::{BeaconChain, BeaconChainTypes};

fn eip8453_err(message: impl Into<String>) -> Error {
    Error::Eip8453(message.into())
}

impl<T: BeaconChainTypes> BeaconChain<T> {
    /// The FG attester duties for `epoch`: one duty per round whose assigned committee slot
    /// falls in the epoch.
    pub fn eip8453_fg_attester_duties(
        &self,
        epoch: Epoch,
        validator_indices: &[u64],
    ) -> Result<(Vec<FgAttesterDuty>, Hash256), Error> {
        let head = self.head_snapshot();
        // EIP8453-STUB: head state, not the finalized state (see module docs).
        let state = &head.beacon_state;
        let dependent_root = self.eip8453_duty_dependent_root();

        let committees_per_round = T::EthSpec::attestation_committees_per_round() as u64;
        let mut assignment: HashMap<u64, u64> = HashMap::new();
        for committee_index in 0..committees_per_round {
            for validator_index in state
                .get_attestation_committee(epoch, committee_index, &self.spec)
                .map_err(Error::BeaconStateError)?
            {
                assignment.insert(validator_index as u64, committee_index);
            }
        }

        let slots_per_epoch = T::EthSpec::slots_per_epoch();
        let start_slot = epoch.start_slot(slots_per_epoch);
        let end_slot = start_slot.saturating_add(slots_per_epoch);

        let pubkey_cache = self.validator_pubkey_cache.read();
        let mut duties = Vec::new();
        for &validator_index in validator_indices {
            let Some(&committee_index) = assignment.get(&validator_index) else {
                continue;
            };
            let Some(pubkey) = pubkey_cache.get_pubkey_bytes(validator_index as usize) else {
                continue;
            };
            let mut round = compute_round_at_slot::<T::EthSpec>(start_slot);
            loop {
                let slot = compute_attestation_committee_slot::<T::EthSpec>(round, committee_index);
                if slot >= end_slot {
                    break;
                }
                if slot >= start_slot {
                    duties.push(FgAttesterDuty {
                        pubkey: *pubkey,
                        validator_index,
                        committee_index,
                        slot,
                    });
                }
                round = round.saturating_add(1);
            }
        }
        Ok((duties, dependent_root))
    }

    /// The head vote duties for `epoch`, read from the fork choice's committee window (the same
    /// source that verifies incoming votes). Slots beyond the known window are omitted.
    pub fn eip8453_head_vote_duties(
        &self,
        epoch: Epoch,
        validator_indices: &[u64],
    ) -> Result<(Vec<HeadVoteDuty>, Hash256), Error> {
        let dependent_root = self.eip8453_duty_dependent_root();
        let slots_per_epoch = T::EthSpec::slots_per_epoch();
        let start_slot = epoch.start_slot(slots_per_epoch);

        // Collect the window under the fork choice lock, then resolve pubkeys without it.
        let mut committees: Vec<(Slot, Vec<u64>)> = Vec::new();
        {
            let fork_choice = self.canonical_head.fork_choice_read_lock();
            let fg = fork_choice
                .fg()
                .ok_or_else(|| eip8453_err("fg fork choice not active"))?;
            for offset in 0..slots_per_epoch {
                let slot = start_slot.saturating_add(offset);
                if let Ok(committee) = fg.committee_at_slot(slot) {
                    committees.push((slot, committee.clone()));
                }
            }
        }

        let pubkey_cache = self.validator_pubkey_cache.read();
        let mut duties = Vec::new();
        for &validator_index in validator_indices {
            let Some(pubkey) = pubkey_cache.get_pubkey_bytes(validator_index as usize) else {
                continue;
            };
            for (slot, committee) in &committees {
                if committee.contains(&validator_index) {
                    duties.push(HeadVoteDuty {
                        pubkey: *pubkey,
                        validator_index,
                        slot: *slot,
                    });
                }
            }
        }
        Ok((duties, dependent_root))
    }

    /// The spec's duty dependent root: the latest finalized block root.
    fn eip8453_duty_dependent_root(&self) -> Hash256 {
        self.canonical_head
            .cached_head()
            .finalized_checkpoint()
            .root
    }

    /// The head vote data for `slot` per the AC voting procedure: `get_voter_head` plus the
    /// same-slot `PENDING` rule.
    pub fn eip8453_produce_head_vote_data(&self, slot: Slot) -> Result<HeadVoteData, Error> {
        let fork_choice = self.canonical_head.fork_choice_read_lock();
        let fg = fork_choice
            .fg()
            .ok_or_else(|| eip8453_err("fg fork choice not active"))?;
        let node = fg
            .get_voter_head(slot)
            .map_err(|e| eip8453_err(format!("get_voter_head: {e:?}")))?;
        let payload_status: PayloadStatus = if fg.block_slot(&node.root) == Some(slot) {
            PAYLOAD_STATUS_PENDING
        } else {
            node.payload_status
        };
        Ok(HeadVoteData {
            slot,
            beacon_block_root: node.root,
            payload_status,
        })
    }

    /// The safe vote data for the current round: `get_safe_vote_node`.
    pub fn eip8453_produce_safe_vote_data(&self) -> Result<SafeVoteData, Error> {
        let slot = self.slot()?;
        let round = compute_round_at_slot::<T::EthSpec>(slot);
        let fork_choice = self.canonical_head.fork_choice_read_lock();
        let fg = fork_choice
            .fg()
            .ok_or_else(|| eip8453_err("fg fork choice not active"))?;
        let node = fg
            .get_safe_vote_node()
            .map_err(|e| eip8453_err(format!("get_safe_vote_node: {e:?}")))?;
        Ok(SafeVoteData {
            round,
            beacon_block_root: node.root,
            payload_status: node.payload_status,
        })
    }

    /// The single AC-to-FG observation backing an FG attestation: target from the confirmed
    /// (source) block's state, finality pair inputs from the head's state, and the fresh-quorum
    /// bit.
    pub fn eip8453_produce_fg_attestation_components(
        &self,
    ) -> Result<FgAttestationComponents, Error> {
        let (source_root, head_root, fresh_quorum) = {
            let fork_choice = self.canonical_head.fork_choice_read_lock();
            let fg = fork_choice
                .fg()
                .ok_or_else(|| eip8453_err("fg fork choice not active"))?;
            let head = fg
                .get_head(None)
                .map_err(|e| eip8453_err(format!("get_head: {e:?}")))?;
            let fresh = fg
                .has_fresh_safe_quorum()
                .map_err(|e| eip8453_err(format!("has_fresh_safe_quorum: {e:?}")))?;
            (fg.confirmed.root, head.root, fresh)
        };

        let source_state = self.eip8453_state_for_block(source_root)?;
        let mut target = source_state
            .target_checkpoint()
            .map_err(Error::BeaconStateError)?;
        // The spec back-fills the root when the source advanced the height.
        if target.root.is_zero() {
            target.root = source_root;
        }

        let head_state = self.eip8453_state_for_block(head_root)?;
        let justified = head_state
            .justified_checkpoint()
            .map_err(Error::BeaconStateError)?;
        let finalized_height = head_state
            .finalized_checkpoint_eip8453()
            .map_err(Error::BeaconStateError)?
            .height;

        Ok(FgAttestationComponents {
            target,
            justified,
            finalized_height,
            fresh_quorum,
        })
    }

    /// The post-state of `block_root`, from the head snapshot when possible, else the store.
    fn eip8453_state_for_block(
        &self,
        block_root: Hash256,
    ) -> Result<BeaconState<T::EthSpec>, Error> {
        let head = self.head_snapshot();
        if head.beacon_block_root == block_root {
            return Ok(head.beacon_state.clone());
        }
        let block = self
            .store
            .get_blinded_block(&block_root)
            .map_err(|e| eip8453_err(format!("load block {block_root:?}: {e:?}")))?
            .ok_or_else(|| eip8453_err(format!("unknown block {block_root:?}")))?;
        self.get_state(&block.state_root(), Some(block.slot()), true)?
            .ok_or_else(|| eip8453_err(format!("missing state for block {block_root:?}")))
    }

    /// Verify and ingest a validator's head vote: committee membership and signature, then the
    /// fork choice pool and the block-packing pool.
    pub fn eip8453_process_head_vote(&self, message: HeadVoteMessage) -> Result<(), Error> {
        let head = self.head_snapshot();
        let signing_root = message.data.compute_signing_root::<T::EthSpec>(
            &head.beacon_state.fork(),
            head.beacon_state.genesis_validators_root(),
            &self.spec,
        );
        self.eip8453_verify_single_signature(message.validator_index, signing_root, |pk| {
            message.signature.verify(pk, signing_root)
        })?;

        {
            let mut fork_choice = self.canonical_head.fork_choice_write_lock();
            let on_committee = fork_choice
                .fg()
                .ok_or_else(|| eip8453_err("fg fork choice not active"))?
                .committee_at_slot(message.data.slot)
                .map_err(|e| eip8453_err(format!("head vote committee: {e:?}")))?
                .contains(&message.validator_index);
            if !on_committee {
                return Err(eip8453_err(format!(
                    "validator {} not on head vote committee of slot {}",
                    message.validator_index, message.data.slot
                )));
            }
            fork_choice.on_head_vote(message.validator_index, message.data);
        }
        self.eip8453_pool.insert_head_vote_single(message);
        Ok(())
    }

    /// Verify and ingest a validator's safe vote.
    pub fn eip8453_process_safe_vote(&self, message: SafeVoteMessage) -> Result<(), Error> {
        let head = self.head_snapshot();
        let signing_root = message.data.compute_signing_root::<T::EthSpec>(
            &head.beacon_state.fork(),
            head.beacon_state.genesis_validators_root(),
            &self.spec,
        );
        self.eip8453_verify_single_signature(message.validator_index, signing_root, |pk| {
            message.signature.verify(pk, signing_root)
        })?;

        self.canonical_head
            .fork_choice_write_lock()
            .on_safe_vote(message.validator_index, message.data);
        self.eip8453_pool.insert_safe_vote_single(message);
        Ok(())
    }

    /// Verify and ingest a validator's FG attestation (a single): committee membership for its
    /// epoch and signature, then the block-packing pool. The fork choice reads FG effects only
    /// from imported blocks' post-states, so no fork-choice feed happens here.
    pub fn eip8453_process_fg_attestation(
        &self,
        attestation: eip8453::SingleAttestation,
    ) -> Result<(), Error> {
        let head = self.head_snapshot();
        let state = &head.beacon_state;
        let committee = state
            .get_attestation_committee(attestation.epoch, attestation.committee_index, &self.spec)
            .map_err(Error::BeaconStateError)?;
        if !committee.contains(&(attestation.attester_index as usize)) {
            return Err(eip8453_err(format!(
                "validator {} not on attestation committee {} of epoch {}",
                attestation.attester_index, attestation.committee_index, attestation.epoch
            )));
        }

        let signing_root = attestation.data.signing_root_at_epoch(
            attestation.epoch,
            &state.fork(),
            state.genesis_validators_root(),
            &self.spec,
        );
        self.eip8453_verify_single_signature(attestation.attester_index, signing_root, |pk| {
            attestation
                .signature
                .fast_aggregate_verify(signing_root, &[pk])
        })?;

        self.eip8453_pool.insert_fg_single(attestation);
        Ok(())
    }

    fn eip8453_verify_single_signature(
        &self,
        validator_index: u64,
        _signing_root: Hash256,
        verify: impl FnOnce(&PublicKey) -> bool,
    ) -> Result<(), Error> {
        let pubkey_cache = self.validator_pubkey_cache.read();
        let pubkey = pubkey_cache.get(validator_index as usize).ok_or(
            Error::ValidatorPubkeyCacheIncomplete(validator_index as usize),
        )?;
        if !verify(pubkey) {
            return Err(eip8453_err(format!(
                "invalid signature from validator {validator_index}"
            )));
        }
        Ok(())
    }

    /// Resolve a pubkey for duty responses (exposed for the HTTP API).
    pub fn eip8453_pubkey_bytes(&self, validator_index: u64) -> Option<PublicKeyBytes> {
        self.validator_pubkey_cache
            .read()
            .get_pubkey_bytes(validator_index as usize)
            .copied()
    }

    /// Aggregate the pooled unaggregated FG objects into block-ready aggregates against the
    /// packing `state`, pushing them into the aggregate pools that block production drains.
    ///
    /// Spec: `get_aggregate_attestation` / `get_attestation_aggregation_bits`,
    /// `get_aggregate_head_vote`, `get_aggregate_safe_vote`. The proposer here doubles as the
    /// aggregator (EIP8453-STUB: aggregator duties and gossip aggregates arrive with p2p in
    /// phase 6).
    pub fn eip8453_aggregate_singles_for_packing(
        &self,
        state: &BeaconState<T::EthSpec>,
    ) -> Result<(), Error> {
        // FG attestations: group by (data, epoch) — attestations of different rounds in the
        // same epoch may share an aggregate.
        let singles = self.eip8453_pool.drain_fg_singles(|_| true);
        let mut groups: Vec<(
            (eip8453::AttestationData, types::Epoch),
            Vec<eip8453::SingleAttestation>,
        )> = Vec::new();
        for single in singles {
            let key = (single.data, single.epoch);
            match groups.iter_mut().find(|(k, _)| *k == key) {
                Some((_, group)) => group.push(single),
                None => groups.push((key, vec![single])),
            }
        }
        for ((data, epoch), group) in groups {
            let mut committee_bits =
                BitVector::<<T::EthSpec as EthSpec>::AttestationCommitteesPerRound>::new();
            let mut participants: Vec<u64> = Vec::new();
            let mut signature = AggregateSignature::infinity();
            for single in &group {
                committee_bits
                    .set(single.committee_index as usize, true)
                    .map_err(|e| eip8453_err(format!("committee bit: {e:?}")))?;
                participants.push(single.attester_index);
                signature.add_assign_aggregate(&single.signature);
            }
            // Encode participants in the concatenation of the selected committees, in
            // committee-index order.
            let mut concatenated: Vec<u64> = Vec::new();
            for committee_index in 0..T::EthSpec::attestation_committees_per_round() as u64 {
                if committee_bits
                    .get(committee_index as usize)
                    .unwrap_or(false)
                {
                    let committee = state
                        .get_attestation_committee(epoch, committee_index, &self.spec)
                        .map_err(Error::BeaconStateError)?;
                    concatenated.extend(committee.iter().map(|&v| v as u64));
                }
            }
            let mut aggregation_bits = ProgressiveBitList::with_capacity(concatenated.len());
            for (position, validator_index) in concatenated.iter().enumerate() {
                if participants.contains(validator_index) {
                    aggregation_bits
                        .set(position, true)
                        .map_err(|e| eip8453_err(format!("aggregation bit: {e:?}")))?;
                }
            }
            self.eip8453_pool
                .insert_fg_attestation(eip8453::Attestation {
                    aggregation_bits,
                    data,
                    signature,
                    committee_bits,
                    epoch,
                });
        }

        // Head votes: group by data; support bits per voter from the proposer's view (a counted
        // vote names a block in the accepted tree whose slot is no later than the vote's slot).
        let singles = self.eip8453_pool.drain_head_vote_singles(|_| true);
        let mut vote_groups: Vec<(HeadVoteData, Vec<HeadVoteMessage>)> = Vec::new();
        for single in singles {
            match vote_groups.iter_mut().find(|(k, _)| *k == single.data) {
                Some((_, group)) => group.push(single),
                None => vote_groups.push((single.data, vec![single])),
            }
        }
        if !vote_groups.is_empty() {
            let supported = {
                let fork_choice = self.canonical_head.fork_choice_read_lock();
                let fg = fork_choice
                    .fg()
                    .ok_or_else(|| eip8453_err("fg fork choice not active"))?;
                vote_groups
                    .iter()
                    .map(|(data, _)| {
                        fg.block_slot(&data.beacon_block_root)
                            .is_some_and(|block_slot| block_slot <= data.slot)
                    })
                    .collect::<Vec<_>>()
            };
            for ((data, mut group), is_supported) in vote_groups.into_iter().zip(supported) {
                group.sort_by_key(|message| message.validator_index);
                group.dedup_by_key(|message| message.validator_index);
                let mut signature = AggregateSignature::infinity();
                let mut voter_indices = Vec::with_capacity(group.len());
                for message in &group {
                    signature.add_assign(&message.signature);
                    voter_indices.push(message.validator_index);
                }
                let mut support = ProgressiveBitList::with_capacity(voter_indices.len());
                if is_supported {
                    for position in 0..voter_indices.len() {
                        support
                            .set(position, true)
                            .map_err(|e| eip8453_err(format!("support bit: {e:?}")))?;
                    }
                }
                let vote = eip8453::HeadVote {
                    voter_indices: VariableList::new(voter_indices)
                        .map_err(|e| eip8453_err(format!("voter indices: {e:?}")))?,
                    data,
                    signature,
                };
                self.eip8453_pool.insert_head_vote(vote, support);
            }
        }

        // Safe votes: group by data.
        let singles = self.eip8453_pool.drain_safe_vote_singles(|_| true);
        let mut safe_groups: Vec<(SafeVoteData, Vec<SafeVoteMessage>)> = Vec::new();
        for single in singles {
            match safe_groups.iter_mut().find(|(k, _)| *k == single.data) {
                Some((_, group)) => group.push(single),
                None => safe_groups.push((single.data, vec![single])),
            }
        }
        for (data, mut group) in safe_groups {
            group.sort_by_key(|message| message.validator_index);
            group.dedup_by_key(|message| message.validator_index);
            let mut signature = AggregateSignature::infinity();
            let mut voter_indices = Vec::with_capacity(group.len());
            for message in &group {
                signature.add_assign(&message.signature);
                voter_indices.push(message.validator_index);
            }
            let vote = eip8453::SafeVote {
                voter_indices: ProgressiveVariableList::try_from_iter(voter_indices)
                    .map_err(|e| eip8453_err(format!("voter indices: {e:?}")))?,
                data,
                signature,
            };
            self.eip8453_pool.insert_safe_vote(vote);
        }
        Ok(())
    }
}
