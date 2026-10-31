//! EIP-8453 (decoupled consensus) block operation processing.
//!
//! Spec: `specs/_features/eip8453/beacon-chain.md` (Block processing) on the
//! `decoupled-consensus` branch. The legacy (Casper FFG) attester slashings reuse the existing
//! pre-EIP-8453 processing path, which implements `process_legacy_attester_slashing` exactly.
//!
//! EIP8453-STUB: `BlockSignatureVerifier` (bulk verification) does not know about the new FG
//! attestation, FG slashing and head vote signatures, so they are only checked when processing
//! with `VerifySignatures::True`. Blocks imported via the bulk-verify path skip them.

use std::collections::BTreeSet;

use milhouse::ProgressiveList;
use safe_arith::SafeArith;
use ssz_types::VariableList;
use typenum::Unsigned;
use types::{
    AbstractExecPayload, BeaconBlockBodyEip8453, BeaconBlockBodyRef, BeaconState, BeaconStateError,
    ChainSpec, Domain, EthSpec, SignedRoot, Slot,
    consts::altair::{PROPOSER_WEIGHT, WEIGHT_DENOMINATOR},
    consts::eip8453::{
        FINALITY_FLAG_INDEX, FINALITY_WEIGHT, PROGRESS_FLAG_INDEX, PROGRESS_WEIGHT,
        SAFE_VOTE_FLAG_INDEX, SAFE_VOTE_WEIGHT,
    },
    consts::gloas::{PAYLOAD_STATUS_EMPTY, PAYLOAD_STATUS_FULL, PAYLOAD_STATUS_PENDING},
    eip8453::{self, HeightCheckpoint, PendingFinality, compute_start_slot_at_round},
};

use crate::common::eip8453::{
    add_participation_flag, get_head_vote_seat_reward, get_round_base_reward, has_quorum,
    is_head_vote_same_slot, timeout_delay_slots,
};
use crate::common::{increase_balance, slash_validator};
use crate::per_block_processing::errors::BlockProcessingError;
use crate::per_block_processing::process_operations::{
    process_attester_slashings, process_bls_to_execution_changes, process_exits,
    process_payload_attestations, process_proposer_slashings,
};
use crate::{ConsensusContext, VerifySignatures};

fn invalid(reason: impl Into<String>) -> BlockProcessingError {
    BlockProcessingError::Eip8453Invalid {
        reason: reason.into(),
    }
}

/// `process_operations` from the spec [Modified in EIP8453].
pub fn process_operations<E: EthSpec, Payload: AbstractExecPayload<E>>(
    state: &mut BeaconState<E>,
    block_body: BeaconBlockBodyRef<E, Payload>,
    body: &BeaconBlockBodyEip8453<E, Payload>,
    verify_signatures: VerifySignatures,
    ctxt: &mut ConsensusContext<E>,
    spec: &ChainSpec,
) -> Result<(), BlockProcessingError> {
    // Operation list length checks (progressive lists have no type-level bound).
    let length_checks: [(&'static str, usize, usize); 9] = [
        (
            "proposer_slashings",
            body.proposer_slashings.len(),
            E::MaxProposerSlashings::to_usize(),
        ),
        (
            "attester_slashings",
            body.attester_slashings.len(),
            E::MaxAttesterSlashingsElectra::to_usize(),
        ),
        (
            "legacy_attester_slashings",
            body.legacy_attester_slashings.len(),
            E::MaxAttesterSlashingsElectra::to_usize(),
        ),
        (
            "attestations",
            body.attestations.len(),
            E::MaxAttestationsElectra::to_usize(),
        ),
        (
            "voluntary_exits",
            body.voluntary_exits.len(),
            E::MaxVoluntaryExits::to_usize(),
        ),
        (
            "bls_to_execution_changes",
            body.bls_to_execution_changes.len(),
            E::MaxBlsToExecutionChanges::to_usize(),
        ),
        (
            "payload_attestations",
            body.payload_attestations.len(),
            E::MaxPayloadAttestations::to_usize(),
        ),
        ("head_votes", body.head_votes.len(), E::max_head_votes()),
        ("safe_votes", body.safe_votes.len(), E::max_safe_votes()),
    ];
    for (kind, length, max) in length_checks {
        if length > max {
            return Err(BlockProcessingError::OperationListTooLong { kind, length, max });
        }
    }
    if body.head_vote_support.len() != body.head_votes.len() {
        return Err(invalid("head vote support count does not match head votes"));
    }

    process_proposer_slashings(
        state,
        &block_body.proposer_slashings().to_cow_slice(),
        verify_signatures,
        ctxt,
        spec,
    )?;
    process_attester_slashings_eip8453(state, body, verify_signatures, ctxt, spec)?;
    // Legacy (Casper FFG) attester slashings: for an Eip8453 body, the unified
    // `attester_slashings()` iterator yields the `legacy_attester_slashings` list, and the
    // pre-EIP-8453 verification path is exactly `process_legacy_attester_slashing`.
    process_attester_slashings(
        state,
        block_body.attester_slashings(),
        verify_signatures,
        ctxt,
        spec,
    )?;
    process_attestations_eip8453(state, body, verify_signatures, ctxt, spec)?;
    process_exits(
        state,
        &block_body.voluntary_exits().to_cow_slice(),
        verify_signatures,
        spec,
    )?;
    process_bls_to_execution_changes(
        state,
        &block_body.bls_to_execution_changes()?.to_cow_slice(),
        verify_signatures,
        spec,
    )?;
    process_payload_attestations(
        state,
        block_body.payload_attestations()?.iter(),
        verify_signatures,
        ctxt,
        spec,
    )?;
    process_head_votes(state, body, verify_signatures, ctxt, spec)?;
    process_safe_votes(state, body, verify_signatures, ctxt, spec)?;

    Ok(())
}

/// `is_valid_attestation_data` from the spec.
fn is_valid_attestation_data<E: EthSpec>(
    state: &BeaconState<E>,
    data: &eip8453::AttestationData,
) -> Result<bool, BeaconStateError> {
    let target_checkpoint = state.target_checkpoint()?;
    if data.target == target_checkpoint {
        return Ok(true);
    }
    Ok(state
        .pending_finality()?
        .iter()
        .any(|pending| pending.checkpoint == data.finality)
        && data.finality.height < data.target.height
        && data.target.height < target_checkpoint.height)
}

/// The attesting indices of an aggregate: `get_aggregate_participants` /
/// `get_attesting_indices` from the spec, plus the `is_valid_aggregation_bits` check.
///
/// Returns the (sorted, unique) attesting indices.
pub fn get_attesting_indices_eip8453<E: EthSpec>(
    state: &BeaconState<E>,
    attestation: &eip8453::Attestation<E>,
    spec: &ChainSpec,
) -> Result<BTreeSet<usize>, BlockProcessingError> {
    let mut participants = BTreeSet::new();
    let mut committee_offset: usize = 0;
    for committee_index in attestation
        .committee_bits
        .iter()
        .enumerate()
        .filter(|(_, bit)| *bit)
        .map(|(index, _)| index)
    {
        let committee =
            state.get_attestation_committee(attestation.epoch, committee_index as u64, spec)?;
        for (position, validator_index) in committee.iter().enumerate() {
            let bit_index = committee_offset.safe_add(position)?;
            if attestation
                .aggregation_bits
                .get(bit_index)
                .map_err(|_| invalid("aggregation bit out of bounds"))?
            {
                participants.insert(*validator_index);
            }
        }
        committee_offset.safe_add_assign(committee.len())?;
    }

    // `is_valid_aggregation_bits`: one bit per member of the marked committees, within the
    // aggregate limit.
    if committee_offset > E::max_validators_per_aggregate() {
        return Err(invalid("aggregate exceeds MAX_VALIDATORS_PER_AGGREGATE"));
    }
    if attestation.aggregation_bits.len() != committee_offset {
        return Err(invalid("aggregation bits do not match committee sizes"));
    }

    Ok(participants)
}

/// Verify an aggregate FG signature over `signing_root` by the (sorted, unique) `indices`.
fn verify_fg_aggregate_signature<E: EthSpec>(
    state: &BeaconState<E>,
    indices: impl IntoIterator<Item = usize>,
    signing_root: types::Hash256,
    signature: &bls::AggregateSignature,
) -> Result<(), BlockProcessingError> {
    let mut pubkeys = Vec::new();
    for index in indices {
        let pubkey = state
            .validators()
            .get(index)
            .ok_or(BeaconStateError::UnknownValidator(index))?
            .pubkey
            .decompress()
            .map_err(|_| invalid("invalid pubkey in state"))?;
        pubkeys.push(pubkey);
    }
    let pubkey_refs: Vec<_> = pubkeys.iter().collect();
    if !signature.fast_aggregate_verify(signing_root, &pubkey_refs) {
        return Err(invalid("invalid aggregate signature"));
    }
    Ok(())
}

/// `process_attestation` from the spec [Modified in EIP8453], applied to each attestation.
pub fn process_attestations_eip8453<E: EthSpec, Payload: AbstractExecPayload<E>>(
    state: &mut BeaconState<E>,
    body: &BeaconBlockBodyEip8453<E, Payload>,
    verify_signatures: VerifySignatures,
    ctxt: &mut ConsensusContext<E>,
    spec: &ChainSpec,
) -> Result<(), BlockProcessingError> {
    let proposer_index = ctxt.get_proposer_index(state, spec)? as usize;
    state.build_total_active_balance_cache(spec)?;

    for attestation in body.attestations.iter() {
        process_attestation_eip8453(state, attestation, proposer_index, verify_signatures, spec)?;
    }

    Ok(())
}

/// `process_attestation` from the spec, for one attestation.
pub fn process_attestation_eip8453<E: EthSpec>(
    state: &mut BeaconState<E>,
    attestation: &eip8453::Attestation<E>,
    proposer_index: usize,
    verify_signatures: VerifySignatures,
    spec: &ChainSpec,
) -> Result<(), BlockProcessingError> {
    {
        let data = &attestation.data;

        if !data.is_well_formed() {
            return Err(invalid("attestation data is not well formed"));
        }
        if !is_valid_attestation_data(state, data)? {
            return Err(invalid("attestation data is not valid for the state"));
        }
        if attestation.epoch > state.current_epoch() {
            return Err(invalid("attestation epoch is in the future"));
        }
        let finalized_epoch = state.finalized_slot()?.epoch(E::slots_per_epoch());
        if attestation.epoch < finalized_epoch {
            return Err(invalid("attestation epoch is before finality"));
        }

        let attesting_indices = get_attesting_indices_eip8453(state, attestation, spec)?;

        // `is_valid_indexed_attestation`.
        if attesting_indices.is_empty() {
            return Err(invalid("attestation has no attesting indices"));
        }
        if verify_signatures.is_true() {
            let signing_root = data.signing_root_at_epoch(
                attestation.epoch,
                &state.fork(),
                state.genesis_validators_root(),
                spec,
            );
            verify_fg_aggregate_signature(
                state,
                attesting_indices.iter().copied(),
                signing_root,
                &attestation.signature,
            )?;
        }

        // Apply: flags are credited only the first time a validator's progress or finality
        // participation is recorded, and count toward the round of inclusion.
        let target_checkpoint = state.target_checkpoint()?;
        let num_pending = state.pending_finality()?.len();
        let mut proposer_reward_numerator: u64 = 0;

        for &index in &attesting_indices {
            let base_reward = get_round_base_reward(state, index, spec)?;

            for pending_index in 0..num_pending {
                let (matches_finality, already_counted) = {
                    let pending = state
                        .pending_finality()?
                        .get(pending_index)
                        .ok_or(BeaconStateError::InvalidIndicesCount)?;
                    (
                        pending.checkpoint == data.finality,
                        pending.participation.get(index).copied().unwrap_or(false),
                    )
                };
                if matches_finality && !already_counted {
                    *state
                        .pending_finality_mut()?
                        .get_mut(pending_index)
                        .ok_or(BeaconStateError::InvalidIndicesCount)?
                        .participation
                        .get_mut(index)
                        .ok_or(BeaconStateError::UnknownValidator(index))? = true;
                    add_participation_flag(state, index, FINALITY_FLAG_INDEX)?;
                    proposer_reward_numerator = proposer_reward_numerator
                        .safe_add(base_reward.safe_mul(FINALITY_WEIGHT)?)?;
                }
            }

            if data.target == target_checkpoint {
                let already_progressed = state
                    .progress_participation()?
                    .get(index)
                    .copied()
                    .unwrap_or(false);
                if !already_progressed {
                    *state
                        .progress_participation_mut()?
                        .get_mut(index)
                        .ok_or(BeaconStateError::UnknownValidator(index))? = true;
                    add_participation_flag(state, index, PROGRESS_FLAG_INDEX)?;
                    proposer_reward_numerator = proposer_reward_numerator
                        .safe_add(base_reward.safe_mul(PROGRESS_WEIGHT)?)?;
                }
                if !data.timeout {
                    *state
                        .target_participation_mut()?
                        .get_mut(index)
                        .ok_or(BeaconStateError::UnknownValidator(index))? = true;
                }
            }
        }

        let reward_divisor = WEIGHT_DENOMINATOR
            .safe_sub(PROPOSER_WEIGHT)?
            .safe_mul(WEIGHT_DENOMINATOR)?
            .safe_div(PROPOSER_WEIGHT)?;
        let proposer_reward = proposer_reward_numerator.safe_div(reward_divisor)?;
        increase_balance(state, proposer_index, proposer_reward)?;
    }

    Ok(())
}

/// `process_attester_slashing` from the spec [Modified in EIP8453]: slashings of finality gadget
/// attestations, using the FG slashability predicates.
pub fn process_attester_slashings_eip8453<E: EthSpec, Payload: AbstractExecPayload<E>>(
    state: &mut BeaconState<E>,
    body: &BeaconBlockBodyEip8453<E, Payload>,
    verify_signatures: VerifySignatures,
    ctxt: &mut ConsensusContext<E>,
    spec: &ChainSpec,
) -> Result<(), BlockProcessingError> {
    state.build_slashings_cache()?;

    for slashing in body.attester_slashings.iter() {
        process_attester_slashing_eip8453(state, slashing, verify_signatures, ctxt, spec)?;
    }

    Ok(())
}

/// `process_attester_slashing` from the spec, for one FG attester slashing. Requires the
/// slashings cache to be built.
pub fn process_attester_slashing_eip8453<E: EthSpec>(
    state: &mut BeaconState<E>,
    slashing: &eip8453::AttesterSlashing<E>,
    verify_signatures: VerifySignatures,
    ctxt: &mut ConsensusContext<E>,
    spec: &ChainSpec,
) -> Result<(), BlockProcessingError> {
    let attestation_1 = &slashing.attestation_1;
    let attestation_2 = &slashing.attestation_2;

    if !eip8453::is_slashable_attestation_data(&attestation_1.data, &attestation_2.data) {
        return Err(invalid("attestations are not slashable"));
    }
    for attestation in [attestation_1, attestation_2] {
        verify_fg_indexed_attestation(state, attestation, verify_signatures, spec)?;
    }

    let indices_1: BTreeSet<u64> = attestation_1.attesting_indices.iter().copied().collect();
    let indices_2: BTreeSet<u64> = attestation_2.attesting_indices.iter().copied().collect();
    let mut slashed_any = false;
    let current_epoch = state.current_epoch();
    for index in indices_1.intersection(&indices_2) {
        let index = *index as usize;
        if state.get_validator(index)?.is_slashable_at(current_epoch) {
            slash_validator(state, index, None, ctxt, spec)?;
            slashed_any = true;
        }
    }
    if !slashed_any {
        return Err(invalid("attester slashing slashed no validators"));
    }

    Ok(())
}

/// `is_valid_indexed_attestation` from the spec [Modified in EIP8453], for an FG
/// `IndexedAttestation`.
fn verify_fg_indexed_attestation<E: EthSpec>(
    state: &BeaconState<E>,
    indexed_attestation: &eip8453::IndexedAttestation<E>,
    verify_signatures: VerifySignatures,
    spec: &ChainSpec,
) -> Result<(), BlockProcessingError> {
    let indices = &indexed_attestation.attesting_indices;
    if indices.is_empty() {
        return Err(invalid("indexed attestation has no indices"));
    }
    if indices.len() > E::max_validators_per_aggregate() {
        return Err(invalid("indexed attestation has too many indices"));
    }
    if !indices.is_sorted_by(|a, b| a < b) {
        return Err(invalid("indices are not sorted and unique"));
    }
    if verify_signatures.is_true() {
        let signing_root = indexed_attestation.data.signing_root_at_epoch(
            indexed_attestation.epoch,
            &state.fork(),
            state.genesis_validators_root(),
            spec,
        );
        verify_fg_aggregate_signature(
            state,
            indices.iter().map(|index| *index as usize),
            signing_root,
            &indexed_attestation.signature,
        )?;
    }
    Ok(())
}

/// `process_head_vote` from the spec, applied to each head vote with its support bits.
pub fn process_head_votes<E: EthSpec, Payload: AbstractExecPayload<E>>(
    state: &mut BeaconState<E>,
    body: &BeaconBlockBodyEip8453<E, Payload>,
    verify_signatures: VerifySignatures,
    ctxt: &mut ConsensusContext<E>,
    spec: &ChainSpec,
) -> Result<(), BlockProcessingError> {
    let proposer_index = ctxt.get_proposer_index(state, spec)? as usize;
    state.build_total_active_balance_cache(spec)?;

    for (vote, support_bits) in body.head_votes.iter().zip(body.head_vote_support.iter()) {
        process_head_vote(
            state,
            vote,
            support_bits,
            proposer_index,
            verify_signatures,
            spec,
        )?;
    }

    Ok(())
}

/// `process_head_vote` from the spec, for one head vote with its support bits. Requires the
/// total active balance cache to be built.
pub fn process_head_vote<E: EthSpec>(
    state: &mut BeaconState<E>,
    vote: &eip8453::HeadVote<E>,
    support_bits: &ssz::ProgressiveBitList,
    proposer_index: usize,
    verify_signatures: VerifySignatures,
    spec: &ChainSpec,
) -> Result<(), BlockProcessingError> {
    {
        let data = &vote.data;

        if data.slot.safe_add(spec.min_attestation_inclusion_delay)? > state.slot() {
            return Err(invalid("head vote is too recent for inclusion"));
        }
        let vote_epoch = data.slot.epoch(E::slots_per_epoch());
        if vote_epoch != state.previous_epoch() && vote_epoch != state.current_epoch() {
            return Err(invalid(
                "head vote is not from the previous or current epoch",
            ));
        }
        if ![
            PAYLOAD_STATUS_EMPTY,
            PAYLOAD_STATUS_FULL,
            PAYLOAD_STATUS_PENDING,
        ]
        .contains(&data.payload_status)
        {
            return Err(invalid("head vote has an unknown payload status"));
        }
        let same_slot = is_head_vote_same_slot(state, data, spec)?;
        if same_slot && data.payload_status != PAYLOAD_STATUS_PENDING {
            return Err(invalid("same-slot head vote must have pending payload"));
        }

        let committee = state.get_head_vote_committee(data.slot, spec)?;
        if vote.voter_indices.is_empty() {
            return Err(invalid("head vote has no voters"));
        }
        if !vote.voter_indices.is_sorted_by(|a, b| a < b) {
            return Err(invalid("head vote voters are not sorted and unique"));
        }
        let committee_members: BTreeSet<usize> = committee.iter().copied().collect();
        for index in vote.voter_indices.iter() {
            if !committee_members.contains(&(*index as usize)) {
                return Err(invalid("head vote voter is not in the committee"));
            }
        }

        if support_bits.len() != vote.voter_indices.len() {
            return Err(invalid("head vote support bits do not match voters"));
        }
        if support_bits.iter().any(|is_supported| is_supported)
            && data.slot.safe_add(1u64)? != state.slot()
        {
            return Err(invalid("supported head vote is not from the previous slot"));
        }

        if verify_signatures.is_true() {
            let domain = spec.get_domain(
                vote_epoch,
                Domain::HeadVote,
                &state.fork(),
                state.genesis_validators_root(),
            );
            let signing_root = data.signing_root(domain);
            verify_fg_aggregate_signature(
                state,
                vote.voter_indices.iter().map(|index| *index as usize),
                signing_root,
                &vote.signature,
            )?;
        }

        // Record the seats that voted for the block of their slot, which weighs builder payments
        // and pays each newly recorded seat [Modified in safe-votes-v2].
        if same_slot {
            let participation_index = if vote_epoch == state.current_epoch() {
                (E::slots_per_epoch() as usize).safe_add(
                    data.slot
                        .as_usize()
                        .safe_rem(E::slots_per_epoch() as usize)?,
                )?
            } else {
                data.slot
                    .as_usize()
                    .safe_rem(E::slots_per_epoch() as usize)?
            };
            let voters: BTreeSet<usize> = vote
                .voter_indices
                .iter()
                .map(|index| *index as usize)
                .collect();

            // Collect the newly recorded seats before mutating, then write the bits and pay.
            let new_seats: Vec<(usize, usize)> = {
                let participation = state
                    .head_vote_participation()?
                    .get(participation_index)
                    .ok_or(BeaconStateError::InvalidIndicesCount)?;
                committee
                    .iter()
                    .enumerate()
                    .filter(|(position, member)| {
                        voters.contains(member) && !participation.get(*position).unwrap_or(true)
                    })
                    .map(|(position, member)| (position, *member))
                    .collect()
            };
            {
                let participation = state
                    .head_vote_participation_mut()?
                    .get_mut(participation_index)
                    .ok_or(BeaconStateError::InvalidIndicesCount)?;
                for (position, _) in &new_seats {
                    participation
                        .set(*position, true)
                        .map_err(|_| invalid("head vote participation bit out of bounds"))?;
                }
            }
            let seat_reward = get_head_vote_seat_reward(state, spec)?;
            for (_, member) in &new_seats {
                increase_balance(state, *member, seat_reward)?;
            }
            let proposer_reward = (new_seats.len() as u64)
                .safe_mul(seat_reward)?
                .safe_mul(PROPOSER_WEIGHT)?
                .safe_div(WEIGHT_DENOMINATOR.safe_sub(PROPOSER_WEIGHT)?)?;
            increase_balance(state, proposer_index, proposer_reward)?;
        }
    }

    Ok(())
}

/// `process_safe_vote` from the spec, for one safe vote. Requires the total active balance cache
/// to be built.
pub fn process_safe_vote<E: EthSpec>(
    state: &mut BeaconState<E>,
    vote: &eip8453::SafeVote<E>,
    proposer_index: usize,
    verify_signatures: VerifySignatures,
    spec: &ChainSpec,
) -> Result<(), BlockProcessingError> {
    let current_round = state.current_round();
    {
        let data = &vote.data;

        if data.round > current_round {
            return Err(invalid("safe vote round is in the future"));
        }
        if data.round.safe_add(E::safe_vote_expiry_rounds())? < current_round {
            return Err(invalid("safe vote round has expired"));
        }
        if ![
            PAYLOAD_STATUS_EMPTY,
            PAYLOAD_STATUS_FULL,
            PAYLOAD_STATUS_PENDING,
        ]
        .contains(&data.payload_status)
        {
            return Err(invalid("safe vote has an unknown payload status"));
        }

        let epoch = compute_start_slot_at_round::<E>(data.round).epoch(E::slots_per_epoch());
        if vote.voter_indices.is_empty() {
            return Err(invalid("safe vote has no voters"));
        }
        if !vote.voter_indices.is_sorted_by(|a, b| a < b) {
            return Err(invalid("safe vote voters are not sorted and unique"));
        }
        for index in vote.voter_indices.iter() {
            if !state.get_validator(*index as usize)?.is_active_at(epoch) {
                return Err(invalid("safe vote voter is not active"));
            }
        }

        if verify_signatures.is_true() {
            let domain = spec.get_domain(
                epoch,
                Domain::SafeVote,
                &state.fork(),
                state.genesis_validators_root(),
            );
            let signing_root = data.signing_root(domain);
            verify_fg_aggregate_signature(
                state,
                vote.voter_indices.iter().map(|index| *index as usize),
                signing_root,
                &vote.signature,
            )?;
        }

        // Credit participation for current-round votes only.
        let mut proposer_reward_numerator: u64 = 0;
        if data.round == current_round {
            for index in vote.voter_indices.iter() {
                let index = *index as usize;
                let already_credited = state
                    .round_participation()?
                    .get(index)
                    .ok_or(BeaconStateError::UnknownValidator(index))?
                    .has_flag(SAFE_VOTE_FLAG_INDEX)?;
                if !already_credited {
                    add_participation_flag(state, index, SAFE_VOTE_FLAG_INDEX)?;
                    let base_reward = get_round_base_reward(state, index, spec)?;
                    proposer_reward_numerator = proposer_reward_numerator
                        .safe_add(base_reward.safe_mul(SAFE_VOTE_WEIGHT)?)?;
                }
            }
        }

        let reward_divisor = WEIGHT_DENOMINATOR
            .safe_sub(PROPOSER_WEIGHT)?
            .safe_mul(WEIGHT_DENOMINATOR)?
            .safe_div(PROPOSER_WEIGHT)?;
        let proposer_reward = proposer_reward_numerator.safe_div(reward_divisor)?;
        increase_balance(state, proposer_index, proposer_reward)?;
    }

    Ok(())
}

/// `process_safe_vote` from the spec [New in safe-votes-v2], applied to each safe vote.
///
/// Safe votes are relayed for the fork choice's stabilization rule; the state transition only
/// verifies them and credits a participation flag for current-round votes. Older votes within the
/// expiry window are valid but credit nothing.
pub fn process_safe_votes<E: EthSpec, Payload: AbstractExecPayload<E>>(
    state: &mut BeaconState<E>,
    body: &BeaconBlockBodyEip8453<E, Payload>,
    verify_signatures: VerifySignatures,
    ctxt: &mut ConsensusContext<E>,
    spec: &ChainSpec,
) -> Result<(), BlockProcessingError> {
    let proposer_index = ctxt.get_proposer_index(state, spec)? as usize;
    state.build_total_active_balance_cache(spec)?;

    for vote in body.safe_votes.iter() {
        process_safe_vote(state, vote, proposer_index, verify_signatures, spec)?;
    }

    Ok(())
}

/// `process_height_events` from the spec, run once per block after all operations.
pub fn process_height_events<E: EthSpec>(
    state: &mut BeaconState<E>,
    spec: &ChainSpec,
) -> Result<(), BlockProcessingError> {
    // Finalize the highest pending justification with a quorum.
    let num_pending = state.pending_finality()?.len();
    let mut newly_finalized: Option<(HeightCheckpoint, Slot)> = None;
    for pending_index in 0..num_pending {
        let pending = state
            .pending_finality()?
            .get(pending_index)
            .ok_or(BeaconStateError::InvalidIndicesCount)?;
        if has_quorum(state, spec, |index| {
            pending.participation.get(index).copied().unwrap_or(false)
        })? {
            newly_finalized = Some((pending.checkpoint, pending.slot));
        }
    }
    if let Some((checkpoint, slot)) = newly_finalized {
        *state.finalized_checkpoint_eip8453_mut()? = checkpoint;
        *state.finalized_slot_mut()? = slot;
    }

    // Drop pending justifications at or below the finalized height.
    let finalized_height = state.finalized_checkpoint_eip8453()?.height;
    if state
        .pending_finality()?
        .iter()
        .any(|pending| pending.checkpoint.height <= finalized_height)
    {
        let retained: Vec<_> = state
            .pending_finality()?
            .iter()
            .filter(|pending| pending.checkpoint.height > finalized_height)
            .cloned()
            .collect();
        *state.pending_finality_mut()? =
            VariableList::new(retained).map_err(BeaconStateError::SszTypesError)?;
    }

    let target_quorum = {
        let target_participation = state.target_participation()?;
        has_quorum(state, spec, |index| {
            target_participation.get(index).copied().unwrap_or(false)
        })?
    };
    if target_quorum {
        // [New in safe-votes-v2] a nonjustifiable height advances on a target quorum without
        // justifying, so healing never waits on a justification.
        if !state.nonjustifiable()? {
            let target_checkpoint = state.target_checkpoint()?;
            let target_slot = state.target_slot()?;
            *state.justified_checkpoint_mut()? = target_checkpoint;

            // Keep only the newest pending justifications.
            let mut pending: Vec<_> = state.pending_finality()?.iter().cloned().collect();
            if pending.len() == E::pending_finality_window() {
                pending.remove(0);
            }
            pending.push(PendingFinality {
                checkpoint: target_checkpoint,
                slot: target_slot,
                participation: ssz_types::ProgressiveVariableList::new(vec![
                    false;
                    state
                        .validators()
                        .len()
                ])
                .map_err(BeaconStateError::SszTypesError)?,
            });
            *state.pending_finality_mut()? =
                VariableList::new(pending).map_err(BeaconStateError::SszTypesError)?;
        }

        advance_height(state)?;
    } else {
        let progress_quorum = {
            let progress_participation = state.progress_participation()?;
            has_quorum(state, spec, |index| {
                progress_participation.get(index).copied().unwrap_or(false)
            })?
        };
        // Timeouts count only after the delay.
        if progress_quorum
            && state.slot()
                >= state
                    .target_slot()?
                    .safe_add(timeout_delay_slots::<E>().as_u64())?
        {
            advance_height(state)?;
        }
    }

    Ok(())
}

/// `advance_height` from the spec. The new target root is left empty and filled in by the next
/// `process_slot`, like the state root of `latest_block_header`.
pub fn advance_height<E: EthSpec>(state: &mut BeaconState<E>) -> Result<(), BeaconStateError> {
    let next_height = state
        .target_checkpoint()?
        .height
        .checked_add(1)
        .ok_or(BeaconStateError::InvalidIndicesCount)?;
    *state.target_checkpoint_mut()? = HeightCheckpoint {
        height: next_height,
        root: Default::default(),
    };
    *state.target_slot_mut()? = state.latest_block_header().slot;

    // [New in safe-votes-v2] whether the new height is nonjustifiable, computed on entry into the
    // height and fixed until it changes: under finality debt, every
    // NONJUSTIFIABLE_HEIGHT_PERIOD-th height advances without justifying.
    let finality_debt = next_height.saturating_sub(state.finalized_checkpoint_eip8453()?.height);
    *state.nonjustifiable_mut()? = next_height
        .checked_rem(E::nonjustifiable_height_period())
        .unwrap_or(0)
        == 0
        && finality_debt > E::finality_debt_threshold();

    let validator_count = state.validators().len();
    *state.target_participation_mut()? =
        ProgressiveList::try_from_iter(std::iter::repeat_n(false, validator_count))?;
    *state.progress_participation_mut()? =
        ProgressiveList::try_from_iter(std::iter::repeat_n(false, validator_count))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::per_slot_processing::per_slot_processing;
    use crate::upgrade::gloas::GloasVerificationContext;
    use ssz::BitVector;
    use ssz::ProgressiveBitList;
    use std::sync::Arc;
    use types::{Epoch, ForkName, MinimalEthSpec, Slot, eip8453::AttestationData};

    type E = MinimalEthSpec;

    const VALIDATOR_COUNT: usize = 32;

    /// A Heze-genesis chain forked to EIP-8453 at epoch 1, advanced (with empty slots) to one
    /// round into epoch 1. Exercises `upgrade_to_eip8453`, the target-root back-fill in
    /// `process_slot`, round processing and the naive epoch processing.
    fn eip8453_state() -> (BeaconState<E>, Arc<ChainSpec>) {
        let mut spec = ForkName::Heze.make_genesis_spec(E::default_spec());
        spec.eip8453_fork_epoch = Some(Epoch::new(1));
        let spec = Arc::new(spec);

        let harness = beacon_chain::test_utils::BeaconChainHarness::builder(E::default())
            .spec(spec.clone())
            .deterministic_keypairs(VALIDATOR_COUNT)
            .fresh_ephemeral_store()
            .mock_execution_layer()
            .build();

        let mut state = harness.get_current_state();
        let target_slot = Slot::new(E::slots_per_epoch() + E::slots_per_round());
        while state.slot() < target_slot {
            per_slot_processing(
                &mut state,
                None,
                GloasVerificationContext::FullVerification,
                &spec,
            )
            .expect("per-slot processing across the fork succeeds");
        }
        (state, spec)
    }

    #[test]
    fn upgrade_and_empty_slot_plumbing() {
        let (state, spec) = eip8453_state();

        assert_eq!(state.fork_name_unchecked(), ForkName::Eip8453);

        // The upgrade starts the FG at height 1 targeting the last pre-fork block, and
        // `process_slot` keeps the target root filled.
        let target = state.target_checkpoint().unwrap();
        assert_eq!(target.height, 1);
        assert!(!target.root.is_zero());
        assert_eq!(state.justified_checkpoint().unwrap().height, 0);
        assert_eq!(state.finalized_checkpoint_eip8453().unwrap().height, 0);

        // The head vote committee window serves the current slot.
        let committee = state.get_head_vote_committee(state.slot(), &spec).unwrap();
        assert_eq!(committee.len(), E::head_vote_committee_size());

        // Attestation committees partition the active set in index order.
        let epoch = state.current_epoch();
        let active = state
            .get_active_validator_indices(epoch, &spec)
            .unwrap()
            .len();
        let mut seen = BTreeSet::new();
        let mut total = 0;
        for committee_index in 0..E::attestation_committees_per_round() as u64 {
            let committee = state
                .get_attestation_committee(epoch, committee_index, &spec)
                .unwrap();
            total += committee.len();
            seen.extend(committee);
        }
        assert_eq!(
            total, active,
            "committees cover every active validator once"
        );
        assert_eq!(seen.len(), active, "no validator sits on two committees");

        // One round has been processed with no attestations: every validator gained one round of
        // inactivity score.
        for index in 0..VALIDATOR_COUNT {
            assert_eq!(
                *state.inactivity_scores().unwrap().get(index).unwrap(),
                spec.inactivity_score_bias
            );
        }
    }

    /// A chain started *at* eip8453 (interop genesis with every fork at epoch 0): the genesis
    /// state matches the pyspec `create_genesis_state` shape (height-1 target with an empty root
    /// back-filled by the first `process_slot`, sized participation lists, initialized committee
    /// window) and empty-slot processing works across a round boundary.
    #[test]
    fn eip8453_at_genesis() {
        let spec = Arc::new(ForkName::Eip8453.make_genesis_spec(E::default_spec()));

        let harness = beacon_chain::test_utils::BeaconChainHarness::builder(E::default())
            .spec(spec.clone())
            .deterministic_keypairs(VALIDATOR_COUNT)
            .fresh_ephemeral_store()
            .mock_execution_layer()
            .build();

        let mut state = harness.get_current_state();
        assert_eq!(state.fork_name_unchecked(), ForkName::Eip8453);
        assert_eq!(state.slot(), spec.genesis_slot);

        // The pyspec genesis shape: height 1, empty target root, zeroed height-0
        // justified/finalized, participation sized to the validator set, window filled.
        let target = state.target_checkpoint().unwrap();
        assert_eq!(target.height, 1);
        assert!(target.root.is_zero(), "back-filled by the first slot");
        assert_eq!(state.justified_checkpoint().unwrap().height, 0);
        assert_eq!(state.finalized_checkpoint_eip8453().unwrap().height, 0);
        assert_eq!(state.round_participation().unwrap().len(), VALIDATOR_COUNT);
        let committee = state.get_head_vote_committee(state.slot(), &spec).unwrap();
        assert_eq!(committee.len(), E::head_vote_committee_size());

        // Empty slots across the first round boundary: the target root back-fills with the
        // genesis block root and round processing runs.
        let genesis_block_root = harness.chain.genesis_block_root;
        let target_slot = Slot::new(E::slots_per_round());
        while state.slot() < target_slot {
            per_slot_processing(
                &mut state,
                None,
                GloasVerificationContext::FullVerification,
                &spec,
            )
            .expect("per-slot processing from eip8453 genesis succeeds");
        }
        assert_eq!(
            state.target_checkpoint().unwrap().root,
            genesis_block_root,
            "target root back-fills with the genesis block root"
        );
        for index in 0..VALIDATOR_COUNT {
            assert_eq!(
                *state.inactivity_scores().unwrap().get(index).unwrap(),
                spec.inactivity_score_bias,
                "one idle round of inactivity"
            );
        }
    }

    #[test]
    fn attestation_sets_flags_and_rewards_proposer() {
        let (mut state, spec) = eip8453_state();
        let epoch = state.current_epoch();
        let committee = state.get_attestation_committee(epoch, 0, &spec).unwrap();

        let mut aggregation_bits = ProgressiveBitList::with_capacity(committee.len());
        for i in 0..committee.len() {
            aggregation_bits.set(i, true).unwrap();
        }
        let mut committee_bits = BitVector::new();
        committee_bits.set(0, true).unwrap();

        let attestation = eip8453::Attestation::<E> {
            aggregation_bits,
            data: AttestationData {
                target: state.target_checkpoint().unwrap(),
                timeout: false,
                finality: HeightCheckpoint::empty(),
            },
            signature: bls::AggregateSignature::infinity(),
            committee_bits,
            epoch,
        };

        let proposer_index = 0;
        let balance_before = *state.balances().get(proposer_index).unwrap();
        process_attestation_eip8453(
            &mut state,
            &attestation,
            proposer_index,
            VerifySignatures::False,
            &spec,
        )
        .expect("attestation applies");

        for index in committee {
            assert!(
                state
                    .progress_participation()
                    .unwrap()
                    .get(index)
                    .copied()
                    .unwrap()
            );
            assert!(
                state
                    .target_participation()
                    .unwrap()
                    .get(index)
                    .copied()
                    .unwrap()
            );
            assert!(
                state
                    .round_participation()
                    .unwrap()
                    .get(index)
                    .unwrap()
                    .has_flag(PROGRESS_FLAG_INDEX)
                    .unwrap()
            );
        }
        assert!(
            *state.balances().get(proposer_index).unwrap() > balance_before,
            "proposer is rewarded"
        );

        // Replaying the same attestation credits nothing further.
        let balance_mid = *state.balances().get(proposer_index).unwrap();
        process_attestation_eip8453(
            &mut state,
            &attestation,
            proposer_index,
            VerifySignatures::False,
            &spec,
        )
        .expect("duplicate attestation is a no-op");
        assert_eq!(*state.balances().get(proposer_index).unwrap(), balance_mid);
    }

    #[test]
    fn safe_vote_sets_flag_and_rewards_proposer() {
        let (mut state, spec) = eip8453_state();
        let current_round = state.current_round();
        assert!(current_round > 0, "state is past the genesis round");

        let voted_root = *state.get_block_root(state.slot() - 1).unwrap();
        let make_vote = |round: u64| {
            use types::{BeaconBlockEip8453, EmptyBlock, FullPayload};
            let mut body = BeaconBlockEip8453::<E, FullPayload<E>>::empty(&spec).body;
            body.safe_votes = ssz_types::ProgressiveVariableList::new(vec![eip8453::SafeVote {
                voter_indices: ssz_types::ProgressiveVariableList::new(vec![0, 1, 2]).unwrap(),
                data: eip8453::SafeVoteData {
                    round,
                    beacon_block_root: voted_root,
                    payload_status: PAYLOAD_STATUS_FULL,
                },
                signature: bls::AggregateSignature::infinity(),
            }])
            .unwrap();
            body
        };

        let mut ctxt = ConsensusContext::new(state.slot());
        let proposer_index = ctxt.get_proposer_index(&state, &spec).unwrap() as usize;
        let balance_before = *state.balances().get(proposer_index).unwrap();

        // A current-round vote credits the flag and rewards the proposer.
        process_safe_votes(
            &mut state,
            &make_vote(current_round),
            VerifySignatures::False,
            &mut ctxt,
            &spec,
        )
        .expect("current-round safe vote applies");
        for index in 0..3 {
            assert!(
                state
                    .round_participation()
                    .unwrap()
                    .get(index)
                    .unwrap()
                    .has_flag(SAFE_VOTE_FLAG_INDEX)
                    .unwrap()
            );
        }
        assert!(
            *state.balances().get(proposer_index).unwrap() > balance_before,
            "proposer is rewarded"
        );

        // A previous-round vote within the expiry window is valid but credits nothing further.
        let balance_mid = *state.balances().get(proposer_index).unwrap();
        process_safe_votes(
            &mut state,
            &make_vote(current_round - 1),
            VerifySignatures::False,
            &mut ctxt,
            &spec,
        )
        .expect("stale safe vote within the expiry window is accepted");
        assert_eq!(*state.balances().get(proposer_index).unwrap(), balance_mid);

        // A future-round vote is rejected.
        assert!(
            process_safe_votes(
                &mut state,
                &make_vote(current_round + 1),
                VerifySignatures::False,
                &mut ctxt,
                &spec,
            )
            .is_err()
        );
    }

    #[test]
    fn height_events_justify_then_finalize() {
        let (mut state, spec) = eip8453_state();
        let target = state.target_checkpoint().unwrap();
        let target_slot = state.target_slot().unwrap();

        // A full target quorum justifies the target, queues it as pending finality and advances
        // the height.
        *state.target_participation_mut().unwrap() =
            ProgressiveList::try_from_iter(std::iter::repeat_n(true, VALIDATOR_COUNT)).unwrap();
        process_height_events(&mut state, &spec).expect("height events");

        assert_eq!(state.justified_checkpoint().unwrap(), target);
        assert_eq!(state.pending_finality().unwrap().len(), 1);
        let pending = state.pending_finality().unwrap().first().unwrap().clone();
        assert_eq!(pending.checkpoint, target);
        assert_eq!(pending.slot, target_slot);
        let new_target = state.target_checkpoint().unwrap();
        assert_eq!(new_target.height, 2);
        assert!(new_target.root.is_zero(), "filled by the next process_slot");
        assert!(
            !state
                .target_participation()
                .unwrap()
                .get(0)
                .copied()
                .unwrap(),
            "participation reset"
        );

        // A quorum on the pending justification finalizes it.
        for index in 0..VALIDATOR_COUNT {
            *state
                .pending_finality_mut()
                .unwrap()
                .get_mut(0)
                .unwrap()
                .participation
                .get_mut(index)
                .unwrap() = true;
        }
        process_height_events(&mut state, &spec).expect("height events");

        assert_eq!(state.finalized_checkpoint_eip8453().unwrap(), target);
        assert_eq!(state.finalized_slot().unwrap(), target_slot);
        assert!(
            state.pending_finality().unwrap().is_empty(),
            "finalized entry is dropped"
        );

        // The compat getter synthesizes an epoch-based checkpoint from the FG state.
        assert_eq!(
            state.finalized_checkpoint().epoch,
            target_slot.epoch(E::slots_per_epoch())
        );
        assert_eq!(state.finalized_checkpoint().root, target.root);
    }
}
