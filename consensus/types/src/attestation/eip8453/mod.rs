//! EIP-8453 (decoupled consensus) attestation and head vote containers.
//!
//! These types are a complete departure from the pre-EIP-8453 attestation family: the
//! finality-gadget `AttestationData` has no slot, no LMD vote and no source/target epochs, so
//! there is no consumer that could meaningfully handle both the old and new containers. They are
//! therefore deliberately standalone types rather than new variants of the `Attestation`
//! superstruct, keeping the (still churning) wire format isolated in this one module.
//!
//! Where a type name collides with its pre-EIP-8453 counterpart (`Attestation`,
//! `AttestationData`, ...), reference it through the module (`eip8453::Attestation`) rather than
//! importing the bare name, mirroring the spec's own cross-fork notation
//! (e.g. `heze.AttesterSlashing`).
//!
//! Spec: `specs/_features/eip8453/beacon-chain.md` on the `decoupled-consensus` branch.

mod attestation;
mod attestation_data;
mod attester_slashing;
mod head_vote;
mod head_vote_aggregate_and_proof;
mod head_vote_data;
mod head_vote_message;
mod height_checkpoint;
mod indexed_attestation;
mod pending_finality;
mod safe_vote;
mod safe_vote_aggregate_and_proof;
mod safe_vote_data;
mod safe_vote_message;
mod single_attestation;

pub use attestation::Attestation;
pub use attestation_data::AttestationData;
pub use attester_slashing::{
    AttesterSlashing, is_conflicting_with_finality_pair, is_double_target_vote,
    is_slashable_attestation_data,
};
pub use head_vote::HeadVote;
pub use head_vote_aggregate_and_proof::{
    HeadVoteAggregateAndProof, SignedHeadVoteAggregateAndProof,
};
pub use head_vote_data::HeadVoteData;
pub use head_vote_message::HeadVoteMessage;
pub use height_checkpoint::HeightCheckpoint;
pub use indexed_attestation::IndexedAttestation;
pub use pending_finality::PendingFinality;
pub use safe_vote::SafeVote;
pub use safe_vote_aggregate_and_proof::{
    SafeVoteAggregateAndProof, SignedSafeVoteAggregateAndProof,
};
pub use safe_vote_data::SafeVoteData;
pub use safe_vote_message::SafeVoteMessage;
pub use single_attestation::SingleAttestation;

pub use crate::core::consts::eip8453::{GENESIS_ROUND, Round};
use crate::core::{EthSpec, Slot};

/// `compute_safe_vote_sub_round` from the spec: the sub-round (slot offset within the round) in
/// which a validator casts its safe vote. A pure function of the index — it schedules bandwidth
/// across the first `SLOTS_PER_ROUND - 2` slots of the round and carries no other meaning.
pub fn compute_safe_vote_sub_round<E: EthSpec>(validator_index: u64) -> u64 {
    // `slots_per_round` is a preset value of at least 3.
    validator_index
        .checked_rem(E::slots_per_round().saturating_sub(2))
        .unwrap_or(0)
}

/// `compute_round_at_slot` from the spec.
pub fn compute_round_at_slot<E: EthSpec>(slot: Slot) -> Round {
    // `slots_per_round` is a non-zero preset value.
    slot.as_u64()
        .checked_div(E::slots_per_round())
        .unwrap_or(GENESIS_ROUND)
}

/// `compute_start_slot_at_round` from the spec.
pub fn compute_start_slot_at_round<E: EthSpec>(round: Round) -> Slot {
    Slot::new(round.saturating_mul(E::slots_per_round()))
}

/// `compute_attestation_committee_slot` from the spec: the slot of `round` assigned to the
/// committee at `committee_index`. The committees of a round are assigned to its slots in order,
/// with `ATTESTATION_COMMITTEES_PER_ROUND // SLOTS_PER_ROUND` committees per slot.
pub fn compute_attestation_committee_slot<E: EthSpec>(round: Round, committee_index: u64) -> Slot {
    let committees_per_slot = (E::attestation_committees_per_round() as u64)
        .checked_div(E::slots_per_round())
        .unwrap_or(1)
        .max(1);
    let offset = committee_index
        .checked_div(committees_per_slot)
        .unwrap_or(0);
    Slot::new(
        compute_start_slot_at_round::<E>(round)
            .as_u64()
            .saturating_add(offset),
    )
}
