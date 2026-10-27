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
mod head_vote_data;
mod head_vote_message;
mod height_checkpoint;
mod indexed_attestation;
mod pending_finality;
mod single_attestation;

pub use attestation::Attestation;
pub use attestation_data::AttestationData;
pub use attester_slashing::{
    AttesterSlashing, is_conflicting_with_finality_pair, is_double_target_vote,
    is_slashable_attestation_data,
};
pub use head_vote::HeadVote;
pub use head_vote_data::HeadVoteData;
pub use head_vote_message::HeadVoteMessage;
pub use height_checkpoint::HeightCheckpoint;
pub use indexed_attestation::IndexedAttestation;
pub use pending_finality::PendingFinality;
pub use single_attestation::SingleAttestation;
