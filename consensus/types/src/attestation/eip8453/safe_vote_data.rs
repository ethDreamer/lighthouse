use context_deserialize::context_deserialize;
use serde::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use tree_hash_derive::TreeHash;

use crate::{
    core::{ChainSpec, Domain, EthSpec, Hash256, SignedRoot, consts::gloas::PayloadStatus},
    fork::{Fork, ForkName},
};

/// The data of a safe vote [New in EIP8453]: the fork-choice node (block root and payload
/// status) the validator considers confirmed in `round`.
///
/// Safe votes stabilize the available chain's fork choice across rounds; blocks relay them for
/// view merge and the state transition records only a participation flag.
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    Hash,
    Serialize,
    Deserialize,
    Encode,
    Decode,
    TreeHash,
)]
#[context_deserialize(ForkName)]
pub struct SafeVoteData {
    #[serde(with = "serde_utils::quoted_u64")]
    pub round: u64,
    pub beacon_block_root: Hash256,
    pub payload_status: PayloadStatus,
}

impl SignedRoot for SafeVoteData {}

impl SafeVoteData {
    /// `get_safe_vote_signature`'s signing root: `DOMAIN_SAFE_VOTE` at the epoch of the round's
    /// start slot.
    pub fn compute_signing_root<E: EthSpec>(
        &self,
        fork: &Fork,
        genesis_validators_root: Hash256,
        spec: &ChainSpec,
    ) -> Hash256 {
        let start_slot = super::compute_start_slot_at_round::<E>(self.round);
        let epoch = start_slot.epoch(E::slots_per_epoch());
        let domain = spec.get_domain(epoch, Domain::SafeVote, fork, genesis_validators_root);
        self.signing_root(domain)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    ssz_and_tree_hash_tests!(SafeVoteData);
}
