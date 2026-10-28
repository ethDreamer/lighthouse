use context_deserialize::context_deserialize;
use serde::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use tree_hash_derive::TreeHash;

use crate::{
    core::{Hash256, SignedRoot, Slot, consts::gloas::PayloadStatus},
    fork::ForkName,
};

/// The data of a head vote [New in EIP8453]: the available chain's vote for the head block.
///
/// `payload_status` is one of the Gloas fork-choice `PAYLOAD_STATUS_*` values: `PENDING` for a
/// same-slot vote, `EMPTY`/`FULL` for a vote on an earlier block.
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
pub struct HeadVoteData {
    pub slot: Slot,
    pub beacon_block_root: Hash256,
    pub payload_status: PayloadStatus,
}

impl SignedRoot for HeadVoteData {}

#[cfg(test)]
mod tests {
    use super::*;

    ssz_and_tree_hash_tests!(HeadVoteData);
}
