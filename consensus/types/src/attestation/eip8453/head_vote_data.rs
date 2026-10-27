use context_deserialize::context_deserialize;
use serde::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use tree_hash_derive::TreeHash;

use crate::{
    core::{Hash256, SignedRoot, Slot},
    fork::ForkName,
};

/// The data of a head vote [New in EIP8453]: the available chain's vote for the head block.
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
}

impl SignedRoot for HeadVoteData {}

#[cfg(test)]
mod tests {
    use super::*;

    ssz_and_tree_hash_tests!(HeadVoteData);
}
