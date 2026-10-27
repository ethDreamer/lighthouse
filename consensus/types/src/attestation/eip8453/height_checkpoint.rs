use context_deserialize::context_deserialize;
use serde::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use tree_hash_derive::TreeHash;

use crate::{core::Hash256, fork::ForkName};

/// A checkpoint of the finality gadget: a height and the root of its target block.
///
/// This is *not* the Casper FFG `Checkpoint`; heights are not epochs.
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
pub struct HeightCheckpoint {
    #[serde(with = "serde_utils::quoted_u64")]
    pub height: u64,
    pub root: Hash256,
}

impl HeightCheckpoint {
    /// The empty checkpoint stands for an absent finality pair.
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    ssz_and_tree_hash_tests!(HeightCheckpoint);
}
