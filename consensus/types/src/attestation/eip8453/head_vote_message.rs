use bls::Signature;
use context_deserialize::context_deserialize;
use educe::Educe;
use serde::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use tree_hash_derive::TreeHash;

use super::HeadVoteData;
use crate::fork::ForkName;

/// A single validator's head vote, as sent on gossip [New in EIP8453].
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Encode, Decode, TreeHash, Educe)]
#[educe(Hash)]
#[context_deserialize(ForkName)]
pub struct HeadVoteMessage {
    #[serde(with = "serde_utils::quoted_u64")]
    pub validator_index: u64,
    pub data: HeadVoteData,
    pub signature: Signature,
}

#[cfg(test)]
mod tests {
    use super::*;

    ssz_and_tree_hash_tests!(HeadVoteMessage);
}
