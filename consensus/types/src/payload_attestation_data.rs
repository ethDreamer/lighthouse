use crate::test_utils::TestRandom;
use crate::*;
use serde::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use test_random_derive::TestRandom;
use tree_hash_derive::TreeHash;

#[derive(
    TestRandom, TreeHash, Debug, Clone, PartialEq, Eq, Encode, Decode, Serialize, Deserialize, Hash,
)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[context_deserialize(ForkName)]
pub struct PayloadAttestationData {
    pub beacon_block_root: Hash256,
    pub slot: Slot,
    pub payload_present: bool,
}
// todo(eip-7732): Mark's implementation  has PayloadStatus as an enum, but spec calls for a bool. Need to clarify this.

impl SignedRoot for PayloadAttestationData {}

#[cfg(test)]
mod payload_attestation_data_tests {
    use super::*;

    ssz_and_tree_hash_tests!(PayloadAttestationData);
}
