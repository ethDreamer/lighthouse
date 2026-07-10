use crate::SignedRoot;
use crate::core::Slot;
use crate::fork::ForkName;
use context_deserialize::context_deserialize;
use serde::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use ssz_types::VariableList;
use tree_hash_derive::TreeHash;

// I would like to avoid defining this on the EthSpec if we can get away with it.
// Since it's outside the consensus-spec and is generically named..
pub type MaxDataSize = typenum::U4096;

pub type RequestAuthUrl = VariableList<u8, MaxDataSize>;

#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, Encode, Decode, TreeHash)]
#[context_deserialize(ForkName)]
pub struct RequestAuthV1 {
    pub data: RequestAuthUrl,
    pub slot: Slot,
}

impl SignedRoot for RequestAuthV1 {}

#[cfg(test)]
mod tests {
    use super::*;

    ssz_and_tree_hash_tests!(RequestAuthV1);
}
