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

#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, Encode, Decode, TreeHash)]
#[context_deserialize(ForkName)]
pub struct RequestAuthV1 {
    data: VariableList<u8, MaxDataSize>,
    slot: Slot,
}

#[cfg(test)]
mod tests {
    use super::*;

    ssz_and_tree_hash_tests!(RequestAuthV1);
}
