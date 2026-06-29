use crate::builder::{BuilderPreferencesV1, SignedRequestAuthV1};
use crate::fork::ForkName;
use context_deserialize::context_deserialize;
use serde::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use tree_hash_derive::TreeHash;

#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, Encode, Decode, TreeHash)]
#[context_deserialize(ForkName)]
pub struct BuilderPreferencesRequestV1 {
    preferences: BuilderPreferencesV1,
    auth: SignedRequestAuthV1,
}

#[cfg(test)]
mod tests {
    use super::*;

    ssz_and_tree_hash_tests!(BuilderPreferencesRequestV1);
}
