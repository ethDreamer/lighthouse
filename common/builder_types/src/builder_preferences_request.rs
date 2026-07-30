use crate::{BuilderPreferencesV1, SignedRequestAuthV1};
use context_deserialize::context_deserialize;
use serde::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use tree_hash_derive::TreeHash;
use types::ForkName;

#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, Encode, Decode, TreeHash)]
#[context_deserialize(ForkName)]
pub struct BuilderPreferencesRequestV1 {
    preferences: BuilderPreferencesV1,
    auth: SignedRequestAuthV1,
}

impl BuilderPreferencesRequestV1 {
    pub fn new(preferences: BuilderPreferencesV1, auth: SignedRequestAuthV1) -> Self {
        Self { preferences, auth }
    }

    pub fn preferences(&self) -> &BuilderPreferencesV1 {
        &self.preferences
    }

    pub fn auth(&self) -> &SignedRequestAuthV1 {
        &self.auth
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    ssz_and_tree_hash_tests!(BuilderPreferencesRequestV1);
}
