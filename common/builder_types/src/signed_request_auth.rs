use crate::{RequestAuthData, RequestAuthV1};
use bls::Signature;
use context_deserialize::context_deserialize;
use serde::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use tree_hash_derive::TreeHash;
use types::{ForkName, Slot};

#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, Encode, Decode, TreeHash)]
#[context_deserialize(ForkName)]
pub struct SignedRequestAuthV1 {
    pub message: RequestAuthV1,
    pub signature: Signature,
}

impl SignedRequestAuthV1 {
    /// The "unset" auth: zero-length `data`, slot `0`, and an all-zero signature. On a
    /// `BuilderEntry` this marks an entry that carries no authenticated bid request.
    pub fn unset() -> Self {
        Self {
            message: RequestAuthV1 {
                data: RequestAuthData::default(),
                slot: Slot::new(0),
            },
            signature: Signature::empty(),
        }
    }

    /// Whether this auth is unset (empty `data` and an all-zero signature).
    pub fn is_unset(&self) -> bool {
        self.message.data.is_empty() && self.signature.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    ssz_and_tree_hash_tests!(SignedRequestAuthV1);
}
