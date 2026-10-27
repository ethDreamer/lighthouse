use bls::AggregateSignature;
use context_deserialize::context_deserialize;
use educe::Educe;
use serde::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use tree_hash_derive::TreeHash;

use super::AttestationData;
use crate::{core::Epoch, fork::ForkName};

/// A single validator's finality gadget attestation, as sent on gossip
/// [Modified in EIP8453].
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Encode, Decode, TreeHash, Educe)]
#[educe(Hash)]
#[context_deserialize(ForkName)]
pub struct SingleAttestation {
    #[serde(with = "serde_utils::quoted_u64")]
    pub committee_index: u64,
    #[serde(with = "serde_utils::quoted_u64")]
    pub attester_index: u64,
    pub data: AttestationData,
    pub signature: AggregateSignature,
    /// [New in EIP8453] The epoch of the validator's assigned slot. Unsigned.
    pub epoch: Epoch,
}

#[cfg(test)]
mod tests {
    use super::*;

    ssz_and_tree_hash_tests!(SingleAttestation);
}
