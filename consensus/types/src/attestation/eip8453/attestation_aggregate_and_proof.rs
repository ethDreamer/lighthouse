use bls::Signature;
use context_deserialize::context_deserialize;
use serde::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use tree_hash_derive::TreeHash;

use super::Attestation;
use crate::{core::EthSpec, fork::ForkName};

/// An FG attestation aggregate wrapped with its aggregator's identity and selection proof, as
/// broadcast on the `attestation_aggregate` gossip topic. The spec reuses the inherited
/// `AggregateAndProof` container shape; this is its EIP-8453 instantiation, wrapping the
/// EIP-8453 `Attestation`.
#[derive(TreeHash, Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(bound = "E: EthSpec", deny_unknown_fields)]
#[cfg_attr(feature = "arbitrary", arbitrary(bound = "E: EthSpec"))]
#[context_deserialize(ForkName)]
pub struct AttestationAggregateAndProof<E: EthSpec> {
    #[serde(with = "serde_utils::quoted_u64")]
    pub aggregator_index: u64,
    pub aggregate: Attestation<E>,
    pub selection_proof: Signature,
}

/// A signed [`AttestationAggregateAndProof`]. The signature is made under
/// `DOMAIN_AGGREGATE_AND_PROOF`.
#[derive(TreeHash, Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(bound = "E: EthSpec", deny_unknown_fields)]
#[cfg_attr(feature = "arbitrary", arbitrary(bound = "E: EthSpec"))]
#[context_deserialize(ForkName)]
pub struct SignedAttestationAggregateAndProof<E: EthSpec> {
    pub message: AttestationAggregateAndProof<E>,
    pub signature: Signature,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Spec;

    ssz_and_tree_hash_tests!(SignedAttestationAggregateAndProof<Spec>);
}
