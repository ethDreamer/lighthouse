use crate::test_utils::TestRandom;
use crate::*;
use derivative::Derivative;
use serde::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use test_random_derive::TestRandom;
use tree_hash_derive::TreeHash;

#[derive(
    TestRandom,
    TreeHash,
    Debug,
    Clone,
    Encode,
    Decode,
    Serialize,
    Deserialize,
    Derivative,
)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(bound = "E: EthSpec", deny_unknown_fields)]
#[cfg_attr(feature = "arbitrary", arbitrary(bound = "E: EthSpec"))]
#[derivative(PartialEq, Hash)]
#[context_deserialize(ForkName)]
pub struct PayloadAttestation<E: EthSpec> {
    pub aggregation_bits: BitList<E::PTCSize>,
    pub data: PayloadAttestationData,
    pub signature: AggregateSignature,
}

#[cfg(test)]
mod payload_attestation_tests {
    use super::*;

    ssz_and_tree_hash_tests!(PayloadAttestation<MinimalEthSpec>);
}
