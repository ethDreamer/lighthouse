use crate::test_utils::TestRandom;
use crate::*;
use beacon_block_body::KzgCommitments;
use derivative::Derivative;
use serde::de::{Deserializer, Error as _};
use serde::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use superstruct::superstruct;
use test_random_derive::TestRandom;
use tree_hash_derive::TreeHash;

// in all likelihood, this will be superstructed so might as well start early eh?
#[superstruct(
    variants(Gloas, NextFork),
    variant_attributes(
        derive(
            Debug,
            Clone,
            Serialize,
            Deserialize,
            Encode,
            Decode,
            TreeHash,
            TestRandom,
            Derivative
        ),
        cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary)),
        derivative(PartialEq, Hash(bound = "E: EthSpec")),
        serde(bound = "E: EthSpec", deny_unknown_fields),
        cfg_attr(feature = "arbitrary", arbitrary(bound = "E: EthSpec"))
    ),
    ref_attributes(
        derive(Debug, PartialEq, TreeHash),
        tree_hash(enum_behaviour = "transparent")
    ),
    cast_error(ty = "Error", expr = "BeaconStateError::IncorrectStateVariant"),
    partial_getter_error(ty = "Error", expr = "BeaconStateError::IncorrectStateVariant")
)]
#[derive(Debug, Clone, Serialize, Encode, Deserialize, TreeHash, Derivative)]
#[derivative(PartialEq, Hash(bound = "E: EthSpec"))]
#[serde(bound = "E: EthSpec", untagged)]
#[ssz(enum_behaviour = "transparent")]
#[tree_hash(enum_behaviour = "transparent")]
pub struct ExecutionPayloadEnvelope<E: EthSpec> {
    #[superstruct(only(Gloas), partial_getter(rename = "payload_gloas"))]
    pub payload: ExecutionPayloadGloas<E>,
    #[superstruct(only(NextFork), partial_getter(rename = "payload_next_fork"))]
    pub payload: ExecutionPayloadGloas<E>,
    pub execution_requests: ExecutionRequests<E>,
    #[serde(with = "serde_utils::quoted_u64")]
    #[superstruct(getter(copy))]
    pub builder_index: u64,
    #[superstruct(getter(copy))]
    pub beacon_block_root: Hash256,
    #[superstruct(getter(copy))]
    pub slot: Slot,
    pub blob_kzg_commitments: KzgCommitments<E>,
    #[superstruct(getter(copy))]
    pub state_root: Hash256,
}

impl<'a, E: EthSpec> SignedRoot for ExecutionPayloadEnvelopeRef<'a, E> {}

impl<'a, E: EthSpec> ExecutionPayloadEnvelopeRef<'a, E> {
    pub fn payload(&self) -> ExecutionPayloadRef<'a, E> {
        match self {
            Self::Gloas(envelope) => ExecutionPayloadRef::Gloas(&envelope.payload),
            Self::NextFork(envelope) => ExecutionPayloadRef::Gloas(&envelope.payload),
        }
    }
}

impl<'de, E: EthSpec> ContextDeserialize<'de, ForkName> for ExecutionPayloadEnvelope<E> {
    fn context_deserialize<D>(deserializer: D, context: ForkName) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value: Self = serde::Deserialize::deserialize(deserializer)?;

        match (context, &value) {
            (ForkName::Gloas, Self::Gloas { .. }) => Ok(value),
            _ => Err(D::Error::custom(format!(
                "ExecutionPayloadEnvelope does not support fork {context:?}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MainnetEthSpec;

    mod gloas {
        use super::*;
        ssz_and_tree_hash_tests!(ExecutionPayloadEnvelopeGloas<MainnetEthSpec>);
    }

    mod next_fork {
        use super::*;
        ssz_and_tree_hash_tests!(ExecutionPayloadEnvelopeNextFork<MainnetEthSpec>);
    }
}
