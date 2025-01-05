use crate::test_utils::TestRandom;
use crate::*;
use beacon_block_body::KzgCommitments;
use derivative::Derivative;
use serde::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use superstruct::superstruct;
use test_random_derive::TestRandom;
use tree_hash_derive::TreeHash;

// in all likelihood, this will be superstructed so might as well start early eh?
#[superstruct(
    variants(EIP7732, NextFork),
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
            Derivative,
            arbitrary::Arbitrary
        ),
        derivative(PartialEq, Hash(bound = "E: EthSpec")),
        serde(bound = "E: EthSpec", deny_unknown_fields),
        arbitrary(bound = "E: EthSpec")
    ),
    ref_attributes(
        derive(Debug, PartialEq, TreeHash),
        tree_hash(enum_behaviour = "transparent")
    ),
    cast_error(ty = "Error", expr = "BeaconStateError::IncorrectStateVariant"),
    partial_getter_error(ty = "Error", expr = "BeaconStateError::IncorrectStateVariant")
)]
#[derive(
    Debug, Clone, Serialize, Encode, Deserialize, TreeHash, Derivative, arbitrary::Arbitrary,
)]
#[derivative(PartialEq, Hash(bound = "E: EthSpec"))]
#[serde(bound = "E: EthSpec", untagged)]
#[arbitrary(bound = "E: EthSpec")]
#[ssz(enum_behaviour = "transparent")]
#[tree_hash(enum_behaviour = "transparent")]
pub struct ExecutionEnvelope<E: EthSpec> {
    #[superstruct(only(EIP7732), partial_getter(rename = "payload_eip7732"))]
    pub payload: ExecutionPayloadEIP7732<E>,
    #[superstruct(only(NextFork), partial_getter(rename = "payload_next_fork"))]
    pub payload: ExecutionPayloadEIP7732<E>,
    pub execution_requests: ExecutionRequests<E>,
    #[serde(with = "serde_utils::quoted_u64")]
    #[superstruct(getter(copy))]
    pub builder_index: u64,
    #[superstruct(getter(copy))]
    pub beacon_block_root: Hash256,
    pub blob_kzg_commitments: KzgCommitments<E>,
    #[superstruct(getter(copy))]
    pub payload_withheld: bool,
    #[superstruct(getter(copy))]
    pub state_root: Hash256,
}

// This is used for replaying blocks
#[derive(PartialEq)]
pub enum BlockOrEnvelope<E: EthSpec> {
    Block(SignedBeaconBlock<E, BlindedPayload<E>>),
    Envelope(SignedExecutionEnvelope<E>, Slot),
}

impl<E: EthSpec> BlockOrEnvelope<E> {
    pub fn slot(&self) -> Slot {
        match self {
            BlockOrEnvelope::Block(block) => block.slot(),
            BlockOrEnvelope::Envelope(_, slot) => *slot,
        }
    }

    pub fn state_root(&self) -> Hash256 {
        match self {
            BlockOrEnvelope::Block(block) => block.state_root(),
            BlockOrEnvelope::Envelope(envelope, _) => envelope.message().state_root(),
        }
    }
}

impl<'a, E: EthSpec> SignedRoot for ExecutionEnvelopeRef<'a, E> {}

impl<'a, E: EthSpec> ExecutionEnvelopeRef<'a, E> {
    pub fn payload(&self) -> ExecutionPayloadRef<'a, E> {
        match self {
            ExecutionEnvelopeRef::EIP7732(envelope) => {
                ExecutionPayloadRef::EIP7732(&envelope.payload)
            }
            ExecutionEnvelopeRef::NextFork(envelope) => {
                ExecutionPayloadRef::EIP7732(&envelope.payload)
            }
        }
    }
}
