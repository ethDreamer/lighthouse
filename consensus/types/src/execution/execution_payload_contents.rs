use crate::execution::{ExecutionPayloadEnvelope, ExecutionPayloadGloas, ExecutionRequestsGloas};
use crate::{EthSpec, ForkName, Hash256};
use context_deserialize::context_deserialize;
use educe::Educe;
use serde::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use tree_hash_derive::TreeHash;

/// The part of an execution payload envelope that the bid commits to and that is disseminated
/// as chunks: the payload and its execution requests.
///
/// The rest of the envelope (`builder_index`, `beacon_block_root`, `parent_beacon_block_root`)
/// is rebuilt from the bid and the block after reconstruction, see
/// `reconstruct_execution_payload_envelope` in the EIP-8142 networking spec.
///
/// consensus-specs/specs/_features/eip8142/beacon-chain.md#executionpayloadcontents
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode, TreeHash, Educe)]
#[cfg_attr(
    feature = "arbitrary",
    derive(arbitrary::Arbitrary),
    arbitrary(bound = "E: EthSpec")
)]
#[educe(PartialEq, Hash(bound(E: EthSpec)))]
#[serde(bound = "E: EthSpec")]
#[context_deserialize(ForkName)]
pub struct ExecutionPayloadContents<E: EthSpec> {
    pub payload: ExecutionPayloadGloas<E>,
    pub execution_requests: ExecutionRequestsGloas<E>,
}

impl<E: EthSpec> ExecutionPayloadContents<E> {
    /// `get_execution_payload_contents` from the spec.
    pub fn from_envelope(envelope: &ExecutionPayloadEnvelope<E>) -> Self {
        Self {
            payload: envelope.payload.clone(),
            execution_requests: envelope.execution_requests.clone(),
        }
    }

    /// The envelope these contents belong to, given the fields the bid and block supply.
    pub fn into_envelope(
        self,
        builder_index: u64,
        beacon_block_root: Hash256,
        parent_beacon_block_root: Hash256,
    ) -> ExecutionPayloadEnvelope<E> {
        ExecutionPayloadEnvelope {
            payload: self.payload,
            execution_requests: self.execution_requests,
            builder_index,
            beacon_block_root,
            parent_beacon_block_root,
        }
    }
}

impl<E: EthSpec> From<&ExecutionPayloadEnvelope<E>> for ExecutionPayloadContents<E> {
    fn from(envelope: &ExecutionPayloadEnvelope<E>) -> Self {
        Self::from_envelope(envelope)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MainnetEthSpec;

    ssz_and_tree_hash_tests!(ExecutionPayloadContents<MainnetEthSpec>);
}
