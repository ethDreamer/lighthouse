use crate::{EthSpec, ForkName, Hash256, Slot};
use context_deserialize::context_deserialize;
use educe::Educe;
use payload_chunks::EncodedPayload;
use serde::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use ssz_types::{FixedVector, VariableList};
use tree_hash_derive::TreeHash;
use typenum::Unsigned;

/// One chunk of an execution payload, as gossiped on `execution_payload_chunk`.
///
/// `data` is chunk number `index` of the Reed-Solomon-extended serialized
/// `ExecutionPayloadContents`, and `proof` is the Merkle branch of `sha256(data)` against the
/// `payload_chunks_root` committed to by the bid in the block with root `beacon_block_root`.
///
/// consensus-specs/specs/_features/eip8142/p2p-interface.md#executionpayloadchunk
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode, TreeHash, Educe)]
#[cfg_attr(
    feature = "arbitrary",
    derive(arbitrary::Arbitrary),
    arbitrary(bound = "E: EthSpec")
)]
#[educe(PartialEq, Hash(bound(E: EthSpec)))]
#[serde(bound = "E: EthSpec")]
#[context_deserialize(ForkName)]
pub struct ExecutionPayloadChunk<E: EthSpec> {
    pub beacon_block_root: Hash256,
    pub slot: Slot,
    #[serde(with = "serde_utils::quoted_u64")]
    pub index: u64,
    #[serde(with = "ssz_types::serde_utils::hex_var_list")]
    pub data: VariableList<u8, E::MaxPayloadChunkSize>,
    pub proof: FixedVector<Hash256, E::PayloadChunkProofDepth>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionPayloadChunkError {
    ChunkIndexOutOfRange { index: usize, chunk_count: usize },
    ChunkTooLarge { size: usize, max: usize },
    ProofDepthMismatch { depth: usize, expected: usize },
}

impl<E: EthSpec> ExecutionPayloadChunk<E> {
    /// The chunk at `index` of an encoded payload, for the block with root `beacon_block_root`.
    pub fn from_encoded(
        beacon_block_root: Hash256,
        slot: Slot,
        index: usize,
        encoded: &EncodedPayload,
    ) -> Result<Self, ExecutionPayloadChunkError> {
        let data =
            encoded
                .chunks
                .get(index)
                .ok_or(ExecutionPayloadChunkError::ChunkIndexOutOfRange {
                    index,
                    chunk_count: encoded.chunks.len(),
                })?;
        let proof =
            encoded
                .proofs
                .get(index)
                .ok_or(ExecutionPayloadChunkError::ChunkIndexOutOfRange {
                    index,
                    chunk_count: encoded.proofs.len(),
                })?;
        Ok(Self {
            beacon_block_root,
            slot,
            index: index as u64,
            data: VariableList::new(data.clone()).map_err(|_| {
                ExecutionPayloadChunkError::ChunkTooLarge {
                    size: data.len(),
                    max: E::MaxPayloadChunkSize::to_usize(),
                }
            })?,
            proof: FixedVector::new(proof.clone()).map_err(|_| {
                ExecutionPayloadChunkError::ProofDepthMismatch {
                    depth: proof.len(),
                    expected: E::payload_chunk_proof_depth(),
                }
            })?,
        })
    }

    /// Every chunk of an encoded payload, in index order.
    pub fn all_from_encoded(
        beacon_block_root: Hash256,
        slot: Slot,
        encoded: &EncodedPayload,
    ) -> Result<Vec<Self>, ExecutionPayloadChunkError> {
        (0..encoded.chunks.len())
            .map(|index| Self::from_encoded(beacon_block_root, slot, index, encoded))
            .collect()
    }

    /// `sha256(data)`, the leaf committed to by the chunks root.
    pub fn chunk_hash(&self) -> Hash256 {
        payload_chunks::chunk_hash(&self.data)
    }

    /// `verify_execution_payload_chunk_proof` from the spec.
    pub fn verify_proof(&self, payload_chunks_root: Hash256) -> bool {
        E::payload_chunk_params().verify_payload_chunk_proof(
            &self.data,
            self.index as usize,
            &self.proof,
            payload_chunks_root,
        )
    }

    /// `MAX_EXECUTION_PAYLOAD_CHUNK_SIZE`.
    pub fn max_size() -> usize {
        E::max_execution_payload_chunk_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MainnetEthSpec, MinimalEthSpec};
    use ssz::Encode;

    ssz_and_tree_hash_tests!(ExecutionPayloadChunk<MainnetEthSpec>);

    fn largest<E: EthSpec>() -> ExecutionPayloadChunk<E> {
        ExecutionPayloadChunk {
            beacon_block_root: Hash256::ZERO,
            slot: Slot::new(0),
            index: 0,
            data: VariableList::new(vec![0u8; E::MaxPayloadChunkSize::to_usize()]).unwrap(),
            proof: FixedVector::default(),
        }
    }

    #[test]
    fn max_size_matches_preset() {
        assert_eq!(
            largest::<MainnetEthSpec>().as_ssz_bytes().len(),
            ExecutionPayloadChunk::<MainnetEthSpec>::max_size()
        );
        assert_eq!(
            largest::<MinimalEthSpec>().as_ssz_bytes().len(),
            ExecutionPayloadChunk::<MinimalEthSpec>::max_size()
        );
    }

    #[test]
    fn chunks_from_encoded_verify() {
        let params = MinimalEthSpec::payload_chunk_params();
        assert_eq!(params, payload_chunks::PayloadChunkParams::MINIMAL);
        assert_eq!(
            MainnetEthSpec::payload_chunk_params(),
            payload_chunks::PayloadChunkParams::MAINNET
        );
        let payload: Vec<u8> = (0..1500u32).map(|i| (i * 31 % 251) as u8).collect();
        let encoded = params.encode_payload(&payload).unwrap();
        let chunks = ExecutionPayloadChunk::<MinimalEthSpec>::all_from_encoded(
            Hash256::repeat_byte(1),
            Slot::new(7),
            &encoded,
        )
        .unwrap();
        assert_eq!(chunks.len(), 12);
        for (i, chunk) in chunks.iter().enumerate() {
            assert_eq!(chunk.index as usize, i);
            assert!(chunk.verify_proof(encoded.chunks_root));
            assert!(!chunk.verify_proof(Hash256::repeat_byte(2)));
        }
    }
}
