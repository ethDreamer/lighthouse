//! Chunked, erasure-coded execution payloads, as specified in
//! `consensus-specs/specs/_features/eip8142/beacon-chain.md`.
//!
//! A serialized payload is split into data chunks, extended with Reed-Solomon parity chunks so
//! that any subset as large as the data recovers it, and committed to by the SSZ root of the list
//! of chunk hashes. This crate provides the sizing helpers, the encoder and decoder (backed by
//! `reed-solomon-simd`, see [`codec`]), the commitment and its proofs (see [`merkle`]), and the
//! re-encode check that makes reconstruction independent of which chunks a node received.
//!
//! The preset-dependent constants are passed in as [`PayloadChunkParams`], so this crate does not
//! depend on `types`.

pub mod codec;
pub mod field;
pub mod merkle;
#[cfg(test)]
mod reference;

use fixed_bytes::Hash256;
use merkle_proof::MerkleTreeError;

pub use field::{FIELD_BASIS, FIELD_MODULUS, SYMBOL_SIZE};
pub use merkle::{chunk_hash, verify_chunk_proof};

/// `PAYLOAD_CHUNK_EXTENSION_FACTOR`: total chunks per data chunk.
pub const EXTENSION_FACTOR: usize = 2;

#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    /// A payload must have at least one byte.
    EmptyPayload,
    /// The payload would need chunks larger than `max_payload_chunk_size`.
    PayloadTooLarge {
        payload_length: usize,
        chunk_size: usize,
        max_chunk_size: usize,
    },
    /// `max_payload_chunks` must be a power of two.
    InvalidMaxChunks(usize),
    /// A chunk size must be a positive multiple of the symbol size.
    InvalidChunkSize(usize),
    /// Chunks of one payload must all have the same size.
    ChunkSizeMismatch,
    /// Both counts must be positive.
    InvalidChunkCounts {
        data_count: usize,
        parity_count: usize,
    },
    /// More chunks than the list limit admits.
    TooManyChunks {
        count: usize,
        max: usize,
    },
    ChunkIndexOutOfRange {
        index: usize,
        chunk_count: usize,
    },
    NotEnoughChunks {
        have: usize,
        need: usize,
    },
    /// The decoder did not restore every data chunk. Not expected to occur.
    ReconstructionIncomplete,
    ReedSolomon(reed_solomon_simd::Error),
    Merkle(MerkleTreeError),
}

impl From<reed_solomon_simd::Error> for Error {
    fn from(e: reed_solomon_simd::Error) -> Self {
        Error::ReedSolomon(e)
    }
}

impl From<MerkleTreeError> for Error {
    fn from(e: MerkleTreeError) -> Self {
        Error::Merkle(e)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for Error {}

/// The preset constants of the chunk code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PayloadChunkParams {
    /// `MAX_PAYLOAD_DATA_CHUNKS`: data chunks per payload once chunks grow.
    pub max_payload_data_chunks: usize,
    /// `MIN_PAYLOAD_CHUNK_SIZE`: chunk size in bytes until the count is hit.
    pub min_payload_chunk_size: usize,
    /// `MAX_PAYLOAD_CHUNK_SIZE`: bound on the chunk size in bytes.
    pub max_payload_chunk_size: usize,
}

impl PayloadChunkParams {
    pub const MAINNET: Self = Self {
        max_payload_data_chunks: 64,
        min_payload_chunk_size: 16_384,
        max_payload_chunk_size: 1_048_576,
    };

    pub const MINIMAL: Self = Self {
        max_payload_data_chunks: 8,
        min_payload_chunk_size: 256,
        max_payload_chunk_size: 4_096,
    };

    /// `MAX_PAYLOAD_CHUNKS`: data and parity chunks per payload.
    pub const fn max_payload_chunks(&self) -> usize {
        self.max_payload_data_chunks * EXTENSION_FACTOR
    }

    /// `PAYLOAD_CHUNK_PROOF_DEPTH`: depth of a chunk hash in the chunks root.
    pub fn proof_depth(&self) -> Result<usize, Error> {
        merkle::proof_depth(self.max_payload_chunks())
    }

    /// `get_payload_chunk_size` from the spec, checked against `max_payload_chunk_size` as bid
    /// processing does.
    pub fn chunk_size(&self, payload_length: usize) -> Result<usize, Error> {
        if payload_length == 0 {
            return Err(Error::EmptyPayload);
        }
        let mut chunk_size = self
            .min_payload_chunk_size
            .max(payload_length.div_ceil(self.max_payload_data_chunks));
        let remainder = chunk_size % SYMBOL_SIZE;
        if remainder != 0 {
            chunk_size += SYMBOL_SIZE - remainder;
        }
        if chunk_size > self.max_payload_chunk_size {
            return Err(Error::PayloadTooLarge {
                payload_length,
                chunk_size,
                max_chunk_size: self.max_payload_chunk_size,
            });
        }
        Ok(chunk_size)
    }

    /// `get_payload_data_chunk_count` from the spec.
    pub fn data_chunk_count(&self, payload_length: usize) -> Result<usize, Error> {
        Ok(payload_length.div_ceil(self.chunk_size(payload_length)?))
    }

    /// `get_payload_chunk_count` from the spec: data and parity chunks.
    pub fn chunk_count(&self, payload_length: usize) -> Result<usize, Error> {
        Ok(self.data_chunk_count(payload_length)? * EXTENSION_FACTOR)
    }

    /// Splits `payload_bytes` into data chunks and extends them with parity chunks
    /// (`compute_payload_chunks` from the spec). Chunk `i` for `i < data_chunk_count` holds the
    /// payload bytes verbatim, the last one zero-padded.
    pub fn compute_payload_chunks(&self, payload_bytes: &[u8]) -> Result<Vec<Vec<u8>>, Error> {
        let chunk_size = self.chunk_size(payload_bytes.len())?;
        let data_count = payload_bytes.len().div_ceil(chunk_size);
        let parity_count = data_count * (EXTENSION_FACTOR - 1);

        let mut chunks: Vec<Vec<u8>> = payload_bytes
            .chunks(chunk_size)
            .map(|c| {
                let mut chunk = c.to_vec();
                chunk.resize(chunk_size, 0);
                chunk
            })
            .collect();
        debug_assert_eq!(chunks.len(), data_count);
        chunks.extend(codec::encode_parity(&chunks, parity_count)?);
        Ok(chunks)
    }

    /// `compute_payload_chunks_root` from the spec.
    pub fn compute_payload_chunks_root<T: AsRef<[u8]>>(
        &self,
        chunks: &[T],
    ) -> Result<Hash256, Error> {
        let hashes: Vec<Hash256> = chunks.iter().map(|c| chunk_hash(c.as_ref())).collect();
        merkle::chunks_root(&hashes, self.max_payload_chunks())
    }

    /// Chunks, their hashes, the chunks root, and a proof per chunk: everything a builder needs
    /// for the bid and for publishing.
    pub fn encode_payload(&self, payload_bytes: &[u8]) -> Result<EncodedPayload, Error> {
        let chunks = self.compute_payload_chunks(payload_bytes)?;
        let chunk_hashes: Vec<Hash256> = chunks.iter().map(|c| chunk_hash(c)).collect();
        let (chunks_root, proofs) =
            merkle::chunks_root_and_proofs(&chunk_hashes, self.max_payload_chunks())?;
        Ok(EncodedPayload {
            chunks,
            chunk_hashes,
            chunks_root,
            proofs,
        })
    }

    /// `recover_payload_bytes` from the spec: recovers the payload from at least
    /// `data_chunk_count` distinct `(index, chunk)` pairs.
    ///
    /// The result is only known to be the payload the bid committed to after
    /// [`is_valid_payload_chunks_root`](Self::is_valid_payload_chunks_root) passes on it.
    pub fn recover_payload_bytes<T: AsRef<[u8]>>(
        &self,
        chunks: &[(usize, T)],
        payload_length: usize,
    ) -> Result<Vec<u8>, Error> {
        let chunk_size = self.chunk_size(payload_length)?;
        let data_count = payload_length.div_ceil(chunk_size);
        let parity_count = data_count * (EXTENSION_FACTOR - 1);
        let data = codec::recover_data(chunks, data_count, parity_count, chunk_size)?;
        let mut payload: Vec<u8> = data.concat();
        payload.truncate(payload_length);
        Ok(payload)
    }

    /// `is_valid_payload_chunks_root` from the spec: re-encodes `payload_bytes` and checks that
    /// every committed chunk matches.
    ///
    /// This is the check that makes reconstruction independent of which chunks were received:
    /// without it a builder could publish chunks that are not one consistent codeword.
    pub fn is_valid_payload_chunks_root(
        &self,
        chunks_root: Hash256,
        payload_length: usize,
        payload_bytes: &[u8],
    ) -> Result<bool, Error> {
        if payload_bytes.len() != payload_length {
            return Ok(false);
        }
        let chunks = self.compute_payload_chunks(payload_bytes)?;
        Ok(self.compute_payload_chunks_root(&chunks)? == chunks_root)
    }

    /// `verify_execution_payload_chunk_proof` from the spec.
    pub fn verify_payload_chunk_proof(
        &self,
        chunk: &[u8],
        index: usize,
        proof: &[Hash256],
        chunks_root: Hash256,
    ) -> bool {
        verify_chunk_proof(
            chunk_hash(chunk),
            index,
            proof,
            self.max_payload_chunks(),
            chunks_root,
        )
    }
}

/// `get_payload_chunk_block_size` from the spec: the number of positions the data chunks are laid
/// out over, the smallest power of two at least both the data and the parity chunk count.
pub fn chunk_block_size(data_chunk_count: usize) -> usize {
    let parity_chunk_count = data_chunk_count * (EXTENSION_FACTOR - 1);
    data_chunk_count.max(parity_chunk_count).next_power_of_two()
}

/// `get_payload_chunk_position` from the spec: data chunks take the positions from the block
/// size upward, parity chunks the positions from zero upward.
pub fn chunk_position(index: usize, data_chunk_count: usize) -> usize {
    if index < data_chunk_count {
        chunk_block_size(data_chunk_count) + index
    } else {
        index - data_chunk_count
    }
}

/// The output of [`PayloadChunkParams::encode_payload`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedPayload {
    pub chunks: Vec<Vec<u8>>,
    pub chunk_hashes: Vec<Hash256>,
    pub chunks_root: Hash256,
    /// `proofs[i]` proves `chunk_hashes[i]` at index `i` against `chunks_root`.
    pub proofs: Vec<Vec<Hash256>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, SeedableRng, rngs::StdRng};

    fn random_payload(rng: &mut StdRng, len: usize) -> Vec<u8> {
        (0..len).map(|_| rng.random()).collect()
    }

    #[test]
    fn sizing_follows_the_spec() {
        let p = PayloadChunkParams::MINIMAL;
        assert_eq!(p.max_payload_chunks(), 16);
        assert_eq!(p.proof_depth().unwrap(), 5);
        assert_eq!(p.chunk_size(1).unwrap(), 256);
        assert_eq!(p.chunk_size(2048).unwrap(), 256);
        assert_eq!(p.data_chunk_count(2048).unwrap(), 8);
        // Beyond 8 chunks of the minimum size the chunks grow, rounded up to whole symbols.
        assert_eq!(p.chunk_size(2049).unwrap(), 258);
        assert_eq!(p.data_chunk_count(2049).unwrap(), 8);
        assert_eq!(p.chunk_size(8 * 4096).unwrap(), 4096);
        assert!(matches!(
            p.chunk_size(8 * 4096 + 1),
            Err(Error::PayloadTooLarge { .. })
        ));
        assert_eq!(p.chunk_size(0), Err(Error::EmptyPayload));

        let m = PayloadChunkParams::MAINNET;
        assert_eq!(m.max_payload_chunks(), 128);
        assert_eq!(m.proof_depth().unwrap(), 8);
        assert_eq!(m.chunk_size(16_384).unwrap(), 16_384);
        assert_eq!(m.chunk_size(16_385).unwrap(), 16_384);
        assert_eq!(m.data_chunk_count(16_385).unwrap(), 2);
        assert_eq!(m.chunk_size(4 << 20).unwrap(), 65_536);
        assert_eq!(m.data_chunk_count(4 << 20).unwrap(), 64);
    }

    #[test]
    fn positions_are_distinct_and_fill_the_block() {
        for data_count in 1..=64 {
            let chunk_count = data_count * EXTENSION_FACTOR;
            let block = chunk_block_size(data_count);
            let positions: std::collections::BTreeSet<usize> = (0..chunk_count)
                .map(|i| chunk_position(i, data_count))
                .collect();
            assert_eq!(positions.len(), chunk_count);
            let zero_positions: std::collections::BTreeSet<usize> =
                (block + data_count..2 * block).collect();
            assert!(positions.is_disjoint(&zero_positions));
            assert!(positions.union(&zero_positions).all(|p| *p < 2 * block));
        }
    }

    #[test]
    fn crate_matches_reference_at_every_data_count() {
        let mut rng = StdRng::seed_from_u64(1);
        let p = PayloadChunkParams::MINIMAL;
        // Every data chunk count from 1 to 8, with the last data chunk partly padded.
        for data_count in 1..=8usize {
            let len = data_count * 256 - 100;
            let payload = random_payload(&mut rng, len);
            let chunks = p.compute_payload_chunks(&payload).unwrap();
            let expected = reference::compute_payload_chunks(&p, &payload);
            assert_eq!(chunks, expected, "data_count = {data_count}");
        }
        // Grown chunks.
        let payload = random_payload(&mut rng, 3000);
        assert_eq!(
            p.compute_payload_chunks(&payload).unwrap(),
            reference::compute_payload_chunks(&p, &payload)
        );
    }

    #[test]
    fn recovers_from_every_subset_shape() {
        let mut rng = StdRng::seed_from_u64(2);
        let p = PayloadChunkParams::MINIMAL;
        for len in [1usize, 700, 2048, 2049, 5000] {
            let payload = random_payload(&mut rng, len);
            let chunks = p.compute_payload_chunks(&payload).unwrap();
            let k = p.data_chunk_count(len).unwrap();
            let n = chunks.len();
            let indexed = |idx: &[usize]| -> Vec<(usize, &[u8])> {
                idx.iter().map(|&i| (i, chunks[i].as_slice())).collect()
            };
            // All data.
            let all_data: Vec<usize> = (0..k).collect();
            assert_eq!(
                p.recover_payload_bytes(&indexed(&all_data), len).unwrap(),
                payload
            );
            // All parity.
            let all_parity: Vec<usize> = (k..n).collect();
            assert_eq!(
                p.recover_payload_bytes(&indexed(&all_parity), len).unwrap(),
                payload
            );
            // Alternating, and a few random subsets, some with extra chunks.
            let alternating: Vec<usize> = (0..n).step_by(2).collect();
            assert_eq!(
                p.recover_payload_bytes(&indexed(&alternating), len)
                    .unwrap(),
                payload
            );
            for _ in 0..5 {
                let mut idx: Vec<usize> = (0..n).collect();
                for i in (1..n).rev() {
                    idx.swap(i, rng.random_range(0..=i));
                }
                let take = rng.random_range(k..=n);
                idx.truncate(take);
                assert_eq!(
                    p.recover_payload_bytes(&indexed(&idx), len).unwrap(),
                    payload
                );
            }
            // Too few.
            let few: Vec<usize> = (0..k - 1).collect();
            assert!(matches!(
                p.recover_payload_bytes(&indexed(&few), len),
                Err(Error::NotEnoughChunks { .. })
            ));
            // Duplicates do not count twice.
            let dup: Vec<usize> = std::iter::repeat_n(0, k).collect();
            if k > 1 {
                assert!(matches!(
                    p.recover_payload_bytes(&indexed(&dup), len),
                    Err(Error::NotEnoughChunks { .. })
                ));
            }
        }
    }

    #[test]
    fn root_check_detects_inconsistent_codewords() {
        let mut rng = StdRng::seed_from_u64(3);
        let p = PayloadChunkParams::MINIMAL;
        let len = 1500;
        let payload = random_payload(&mut rng, len);
        let encoded = p.encode_payload(&payload).unwrap();
        let k = p.data_chunk_count(len).unwrap();
        assert!(
            p.is_valid_payload_chunks_root(encoded.chunks_root, len, &payload)
                .unwrap()
        );
        assert!(
            !p.is_valid_payload_chunks_root(encoded.chunks_root, len + 1, &payload)
                .unwrap()
        );

        // A builder that corrupts one parity chunk and commits to the corrupted set: a node
        // decoding from a subset that includes it recovers different bytes, and the root check
        // rejects them; a node decoding from the data chunks recovers the real payload, and the
        // root check rejects that too, since the committed parity is not what re-encoding gives.
        let mut bad_chunks = encoded.chunks.clone();
        bad_chunks[k][0] ^= 1;
        let bad_root = p.compute_payload_chunks_root(&bad_chunks).unwrap();
        let from_parity: Vec<(usize, &[u8])> =
            (k..2 * k).map(|i| (i, bad_chunks[i].as_slice())).collect();
        let recovered = p.recover_payload_bytes(&from_parity, len).unwrap();
        assert_ne!(recovered, payload);
        assert!(
            !p.is_valid_payload_chunks_root(bad_root, len, &recovered)
                .unwrap()
        );
        assert!(
            !p.is_valid_payload_chunks_root(bad_root, len, &payload)
                .unwrap()
        );
    }

    #[test]
    fn proofs_verify_for_every_chunk() {
        let mut rng = StdRng::seed_from_u64(4);
        let p = PayloadChunkParams::MAINNET;
        let payload = random_payload(&mut rng, 100_000);
        let encoded = p.encode_payload(&payload).unwrap();
        assert_eq!(encoded.chunks.len(), 14);
        for (i, chunk) in encoded.chunks.iter().enumerate() {
            assert!(p.verify_payload_chunk_proof(
                chunk,
                i,
                &encoded.proofs[i],
                encoded.chunks_root
            ));
            let mut tampered = chunk.clone();
            tampered[5] ^= 0x80;
            assert!(!p.verify_payload_chunk_proof(
                &tampered,
                i,
                &encoded.proofs[i],
                encoded.chunks_root
            ));
        }
    }
}
