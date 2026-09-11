//! The Reed-Solomon code of the spec, computed with `reed-solomon-simd`.
//!
//! The spec defines the code by Lagrange evaluation over GF(2^16): for every symbol position the
//! symbols of the chunks are the values of one polynomial of degree below the block size, data
//! chunks sitting on the coset from the block size upward and parity chunks on the subspace from
//! zero (see `get_payload_chunk_position`). That is exactly the code `reed-solomon-simd`
//! computes with its additive FFT over the Cantor basis, provided it runs in a single FFT block,
//! which the encoder choice below guarantees. Two translations bridge the representations:
//!
//! - Symbols: the spec uses polynomial-basis elements, the crate uses Cantor coordinates
//!   ([`Translation`]).
//! - Layout: the spec stores a symbol as two consecutive little-endian bytes, the crate stores
//!   shards as 64-byte blocks holding the low bytes of 32 symbols followed by their high bytes.
//!
//! Both are per-symbol bijections, so they commute with the code. Chunks are padded to a multiple
//! of 64 bytes for the crate; the added symbols are zero, encode to zero parity symbols, and are
//! truncated away again.

use crate::Error;
use crate::field::{SYMBOL_SIZE, Translation};
use reed_solomon_simd::engine::DefaultEngine;
use reed_solomon_simd::rate::{
    HighRateDecoder, HighRateEncoder, LowRateDecoder, LowRateEncoder, RateDecoder, RateEncoder,
};

const BLOCK_BYTES: usize = 64;
const SYMBOLS_PER_BLOCK: usize = BLOCK_BYTES / SYMBOL_SIZE;

/// Length of the crate shard that carries a chunk of `chunk_size` bytes.
fn shard_len(chunk_size: usize) -> usize {
    chunk_size.div_ceil(BLOCK_BYTES) * BLOCK_BYTES
}

/// Translates a spec chunk into a crate shard of `shard_len(chunk.len())` bytes.
fn chunk_to_shard(chunk: &[u8], translation: &Translation) -> Vec<u8> {
    let mut shard = vec![0u8; shard_len(chunk.len())];
    for (i, symbol) in chunk.as_chunks::<SYMBOL_SIZE>().0.iter().enumerate() {
        let element = u16::from_le_bytes(*symbol);
        let coordinates = translation.to_cantor(element);
        let block = i / SYMBOLS_PER_BLOCK;
        let offset = i % SYMBOLS_PER_BLOCK;
        shard[block * BLOCK_BYTES + offset] = coordinates as u8;
        shard[block * BLOCK_BYTES + SYMBOLS_PER_BLOCK + offset] = (coordinates >> 8) as u8;
    }
    shard
}

/// Translates a crate shard back into a spec chunk of `chunk_size` bytes.
fn shard_to_chunk(shard: &[u8], chunk_size: usize, translation: &Translation) -> Vec<u8> {
    let mut chunk = vec![0u8; chunk_size];
    for (i, symbol) in chunk
        .as_chunks_mut::<SYMBOL_SIZE>()
        .0
        .iter_mut()
        .enumerate()
    {
        let block = i / SYMBOLS_PER_BLOCK;
        let offset = i % SYMBOLS_PER_BLOCK;
        let low = shard[block * BLOCK_BYTES + offset];
        let high = shard[block * BLOCK_BYTES + SYMBOLS_PER_BLOCK + offset];
        let element = translation.to_poly(u16::from_le_bytes([low, high]));
        symbol.copy_from_slice(&element.to_le_bytes());
    }
    chunk
}

fn check_chunk_size(chunk_size: usize) -> Result<(), Error> {
    if chunk_size == 0 || !chunk_size.is_multiple_of(SYMBOL_SIZE) {
        return Err(Error::InvalidChunkSize(chunk_size));
    }
    Ok(())
}

/// Computes the `parity_count` parity chunks of `data_chunks`, which must all have the same even
/// length. Chunk `i` of the result is the chunk at index `data_chunks.len() + i`.
pub fn encode_parity<T: AsRef<[u8]>>(
    data_chunks: &[T],
    parity_count: usize,
) -> Result<Vec<Vec<u8>>, Error> {
    let data_count = data_chunks.len();
    if data_count == 0 || parity_count == 0 {
        return Err(Error::InvalidChunkCounts {
            data_count,
            parity_count,
        });
    }
    let chunk_size = data_chunks[0].as_ref().len();
    check_chunk_size(chunk_size)?;
    if data_chunks.iter().any(|c| c.as_ref().len() != chunk_size) {
        return Err(Error::ChunkSizeMismatch);
    }
    let translation = Translation::get();
    let shard_bytes = shard_len(chunk_size);

    // The spec's block size is the next power of two of the larger of the two counts. The crate
    // encodes in a single FFT block of that size when the rate matches the counts: the low-rate
    // encoder sizes its block by the data count, the high-rate encoder by the parity count.
    let recovery: Vec<Vec<u8>> = if parity_count <= data_count {
        let mut encoder = LowRateEncoder::new(
            data_count,
            parity_count,
            shard_bytes,
            DefaultEngine::new(),
            None,
        )?;
        for chunk in data_chunks {
            encoder.add_original_shard(chunk_to_shard(chunk.as_ref(), translation))?;
        }
        let result = encoder.encode()?;
        result.recovery_iter().map(<[u8]>::to_vec).collect()
    } else {
        let mut encoder = HighRateEncoder::new(
            data_count,
            parity_count,
            shard_bytes,
            DefaultEngine::new(),
            None,
        )?;
        for chunk in data_chunks {
            encoder.add_original_shard(chunk_to_shard(chunk.as_ref(), translation))?;
        }
        let result = encoder.encode()?;
        result.recovery_iter().map(<[u8]>::to_vec).collect()
    };

    Ok(recovery
        .iter()
        .map(|shard| shard_to_chunk(shard, chunk_size, translation))
        .collect())
}

/// Recovers the `data_count` data chunks from `known`, a list of `(index, chunk)` pairs holding
/// at least `data_count` distinct chunks of a code with `data_count` data chunks and
/// `parity_count` parity chunks. Parity chunk `i` has index `data_count + i`.
pub fn recover_data<T: AsRef<[u8]>>(
    known: &[(usize, T)],
    data_count: usize,
    parity_count: usize,
    chunk_size: usize,
) -> Result<Vec<Vec<u8>>, Error> {
    if data_count == 0 || parity_count == 0 {
        return Err(Error::InvalidChunkCounts {
            data_count,
            parity_count,
        });
    }
    check_chunk_size(chunk_size)?;
    let chunk_count = data_count + parity_count;

    // Take the first `data_count` distinct indices in increasing order, as the spec does.
    let mut selected: Vec<(usize, &[u8])> = Vec::with_capacity(data_count);
    let mut sorted: Vec<(usize, &[u8])> = known.iter().map(|(i, c)| (*i, c.as_ref())).collect();
    sorted.sort_by_key(|(i, _)| *i);
    for (index, chunk) in sorted {
        if index >= chunk_count {
            return Err(Error::ChunkIndexOutOfRange { index, chunk_count });
        }
        if chunk.len() != chunk_size {
            return Err(Error::ChunkSizeMismatch);
        }
        if selected.last().is_some_and(|(last, _)| *last == index) {
            continue;
        }
        selected.push((index, chunk));
        if selected.len() == data_count {
            break;
        }
    }
    if selected.len() < data_count {
        return Err(Error::NotEnoughChunks {
            have: selected.len(),
            need: data_count,
        });
    }

    // Fast path: every data chunk is present, nothing to decode.
    if selected.iter().all(|(i, _)| *i < data_count) {
        return Ok(selected.into_iter().map(|(_, c)| c.to_vec()).collect());
    }

    let translation = Translation::get();
    let shard_bytes = shard_len(chunk_size);
    let mut data: Vec<Option<Vec<u8>>> = vec![None; data_count];

    macro_rules! decode_with {
        ($decoder:ident) => {{
            let mut decoder = $decoder::new(
                data_count,
                parity_count,
                shard_bytes,
                DefaultEngine::new(),
                None,
            )?;
            for (index, chunk) in &selected {
                let shard = chunk_to_shard(chunk, translation);
                if *index < data_count {
                    data[*index] = Some(chunk.to_vec());
                    decoder.add_original_shard(*index, shard)?;
                } else {
                    decoder.add_recovery_shard(*index - data_count, shard)?;
                }
            }
            let result = decoder.decode()?;
            for (index, shard) in result.restored_original_iter() {
                data[index] = Some(shard_to_chunk(shard, chunk_size, translation));
            }
        }};
    }
    if parity_count <= data_count {
        decode_with!(LowRateDecoder);
    } else {
        decode_with!(HighRateDecoder);
    }

    data.into_iter()
        .map(|chunk| chunk.ok_or(Error::ReconstructionIncomplete))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_round_trips() {
        let translation = Translation::get();
        for chunk_size in [2usize, 64, 66, 126, 128, 1000, 16384] {
            let chunk: Vec<u8> = (0..chunk_size).map(|i| (i * 7 + 3) as u8).collect();
            let shard = chunk_to_shard(&chunk, translation);
            assert_eq!(shard.len(), shard_len(chunk_size));
            assert_eq!(shard_to_chunk(&shard, chunk_size, translation), chunk);
        }
    }
}
