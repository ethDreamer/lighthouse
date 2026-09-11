//! A direct port of the spec's Lagrange-interpolation definition of the code, used only to check
//! that the `reed-solomon-simd` path computes the same chunks. Quadratic in the chunk count, so
//! only ever run on small inputs.

use crate::field::{SYMBOL_SIZE, chunk_point, gf16_inverse, gf16_multiply};
use crate::{EXTENSION_FACTOR, PayloadChunkParams, chunk_block_size, chunk_position};
use std::collections::BTreeMap;

/// `compute_lagrange_coefficients` from the spec.
fn lagrange_coefficients(points: &[u16], target: u16) -> Vec<u16> {
    points
        .iter()
        .enumerate()
        .map(|(i, &point)| {
            let mut numerator = 1u16;
            let mut denominator = 1u16;
            for (j, &other) in points.iter().enumerate() {
                if i != j {
                    numerator = gf16_multiply(numerator, target ^ other);
                    denominator = gf16_multiply(denominator, point ^ other);
                }
            }
            gf16_multiply(
                numerator,
                gf16_inverse(denominator).expect("distinct points"),
            )
        })
        .collect()
}

/// `compute_payload_chunk` from the spec: the chunk at `index` from exactly `data_chunk_count`
/// known chunks, with the zero chunks of the block among the known evaluations.
pub fn compute_payload_chunk(
    known_chunks: &BTreeMap<usize, Vec<u8>>,
    data_chunk_count: usize,
    index: usize,
) -> Vec<u8> {
    assert_eq!(known_chunks.len(), data_chunk_count);
    let block_size = chunk_block_size(data_chunk_count);

    let mut points: Vec<u16> = known_chunks
        .keys()
        .map(|&i| chunk_point(chunk_position(i, data_chunk_count)))
        .collect();
    points.extend((block_size + data_chunk_count..2 * block_size).map(chunk_point));
    let target = chunk_point(chunk_position(index, data_chunk_count));
    let coefficients = lagrange_coefficients(&points, target);

    let chunk_size = known_chunks.values().next().unwrap().len();
    let mut chunk = Vec::with_capacity(chunk_size);
    for offset in (0..chunk_size).step_by(SYMBOL_SIZE) {
        let mut symbol = 0u16;
        for (i, known) in known_chunks.values().enumerate() {
            let known_symbol = u16::from_le_bytes([known[offset], known[offset + 1]]);
            symbol ^= gf16_multiply(coefficients[i], known_symbol);
        }
        chunk.extend_from_slice(&symbol.to_le_bytes());
    }
    chunk
}

/// `compute_payload_chunks` from the spec.
pub fn compute_payload_chunks(params: &PayloadChunkParams, payload_bytes: &[u8]) -> Vec<Vec<u8>> {
    let chunk_size = params.chunk_size(payload_bytes.len()).unwrap();
    let data_count = payload_bytes.len().div_ceil(chunk_size);
    let chunk_count = data_count * EXTENSION_FACTOR;

    let data_chunks: BTreeMap<usize, Vec<u8>> = payload_bytes
        .chunks(chunk_size)
        .enumerate()
        .map(|(i, c)| {
            let mut chunk = c.to_vec();
            chunk.resize(chunk_size, 0);
            (i, chunk)
        })
        .collect();

    let mut chunks: Vec<Vec<u8>> = data_chunks.values().cloned().collect();
    for index in data_count..chunk_count {
        chunks.push(compute_payload_chunk(&data_chunks, data_count, index));
    }
    chunks
}
