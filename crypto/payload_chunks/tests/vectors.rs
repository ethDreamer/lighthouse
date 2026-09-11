//! Known-answer tests.
//!
//! - `data/spec_vectors.json` was produced by the consensus-specs pyspec (`eip8142`, minimal and
//!   mainnet presets) on deterministic payloads, and pins sizing, chunks, root and proofs.
//! - `data/reed_solomon_simd_vectors.json` is the crate-output file the spec tests carry, in the
//!   crate's own representation; it is checked here against the `encode_parity` path so that
//!   the translation in `codec` is proven against raw crate output too.

use ethereum_hashing::hash;
use fixed_bytes::Hash256;
use payload_chunks::field::Translation;
use payload_chunks::{PayloadChunkParams, codec};
use serde::Deserialize;

#[derive(Deserialize)]
struct SpecVectors {
    seed: String,
    cases: Vec<SpecCase>,
}

#[derive(Deserialize)]
struct SpecCase {
    preset: String,
    payload_length: usize,
    chunk_size: usize,
    data_chunk_count: usize,
    chunk_count: usize,
    chunks_root: String,
    chunk_hashes: Vec<String>,
    proofs: Vec<Vec<String>>,
}

fn h256(hex: &str) -> Hash256 {
    Hash256::from_slice(&hex::decode(hex).unwrap())
}

/// `sha256(seed || u64_le(i))` concatenated, truncated to `length`.
fn payload(seed: &[u8], length: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(length + 32);
    let mut i = 0u64;
    while out.len() < length {
        let mut input = seed.to_vec();
        input.extend_from_slice(&i.to_le_bytes());
        out.extend_from_slice(&hash(&input));
        i += 1;
    }
    out.truncate(length);
    out
}

#[test]
fn spec_vectors() {
    let json = include_str!("data/spec_vectors.json");
    let vectors: SpecVectors = serde_json::from_str(json).unwrap();
    assert!(vectors.cases.len() >= 10);
    for case in &vectors.cases {
        let params = match case.preset.as_str() {
            "minimal" => PayloadChunkParams::MINIMAL,
            "mainnet" => PayloadChunkParams::MAINNET,
            other => panic!("unknown preset {other}"),
        };
        let name = format!("{} {}", case.preset, case.payload_length);
        let payload = payload(vectors.seed.as_bytes(), case.payload_length);

        assert_eq!(
            params.chunk_size(case.payload_length).unwrap(),
            case.chunk_size,
            "{name}"
        );
        assert_eq!(
            params.data_chunk_count(case.payload_length).unwrap(),
            case.data_chunk_count,
            "{name}"
        );
        assert_eq!(
            params.chunk_count(case.payload_length).unwrap(),
            case.chunk_count,
            "{name}"
        );

        let encoded = params.encode_payload(&payload).unwrap();
        assert_eq!(encoded.chunks.len(), case.chunk_count, "{name}");
        let expected_hashes: Vec<Hash256> = case.chunk_hashes.iter().map(|h| h256(h)).collect();
        assert_eq!(
            encoded.chunk_hashes, expected_hashes,
            "{name}: chunk hashes"
        );
        assert_eq!(encoded.chunks_root, h256(&case.chunks_root), "{name}: root");
        let expected_proofs: Vec<Vec<Hash256>> = case
            .proofs
            .iter()
            .map(|p| p.iter().map(|h| h256(h)).collect())
            .collect();
        assert_eq!(encoded.proofs, expected_proofs, "{name}: proofs");

        assert!(
            params
                .is_valid_payload_chunks_root(encoded.chunks_root, case.payload_length, &payload)
                .unwrap(),
            "{name}: root check"
        );
        // Recover from the parity chunks alone.
        let parity: Vec<(usize, &[u8])> = (case.data_chunk_count..case.chunk_count)
            .map(|i| (i, encoded.chunks[i].as_slice()))
            .collect();
        assert_eq!(
            params
                .recover_payload_bytes(&parity, case.payload_length)
                .unwrap(),
            payload,
            "{name}: recovery"
        );
    }
}

#[derive(Deserialize)]
struct CrateCase {
    original_count: usize,
    original: Vec<String>,
    recovery: Vec<String>,
}

/// Symbols of a crate shard: per 64-byte block the low bytes then the high bytes, and likewise
/// for the tail.
fn crate_symbols(shard: &[u8]) -> Vec<u16> {
    let mut symbols = Vec::with_capacity(shard.len() / 2);
    let full = shard.len() / 64 * 64;
    for block in shard[..full].as_chunks::<64>().0 {
        for i in 0..32 {
            symbols.push(block[i] as u16 | (block[32 + i] as u16) << 8);
        }
    }
    let tail = &shard[full..];
    let half = tail.len() / 2;
    for i in 0..half {
        symbols.push(tail[i] as u16 | (tail[half + i] as u16) << 8);
    }
    symbols
}

/// A crate shard as a spec chunk: Cantor coordinates become polynomial-basis elements, stored as
/// consecutive little-endian symbols.
fn crate_shard_to_chunk(shard: &[u8]) -> Vec<u8> {
    let translation = Translation::get();
    crate_symbols(shard)
        .into_iter()
        .flat_map(|coordinates| translation.to_poly(coordinates).to_le_bytes())
        .collect()
}

#[test]
fn reed_solomon_simd_vectors() {
    let json = include_str!("data/reed_solomon_simd_vectors.json");
    let cases: Vec<CrateCase> = serde_json::from_str(json).unwrap();
    assert!(cases.len() >= 10);
    for case in &cases {
        let data: Vec<Vec<u8>> = case
            .original
            .iter()
            .map(|s| crate_shard_to_chunk(&hex::decode(s).unwrap()))
            .collect();
        let expected: Vec<Vec<u8>> = case
            .recovery
            .iter()
            .map(|s| crate_shard_to_chunk(&hex::decode(s).unwrap()))
            .collect();
        assert_eq!(data.len(), case.original_count);
        let parity = codec::encode_parity(&data, expected.len()).unwrap();
        assert_eq!(parity, expected, "original_count = {}", case.original_count);
    }
}
