# Test vectors

`spec_vectors.json` was generated from the consensus-specs pyspec at
`specs/_features/eip8142` (branch `eip-8142`, commit `6055f4521`) for the minimal and mainnet
presets. Each case pins the chunk size and counts, the SHA-256 of every chunk, the chunks root
and every chunk's proof for a deterministic payload: `sha256(seed || u64_le(i))` for
`i = 0, 1, ...` concatenated and truncated to `payload_length`, with the seed given in the file.

`reed_solomon_simd_vectors.json` is a copy of
`tests/core/pyspec/eth_consensus_specs/test/helpers/eip8142/reed_solomon_simd/vectors.json` from
the same spec branch: encodings produced directly by the `reed-solomon-simd` crate
(`reed_solomon_simd::encode(original_count, original_count, original)`), in the crate's own shard
representation. The test translates them into the spec's representation before comparing.
