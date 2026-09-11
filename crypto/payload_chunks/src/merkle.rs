//! The chunk commitment: `hash_tree_root(List[Bytes32, MAX_PAYLOAD_CHUNKS])` over the SHA-256
//! hashes of the chunks, and Merkle proofs of a chunk hash against it.
//!
//! The root is the SSZ root of a list, so it is the Merkle root of the hashes padded to
//! `MAX_PAYLOAD_CHUNKS` leaves, mixed with the chunk count. A proof therefore has
//! `log2(MAX_PAYLOAD_CHUNKS)` sibling hashes followed by the length mix-in, which is the depth the
//! spec calls `PAYLOAD_CHUNK_PROOF_DEPTH`.

use crate::Error;
use ethereum_hashing::{hash, hash32_concat};
use fixed_bytes::Hash256;
use merkle_proof::{MerkleTree, verify_merkle_proof};

/// SHA-256 of a chunk, the leaf committed to by the chunks root.
pub fn chunk_hash(chunk: &[u8]) -> Hash256 {
    Hash256::from_slice(&hash(chunk))
}

/// The length mix-in node of an SSZ list of `length` items.
fn length_node(length: usize) -> Hash256 {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&(length as u64).to_le_bytes());
    Hash256::from(bytes)
}

fn list_depth(max_chunks: usize) -> Result<usize, Error> {
    if !max_chunks.is_power_of_two() {
        return Err(Error::InvalidMaxChunks(max_chunks));
    }
    Ok(max_chunks.ilog2() as usize)
}

/// Proof depth for a list limit of `max_chunks`: `floorlog2(max_chunks) + 1`.
pub fn proof_depth(max_chunks: usize) -> Result<usize, Error> {
    Ok(list_depth(max_chunks)? + 1)
}

fn tree(chunk_hashes: &[Hash256], max_chunks: usize) -> Result<(MerkleTree, usize), Error> {
    let depth = list_depth(max_chunks)?;
    if chunk_hashes.len() > max_chunks {
        return Err(Error::TooManyChunks {
            count: chunk_hashes.len(),
            max: max_chunks,
        });
    }
    Ok((MerkleTree::create(chunk_hashes, depth), depth))
}

/// `compute_payload_chunks_root` from the spec, given the chunk hashes.
pub fn chunks_root(chunk_hashes: &[Hash256], max_chunks: usize) -> Result<Hash256, Error> {
    let (tree, _) = tree(chunk_hashes, max_chunks)?;
    Ok(Hash256::from(hash32_concat(
        tree.hash().as_slice(),
        length_node(chunk_hashes.len()).as_slice(),
    )))
}

/// The chunks root together with the proof of every chunk hash against it.
pub fn chunks_root_and_proofs(
    chunk_hashes: &[Hash256],
    max_chunks: usize,
) -> Result<(Hash256, Vec<Vec<Hash256>>), Error> {
    let (tree, depth) = tree(chunk_hashes, max_chunks)?;
    let length = length_node(chunk_hashes.len());
    let root = Hash256::from(hash32_concat(tree.hash().as_slice(), length.as_slice()));
    let proofs = (0..chunk_hashes.len())
        .map(|index| {
            let (_, mut branch) = tree.generate_proof(index, depth)?;
            branch.push(length);
            Ok(branch)
        })
        .collect::<Result<Vec<_>, Error>>()?;
    Ok((root, proofs))
}

/// `verify_execution_payload_chunk_proof` from the spec, given the chunk hash.
pub fn verify_chunk_proof(
    chunk_hash: Hash256,
    index: usize,
    proof: &[Hash256],
    max_chunks: usize,
    root: Hash256,
) -> bool {
    let Ok(depth) = proof_depth(max_chunks) else {
        return false;
    };
    if index >= max_chunks {
        return false;
    }
    // `index < max_chunks` keeps the top bit clear, which selects the list's data subtree rather
    // than the length node at the mix-in level.
    verify_merkle_proof(chunk_hash, proof, depth, index, root)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ssz_types::VariableList;
    use tree_hash::TreeHash;
    use typenum::{U16, U128};

    fn hashes(n: usize) -> Vec<Hash256> {
        (0..n).map(|i| chunk_hash(&[i as u8; 3])).collect()
    }

    #[test]
    fn root_matches_ssz_list_root() {
        for n in [1usize, 2, 3, 7, 16] {
            let list = VariableList::<Hash256, U16>::new(hashes(n)).unwrap();
            assert_eq!(chunks_root(&hashes(n), 16).unwrap(), list.tree_hash_root());
        }
        for n in [1usize, 14, 100, 128] {
            let list = VariableList::<Hash256, U128>::new(hashes(n)).unwrap();
            assert_eq!(chunks_root(&hashes(n), 128).unwrap(), list.tree_hash_root());
        }
    }

    #[test]
    fn proofs_verify_and_reject_tampering() {
        let h = hashes(26);
        let (root, proofs) = chunks_root_and_proofs(&h, 128).unwrap();
        assert_eq!(root, chunks_root(&h, 128).unwrap());
        for (i, proof) in proofs.iter().enumerate() {
            assert_eq!(proof.len(), 8);
            assert!(verify_chunk_proof(h[i], i, proof, 128, root));
            assert!(!verify_chunk_proof(h[i], i + 1, proof, 128, root));
            assert!(!verify_chunk_proof(chunk_hash(b"x"), i, proof, 128, root));
            assert!(!verify_chunk_proof(h[i], i, &proof[..7], 128, root));
        }
        // An index beyond the list limit never verifies, whatever the branch.
        assert!(!verify_chunk_proof(h[0], 128, &proofs[0], 128, root));
    }

    #[test]
    fn too_many_chunks_is_an_error() {
        assert!(matches!(
            chunks_root(&hashes(17), 16),
            Err(Error::TooManyChunks { count: 17, max: 16 })
        ));
    }
}
