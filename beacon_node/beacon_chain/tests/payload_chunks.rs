//! End-to-end tests of EIP-8142 chunked payloads at Heze: a locally built block commits to the
//! chunks of its payload, and a node holding any `data_chunk_count` of them reconstructs and
//! imports the envelope.

use beacon_chain::payload_chunk_verification::{GossipPayloadChunkError, PayloadChunkOutcome};
use beacon_chain::payload_envelope_verification::EnvelopeSource;
use beacon_chain::test_utils::{BeaconChainHarness, EphemeralHarnessType};
use beacon_chain::{AvailabilityProcessingStatus, NotifyExecutionLayer};
use ssz::Encode;
use std::sync::Arc;
use types::{
    BlockImportSource, EthSpec, ExecutionPayloadChunk, ExecutionPayloadContents, ForkName, Hash256,
    MinimalEthSpec, Slot,
};

type E = MinimalEthSpec;

fn heze_harness() -> BeaconChainHarness<EphemeralHarnessType<E>> {
    let spec = ForkName::Heze.make_genesis_spec(E::default_spec());
    BeaconChainHarness::builder(E::default())
        .spec(spec.into())
        .deterministic_keypairs(64)
        .fresh_ephemeral_store()
        .mock_execution_layer()
        .build()
}

/// A Heze block, its envelope, and the chunks its bid committed to, with the block imported
/// and the envelope not.
struct Produced {
    block_root: Hash256,
    slot: Slot,
    chunks: Vec<Arc<ExecutionPayloadChunk<E>>>,
    data_chunk_count: usize,
}

async fn produce_heze_block(harness: &BeaconChainHarness<EphemeralHarnessType<E>>) -> Produced {
    harness.extend_to_slot(Slot::new(1)).await;
    let state = harness.get_current_state();
    let slot = Slot::new(2);
    harness.advance_slot();
    let (block_contents, opt_envelope, _) = harness.make_block_with_envelope(state, slot).await;
    let block = block_contents.0.clone();
    assert_eq!(block.fork_name_unchecked(), ForkName::Heze);
    let block_root = block.canonical_root();
    harness
        .process_block(slot, block_root, block_contents)
        .await
        .expect("block should import");
    let envelope = opt_envelope.expect("locally built block has an envelope");

    // The bid commits to the chunks of exactly the payload contents the envelope carries.
    let bid = block
        .message()
        .body()
        .signed_execution_payload_bid()
        .expect("heze bid")
        .message();
    let contents_bytes = ExecutionPayloadContents::from_envelope(&envelope.message).as_ssz_bytes();
    let params = E::payload_chunk_params();
    let encoded = params.encode_payload(&contents_bytes).expect("encode");
    assert_eq!(
        bid.payload_length().expect("heze"),
        contents_bytes.len() as u64
    );
    assert_eq!(
        bid.payload_chunks_root().expect("heze"),
        encoded.chunks_root
    );

    // Production cached the same encoding for the reveal.
    let cached = harness
        .chain
        .pending_payload_envelopes
        .read()
        .get_encoded_chunks(block_root)
        .expect("chunks cached at production");
    assert_eq!(cached.chunks_root, encoded.chunks_root);
    assert_eq!(cached.chunks, encoded.chunks);

    let chunks = ExecutionPayloadChunk::<E>::all_from_encoded(block_root, slot, &encoded)
        .expect("chunks")
        .into_iter()
        .map(Arc::new)
        .collect();
    Produced {
        block_root,
        slot,
        chunks,
        data_chunk_count: params
            .data_chunk_count(contents_bytes.len())
            .expect("count"),
    }
}

#[tokio::test]
async fn heze_payload_is_reconstructed_from_any_data_chunk_count_of_chunks() {
    let harness = heze_harness();
    let Produced {
        block_root,
        chunks,
        data_chunk_count: k,
        ..
    } = produce_heze_block(&harness).await;
    let n = chunks.len();
    assert_eq!(n, 2 * k);
    assert!(
        !harness
            .chain
            .canonical_head
            .fork_choice_read_lock()
            .is_payload_received(&block_root)
    );

    // Alternate parity and data chunks, so decoding is exercised, and stop at `k`.
    let subset: Vec<usize> = (0..n).step_by(2).take(k).collect();
    let mut reconstructed = None;
    for (i, index) in subset.iter().enumerate() {
        let verified = harness
            .chain
            .verify_payload_chunk_for_gossip(chunks[*index].clone())
            .expect("chunk verifies against the bid");
        let outcome = harness
            .chain
            .process_gossip_verified_payload_chunk(verified)
            .await
            .expect("chunk processed");
        match outcome {
            PayloadChunkOutcome::Pending { reason, held } => {
                assert!(
                    i + 1 < k,
                    "expected reconstruction at chunk {i}, got {reason}"
                );
                assert_eq!(held, i + 1);
            }
            PayloadChunkOutcome::Reconstructed(envelope) => {
                assert_eq!(i + 1, k, "reconstructed early at chunk {i}");
                reconstructed = Some(envelope);
            }
        }
    }
    let envelope = reconstructed.expect("payload reconstructed");
    assert_eq!(envelope.beacon_block_root(), block_root);

    // The reconstructed envelope goes through the ordinary import path.
    let verified = harness
        .chain
        .verify_envelope_for_gossip(envelope, EnvelopeSource::Reconstructed)
        .await
        .expect("reconstructed envelope verifies");
    let status = harness
        .chain
        .process_execution_payload_envelope(
            block_root,
            verified,
            NotifyExecutionLayer::Yes,
            BlockImportSource::Gossip,
            || Ok(()),
        )
        .await
        .expect("envelope imports");
    assert!(matches!(status, AvailabilityProcessingStatus::Imported(..)));
    assert!(
        harness
            .chain
            .canonical_head
            .fork_choice_read_lock()
            .is_payload_received(&block_root)
    );

    // Late chunks for an available payload are ignored.
    let late = chunks[1].clone();
    assert!(matches!(
        harness.chain.verify_payload_chunk_for_gossip(late),
        Err(GossipPayloadChunkError::PayloadAlreadyAvailable { .. })
    ));
}

#[tokio::test]
async fn chunks_are_rejected_when_tampered_or_misattributed() {
    let harness = heze_harness();
    let Produced {
        block_root,
        slot,
        chunks,
        ..
    } = produce_heze_block(&harness).await;

    // A flipped byte fails the proof.
    let mut tampered = chunks[0].as_ref().clone();
    tampered.data[0] ^= 0x80;
    assert!(matches!(
        harness
            .chain
            .verify_payload_chunk_for_gossip(Arc::new(tampered)),
        Err(GossipPayloadChunkError::InvalidProof { .. })
    ));

    // A proof presented for the wrong index fails.
    let mut wrong_index = chunks[0].as_ref().clone();
    wrong_index.index = 1;
    assert!(matches!(
        harness
            .chain
            .verify_payload_chunk_for_gossip(Arc::new(wrong_index)),
        Err(GossipPayloadChunkError::InvalidProof { .. })
    ));

    // An index beyond the committed count is rejected before the proof is checked.
    let mut out_of_range = chunks[0].as_ref().clone();
    out_of_range.index = chunks.len() as u64;
    assert!(matches!(
        harness
            .chain
            .verify_payload_chunk_for_gossip(Arc::new(out_of_range)),
        Err(GossipPayloadChunkError::IndexOutOfRange { .. })
    ));

    // The wrong slot for the block is rejected.
    let mut wrong_slot = chunks[0].as_ref().clone();
    wrong_slot.slot = slot + 1;
    harness.advance_slot();
    assert!(matches!(
        harness
            .chain
            .verify_payload_chunk_for_gossip(Arc::new(wrong_slot)),
        Err(GossipPayloadChunkError::SlotMismatch { .. })
    ));

    // An unknown block root is deferred, not rejected.
    let mut unknown = chunks[0].as_ref().clone();
    unknown.beacon_block_root = Hash256::repeat_byte(0xab);
    assert!(matches!(
        harness
            .chain
            .verify_payload_chunk_for_gossip(Arc::new(unknown)),
        Err(GossipPayloadChunkError::BlockRootUnknown { .. })
    ));

    // The genuine chunk still verifies, and a second copy is a duplicate.
    let verified = harness
        .chain
        .verify_payload_chunk_for_gossip(chunks[0].clone())
        .expect("genuine chunk verifies");
    harness
        .chain
        .process_gossip_verified_payload_chunk(verified)
        .await
        .expect("stored");
    assert!(matches!(
        harness
            .chain
            .verify_payload_chunk_for_gossip(chunks[0].clone()),
        Err(GossipPayloadChunkError::ChunkAlreadySeen { .. })
    ));
    let _ = block_root;
}

/// The harness's own Heze block flow, which imports envelopes whole through the range-sync
/// style verification, still works: the root check accepts the committed payload.
#[tokio::test]
async fn heze_chain_extends_with_whole_envelope_import() {
    let harness = heze_harness();
    harness.extend_to_slot(Slot::new(4)).await;
    let head = harness.chain.head_snapshot();
    assert_eq!(head.beacon_block.slot(), Slot::new(4));
    assert!(
        harness
            .chain
            .canonical_head
            .fork_choice_read_lock()
            .is_payload_received(&head.beacon_block_root)
    );
}
