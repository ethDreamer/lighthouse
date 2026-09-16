//! [Heze:EIP8142] Gossip verification of execution payload chunks, their accumulation in the
//! pending payload cache, and reconstruction of the payload envelope from them.
//!
//! This is `validate_execution_payload_chunk_gossip` and
//! `reconstruct_execution_payload_envelope` from
//! `consensus-specs/specs/_features/eip8142/p2p-interface.md`. A chunk is verified on its own,
//! against the chunk commitment in the bid of its block, and forwarded at once. Once
//! `data_chunk_count` verified chunks are held the payload is recovered, re-encoded and checked
//! against the committed root, and the envelope rebuilt from the bid and block enters the same
//! import path a gossiped envelope did before Heze.

use crate::data_column_verification::load_gloas_payload_bid;
use crate::pending_payload_cache::{PayloadReconstructionError, ReconstructPayloadDecision};
use crate::{BeaconChain, BeaconChainError, BeaconChainTypes, metrics};
use bls::Signature;
use slot_clock::SlotClock;
use std::sync::Arc;
use task_executor::RayonPoolType;
use tracing::debug;
use types::{
    BeaconStateError, EthSpec, ExecutionPayloadChunk, Hash256, SignedExecutionPayloadEnvelope, Slot,
};

/// Why a chunk failed gossip verification. The doc comment on each variant gives the gossip
/// verdict the spec assigns to it.
#[derive(Debug)]
pub enum GossipPayloadChunkError {
    /// IGNORE. A verified chunk with this index is already held for the block root.
    ChunkAlreadySeen { block_root: Hash256, index: u64 },
    /// IGNORE. The chunk's slot is ahead of the clock.
    FutureSlot {
        message_slot: Slot,
        latest_permissible_slot: Slot,
    },
    /// IGNORE (and queue). The chunk's block has not been seen.
    BlockRootUnknown { block_root: Hash256 },
    /// IGNORE. The chunk is from before finalization.
    PriorToFinalization {
        chunk_slot: Slot,
        finalized_slot: Slot,
    },
    /// IGNORE. The payload for the chunk's block is already verified, whole or reconstructed.
    PayloadAlreadyAvailable { block_root: Hash256 },
    /// REJECT. The chunk's slot does not match the block's slot.
    SlotMismatch { chunk_slot: Slot, block_slot: Slot },
    /// REJECT. The block's bid carries no chunk commitment (it is a pre-Heze bid).
    BlockHasNoChunkCommitment { block_root: Hash256 },
    /// REJECT. The chunk index is beyond the chunk count the bid commits to.
    IndexOutOfRange { index: u64, chunk_count: u64 },
    /// REJECT. The Merkle proof does not connect the chunk to the committed root.
    InvalidProof { block_root: Hash256, index: u64 },
    /// IGNORE. An internal error.
    BeaconChainError(BeaconChainError),
}

impl From<BeaconChainError> for GossipPayloadChunkError {
    fn from(e: BeaconChainError) -> Self {
        Self::BeaconChainError(e)
    }
}

impl From<BeaconStateError> for GossipPayloadChunkError {
    fn from(e: BeaconStateError) -> Self {
        Self::BeaconChainError(BeaconChainError::BeaconStateError(e))
    }
}

/// A chunk that passed gossip verification against the bid of its block.
#[derive(Debug)]
pub struct GossipVerifiedPayloadChunk<E: EthSpec> {
    chunk: Arc<ExecutionPayloadChunk<E>>,
    /// The slot of the block the chunk belongs to, as recorded in fork choice.
    block_slot: Slot,
}

impl<E: EthSpec> GossipVerifiedPayloadChunk<E> {
    pub fn chunk(&self) -> &Arc<ExecutionPayloadChunk<E>> {
        &self.chunk
    }

    pub fn block_root(&self) -> Hash256 {
        self.chunk.beacon_block_root
    }

    pub fn index(&self) -> u64 {
        self.chunk.index
    }

    pub fn block_slot(&self) -> Slot {
        self.block_slot
    }
}

/// What became of a payload after enough of its chunks were verified.
#[derive(Debug)]
pub enum PayloadChunkOutcome<E: EthSpec> {
    /// Not enough chunks yet, or reconstruction is already under way or has failed. `held` is
    /// the number of verified chunks now held for the block root.
    Pending { reason: &'static str, held: usize },
    /// The payload was recovered and passed the chunk root check. The envelope is rebuilt from
    /// the bid and block and carries no signature, as Heze envelopes do not.
    Reconstructed(Arc<SignedExecutionPayloadEnvelope<E>>),
}

impl<T: BeaconChainTypes> BeaconChain<T> {
    /// `validate_execution_payload_chunk_gossip` from the spec.
    ///
    /// Loads the bid of the chunk's block, which may read the store, so this runs on a blocking
    /// thread.
    pub fn verify_payload_chunk_for_gossip(
        &self,
        chunk: Arc<ExecutionPayloadChunk<T::EthSpec>>,
    ) -> Result<GossipVerifiedPayloadChunk<T::EthSpec>, GossipPayloadChunkError> {
        let _timer = metrics::start_timer(&metrics::PAYLOAD_CHUNK_GOSSIP_VERIFICATION_SECONDS);
        let result = self.verify_payload_chunk_for_gossip_inner(chunk);
        let outcome = match &result {
            Ok(_) => "accept",
            Err(e) => e.metric_label(),
        };
        metrics::inc_counter_vec(
            &metrics::PAYLOAD_CHUNK_GOSSIP_VERIFICATION_TOTAL,
            &[outcome],
        );
        result
    }

    fn verify_payload_chunk_for_gossip_inner(
        &self,
        chunk: Arc<ExecutionPayloadChunk<T::EthSpec>>,
    ) -> Result<GossipVerifiedPayloadChunk<T::EthSpec>, GossipPayloadChunkError> {
        let block_root = chunk.beacon_block_root;
        let index = chunk.index;

        // [IGNORE] This is the first chunk seen for this block root and index.
        if self
            .pending_payload_cache
            .has_payload_chunk(&block_root, index)
        {
            return Err(GossipPayloadChunkError::ChunkAlreadySeen { block_root, index });
        }

        // [IGNORE] The chunk is not from a future slot.
        let latest_permissible_slot = self
            .slot_clock
            .now_with_future_tolerance(self.spec.maximum_gossip_clock_disparity())
            .ok_or(BeaconChainError::UnableToReadSlot)?;
        if chunk.slot > latest_permissible_slot {
            return Err(GossipPayloadChunkError::FutureSlot {
                message_slot: chunk.slot,
                latest_permissible_slot,
            });
        }

        // [IGNORE] The chunk's block root has been seen (MAY be queued until it is), and
        // [REJECT] the block passes validation: blocks in fork choice have.
        let (block_slot, payload_received) = {
            let fork_choice = self.canonical_head.fork_choice_read_lock();
            let Some(proto_block) = fork_choice.get_block(&block_root) else {
                return Err(GossipPayloadChunkError::BlockRootUnknown { block_root });
            };
            (
                proto_block.slot,
                fork_choice.is_payload_received(&block_root),
            )
        };

        // [IGNORE] The chunk is from a slot at or after the latest finalized slot.
        let finalized_slot = self
            .canonical_head
            .cached_head()
            .finalized_checkpoint()
            .epoch
            .start_slot(T::EthSpec::slots_per_epoch());
        if chunk.slot < finalized_slot {
            return Err(GossipPayloadChunkError::PriorToFinalization {
                chunk_slot: chunk.slot,
                finalized_slot,
            });
        }

        // [IGNORE] The payload for the chunk's block has not been verified yet.
        if payload_received
            || self
                .pending_payload_cache
                .get_executed_payload_envelope(&block_root)
                .is_some()
        {
            return Err(GossipPayloadChunkError::PayloadAlreadyAvailable { block_root });
        }

        // [REJECT] The chunk's slot matches the slot of the block.
        if chunk.slot != block_slot {
            return Err(GossipPayloadChunkError::SlotMismatch {
                chunk_slot: chunk.slot,
                block_slot,
            });
        }

        // The bid commits to the chunks. Loading it also creates the pending entry that holds
        // the chunks, if the block's import has not created it already.
        let bid = load_gloas_payload_bid(block_root, self)?
            .ok_or(GossipPayloadChunkError::BlockRootUnknown { block_root })?;
        let bid = bid.message();
        let (Ok(chunks_root), Ok(payload_length)) =
            (bid.payload_chunks_root(), bid.payload_length())
        else {
            return Err(GossipPayloadChunkError::BlockHasNoChunkCommitment { block_root });
        };

        // [REJECT] The chunk's index is within the chunk count committed to by the bid.
        // A bid with a payload length the presets reject cannot have been processed, but be safe.
        let chunk_count = T::EthSpec::payload_chunk_params()
            .chunk_count(payload_length as usize)
            .map_err(|_| GossipPayloadChunkError::BlockHasNoChunkCommitment { block_root })?
            as u64;
        if index >= chunk_count {
            return Err(GossipPayloadChunkError::IndexOutOfRange { index, chunk_count });
        }

        // [REJECT] The chunk's proof is valid against the chunks root committed to by the bid.
        if !chunk.verify_proof(chunks_root) {
            return Err(GossipPayloadChunkError::InvalidProof { block_root, index });
        }

        Ok(GossipVerifiedPayloadChunk { chunk, block_slot })
    }

    /// Runs [`Self::verify_payload_chunk_for_gossip`] on a blocking thread.
    pub async fn verify_payload_chunk_for_gossip_async(
        self: &Arc<Self>,
        chunk: Arc<ExecutionPayloadChunk<T::EthSpec>>,
    ) -> Result<GossipVerifiedPayloadChunk<T::EthSpec>, GossipPayloadChunkError> {
        let chain = self.clone();
        self.task_executor
            .clone()
            .spawn_blocking_handle(
                move || chain.verify_payload_chunk_for_gossip(chunk),
                "gossip_payload_chunk_verification_handle",
            )
            .ok_or(BeaconChainError::RuntimeShutdown)?
            .await
            .map_err(BeaconChainError::TokioJoin)?
    }

    /// Stores a verified chunk and, once enough are held, reconstructs the payload envelope on a
    /// blocking thread. The returned envelope has passed the chunk root check and is ready for
    /// the envelope import path with `EnvelopeSource::Reconstructed`.
    pub async fn process_gossip_verified_payload_chunk(
        self: &Arc<Self>,
        verified: GossipVerifiedPayloadChunk<T::EthSpec>,
    ) -> Result<PayloadChunkOutcome<T::EthSpec>, BeaconChainError> {
        let block_root = verified.block_root();
        let decision = self
            .pending_payload_cache
            .put_gossip_verified_payload_chunk(verified.chunk.clone())
            .map_err(BeaconChainError::AvailabilityCheckError)?;
        let held = self
            .pending_payload_cache
            .payload_chunks_held(&block_root)
            .unwrap_or(0);
        match decision {
            ReconstructPayloadDecision::No(reason) => {
                return Ok(PayloadChunkOutcome::Pending { reason, held });
            }
            ReconstructPayloadDecision::Yes => {}
        }

        let chain = self.clone();
        let result = self
            .task_executor
            .spawn_blocking_with_rayon_async(RayonPoolType::HighPriority, move || {
                chain.reconstruct_payload_envelope(block_root)
            })
            .await
            .map_err(|_| BeaconChainError::RuntimeShutdown)?;

        match result {
            Ok(envelope) => Ok(PayloadChunkOutcome::Reconstructed(envelope)),
            Err(e) => {
                debug!(
                    ?block_root,
                    error = ?e,
                    "Payload reconstruction from chunks did not produce an envelope"
                );
                Ok(PayloadChunkOutcome::Pending {
                    reason: "reconstruction failed",
                    held,
                })
            }
        }
    }

    /// `reconstruct_execution_payload_envelope` from the spec. Blocking.
    pub fn reconstruct_payload_envelope(
        &self,
        block_root: Hash256,
    ) -> Result<Arc<SignedExecutionPayloadEnvelope<T::EthSpec>>, PayloadReconstructionError> {
        let contents = self
            .pending_payload_cache
            .reconstruct_payload_contents(&block_root)?;

        let (builder_index, parent_beacon_block_root) = {
            let bid = self
                .pending_payload_cache
                .get_bid(&block_root)
                .ok_or(PayloadReconstructionError::MissingBid(block_root))?;
            let parent_root = self
                .canonical_head
                .fork_choice_read_lock()
                .get_block(&block_root)
                .and_then(|proto_block| proto_block.parent_root)
                .ok_or(PayloadReconstructionError::MissingBid(block_root))?;
            (bid.message().builder_index(), parent_root)
        };

        let envelope = contents.into_envelope(builder_index, block_root, parent_beacon_block_root);
        Ok(Arc::new(SignedExecutionPayloadEnvelope {
            message: envelope,
            // Heze envelopes are unsigned; the bid's chunk commitment authenticates them.
            signature: Signature::empty(),
        }))
    }
}

/// The gossip verdict for a chunk that failed verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkAcceptance {
    Ignore,
    Reject,
    /// Ignore now, and re-run once the block arrives.
    UnknownBlock,
}

impl GossipPayloadChunkError {
    /// A stable label for metrics, one per verdict and reason.
    pub fn metric_label(&self) -> &'static str {
        match self {
            Self::ChunkAlreadySeen { .. } => "ignore_duplicate",
            Self::FutureSlot { .. } => "ignore_future_slot",
            Self::BlockRootUnknown { .. } => "ignore_unknown_block",
            Self::PriorToFinalization { .. } => "ignore_finalized",
            Self::PayloadAlreadyAvailable { .. } => "ignore_payload_available",
            Self::BeaconChainError(_) => "ignore_error",
            Self::SlotMismatch { .. } => "reject_slot_mismatch",
            Self::BlockHasNoChunkCommitment { .. } => "reject_no_commitment",
            Self::IndexOutOfRange { .. } => "reject_index_out_of_range",
            Self::InvalidProof { .. } => "reject_invalid_proof",
        }
    }

    pub fn acceptance(&self) -> ChunkAcceptance {
        match self {
            Self::ChunkAlreadySeen { .. }
            | Self::FutureSlot { .. }
            | Self::PriorToFinalization { .. }
            | Self::PayloadAlreadyAvailable { .. }
            | Self::BeaconChainError(_) => ChunkAcceptance::Ignore,
            Self::BlockRootUnknown { .. } => ChunkAcceptance::UnknownBlock,
            Self::SlotMismatch { .. }
            | Self::BlockHasNoChunkCommitment { .. }
            | Self::IndexOutOfRange { .. }
            | Self::InvalidProof { .. } => ChunkAcceptance::Reject,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::test_utils::{BeaconChainHarness, EphemeralHarnessType};
    use ssz::Encode;
    use ssz_types::ProgressiveVariableList;
    use state_processing::envelope_processing::{
        EnvelopeProcessingError, verify_payload_chunks_root,
    };
    use types::{
        EthSpec, ExecutionPayloadBidRefMut, ExecutionPayloadContents, ExecutionPayloadEnvelope,
        ExecutionPayloadGloas, ExecutionRequestsGloas, ForkName, Hash256, MinimalEthSpec,
    };

    type E = MinimalEthSpec;

    fn heze_harness() -> BeaconChainHarness<EphemeralHarnessType<E>> {
        let spec = ForkName::Heze.make_genesis_spec(E::default_spec());
        BeaconChainHarness::builder(E::default())
            .spec(spec.into())
            .deterministic_keypairs(8)
            .fresh_ephemeral_store()
            .mock_execution_layer()
            .build()
    }

    fn envelope() -> ExecutionPayloadEnvelope<E> {
        let transactions =
            (0..4u8).map(|i| ProgressiveVariableList::from_iter((0..200u8).map(|j| j ^ i)));
        ExecutionPayloadEnvelope {
            payload: ExecutionPayloadGloas {
                transactions: ProgressiveVariableList::from_iter(transactions),
                ..ExecutionPayloadGloas::default()
            },
            execution_requests: ExecutionRequestsGloas::default(),
            builder_index: 0,
            beacon_block_root: Hash256::ZERO,
            parent_beacon_block_root: Hash256::ZERO,
        }
    }

    /// At Heze an envelope is authenticated by re-encoding its contents and comparing with the
    /// root and length the state's bid committed to.
    #[tokio::test]
    async fn heze_envelope_is_checked_against_the_bid_commitment() {
        let harness = heze_harness();
        let mut state = harness.get_current_state();
        assert_eq!(state.fork_name_unchecked(), ForkName::Heze);

        let envelope = envelope();
        let bytes = ExecutionPayloadContents::from_envelope(&envelope).as_ssz_bytes();
        let encoded = E::payload_chunk_params()
            .encode_payload(&bytes)
            .expect("encode");

        // The genesis bid carries an empty commitment: nothing verifies against it.
        assert!(matches!(
            verify_payload_chunks_root(&state, &envelope),
            Err(EnvelopeProcessingError::PayloadLengthMismatch { .. })
        ));

        let ExecutionPayloadBidRefMut::Heze(bid) =
            state.latest_execution_payload_bid_mut().expect("heze bid")
        else {
            panic!("expected a Heze bid");
        };
        bid.payload_chunks_root = encoded.chunks_root;
        bid.payload_length = bytes.len() as u64;

        verify_payload_chunks_root(&state, &envelope).expect("committed contents verify");

        // Same length, different contents: the root check catches it.
        let mut tampered = envelope.clone();
        tampered.payload.prev_randao = Hash256::repeat_byte(1);
        assert!(matches!(
            verify_payload_chunks_root(&state, &tampered),
            Err(EnvelopeProcessingError::PayloadChunksRootMismatch { .. })
        ));
    }
}
