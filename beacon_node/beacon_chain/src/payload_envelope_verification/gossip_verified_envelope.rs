use std::sync::Arc;

use educe::Educe;
use eth2::types::{EventKind, SseExecutionPayloadGossip};
use store::DatabaseBlock;
use tracing::debug;
use tree_hash::TreeHash;
use types::{EthSpec, ExecutionPayloadBidRef, ExecutionPayloadEnvelope, SignedBeaconBlock, Slot};

use crate::payload_envelope_verification::observed_payload_envelopes::ObservedPayloadEnvelopes;
use crate::{
    BeaconChain, BeaconChainError, BeaconChainTypes, BeaconStore, ServerSentEventHandler,
    canonical_head::CanonicalHead,
    payload_envelope_verification::{EnvelopeError, EnvelopeSource},
};

/// Bundles only the dependencies needed for gossip verification of execution payload envelopes,
/// decoupling `GossipVerifiedEnvelope::new` from the full `BeaconChain`.
pub struct GossipVerificationContext<'a, T: BeaconChainTypes> {
    pub source: EnvelopeSource,
    pub canonical_head: &'a CanonicalHead<T>,
    pub store: &'a BeaconStore<T>,
    pub observed_payload_envelopes: &'a ObservedPayloadEnvelopes,
    pub event_handler: &'a Option<ServerSentEventHandler<T::EthSpec>>,
}

/// Verify that an execution payload envelope is consistent with its beacon block
/// and execution bid.
pub(crate) fn verify_envelope_consistency<E: EthSpec>(
    envelope: &ExecutionPayloadEnvelope<E>,
    block: &SignedBeaconBlock<E>,
    execution_bid: ExecutionPayloadBidRef<E>,
    latest_finalized_slot: Slot,
) -> Result<(), EnvelopeError> {
    // Check that the envelope's slot isn't from a slot prior
    // to the latest finalized slot.
    if envelope.slot() < latest_finalized_slot {
        return Err(EnvelopeError::PriorToFinalization {
            payload_slot: envelope.slot(),
            latest_finalized_slot,
        });
    }

    // Check that the slot of the envelope matches the slot of the block.
    if envelope.slot() != block.slot() {
        return Err(EnvelopeError::SlotMismatch {
            block: block.slot(),
            envelope: envelope.slot(),
        });
    }

    // The envelope's parent beacon block root must match the block's parent root. The bid does
    // not commit to this field directly, so without this check an otherwise valid envelope
    // could be altered and still be accepted here.
    if envelope.parent_beacon_block_root != block.parent_root() {
        return Err(EnvelopeError::ParentBeaconBlockRootMismatch {
            block: block.parent_root(),
            envelope: envelope.parent_beacon_block_root,
        });
    }

    // The block hash should match the block hash of the execution bid.
    if envelope.payload.block_hash != execution_bid.block_hash() {
        return Err(EnvelopeError::BlockHashMismatch {
            committed_bid: execution_bid.block_hash(),
            envelope: envelope.payload.block_hash,
        });
    }

    let requests = &envelope.execution_requests;
    if requests.withdrawals.len() > E::max_withdrawal_requests_per_payload() {
        return Err(EnvelopeError::OperationListTooLong {
            kind: "withdrawal_requests",
            length: requests.withdrawals.len(),
            max: E::max_withdrawal_requests_per_payload(),
        });
    }
    if requests.consolidations.len() > E::max_consolidation_requests_per_payload() {
        return Err(EnvelopeError::OperationListTooLong {
            kind: "consolidation_requests",
            length: requests.consolidations.len(),
            max: E::max_consolidation_requests_per_payload(),
        });
    }
    if requests.builder_deposits.len() > E::max_builder_deposit_requests_per_payload() {
        return Err(EnvelopeError::OperationListTooLong {
            kind: "builder_deposit_requests",
            length: requests.builder_deposits.len(),
            max: E::max_builder_deposit_requests_per_payload(),
        });
    }
    if requests.builder_exits.len() > E::max_builder_exit_requests_per_payload() {
        return Err(EnvelopeError::OperationListTooLong {
            kind: "builder_exit_requests",
            length: requests.builder_exits.len(),
            max: E::max_builder_exit_requests_per_payload(),
        });
    }
    if envelope.payload.withdrawals.len() > E::max_withdrawals_per_payload() {
        return Err(EnvelopeError::OperationListTooLong {
            kind: "withdrawals",
            length: envelope.payload.withdrawals.len(),
            max: E::max_withdrawals_per_payload(),
        });
    }

    // The SSZ root of the envelope's execution requests must match the committed bid, per
    // `verify_execution_payload_envelope` in the spec.
    let execution_requests_root = requests.tree_hash_root();
    if execution_requests_root != execution_bid.execution_requests_root() {
        return Err(EnvelopeError::ExecutionRequestsRootMismatch {
            committed_bid: execution_bid.execution_requests_root(),
            envelope: execution_requests_root,
        });
    }

    // The SSZ root of the envelope's payload must match the committed bid.
    let payload_root = envelope.payload.tree_hash_root();
    if payload_root != execution_bid.payload_root() {
        return Err(EnvelopeError::PayloadRootMismatch {
            committed_bid: execution_bid.payload_root(),
            envelope: payload_root,
        });
    }

    Ok(())
}

/// A wrapper around a `ExecutionPayloadEnvelope` that indicates it has been approved for re-gossiping on
/// the p2p network.
#[derive(Educe)]
#[educe(Debug(bound = "T: BeaconChainTypes"))]
pub struct GossipVerifiedEnvelope<T: BeaconChainTypes> {
    pub envelope: Arc<ExecutionPayloadEnvelope<T::EthSpec>>,
    pub block: Arc<SignedBeaconBlock<T::EthSpec>>,
}

impl<T: BeaconChainTypes> GossipVerifiedEnvelope<T> {
    pub fn new(
        envelope: Arc<ExecutionPayloadEnvelope<T::EthSpec>>,
        ctx: &GossipVerificationContext<'_, T>,
    ) -> Result<Self, EnvelopeError> {
        let beacon_block_root = envelope.beacon_block_root;

        // Check that we've seen the beacon block for this envelope and that it passes validation.
        // TODO(EIP-7732): We might need some type of status table in order to differentiate between:
        // If we have a block_processing_table, we could have a Processed(Bid, bool) state that is only
        // entered post adding to fork choice. That way, we could potentially need only a single call to make
        // sure the block is valid and to do all consequent checks with the bid
        //
        // 1. Blocks we haven't seen (IGNORE), and
        // 2. Blocks we've seen that are invalid (REJECT).
        //
        // Presently these two cases are conflated.
        let fork_choice_read_lock = ctx.canonical_head.fork_choice_read_lock();
        let Some(proto_block) = fork_choice_read_lock.get_block(&beacon_block_root) else {
            return Err(EnvelopeError::BlockRootUnknown {
                block_root: beacon_block_root,
            });
        };

        drop(fork_choice_read_lock);

        let block_slot = proto_block.slot;
        if ctx.source == EnvelopeSource::Gossip
            && ctx
                .observed_payload_envelopes
                .envelope_has_been_observed(block_slot, beacon_block_root)
        {
            // If we've already seen a valid envelope for this beacon block, ignore the duplicate
            return Err(EnvelopeError::EnvelopeAlreadySeen {
                block_root: beacon_block_root,
            });
        }

        let latest_finalized_slot = ctx
            .canonical_head
            .cached_head()
            .finalized_checkpoint()
            .epoch
            .start_slot(T::EthSpec::slots_per_epoch());

        let block = match ctx.store.try_get_full_block(&beacon_block_root)? {
            Some(DatabaseBlock::Full(block)) => Arc::new(block),
            Some(DatabaseBlock::Blinded(_)) | None => {
                return Err(EnvelopeError::from(BeaconChainError::MissingBeaconBlock(
                    beacon_block_root,
                )));
            }
        };
        let execution_bid = block
            .message()
            .body()
            .signed_execution_payload_bid()?
            .message();
        let builder_index = execution_bid.builder_index();
        let payload_root = execution_bid.payload_root();

        // The envelope is not signed. The block commits to every field of it through its bid, so
        // consistency with the bid is what authenticates the envelope.
        verify_envelope_consistency(&envelope, &block, execution_bid, latest_finalized_slot)?;

        let gossip_verified_envelope = Self { envelope, block };

        // Mark the envelope as seen regardless of its source, so that gossip deduplicates
        // against envelopes we already hold, whichever path they arrived through.
        // The operation is atomic, so if a concurrent verification marked the same `block_root`
        // first, this envelope is considered a duplicate.
        let envelope_already_seen = !ctx
            .observed_payload_envelopes
            .observe_envelope(&gossip_verified_envelope);

        if envelope_already_seen && ctx.source == EnvelopeSource::Gossip {
            return Err(EnvelopeError::EnvelopeAlreadySeen {
                block_root: beacon_block_root,
            });
        }

        if !envelope_already_seen {
            let payload = &gossip_verified_envelope.envelope.payload;

            // Emit the SSE event once for the first observation from any source.
            if let Some(event_handler) = ctx.event_handler.as_ref()
                && event_handler.has_execution_payload_gossip_subscribers()
            {
                event_handler.register(EventKind::ExecutionPayloadGossip(
                    SseExecutionPayloadGossip {
                        slot: block_slot,
                        builder_index,
                        block_hash: payload.block_hash,
                        payload_root,
                        block_root: beacon_block_root,
                    },
                ));
            }
        }

        Ok(gossip_verified_envelope)
    }

    pub fn envelope_cloned(&self) -> Arc<ExecutionPayloadEnvelope<T::EthSpec>> {
        self.envelope.clone()
    }
}

impl<T: BeaconChainTypes> BeaconChain<T> {
    /// Build a `GossipVerificationContext` from this `BeaconChain` for `GossipVerifiedEnvelope`.
    pub fn payload_envelope_gossip_verification_context(
        &self,
        source: EnvelopeSource,
    ) -> GossipVerificationContext<'_, T> {
        GossipVerificationContext {
            source,
            canonical_head: &self.canonical_head,
            store: &self.store,
            observed_payload_envelopes: &self.observed_payload_envelopes,
            event_handler: &self.event_handler,
        }
    }

    /// Returns `Ok(GossipVerifiedEnvelope)` if the supplied `envelope` should be forwarded onto the
    /// gossip network. The envelope is not imported into the chain, it is just partially verified.
    ///
    /// `source` is the path the envelope arrived through. An envelope for an already seen
    /// `block_root` errors with `EnvelopeError::EnvelopeAlreadySeen` only on the gossip path.
    ///
    /// The returned `GossipVerifiedEnvelope` should be provided to `Self::process_execution_payload_envelope` immediately
    /// after it is returned, unless some other circumstance decides it should not be imported at
    /// all.
    ///
    /// ## Errors
    ///
    /// Returns an `Err` if the given envelope was invalid, or an error was encountered during verification.
    pub async fn verify_envelope_for_gossip(
        self: &Arc<Self>,
        envelope: Arc<ExecutionPayloadEnvelope<T::EthSpec>>,
        source: EnvelopeSource,
    ) -> Result<GossipVerifiedEnvelope<T>, EnvelopeError> {
        let chain = self.clone();
        self.task_executor
            .clone()
            .spawn_blocking_handle(
                move || {
                    let slot = envelope.slot();
                    let beacon_block_root = envelope.beacon_block_root;

                    let ctx = chain.payload_envelope_gossip_verification_context(source);
                    match GossipVerifiedEnvelope::new(envelope, &ctx) {
                        Ok(verified) => {
                            debug!(
                                %slot,
                                ?beacon_block_root,
                                "Successfully verified gossip envelope"
                            );

                            Ok(verified)
                        }
                        Err(e) => {
                            debug!(
                                error = e.to_string(),
                                ?beacon_block_root,
                                %slot,
                                "Rejected gossip envelope"
                            );

                            Err(e)
                        }
                    }
                },
                "gossip_envelope_verification_handle",
            )
            .ok_or(BeaconChainError::RuntimeShutdown)?
            .await
            .map_err(BeaconChainError::TokioJoin)?
    }
}

#[cfg(test)]
mod tests {
    use std::marker::PhantomData;

    use bls::{PublicKeyBytes, Signature, SignatureBytes};
    use ssz_types::ProgressiveVariableList;
    use types::{
        Address, BeaconBlock, BeaconBlockBodyGloas, BeaconBlockGloas, BuilderDepositRequest,
        BuilderExitRequest, ConsolidationRequest, Eth1Data, EthSpec, ExecutionBlockHash,
        ExecutionPayloadBid, ExecutionPayloadBidGloas, ExecutionPayloadBidRef,
        ExecutionPayloadEnvelope, ExecutionPayloadGloas, ExecutionRequestsGloas, Graffiti, Hash256,
        SignedBeaconBlock, SignedExecutionPayloadBidGloas, Slot, Spec, SyncAggregate, Withdrawal,
        WithdrawalRequest,
    };

    use super::verify_envelope_consistency;
    use crate::payload_envelope_verification::EnvelopeError;
    use tree_hash::TreeHash;

    type E = Spec;

    fn make_envelope(slot: Slot, block_hash: ExecutionBlockHash) -> ExecutionPayloadEnvelope<E> {
        ExecutionPayloadEnvelope {
            payload: ExecutionPayloadGloas {
                block_hash,
                slot_number: slot,
                ..ExecutionPayloadGloas::default()
            },
            execution_requests: ExecutionRequestsGloas::default(),
            beacon_block_root: Hash256::ZERO,
            parent_beacon_block_root: Hash256::ZERO,
        }
    }

    fn make_block(slot: Slot) -> SignedBeaconBlock<E> {
        let block = BeaconBlock::Gloas(BeaconBlockGloas {
            slot,
            proposer_index: 0,
            parent_root: Hash256::ZERO,
            state_root: Hash256::ZERO,
            body: BeaconBlockBodyGloas {
                randao_reveal: Signature::empty(),
                eth1_data: Eth1Data {
                    deposit_root: Hash256::ZERO,
                    block_hash: Hash256::ZERO,
                    deposit_count: 0,
                },
                graffiti: Graffiti::default(),
                proposer_slashings: ProgressiveVariableList::empty(),
                attester_slashings: ProgressiveVariableList::empty(),
                attestations: ProgressiveVariableList::empty(),
                deposits: ProgressiveVariableList::empty(),
                voluntary_exits: ProgressiveVariableList::empty(),
                sync_aggregate: SyncAggregate::empty(),
                bls_to_execution_changes: ProgressiveVariableList::empty(),
                parent_execution_requests: ExecutionRequestsGloas::default(),
                signed_execution_payload_bid: SignedExecutionPayloadBidGloas::empty(),
                payload_attestations: ProgressiveVariableList::empty(),
                _phantom: PhantomData,
            },
        });
        SignedBeaconBlock::from_block(block, Signature::empty())
    }

    /// The bid committing to `envelope`.
    fn make_bid(envelope: &ExecutionPayloadEnvelope<E>) -> ExecutionPayloadBid<E> {
        ExecutionPayloadBid::Gloas(ExecutionPayloadBidGloas {
            block_hash: envelope.payload.block_hash,
            payload_root: envelope.payload.tree_hash_root(),
            execution_requests_root: envelope.execution_requests.tree_hash_root(),
            ..ExecutionPayloadBidGloas::default()
        })
    }

    #[test]
    fn test_valid_envelope() {
        let slot = Slot::new(10);
        let block_hash = ExecutionBlockHash::repeat_byte(0xaa);

        let envelope = make_envelope(slot, block_hash);
        let block = make_block(slot);
        let bid = make_bid(&envelope);

        assert!(
            verify_envelope_consistency::<E>(&envelope, &block, bid.to_ref(), Slot::new(0)).is_ok()
        );
    }

    #[test]
    fn test_prior_to_finalization() {
        let slot = Slot::new(5);
        let block_hash = ExecutionBlockHash::repeat_byte(0xbb);

        let envelope = make_envelope(slot, block_hash);
        let block = make_block(slot);
        let bid = make_bid(&envelope);
        let latest_finalized_slot = Slot::new(10);

        let result = verify_envelope_consistency::<E>(
            &envelope,
            &block,
            bid.to_ref(),
            latest_finalized_slot,
        );
        assert!(matches!(
            result,
            Err(EnvelopeError::PriorToFinalization { .. })
        ));
    }

    #[test]
    fn test_slot_mismatch() {
        let block_hash = ExecutionBlockHash::repeat_byte(0xcc);

        let envelope = make_envelope(Slot::new(10), block_hash);
        let block = make_block(Slot::new(20));
        let bid = make_bid(&envelope);

        let result =
            verify_envelope_consistency::<E>(&envelope, &block, bid.to_ref(), Slot::new(0));
        assert!(matches!(result, Err(EnvelopeError::SlotMismatch { .. })));
    }

    #[test]
    fn test_payload_root_mismatch() {
        let slot = Slot::new(10);
        let block_hash = ExecutionBlockHash::repeat_byte(0xdd);

        let mut envelope = make_envelope(slot, block_hash);
        let block = make_block(slot);
        let bid = make_bid(&envelope);

        // Tamper a field which the bid only commits to via `payload_root`.
        envelope.payload.state_root = Hash256::repeat_byte(0x11);

        let result =
            verify_envelope_consistency::<E>(&envelope, &block, bid.to_ref(), Slot::new(0));
        assert!(matches!(
            result,
            Err(EnvelopeError::PayloadRootMismatch { .. })
        ));
    }

    #[test]
    fn test_parent_beacon_block_root_mismatch() {
        let slot = Slot::new(10);
        let block_hash = ExecutionBlockHash::repeat_byte(0xab);

        let mut envelope = make_envelope(slot, block_hash);
        // The block's parent root is `Hash256::ZERO`; claim a different parent beacon
        // block root in the envelope.
        envelope.parent_beacon_block_root = Hash256::repeat_byte(0x11);
        let block = make_block(slot);
        let bid = make_bid(&envelope);

        let result =
            verify_envelope_consistency::<E>(&envelope, &block, bid.to_ref(), Slot::new(0));
        assert!(matches!(
            result,
            Err(EnvelopeError::ParentBeaconBlockRootMismatch { .. })
        ));
    }

    #[test]
    fn test_block_hash_mismatch() {
        let slot = Slot::new(10);

        let envelope = make_envelope(slot, ExecutionBlockHash::repeat_byte(0xee));
        let block = make_block(slot);
        let bid = make_bid(&make_envelope(slot, ExecutionBlockHash::repeat_byte(0xff)));

        let result =
            verify_envelope_consistency::<E>(&envelope, &block, bid.to_ref(), Slot::new(0));
        assert!(matches!(
            result,
            Err(EnvelopeError::BlockHashMismatch { .. })
        ));
    }

    #[test]
    fn test_payload_withdrawals_over_limit() {
        let slot = Slot::new(10);
        let block_hash = ExecutionBlockHash::repeat_byte(0xaa);

        let mut envelope = make_envelope(slot, block_hash);
        let block = make_block(slot);

        let withdrawal = Withdrawal {
            index: 0,
            validator_index: 0,
            address: Address::ZERO,
            amount: 0,
        };
        let max = E::max_withdrawals_per_payload();
        envelope.payload.withdrawals =
            ProgressiveVariableList::new(vec![withdrawal.clone(); max]).unwrap();
        let bid = make_bid(&envelope);
        assert!(
            verify_envelope_consistency::<E>(&envelope, &block, bid.to_ref(), Slot::new(0)).is_ok()
        );

        assert!(
            ProgressiveVariableList::<Withdrawal, <E as EthSpec>::MaxWithdrawalsPerPayload>::new(
                vec![withdrawal; max + 1]
            )
            .is_err()
        );
    }

    fn assert_requests_list_bound(
        kind: &'static str,
        max: usize,
        set_len: impl Fn(&mut ExecutionRequestsGloas<E>, usize) -> Result<(), ssz_types::Error>,
    ) {
        let slot = Slot::new(10);
        let block_hash = ExecutionBlockHash::repeat_byte(0xaa);

        let mut envelope = make_envelope(slot, block_hash);
        let block = make_block(slot);

        set_len(&mut envelope.execution_requests, max).unwrap();
        let bid = ExecutionPayloadBidGloas {
            block_hash,
            payload_root: envelope.payload.tree_hash_root(),
            execution_requests_root: envelope.execution_requests.tree_hash_root(),
            ..ExecutionPayloadBidGloas::default()
        };
        assert!(
            verify_envelope_consistency::<E>(
                &envelope,
                &block,
                ExecutionPayloadBidRef::Gloas(&bid),
                Slot::new(0)
            )
            .is_ok(),
            "{kind} at max should be accepted"
        );

        assert!(
            set_len(&mut envelope.execution_requests, max + 1).is_err(),
            "{kind} over max should be rejected during construction"
        );
    }

    #[test]
    fn test_execution_requests_over_limit() {
        assert_requests_list_bound(
            "withdrawal_requests",
            E::max_withdrawal_requests_per_payload(),
            |requests, len| {
                let withdrawal_request = WithdrawalRequest {
                    source_address: Address::ZERO,
                    validator_pubkey: PublicKeyBytes::empty(),
                    amount: 0,
                };
                requests.withdrawals = ProgressiveVariableList::new(vec![withdrawal_request; len])?;
                Ok(())
            },
        );

        assert_requests_list_bound(
            "consolidation_requests",
            E::max_consolidation_requests_per_payload(),
            |requests, len| {
                let consolidation_request = ConsolidationRequest {
                    source_address: Address::ZERO,
                    source_pubkey: PublicKeyBytes::empty(),
                    target_pubkey: PublicKeyBytes::empty(),
                };
                requests.consolidations =
                    ProgressiveVariableList::new(vec![consolidation_request; len])?;
                Ok(())
            },
        );

        assert_requests_list_bound(
            "builder_deposit_requests",
            E::max_builder_deposit_requests_per_payload(),
            |requests, len| {
                let builder_deposit_request = BuilderDepositRequest {
                    pubkey: PublicKeyBytes::empty(),
                    withdrawal_credentials: Hash256::ZERO,
                    amount: 0,
                    signature: SignatureBytes::empty(),
                };
                requests.builder_deposits =
                    ProgressiveVariableList::new(vec![builder_deposit_request; len])?;
                Ok(())
            },
        );

        assert_requests_list_bound(
            "builder_exit_requests",
            E::max_builder_exit_requests_per_payload(),
            |requests, len| {
                let builder_exit_request = BuilderExitRequest {
                    source_address: Address::ZERO,
                    pubkey: PublicKeyBytes::empty(),
                };
                requests.builder_exits =
                    ProgressiveVariableList::new(vec![builder_exit_request; len])?;
                Ok(())
            },
        );
    }
}
