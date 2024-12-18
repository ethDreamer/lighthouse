use crate::per_block_processing::process_operations::{
    process_consolidation_requests, process_deposit_requests, process_withdrawal_requests,
};
use crate::BlockProcessingError;
use crate::VerifySignatures;
use types::{BeaconState, BeaconStateError, ChainSpec, EthSpec, Hash256, SignedExecutionEnvelope};

pub enum EnvelopeProcessingError {
    /// Bad Signature
    BadSignature,
    BeaconStateError(BeaconStateError),
    BlockProcessingError(BlockProcessingError),
}

impl From<BeaconStateError> for EnvelopeProcessingError {
    fn from(e: BeaconStateError) -> Self {
        EnvelopeProcessingError::BeaconStateError(e)
    }
}

impl From<BlockProcessingError> for EnvelopeProcessingError {
    fn from(e: BlockProcessingError) -> Self {
        EnvelopeProcessingError::BlockProcessingError(e)
    }
}

/// Processes a `SignedExecutionEnvelope`
///
/// This function does all the state modifications inside `process_execution_payload()`
pub fn envelope_processing<E: EthSpec>(
    state: &mut BeaconState<E>,
    signed_envelope: &SignedExecutionEnvelope<E>,
    verify_signatures: VerifySignatures,
    spec: &ChainSpec,
) -> Result<(), EnvelopeProcessingError> {
    if verify_signatures.is_true() {
        // Verify Signed Envelope Signature
        if !signed_envelope.verify_signature(&state, spec)? {
            return Err(EnvelopeProcessingError::BadSignature);
        }
    }

    let previous_state_root = state.canonical_root()?;
    if state.latest_block_header().state_root == Hash256::default() {
        state.latest_block_header_mut().state_root = previous_state_root;
    }

    // process electra operations
    let envelope = signed_envelope.message();
    let payload = envelope.payload();
    let execution_requests = envelope.execution_requests();
    process_deposit_requests(state, &execution_requests.deposits, spec)?;
    process_withdrawal_requests(state, &execution_requests.withdrawals, spec)?;
    process_consolidation_requests(state, &execution_requests.consolidations, spec)?;

    // cache the latest block hash and full slot
    *state.latest_block_hash_mut()? = payload.block_hash();
    *state.latest_full_slot_mut()? = state.slot();

    Ok(())
}
