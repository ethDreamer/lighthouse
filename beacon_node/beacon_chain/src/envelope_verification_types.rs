use crate::data_availability_checker::{AvailableEnvelope, MaybeAvailableEnvelope};
use crate::PayloadVerificationOutcome;
use std::sync::Arc;
use types::{
    BeaconState, BlobIdentifier, EthSpec, Hash256, SignedBlindedBeaconBlock,
    SignedExecutionEnvelope,
};

/// A block that has completed all pre-deneb block processing checks including verification
/// by an EL client **and** has all requisite blob data to be imported into fork choice.
#[derive(PartialEq)]
pub struct AvailableExecutedEnvelope<E: EthSpec> {
    pub envelope: AvailableEnvelope<E>,
    pub import_data: EnvelopeImportData<E>,
    pub payload_verification_outcome: PayloadVerificationOutcome,
}

impl<E: EthSpec> AvailableExecutedEnvelope<E> {
    pub fn new(
        envelope: AvailableEnvelope<E>,
        import_data: EnvelopeImportData<E>,
        payload_verification_outcome: PayloadVerificationOutcome,
    ) -> Self {
        Self {
            envelope,
            import_data,
            payload_verification_outcome,
        }
    }

    pub fn get_all_blob_ids(&self) -> Vec<BlobIdentifier> {
        let num_blobs_expected = self
            .envelope
            .envelope()
            .message()
            .blob_kzg_commitments()
            .len();
        let mut blob_ids = Vec::with_capacity(num_blobs_expected);
        for i in 0..num_blobs_expected {
            blob_ids.push(BlobIdentifier {
                block_root: self.import_data.block_root,
                index: i as u64,
            });
        }
        blob_ids
    }
}

#[derive(PartialEq)]
pub struct EnvelopeImportData<E: EthSpec> {
    pub block_root: Hash256,
    pub parent_block: Arc<SignedBlindedBeaconBlock<E>>,
    pub post_state: Box<BeaconState<E>>,
}

pub struct AvailabilityPendingExecutedEnvelope<E: EthSpec> {
    pub envelope: Arc<SignedExecutionEnvelope<E>>,
    pub import_data: EnvelopeImportData<E>,
    pub payload_verification_outcome: PayloadVerificationOutcome,
}

impl<E: EthSpec> AvailabilityPendingExecutedEnvelope<E> {
    pub fn new(
        envelope: Arc<SignedExecutionEnvelope<E>>,
        import_data: EnvelopeImportData<E>,
        payload_verification_outcome: PayloadVerificationOutcome,
    ) -> Self {
        Self {
            envelope,
            import_data,
            payload_verification_outcome,
        }
    }

    pub fn as_envelope(&self) -> &SignedExecutionEnvelope<E> {
        self.envelope.as_ref()
    }

    pub fn num_blobs_expected(&self) -> usize {
        self.envelope.message().blob_kzg_commitments().len()
    }
}

/// An envelope that has gone through all envelope processing checks including envelope processing
/// and execution by an EL client. This block hasn't necessarily completed data availability checks.
///
///
/// It contains 2 variants:
/// 1. `Available`: This envelope has been executed and also contains all data to consider it a
///    fully available envelope.
/// 2. `AvailabilityPending`: This envelope hasn't received all required blobs to consider it a
///    fully available envelope.
pub enum ExecutedEnvelope<E: EthSpec> {
    Available(AvailableExecutedEnvelope<E>),
    AvailabilityPending(AvailabilityPendingExecutedEnvelope<E>),
}

impl<E: EthSpec> ExecutedEnvelope<E> {
    pub fn new(
        envelope: MaybeAvailableEnvelope<E>,
        import_data: EnvelopeImportData<E>,
        payload_verification_outcome: PayloadVerificationOutcome,
    ) -> Self {
        match envelope {
            MaybeAvailableEnvelope::Available(available_envelope) => {
                Self::Available(AvailableExecutedEnvelope::new(
                    available_envelope,
                    import_data,
                    payload_verification_outcome,
                ))
            }
            MaybeAvailableEnvelope::AvailabilityPending {
                block_root: _,
                envelope,
            } => Self::AvailabilityPending(AvailabilityPendingExecutedEnvelope::new(
                envelope,
                import_data,
                payload_verification_outcome,
            )),
        }
    }

    pub fn as_envelope(&self) -> &SignedExecutionEnvelope<E> {
        match self {
            Self::Available(available) => available.envelope.envelope(),
            Self::AvailabilityPending(pending) => pending.envelope.as_ref(),
        }
    }

    pub fn block_root(&self) -> Hash256 {
        match self {
            Self::Available(available) => available.import_data.block_root,
            Self::AvailabilityPending(pending) => pending.import_data.block_root,
        }
    }
}
