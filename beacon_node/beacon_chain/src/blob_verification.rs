use derivative::Derivative;
use slot_clock::SlotClock;
use state_processing::state_advance::partial_state_advance;
use std::sync::Arc;

use crate::beacon_chain::{
    BeaconChain, BeaconChainTypes, BLOCK_PROCESSING_CACHE_LOCK_TIMEOUT,
    VALIDATOR_PUBKEY_CACHE_LOCK_TIMEOUT,
};
use crate::data_availability_checker::AvailabilityCheckError;
use crate::kzg_utils::{validate_blob, validate_blobs};
use crate::BeaconChainError;
use kzg::Kzg;
use slog::{debug, warn};
use ssz_derive::{Decode, Encode};
use ssz_types::VariableList;
use std::borrow::Cow;
use types::blob_sidecar::BlobIdentifier;
use types::{
    BeaconState, BeaconStateError, BlobSidecar, BlobSidecarList, ChainSpec, CloneConfig, EthSpec,
    Hash256, RelativeEpoch, SignedBlobSidecar, Slot,
};

/// An error occurred while validating a gossip blob.
#[derive(Debug)]
pub enum GossipBlobError<T: EthSpec> {
    /// The blob sidecar is from a slot that is later than the current slot (with respect to the
    /// gossip clock disparity).
    ///
    /// ## Peer scoring
    ///
    /// Assuming the local clock is correct, the peer has sent an invalid message.
    FutureSlot {
        message_slot: Slot,
        latest_permissible_slot: Slot,
    },

    /// There was an error whilst processing the blob. It is not known if it is
    /// valid or invalid.
    ///
    /// ## Peer scoring
    ///
    /// We were unable to process this blob due to an internal error. It's
    /// unclear if the blob is valid.
    BeaconChainError(BeaconChainError),

    /// The `BlobSidecar` was gossiped over an incorrect subnet.
    ///
    /// ## Peer scoring
    ///
    /// The blob is invalid or the peer is faulty.
    InvalidSubnet { expected: u64, received: u64 },

    /// The sidecar corresponds to a slot older than the finalized head slot.
    ///
    /// ## Peer scoring
    ///
    /// It's unclear if this blob is valid, but this blob is for a finalized slot and is
    /// therefore useless to us.
    PastFinalizedSlot {
        blob_slot: Slot,
        finalized_slot: Slot,
    },

    /// The proposer index specified in the sidecar does not match the locally computed
    /// proposer index.
    ///
    /// ## Peer scoring
    ///
    /// The blob is invalid and the peer is faulty.
    ProposerIndexMismatch { sidecar: usize, local: usize },

    /// The proposal signature in invalid.
    ///
    /// ## Peer scoring
    ///
    /// The blob is invalid and the peer is faulty.
    ProposerSignatureInvalid,

    /// The proposal_index corresponding to blob.beacon_block_root is not known.
    ///
    /// ## Peer scoring
    ///
    /// The blob is invalid and the peer is faulty.
    UnknownValidator(u64),

    /// The provided blob is not from a later slot than its parent.
    ///
    /// ## Peer scoring
    ///
    /// The blob is invalid and the peer is faulty.
    BlobIsNotLaterThanParent { blob_slot: Slot, parent_slot: Slot },

    /// The provided blob's parent block is unknown.
    ///
    /// ## Peer scoring
    ///
    /// We cannot process the blob without validating its parent, the peer isn't necessarily faulty.
    BlobParentUnknown(Arc<BlobSidecar<T>>),

    /// A blob has already been seen for the given `(sidecar.block_root, sidecar.index)` tuple
    /// over gossip or no gossip sources.
    ///
    /// ## Peer scoring
    ///
    /// The peer isn't faulty, but we do not forward it over gossip.
    RepeatBlob {
        proposer: u64,
        slot: Slot,
        index: u64,
    },
}

impl<T: EthSpec> std::fmt::Display for GossipBlobError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GossipBlobError::BlobParentUnknown(blob_sidecar) => {
                write!(
                    f,
                    "BlobParentUnknown(parent_root:{})",
                    blob_sidecar.block_parent_root
                )
            }
            other => write!(f, "{:?}", other),
        }
    }
}

impl<T: EthSpec> From<BeaconChainError> for GossipBlobError<T> {
    fn from(e: BeaconChainError) -> Self {
        GossipBlobError::BeaconChainError(e)
    }
}

impl<T: EthSpec> From<BeaconStateError> for GossipBlobError<T> {
    fn from(e: BeaconStateError) -> Self {
        GossipBlobError::BeaconChainError(BeaconChainError::BeaconStateError(e))
    }
}

pub type GossipVerifiedBlobList<T> = VariableList<
    GossipVerifiedBlob<T>,
    <<T as BeaconChainTypes>::EthSpec as EthSpec>::MaxBlobsPerBlock,
>;

/// A wrapper around a `BlobSidecar` that indicates it has been approved for re-gossiping on
/// the p2p network.
#[derive(Debug)]
pub struct GossipVerifiedBlob<T: BeaconChainTypes> {
    blob: SignedBlobSidecar<T::EthSpec>,
}

impl<T: BeaconChainTypes> GossipVerifiedBlob<T> {
    pub fn new(
        blob: SignedBlobSidecar<T::EthSpec>,
        chain: &BeaconChain<T>,
    ) -> Result<Self, GossipBlobError<T::EthSpec>> {
        let blob_index = blob.message.index;
        validate_blob_sidecar_for_gossip(blob, blob_index, chain)
    }
    pub fn id(&self) -> BlobIdentifier {
        self.blob.message.id()
    }
    pub fn block_root(&self) -> Hash256 {
        self.blob.message.block_root
    }
    pub fn to_blob(self) -> Arc<BlobSidecar<T::EthSpec>> {
        self.blob.message
    }
    pub fn signed_blob(&self) -> SignedBlobSidecar<T::EthSpec> {
        self.blob.clone()
    }
    pub fn slot(&self) -> Slot {
        self.blob.message.slot
    }
    pub fn proposer_index(&self) -> u64 {
        self.blob.message.proposer_index
    }
}

pub fn validate_blob_sidecar_for_gossip<T: BeaconChainTypes>(
    signed_blob_sidecar: SignedBlobSidecar<T::EthSpec>,
    subnet: u64,
    chain: &BeaconChain<T>,
) -> Result<GossipVerifiedBlob<T>, GossipBlobError<T::EthSpec>> {
    let blob_slot = signed_blob_sidecar.message.slot;
    let blob_index = signed_blob_sidecar.message.index;
    let block_parent_root = signed_blob_sidecar.message.block_parent_root;
    let blob_proposer_index = signed_blob_sidecar.message.proposer_index;
    let block_root = signed_blob_sidecar.message.block_root;
    let blob_epoch = blob_slot.epoch(T::EthSpec::slots_per_epoch());

    // Verify that the blob_sidecar was received on the correct subnet.
    if blob_index != subnet {
        return Err(GossipBlobError::InvalidSubnet {
            expected: blob_index,
            received: subnet,
        });
    }

    // Verify that the sidecar is not from a future slot.
    let latest_permissible_slot = chain
        .slot_clock
        .now_with_future_tolerance(chain.spec.maximum_gossip_clock_disparity())
        .ok_or(BeaconChainError::UnableToReadSlot)?;
    if blob_slot > latest_permissible_slot {
        return Err(GossipBlobError::FutureSlot {
            message_slot: blob_slot,
            latest_permissible_slot,
        });
    }

    // Verify that the sidecar slot is greater than the latest finalized slot
    let latest_finalized_slot = chain
        .head()
        .finalized_checkpoint()
        .epoch
        .start_slot(T::EthSpec::slots_per_epoch());
    if blob_slot <= latest_finalized_slot {
        return Err(GossipBlobError::PastFinalizedSlot {
            blob_slot,
            finalized_slot: latest_finalized_slot,
        });
    }

    // Verify that this is the first blob sidecar received for the (sidecar.block_root, sidecar.index) tuple
    if chain
        .observed_blob_sidecars
        .read()
        .is_known(&signed_blob_sidecar.message)
        .map_err(|e| GossipBlobError::BeaconChainError(e.into()))?
    {
        return Err(GossipBlobError::RepeatBlob {
            proposer: blob_proposer_index,
            slot: blob_slot,
            index: blob_index,
        });
    }

    // We have already verified that the blob is past finalization, so we can
    // just check fork choice for the block's parent.
    let Some(parent_block) = chain
        .canonical_head
        .fork_choice_read_lock()
        .get_block(&block_parent_root)
    else {
        return Err(GossipBlobError::BlobParentUnknown(
            signed_blob_sidecar.message,
        ));
    };

    if parent_block.slot >= blob_slot {
        return Err(GossipBlobError::BlobIsNotLaterThanParent {
            blob_slot,
            parent_slot: parent_block.slot,
        });
    }

    // Note: We check that the proposer_index matches against the shuffling first to avoid
    // signature verification against an invalid proposer_index.
    let proposer_shuffling_root =
        if parent_block.slot.epoch(T::EthSpec::slots_per_epoch()) == blob_epoch {
            parent_block
                .next_epoch_shuffling_id
                .shuffling_decision_block
        } else {
            parent_block.root
        };

    let proposer_opt = chain
        .beacon_proposer_cache
        .lock()
        .get_slot::<T::EthSpec>(proposer_shuffling_root, blob_slot);

    let (proposer_index, fork) = if let Some(proposer) = proposer_opt {
        (proposer.index, proposer.fork)
    } else {
        debug!(
            chain.log,
            "Proposer shuffling cache miss for blob verification";
            "block_root" => %block_root,
            "index" => %blob_index,
        );
        if let Some(mut snapshot) = chain
            .snapshot_cache
            .try_read_for(BLOCK_PROCESSING_CACHE_LOCK_TIMEOUT)
            .and_then(|snapshot_cache| {
                snapshot_cache.get_cloned(block_parent_root, CloneConfig::committee_caches_only())
            })
        {
            if snapshot.beacon_state.slot() == blob_slot {
                debug!(
                    chain.log,
                    "Cloning snapshot cache state for blob verification";
                    "block_root" => %block_root,
                    "index" => %blob_index,
                );
                (
                    snapshot
                        .beacon_state
                        .get_beacon_proposer_index(blob_slot, &chain.spec)?,
                    snapshot.beacon_state.fork(),
                )
            } else {
                debug!(
                    chain.log,
                    "Cloning and advancing snapshot cache state for blob verification";
                    "block_root" => %block_root,
                    "index" => %blob_index,
                );
                let state = cheap_state_advance_to_obtain_committees(
                    &mut snapshot.beacon_state,
                    Some(snapshot.beacon_block_root),
                    blob_slot,
                    &chain.spec,
                )?;
                (
                    state.get_beacon_proposer_index(blob_slot, &chain.spec)?,
                    state.fork(),
                )
            }
        }
        // Need to advance the state to get the proposer index
        else {
            warn!(
                chain.log,
                "Snapshot cache miss for blob verification";
                "block_root" => %block_root,
                "index" => %blob_index,
            );

            let parent_block = chain
                .get_blinded_block(&block_parent_root)
                .map_err(GossipBlobError::BeaconChainError)?
                .ok_or_else(|| {
                    GossipBlobError::from(BeaconChainError::MissingBeaconBlock(block_parent_root))
                })?;

            let mut parent_state = chain
                .get_state(&parent_block.state_root(), Some(parent_block.slot()))?
                .ok_or_else(|| {
                    BeaconChainError::DBInconsistent(format!(
                        "Missing state {:?}",
                        parent_block.state_root()
                    ))
                })?;
            let state = cheap_state_advance_to_obtain_committees(
                &mut parent_state,
                Some(parent_block.state_root()),
                blob_slot,
                &chain.spec,
            )?;

            let proposers = state.get_beacon_proposer_indices(&chain.spec)?;
            let proposer_index = *proposers
                .get(blob_slot.as_usize() % T::EthSpec::slots_per_epoch() as usize)
                .ok_or_else(|| BeaconChainError::NoProposerForSlot(blob_slot))?;

            // Prime the proposer shuffling cache with the newly-learned value.
            chain.beacon_proposer_cache.lock().insert(
                blob_epoch,
                proposer_shuffling_root,
                proposers,
                state.fork(),
            )?;
            (proposer_index, state.fork())
        }
    };

    if proposer_index != blob_proposer_index as usize {
        return Err(GossipBlobError::ProposerIndexMismatch {
            sidecar: blob_proposer_index as usize,
            local: proposer_index,
        });
    }

    // Signature verification
    let signature_is_valid = {
        let pubkey_cache = chain
            .validator_pubkey_cache
            .try_read_for(VALIDATOR_PUBKEY_CACHE_LOCK_TIMEOUT)
            .ok_or(BeaconChainError::ValidatorPubkeyCacheLockTimeout)
            .map_err(GossipBlobError::BeaconChainError)?;

        let pubkey = pubkey_cache
            .get(proposer_index)
            .ok_or_else(|| GossipBlobError::UnknownValidator(proposer_index as u64))?;

        signed_blob_sidecar.verify_signature(
            None,
            pubkey,
            &fork,
            chain.genesis_validators_root,
            &chain.spec,
        )
    };

    if !signature_is_valid {
        return Err(GossipBlobError::ProposerSignatureInvalid);
    }

    // Now the signature is valid, store the proposal so we don't accept another blob sidecar
    // with the same `BlobIdentifier`.
    // It's important to double-check that the proposer still hasn't been observed so we don't
    // have a race-condition when verifying two blocks simultaneously.
    //
    // Note: If this BlobSidecar goes on to fail full verification, we do not evict it from the seen_cache
    // as alternate blob_sidecars for the same identifier can still be retrieved
    // over rpc. Evicting them from this cache would allow faster propagation over gossip. So we allow
    // retrieval of potentially valid blocks over rpc, but try to punish the proposer for signing
    // invalid messages. Issue for more background
    // https://github.com/ethereum/consensus-specs/issues/3261
    if chain
        .observed_blob_sidecars
        .write()
        .observe_sidecar(&signed_blob_sidecar.message)
        .map_err(|e| GossipBlobError::BeaconChainError(e.into()))?
    {
        return Err(GossipBlobError::RepeatBlob {
            proposer: proposer_index as u64,
            slot: blob_slot,
            index: blob_index,
        });
    }

    Ok(GossipVerifiedBlob {
        blob: signed_blob_sidecar,
    })
}

/// Performs a cheap (time-efficient) state advancement so the committees and proposer shuffling for
/// `slot` can be obtained from `state`.
///
/// The state advancement is "cheap" since it does not generate state roots. As a result, the
/// returned state might be holistically invalid but the committees/proposers will be correct (since
/// they do not rely upon state roots).
///
/// If the given `state` can already serve the `slot`, the committees will be built on the `state`
/// and `Cow::Borrowed(state)` will be returned. Otherwise, the state will be cloned, cheaply
/// advanced and then returned as a `Cow::Owned`. The end result is that the given `state` is never
/// mutated to be invalid (in fact, it is never changed beyond a simple committee cache build).
///
/// Note: This is a copy of the `block_verification::cheap_state_advance_to_obtain_committees` to return
/// a BlobError error type instead.
fn cheap_state_advance_to_obtain_committees<'a, E: EthSpec>(
    state: &'a mut BeaconState<E>,
    state_root_opt: Option<Hash256>,
    blob_slot: Slot,
    spec: &ChainSpec,
) -> Result<Cow<'a, BeaconState<E>>, GossipBlobError<E>> {
    let block_epoch = blob_slot.epoch(E::slots_per_epoch());

    if state.current_epoch() == block_epoch {
        // Build both the current and previous epoch caches, as the previous epoch caches are
        // useful for verifying attestations in blocks from the current epoch.
        state.build_committee_cache(RelativeEpoch::Previous, spec)?;
        state.build_committee_cache(RelativeEpoch::Current, spec)?;

        Ok(Cow::Borrowed(state))
    } else if state.slot() > blob_slot {
        Err(GossipBlobError::BlobIsNotLaterThanParent {
            blob_slot,
            parent_slot: state.slot(),
        })
    } else {
        let mut state = state.clone_with(CloneConfig::committee_caches_only());
        let target_slot = block_epoch.start_slot(E::slots_per_epoch());

        // Advance the state into the same epoch as the block. Use the "partial" method since state
        // roots are not important for proposer/attester shuffling.
        partial_state_advance(&mut state, state_root_opt, target_slot, spec)
            .map_err(|e| GossipBlobError::BeaconChainError(BeaconChainError::from(e)))?;

        state.build_committee_cache(RelativeEpoch::Previous, spec)?;
        state.build_committee_cache(RelativeEpoch::Current, spec)?;

        Ok(Cow::Owned(state))
    }
}

/// Wrapper over a `BlobSidecar` for which we have completed kzg verification.
/// i.e. `verify_blob_kzg_proof(blob, commitment, proof) == true`.
#[derive(Debug, Derivative, Clone, Encode, Decode)]
#[derivative(PartialEq, Eq)]
#[ssz(struct_behaviour = "transparent")]
pub struct KzgVerifiedBlob<T: EthSpec> {
    blob: Arc<BlobSidecar<T>>,
}

impl<T: EthSpec> PartialOrd for KzgVerifiedBlob<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.blob.partial_cmp(&other.blob)
    }
}

impl<T: EthSpec> Ord for KzgVerifiedBlob<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.blob.cmp(&other.blob)
    }
}

impl<T: EthSpec> KzgVerifiedBlob<T> {
    pub fn to_blob(self) -> Arc<BlobSidecar<T>> {
        self.blob
    }
    pub fn as_blob(&self) -> &BlobSidecar<T> {
        &self.blob
    }
    pub fn clone_blob(&self) -> Arc<BlobSidecar<T>> {
        self.blob.clone()
    }
    pub fn block_root(&self) -> Hash256 {
        self.blob.block_root
    }
    pub fn blob_index(&self) -> u64 {
        self.blob.index
    }
}

/// Complete kzg verification for a `GossipVerifiedBlob`.
///
/// Returns an error if the kzg verification check fails.
pub fn verify_kzg_for_blob<T: EthSpec>(
    sidecar: Arc<BlobSidecar<T>>,
    kzg: &Kzg<T::Kzg>,
) -> Result<KzgVerifiedBlob<T>, AvailabilityCheckError> {
    let _timer = crate::metrics::start_timer(&crate::metrics::KZG_VERIFICATION_SINGLE_TIMES);
    if validate_blob::<T>(
        kzg,
        &sidecar.blob,
        sidecar.kzg_commitment,
        sidecar.kzg_proof,
    )
    .map_err(AvailabilityCheckError::Kzg)?
    {
        Ok(KzgVerifiedBlob { blob: sidecar })
    } else {
        Err(AvailabilityCheckError::KzgVerificationFailed)
    }
}

/// Complete kzg verification for a list of `BlobSidecar`s.
/// Returns an error if any of the `BlobSidecar`s fails kzg verification.
///
/// Note: This function should be preferred over calling `verify_kzg_for_blob`
/// in a loop since this function kzg verifies a list of blobs more efficiently.
pub fn verify_kzg_for_blob_list<T: EthSpec>(
    blob_list: &BlobSidecarList<T>,
    kzg: &Kzg<T::Kzg>,
) -> Result<(), AvailabilityCheckError> {
    let _timer = crate::metrics::start_timer(&crate::metrics::KZG_VERIFICATION_BATCH_TIMES);
    let (blobs, (commitments, proofs)): (Vec<_>, (Vec<_>, Vec<_>)) = blob_list
        .iter()
        .map(|sidecar| {
            (
                sidecar.blob.clone(),
                (sidecar.kzg_commitment, sidecar.kzg_proof),
            )
        })
        .unzip();
    if validate_blobs::<T>(
        kzg,
        commitments.as_slice(),
        blobs.as_slice(),
        proofs.as_slice(),
    )
    .map_err(AvailabilityCheckError::Kzg)?
    {
        Ok(())
    } else {
        Err(AvailabilityCheckError::KzgVerificationFailed)
    }
}
