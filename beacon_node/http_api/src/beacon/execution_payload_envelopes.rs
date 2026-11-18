use crate::block_id::BlockId;
use crate::publish_blocks::{check_slashable, publish_column_sidecars};
use crate::task_spawner::{Priority, TaskSpawner};
use crate::utils::{
    ChainFilter, EthV1Filter, NetworkTxFilter, NotWhileSyncingFilter, ResponseFilter,
    TaskSpawnerFilter,
};
use crate::version::{
    ResponseIncludesVersion, add_consensus_version_header, add_ssz_content_type_header,
    execution_optimistic_finalized_beacon_response,
};
use beacon_chain::data_column_verification::{GossipDataColumnError, GossipVerifiedDataColumn};
use beacon_chain::payload_envelope_streamer::EnvelopeRequestSource;
use beacon_chain::payload_envelope_verification::EnvelopeError;
use beacon_chain::payload_envelope_verification::EnvelopeSource;
use beacon_chain::{
    AvailabilityProcessingStatus, BeaconChain, BeaconChainError, BeaconChainTypes, BlockError,
    NotifyExecutionLayer,
};
use bytes::Bytes;
use eth2::{
    CONSENSUS_VERSION_HEADER,
    types::{self as api_types, BroadcastValidation, ExecutionPayloadEnvelopeContents},
};
use futures::StreamExt;
use lighthouse_network::PubsubMessage;
use network::NetworkMessage;
use ssz::{Decode, Encode};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, error, info, warn};
use types::{
    BlockImportSource, DataColumnSidecar, EthSpec, ExecutionPayloadEnvelope, ForkName, KzgProofs,
};
use warp::{
    Filter, Rejection,
    http::response::Builder,
    reply::{Reply, Response},
};

/// An execution payload envelope to publish.
pub enum EnvelopeSubmission<E: EthSpec> {
    /// Envelope only; blobs and KZG proofs are taken from the node's pending payload envelope
    /// cache. This is not accepted by `POST beacon/execution_payload_envelopes`, it is for an
    /// envelope which this node built, see [`publish_self_built_execution_payload_envelope`].
    EnvelopeOnly(Arc<ExecutionPayloadEnvelope<E>>),
    /// Full envelope bundled with blobs and KZG proofs, the request body of
    /// `POST beacon/execution_payload_envelopes`. Allows publication via a beacon node that did
    /// not build the payload.
    EnvelopeAndBlobData(Box<ExecutionPayloadEnvelopeContents<E>>),
}

fn ensure_gloas_consensus_version(fork_name: ForkName) -> Result<(), Rejection> {
    if !fork_name.gloas_enabled() {
        return Err(warp_utils::reject::custom_bad_request(format!(
            "Eth-Consensus-Version {fork_name} is not supported for execution payload envelopes"
        )));
    }
    Ok(())
}

impl<E: EthSpec> EnvelopeSubmission<E> {
    fn from_ssz_bytes(bytes: &[u8]) -> Result<Self, Rejection> {
        let invalid_ssz = |e| warp_utils::reject::custom_bad_request(format!("invalid SSZ: {e:?}"));
        Ok(Self::EnvelopeAndBlobData(Box::new(
            ExecutionPayloadEnvelopeContents::from_ssz_bytes(bytes).map_err(invalid_ssz)?,
        )))
    }

    fn from_json(bytes: &[u8]) -> Result<Self, Rejection> {
        let invalid_json =
            |e| warp_utils::reject::custom_bad_request(format!("invalid JSON: {e:?}"));
        Ok(Self::EnvelopeAndBlobData(Box::new(
            serde_json::from_slice(bytes).map_err(invalid_json)?,
        )))
    }
}

// POST beacon/execution_payload_envelopes (SSZ)
pub(crate) fn post_beacon_execution_payload_envelopes_ssz<T: BeaconChainTypes>(
    eth_v1: EthV1Filter,
    task_spawner_filter: TaskSpawnerFilter<T>,
    chain_filter: ChainFilter<T>,
    network_tx_filter: NetworkTxFilter<T>,
    not_while_syncing_filter: NotWhileSyncingFilter,
) -> ResponseFilter {
    eth_v1
        .and(warp::path("beacon"))
        .and(warp::path("execution_payload_envelopes"))
        .and(warp::query::<api_types::BroadcastValidationQuery>())
        .and(warp::path::end())
        .and(warp::header::<ForkName>(CONSENSUS_VERSION_HEADER))
        .and(warp::body::bytes())
        .and(not_while_syncing_filter)
        .and(task_spawner_filter)
        .and(chain_filter)
        .and(network_tx_filter)
        .then(
            |validation_level: api_types::BroadcastValidationQuery,
             fork_name: ForkName,
             body_bytes: Bytes,
             not_synced_filter: Result<(), Rejection>,
             task_spawner: TaskSpawner<T::EthSpec>,
             chain: Arc<BeaconChain<T>>,
             network_tx: UnboundedSender<NetworkMessage<T::EthSpec>>| {
                task_spawner.spawn_async_with_rejection(Priority::P0, async move {
                    not_synced_filter?;
                    ensure_gloas_consensus_version(fork_name)?;
                    let submission = EnvelopeSubmission::from_ssz_bytes(&body_bytes)?;
                    publish_execution_payload_envelope(
                        submission,
                        validation_level.broadcast_validation,
                        chain,
                        &network_tx,
                    )
                    .await
                })
            },
        )
        .boxed()
}

// POST beacon/execution_payload_envelopes
pub(crate) fn post_beacon_execution_payload_envelopes<T: BeaconChainTypes>(
    eth_v1: EthV1Filter,
    task_spawner_filter: TaskSpawnerFilter<T>,
    chain_filter: ChainFilter<T>,
    network_tx_filter: NetworkTxFilter<T>,
    not_while_syncing_filter: NotWhileSyncingFilter,
) -> ResponseFilter {
    eth_v1
        .and(warp::path("beacon"))
        .and(warp::path("execution_payload_envelopes"))
        .and(warp::query::<api_types::BroadcastValidationQuery>())
        .and(warp::path::end())
        .and(warp::header::<ForkName>(CONSENSUS_VERSION_HEADER))
        .and(warp::body::bytes())
        .and(not_while_syncing_filter)
        .and(task_spawner_filter)
        .and(chain_filter)
        .and(network_tx_filter)
        .then(
            |validation_level: api_types::BroadcastValidationQuery,
             fork_name: ForkName,
             body_bytes: Bytes,
             not_synced_filter: Result<(), Rejection>,
             task_spawner: TaskSpawner<T::EthSpec>,
             chain: Arc<BeaconChain<T>>,
             network_tx: UnboundedSender<NetworkMessage<T::EthSpec>>| {
                task_spawner.spawn_async_with_rejection(Priority::P0, async move {
                    not_synced_filter?;
                    ensure_gloas_consensus_version(fork_name)?;
                    let submission = EnvelopeSubmission::from_json(&body_bytes)?;
                    publish_execution_payload_envelope(
                        submission,
                        validation_level.broadcast_validation,
                        chain,
                        &network_tx,
                    )
                    .await
                })
            },
        )
        .boxed()
}

/// Publishes an execution payload envelope to the network. Implements
/// `POST /eth/v1/beacon/execution_payload_envelopes` per the in-flight beacon-APIs PRs
/// <https://github.com/ethereum/beacon-APIs/pull/580>,
/// <https://github.com/ethereum/beacon-APIs/pull/624> and
/// <https://github.com/ethereum/beacon-APIs/pull/677>.
pub async fn publish_execution_payload_envelope<T: BeaconChainTypes>(
    submission: EnvelopeSubmission<T::EthSpec>,
    validation_level: BroadcastValidation,
    chain: Arc<BeaconChain<T>>,
    network_tx: &UnboundedSender<NetworkMessage<T::EthSpec>>,
) -> Result<Response, Rejection> {
    if !chain.spec.is_gloas_scheduled() {
        return Err(warp_utils::reject::custom_bad_request(
            "Execution payload envelopes are not supported before the Gloas fork".into(),
        ));
    }

    let includes_blob_data = matches!(&submission, EnvelopeSubmission::EnvelopeAndBlobData(_));
    let (envelope, blobs_and_proofs) = match submission {
        EnvelopeSubmission::EnvelopeAndBlobData(contents) => {
            let ExecutionPayloadEnvelopeContents {
                execution_payload_envelope: envelope,
                kzg_proofs,
                blobs,
            } = *contents;
            let expected_proofs = blobs.len() * T::EthSpec::number_of_columns();
            if kzg_proofs.len() != expected_proofs {
                return Err(warp_utils::reject::custom_bad_request(format!(
                    "invalid number of kzg proofs: expected {}, got {}",
                    expected_proofs,
                    kzg_proofs.len()
                )));
            }
            (
                Arc::new(envelope),
                Some((Arc::new(blobs), Some(kzg_proofs))),
            )
        }
        EnvelopeSubmission::EnvelopeOnly(envelope) => {
            let blobs = chain
                .pending_payload_envelopes
                .write()
                .take_blobs(envelope.beacon_block_root);
            (envelope, blobs.map(|blobs| (blobs, None)))
        }
    };

    let slot = envelope.slot();
    let beacon_block_root = envelope.beacon_block_root;

    info!(
        %slot,
        %beacon_block_root,
        "Publishing execution payload envelope to network"
    );

    // Spawn the column-build task (CPU-bound KZG cell-and-proof computation) before
    // publishing the envelope so it runs in parallel with envelope gossip, narrowing
    // the window in which peers see envelope-without-columns. If envelope import
    // fails below, dropping this future drops the spawned `JoinHandle` (the running
    // closure on the blocking pool finishes and is then discarded — no work cancellation).
    let column_build_future = match blobs_and_proofs {
        Some((blobs, cell_proofs)) if !blobs.is_empty() => {
            Some(spawn_build_gloas_data_columns_task(
                &chain,
                beacon_block_root,
                slot,
                blobs,
                cell_proofs,
            )?)
        }
        _ => None,
    };

    // Gossip-verify the envelope before publishing.
    let gossip_verified = chain
        .verify_envelope_for_gossip(envelope, EnvelopeSource::Http)
        .await
        .map_err(|e| {
            warn!(%slot, error = ?e, "Execution payload envelope failed gossip verification");
            warp_utils::reject::custom_bad_request(format!(
                "envelope failed gossip verification: {e}"
            ))
        })?;

    // Reject stateful submissions before broadcast when the block commits to blobs but the
    // node has none cached.
    if !includes_blob_data && column_build_future.is_none() {
        let commits_to_blobs = gossip_verified
            .block
            .message()
            .body()
            .signed_execution_payload_bid()
            .is_ok_and(|bid| !bid.message().blob_kzg_commitments().is_empty());
        if commits_to_blobs {
            return Err(warp_utils::reject::custom_bad_request(
                "block commits to blobs but the beacon node has no cached blobs and KZG proofs to attach"
                    .into(),
            ));
        }
    }

    let network_tx_clone = network_tx.clone();
    let block_for_equivocation_check = gossip_verified.block.clone();
    let envelope_for_gossip = gossip_verified.envelope.clone();
    let publish_envelope = || {
        crate::utils::publish_pubsub_message(
            &network_tx_clone,
            PubsubMessage::ExecutionPayload(Box::new(envelope_for_gossip.as_ref().clone())),
        )
        .map_err(|_| {
            EnvelopeError::BeaconChainError(Box::new(
                beacon_chain::BeaconChainError::UnableToPublish,
            ))
        })
    };

    let published = AtomicBool::new(false);
    if validation_level == BroadcastValidation::Gossip {
        publish_envelope().map_err(|_| {
            warp_utils::reject::custom_server_error(
                "unable to publish to network channel".to_string(),
            )
        })?;
        published.store(true, Ordering::SeqCst);
    }

    let publish_fn = || {
        match validation_level {
            BroadcastValidation::Gossip => return Ok(()),
            BroadcastValidation::Consensus => {}
            BroadcastValidation::ConsensusAndEquivocation => {
                check_slashable(&chain, beacon_block_root, &block_for_equivocation_check).map_err(
                    |e| match e {
                        BlockError::BeaconChainError(e) => EnvelopeError::BeaconChainError(e),
                        e => EnvelopeError::InternalError(format!("{e:?}")),
                    },
                )?;
            }
        }

        publish_envelope()?;
        published.store(true, Ordering::SeqCst);
        Ok(())
    };

    let import_result = chain
        .process_execution_payload_envelope(
            beacon_block_root,
            gossip_verified,
            NotifyExecutionLayer::Yes,
            BlockImportSource::HttpApi,
            publish_fn,
        )
        .await;

    let mut envelope_imported = match import_result {
        Ok(AvailabilityProcessingStatus::Imported(_, _)) => true,
        Ok(AvailabilityProcessingStatus::MissingComponents(_, _)) => false,
        Err(e) => {
            if is_unable_to_publish(&e) {
                return Err(warp_utils::reject::custom_server_error(
                    "unable to publish to network channel".to_string(),
                ));
            }

            if published.load(Ordering::SeqCst) {
                warn!(%slot, error = ?e, "Failed to import execution payload envelope after broadcast");
                return Err(warp_utils::reject::broadcast_without_import(format!(
                    "envelope import failed: {e:?}"
                )));
            }

            warn!(%slot, error = ?e, "Rejecting execution payload envelope before broadcast");
            return Err(warp_utils::reject::custom_bad_request(format!(
                "envelope rejected: {e:?}"
            )));
        }
    };

    // The envelope has already been published over gossip, only columns can fail from here.
    // `EnvelopeAndBlobData`: the caller still holds the blobs so we return an error. The caller
    // can retry against a different beacon node.
    // `EnvelopeOnly`: the cached blobs have been consumed, a retry cannot rebuild the columns. We
    // also cant retry against another beacon node. So we just log a failure and continue.
    if let Some(column_build_future) = column_build_future {
        match column_build_future.await {
            Ok(columns) => {
                match publish_and_import_columns(&chain, network_tx, slot, columns).await {
                    Ok(imported) => {
                        if imported {
                            envelope_imported = true;
                        }
                    }
                    Err(rejection) => {
                        if includes_blob_data {
                            return Err(rejection);
                        }
                    }
                }
            }
            Err(e) => {
                error!(
                    %slot,
                    error = ?e,
                    "Failed to build data columns after envelope publication"
                );
                if includes_blob_data {
                    return Err(e);
                }
            }
        }
    }

    // Return 202 when broadcast succeeds but integration does not. Unlike the block path,
    // 202 applies at every validation level: missing components here are node-cached columns
    // rather than request-supplied blobs, so incomplete import is not the submitter's fault.
    if envelope_imported {
        chain.recompute_head_at_current_slot().await;
        Ok(warp::reply().into_response())
    } else {
        Err(warp_utils::reject::broadcast_without_import(format!(
            "envelope for slot {slot} was broadcast but not fully imported"
        )))
    }
}

/// Broadcasts the execution payload envelope and blobs of a block which this node self-built,
/// if it holds them and has not broadcast them yet.
///
/// Per <https://github.com/ethereum/beacon-APIs/pull/677> the envelope is not signed, so the
/// beacon node broadcasts it without a further request from the validator client. This is called
/// once the block has been published through this node and imported; the broadcast runs in the
/// background and does not affect the response to the block publication.
pub fn publish_self_built_execution_payload_envelope<T: BeaconChainTypes>(
    chain: &Arc<BeaconChain<T>>,
    network_tx: &UnboundedSender<NetworkMessage<T::EthSpec>>,
    beacon_block_root: types::Hash256,
    block_slot: types::Slot,
) {
    // The blobs are taken from the cache when the envelope is published, so their presence
    // marks an envelope which is still to be broadcast.
    let envelope = {
        let pending_envelopes = chain.pending_payload_envelopes.read();
        if !pending_envelopes.has_blobs(beacon_block_root) {
            return;
        }
        let Some(envelope) = pending_envelopes.get_by_block_root(beacon_block_root) else {
            return;
        };
        envelope.clone()
    };

    // The envelope need not be broadcast once the slot of the block has passed.
    if chain.slot().ok() != Some(block_slot) {
        debug!(
            %beacon_block_root,
            %block_slot,
            "Not broadcasting self-built execution payload envelope after its slot"
        );
        return;
    }

    let inner_chain = chain.clone();
    let network_tx = network_tx.clone();
    chain.task_executor.spawn(
        async move {
            // Only the gossip validation of the envelope is applied before it is broadcast.
            match publish_execution_payload_envelope(
                EnvelopeSubmission::EnvelopeOnly(envelope),
                BroadcastValidation::Gossip,
                inner_chain,
                &network_tx,
            )
            .await
            {
                Ok(_) => debug!(
                    %beacon_block_root,
                    %block_slot,
                    "Broadcast self-built execution payload envelope"
                ),
                Err(e) => warn!(
                    %beacon_block_root,
                    %block_slot,
                    error = ?e,
                    "Failed to broadcast self-built execution payload envelope"
                ),
            }
        },
        "publish_self_built_envelope",
    );
}

/// The data columns built from an envelope's blobs.
struct BuiltDataColumns<T: BeaconChainTypes> {
    /// Columns with cells we have not seen yet, to publish and import.
    verified: Vec<GossipVerifiedDataColumn<T>>,
    /// Columns already complete in the pending payload cache but not published yet, e.g. ones
    /// retrieved from the EL that await their gradual publication. Building them marked them as
    /// seen, which makes that publication skip them, so they must be published with the others.
    unpublished: Vec<Arc<DataColumnSidecar<T::EthSpec>>>,
}

/// Publishes locally-built data columns and imports the node's sampling subset of the verified
/// ones.
///
/// Returns `Ok(true)` if envelope import was completed.
/// Returns `Err` on publication failure.
async fn publish_and_import_columns<T: BeaconChainTypes>(
    chain: &Arc<BeaconChain<T>>,
    network_tx: &UnboundedSender<NetworkMessage<T::EthSpec>>,
    slot: types::Slot,
    columns: BuiltDataColumns<T>,
) -> Result<bool, Rejection> {
    let BuiltDataColumns {
        verified: gossip_verified_columns,
        unpublished,
    } = columns;
    if gossip_verified_columns.is_empty() && unpublished.is_empty() {
        return Ok(false);
    }

    let columns_to_publish = gossip_verified_columns
        .iter()
        .map(|column| column.clone_data_column())
        .chain(unpublished);
    if let Err(e) = publish_column_sidecars(network_tx, columns_to_publish, chain) {
        error!(
            %slot,
            error = ?e,
            "Failed to publish data column sidecars after envelope publication"
        );
        return Err(warp_utils::reject::custom_server_error(format!(
            "failed to publish data column sidecars: {e:?}"
        )));
    }

    let epoch = slot.epoch(T::EthSpec::slots_per_epoch());
    let sampling_column_indices = chain.custody_context.sampling_columns_for_epoch(epoch);
    let sampling_columns = gossip_verified_columns
        .into_iter()
        .filter(|col| sampling_column_indices.contains(&col.index()))
        .collect::<Vec<_>>();

    if sampling_columns.is_empty() {
        return Ok(false);
    }

    match Box::pin(chain.process_gossip_data_columns(sampling_columns, || Ok(()))).await {
        Ok(AvailabilityProcessingStatus::Imported(_, _)) => Ok(true),
        Ok(AvailabilityProcessingStatus::MissingComponents(_, _)) => Ok(false),
        Err(e) => {
            error!(
                %slot,
                error = ?e,
                "Failed to process sampling data columns during envelope publication"
            );
            Ok(false)
        }
    }
}

fn is_unable_to_publish(error: &BlockError) -> bool {
    match error {
        BlockError::BeaconChainError(error) => {
            matches!(error.as_ref(), BeaconChainError::UnableToPublish)
        }
        BlockError::EnvelopeError(error) => matches!(
            error.as_ref(),
            EnvelopeError::BeaconChainError(error)
                if matches!(error.as_ref(), BeaconChainError::UnableToPublish)
        ),
        _ => false,
    }
}

fn spawn_build_gloas_data_columns_task<T: BeaconChainTypes>(
    chain: &Arc<BeaconChain<T>>,
    beacon_block_root: types::Hash256,
    slot: types::Slot,
    blobs: Arc<types::BlobsList<T::EthSpec>>,
    cell_proofs: Option<KzgProofs<T::EthSpec>>,
) -> Result<impl Future<Output = Result<BuiltDataColumns<T>, Rejection>>, Rejection> {
    let chain_for_build = chain.clone();
    let handle = chain
        .task_executor
        .spawn_blocking_handle(
            move || {
                build_gloas_data_columns(
                    &chain_for_build,
                    beacon_block_root,
                    slot,
                    &blobs,
                    cell_proofs,
                )
            },
            "build_gloas_data_columns",
        )
        .ok_or_else(|| warp_utils::reject::custom_server_error("runtime shutdown".to_string()))?;

    Ok(async move {
        handle
            .await
            .map_err(|_| warp_utils::reject::custom_server_error("join error".to_string()))?
    })
}

fn build_gloas_data_columns<T: BeaconChainTypes>(
    chain: &BeaconChain<T>,
    beacon_block_root: types::Hash256,
    slot: types::Slot,
    blobs: &types::BlobsList<T::EthSpec>,
    cell_proofs: Option<KzgProofs<T::EthSpec>>,
) -> Result<BuiltDataColumns<T>, Rejection> {
    let blob_refs: Vec<_> = blobs.iter().collect();
    let data_column_sidecars = match cell_proofs {
        Some(proofs) => beacon_chain::kzg_utils::blobs_to_data_column_sidecars_gloas_with_proofs(
            &blob_refs,
            proofs.to_vec(),
            beacon_block_root,
            slot,
            &chain.kzg,
            &chain.spec,
        ),
        None => beacon_chain::kzg_utils::blobs_to_data_column_sidecars_gloas(
            &blob_refs,
            beacon_block_root,
            slot,
            &chain.kzg,
            &chain.spec,
        ),
    }
    .map_err(|e| {
        error!(
            error = ?e,
            %slot,
            "Failed to build data column sidecars for envelope"
        );
        warp_utils::reject::custom_server_error(format!("{e:?}"))
    })?;

    let mut verified = Vec::new();
    let mut unpublished = Vec::new();
    for col in data_column_sidecars {
        let index = *col.index();
        match GossipVerifiedDataColumn::new_for_block_publishing(col.clone(), chain) {
            Ok(column) => verified.push(column),
            Err(GossipDataColumnError::PriorKnown { .. }) => {}
            Err(GossipDataColumnError::PriorKnownUnpublished) => unpublished.push(col),
            Err(e) => {
                warn!(
                    %slot,
                    column_index = index,
                    error = ?e,
                    "Locally-built data column failed gossip verification"
                );
            }
        }
    }

    debug!(
        %slot,
        column_count = verified.len(),
        unpublished_count = unpublished.len(),
        "Built data columns for envelope publication"
    );

    Ok(BuiltDataColumns {
        verified,
        unpublished,
    })
}

// GET beacon/execution_payload_envelopes/{block_id}
pub(crate) fn get_beacon_execution_payload_envelopes<T: BeaconChainTypes>(
    eth_v1: EthV1Filter,
    block_id_or_err: impl Filter<Extract = (BlockId,), Error = Rejection>
    + Clone
    + Send
    + Sync
    + 'static,
    task_spawner_filter: TaskSpawnerFilter<T>,
    chain_filter: ChainFilter<T>,
) -> ResponseFilter {
    eth_v1
        .and(warp::path("beacon"))
        .and(warp::path("execution_payload_envelopes"))
        .and(block_id_or_err)
        .and(warp::path::end())
        .and(task_spawner_filter)
        .and(chain_filter)
        .and(warp::header::optional::<api_types::Accept>("accept"))
        .then(
            |block_id: BlockId,
             task_spawner: TaskSpawner<T::EthSpec>,
             chain: Arc<BeaconChain<T>>,
             accept_header: Option<api_types::Accept>| {
                task_spawner.spawn_async_with_rejection(Priority::P1, async move {
                    let (root, execution_optimistic, finalized) = block_id.root(&chain)?;

                    let mut envelope_stream =
                        chain.get_payload_envelopes(vec![root], EnvelopeRequestSource::ByRoot);
                    let (_, result) = envelope_stream.next().await.ok_or_else(|| {
                        warp_utils::reject::custom_server_error(format!(
                            "payload envelope stream ended before returning block root {root}"
                        ))
                    })?;
                    let envelope = match result.as_ref() {
                        Ok(Some(envelope)) => envelope.clone(),
                        Ok(None) => {
                            return Err(warp_utils::reject::custom_not_found(format!(
                                "execution payload envelope for block root {root}"
                            )));
                        }
                        Err(error) => {
                            return Err(warp_utils::reject::custom_server_error(format!(
                                "failed to load execution payload envelope for block root \
                                 {root}: {error:?}"
                            )));
                        }
                    };

                    let fork_name = chain.spec.fork_name_at_slot::<T::EthSpec>(envelope.slot());

                    match accept_header {
                        Some(api_types::Accept::Ssz) => Builder::new()
                            .status(200)
                            .body(envelope.as_ssz_bytes())
                            .map(add_ssz_content_type_header)
                            .map_err(|e| {
                                warp_utils::reject::custom_server_error(format!(
                                    "failed to create response: {}",
                                    e
                                ))
                            }),
                        _ => {
                            let res = execution_optimistic_finalized_beacon_response(
                                ResponseIncludesVersion::Yes(fork_name),
                                execution_optimistic,
                                finalized,
                                &envelope,
                            )?;
                            Ok(warp::reply::json(&res).into_response())
                        }
                    }
                    .map(|resp| add_consensus_version_header(resp, fork_name))
                })
            },
        )
        .boxed()
}
