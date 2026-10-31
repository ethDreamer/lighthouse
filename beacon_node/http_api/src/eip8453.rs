//! EIP-8453 validator duty and vote endpoints (prototype).
//!
//! Non-standard endpoints: the Beacon API has no EIP-8453 surface yet, so these are JSON-only
//! prototypes under `/eth/v1/validator` and `/eth/v1/beacon/pool`, modeled on the PTC duty and
//! payload-attestation endpoints.

use std::sync::Arc;

use beacon_chain::{BeaconChain, BeaconChainTypes};
use eth2::types::{self as api_types, GenericResponse, ValidatorIndexData};
use serde::Deserialize;
use types::eip8453::{HeadVoteMessage, SafeVoteMessage};
use types::{Epoch, Slot, eip8453};
use warp::{Filter, Rejection};

use crate::task_spawner::{Priority, TaskSpawner};
use crate::utils::{
    ChainFilter, EthV1Filter, NotWhileSyncingFilter, ResponseFilter, TaskSpawnerFilter,
};

#[derive(Deserialize)]
struct HeadVoteDataQuery {
    slot: Slot,
}

fn check_eip8453_enabled<T: BeaconChainTypes>(
    chain: &BeaconChain<T>,
    epoch: Epoch,
) -> Result<(), Rejection> {
    if !chain.spec.fork_name_at_epoch(epoch).eip8453_enabled() {
        return Err(warp_utils::reject::custom_bad_request(format!(
            "EIP-8453 is not enabled at epoch {epoch}"
        )));
    }
    Ok(())
}

// POST validator/duties/fg_attester/{epoch}
pub fn post_validator_duties_fg_attester<T: BeaconChainTypes>(
    eth_v1: EthV1Filter,
    chain_filter: ChainFilter<T>,
    not_while_syncing_filter: NotWhileSyncingFilter,
    task_spawner_filter: TaskSpawnerFilter<T>,
) -> ResponseFilter {
    eth_v1
        .and(warp::path("validator"))
        .and(warp::path("duties"))
        .and(warp::path("fg_attester"))
        .and(warp::path::param::<Epoch>().or_else(|_| async {
            Err(warp_utils::reject::custom_bad_request(
                "Invalid epoch".to_string(),
            ))
        }))
        .and(warp::path::end())
        .and(not_while_syncing_filter)
        .and(warp_utils::json::json())
        .and(task_spawner_filter)
        .and(chain_filter)
        .then(
            |epoch: Epoch,
             not_synced_filter: Result<(), Rejection>,
             indices: ValidatorIndexData,
             task_spawner: TaskSpawner<T::EthSpec>,
             chain: Arc<BeaconChain<T>>| {
                task_spawner.blocking_json_task(Priority::P0, move || {
                    not_synced_filter?;
                    check_eip8453_enabled(&chain, epoch)?;
                    let (duties, dependent_root) = chain
                        .eip8453_fg_attester_duties(epoch, &indices.0)
                        .map_err(warp_utils::reject::unhandled_error)?;
                    Ok(api_types::DutiesResponse {
                        dependent_root,
                        execution_optimistic: Some(false),
                        data: duties,
                    })
                })
            },
        )
        .boxed()
}

// POST validator/duties/head_vote/{epoch}
pub fn post_validator_duties_head_vote<T: BeaconChainTypes>(
    eth_v1: EthV1Filter,
    chain_filter: ChainFilter<T>,
    not_while_syncing_filter: NotWhileSyncingFilter,
    task_spawner_filter: TaskSpawnerFilter<T>,
) -> ResponseFilter {
    eth_v1
        .and(warp::path("validator"))
        .and(warp::path("duties"))
        .and(warp::path("head_vote"))
        .and(warp::path::param::<Epoch>().or_else(|_| async {
            Err(warp_utils::reject::custom_bad_request(
                "Invalid epoch".to_string(),
            ))
        }))
        .and(warp::path::end())
        .and(not_while_syncing_filter)
        .and(warp_utils::json::json())
        .and(task_spawner_filter)
        .and(chain_filter)
        .then(
            |epoch: Epoch,
             not_synced_filter: Result<(), Rejection>,
             indices: ValidatorIndexData,
             task_spawner: TaskSpawner<T::EthSpec>,
             chain: Arc<BeaconChain<T>>| {
                task_spawner.blocking_json_task(Priority::P0, move || {
                    not_synced_filter?;
                    check_eip8453_enabled(&chain, epoch)?;
                    let (duties, dependent_root) = chain
                        .eip8453_head_vote_duties(epoch, &indices.0)
                        .map_err(warp_utils::reject::unhandled_error)?;
                    Ok(api_types::DutiesResponse {
                        dependent_root,
                        execution_optimistic: Some(false),
                        data: duties,
                    })
                })
            },
        )
        .boxed()
}

// GET validator/eip8453_head_vote_data?slot
pub fn get_validator_eip8453_head_vote_data<T: BeaconChainTypes>(
    eth_v1: EthV1Filter,
    chain_filter: ChainFilter<T>,
    not_while_syncing_filter: NotWhileSyncingFilter,
    task_spawner_filter: TaskSpawnerFilter<T>,
) -> ResponseFilter {
    eth_v1
        .and(warp::path("validator"))
        .and(warp::path("eip8453_head_vote_data"))
        .and(warp::path::end())
        .and(warp::query::<HeadVoteDataQuery>())
        .and(not_while_syncing_filter)
        .and(task_spawner_filter)
        .and(chain_filter)
        .then(
            |query: HeadVoteDataQuery,
             not_synced_filter: Result<(), Rejection>,
             task_spawner: TaskSpawner<T::EthSpec>,
             chain: Arc<BeaconChain<T>>| {
                task_spawner.blocking_json_task(Priority::P0, move || {
                    not_synced_filter?;
                    let data = chain
                        .eip8453_produce_head_vote_data(query.slot)
                        .map_err(warp_utils::reject::unhandled_error)?;
                    Ok(GenericResponse::from(data))
                })
            },
        )
        .boxed()
}

// GET validator/eip8453_safe_vote_data
pub fn get_validator_eip8453_safe_vote_data<T: BeaconChainTypes>(
    eth_v1: EthV1Filter,
    chain_filter: ChainFilter<T>,
    not_while_syncing_filter: NotWhileSyncingFilter,
    task_spawner_filter: TaskSpawnerFilter<T>,
) -> ResponseFilter {
    eth_v1
        .and(warp::path("validator"))
        .and(warp::path("eip8453_safe_vote_data"))
        .and(warp::path::end())
        .and(not_while_syncing_filter)
        .and(task_spawner_filter)
        .and(chain_filter)
        .then(
            |not_synced_filter: Result<(), Rejection>,
             task_spawner: TaskSpawner<T::EthSpec>,
             chain: Arc<BeaconChain<T>>| {
                task_spawner.blocking_json_task(Priority::P0, move || {
                    not_synced_filter?;
                    let data = chain
                        .eip8453_produce_safe_vote_data()
                        .map_err(warp_utils::reject::unhandled_error)?;
                    Ok(GenericResponse::from(data))
                })
            },
        )
        .boxed()
}

// GET validator/eip8453_fg_attestation_components
pub fn get_validator_eip8453_fg_attestation_components<T: BeaconChainTypes>(
    eth_v1: EthV1Filter,
    chain_filter: ChainFilter<T>,
    not_while_syncing_filter: NotWhileSyncingFilter,
    task_spawner_filter: TaskSpawnerFilter<T>,
) -> ResponseFilter {
    eth_v1
        .and(warp::path("validator"))
        .and(warp::path("eip8453_fg_attestation_components"))
        .and(warp::path::end())
        .and(not_while_syncing_filter)
        .and(task_spawner_filter)
        .and(chain_filter)
        .then(
            |not_synced_filter: Result<(), Rejection>,
             task_spawner: TaskSpawner<T::EthSpec>,
             chain: Arc<BeaconChain<T>>| {
                task_spawner.blocking_json_task(Priority::P0, move || {
                    not_synced_filter?;
                    let components = chain
                        .eip8453_produce_fg_attestation_components()
                        .map_err(warp_utils::reject::unhandled_error)?;
                    Ok(GenericResponse::from(components))
                })
            },
        )
        .boxed()
}

// POST beacon/pool/eip8453_head_votes
pub fn post_beacon_pool_eip8453_head_votes<T: BeaconChainTypes>(
    eth_v1: EthV1Filter,
    chain_filter: ChainFilter<T>,
    task_spawner_filter: TaskSpawnerFilter<T>,
) -> ResponseFilter {
    eth_v1
        .and(warp::path("beacon"))
        .and(warp::path("pool"))
        .and(warp::path("eip8453_head_votes"))
        .and(warp::path::end())
        .and(warp_utils::json::json())
        .and(task_spawner_filter)
        .and(chain_filter)
        .then(
            |messages: Vec<HeadVoteMessage>,
             task_spawner: TaskSpawner<T::EthSpec>,
             chain: Arc<BeaconChain<T>>| {
                task_spawner.blocking_json_task(Priority::P0, move || {
                    for message in messages {
                        chain
                            .eip8453_process_head_vote(message)
                            .map_err(warp_utils::reject::unhandled_error)?;
                    }
                    Ok(())
                })
            },
        )
        .boxed()
}

// POST beacon/pool/eip8453_safe_votes
pub fn post_beacon_pool_eip8453_safe_votes<T: BeaconChainTypes>(
    eth_v1: EthV1Filter,
    chain_filter: ChainFilter<T>,
    task_spawner_filter: TaskSpawnerFilter<T>,
) -> ResponseFilter {
    eth_v1
        .and(warp::path("beacon"))
        .and(warp::path("pool"))
        .and(warp::path("eip8453_safe_votes"))
        .and(warp::path::end())
        .and(warp_utils::json::json())
        .and(task_spawner_filter)
        .and(chain_filter)
        .then(
            |messages: Vec<SafeVoteMessage>,
             task_spawner: TaskSpawner<T::EthSpec>,
             chain: Arc<BeaconChain<T>>| {
                task_spawner.blocking_json_task(Priority::P0, move || {
                    for message in messages {
                        chain
                            .eip8453_process_safe_vote(message)
                            .map_err(warp_utils::reject::unhandled_error)?;
                    }
                    Ok(())
                })
            },
        )
        .boxed()
}

// POST beacon/pool/eip8453_fg_attestations
pub fn post_beacon_pool_eip8453_fg_attestations<T: BeaconChainTypes>(
    eth_v1: EthV1Filter,
    chain_filter: ChainFilter<T>,
    task_spawner_filter: TaskSpawnerFilter<T>,
) -> ResponseFilter {
    eth_v1
        .and(warp::path("beacon"))
        .and(warp::path("pool"))
        .and(warp::path("eip8453_fg_attestations"))
        .and(warp::path::end())
        .and(warp_utils::json::json())
        .and(task_spawner_filter)
        .and(chain_filter)
        .then(
            |attestations: Vec<eip8453::SingleAttestation>,
             task_spawner: TaskSpawner<T::EthSpec>,
             chain: Arc<BeaconChain<T>>| {
                task_spawner.blocking_json_task(Priority::P0, move || {
                    for attestation in attestations {
                        chain
                            .eip8453_process_fg_attestation(attestation)
                            .map_err(warp_utils::reject::unhandled_error)?;
                    }
                    Ok(())
                })
            },
        )
        .boxed()
}
