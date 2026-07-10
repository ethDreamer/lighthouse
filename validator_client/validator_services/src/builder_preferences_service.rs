use crate::duties_service::DutiesService;
use crate::request_auth_cache::RequestAuthCache;
use beacon_node_fallback::BeaconNodeFallback;
use bls::PublicKeyBytes;
use builder_store::BuilderStore;
use eth2::types::SubmitBuilderPreferencesRequest;
use slot_clock::SlotClock;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use task_executor::TaskExecutor;
use tokio::time::sleep;
use tracing::{error, info};
use types::{BuilderPreferencesRequestV1, BuilderPreferencesV1, RequestAuthV1};
use types::{ChainSpec, EthSpec, Slot, builder::RequestAuthUrl};
use validator_store::ValidatorStore;

// TODO: if batching becomes real when the beacon api is finalized, consider
// making this a cli flag similar to validator_registration_batch_size
const BUILDER_PREFERENCES_BATCH_SIZE: usize = 64;

#[derive(PartialEq, Eq, Hash)]
struct InnerPreferencesKey {
    pubkey: PublicKeyBytes,
    builder_url: RequestAuthUrl,
    max_execution_payment: u64,
}

impl From<&SubmitBuilderPreferencesRequest> for InnerPreferencesKey {
    fn from(request: &SubmitBuilderPreferencesRequest) -> Self {
        Self {
            pubkey: request.pubkey,
            builder_url: request.preferences.auth().message.data.clone(),
            max_execution_payment: request.preferences.preferences().max_execution_payment,
        }
    }
}

#[derive(Default)]
struct PublishedBuilderPreferencesCache {
    entries: BTreeMap<Slot, HashSet<InnerPreferencesKey>>,
}

impl PublishedBuilderPreferencesCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn contains(
        &self,
        slot: Slot,
        pubkey: PublicKeyBytes,
        builder_url: RequestAuthUrl,
        max_execution_payment: u64,
    ) -> bool {
        self.entries.get(&slot).is_some_and(|set| {
            set.contains(&InnerPreferencesKey {
                pubkey,
                builder_url,
                max_execution_payment,
            })
        })
    }

    pub fn mark_sent(&mut self, request: &SubmitBuilderPreferencesRequest) {
        let slot = request.preferences.auth().message.slot;
        self.entries.entry(slot).or_default().insert(request.into());
    }

    pub fn prune(&mut self, current_slot: Slot) {
        self.entries = self.entries.split_off(&current_slot);
    }
}

// Minimizes `Arc` usage
struct Inner<S, T> {
    duties_service: Arc<DutiesService<S, T>>,
    validator_store: Arc<S>,
    slot_clock: T,
    beacon_nodes: Arc<BeaconNodeFallback<T>>,
    configured_builders: BuilderStore,
    request_auth_cache: RequestAuthCache,
    executor: TaskExecutor,
    chain_spec: Arc<ChainSpec>,
}

pub struct BuilderPreferencesService<S, T> {
    inner: Arc<Inner<S, T>>,
}

// Generic clone implementation is too dumb to do this
impl<S, T> Clone for BuilderPreferencesService<S, T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<S: ValidatorStore + 'static, T: SlotClock + 'static> BuilderPreferencesService<S, T> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        duties_service: Arc<DutiesService<S, T>>,
        validator_store: Arc<S>,
        slot_clock: T,
        beacon_nodes: Arc<BeaconNodeFallback<T>>,
        configured_builders: BuilderStore,
        request_auth_cache: RequestAuthCache,
        executor: TaskExecutor,
        chain_spec: Arc<ChainSpec>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                duties_service,
                validator_store,
                slot_clock,
                beacon_nodes,
                configured_builders,
                request_auth_cache,
                executor,
                chain_spec,
            }),
        }
    }

    pub fn start_update_service(self) -> Result<(), String> {
        let slot_duration = self.inner.chain_spec.get_slot_duration();
        info!("Builder preferences service started");

        let executor = self.inner.executor.clone();

        let interval_fut = async move {
            let mut published_preferences = PublishedBuilderPreferencesCache::new();

            loop {
                let Some(current_slot) = self.inner.slot_clock.now() else {
                    error!("Failed to read slot clock");
                    sleep(slot_duration).await;
                    continue;
                };

                self.poll_and_publish_preferences(current_slot, &mut published_preferences)
                    .await;

                published_preferences.prune(current_slot);
                self.inner.request_auth_cache.prune(current_slot);

                let duration_to_next_slot = self
                    .inner
                    .slot_clock
                    .duration_to_next_slot()
                    .unwrap_or(slot_duration);
                sleep(duration_to_next_slot).await;
            }
        };

        executor.spawn(interval_fut, "builder_preferences_service");
        Ok(())
    }

    /// Publish builder preferences for `current_epoch` and `current_epoch + 1`.
    /// Will only publish preferences for a given epoch once per dependent root.
    async fn poll_and_publish_preferences(
        &self,
        current_slot: Slot,
        published_preferences: &mut PublishedBuilderPreferencesCache,
    ) {
        let current_epoch = current_slot.epoch(S::E::slots_per_epoch());
        let mut unsent_requests = Vec::new();

        for (epoch, fork_name) in [
            (
                current_epoch,
                self.inner.chain_spec.fork_name_at_epoch(current_epoch),
            ),
            (
                current_epoch + 1,
                self.inner.chain_spec.fork_name_at_epoch(current_epoch + 1),
            ),
        ] {
            if !fork_name.gloas_enabled() {
                continue;
            }

            let proposers = match self.inner.duties_service.proposers.read().get(&epoch) {
                Some((_, proposers)) => proposers.clone(),
                None => continue,
            };

            for proposer_data in &proposers {
                for (builder_url, max_execution_payment) in
                    self.inner.configured_builders.enabled_builders()
                {
                    if published_preferences.contains(
                        proposer_data.slot,
                        proposer_data.pubkey,
                        builder_url.clone(),
                        max_execution_payment,
                    ) {
                        // already published, skip
                        continue;
                    }

                    match self
                        .get_submit_builder_preferences_request(
                            proposer_data.slot,
                            proposer_data.pubkey,
                            builder_url,
                            max_execution_payment,
                        )
                        .await
                    {
                        Ok(builder_preferences) => {
                            unsent_requests.push(builder_preferences);
                        }
                        Err(e) => {
                            error!(
                                error = ?e,
                                validator = ?proposer_data.pubkey,
                                "Failed to sign builder preferences"
                            );
                        }
                    }
                }
            }
        }

        for batch in unsent_requests.chunks(BUILDER_PREFERENCES_BATCH_SIZE) {
            match self
                .inner
                .beacon_nodes
                // first success is okay here because later we'll be
                // resending the auths when we publish the beacon block
                .first_success(|beacon_node| async move {
                    beacon_node.post_validator_builder_preferences(batch).await
                })
                .await
            {
                Ok(()) => {
                    for request in batch {
                        published_preferences.mark_sent(request);
                    }
                }
                Err(e) => error!(error = %e, "Failed to publish builder preferences"),
            }
        }
    }

    async fn get_submit_builder_preferences_request(
        &self,
        slot: Slot,
        pubkey: PublicKeyBytes,
        builder_url: RequestAuthUrl,
        max_execution_payment: u64,
    ) -> Result<SubmitBuilderPreferencesRequest, validator_store::Error<S::Error>> {
        let signed_auth = if let Some(signed_auth) =
            self.inner
                .request_auth_cache
                .get(slot, pubkey, &builder_url)
        {
            signed_auth
        } else {
            let request_auth_v1 = RequestAuthV1 {
                data: builder_url.clone(),
                slot,
            };
            let signed_request_auth = self
                .inner
                .validator_store
                .sign_request_auth_v1(pubkey, request_auth_v1)
                .await?;
            self.inner.request_auth_cache.insert(
                slot,
                pubkey,
                builder_url,
                signed_request_auth.clone(),
            );
            signed_request_auth
        };

        let preferences = BuilderPreferencesRequestV1::new(
            BuilderPreferencesV1 {
                max_execution_payment,
            },
            signed_auth,
        );

        Ok(SubmitBuilderPreferencesRequest {
            preferences,
            pubkey,
        })
    }
}
