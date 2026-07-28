use crate::duties_service::DutiesService;
use crate::request_auth_cache::RequestAuthCache;
use beacon_node_fallback::BeaconNodeFallback;
use bls::PublicKeyBytes;
use builder_store::{BuilderStore, DirectBuilder};
use eth2::types::BuilderPreferenceEntryV1;
use slot_clock::SlotClock;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use task_executor::TaskExecutor;
use tokio::time::sleep;
use tracing::{error, info};
use types::builder::BuilderUrl;
use types::{ChainSpec, EthSpec, RequestAuthData, RequestAuthV1, Slot};
use validator_store::ValidatorStore;

/// The non-slot part of a published entry's identity: the proposer pubkey plus the decomposed
/// `BuilderPreferenceEntryV1` with its `slot` factored out to the enclosing map's key.
/// - `pubkey`: the proposer the entry was submitted for
/// - `url`: `entry.url`
/// - `auth_data`: `entry.auth.message.data`
/// - `max_execution_payment`: `entry.max_execution_payment`
///
/// See [`PublishedBuilderPreferencesCache`] for how `entry.auth` decomposes into `auth_data` here
/// and `slot` at the map level, and why the `auth` signature is dropped.
#[derive(PartialEq, Eq, Hash)]
struct InnerPreferencesKey {
    pubkey: PublicKeyBytes,
    url: BuilderUrl,
    auth_data: RequestAuthData,
    max_execution_payment: u64,
}

/// De-duplicates the `BuilderPreferenceEntryV1`s we've already published, so we don't re-send one.
///
/// The identity of a published entry is `(proposer_pubkey, decompose(entry))`. That decomposition is
/// split across the two levels of this map:
/// - `entry.auth.message.slot` becomes the outer `BTreeMap<Slot, _>` key;
/// - the rest — `proposer_pubkey`, `entry.url`, `entry.auth.message.data`, and
///   `entry.max_execution_payment` — forms the [`InnerPreferencesKey`] held in the per-slot set.
///
/// So `entry.auth` decomposes into its `slot` (the map key) and its `data`/`auth_data` (in the inner
/// key); the `auth` signature is dropped, as it is a deterministic function of the proposer, the
/// `auth_data`, and the slot and so adds no identity.
///
/// Operators may change their builder config at any time. Because this identity captures every entry
/// field that reaches a builder, any edit yields a new key that won't match a previously-sent entry,
/// so the updated preference is published again.
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
        url: &BuilderUrl,
        auth_data: &RequestAuthData,
        max_execution_payment: u64,
    ) -> bool {
        self.entries.get(&slot).is_some_and(|set| {
            set.contains(&InnerPreferencesKey {
                pubkey,
                url: url.clone(),
                auth_data: auth_data.clone(),
                max_execution_payment,
            })
        })
    }

    pub fn mark_sent(
        &mut self,
        pubkey: PublicKeyBytes,
        builder_preferences_entry: BuilderPreferenceEntryV1,
    ) {
        let slot = builder_preferences_entry.auth.message.slot;
        let inner_key = InnerPreferencesKey {
            pubkey,
            url: builder_preferences_entry.url,
            auth_data: builder_preferences_entry.auth.message.data,
            max_execution_payment: builder_preferences_entry.max_execution_payment,
        };
        self.entries.entry(slot).or_default().insert(inner_key);
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
    /// Will only publish a given `(proposer, builder, max_execution_payment)` preference once.
    async fn poll_and_publish_preferences(
        &self,
        current_slot: Slot,
        published_preferences: &mut PublishedBuilderPreferencesCache,
    ) {
        let current_epoch = current_slot.epoch(S::E::slots_per_epoch());
        let mut pending_requests: HashMap<PublicKeyBytes, Vec<BuilderPreferenceEntryV1>> =
            HashMap::new();

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
                for direct_builder in self.inner.configured_builders.direct_builders() {
                    if published_preferences.contains(
                        proposer_data.slot,
                        proposer_data.pubkey,
                        &direct_builder.url,
                        &direct_builder.auth_data,
                        direct_builder.max_execution_payment,
                    ) {
                        // already published, skip
                        continue;
                    }

                    match self
                        .build_entry(proposer_data.slot, proposer_data.pubkey, &direct_builder)
                        .await
                    {
                        Ok(builder_preferences_entry) => pending_requests
                            .entry(proposer_data.pubkey)
                            .or_default()
                            .push(builder_preferences_entry),
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

        for (pubkey, entries) in pending_requests {
            let pubkey_ref = &pubkey;
            let entries_ref = entries.as_slice();

            match self
                .inner
                .beacon_nodes
                // first success is okay here because later we'll be
                // resending the auths when we publish the beacon block
                .first_success(|beacon_node| async move {
                    beacon_node
                        .post_validator_builder_preferences(pubkey_ref, entries_ref)
                        .await
                })
                .await
            {
                Ok(()) => {
                    for entry in entries {
                        published_preferences.mark_sent(pubkey, entry);
                    }
                }
                Err(e) => error!(error = %e, "Failed to publish builder preferences"),
            }
        }
    }

    /// Build a `BuilderPreferenceEntryV1` for a proposer/builder, signing (and caching) the request
    /// auth over the builder's opaque `auth_data` if it hasn't been signed for this slot yet.
    async fn build_entry(
        &self,
        slot: Slot,
        pubkey: PublicKeyBytes,
        direct_builder: &DirectBuilder,
    ) -> Result<BuilderPreferenceEntryV1, validator_store::Error<S::Error>> {
        let signed_auth = if let Some(signed_auth) =
            self.inner
                .request_auth_cache
                .get(slot, pubkey, &direct_builder.auth_data)
        {
            signed_auth
        } else {
            let request_auth_v1 = RequestAuthV1 {
                data: direct_builder.auth_data.clone(),
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
                direct_builder.auth_data.clone(),
                signed_request_auth.clone(),
            );
            signed_request_auth
        };

        Ok(BuilderPreferenceEntryV1::new(
            direct_builder.url.clone(),
            signed_auth,
            direct_builder.max_execution_payment,
        ))
    }
}
