//! The EIP-8453 duty services: head voting, safe voting, and FG attesting.
//!
//! Per validator.md each duty reads the fork choice exactly once, at its deadline:
//! - head vote: `get_head_vote_due_ms()` (one quarter) into the duty slot;
//! - safe vote: one quarter into the validator's assigned sub-round slot;
//! - FG attestation: `get_attestation_due_ms()` into the committee's slot.
//!
//! The beacon node serves the fork-choice reads (`get_voter_head`, `get_safe_vote_node`, and the
//! FG attestation components) and the validator store applies the signing-record rules.

use std::ops::Deref;
use std::sync::Arc;

use crate::duties_service::DutiesService;
use beacon_node_fallback::BeaconNodeFallback;
use futures::future::join_all;
use slot_clock::SlotClock;
use task_executor::TaskExecutor;
use tokio::time::{Duration, sleep};
use tracing::{debug, error, info, warn};
use types::eip8453::{compute_round_at_slot, compute_safe_vote_sub_round};
use types::{ChainSpec, EthSpec, Slot};
use validator_store::{DoppelgangerStatus, ValidatorStore};

pub struct Inner<S, T> {
    duties_service: Arc<DutiesService<S, T>>,
    validator_store: Arc<S>,
    slot_clock: T,
    beacon_nodes: Arc<BeaconNodeFallback<T>>,
    executor: TaskExecutor,
    chain_spec: Arc<ChainSpec>,
}

pub struct Eip8453Service<S, T> {
    inner: Arc<Inner<S, T>>,
}

impl<S, T> Clone for Eip8453Service<S, T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<S, T> Deref for Eip8453Service<S, T> {
    type Target = Inner<S, T>;

    fn deref(&self) -> &Self::Target {
        self.inner.deref()
    }
}

impl<S, T> Eip8453Service<S, T>
where
    S: ValidatorStore + 'static,
    T: SlotClock + 'static,
{
    pub fn new(
        duties_service: Arc<DutiesService<S, T>>,
        validator_store: Arc<S>,
        slot_clock: T,
        beacon_nodes: Arc<BeaconNodeFallback<T>>,
        executor: TaskExecutor,
        chain_spec: Arc<ChainSpec>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                duties_service,
                validator_store,
                slot_clock,
                beacon_nodes,
                executor,
                chain_spec,
            }),
        }
    }

    pub fn start_update_service(self) -> Result<(), String> {
        info!(
            head_vote_due_ms = self.chain_spec.get_head_vote_due().as_millis(),
            "EIP-8453 duty services started"
        );
        let executor = self.executor.clone();

        let head_vote_service = self.clone();
        executor.spawn(
            async move {
                loop {
                    if let Err(e) = head_vote_service.head_vote_tick().await {
                        error!(error = e, "Head vote duty failed");
                    }
                }
            },
            "eip8453_head_vote_service",
        );

        let safe_vote_service = self.clone();
        executor.spawn(
            async move {
                loop {
                    if let Err(e) = safe_vote_service.safe_vote_tick().await {
                        error!(error = e, "Safe vote duty failed");
                    }
                }
            },
            "eip8453_safe_vote_service",
        );

        let fg_service = self;
        executor.spawn(
            async move {
                loop {
                    if let Err(e) = fg_service.fg_attestation_tick().await {
                        error!(error = e, "FG attestation duty failed");
                    }
                }
            },
            "eip8453_fg_attestation_service",
        );

        Ok(())
    }

    /// Sleep until `offset` into the next slot. Returns the slot when the clock lands there and
    /// EIP-8453 is enabled at it, `None` otherwise (after sleeping toward the fork if needed).
    async fn wait_until_offset_of_next_slot(&self, offset: Duration) -> Option<Slot> {
        let slot_duration = self.chain_spec.get_slot_duration();
        let Some(duration_to_next_slot) = self.slot_clock.duration_to_next_slot() else {
            error!("Failed to read slot clock");
            sleep(slot_duration).await;
            return None;
        };
        let Some(current_slot) = self.slot_clock.now() else {
            sleep(slot_duration).await;
            return None;
        };
        let duty_slot = current_slot + 1;

        if !self
            .chain_spec
            .fork_name_at_slot::<S::E>(duty_slot)
            .eip8453_enabled()
        {
            let sleep_duration = self
                .chain_spec
                .eip8453_fork_epoch
                .and_then(|fork_epoch| {
                    let pre_fork_slot = fork_epoch
                        .start_slot(S::E::slots_per_epoch())
                        .saturating_sub(1u64);
                    self.slot_clock.duration_to_slot(pre_fork_slot)
                })
                .unwrap_or(slot_duration);
            sleep(sleep_duration).await;
            return None;
        }

        sleep(duration_to_next_slot + offset).await;

        let post_sleep_slot = self.slot_clock.now()?;
        if post_sleep_slot != duty_slot {
            warn!(%duty_slot, %post_sleep_slot, "Slot clock drifted during sleep");
            return None;
        }
        Some(duty_slot)
    }

    /// One head-vote duty slot: read `get_voter_head` at the deadline, sign per duty, publish.
    async fn head_vote_tick(&self) -> Result<(), String> {
        let Some(slot) = self
            .wait_until_offset_of_next_slot(self.chain_spec.get_head_vote_due())
            .await
        else {
            return Ok(());
        };
        let duties = self.duties_service.get_head_vote_duties_for_slot(slot);
        if duties.is_empty() {
            return Ok(());
        }

        let data = self
            .beacon_nodes
            .first_success(|beacon_node| async move {
                beacon_node
                    .get_validator_eip8453_head_vote_data(slot)
                    .await
                    .map(|resp| resp.data)
            })
            .await
            .map_err(|e| format!("Failed to get head vote data: {e}"))?;
        if data.slot != slot {
            return Err(format!(
                "head vote data slot mismatch: wanted {slot}, got {}",
                data.slot
            ));
        }

        let signing_futures = duties.iter().map(|duty| async move {
            (
                duty,
                self.validator_store
                    .sign_eip8453_head_vote(duty.pubkey, data)
                    .await,
            )
        });
        let mut messages = Vec::with_capacity(duties.len());
        for (duty, result) in join_all(signing_futures).await {
            match result {
                Ok(message) => messages.push(message),
                Err(e) => {
                    warn!(error = ?e, validator = ?duty.pubkey, %slot, "Head vote not signed")
                }
            }
        }
        if messages.is_empty() {
            return Ok(());
        }

        let count = messages.len();
        self.beacon_nodes
            .first_success(|beacon_node| {
                let messages = messages.clone();
                async move {
                    beacon_node
                        .post_beacon_pool_eip8453_head_votes(&messages)
                        .await
                        .map_err(|e| format!("Failed to publish head votes: {e:?}"))
                }
            })
            .await
            .map_err(|e| e.to_string())?;
        info!(%slot, %count, root = ?data.beacon_block_root, "Published head votes");
        Ok(())
    }

    /// One safe-vote sub-round slot: every local validator assigned to this sub-round names the
    /// fork choice's `get_safe_vote_node` selection.
    async fn safe_vote_tick(&self) -> Result<(), String> {
        let Some(slot) = self
            .wait_until_offset_of_next_slot(self.chain_spec.get_head_vote_due())
            .await
        else {
            return Ok(());
        };
        let slots_per_round = S::E::slots_per_round();
        let sub_rounds = slots_per_round.saturating_sub(2);
        let sub_round = slot.as_u64() % slots_per_round;
        if sub_round >= sub_rounds {
            return Ok(());
        }
        let round = compute_round_at_slot::<S::E>(slot);

        // The assignment is a pure function of the validator index.
        let voting_pubkeys: Vec<bls::PublicKeyBytes> = self
            .validator_store
            .voting_pubkeys(DoppelgangerStatus::only_safe);
        let assigned: Vec<_> = voting_pubkeys
            .into_iter()
            .filter(|pubkey| {
                self.validator_store
                    .validator_index(pubkey)
                    .is_some_and(|index| compute_safe_vote_sub_round::<S::E>(index) == sub_round)
            })
            .collect();
        if assigned.is_empty() {
            return Ok(());
        }

        let data = self
            .beacon_nodes
            .first_success(|beacon_node| async move {
                beacon_node
                    .get_validator_eip8453_safe_vote_data()
                    .await
                    .map(|resp| resp.data)
            })
            .await
            .map_err(|e| format!("Failed to get safe vote data: {e}"))?;
        if data.round != round {
            return Err(format!(
                "safe vote data round mismatch: wanted {round}, got {}",
                data.round
            ));
        }

        let signing_futures = assigned.iter().map(|pubkey| async move {
            (
                *pubkey,
                self.validator_store
                    .sign_eip8453_safe_vote(*pubkey, data)
                    .await,
            )
        });
        let mut messages = Vec::with_capacity(assigned.len());
        for (pubkey, result) in join_all(signing_futures).await {
            match result {
                Ok(message) => messages.push(message),
                Err(e) => warn!(error = ?e, validator = ?pubkey, %slot, "Safe vote not signed"),
            }
        }
        if messages.is_empty() {
            return Ok(());
        }

        let count = messages.len();
        self.beacon_nodes
            .first_success(|beacon_node| {
                let messages = messages.clone();
                async move {
                    beacon_node
                        .post_beacon_pool_eip8453_safe_votes(&messages)
                        .await
                        .map_err(|e| format!("Failed to publish safe votes: {e:?}"))
                }
            })
            .await
            .map_err(|e| e.to_string())?;
        info!(%slot, %round, %count, root = ?data.beacon_block_root, "Published safe votes");
        Ok(())
    }

    /// One FG attestation slot: take the single AC-to-FG observation at the deadline, fold it
    /// through each duty's signing record, and publish whatever survives.
    async fn fg_attestation_tick(&self) -> Result<(), String> {
        // The EIP-8453 attestation deadline is the inherited (post-Gloas) attestation due.
        let Some(slot) = self
            .wait_until_offset_of_next_slot(
                self.chain_spec
                    .get_attestation_due::<S::E>(self.slot_clock.now().unwrap_or_default() + 1),
            )
            .await
        else {
            return Ok(());
        };
        let duties = self.duties_service.get_fg_attester_duties_for_slot(slot);
        if duties.is_empty() {
            return Ok(());
        }

        let components = self
            .beacon_nodes
            .first_success(|beacon_node| async move {
                beacon_node
                    .get_validator_eip8453_fg_attestation_components()
                    .await
                    .map(|resp| resp.data)
            })
            .await
            .map_err(|e| format!("Failed to get FG attestation components: {e}"))?;

        // EIP8453-STUB (spec friction): validator.md gates the height pair on
        // `has_fresh_safe_quorum`, saying the attestation then "carries its finality pair
        // alone" — but `get_finality_attestation_data` has no such input and
        // `is_valid_attestation_data` admits no height-less encoding. The prototype withholds
        // the whole attestation.
        if !components.fresh_quorum {
            debug!(%slot, "No fresh safe quorum; withholding FG attestations");
            return Ok(());
        }

        let epoch = slot.epoch(S::E::slots_per_epoch());
        let signing_futures = duties.iter().map(|duty| {
            let components = components.clone();
            async move {
                (
                    duty,
                    self.validator_store
                        .produce_eip8453_fg_attestation(
                            duty.pubkey,
                            epoch,
                            duty.committee_index,
                            components.target,
                            components.justified,
                            components.finalized_height,
                        )
                        .await,
                )
            }
        });
        let mut attestations = Vec::with_capacity(duties.len());
        for (duty, result) in join_all(signing_futures).await {
            match result {
                Ok(Some(attestation)) => attestations.push(attestation),
                Ok(None) => {
                    debug!(validator = ?duty.pubkey, %slot, "Signing record suppressed FG attestation")
                }
                Err(e) => {
                    warn!(error = ?e, validator = ?duty.pubkey, %slot, "FG attestation not signed")
                }
            }
        }
        if attestations.is_empty() {
            return Ok(());
        }

        let count = attestations.len();
        self.beacon_nodes
            .first_success(|beacon_node| {
                let attestations = attestations.clone();
                async move {
                    beacon_node
                        .post_beacon_pool_eip8453_fg_attestations(&attestations)
                        .await
                        .map_err(|e| format!("Failed to publish FG attestations: {e:?}"))
                }
            })
            .await
            .map_err(|e| e.to_string())?;
        info!(
            %slot,
            %count,
            target_height = components.target.height,
            "Published FG attestations"
        );
        Ok(())
    }
}
