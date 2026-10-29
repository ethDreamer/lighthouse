//! EIP-8453 (decoupled consensus) round and epoch processing.
//!
//! This is a deliberately naive, spec-shaped implementation: the single-pass epoch processing
//! optimization is built around Casper FFG epoch participation, which EIP-8453 removes, so this
//! fork bypasses it entirely (EIP8453-STUB: no single-pass, no `ProgressiveBalancesCache`).
//!
//! Spec: `specs/_features/eip8453/beacon-chain.md` (Round processing, Epoch processing) on the
//! `decoupled-consensus` branch.

use std::collections::BTreeSet;

use milhouse::{AnyList, ProgressiveList, Vector};
use safe_arith::SafeArith;
use ssz_types::FixedVector;
use types::{
    BeaconState, BeaconStateError, ChainSpec, DepositData, EpochTotalBalances, EthSpec,
    ParticipationFlags, PendingDeposit, ProgressiveBalancesCache, Slot,
    consts::eip8453::{NUM_FLAG_INDICES, PROGRESS_FLAG_INDEX},
};

use crate::common::eip8453::{
    add_participation_flag, get_flag_index_rewards, get_unslashed_participating_indices,
    has_builder_payment_quorum,
};
use crate::common::{increase_balance, initiate_validator_exit};
use crate::per_block_processing::is_valid_deposit_signature;
use crate::per_epoch_processing::{
    EpochProcessingSummary, Error, ParticipationEpochSummary,
    capella::process_historical_summaries_update,
    resets::{process_randao_mixes_reset, process_slashings_reset},
    single_pass::process_proposer_lookahead,
    slashings::process_slashings,
};

/// `process_round` from the spec, run on the last slot of each round (before epoch processing at
/// shared boundaries).
pub fn process_round<E: EthSpec>(
    state: &mut BeaconState<E>,
    spec: &ChainSpec,
) -> Result<(), Error> {
    state.build_total_active_balance_cache(spec)?;
    process_inactivity_updates(state, spec)?;
    process_rewards(state, spec)?;
    process_participation_flag_updates(state)?;
    Ok(())
}

/// `process_inactivity_updates` from the spec, rewritten for the virtual penalty system:
/// inactivity scores are updated every round, not only during an inactivity leak, and a validator
/// reaching `EJECTION_INACTIVITY_SCORE` is force-exited (keeping its stake).
fn process_inactivity_updates<E: EthSpec>(
    state: &mut BeaconState<E>,
    spec: &ChainSpec,
) -> Result<(), Error> {
    let participating: BTreeSet<usize> =
        get_unslashed_participating_indices(state, PROGRESS_FLAG_INDEX, spec)?
            .into_iter()
            .collect();
    let active_indices = state.get_active_validator_indices(state.current_epoch(), spec)?;
    let ejection_score = E::ejection_inactivity_score();

    for index in active_indices {
        let score = *state
            .inactivity_scores()?
            .get(index)
            .ok_or(BeaconStateError::UnknownValidator(index))?;
        if participating.contains(&index) {
            let new_score = score.saturating_sub(spec.inactivity_score_recovery_rate);
            if new_score != score {
                *state
                    .inactivity_scores_mut()?
                    .get_mut(index)
                    .ok_or(BeaconStateError::UnknownValidator(index))? = new_score;
            }
        } else {
            let new_score = score.safe_add(spec.inactivity_score_bias)?;
            *state
                .inactivity_scores_mut()?
                .get_mut(index)
                .ok_or(BeaconStateError::UnknownValidator(index))? = new_score;
            if new_score >= ejection_score {
                initiate_validator_exit(state, index, spec)?;
            }
        }
    }
    Ok(())
}

/// `process_rewards` from the spec: settle round rewards. Inactivity is no longer penalized.
fn process_rewards<E: EthSpec>(state: &mut BeaconState<E>, spec: &ChainSpec) -> Result<(), Error> {
    for flag_index in 0..NUM_FLAG_INDICES {
        let rewards = get_flag_index_rewards(state, flag_index, spec)?;
        for (index, reward) in rewards {
            increase_balance(state, index, reward)?;
        }
    }
    Ok(())
}

/// `process_participation_flag_updates` from the spec: reset round participation, carrying
/// progress already made at the current height into the new round.
fn process_participation_flag_updates<E: EthSpec>(state: &mut BeaconState<E>) -> Result<(), Error> {
    let validator_count = state.validators().len();
    *state.round_participation_mut()? = ProgressiveList::try_from_iter(std::iter::repeat_n(
        ParticipationFlags::default(),
        validator_count,
    ))
    .map_err(BeaconStateError::MilhouseError)?;

    for index in 0..validator_count {
        let progressed = state
            .progress_participation()?
            .get(index)
            .copied()
            .unwrap_or(false);
        if progressed {
            add_participation_flag(state, index, PROGRESS_FLAG_INDEX)?;
        }
    }
    Ok(())
}

/// `process_epoch` from the spec [Modified in EIP8453]: justification & finalization, inactivity,
/// rewards and participation updates are gone (the FG and round processing replace them).
pub fn process_epoch<E: EthSpec>(
    state: &mut BeaconState<E>,
    spec: &ChainSpec,
) -> Result<EpochProcessingSummary<E>, Error> {
    state.build_total_active_balance_cache(spec)?;
    state.update_pubkey_cache()?;
    state.build_exit_cache(spec)?;

    let sync_committee = state.current_sync_committee()?.clone();
    let current_epoch_total_active_balance = state.get_total_active_balance()?;

    // Snapshot for the epoch summary (see below).
    let validators_snapshot = state.validators().to_owned_list();
    let round_participation_snapshot: AnyList<ParticipationFlags, E::ValidatorRegistryLimit> =
        AnyList::from(state.round_participation()?.clone());

    process_registry_updates(state, spec)?;
    process_slashings(state, current_epoch_total_active_balance, spec)?;
    process_pending_deposits(state, spec)?;
    process_pending_consolidations(state)?;
    process_builder_pending_payments(state, spec)?;
    process_effective_balance_updates(state, spec)?;
    process_slashings_reset(state)?;
    process_randao_mixes_reset(state)?;
    process_historical_summaries_update(state)?;
    crate::per_epoch_processing::altair::process_sync_committee_updates(state, spec)?;
    process_proposer_lookahead(state, spec)?;
    process_ptc_window(state, spec)?;
    process_head_vote_committee_window(state, spec)?;

    state.advance_caches()?;

    // EIP8453-STUB: the epoch summary (consumed by the validator monitor and metrics) is
    // FFG-shaped. Reuse the Altair variant with the round participation snapshot standing in for
    // both epochs' participation (flag 0 = progress, flag 1 = finality) and a zero-initialized
    // progressive balances cache so its balance methods return zero instead of erroring.
    let mut progressive_balances = ProgressiveBalancesCache::default();
    progressive_balances.initialize(
        state.current_epoch(),
        EpochTotalBalances::new(spec),
        EpochTotalBalances::new(spec),
    );
    let participation = ParticipationEpochSummary::new(
        validators_snapshot,
        round_participation_snapshot.clone(),
        round_participation_snapshot,
        state.previous_epoch(),
        state.current_epoch(),
    );

    Ok(EpochProcessingSummary::Altair {
        progressive_balances,
        current_epoch_total_active_balance,
        participation: Box::new(participation),
        sync_committee,
    })
}

/// `process_registry_updates` from the spec [Modified in EIP8453]: activation eligibility is now
/// keyed on `compute_epoch_at_slot(state.finalized_slot)`.
fn process_registry_updates<E: EthSpec>(
    state: &mut BeaconState<E>,
    spec: &ChainSpec,
) -> Result<(), Error> {
    let current_epoch = state.current_epoch();
    let fork_name = state.fork_name_unchecked();
    let finalized_epoch = state.finalized_slot()?.epoch(E::slots_per_epoch());
    let delayed_activation_epoch = state.compute_activation_exit_epoch(current_epoch, spec)?;
    let validator_count = state.validators().len();

    for index in 0..validator_count {
        let validator = state.get_validator(index)?;
        let is_eligible_for_queue = validator.is_eligible_for_activation_queue(spec, fork_name);
        let is_ejectable = validator.is_active_at(current_epoch)
            && validator.effective_balance <= spec.ejection_balance;
        let is_eligible_for_activation = validator.activation_eligibility_epoch <= finalized_epoch
            && validator.activation_epoch == spec.far_future_epoch;

        if is_eligible_for_queue {
            state.get_validator_mut(index)?.activation_eligibility_epoch =
                current_epoch.safe_add(1)?;
        }
        if is_ejectable {
            initiate_validator_exit(state, index, spec)?;
        }
        if is_eligible_for_activation {
            state.get_validator_mut(index)?.activation_epoch = delayed_activation_epoch;
        }
    }
    Ok(())
}

/// `apply_pending_deposit` from the spec: top up a known validator, or add a new one if the
/// deposit signature is valid.
fn apply_pending_deposit<E: EthSpec>(
    state: &mut BeaconState<E>,
    deposit: &PendingDeposit,
    spec: &ChainSpec,
) -> Result<(), Error> {
    if let Some(index) = state.get_validator_index(&deposit.pubkey)? {
        increase_balance(state, index, deposit.amount)?;
    } else {
        let deposit_data = DepositData {
            pubkey: deposit.pubkey,
            withdrawal_credentials: deposit.withdrawal_credentials,
            amount: deposit.amount,
            signature: deposit.signature.clone(),
        };
        if is_valid_deposit_signature(&deposit_data, spec).is_ok() {
            state.add_validator_to_registry(
                deposit_data.pubkey,
                deposit_data.withdrawal_credentials,
                deposit_data.amount,
                spec,
            )?;
        }
    }
    Ok(())
}

/// `process_pending_deposits` from the spec [Modified in EIP8453]: a deposit is processed only
/// once its slot is at or before `state.finalized_slot` (exactly, not rounded to an epoch).
fn process_pending_deposits<E: EthSpec>(
    state: &mut BeaconState<E>,
    spec: &ChainSpec,
) -> Result<(), Error> {
    let next_epoch = state.next_epoch()?;
    let finalized_slot = state.finalized_slot()?;
    let available_for_processing = state
        .deposit_balance_to_consume()?
        .safe_add(state.get_activation_churn_limit(spec)?)?;
    let mut processed_amount: u64 = 0;
    let mut next_deposit_index: usize = 0;
    let mut deposits_to_postpone: Vec<PendingDeposit> = vec![];
    let mut deposits_to_apply: Vec<PendingDeposit> = vec![];
    let mut is_churn_limit_reached = false;

    for deposit in state.pending_deposits()?.iter() {
        if deposit.slot > finalized_slot {
            break;
        }
        if next_deposit_index >= E::max_pending_deposits_per_epoch() {
            break;
        }

        let mut is_validator_exited = false;
        let mut is_validator_withdrawn = false;
        if let Some(index) = state.pubkey_cache().get(&deposit.pubkey) {
            let validator = state.get_validator(index)?;
            is_validator_exited = validator.exit_epoch < spec.far_future_epoch;
            is_validator_withdrawn = validator.withdrawable_epoch < next_epoch;
        }

        if is_validator_withdrawn {
            // Deposited balance will never become active: apply without consuming churn.
            deposits_to_apply.push(deposit.clone());
        } else if is_validator_exited {
            // Validator is exiting: postpone the deposit until after the withdrawable epoch.
            deposits_to_postpone.push(deposit.clone());
        } else {
            is_churn_limit_reached =
                processed_amount.safe_add(deposit.amount)? > available_for_processing;
            if is_churn_limit_reached {
                break;
            }
            processed_amount.safe_add_assign(deposit.amount)?;
            deposits_to_apply.push(deposit.clone());
        }

        next_deposit_index.safe_add_assign(1)?;
    }

    for deposit in &deposits_to_apply {
        apply_pending_deposit(state, deposit, spec)?;
    }

    let remaining: Vec<PendingDeposit> = state
        .pending_deposits()?
        .iter_from(next_deposit_index)?
        .cloned()
        .chain(deposits_to_postpone)
        .collect();
    state.set_pending_deposits_from_iter(remaining)?;

    *state.deposit_balance_to_consume_mut()? = if is_churn_limit_reached {
        available_for_processing.safe_sub(processed_amount)?
    } else {
        0
    };

    Ok(())
}

/// `process_pending_consolidations` (unchanged from Electra, naive form).
fn process_pending_consolidations<E: EthSpec>(state: &mut BeaconState<E>) -> Result<(), Error> {
    use crate::common::decrease_balance;

    let next_epoch = state.next_epoch()?;
    let mut next_pending_consolidation: usize = 0;
    let pending_consolidations = state.pending_consolidations()?.to_owned_list();

    for pending_consolidation in pending_consolidations.iter() {
        let source_index = pending_consolidation.source_index as usize;
        let target_index = pending_consolidation.target_index as usize;
        let source_validator = state.get_validator(source_index)?;
        if source_validator.slashed {
            next_pending_consolidation.safe_add_assign(1)?;
            continue;
        }
        if source_validator.withdrawable_epoch > next_epoch {
            break;
        }

        let source_effective_balance = std::cmp::min(
            *state
                .balances()
                .get(source_index)
                .ok_or(BeaconStateError::UnknownValidator(source_index))?,
            source_validator.effective_balance,
        );
        decrease_balance(state, source_index, source_effective_balance)?;
        increase_balance(state, target_index, source_effective_balance)?;

        next_pending_consolidation.safe_add_assign(1)?;
    }

    state
        .pending_consolidations_mut()?
        .pop_front(next_pending_consolidation)
        .map_err(BeaconStateError::MilhouseError)?;

    Ok(())
}

/// `process_builder_pending_payments` from the spec [Modified in EIP8453]: payments are weighed
/// by the head votes recorded in `head_vote_participation`, which rotates alongside them.
fn process_builder_pending_payments<E: EthSpec>(
    state: &mut BeaconState<E>,
    spec: &ChainSpec,
) -> Result<(), Error> {
    let slots_per_epoch = E::slots_per_epoch() as usize;

    let mut qualifying_withdrawals = vec![];
    for index in 0..slots_per_epoch {
        let payment = state
            .builder_pending_payments()?
            .get(index)
            .ok_or(BeaconStateError::InvalidIndicesCount)?
            .clone();
        if payment.withdrawal.amount == 0 {
            continue;
        }
        let num_set_seats = state
            .head_vote_participation()?
            .get(index)
            .ok_or(BeaconStateError::InvalidIndicesCount)?
            .num_set_bits() as u64;
        if has_builder_payment_quorum::<E>(num_set_seats, spec)? {
            qualifying_withdrawals.push(payment.withdrawal);
        }
    }
    for withdrawal in qualifying_withdrawals {
        state.builder_pending_withdrawals_mut()?.push(withdrawal)?;
    }

    // Rotate the payments window.
    let updated_payments = state
        .builder_pending_payments()?
        .iter()
        .skip(slots_per_epoch)
        .cloned()
        .chain((0..slots_per_epoch).map(|_| Default::default()))
        .collect::<Vec<_>>();
    *state.builder_pending_payments_mut()? =
        Vector::new(updated_payments).map_err(BeaconStateError::MilhouseError)?;

    // Rotate the head vote participation window in step.
    let updated_participation = state
        .head_vote_participation()?
        .iter()
        .skip(slots_per_epoch)
        .cloned()
        .chain((0..slots_per_epoch).map(|_| Default::default()))
        .collect::<Vec<_>>();
    *state.head_vote_participation_mut()? =
        FixedVector::new(updated_participation).map_err(BeaconStateError::SszTypesError)?;

    Ok(())
}

/// `process_effective_balance_updates` (naive form, with the post-Electra per-validator maximum
/// effective balance).
fn process_effective_balance_updates<E: EthSpec>(
    state: &mut BeaconState<E>,
    spec: &ChainSpec,
) -> Result<(), Error> {
    let next_epoch = state.next_epoch()?;
    let fork_name = state.fork_name_unchecked();
    let mut new_total_active_balance: u64 = 0;

    let hysteresis_increment = spec
        .effective_balance_increment
        .safe_div(spec.hysteresis_quotient)?;
    let downward_threshold = hysteresis_increment.safe_mul(spec.hysteresis_downward_multiplier)?;
    let upward_threshold = hysteresis_increment.safe_mul(spec.hysteresis_upward_multiplier)?;

    let validator_count = state.validators().len();
    for index in 0..validator_count {
        let balance = *state
            .balances()
            .get(index)
            .ok_or(BeaconStateError::BalancesOutOfBounds(index))?;
        let validator = state.get_validator(index)?;
        let max_effective_balance = validator.get_max_effective_balance(spec, fork_name);

        let new_effective_balance = if balance.safe_add(downward_threshold)?
            < validator.effective_balance
            || validator.effective_balance.safe_add(upward_threshold)? < balance
        {
            std::cmp::min(
                balance.safe_sub(balance.safe_rem(spec.effective_balance_increment)?)?,
                max_effective_balance,
            )
        } else {
            validator.effective_balance
        };

        if validator.is_active_at(next_epoch) {
            new_total_active_balance.safe_add_assign(new_effective_balance)?;
        }
        if new_effective_balance != validator.effective_balance {
            state.get_validator_mut(index)?.effective_balance = new_effective_balance;
        }
    }

    state.set_total_active_balance(next_epoch, new_total_active_balance, spec);
    Ok(())
}

/// `process_ptc_window` [Modified in EIP8453]: the modified `compute_ptc` samples active
/// validators directly, so no lookahead committee cache is involved.
fn process_ptc_window<E: EthSpec>(
    state: &mut BeaconState<E>,
    spec: &ChainSpec,
) -> Result<(), Error> {
    let slots_per_epoch = E::slots_per_epoch() as usize;
    let lookahead_epoch = state
        .current_epoch()
        .safe_add(spec.min_seed_lookahead.as_u64())?
        .safe_add(1)?;

    let mut window: Vec<FixedVector<u64, E::PTCSize>> = state
        .ptc_window()?
        .iter()
        .skip(slots_per_epoch)
        .cloned()
        .collect();
    let start_slot = lookahead_epoch.start_slot(E::slots_per_epoch());
    for i in 0..slots_per_epoch {
        let slot = start_slot.safe_add(i as u64)?;
        let ptc = state.compute_ptc_eip8453(slot, spec)?;
        let ptc_u64: Vec<u64> = ptc.into_iter().map(|v| v as u64).collect();
        window.push(FixedVector::new(ptc_u64).map_err(BeaconStateError::SszTypesError)?);
    }
    *state.ptc_window_mut()? = Vector::new(window).map_err(BeaconStateError::MilhouseError)?;
    Ok(())
}

/// `process_head_vote_committee_window` from the spec: rotate the cached head vote committees.
pub fn process_head_vote_committee_window<E: EthSpec>(
    state: &mut BeaconState<E>,
    spec: &ChainSpec,
) -> Result<(), Error> {
    let slots_per_epoch = E::slots_per_epoch() as usize;
    let lookahead_epoch = state
        .current_epoch()
        .safe_add(spec.min_seed_lookahead.as_u64())?
        .safe_add(1)?;

    let mut window: Vec<FixedVector<u64, E::HeadVoteCommitteeSize>> = state
        .head_vote_committee_window()?
        .iter()
        .skip(slots_per_epoch)
        .cloned()
        .collect();
    let start_slot = lookahead_epoch.start_slot(E::slots_per_epoch());
    for i in 0..slots_per_epoch {
        let slot: Slot = start_slot.safe_add(i as u64)?;
        let committee = state.compute_head_vote_committee(slot, spec)?;
        let committee_u64: Vec<u64> = committee.into_iter().map(|v| v as u64).collect();
        window.push(FixedVector::new(committee_u64).map_err(BeaconStateError::SszTypesError)?);
    }
    *state.head_vote_committee_window_mut()? =
        Vector::new(window).map_err(BeaconStateError::MilhouseError)?;
    Ok(())
}
