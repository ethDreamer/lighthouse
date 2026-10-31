//! Shared helpers for the EIP-8453 finality gadget state transition.
//!
//! Spec: `specs/_features/eip8453/beacon-chain.md` (Helpers) on the `decoupled-consensus` branch.

use integer_sqrt::IntegerSquareRoot;
use safe_arith::SafeArith;
use types::{
    BeaconState, BeaconStateError, ChainSpec, EthSpec, Slot,
    consts::eip8453::{PARTICIPATION_FLAG_WEIGHTS, Round},
    eip8453::{HeadVoteData, compute_round_at_slot},
};

/// `get_validator_weight` from the spec: effective-balance increments multiplied by
/// `EJECTION_INACTIVITY_SCORE**2 - inactivity_score**2`.
///
/// The spec computes weights in `Uint256`; `u128` provably suffices (per-validator weight is at
/// most `2^11 * 2^32 = 2^43`, and a sum over every possible validator stays below `2^96`), which
/// is a report-back item.
pub fn get_validator_weight<E: EthSpec>(
    state: &BeaconState<E>,
    index: usize,
    spec: &ChainSpec,
) -> Result<u128, BeaconStateError> {
    let ejection_score = E::ejection_inactivity_score();
    let effective_balance = state.get_effective_balance(index)?;
    let increments = effective_balance.safe_div(spec.effective_balance_increment)?;
    let inactivity_score = std::cmp::min(
        *state
            .inactivity_scores()?
            .get(index)
            .ok_or(BeaconStateError::UnknownValidator(index))?,
        ejection_score,
    );
    let inactivity_weight = (ejection_score.saturating_sub(inactivity_score) as u128)
        .saturating_mul(ejection_score.saturating_add(inactivity_score) as u128);
    Ok((increments as u128).saturating_mul(inactivity_weight))
}

/// `get_total_weight` from the spec: the combined weight of `indices`.
pub fn get_total_weight<E: EthSpec>(
    state: &BeaconState<E>,
    indices: impl IntoIterator<Item = usize>,
    spec: &ChainSpec,
) -> Result<u128, BeaconStateError> {
    let mut total: u128 = 0;
    for index in indices {
        total = total.saturating_add(get_validator_weight(state, index, spec)?);
    }
    Ok(total)
}

/// `get_participating_weight` + `has_quorum` from the spec, generalized over the participation
/// list representation (the state's milhouse `ProgressiveList<bool>` and `PendingFinality`'s
/// `ProgressiveVariableList<bool>` are different types).
///
/// Checks whether the active, unslashed validators for which `is_marked` returns true hold at
/// least two thirds of the total weight of the active validators.
pub fn has_quorum<E: EthSpec>(
    state: &BeaconState<E>,
    spec: &ChainSpec,
    is_marked: impl Fn(usize) -> bool,
) -> Result<bool, BeaconStateError> {
    let active_indices = state.get_active_validator_indices(state.current_epoch(), spec)?;
    let mut participating_weight: u128 = 0;
    let mut total_weight: u128 = 0;
    for index in active_indices {
        let weight = get_validator_weight(state, index, spec)?;
        total_weight = total_weight.saturating_add(weight);
        if is_marked(index) && !state.get_validator(index)?.slashed {
            participating_weight = participating_weight.saturating_add(weight);
        }
    }
    Ok(participating_weight > 0
        && participating_weight.saturating_mul(3) >= total_weight.saturating_mul(2))
}

/// `has_builder_payment_quorum` from the spec: whether the seats set in `participation` reach the
/// builder payment quorum.
pub fn has_builder_payment_quorum<E: EthSpec>(
    num_set_seats: u64,
    spec: &ChainSpec,
) -> Result<bool, BeaconStateError> {
    let quorum = (E::head_vote_committee_size() as u64)
        .safe_mul(spec.builder_payment_threshold_numerator)?;
    Ok(num_set_seats.saturating_mul(spec.builder_payment_threshold_denominator) >= quorum)
}

/// `get_finality_delay` from the spec, measured in rounds (saturating at zero: the finality
/// gadget can finalize a block of the current round).
pub fn get_finality_delay<E: EthSpec>(state: &BeaconState<E>) -> Result<Round, BeaconStateError> {
    let previous_round = state.previous_round();
    let finalized_round = compute_round_at_slot::<E>(state.finalized_slot()?);
    Ok(previous_round.saturating_sub(finalized_round))
}

/// `is_in_inactivity_leak` from the spec [Modified in EIP8453].
pub fn is_in_inactivity_leak<E: EthSpec>(state: &BeaconState<E>) -> Result<bool, BeaconStateError> {
    Ok(get_finality_delay(state)? > E::min_rounds_to_inactivity_leak())
}

/// `get_round_base_reward` from the spec: the base reward for one round of the validator at
/// `index`. Requires the total active balance cache to be built.
pub fn get_round_base_reward<E: EthSpec>(
    state: &BeaconState<E>,
    index: usize,
    spec: &ChainSpec,
) -> Result<u64, BeaconStateError> {
    let increments = state
        .get_effective_balance(index)?
        .safe_div(spec.effective_balance_increment)?;
    let total_balance_root = state.get_total_active_balance()?.integer_sqrt();
    let reward_per_increment = spec
        .effective_balance_increment
        .safe_mul(E::round_reward_factor())?
        .safe_div(total_balance_root)?;
    Ok(increments.safe_mul(reward_per_increment)?)
}

/// `add_participation_flag` from the spec: add the flag at `flag_index` to the validator's
/// participation in the current round.
pub fn add_participation_flag<E: EthSpec>(
    state: &mut BeaconState<E>,
    index: usize,
    flag_index: usize,
) -> Result<(), BeaconStateError> {
    let flags = state
        .round_participation_mut()?
        .get_mut(index)
        .ok_or(BeaconStateError::UnknownValidator(index))?;
    flags.add_flag(flag_index)?;
    Ok(())
}

/// `get_unslashed_participating_indices` from the spec, rewritten to use round participation.
pub fn get_unslashed_participating_indices<E: EthSpec>(
    state: &BeaconState<E>,
    flag_index: usize,
    spec: &ChainSpec,
) -> Result<Vec<usize>, BeaconStateError> {
    let mut participating = Vec::new();
    for index in state.get_active_validator_indices(state.current_epoch(), spec)? {
        let flags = state
            .round_participation()?
            .get(index)
            .copied()
            .ok_or(BeaconStateError::UnknownValidator(index))?;
        if flags.has_flag(flag_index)? && !state.get_validator(index)?.slashed {
            participating.push(index);
        }
    }
    Ok(participating)
}

/// `get_flag_index_rewards` from the spec: per-validator rewards for `flag_index`, settled at the
/// end of each round. A validator without the flag forfeits the reward and is not penalized, and
/// no rewards are paid during an inactivity leak.
pub fn get_flag_index_rewards<E: EthSpec>(
    state: &BeaconState<E>,
    flag_index: usize,
    spec: &ChainSpec,
) -> Result<Vec<(usize, u64)>, BeaconStateError> {
    use types::consts::altair::WEIGHT_DENOMINATOR;

    if is_in_inactivity_leak(state)? {
        return Ok(vec![]);
    }

    let unslashed_participating_indices =
        get_unslashed_participating_indices(state, flag_index, spec)?;
    let weight = *PARTICIPATION_FLAG_WEIGHTS
        .get(flag_index)
        .ok_or(BeaconStateError::InvalidFlagIndex(flag_index))?;

    // `get_total_balance`: sum of effective balances, floored at one increment.
    let mut unslashed_participating_balance: u64 = 0;
    for &index in &unslashed_participating_indices {
        unslashed_participating_balance =
            unslashed_participating_balance.saturating_add(state.get_effective_balance(index)?);
    }
    let unslashed_participating_balance = std::cmp::max(
        spec.effective_balance_increment,
        unslashed_participating_balance,
    );

    let unslashed_participating_increments =
        unslashed_participating_balance.safe_div(spec.effective_balance_increment)?;
    let active_increments = state
        .get_total_active_balance()?
        .safe_div(spec.effective_balance_increment)?;

    let mut rewards = Vec::with_capacity(unslashed_participating_indices.len());
    for index in unslashed_participating_indices {
        let base_reward = get_round_base_reward(state, index, spec)?;
        let reward_numerator = (base_reward as u128)
            .saturating_mul(weight as u128)
            .saturating_mul(unslashed_participating_increments as u128);
        let reward_denominator =
            (active_increments as u128).saturating_mul(WEIGHT_DENOMINATOR as u128);
        let reward = reward_numerator
            .checked_div(reward_denominator)
            .unwrap_or(0);
        rewards.push((index, reward as u64));
    }
    Ok(rewards)
}

/// `is_head_vote_same_slot` from the spec: whether `data` votes for the block proposed at its
/// slot.
pub fn is_head_vote_same_slot<E: EthSpec>(
    state: &BeaconState<E>,
    data: &HeadVoteData,
    spec: &ChainSpec,
) -> Result<bool, BeaconStateError> {
    if data.slot == spec.genesis_slot {
        return Ok(true);
    }
    if data.beacon_block_root != *state.get_block_root(data.slot)? {
        return Ok(false);
    }
    if data.beacon_block_root == *state.get_block_root(data.slot.saturating_sub(1u64))? {
        return Ok(false);
    }
    Ok(true)
}

/// The spec's `TIMEOUT_DELAY_SLOTS` constant (`= 2 * SLOTS_PER_ROUND`).
///
/// [Modified in safe-votes-v2] two rounds: target votes of one round are assumed to reach every
/// honest store by the end of the next, so a timeout may not leave a height before its target
/// votes have had time to arrive.
pub fn timeout_delay_slots<E: EthSpec>() -> Slot {
    Slot::new(E::slots_per_round().saturating_mul(2))
}

/// `get_head_vote_seat_reward` from the spec [New in safe-votes-v2]: the flat reward for one newly
/// recorded head vote seat, funded by the `HEAD_VOTE_WEIGHT` share of the round's total base
/// rewards. Zero during an inactivity leak. Requires the total active balance cache.
pub fn get_head_vote_seat_reward<E: EthSpec>(
    state: &BeaconState<E>,
    spec: &ChainSpec,
) -> Result<u64, BeaconStateError> {
    use types::consts::altair::WEIGHT_DENOMINATOR;
    use types::consts::eip8453::HEAD_VOTE_WEIGHT;

    if is_in_inactivity_leak(state)? {
        return Ok(0);
    }
    let total_active_balance = state.get_total_active_balance()?;
    let total_increments = total_active_balance.safe_div(spec.effective_balance_increment)?;
    let reward_per_increment = spec
        .effective_balance_increment
        .safe_mul(E::round_reward_factor())?
        .safe_div(total_active_balance.integer_sqrt())?;
    let total_round_rewards =
        (total_increments as u128).saturating_mul(reward_per_increment as u128);
    let denominator = (WEIGHT_DENOMINATOR as u128)
        .saturating_mul(E::slots_per_round() as u128)
        .saturating_mul(E::head_vote_committee_size() as u128);
    let reward = total_round_rewards
        .saturating_mul(HEAD_VOTE_WEIGHT as u128)
        .checked_div(denominator)
        .unwrap_or(0);
    Ok(reward as u64)
}
