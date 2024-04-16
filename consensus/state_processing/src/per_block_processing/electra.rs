use safe_arith::SafeArith;
use types::BeaconStateError as Error;
use types::{BeaconState, ChainSpec, Epoch, EthSpec};

// Thus function will return an error if not called on a post-electra state
//
// TODO: finish commenting
pub fn compute_consolidation_epoch_and_update_churn<E: EthSpec>(
    state: &mut BeaconState<E>,
    consolidation_balance: u64,
    spec: &ChainSpec,
) -> Result<Epoch, Error> {
    let earliest_consolidation_epoch = spec.compute_activation_exit_epoch(state.current_epoch())?;
    let per_epoch_consolidation_churn = state.get_consolidation_churn_limit(spec)?;
    // New epoch for consolidations
    if state.earliest_consolidation_epoch()? < earliest_consolidation_epoch {
        *state.earliest_consolidation_epoch_mut()? = earliest_consolidation_epoch;
        *state.consolidation_balance_to_consume_mut()? = per_epoch_consolidation_churn;
    }

    if consolidation_balance <= state.consolidation_balance_to_consume()? {
        // Consolidation fits in the current earliest consolidation epoch
        state
            .consolidation_balance_to_consume_mut()?
            .safe_sub_assign(consolidation_balance)?;
    } else {
        // Consolidation doesn't fit in the current earliest consolidation epoch
        let balance_to_process =
            consolidation_balance.safe_sub(state.consolidation_balance_to_consume()?)?;
        let additional_epochs = balance_to_process.safe_div(per_epoch_consolidation_churn)?;
        let remainder = balance_to_process.safe_rem(per_epoch_consolidation_churn)?;

        state
            .earliest_consolidation_epoch_mut()?
            .safe_add_assign(additional_epochs.safe_add(1)?)?;
        *state.consolidation_balance_to_consume_mut()? =
            per_epoch_consolidation_churn.safe_sub(remainder)?;
    }

    state.earliest_consolidation_epoch()
}

// Thus function will return an error if not called on a post-electra state
//
// TODO: finish commenting
pub fn compute_exit_epoch_and_update_churn<E: EthSpec>(
    state: &mut BeaconState<E>,
    exit_balance: u64,
    spec: &ChainSpec,
) -> Result<Epoch, Error> {
    let earliest_exit_epoch = spec.compute_activation_exit_epoch(state.current_epoch())?;
    let per_epoch_churn = state.get_activation_exit_churn_limit(spec)?;

    if state.earliest_exit_epoch()? < earliest_exit_epoch {
        *state.earliest_exit_epoch_mut()? = earliest_exit_epoch;
        *state.exit_balance_to_consume_mut()? = per_epoch_churn;
    }
    if exit_balance <= state.exit_balance_to_consume()? {
        // Exit fits in the current earliest epoch
        state
            .exit_balance_to_consume_mut()?
            .safe_sub_assign(exit_balance)?;
    } else {
        // Exit does not fit in the current earliest epoch
        let balance_to_process = exit_balance.safe_sub(state.exit_balance_to_consume()?)?;
        let additional_epochs = balance_to_process.safe_div(per_epoch_churn)?;
        let remainder = balance_to_process.safe_rem(per_epoch_churn)?;
        state
            .earliest_exit_epoch_mut()?
            .safe_add_assign(additional_epochs.safe_add(1)?)?;
        *state.exit_balance_to_consume_mut()? = per_epoch_churn.safe_sub(remainder)?;
    }

    state.earliest_exit_epoch()
}
