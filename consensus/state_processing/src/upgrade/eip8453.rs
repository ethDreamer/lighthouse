use milhouse::ProgressiveList;
use std::mem;
use types::{
    BeaconState, BeaconStateEip8453, BeaconStateError as Error, ChainSpec, EthSpec, Fork,
    HeightCheckpoint, ParticipationFlags,
};

/// Transform a `Heze` state into an `Eip8453` state.
///
/// EIP8453-STUB: the spec's upgrade function is not yet defined (fork.md: "the upgrade function,
/// which sets the initial state of the finality gadget, is not yet specified"). Every choice
/// below about the initial finality gadget state is therefore an invention to report back to the
/// spec authors:
///
/// - `target_checkpoint` starts at height 1 with an empty root, which the next `process_slot`
///   back-fills with the latest block root (mirroring `advance_height`).
/// - `target_slot` is the slot of the latest block header.
/// - `justified_checkpoint` and `finalized_checkpoint` start at height 0 with the pre-fork
///   finalized root, and `finalized_slot` is the start slot of the pre-fork finalized epoch, so
///   that deposit/activation processing keyed on `finalized_slot` continues from the Casper FFG
///   finality at the fork.
/// - All participation is empty and `pending_finality` starts empty.
pub fn upgrade_to_eip8453<E: EthSpec>(
    pre_state: &mut BeaconState<E>,
    spec: &ChainSpec,
) -> Result<(), Error> {
    let post = upgrade_state_to_eip8453(pre_state, spec)?;

    *pre_state = post;

    Ok(())
}

pub fn upgrade_state_to_eip8453<E: EthSpec>(
    pre_state: &mut BeaconState<E>,
    spec: &ChainSpec,
) -> Result<BeaconState<E>, Error> {
    let epoch = pre_state.current_epoch();
    let pre = pre_state.as_heze_mut()?;

    let validator_count = pre.validators.len();
    let pre_finalized = pre.finalized_checkpoint;
    let finalized_slot = pre_finalized.epoch.start_slot(E::slots_per_epoch());

    // Where possible, use something like `mem::take` to move fields from behind the &mut
    // reference. For other fields that don't have a good default value, use `clone`.
    //
    // Fixed size vectors get cloned because replacing them would require the same size
    // allocation as cloning.
    let post = BeaconState::Eip8453(BeaconStateEip8453 {
        // Versioning
        genesis_time: pre.genesis_time,
        genesis_validators_root: pre.genesis_validators_root,
        slot: pre.slot,
        fork: Fork {
            previous_version: pre.fork.current_version,
            current_version: spec.eip8453_fork_version,
            epoch,
        },
        // History
        latest_block_header: pre.latest_block_header.clone(),
        block_roots: pre.block_roots.clone(),
        state_roots: pre.state_roots.clone(),
        historical_roots: mem::take(&mut pre.historical_roots),
        // [Modified in EIP8453] eth1 data removed.
        // Registry
        validators: mem::take(&mut pre.validators),
        balances: mem::take(&mut pre.balances),
        // Randomness
        randao_mixes: pre.randao_mixes.clone(),
        // Slashings
        slashings: pre.slashings.clone(),
        // [Modified in EIP8453] epoch participation and Casper FFG fields removed.
        // Inactivity
        // EIP8453-STUB: inactivity scores change semantics at the fork (per-round updates with
        // weight-based ejection); they are carried over unchanged for now.
        inactivity_scores: mem::take(&mut pre.inactivity_scores),
        // Sync committees
        current_sync_committee: pre.current_sync_committee.clone(),
        next_sync_committee: pre.next_sync_committee.clone(),
        // Execution Bid (unchanged from Heze)
        latest_execution_payload_bid: pre.latest_execution_payload_bid.clone(),
        // Capella
        next_withdrawal_index: pre.next_withdrawal_index,
        next_withdrawal_validator_index: pre.next_withdrawal_validator_index,
        historical_summaries: pre.historical_summaries.clone(),
        // Electra
        deposit_balance_to_consume: pre.deposit_balance_to_consume,
        exit_balance_to_consume: pre.exit_balance_to_consume,
        earliest_exit_epoch: pre.earliest_exit_epoch,
        consolidation_balance_to_consume: pre.consolidation_balance_to_consume,
        earliest_consolidation_epoch: pre.earliest_consolidation_epoch,
        pending_deposits: pre.pending_deposits.clone(),
        pending_partial_withdrawals: pre.pending_partial_withdrawals.clone(),
        pending_consolidations: pre.pending_consolidations.clone(),
        proposer_lookahead: mem::take(&mut pre.proposer_lookahead),
        // Gloas
        builders: mem::take(&mut pre.builders),
        next_withdrawal_builder_index: pre.next_withdrawal_builder_index,
        execution_payload_availability: pre.execution_payload_availability.clone(),
        builder_pending_payments: pre.builder_pending_payments.clone(),
        builder_pending_withdrawals: mem::take(&mut pre.builder_pending_withdrawals),
        latest_block_hash: pre.latest_block_hash,
        payload_expected_withdrawals: mem::take(&mut pre.payload_expected_withdrawals),
        ptc_window: pre.ptc_window.clone(),
        // Finality gadget [New in EIP8453]
        target_checkpoint: HeightCheckpoint {
            height: 1,
            // Back-filled with the latest block root by the next `process_slot`.
            root: Default::default(),
        },
        justified_checkpoint: HeightCheckpoint {
            height: 0,
            root: pre_finalized.root,
        },
        finalized_checkpoint: HeightCheckpoint {
            height: 0,
            root: pre_finalized.root,
        },
        target_slot: pre.latest_block_header.slot,
        finalized_slot,
        target_participation: ProgressiveList::try_from_iter(std::iter::repeat_n(
            false,
            validator_count,
        ))?,
        progress_participation: ProgressiveList::try_from_iter(std::iter::repeat_n(
            false,
            validator_count,
        ))?,
        round_participation: ProgressiveList::try_from_iter(std::iter::repeat_n(
            ParticipationFlags::default(),
            validator_count,
        ))?,
        pending_finality: Default::default(),
        head_vote_participation: Default::default(),
        // Caches
        total_active_balance: pre.total_active_balance,
        progressive_balances_cache: mem::take(&mut pre.progressive_balances_cache),
        committee_caches: mem::take(&mut pre.committee_caches),
        pubkey_cache: mem::take(&mut pre.pubkey_cache),
        exit_cache: mem::take(&mut pre.exit_cache),
        slashings_cache: mem::take(&mut pre.slashings_cache),
        epoch_cache: mem::take(&mut pre.epoch_cache),
    });

    Ok(post)
}
