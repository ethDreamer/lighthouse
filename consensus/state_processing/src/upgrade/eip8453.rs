use milhouse::{ProgressiveList, Vector};
use safe_arith::SafeArith;
use ssz_types::FixedVector;
use std::mem;
use tree_hash::TreeHash;
use types::{
    BeaconState, BeaconStateEip8453, BeaconStateError as Error, ChainSpec, EthSpec, Fork,
    ParticipationFlags, Slot, eip8453::HeightCheckpoint,
};

/// Transform a `Heze` state into an `Eip8453` state.
///
/// `upgrade_to_eip8453` from fork.md: the finality gadget starts at height 0 from the last
/// finalized Casper FFG checkpoint, and the target of height 1 is the latest block before the
/// upgrade. `finalized_slot` is the start slot of the pre-fork finalized epoch, so that
/// deposit/activation processing keyed on `finalized_slot` continues from Casper FFG finality.
/// Participation in the last pre-fork epoch is not rewarded. Inactivity scores carry over
/// unchanged despite their semantics changing to per-round updates.
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
    let mut post = BeaconState::Eip8453(BeaconStateEip8453 {
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
            // The root of the latest block before the upgrade. The header's state root has
            // already been back-filled by `process_slot`, so this is the true block root.
            root: pre.latest_block_header.tree_hash_root(),
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
        // Placeholder, initialized below once the post-state exists.
        head_vote_committee_window: Vector::from_elem(FixedVector::from_elem(0))?,
        // Caches
        total_active_balance: pre.total_active_balance,
        progressive_balances_cache: mem::take(&mut pre.progressive_balances_cache),
        committee_caches: mem::take(&mut pre.committee_caches),
        pubkey_cache: mem::take(&mut pre.pubkey_cache),
        exit_cache: mem::take(&mut pre.exit_cache),
        slashings_cache: mem::take(&mut pre.slashings_cache),
        epoch_cache: mem::take(&mut pre.epoch_cache),
    });

    initialize_head_vote_committee_window(&mut post, spec)?;

    Ok(post)
}

/// Initialize the `head_vote_committee_window` field at the fork transition.
///
/// `initialize_head_vote_committee_window` from fork.md: one epoch of empty (all-zero) entries
/// for the previous epoch, then committees computed for the current epoch through
/// `MIN_SEED_LOOKAHEAD` epochs of lookahead.
fn initialize_head_vote_committee_window<E: EthSpec>(
    state: &mut BeaconState<E>,
    spec: &ChainSpec,
) -> Result<(), Error> {
    let slots_per_epoch = E::slots_per_epoch() as usize;

    let empty_previous_epoch =
        vec![FixedVector::<u64, E::HeadVoteCommitteeSize>::from_elem(0); slots_per_epoch];
    let mut committees = empty_previous_epoch;

    let current_epoch = state.current_epoch();
    for e in 0..=spec.min_seed_lookahead.as_u64() {
        let epoch = current_epoch.safe_add(e)?;
        let start_slot = epoch.start_slot(E::slots_per_epoch());
        for i in 0..slots_per_epoch {
            let slot = start_slot.safe_add(Slot::new(i as u64))?;
            let committee = state.compute_head_vote_committee(slot, spec)?;
            let committee_u64: Vec<u64> = committee.into_iter().map(|v| v as u64).collect();
            committees.push(FixedVector::new(committee_u64)?);
        }
    }

    *state.head_vote_committee_window_mut()? = Vector::new(committees)?;

    Ok(())
}
