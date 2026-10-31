//! Runner for the EIP-8453 fork-choice vectors (locally generated; see
//! `tests/core/pyspec/eth_consensus_specs/test/eip8453/fork_choice/` on the consensus-specs
//! `safe-votes-v2` branch).
//!
//! The vectors drive the spec `Store` directly, so this runner drives `FgForkChoiceStore` +
//! `state_processing` rather than a full `BeaconChainHarness`: blocks are state-transitioned
//! here, their post-state FG summaries folded into the store, and the committee/weight
//! dependent state is the latest finalized block's post-state, exactly as the spec reads it.

use super::common::{load_config, testing_spec_with_config};
use super::*;
use crate::decode::{ssz_decode_file, ssz_decode_file_with, ssz_decode_state, yaml_decode_file};
use ::fg_fork_choice::{BlockSummary, FgForkChoiceStore, FgNode};
use serde::Deserialize;
use state_processing::{
    BlockSignatureStrategy, ConsensusContext, VerifyBlockRoot,
    per_block_processing::per_block_processing, per_slot_processing::per_slot_processing,
    upgrade::gloas::GloasVerificationContext,
};
use std::collections::{BTreeMap, HashMap};
use types::{
    BeaconBlock, BeaconState, Hash256, SignedBeaconBlock, Slot,
    eip8453::{HeadVoteMessage, SafeVoteMessage},
};

#[derive(Debug, Clone, Deserialize)]
pub struct NodeCheck {
    pub root: Hash256,
    pub payload_status: u8,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HeadCheck {
    pub slot: u64,
    pub root: Hash256,
    pub payload_status: u8,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HeightCheckpointCheck {
    pub height: u64,
    pub root: Hash256,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FgChecks {
    pub head: HeadCheck,
    pub justified_checkpoint: HeightCheckpointCheck,
    pub finalized_checkpoint: HeightCheckpointCheck,
    pub finalized_slot: u64,
    pub max_height: u64,
    pub grade_2: Option<NodeCheck>,
    pub grade_1: Option<NodeCheck>,
    pub grade_0: Option<NodeCheck>,
    pub confirmed: NodeCheck,
    pub stable: NodeCheck,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum FgStep {
    Tick { tick_ms: u64 },
    Block { block: String, valid: bool },
    HeadVote { head_vote: String },
    SafeVote { safe_vote: String },
    Checks { checks: Box<FgChecks> },
}

#[derive(Debug)]
pub struct FgForkChoiceTest<E: EthSpec> {
    pub path: std::path::PathBuf,
    pub config: Option<types::Config>,
    pub anchor_state: BeaconState<E>,
    pub steps: Vec<FgStep>,
}

impl<E: EthSpec> LoadCase for FgForkChoiceTest<E> {
    fn load_from_dir(path: &Path, fork_name: ForkName) -> Result<Self, Error> {
        let config = load_config(path)?;
        let spec = &testing_spec_with_config::<E>(fork_name, config.as_ref())?;
        let anchor_state = ssz_decode_state(&path.join("anchor_state.ssz_snappy"), spec)?;
        let steps: Vec<FgStep> = yaml_decode_file(&path.join("steps.yaml"))?;
        Ok(Self {
            path: path.to_path_buf(),
            config,
            anchor_state,
            steps,
        })
    }
}

impl<E: EthSpec> Case for FgForkChoiceTest<E> {
    fn is_enabled_for_fork(fork_name: ForkName) -> bool {
        fork_name.eip8453_enabled()
    }

    fn result(&self, _case_index: usize, fork_name: ForkName) -> Result<(), Error> {
        let spec = testing_spec_with_config::<E>(fork_name, self.config.as_ref())?;
        let fail = |message: String| Error::NotEqual(message);

        let anchor_block: BeaconBlock<E> =
            ssz_decode_file_with(&self.path.join("anchor_block.ssz_snappy"), |bytes| {
                BeaconBlock::from_ssz_bytes(bytes, &spec)
            })?;
        let anchor_root = anchor_block.canonical_root();
        let anchor_state = self.anchor_state.clone();

        let (bid_parent_hash, bid_block_hash) = anchor_block
            .body()
            .signed_execution_payload_bid()
            .map(|bid| {
                (
                    bid.message().parent_block_hash(),
                    bid.message().block_hash(),
                )
            })
            .unwrap_or_default();
        let summary = fg_summary(&anchor_state)?;

        let mut store = FgForkChoiceStore::<E>::from_anchor(
            anchor_root,
            anchor_block.parent_root(),
            anchor_block.slot(),
            anchor_block.proposer_index(),
            bid_block_hash,
            bid_parent_hash,
            summary,
            anchor_state.genesis_time(),
            spec.genesis_slot,
            spec.get_slot_duration().as_secs(),
        )
        .map_err(|e| fail(format!("from_anchor: {e:?}")))?;

        let mut states: HashMap<Hash256, BeaconState<E>> = HashMap::new();
        states.insert(anchor_root, anchor_state);
        refresh_dependents(&mut store, &states, &spec)?;

        for (step_index, step) in self.steps.iter().enumerate() {
            match step {
                FgStep::Tick { tick_ms } => {
                    store
                        .on_tick(*tick_ms)
                        .map_err(|e| fail(format!("step {step_index} on_tick: {e:?}")))?;
                    refresh_dependents(&mut store, &states, &spec)?;
                }
                FgStep::Block { block, valid } => {
                    let signed_block: SignedBeaconBlock<E> = ssz_decode_file_with(
                        &self.path.join(format!("{block}.ssz_snappy")),
                        |bytes| SignedBeaconBlock::from_ssz_bytes(bytes, &spec),
                    )?;
                    let result = apply_block(&mut store, &mut states, &signed_block, &spec);
                    if *valid {
                        result.map_err(|e| {
                            fail(format!("step {step_index} valid block failed: {e}"))
                        })?;
                        refresh_dependents(&mut store, &states, &spec)?;
                    } else if result.is_ok() {
                        return Err(fail(format!(
                            "step {step_index}: invalid block was accepted"
                        )));
                    }
                }
                FgStep::HeadVote { head_vote } => {
                    let message: HeadVoteMessage =
                        ssz_decode_file(&self.path.join(format!("{head_vote}.ssz_snappy")))?;
                    store.on_head_vote(message.validator_index, message.data);
                }
                FgStep::SafeVote { safe_vote } => {
                    let message: SafeVoteMessage =
                        ssz_decode_file(&self.path.join(format!("{safe_vote}.ssz_snappy")))?;
                    store.on_safe_vote(message.validator_index, message.data);
                }
                FgStep::Checks { checks } => {
                    run_checks(&store, checks, step_index).map_err(fail)?;
                }
            }
        }

        Ok(())
    }
}

fn fg_summary<E: EthSpec>(state: &BeaconState<E>) -> Result<BlockSummary, Error> {
    let convert = |e: types::BeaconStateError| Error::NotEqual(format!("summary: {e:?}"));
    Ok(BlockSummary {
        justified: state.justified_checkpoint().map_err(convert)?,
        finalized: state.finalized_checkpoint_eip8453().map_err(convert)?,
        finalized_slot: state.finalized_slot().map_err(convert)?,
        target_height: state.target_checkpoint().map_err(convert)?.height,
    })
}

/// State-transition `signed_block` on its parent's post-state and fold the result into the
/// store (the spec's `on_block` runs the state transition inline).
fn apply_block<E: EthSpec>(
    store: &mut FgForkChoiceStore<E>,
    states: &mut HashMap<Hash256, BeaconState<E>>,
    signed_block: &SignedBeaconBlock<E>,
    spec: &ChainSpec,
) -> Result<(), String> {
    let block = signed_block.message();
    let block_root = signed_block.canonical_root();
    let mut state = states
        .get(&block.parent_root())
        .ok_or_else(|| format!("unknown parent {:?}", block.parent_root()))?
        .clone();

    while state.slot() < block.slot() {
        per_slot_processing(
            &mut state,
            None,
            GloasVerificationContext::FullVerification,
            spec,
        )
        .map_err(|e| format!("per_slot_processing: {e:?}"))?;
    }
    state
        .build_caches(spec)
        .map_err(|e| format!("build_caches: {e:?}"))?;

    let mut ctxt = ConsensusContext::new(state.slot());
    per_block_processing(
        &mut state,
        signed_block,
        BlockSignatureStrategy::VerifyIndividual,
        VerifyBlockRoot::True,
        &mut ctxt,
        spec,
    )
    .map_err(|e| format!("per_block_processing: {e:?}"))?;

    let summary = fg_summary(&state).map_err(|e| format!("summary: {e:?}"))?;

    let head_votes: Vec<(u64, types::eip8453::HeadVoteData, bool)> = block
        .body()
        .head_votes()
        .ok()
        .zip(block.body().head_vote_support().ok())
        .map(|(votes, support)| {
            votes
                .iter()
                .zip(support.iter())
                .flat_map(|(vote, support_bits)| {
                    vote.voter_indices
                        .iter()
                        .enumerate()
                        .map(|(position, index)| {
                            (
                                *index,
                                vote.data,
                                support_bits.get(position).unwrap_or(false),
                            )
                        })
                        .collect::<Vec<_>>()
                })
                .collect()
        })
        .unwrap_or_default();
    let safe_votes: Vec<(u64, types::eip8453::SafeVoteData)> = block
        .body()
        .safe_votes()
        .map(|votes| {
            votes
                .iter()
                .flat_map(|vote| {
                    vote.voter_indices
                        .iter()
                        .map(|index| (*index, vote.data))
                        .collect::<Vec<_>>()
                })
                .collect()
        })
        .unwrap_or_default();

    let (bid_parent_hash, bid_block_hash) = block
        .body()
        .signed_execution_payload_bid()
        .map(|bid| {
            (
                bid.message().parent_block_hash(),
                bid.message().block_hash(),
            )
        })
        .unwrap_or_default();

    store
        .on_block(
            block_root,
            block.parent_root(),
            block.slot(),
            block.proposer_index(),
            bid_block_hash,
            bid_parent_hash,
            summary,
            head_votes,
            safe_votes.into_iter(),
        )
        .map_err(|e| format!("fg on_block: {e:?}"))?;

    states.insert(block_root, state);
    Ok(())
}

/// Refresh the committee and weight snapshots from the latest finalized block's post-state —
/// the spec's dependent state (`get_head_vote_committee_at_slot`), advanced through empty
/// slots when a needed slot lies beyond its committee window.
fn refresh_dependents<E: EthSpec>(
    store: &mut FgForkChoiceStore<E>,
    states: &HashMap<Hash256, BeaconState<E>>,
    spec: &ChainSpec,
) -> Result<(), Error> {
    let Some(finalized_state) = states.get(&store.finalized_checkpoint.root) else {
        return Ok(());
    };

    store.set_finalized_weights(fg_weights(finalized_state, spec));

    let needed_end = store
        .get_current_slot()
        .saturating_add(E::slots_per_epoch());
    let mut committees: BTreeMap<Slot, Vec<u64>> = BTreeMap::new();
    let mut dependent = finalized_state.clone();
    let mut slot = spec.genesis_slot;
    while slot <= needed_end {
        match dependent.get_head_vote_committee(slot, spec) {
            Ok(committee) => {
                committees.insert(slot, committee.into_iter().map(|v| v as u64).collect());
                slot = slot.saturating_add(1u64);
            }
            Err(_) => {
                // Either before the dependent window (skip the slot) or beyond it (advance the
                // dependent state one slot, as the spec's `process_slots` fallback does).
                if slot < dependent.slot() {
                    slot = slot.saturating_add(1u64);
                } else {
                    dependent.build_caches(spec).map_err(|e| {
                        Error::NotEqual(format!("dependent cache build failed: {e:?}"))
                    })?;
                    per_slot_processing(
                        &mut dependent,
                        None,
                        GloasVerificationContext::FullVerification,
                        spec,
                    )
                    .map_err(|e| {
                        Error::NotEqual(format!("dependent state advance failed: {e:?}"))
                    })?;
                }
            }
        }
    }
    store.set_committees(committees);
    Ok(())
}

/// `get_validator_weight` over the dependent state.
fn fg_weights<E: EthSpec>(state: &BeaconState<E>, spec: &ChainSpec) -> HashMap<u64, u128> {
    let ejection_score = E::ejection_inactivity_score();
    let mut weights = HashMap::new();
    let Ok(scores) = state.inactivity_scores() else {
        return weights;
    };
    for (index, validator) in state.validators().iter().enumerate() {
        let increments = validator
            .effective_balance
            .checked_div(spec.effective_balance_increment)
            .unwrap_or(0);
        let score = std::cmp::min(scores.get(index).copied().unwrap_or(0), ejection_score);
        let margin = (ejection_score.saturating_sub(score) as u128)
            .saturating_mul(ejection_score.saturating_add(score) as u128);
        weights.insert(index as u64, (increments as u128).saturating_mul(margin));
    }
    weights
}

fn check_node(
    actual: Option<FgNode>,
    expected: &Option<NodeCheck>,
    what: &str,
    step: usize,
) -> Result<(), String> {
    match (actual, expected) {
        (None, None) => Ok(()),
        (Some(node), Some(check))
            if node.root == check.root && node.payload_status == check.payload_status =>
        {
            Ok(())
        }
        _ => Err(format!(
            "step {step}: {what} mismatch: actual {actual:?}, expected {expected:?}"
        )),
    }
}

fn run_checks<E: EthSpec>(
    store: &FgForkChoiceStore<E>,
    checks: &FgChecks,
    step: usize,
) -> Result<(), String> {
    let head = store
        .get_head(None)
        .map_err(|e| format!("step {step}: get_head: {e:?}"))?;
    let head_slot = store
        .block_meta(&head.root)
        .map(|meta| meta.slot.as_u64())
        .ok_or_else(|| format!("step {step}: head block missing"))?;
    if head.root != checks.head.root
        || head.payload_status != checks.head.payload_status
        || head_slot != checks.head.slot
    {
        return Err(format!(
            "step {step}: head mismatch: actual (slot {head_slot}, {:?}, status {}), expected \
             (slot {}, {:?}, status {})",
            head.root,
            head.payload_status,
            checks.head.slot,
            checks.head.root,
            checks.head.payload_status,
        ));
    }

    let justified = store.justified_checkpoint;
    if justified.height != checks.justified_checkpoint.height
        || justified.root != checks.justified_checkpoint.root
    {
        return Err(format!(
            "step {step}: justified mismatch: actual {justified:?}, expected {:?}",
            checks.justified_checkpoint
        ));
    }
    let finalized = store.finalized_checkpoint;
    if finalized.height != checks.finalized_checkpoint.height
        || finalized.root != checks.finalized_checkpoint.root
    {
        return Err(format!(
            "step {step}: finalized mismatch: actual {finalized:?}, expected {:?}",
            checks.finalized_checkpoint
        ));
    }
    if store.finalized_slot.as_u64() != checks.finalized_slot {
        return Err(format!(
            "step {step}: finalized_slot mismatch: actual {}, expected {}",
            store.finalized_slot, checks.finalized_slot
        ));
    }
    if store.max_height != checks.max_height {
        return Err(format!(
            "step {step}: max_height mismatch: actual {}, expected {}",
            store.max_height, checks.max_height
        ));
    }

    check_node(store.grade_2, &checks.grade_2, "grade_2", step)?;
    check_node(store.grade_1, &checks.grade_1, "grade_1", step)?;
    check_node(store.grade_0, &checks.grade_0, "grade_0", step)?;
    check_node(
        Some(store.get_confirmed()),
        &Some(checks.confirmed.clone()),
        "confirmed",
        step,
    )?;
    check_node(
        Some(store.get_stable()),
        &Some(checks.stable.clone()),
        "stable",
        step,
    )?;

    Ok(())
}
