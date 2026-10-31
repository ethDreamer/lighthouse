//! EIP-8453 (decoupled consensus) fork choice: a standalone, spec-shaped implementation of
//! `specs/_features/eip8453/fork-choice.md` (branch `safe-votes-v2`).
//!
//! This deliberately mirrors the pyspec `Store` rather than extending `proto_array`: head votes
//! carry weight for one slot only and safe votes for one expiry window, so there is nothing to
//! incrementalize — every score is recomputed per call over at most a committee of seats. The
//! store keeps per-block *metadata* instead of full blocks/states (Lighthouse's store owns
//! those); the caller folds each imported block's post-state FG summary via [`BlockSummary`].
//!
//! Prototype honesty notes (all greppable):
//! - EIP8453-STUB: PTC payload timeliness / data availability votes are not tracked; a payload
//!   counts as timely and available once verified, and the payload-status tiebreaker reduces to
//!   the raw status.
//! - Head-vote committees are recorded by the caller (`record_head_vote_committee` /
//!   `insert_committee`) from the processing state; a walk over a slot with no recorded
//!   committee scores zero instead of failing, since no votes could have been verified for it
//!   either.

use safe_arith::ArithError;
use std::collections::{BTreeMap, HashMap, HashSet};
use types::consts::eip8453::{GENESIS_ROUND, Round};
use types::consts::gloas::{
    PAYLOAD_STATUS_EMPTY, PAYLOAD_STATUS_FULL, PAYLOAD_STATUS_PENDING, PayloadStatus,
};
use types::eip8453::{
    HeadVoteData, HeightCheckpoint, SafeVoteData, compute_round_at_slot,
    compute_start_slot_at_round,
};
use types::{EthSpec, ExecutionBlockHash, Hash256, Slot};

/// Spec: `SLOW_LOCK_WINDOW` — the slow-confirmation window, in slots.
pub const SLOW_LOCK_WINDOW: u64 = 4;

/// Spec: `INFINITE_TIME_MS` — the resolution time of an unresolved vote.
pub const INFINITE_TIME_MS: u64 = u64::MAX;

#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    MissingBlock(Hash256),
    MissingCommittee(Slot),
    Arith(ArithError),
}

impl From<ArithError> for Error {
    fn from(e: ArithError) -> Self {
        Error::Arith(e)
    }
}

/// The spec's `ForkChoiceNode`: a block root together with a payload status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FgNode {
    pub root: Hash256,
    pub payload_status: PayloadStatus,
}

impl FgNode {
    pub fn pending(root: Hash256) -> Self {
        Self {
            root,
            payload_status: PAYLOAD_STATUS_PENDING,
        }
    }
}

/// A pooled head-vote P2P message: one voter, the signed data, and the local receipt time
/// (the spec's `head_vote_messages` entry joined with `head_vote_message_receipt_times`).
#[derive(Debug, Clone, PartialEq)]
pub struct StoreHeadVote {
    pub validator_index: u64,
    pub data: HeadVoteData,
    pub receipt_ms: u64,
}

/// The spec's `StoreSafeVote`.
#[derive(Debug, Clone, PartialEq)]
pub struct StoreSafeVote {
    pub validator_index: u64,
    pub data: SafeVoteData,
    pub arrival_ms: u64,
}

/// The spec's `ACVote`: an individual available-chain vote in set form.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AcVote {
    pub slot: Slot,
    pub validator_index: u64,
    pub supported_node: FgNode,
}

/// The spec's `FGFrame`: the finality gadget's constraints on the available-chain walks.
#[derive(Debug, Clone)]
pub struct FgFrame {
    pub finalized_node: FgNode,
    pub forkchoice_root: FgNode,
    pub tree: Vec<FgNode>,
    tree_set: HashSet<FgNode>,
    pub viable_heads: HashSet<FgNode>,
}

/// Per-block metadata standing in for the spec Store's `blocks`/`block_states` entries.
#[derive(Debug, Clone, PartialEq)]
pub struct BlockMeta {
    pub parent_root: Hash256,
    pub slot: Slot,
    pub proposer_index: u64,
    /// `block_states[root].target_checkpoint.height`: drives viability filtering.
    pub state_target_height: u64,
    /// The block's bid `block_hash`/`parent_block_hash`, for parent payload status.
    pub bid_block_hash: ExecutionBlockHash,
    pub bid_parent_hash: ExecutionBlockHash,
    /// The head votes the block carried, with each voter's published support bit. Block-carried
    /// votes are never pooled: the view builders read them from here on demand.
    pub carried_head_votes: Vec<(u64, HeadVoteData, bool)>,
}

/// The FG summary of an imported block's post-state, folded by `update_finality`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BlockSummary {
    pub justified: HeightCheckpoint,
    pub finalized: HeightCheckpoint,
    pub finalized_slot: Slot,
    pub target_height: u64,
}

/// The spec's fork-choice `Store` for EIP-8453.
#[derive(Debug, Clone)]
pub struct FgForkChoiceStore<E> {
    pub time_ms: u64,
    pub genesis_time_ms: u64,
    slot_duration_ms: u64,
    genesis_slot: Slot,

    // FG summary
    pub justified_checkpoint: HeightCheckpoint,
    pub finalized_checkpoint: HeightCheckpoint,
    pub finalized_slot: Slot,
    pub max_height: u64,

    // Block DAG metadata
    blocks: HashMap<Hash256, BlockMeta>,
    children: HashMap<Hash256, Vec<Hash256>>,
    payload_verified: HashSet<Hash256>,
    block_receipt_times: HashMap<Hash256, u64>,
    payload_receipt_times: HashMap<Hash256, u64>,

    // Vote pools
    pub head_vote_messages: BTreeMap<Slot, Vec<StoreHeadVote>>,
    pub safe_votes: BTreeMap<Round, Vec<StoreSafeVote>>,

    // Confirmation state
    pub fast_confirmed_path: HashSet<FgNode>,
    pub latest_confirmed: FgNode,
    pub saved_confirmations: BTreeMap<Slot, FgNode>,
    /// The lock set: expiry deadline (ms) to locked node.
    pub locks: BTreeMap<u64, FgNode>,

    // Stabilization grades
    pub grade_2: Option<FgNode>,
    pub grade_1: Option<FgNode>,
    pub grade_0: Option<FgNode>,
    pub grade_2_round: Round,
    pub grade_1_round: Round,
    pub grade_0_round: Round,
    pub latest_stable: FgNode,

    /// Head-vote committees per slot, recorded from the processing state (the spec's
    /// `head_vote_committees`).
    committees: BTreeMap<Slot, Vec<u64>>,

    /// FG validator weights from the latest finalized block's post-state, for the stabilization
    /// rule's relative majority.
    finalized_weights: HashMap<u64, u128>,

    _phantom: std::marker::PhantomData<E>,
}

impl<E: EthSpec> FgForkChoiceStore<E> {
    /// The spec's `get_forkchoice_store`, from an anchor block's metadata and post-state summary.
    ///
    /// As in prior forks, the anchor stands in for both the justified and the finalized root.
    #[allow(clippy::too_many_arguments)]
    pub fn from_anchor(
        anchor_root: Hash256,
        anchor_parent_root: Hash256,
        anchor_slot: Slot,
        anchor_proposer_index: u64,
        anchor_bid_block_hash: ExecutionBlockHash,
        anchor_bid_parent_hash: ExecutionBlockHash,
        summary: BlockSummary,
        genesis_time: u64,
        genesis_slot: Slot,
        seconds_per_slot: u64,
    ) -> Result<Self, Error> {
        let genesis_time_ms = genesis_time.saturating_mul(1000);
        let slot_duration_ms = seconds_per_slot.saturating_mul(1000);
        let time_ms = genesis_time_ms.saturating_add(
            anchor_slot
                .as_u64()
                .saturating_sub(genesis_slot.as_u64())
                .saturating_mul(slot_duration_ms),
        );
        let anchor_node = FgNode::pending(anchor_root);
        let mut blocks = HashMap::new();
        blocks.insert(
            anchor_root,
            BlockMeta {
                parent_root: anchor_parent_root,
                slot: anchor_slot,
                proposer_index: anchor_proposer_index,
                state_target_height: summary.target_height,
                bid_block_hash: anchor_bid_block_hash,
                bid_parent_hash: anchor_bid_parent_hash,
                carried_head_votes: Vec::new(),
            },
        );
        let mut block_receipt_times = HashMap::new();
        block_receipt_times.insert(anchor_root, time_ms);
        Ok(Self {
            time_ms,
            genesis_time_ms,
            slot_duration_ms,
            genesis_slot,
            justified_checkpoint: HeightCheckpoint {
                height: summary.justified.height,
                root: anchor_root,
            },
            finalized_checkpoint: HeightCheckpoint {
                height: summary.finalized.height,
                root: anchor_root,
            },
            finalized_slot: summary.finalized_slot,
            max_height: summary.target_height,
            blocks,
            children: HashMap::new(),
            payload_verified: HashSet::new(),
            block_receipt_times,
            payload_receipt_times: HashMap::new(),
            head_vote_messages: BTreeMap::new(),
            safe_votes: BTreeMap::new(),
            fast_confirmed_path: HashSet::new(),
            latest_confirmed: anchor_node,
            saved_confirmations: BTreeMap::new(),
            locks: BTreeMap::new(),
            grade_2: None,
            grade_1: None,
            grade_0: None,
            grade_2_round: GENESIS_ROUND,
            grade_1_round: GENESIS_ROUND,
            grade_0_round: GENESIS_ROUND,
            latest_stable: anchor_node,
            committees: BTreeMap::new(),
            finalized_weights: HashMap::new(),
            _phantom: std::marker::PhantomData,
        })
    }

    /* ---------------- Time ---------------- */

    pub fn get_current_slot(&self) -> Slot {
        self.slot_at_time_ms(self.time_ms)
    }

    /// `compute_slot_at_time_ms`.
    fn slot_at_time_ms(&self, time_ms: u64) -> Slot {
        let since_genesis_ms = time_ms.saturating_sub(self.genesis_time_ms);
        let slots = since_genesis_ms
            .checked_div(self.slot_duration_ms)
            .unwrap_or(0);
        self.genesis_slot.saturating_add(slots)
    }

    fn slot_component_duration_ms(&self, bps: u64) -> u64 {
        self.slot_duration_ms
            .saturating_mul(bps)
            .checked_div(10_000)
            .unwrap_or(0)
    }

    /// `compute_time_at_slot_ms`.
    fn slot_start_ms(&self, slot: Slot) -> u64 {
        self.genesis_time_ms.saturating_add(
            slot.as_u64()
                .saturating_sub(self.genesis_slot.as_u64())
                .saturating_mul(self.slot_duration_ms),
        )
    }

    /// `get_time_into_slot_ms`.
    fn time_into_slot_ms(&self) -> u64 {
        self.time_ms
            .saturating_sub(self.slot_start_ms(self.get_current_slot()))
    }

    /// `get_head_vote_due_ms` (`HEAD_VOTE_DUE_BPS`).
    fn head_vote_due_ms(&self) -> u64 {
        self.slot_component_duration_ms(2500)
    }

    /// `get_support_cutoff_ms` (`SUPPORT_CUTOFF_BPS`).
    fn support_cutoff_ms(&self) -> u64 {
        self.slot_component_duration_ms(5000)
    }

    /// `get_view_freeze_ms` (`VIEW_FREEZE_BPS`).
    fn view_freeze_ms(&self) -> u64 {
        self.slot_component_duration_ms(7500)
    }

    /// The spec's `on_tick`.
    ///
    /// The spec schedules the unlocking and confirmation duties on exact quarter-slot offsets
    /// and expects a tick at each; this driver synthesizes every duty boundary crossed since the
    /// last tick, in order, so sparse callers observe the same store a boundary-exact ticker
    /// would (each duty runs with the store time of the preceding tick, as the spec's ordering
    /// has it).
    pub fn on_tick(&mut self, time_ms: u64) -> Result<(), Error> {
        if time_ms < self.time_ms {
            return Ok(());
        }
        let first_slot = self.slot_at_time_ms(self.time_ms);
        let last_slot = self.slot_at_time_ms(time_ms);
        let mut slot = first_slot;
        while slot <= last_slot {
            if slot > self.genesis_slot {
                let unlock_boundary = self
                    .slot_start_ms(slot)
                    .saturating_add(self.head_vote_due_ms());
                if self.time_ms < unlock_boundary && unlock_boundary <= time_ms {
                    self.unlock(slot.saturating_sub(1u64), unlock_boundary)?;
                    self.time_ms = unlock_boundary;
                    self.post_tick_updates()?;
                }
                let confirmation_boundary = self
                    .slot_start_ms(slot)
                    .saturating_add(self.support_cutoff_ms());
                if self.time_ms < confirmation_boundary && confirmation_boundary <= time_ms {
                    self.update_confirmation(slot.saturating_sub(1u64))?;
                    self.time_ms = confirmation_boundary;
                    self.post_tick_updates()?;
                }
            }
            slot = slot.saturating_add(1u64);
        }
        self.time_ms = time_ms;
        self.post_tick_updates()
    }

    /// The tail of the spec's `on_tick`: the updates run on every tick after the time advances.
    fn post_tick_updates(&mut self) -> Result<(), Error> {
        self.update_grades()?;
        self.update_sg_lock()?;
        self.update_stable_record()?;
        Ok(())
    }

    /// Advance the clock to the start of `slot`.
    pub fn on_tick_at_slot(&mut self, slot: Slot) -> Result<(), Error> {
        self.on_tick(self.slot_start_ms(slot))
    }

    /* ---------------- Blocks ---------------- */

    pub fn contains_block(&self, root: &Hash256) -> bool {
        self.blocks.contains_key(root)
    }

    pub fn block_meta(&self, root: &Hash256) -> Option<&BlockMeta> {
        self.blocks.get(root)
    }

    fn block(&self, root: &Hash256) -> Result<&BlockMeta, Error> {
        self.blocks.get(root).ok_or(Error::MissingBlock(*root))
    }

    /// The slot of a known block.
    pub fn block_slot(&self, root: &Hash256) -> Option<Slot> {
        self.blocks.get(root).map(|meta| meta.slot)
    }

    /// The spec's `on_block`, minus the state transition (the caller has already imported and
    /// verified the block; carried votes were verified by the state transition).
    ///
    /// The caller also records the head-vote committees of the carried votes' slots from the
    /// block's post-state (`record_head_vote_committee`).
    #[allow(clippy::too_many_arguments)]
    pub fn on_block(
        &mut self,
        block_root: Hash256,
        parent_root: Hash256,
        slot: Slot,
        proposer_index: u64,
        bid_block_hash: ExecutionBlockHash,
        bid_parent_hash: ExecutionBlockHash,
        summary: BlockSummary,
        carried_head_votes: Vec<(u64, HeadVoteData, bool)>,
        carried_safe_votes: impl Iterator<Item = (u64, SafeVoteData)>,
    ) -> Result<(), Error> {
        // Return early if the block is already known.
        if self.blocks.contains_key(&block_root) {
            return Ok(());
        }
        // Parent block must be known.
        if !self.blocks.contains_key(&parent_root) {
            return Err(Error::MissingBlock(parent_root));
        }

        self.blocks.insert(
            block_root,
            BlockMeta {
                parent_root,
                slot,
                proposer_index,
                state_target_height: summary.target_height,
                bid_block_hash,
                bid_parent_hash,
                carried_head_votes,
            },
        );
        self.children
            .entry(parent_root)
            .or_default()
            .push(block_root);

        // Fold the post-state's FG summary into the store.
        self.update_finality(summary)?;

        // `record_block_receipt_time`.
        self.block_receipt_times.insert(block_root, self.time_ms);

        // The safe votes the block carries enter the pool; the carried head votes stay on the
        // block and are read by the view builders on demand.
        for (validator_index, data) in carried_safe_votes {
            self.record_safe_vote(validator_index, data);
        }

        Ok(())
    }

    /// Mark a payload verified and record its receipt time
    /// (`on_execution_payload_envelope` + `record_payload_receipt_time`).
    pub fn set_payload_verified(&mut self, root: Hash256) {
        if self.payload_verified.insert(root) {
            self.payload_receipt_times.insert(root, self.time_ms);
        }
    }

    pub fn is_payload_verified(&self, root: &Hash256) -> bool {
        self.payload_verified.contains(root)
    }

    /// The inherited `should_extend_payload`.
    ///
    /// EIP8453-STUB: inclusion-list satisfaction and the PTC timeliness/data-availability votes
    /// are not tracked yet, so they collapse to payload verification.
    pub fn should_extend_payload(&self, root: &Hash256) -> bool {
        self.is_payload_verified(root)
    }

    /* ---------------- Committees and weights ---------------- */

    /// The spec's `record_head_vote_committee`: store the committee for a vote from the previous
    /// or current slot, assuming all branches agree on the committee for a given slot.
    pub fn record_head_vote_committee(&mut self, slot: Slot, committee: Vec<u64>) {
        if slot.saturating_add(1u64) >= self.get_current_slot() {
            self.committees.entry(slot).or_insert(committee);
        }
    }

    /// Merge one slot's committee unconditionally (anchor seeding and lookahead windows).
    pub fn insert_committee(&mut self, slot: Slot, committee: Vec<u64>) {
        self.committees.insert(slot, committee);
    }

    /// Replace the recorded committees wholesale.
    pub fn set_committees(&mut self, committees: BTreeMap<Slot, Vec<u64>>) {
        self.committees = committees;
    }

    /// Replace the FG validator weights (from the latest finalized block's post-state).
    pub fn set_finalized_weights(&mut self, weights: HashMap<u64, u128>) {
        self.finalized_weights = weights;
    }

    pub fn committee_at_slot(&self, slot: Slot) -> Result<&Vec<u64>, Error> {
        self.committees
            .get(&slot)
            .ok_or(Error::MissingCommittee(slot))
    }

    pub fn max_committee_slot(&self) -> Option<Slot> {
        self.committees.keys().next_back().copied()
    }

    /// The spec's `get_head_vote_committee_seats_count`.
    fn committee_seats_count() -> u64 {
        E::head_vote_committee_size() as u64
    }

    /// The spec's `get_validator_seats_count`.
    ///
    /// A slot with no recorded committee scores zero rather than failing: no vote for it could
    /// have been verified either.
    fn validator_seats_count(&self, slot: Slot, validator_index: u64) -> u64 {
        self.committees
            .get(&slot)
            .map(|committee| {
                committee
                    .iter()
                    .filter(|&&index| index == validator_index)
                    .count() as u64
            })
            .unwrap_or(0)
    }

    /* ---------------- Vote pools ---------------- */

    /// The spec's `on_head_vote_message` + `record_head_vote_message`, minus the signature and
    /// committee-membership validation (`is_valid_head_vote_message`), which the caller runs
    /// against the pulled-up head state. A refused message is ignored, not consumed.
    pub fn on_head_vote_message(&mut self, validator_index: u64, data: HeadVoteData) {
        if ![
            PAYLOAD_STATUS_EMPTY,
            PAYLOAD_STATUS_FULL,
            PAYLOAD_STATUS_PENDING,
        ]
        .contains(&data.payload_status)
        {
            return;
        }
        // The vote must be from the previous or current slot.
        let current_slot = self.get_current_slot();
        if data.slot.saturating_add(1u64) < current_slot || data.slot > current_slot {
            return;
        }
        let pool = self.head_vote_messages.entry(data.slot).or_default();
        // The message must not have been seen.
        if pool
            .iter()
            .any(|vote| vote.validator_index == validator_index && vote.data == data)
        {
            return;
        }
        pool.push(StoreHeadVote {
            validator_index,
            data,
            receipt_ms: self.time_ms,
        });
    }

    /// The spec's `record_safe_vote`: at most one copy of each vote and at most two distinct
    /// votes per validator per round — the second is equivocation evidence. A refused vote is
    /// not consumed.
    pub fn record_safe_vote(&mut self, validator_index: u64, data: SafeVoteData) {
        let current_round = compute_round_at_slot::<E>(self.get_current_slot());
        if data.round > current_round {
            return;
        }
        if data.round.saturating_add(E::safe_vote_expiry_rounds()) < current_round {
            return;
        }
        let votes = self.safe_votes.entry(data.round).or_default();
        let mut distinct: Vec<&SafeVoteData> = Vec::new();
        for vote in votes.iter() {
            if vote.validator_index != validator_index {
                continue;
            }
            if vote.data == data {
                return;
            }
            if !distinct.contains(&&vote.data) {
                distinct.push(&vote.data);
            }
        }
        if distinct.len() >= 2 {
            return;
        }
        votes.push(StoreSafeVote {
            validator_index,
            data,
            arrival_ms: self.time_ms,
        });
    }

    /// The spec's `on_safe_vote` (payload-status check + record).
    pub fn on_safe_vote(&mut self, validator_index: u64, data: SafeVoteData) {
        if ![
            PAYLOAD_STATUS_EMPTY,
            PAYLOAD_STATUS_FULL,
            PAYLOAD_STATUS_PENDING,
        ]
        .contains(&data.payload_status)
        {
            return;
        }
        self.record_safe_vote(validator_index, data);
    }

    /* ---------------- Ancestry ---------------- */

    /// The spec's `get_parent_payload_status`.
    fn parent_payload_status(&self, meta: &BlockMeta) -> Result<PayloadStatus, Error> {
        let parent = self.block(&meta.parent_root)?;
        Ok(if meta.bid_parent_hash == parent.bid_block_hash {
            PAYLOAD_STATUS_FULL
        } else {
            PAYLOAD_STATUS_EMPTY
        })
    }

    /// The spec's `get_node_parent`.
    fn node_parent(&self, node: &FgNode) -> Result<Option<FgNode>, Error> {
        if node.payload_status != PAYLOAD_STATUS_PENDING {
            return Ok(Some(FgNode::pending(node.root)));
        }
        let meta = self.block(&node.root)?;
        if !self.blocks.contains_key(&meta.parent_root) {
            return Ok(None);
        }
        Ok(Some(FgNode {
            root: meta.parent_root,
            payload_status: self.parent_payload_status(meta)?,
        }))
    }

    /// The spec's `get_ancestor` (iterative).
    pub fn get_ancestor(&self, node: FgNode, slot: Slot) -> Result<FgNode, Error> {
        let mut node = node;
        loop {
            let meta = self.block(&node.root)?;
            if meta.slot <= slot {
                return Ok(node);
            }
            node = FgNode {
                root: meta.parent_root,
                payload_status: self.parent_payload_status(meta)?,
            };
        }
    }

    /// The spec's `is_ancestor` over fork-choice nodes: is `ancestor` an ancestor of `node`?
    pub fn is_ancestor(&self, node: FgNode, ancestor: FgNode) -> Result<bool, Error> {
        let ancestor_slot = self.block(&ancestor.root)?.slot;
        let node_ancestor = self.get_ancestor(node, ancestor_slot)?;
        if node_ancestor.root != ancestor.root {
            return Ok(false);
        }
        Ok(node_ancestor.payload_status == ancestor.payload_status
            || ancestor.payload_status == PAYLOAD_STATUS_PENDING)
    }

    /// `is_ancestor` where an unknown root means "not an ancestor".
    fn is_ancestor_or_false(&self, node: FgNode, ancestor: FgNode) -> bool {
        self.is_ancestor(node, ancestor).unwrap_or(false)
    }

    /// The spec's `is_ancestor_root`.
    pub fn is_ancestor_root(
        &self,
        ancestor_root: Hash256,
        descendant_root: Hash256,
    ) -> Result<bool, Error> {
        self.is_ancestor(
            FgNode::pending(descendant_root),
            FgNode::pending(ancestor_root),
        )
    }

    /// The spec's `is_strict_ancestor_root`.
    fn is_strict_ancestor_root(
        &self,
        ancestor_root: Hash256,
        descendant_root: Hash256,
    ) -> Result<bool, Error> {
        Ok(ancestor_root != descendant_root
            && self.is_ancestor_root(ancestor_root, descendant_root)?)
    }

    /// The spec's `get_node_children`.
    fn node_children(&self, node: &FgNode) -> Result<Vec<FgNode>, Error> {
        if node.payload_status == PAYLOAD_STATUS_PENDING {
            let mut children = Vec::with_capacity(2);
            children.push(FgNode {
                root: node.root,
                payload_status: PAYLOAD_STATUS_EMPTY,
            });
            if self.is_payload_verified(&node.root) {
                children.push(FgNode {
                    root: node.root,
                    payload_status: PAYLOAD_STATUS_FULL,
                });
            }
            Ok(children)
        } else {
            let mut children = Vec::new();
            for child_root in self
                .children
                .get(&node.root)
                .map(Vec::as_slice)
                .unwrap_or(&[])
            {
                let meta = self.block(child_root)?;
                if self.parent_payload_status(meta)? == node.payload_status {
                    children.push(FgNode::pending(*child_root));
                }
            }
            Ok(children)
        }
    }

    /// The spec's `get_subtree` (iterative).
    fn get_subtree(&self, subtree_root: FgNode) -> Result<Vec<FgNode>, Error> {
        let mut subtree = Vec::new();
        let mut stack = vec![subtree_root];
        while let Some(node) = stack.pop() {
            subtree.push(node);
            stack.extend(self.node_children(&node)?);
        }
        Ok(subtree)
    }

    /// The spec's `get_deepest_node`: highest slot, payload decisions deeper than pending, with
    /// the root as a deterministic final tiebreaker.
    fn get_deepest_node(&self, nodes: impl IntoIterator<Item = FgNode>) -> Option<FgNode> {
        nodes.into_iter().max_by_key(|node| {
            (
                self.blocks.get(&node.root).map(|meta| meta.slot),
                u64::from(node.payload_status != PAYLOAD_STATUS_PENDING),
                node.root,
            )
        })
    }

    /// The spec's `is_compatible`: do the nodes belong to one chain?
    fn is_compatible(&self, node_1: FgNode, node_2: FgNode) -> Result<bool, Error> {
        if self.get_deepest_node([node_1, node_2]) == Some(node_1) {
            self.is_ancestor(node_1, node_2)
        } else {
            self.is_ancestor(node_2, node_1)
        }
    }

    /// The spec's `get_payload_status_tiebreaker`.
    ///
    /// EIP8453-STUB: the previous-slot payload-decision refinement (which consults
    /// `should_extend_payload` over PTC votes) is reduced to the raw payload status.
    fn payload_status_tiebreaker(&self, node: &FgNode) -> u8 {
        node.payload_status
    }

    /// The inherited `is_previous_slot_payload_decision`.
    fn is_previous_slot_payload_decision(&self, node: &FgNode) -> Result<bool, Error> {
        let meta = self.block(&node.root)?;
        Ok(meta.slot.saturating_add(1u64) == self.get_current_slot()
            && node.payload_status != PAYLOAD_STATUS_PENDING)
    }

    /* ---------------- Finality gadget frame ---------------- */

    /// The spec's `is_fg_contest`: the maximum known height is exactly one above the justified
    /// height and a block at that height extends finalized but conflicts with justified.
    fn is_fg_contest(&self) -> Result<bool, Error> {
        if self.max_height > self.justified_checkpoint.height.saturating_add(1) {
            return Ok(false);
        }
        for (root, meta) in &self.blocks {
            if meta.state_target_height != self.max_height {
                continue;
            }
            if !self.is_ancestor_root(self.finalized_checkpoint.root, *root)? {
                continue;
            }
            if !self.is_ancestor_root(self.justified_checkpoint.root, *root)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// The spec's `is_fg_viable_block`: the height floor depends on FG contest.
    fn is_fg_viable_block(&self, block_root: Hash256, contest: bool) -> Result<bool, Error> {
        let target_height = self.block(&block_root)?.state_target_height;
        Ok(if contest {
            target_height >= self.max_height
        } else {
            target_height.saturating_add(1) >= self.max_height
        })
    }

    /// The spec's `has_viable_descendant`.
    fn has_viable_descendant(&self, block_root: Hash256) -> Result<bool, Error> {
        let contest = self.is_fg_contest()?;
        for root in self.blocks.keys() {
            if self.is_ancestor_root(block_root, *root)?
                && self.is_fg_viable_block(*root, contest)?
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// The spec's `get_fg_frame`.
    pub fn get_fg_frame(&self) -> Result<FgFrame, Error> {
        let finalized_node = FgNode::pending(self.finalized_checkpoint.root);
        let tree = self.get_subtree(finalized_node)?;
        let tree_set: HashSet<FgNode> = tree.iter().copied().collect();

        let contest = self.is_fg_contest()?;
        let forkchoice_block_root =
            if self.max_height == self.justified_checkpoint.height.saturating_add(1) && !contest {
                self.justified_checkpoint.root
            } else {
                self.finalized_checkpoint.root
            };
        let forkchoice_root = FgNode::pending(forkchoice_block_root);

        let mut viable_heads = HashSet::new();
        for node in &tree {
            if self.is_ancestor(*node, forkchoice_root)?
                && self.is_fg_viable_block(node.root, contest)?
            {
                viable_heads.insert(*node);
            }
        }

        Ok(FgFrame {
            finalized_node,
            forkchoice_root,
            tree,
            tree_set,
            viable_heads,
        })
    }

    /// The spec's `get_viable_subtree`: descendants of the forkchoice root with a viable head
    /// above them, within `tree`.
    fn get_viable_subtree(&self, frame: &FgFrame, tree: &[FgNode]) -> Result<Vec<FgNode>, Error> {
        let viable_in_tree: Vec<FgNode> = tree
            .iter()
            .copied()
            .filter(|node| frame.viable_heads.contains(node))
            .collect();
        let mut subtree = Vec::new();
        for node in tree {
            if !self.is_ancestor(*node, frame.forkchoice_root)? {
                continue;
            }
            for viable_head in &viable_in_tree {
                if self.is_ancestor(*viable_head, *node)? {
                    subtree.push(*node);
                    break;
                }
            }
        }
        Ok(subtree)
    }

    /* ---------------- Views ---------------- */

    /// The spec's `get_block_and_payload_receipt_time`.
    fn block_and_payload_receipt_time(&self, node: &FgNode) -> u64 {
        let block_receipt = self
            .block_receipt_times
            .get(&node.root)
            .copied()
            .unwrap_or(INFINITE_TIME_MS);
        if node.payload_status == PAYLOAD_STATUS_FULL {
            let payload_receipt = self
                .payload_receipt_times
                .get(&node.root)
                .copied()
                .unwrap_or(INFINITE_TIME_MS);
            std::cmp::max(block_receipt, payload_receipt)
        } else {
            block_receipt
        }
    }

    /// The spec's `get_vote_resolution_time` (`INFINITE_TIME_MS` when unresolved).
    fn vote_resolution_time(
        &self,
        supported_node: &FgNode,
        vote_slot: Slot,
        vote_receipt_ms: u64,
    ) -> u64 {
        // Votes must be for a known block.
        let Some(meta) = self.blocks.get(&supported_node.root) else {
            return INFINITE_TIME_MS;
        };
        // The supported block must not be later than the vote's slot.
        if meta.slot > vote_slot {
            return INFINITE_TIME_MS;
        }
        // Votes for a block in the vote's own slot must support PENDING.
        if meta.slot == vote_slot && supported_node.payload_status != PAYLOAD_STATUS_PENDING {
            return INFINITE_TIME_MS;
        }
        // Full votes require a verified payload.
        if supported_node.payload_status == PAYLOAD_STATUS_FULL
            && !self.is_payload_verified(&supported_node.root)
        {
            return INFINITE_TIME_MS;
        }
        // Votes must support blocks belonging to the FG frame.
        if !self
            .is_ancestor_root(self.finalized_checkpoint.root, supported_node.root)
            .unwrap_or(false)
        {
            return INFINITE_TIME_MS;
        }
        std::cmp::max(
            vote_receipt_ms,
            self.block_and_payload_receipt_time(supported_node),
        )
    }

    /// The spec's `get_local_view_from_p2p_messages`: messages for `vote_slot` received before
    /// `receipt_threshold_ms`, and the indices of those resolved before
    /// `resolution_threshold_ms`.
    pub fn get_local_view_from_p2p_messages(
        &self,
        vote_slot: Slot,
        receipt_threshold_ms: u64,
        resolution_threshold_ms: u64,
    ) -> (Vec<StoreHeadVote>, Vec<usize>) {
        let mut vote_messages = Vec::new();
        let mut support_indices = Vec::new();
        for message in self
            .head_vote_messages
            .get(&vote_slot)
            .map(Vec::as_slice)
            .unwrap_or(&[])
        {
            if message.receipt_ms >= receipt_threshold_ms {
                continue;
            }
            let supported_node = FgNode {
                root: message.data.beacon_block_root,
                payload_status: message.data.payload_status,
            };
            let index = vote_messages.len();
            vote_messages.push(message.clone());
            if self.vote_resolution_time(&supported_node, message.data.slot, message.receipt_ms)
                < resolution_threshold_ms
            {
                support_indices.push(index);
            }
        }
        (vote_messages, support_indices)
    }

    /// The spec's `get_local_view_from_blocks`: votes carried by blocks of the following slot
    /// received before `receipt_threshold_ms`, supporting where the voted node resolved locally
    /// before `resolution_threshold_ms`.
    fn get_local_view_from_blocks(
        &self,
        vote_slot: Slot,
        receipt_threshold_ms: u64,
        resolution_threshold_ms: u64,
    ) -> Vec<(AcVote, bool)> {
        let block_slot = vote_slot.saturating_add(1u64);
        let mut view = Vec::new();
        for (root, meta) in &self.blocks {
            if meta.slot != block_slot {
                continue;
            }
            let block_receipt_ms = self
                .block_receipt_times
                .get(root)
                .copied()
                .unwrap_or(INFINITE_TIME_MS);
            if block_receipt_ms >= receipt_threshold_ms {
                continue;
            }
            for (validator_index, data, _support_bit) in &meta.carried_head_votes {
                if data.slot != vote_slot {
                    continue;
                }
                let supported_node = FgNode {
                    root: data.beacon_block_root,
                    payload_status: data.payload_status,
                };
                // All of the vote's voters support the locally resolved node.
                let supports =
                    self.vote_resolution_time(&supported_node, data.slot, block_receipt_ms)
                        < resolution_threshold_ms;
                view.push((
                    AcVote {
                        slot: data.slot,
                        validator_index: *validator_index,
                        supported_node,
                    },
                    supports,
                ));
            }
        }
        view
    }

    /// The spec's `get_proposer_view`: the views published through blocks of `block_slot`, with
    /// each voter's own support bit, gated on the supported block being locally known (and its
    /// payload verified for full support).
    fn get_proposer_view(&self, block_slot: Slot, vote_slot: Slot) -> Vec<(AcVote, bool)> {
        let mut view = Vec::new();
        for (root, meta) in &self.blocks {
            if meta.slot != block_slot {
                continue;
            }
            if !self
                .is_ancestor_root(self.finalized_checkpoint.root, *root)
                .unwrap_or(false)
            {
                continue;
            }
            for (validator_index, data, support_bit) in &meta.carried_head_votes {
                if data.slot != vote_slot {
                    continue;
                }
                let supported_node = FgNode {
                    root: data.beacon_block_root,
                    payload_status: data.payload_status,
                };
                let data_supportable =
                    self.blocks
                        .get(&data.beacon_block_root)
                        .is_some_and(|supported_meta| {
                            (supported_meta.slot < vote_slot
                                || data.payload_status == PAYLOAD_STATUS_PENDING)
                                && supported_meta.slot <= vote_slot
                                && (data.payload_status != PAYLOAD_STATUS_FULL
                                    || self.is_payload_verified(&data.beacon_block_root))
                        });
                view.push((
                    AcVote {
                        slot: data.slot,
                        validator_index: *validator_index,
                        supported_node,
                    },
                    *support_bit && data_supportable,
                ));
            }
        }
        view
    }

    /// The spec's `compute_merged_view`: deduplicating merge into the vote set and its
    /// supporting subset.
    fn compute_merged_view(
        message_view: &(Vec<StoreHeadVote>, Vec<usize>),
        block_view: &[(AcVote, bool)],
    ) -> (HashSet<AcVote>, HashSet<AcVote>) {
        let mut votes = HashSet::new();
        let mut support_votes = HashSet::new();
        let (vote_messages, support_indices) = message_view;
        for (index, message) in vote_messages.iter().enumerate() {
            let vote = AcVote {
                slot: message.data.slot,
                validator_index: message.validator_index,
                supported_node: FgNode {
                    root: message.data.beacon_block_root,
                    payload_status: message.data.payload_status,
                },
            };
            votes.insert(vote);
            if support_indices.contains(&index) {
                support_votes.insert(vote);
            }
        }
        for (vote, supports) in block_view {
            votes.insert(*vote);
            if *supports {
                support_votes.insert(*vote);
            }
        }
        (votes, support_votes)
    }

    /// The spec's `get_combined_local_view`.
    fn get_combined_local_view(
        &self,
        slot: Slot,
        receipt_threshold_ms: u64,
        resolution_threshold_ms: u64,
    ) -> (HashSet<AcVote>, HashSet<AcVote>) {
        let message_view = self.get_local_view_from_p2p_messages(
            slot,
            receipt_threshold_ms,
            resolution_threshold_ms,
        );
        let block_view =
            self.get_local_view_from_blocks(slot, receipt_threshold_ms, resolution_threshold_ms);
        Self::compute_merged_view(&message_view, &block_view)
    }

    /// The spec's `get_equivocating_indices`.
    fn get_equivocating_indices(votes: &HashSet<AcVote>) -> HashSet<u64> {
        let mut seen: HashMap<u64, &AcVote> = HashMap::new();
        let mut equivocating = HashSet::new();
        for vote in votes {
            if let Some(first) = seen.get(&vote.validator_index) {
                if *first != vote {
                    equivocating.insert(vote.validator_index);
                }
            } else {
                seen.insert(vote.validator_index, vote);
            }
        }
        equivocating
    }

    /* ---------------- Head walk ---------------- */

    /// The spec's `get_live_locks`: unexpired locks within the frame's tree.
    fn get_live_locks(&self, frame: &FgFrame, time_ms: u64) -> Vec<(u64, FgNode)> {
        self.locks
            .iter()
            .filter(|(deadline, lock)| **deadline > time_ms && frame.tree_set.contains(lock))
            .map(|(deadline, lock)| (*deadline, *lock))
            .collect()
    }

    /// The spec's `compute_ghost_head`: the generic GHOST walk.
    fn compute_ghost_head<S, El>(
        &self,
        anchor: FgNode,
        tree: &HashSet<FgNode>,
        get_score: S,
        is_eligible: El,
    ) -> Result<FgNode, Error>
    where
        S: Fn(&FgNode) -> Result<u64, Error>,
        El: Fn(&FgNode) -> Result<bool, Error>,
    {
        let mut head = anchor;
        loop {
            let mut best: Option<(u64, FgNode)> = None;
            for child in self.node_children(&head)? {
                if !tree.contains(&child) || !is_eligible(&child)? {
                    continue;
                }
                let score = get_score(&child)?;
                let better = match &best {
                    None => true,
                    Some((best_score, best_child)) => {
                        (score, child.root, self.payload_status_tiebreaker(&child))
                            > (
                                *best_score,
                                best_child.root,
                                self.payload_status_tiebreaker(best_child),
                            )
                    }
                };
                if better {
                    best = Some((score, child));
                }
            }
            match best {
                Some((_, child)) => head = child,
                None => return Ok(head),
            }
        }
    }

    /// The spec's `compute_head_walk_score`: equivocating seats count for every node, supporting
    /// seats for the node's chain.
    fn compute_head_walk_score(
        &self,
        votes: &HashSet<AcVote>,
        support_votes: &HashSet<AcVote>,
        slot: Slot,
        node: &FgNode,
    ) -> Result<u64, Error> {
        let equivocating = Self::get_equivocating_indices(votes);
        let mut supporting: HashSet<u64> = HashSet::new();
        for vote in support_votes {
            if equivocating.contains(&vote.validator_index) || vote.slot != slot {
                continue;
            }
            if self.is_ancestor_or_false(vote.supported_node, *node) {
                supporting.insert(vote.validator_index);
            }
        }
        let mut score: u64 = 0;
        for &voter in &equivocating {
            score = score.saturating_add(self.validator_seats_count(slot, voter));
        }
        for &voter in &supporting {
            score = score.saturating_add(self.validator_seats_count(slot, voter));
        }
        Ok(score)
    }

    /// Total committee seats of every voter in `votes`.
    fn voting_seats_count(&self, votes: &HashSet<AcVote>, slot: Slot) -> u64 {
        let voters: HashSet<u64> = votes.iter().map(|vote| vote.validator_index).collect();
        voters
            .iter()
            .map(|&voter| self.validator_seats_count(slot, voter))
            .fold(0u64, u64::saturating_add)
    }

    /// The spec's `is_eligible_for_head_walk`.
    fn is_eligible_for_head_walk(
        &self,
        frame: &FgFrame,
        votes: &HashSet<AcVote>,
        support_votes: &HashSet<AcVote>,
        slot: Slot,
        node: &FgNode,
    ) -> Result<bool, Error> {
        // Full or empty nodes from the previous slot are always eligible.
        if self.is_previous_slot_payload_decision(node)? {
            return Ok(true);
        }
        let meta = self.block(&node.root)?;
        let parent = FgNode::pending(meta.parent_root);
        if !frame.viable_heads.contains(&parent) {
            return Ok(true);
        }
        if meta.slot == self.get_current_slot() {
            return Ok(true);
        }
        // Require a strict majority of participating committee seats.
        let score = self.compute_head_walk_score(votes, support_votes, slot, node)?;
        Ok(score.saturating_mul(2) > self.voting_seats_count(votes, slot))
    }

    /// The spec's `get_head_walk_anchor`: the deepest locked node in the full viable subtree, or
    /// the forkchoice root. Ancestors of live lock tips count as locked.
    fn get_head_walk_anchor(&self, frame: &FgFrame) -> Result<FgNode, Error> {
        let live_locks = self.get_live_locks(frame, self.time_ms);
        let mut locked_viable = Vec::new();
        for node in self.get_viable_subtree(frame, &frame.tree)? {
            for (_, lock) in &live_locks {
                if self.is_ancestor(*lock, node)? {
                    locked_viable.push(node);
                    break;
                }
            }
        }
        Ok(self
            .get_deepest_node(locked_viable)
            .unwrap_or(frame.forkchoice_root))
    }

    /// The spec's `get_head_in_tree`: the lock-anchored walk over the viable subtree of `tree`.
    fn get_head_in_tree(
        &self,
        frame: &FgFrame,
        tree: &[FgNode],
        votes: &HashSet<AcVote>,
        support_votes: &HashSet<AcVote>,
        slot: Slot,
    ) -> Result<FgNode, Error> {
        let anchor = self.get_head_walk_anchor(frame)?;
        let viable_subtree: HashSet<FgNode> =
            self.get_viable_subtree(frame, tree)?.into_iter().collect();
        self.compute_ghost_head(
            anchor,
            &viable_subtree,
            |node| self.compute_head_walk_score(votes, support_votes, slot, node),
            |node| self.is_eligible_for_head_walk(frame, votes, support_votes, slot, node),
        )
    }

    /// The spec's `get_voter_subtree`: nodes received before the previous slot's view-freeze
    /// deadline, together with the voting slot's proposals and their ancestors.
    fn get_voter_subtree(&self, tree: &[FgNode], slot: Slot) -> Result<Vec<FgNode>, Error> {
        let view_freeze_ms = self
            .slot_start_ms(slot.saturating_sub(1u64))
            .saturating_add(self.view_freeze_ms());
        let slot_proposals: Vec<FgNode> = tree
            .iter()
            .copied()
            .filter(|node| {
                self.blocks
                    .get(&node.root)
                    .is_some_and(|meta| meta.slot == slot)
            })
            .collect();
        let mut subtree = Vec::new();
        for node in tree {
            if self.block_and_payload_receipt_time(node) < view_freeze_ms {
                subtree.push(*node);
                continue;
            }
            for proposal in &slot_proposals {
                if self.is_ancestor(*proposal, *node)? {
                    subtree.push(*node);
                    break;
                }
            }
        }
        Ok(subtree)
    }

    /* ---------------- Confirmation ---------------- */

    /// The spec's `compute_confirmation_score`: supporting seats only; equivocators contribute
    /// nothing.
    fn compute_confirmation_score(
        &self,
        votes: &HashSet<AcVote>,
        support_votes: &HashSet<AcVote>,
        slot: Slot,
        node: &FgNode,
    ) -> u64 {
        let equivocating = Self::get_equivocating_indices(votes);
        let mut supporting: HashSet<u64> = HashSet::new();
        for vote in support_votes {
            if equivocating.contains(&vote.validator_index) {
                continue;
            }
            if self.is_ancestor_or_false(vote.supported_node, *node) {
                supporting.insert(vote.validator_index);
            }
        }
        supporting
            .iter()
            .map(|&voter| self.validator_seats_count(slot, voter))
            .fold(0u64, u64::saturating_add)
    }

    /// The spec's `get_confirmed_head`: the confirmation walk, `None` when the head itself lacks
    /// a strict majority (including a walk stopped at the finalized node).
    fn get_confirmed_head(
        &self,
        frame: &FgFrame,
        votes: &HashSet<AcVote>,
        support_votes: &HashSet<AcVote>,
        slot: Slot,
    ) -> Result<Option<FgNode>, Error> {
        let voting_seats = self.voting_seats_count(votes, slot);
        let is_eligible = |node: &FgNode| -> bool {
            self.compute_confirmation_score(votes, support_votes, slot, node)
                .saturating_mul(2)
                > voting_seats
        };
        let head = self.compute_ghost_head(
            frame.finalized_node,
            &frame.tree_set,
            |node| Ok(self.compute_confirmation_score(votes, support_votes, slot, node)),
            |node| Ok(is_eligible(node)),
        )?;
        Ok(is_eligible(&head).then_some(head))
    }

    /// The spec's `compute_confirmed_path_and_lock`: the non-genesis nodes on the path to
    /// `confirmed` passing `test`, and the deepest of them.
    fn compute_confirmed_path_and_lock<F>(
        &self,
        frame: &FgFrame,
        confirmed: &FgNode,
        test_predicate: F,
    ) -> Result<(HashSet<FgNode>, Option<FgNode>), Error>
    where
        F: Fn(&FgNode) -> Result<bool, Error>,
    {
        let mut path = HashSet::new();
        for node in &frame.tree {
            if !self.is_ancestor(*confirmed, *node)? {
                continue;
            }
            if self.block(&node.root)?.slot <= self.genesis_slot {
                continue;
            }
            if test_predicate(node)? {
                path.insert(*node);
            }
        }
        let lock = self.get_deepest_node(path.iter().copied());
        Ok((path, lock))
    }

    /// The spec's `compute_fast_path_and_lock`: confirmation support of at least half of all
    /// committee seats along the confirmed path.
    fn compute_fast_path_and_lock(
        &self,
        frame: &FgFrame,
        votes: &HashSet<AcVote>,
        support_votes: &HashSet<AcVote>,
        slot: Slot,
        confirmed: &FgNode,
    ) -> Result<(HashSet<FgNode>, Option<FgNode>), Error> {
        self.compute_confirmed_path_and_lock(frame, confirmed, |node| {
            Ok(self
                .compute_confirmation_score(votes, support_votes, slot, node)
                .saturating_mul(2)
                >= Self::committee_seats_count())
        })
    }

    /// The spec's `compute_slow_lock`: the deepest common ancestor of the current candidate and
    /// the previous `SLOW_LOCK_WINDOW - 1` slots' saved confirmations.
    fn compute_slow_lock(
        &self,
        frame: &FgFrame,
        slot: Slot,
        confirmed: &FgNode,
    ) -> Result<Option<FgNode>, Error> {
        let slot_number = slot.as_u64();
        if slot_number.saturating_add(1) < SLOW_LOCK_WINDOW {
            return Ok(None);
        }
        let window_start = slot_number
            .saturating_add(1)
            .saturating_sub(SLOW_LOCK_WINDOW);
        let previous_slots: Vec<Slot> = (window_start..slot_number).map(Slot::new).collect();
        for previous_slot in &previous_slots {
            match self.saved_confirmations.get(previous_slot) {
                Some(saved) if frame.tree_set.contains(saved) => {}
                _ => return Ok(None),
            }
        }
        let (_, slow_lock) = self.compute_confirmed_path_and_lock(frame, confirmed, |node| {
            for previous_slot in &previous_slots {
                let saved = self
                    .saved_confirmations
                    .get(previous_slot)
                    .ok_or(Error::MissingBlock(node.root))?;
                if !self.is_ancestor(*saved, *node)? {
                    return Ok(false);
                }
            }
            Ok(true)
        })?;
        Ok(slow_lock)
    }

    /// The spec's `add_lock`: register a lock expiring at `deadline`.
    pub fn add_lock(&mut self, deadline: u64, node: FgNode) {
        self.locks.insert(deadline, node);
    }

    /// The spec's `update_locks_and_confirmation`.
    fn update_locks_and_confirmation(
        &mut self,
        slot: Slot,
        frame: &FgFrame,
        confirmed: FgNode,
        fast_confirmed_path: HashSet<FgNode>,
        fast_lock: Option<FgNode>,
        slow_lock: Option<FgNode>,
    ) -> Result<(), Error> {
        self.fast_confirmed_path = fast_confirmed_path;
        self.saved_confirmations.insert(slot, confirmed);

        // The deepest node passing either test.
        let lock = match (fast_lock, slow_lock) {
            (Some(fast), Some(slow)) => self.get_deepest_node([fast, slow]),
            (Some(fast), None) => Some(fast),
            (None, slow) => slow,
        };
        if let Some(lock) = lock {
            let deadline = self
                .slot_start_ms(slot.saturating_add(SLOW_LOCK_WINDOW).saturating_add(1u64))
                .saturating_add(self.head_vote_due_ms());
            self.add_lock(deadline, lock);
        }

        // Keep `latest_confirmed` while it stays in the frame and extends or equals `confirmed`.
        if !(frame.tree_set.contains(&self.latest_confirmed)
            && self.is_ancestor_or_false(self.latest_confirmed, confirmed))
        {
            self.latest_confirmed = confirmed;
        }
        Ok(())
    }

    /// The spec's `update_confirmation`, the on-tick duty at the support cutoff.
    pub fn update_confirmation(&mut self, slot: Slot) -> Result<(), Error> {
        let frame = self.get_fg_frame()?;
        let support_cutoff_ms = self
            .slot_start_ms(slot)
            .saturating_add(self.support_cutoff_ms());
        let (votes, support_votes) =
            self.get_combined_local_view(slot, INFINITE_TIME_MS, support_cutoff_ms);
        let Some(confirmed) = self.get_confirmed_head(&frame, &votes, &support_votes, slot)? else {
            self.fast_confirmed_path = HashSet::new();
            return Ok(());
        };

        // A live lock vetoing the candidate still saves it for the slow-lock window.
        for (_, lock) in self.get_live_locks(&frame, self.time_ms) {
            if !self.is_compatible(lock, confirmed).unwrap_or(false) {
                self.saved_confirmations.insert(slot, confirmed);
                self.fast_confirmed_path = HashSet::new();
                return Ok(());
            }
        }

        let (fast_confirmed_path, fast_lock) =
            self.compute_fast_path_and_lock(&frame, &votes, &support_votes, slot, &confirmed)?;
        let slow_lock = self.compute_slow_lock(&frame, slot, &confirmed)?;
        self.update_locks_and_confirmation(
            slot,
            &frame,
            confirmed,
            fast_confirmed_path,
            fast_lock,
            slow_lock,
        )
    }

    /// The spec's `unlock`, the on-tick duty at the head-vote deadline: move majority-opposed
    /// live locks to their deepest unopposed ancestors, preserving deadlines.
    pub fn unlock(&mut self, slot: Slot, time_ms: u64) -> Result<(), Error> {
        let frame = self.get_fg_frame()?;
        let (votes, support_votes) =
            self.get_combined_local_view(slot, INFINITE_TIME_MS, INFINITE_TIME_MS);
        let equivocating = Self::get_equivocating_indices(&votes);

        let is_opposed_by_majority = |node: &FgNode| -> bool {
            let opposing_voters: HashSet<u64> = support_votes
                .iter()
                .filter(|vote| {
                    !equivocating.contains(&vote.validator_index)
                        && !self
                            .is_compatible(vote.supported_node, *node)
                            .unwrap_or(true)
                })
                .map(|vote| vote.validator_index)
                .collect();
            let opposing_score = opposing_voters
                .iter()
                .map(|&voter| self.validator_seats_count(slot, voter))
                .fold(0u64, u64::saturating_add);
            // Opposition must account for at least half of all committee seats.
            opposing_score.saturating_mul(2) >= Self::committee_seats_count()
        };

        let mut moves = Vec::new();
        for (deadline, lock) in self.get_live_locks(&frame, time_ms) {
            if !is_opposed_by_majority(&lock) {
                continue;
            }
            let mut fallback_path = Vec::new();
            for node in &frame.tree {
                if self.is_ancestor(lock, *node)? && !is_opposed_by_majority(node) {
                    fallback_path.push(*node);
                }
            }
            // The finalized node is unopposed on any frame-supported view, so the path is only
            // empty when the whole frame is opposed; leave such a lock in place.
            if let Some(fallback) = self.get_deepest_node(fallback_path) {
                moves.push((deadline, fallback));
            }
        }
        for (deadline, fallback) in moves {
            self.add_lock(deadline, fallback);
        }
        Ok(())
    }

    /* ---------------- FG summary ---------------- */

    /// The spec's `update_finality`: fold a post-state summary into the store.
    pub fn update_finality(&mut self, summary: BlockSummary) -> Result<(), Error> {
        self.max_height = std::cmp::max(self.max_height, summary.target_height);

        // Justification: lexicographically greater and descending from the finalized block.
        // An unknown (pruned) root cannot be adopted.
        let justified = summary.justified;
        if self.blocks.contains_key(&justified.root) {
            let store_justified = (
                self.justified_checkpoint.height,
                self.justified_checkpoint.root,
            );
            if (justified.height, justified.root) > store_justified
                && self.is_ancestor_root(self.finalized_checkpoint.root, justified.root)?
            {
                self.justified_checkpoint = justified;
            }
        }

        // Finality: higher, strictly descending the finalized block, at or below the justified
        // block, with a viable descendant.
        let finalized = summary.finalized;
        if self.blocks.contains_key(&finalized.root) {
            let is_advance = finalized.height > self.finalized_checkpoint.height
                && self.is_strict_ancestor_root(self.finalized_checkpoint.root, finalized.root)?
                && self.is_ancestor_root(finalized.root, self.justified_checkpoint.root)?
                && self.has_viable_descendant(finalized.root)?;
            if is_advance {
                self.finalized_checkpoint = finalized;
                self.finalized_slot = summary.finalized_slot;
            }
        }

        Ok(())
    }

    /* ---------------- Stabilization ---------------- */

    /// The spec's `get_window_safe_votes`: the expiry window's votes, rounds `[r - η, r)`.
    fn window_safe_votes(&self, round: Round) -> Vec<StoreSafeVote> {
        let start = round.saturating_sub(std::cmp::min(round, E::safe_vote_expiry_rounds()));
        let mut votes = Vec::new();
        for window_round in start..round {
            if let Some(round_votes) = self.safe_votes.get(&window_round) {
                votes.extend(round_votes.iter().cloned());
            }
        }
        votes
    }

    /// The spec's `get_safe_vote_resolution_time_ms`: when the vote's block became available on
    /// a finalized-compatible chain, or `None`.
    fn safe_vote_resolution_time_ms(&self, vote: &StoreSafeVote) -> Option<u64> {
        let root = vote.data.beacon_block_root;
        if !self.blocks.contains_key(&root) {
            return None;
        }
        let finalized_root = self.finalized_checkpoint.root;
        let is_compatible = self.is_ancestor_root(finalized_root, root).unwrap_or(false)
            || self.is_ancestor_root(root, finalized_root).unwrap_or(false);
        if !is_compatible {
            return None;
        }
        let block_receipt = self
            .block_receipt_times
            .get(&root)
            .copied()
            .unwrap_or(INFINITE_TIME_MS);
        Some(std::cmp::max(vote.arrival_ms, block_receipt))
    }

    /// The spec's `safe_vote_covers`.
    fn safe_vote_covers(&self, data: &SafeVoteData, node: FgNode) -> bool {
        if !self.blocks.contains_key(&data.beacon_block_root) {
            return false;
        }
        let vote_node = FgNode {
            root: data.beacon_block_root,
            payload_status: data.payload_status,
        };
        self.is_ancestor(vote_node, node).unwrap_or(false)
    }

    /// The spec's `has_safe_vote_equivocation`: two distinct votes of one round at or after
    /// `round_floor`.
    fn has_safe_vote_equivocation(votes: &[&StoreSafeVote], round_floor: Round) -> bool {
        let mut seen: HashMap<Round, &SafeVoteData> = HashMap::new();
        for vote in votes {
            if vote.data.round < round_floor {
                continue;
            }
            if let Some(first) = seen.get(&vote.data.round) {
                if **first != vote.data {
                    return true;
                }
            } else {
                seen.insert(vote.data.round, &vote.data);
            }
        }
        false
    }

    /// The spec's `supports_node`.
    fn supports_node(
        &self,
        window: &[StoreSafeVote],
        early_ms: u64,
        late_ms: u64,
        validator_index: u64,
        node: FgNode,
    ) -> bool {
        let own_votes: Vec<&StoreSafeVote> = window
            .iter()
            .filter(|vote| vote.validator_index == validator_index)
            .collect();
        let received: Vec<&StoreSafeVote> = own_votes
            .iter()
            .copied()
            .filter(|vote| vote.arrival_ms < late_ms)
            .collect();
        let mut early_resolved: Vec<&StoreSafeVote> = Vec::new();
        let mut late_resolved: Vec<&StoreSafeVote> = Vec::new();
        for vote in &own_votes {
            let Some(resolution_ms) = self.safe_vote_resolution_time_ms(vote) else {
                continue;
            };
            if resolution_ms < early_ms {
                early_resolved.push(vote);
            }
            if resolution_ms < late_ms {
                late_resolved.push(vote);
            }
        }
        let Some(latest) = early_resolved.iter().max_by_key(|vote| vote.data.round) else {
            return false;
        };
        if !self.safe_vote_covers(&latest.data, node) {
            return false;
        }
        if Self::has_safe_vote_equivocation(&received, latest.data.round) {
            return false;
        }
        late_resolved
            .iter()
            .filter(|vote| vote.data.round > latest.data.round)
            .all(|vote| self.safe_vote_covers(&vote.data, node))
    }

    /// The spec's `opposes_node`.
    fn opposes_node(
        &self,
        window: &[StoreSafeVote],
        early_ms: u64,
        late_ms: u64,
        validator_index: u64,
        node: FgNode,
    ) -> bool {
        let own_votes: Vec<&StoreSafeVote> = window
            .iter()
            .filter(|vote| vote.validator_index == validator_index)
            .collect();
        let received: Vec<&StoreSafeVote> = own_votes
            .iter()
            .copied()
            .filter(|vote| vote.arrival_ms < late_ms)
            .collect();
        let mut early_rounds: Vec<Round> = Vec::new();
        let mut late_resolved: Vec<&StoreSafeVote> = Vec::new();
        for vote in &own_votes {
            let Some(resolution_ms) = self.safe_vote_resolution_time_ms(vote) else {
                continue;
            };
            if resolution_ms < early_ms {
                early_rounds.push(vote.data.round);
            }
            if resolution_ms < late_ms {
                late_resolved.push(vote);
            }
        }
        let round_floor = early_rounds.iter().copied().max().unwrap_or(0);
        let has_defection = late_resolved
            .iter()
            .filter(|vote| vote.data.round >= round_floor)
            .any(|vote| !self.safe_vote_covers(&vote.data, node));
        has_defection || Self::has_safe_vote_equivocation(&received, round_floor)
    }

    /// The spec's `has_relative_majority`: supporting weight strictly exceeds opposing weight.
    fn has_relative_majority(
        &self,
        round: Round,
        early_ms: u64,
        late_ms: u64,
        node: FgNode,
    ) -> bool {
        let window = self.window_safe_votes(round);
        let mut participants: HashSet<u64> = HashSet::new();
        for vote in &window {
            if vote.arrival_ms < late_ms {
                participants.insert(vote.validator_index);
            }
        }
        let mut supporting_weight: u128 = 0;
        let mut opposing_weight: u128 = 0;
        for validator_index in participants {
            // A validator unknown to the weight state carries no weight.
            let Some(weight) = self.finalized_weights.get(&validator_index).copied() else {
                continue;
            };
            if self.supports_node(&window, early_ms, late_ms, validator_index, node) {
                supporting_weight = supporting_weight.saturating_add(weight);
            } else if self.opposes_node(&window, early_ms, late_ms, validator_index, node) {
                opposing_weight = opposing_weight.saturating_add(weight);
            }
        }
        supporting_weight > opposing_weight
    }

    /// Test/debug probe: the supporting and opposing weight for one node.
    #[doc(hidden)]
    pub fn probe_majority(
        &self,
        round: Round,
        early_ms: u64,
        late_ms: u64,
        node: FgNode,
    ) -> (u128, u128, usize, usize) {
        let window = self.window_safe_votes(round);
        let mut participants: HashSet<u64> = HashSet::new();
        for vote in &window {
            if vote.arrival_ms < late_ms {
                participants.insert(vote.validator_index);
            }
        }
        let num_participants = participants.len();
        let mut supporting: u128 = 0;
        let mut opposing: u128 = 0;
        for validator_index in participants {
            let Some(weight) = self.finalized_weights.get(&validator_index).copied() else {
                continue;
            };
            if self.supports_node(&window, early_ms, late_ms, validator_index, node) {
                supporting = supporting.saturating_add(weight);
            } else if self.opposes_node(&window, early_ms, late_ms, validator_index, node) {
                opposing = opposing.saturating_add(weight);
            }
        }
        (
            supporting,
            opposing,
            num_participants,
            self.finalized_weights.len(),
        )
    }

    /// The spec's `compute_graded_node`: the deepest node holding a relative majority.
    fn compute_graded_node(&self, round: Round, early_ms: u64, late_ms: u64) -> Option<FgNode> {
        let statuses = [
            PAYLOAD_STATUS_PENDING,
            PAYLOAD_STATUS_EMPTY,
            PAYLOAD_STATUS_FULL,
        ];
        let mut graded: Vec<FgNode> = Vec::new();
        for root in self.blocks.keys() {
            for payload_status in statuses {
                let node = FgNode {
                    root: *root,
                    payload_status,
                };
                if self.has_relative_majority(round, early_ms, late_ms, node) {
                    graded.push(node);
                }
            }
        }
        self.get_deepest_node(graded)
    }

    /// The spec's `update_grades`: evaluate each due grade once, late evaluations reading the
    /// same receipt-time sets a timely one would have.
    pub fn update_grades(&mut self) -> Result<(), Error> {
        let quarter_ms = self.head_vote_due_ms();
        let current_round = compute_round_at_slot::<E>(self.get_current_slot());
        if current_round == GENESIS_ROUND {
            return Ok(());
        }
        let start_ms = self.slot_start_ms(compute_start_slot_at_round::<E>(current_round));
        // Grade 2 of a round completes in the last quarter-slot of the previous round; a node
        // that missed that tick evaluates it late.
        if self.grade_2_round < current_round {
            self.grade_2 = self.compute_graded_node(
                current_round,
                start_ms.saturating_sub(quarter_ms.saturating_mul(5)),
                start_ms.saturating_sub(quarter_ms),
            );
            self.grade_2_round = current_round;
        }
        let next_round = current_round.saturating_add(1);
        let next_start_ms = self.slot_start_ms(compute_start_slot_at_round::<E>(next_round));
        if self.time_ms.saturating_add(quarter_ms) >= next_start_ms
            && self.grade_2_round < next_round
        {
            self.grade_2 = self.compute_graded_node(
                next_round,
                next_start_ms.saturating_sub(quarter_ms.saturating_mul(5)),
                next_start_ms.saturating_sub(quarter_ms),
            );
            self.grade_2_round = next_round;
        }
        // Grade 1 completes at the round boundary.
        if self.grade_1_round < current_round {
            self.grade_1 = self.compute_graded_node(
                current_round,
                start_ms.saturating_sub(quarter_ms.saturating_mul(4)),
                start_ms.saturating_sub(quarter_ms.saturating_mul(2)),
            );
            self.grade_1_round = current_round;
        }
        // Grade 0 completes one quarter-slot into the round.
        if self.time_ms >= start_ms.saturating_add(quarter_ms) && self.grade_0_round < current_round
        {
            self.grade_0 = self.compute_graded_node(
                current_round,
                start_ms.saturating_sub(quarter_ms.saturating_mul(3)),
                start_ms.saturating_sub(quarter_ms.saturating_mul(3)),
            );
            self.grade_0_round = current_round;
        }
        Ok(())
    }

    /// The spec's `get_active_prefix`: the deepest ancestor of a saved grade inside the frame's
    /// viable subtree.
    fn get_active_prefix(
        &self,
        frame: &FgFrame,
        node: Option<FgNode>,
    ) -> Result<Option<FgNode>, Error> {
        let Some(node) = node else {
            return Ok(None);
        };
        if !self.blocks.contains_key(&node.root) {
            return Ok(None);
        }
        let viable: HashSet<FgNode> = self
            .get_viable_subtree(frame, &frame.tree)?
            .into_iter()
            .collect();
        let mut current = Some(node);
        while let Some(candidate) = current {
            if viable.contains(&candidate) {
                return Ok(Some(candidate));
            }
            current = self.node_parent(&candidate)?;
        }
        Ok(None)
    }

    /// The spec's `get_sg_anchor`: the grade-1 active prefix, or the forkchoice root.
    fn get_sg_anchor(&self, frame: &FgFrame) -> Result<FgNode, Error> {
        if let Some(prefix) = self.get_active_prefix(frame, self.grade_1)? {
            return Ok(prefix);
        }
        Ok(frame.forkchoice_root)
    }

    /// The spec's `update_sg_lock`: the stabilization layer's lock producer. The deadline spans
    /// the grade's own round, so an unresolved round lets the lock expire.
    pub fn update_sg_lock(&mut self) -> Result<(), Error> {
        let frame = self.get_fg_frame()?;
        let Some(prefix) = self.get_active_prefix(&frame, self.grade_1)? else {
            return Ok(());
        };
        let deadline = self
            .slot_start_ms(compute_start_slot_at_round::<E>(
                self.grade_1_round.saturating_add(1),
            ))
            .saturating_add(self.head_vote_due_ms());
        self.add_lock(deadline, prefix);
        Ok(())
    }

    /// The spec's `is_grade_0_compatible`.
    pub fn is_grade_0_compatible(&self, node: FgNode) -> Result<bool, Error> {
        let Some(veto) = self.grade_0 else {
            return Ok(true);
        };
        if !self.blocks.contains_key(&veto.root) || !self.blocks.contains_key(&node.root) {
            return Ok(true);
        }
        self.is_compatible(node, veto)
    }

    /// The spec's `get_stable_root`: the grade-2 active prefix, or the forkchoice root.
    fn get_stable_root(&self, frame: &FgFrame) -> Result<FgNode, Error> {
        if let Some(prefix) = self.get_active_prefix(frame, self.grade_2)? {
            return Ok(prefix);
        }
        Ok(frame.forkchoice_root)
    }

    /// The spec's `get_safe_vote_node`: the node an honest validator names in its safe vote.
    pub fn get_safe_vote_node(&self) -> Result<FgNode, Error> {
        let frame = self.get_fg_frame()?;
        let anchor = self.get_sg_anchor(&frame)?;
        let confirmed = self.get_confirmed_node()?;
        if self.is_ancestor_or_false(confirmed, anchor) {
            let mut current = Some(confirmed);
            while let Some(node) = current {
                if self.is_grade_0_compatible(node)? {
                    return Ok(node);
                }
                if node == anchor {
                    break;
                }
                current = self.node_parent(&node)?;
            }
        }
        if let Some(prefix) = self.get_active_prefix(&frame, self.grade_2)? {
            return Ok(prefix);
        }
        if self.grade_2.is_some() {
            return Ok(frame.forkchoice_root);
        }
        Ok(anchor)
    }

    /// The spec's `advance_record`: no candidate or an ancestor candidate leaves the record
    /// unchanged; an extension or a conflicting candidate replaces it.
    fn advance_record(&self, old: FgNode, candidate: Option<FgNode>) -> FgNode {
        match candidate {
            None => old,
            Some(candidate) if candidate == old || self.is_ancestor_or_false(old, candidate) => old,
            Some(candidate) => candidate,
        }
    }

    /// The spec's `update_stable_record`.
    pub fn update_stable_record(&mut self) -> Result<(), Error> {
        let frame = self.get_fg_frame()?;
        let candidate = self.get_stable_root(&frame)?;
        self.latest_stable = self.advance_record(self.latest_stable, Some(candidate));
        Ok(())
    }

    /// The spec's `get_stable` (user-facing; no voting rule reads it).
    pub fn get_stable(&self) -> FgNode {
        let finalized_root = self.finalized_checkpoint.root;
        if self
            .is_ancestor_root(finalized_root, self.latest_stable.root)
            .unwrap_or(false)
        {
            return self.latest_stable;
        }
        FgNode::pending(finalized_root)
    }

    /* ---------------- Internal API ---------------- */

    /// The spec's `get_ac_proposal`: the proposal head over the previous slot's full P2P view,
    /// plus the vote messages and supporting indices the proposer publishes.
    ///
    /// The spec pins this to the exact start of a slot; the prototype computes from the store's
    /// current time.
    pub fn get_ac_proposal(&self) -> Result<(FgNode, Vec<StoreHeadVote>, Vec<usize>), Error> {
        let current_slot = self.get_current_slot();
        if current_slot == self.genesis_slot {
            return Ok((
                FgNode {
                    root: self.finalized_checkpoint.root,
                    payload_status: PAYLOAD_STATUS_EMPTY,
                },
                Vec::new(),
                Vec::new(),
            ));
        }
        let vote_slot = current_slot.saturating_sub(1u64);
        let frame = self.get_fg_frame()?;
        let message_view =
            self.get_local_view_from_p2p_messages(vote_slot, INFINITE_TIME_MS, INFINITE_TIME_MS);
        // The proposal view is built from P2P messages only.
        let (votes, support_votes) = Self::compute_merged_view(&message_view, &[]);
        let head = self.get_head_in_tree(&frame, &frame.tree, &votes, &support_votes, vote_slot)?;
        let (vote_messages, support_indices) = message_view;

        // Convert a pending head to empty for proposal construction.
        let head = if head.payload_status == PAYLOAD_STATUS_PENDING {
            FgNode {
                root: head.root,
                payload_status: PAYLOAD_STATUS_EMPTY,
            }
        } else {
            head
        };
        Ok((head, vote_messages, support_indices))
    }

    /// The spec's `get_ac_vote_head`: the head for an AC vote at the head-vote deadline, merging
    /// the frozen local view with the views published by the slot's proposals, walked over the
    /// voter's subtree.
    ///
    /// The spec pins this to the exact head-vote deadline; the prototype computes from the
    /// store's current time.
    pub fn get_ac_vote_head(&self) -> Result<FgNode, Error> {
        let current_slot = self.get_current_slot();
        if current_slot == self.genesis_slot {
            return Ok(FgNode {
                root: self.finalized_checkpoint.root,
                payload_status: PAYLOAD_STATUS_EMPTY,
            });
        }
        let vote_slot = current_slot.saturating_sub(1u64);
        let frame = self.get_fg_frame()?;
        let view_freeze_ms = self
            .slot_start_ms(vote_slot)
            .saturating_add(self.view_freeze_ms());
        let local_view =
            self.get_local_view_from_p2p_messages(vote_slot, view_freeze_ms, view_freeze_ms);
        let proposer_view = self.get_proposer_view(current_slot, vote_slot);
        let (votes, support_votes) = Self::compute_merged_view(&local_view, &proposer_view);

        let voter_subtree = self.get_voter_subtree(&frame.tree, current_slot)?;
        let head =
            self.get_head_in_tree(&frame, &voter_subtree, &votes, &support_votes, vote_slot)?;

        // Never return pending nodes for previous slots.
        if self.block(&head.root)?.slot < current_slot
            && head.payload_status == PAYLOAD_STATUS_PENDING
        {
            Ok(FgNode {
                root: head.root,
                payload_status: PAYLOAD_STATUS_EMPTY,
            })
        } else {
            Ok(head)
        }
    }

    /// The spec's `get_ac_frame`: the FG confirmation node (the deepest fast-confirmed viable
    /// node) and the head over the current slot's local view, read for FG attestations at or
    /// after the support cutoff.
    pub fn get_ac_frame(&self) -> Result<(Option<FgNode>, FgNode), Error> {
        let current_slot = self.get_current_slot();
        let frame = self.get_fg_frame()?;
        let (votes, support_votes) =
            self.get_combined_local_view(current_slot, INFINITE_TIME_MS, INFINITE_TIME_MS);
        let head =
            self.get_head_in_tree(&frame, &frame.tree, &votes, &support_votes, current_slot)?;

        let viable: HashSet<FgNode> = self
            .get_viable_subtree(&frame, &frame.tree)?
            .into_iter()
            .collect();
        let fg_confirmed = self.get_deepest_node(
            self.fast_confirmed_path
                .iter()
                .copied()
                .filter(|node| viable.contains(node)),
        );
        Ok((fg_confirmed, head))
    }

    /// The spec's `get_fg_target_source`, as a root (the caller resolves the state).
    pub fn fg_target_source_root(&self) -> Result<Hash256, Error> {
        Ok(self.get_confirmed_node()?.root)
    }

    /// The spec's `get_fg_finality_source`, as a node (the caller resolves the state).
    pub fn fg_finality_source(&self) -> Result<FgNode, Error> {
        self.get_head()
    }

    /// The spec's `has_fresh_safe_quorum`: the grade-2 node is compatible with the forkchoice
    /// root — fresh quorum evidence from the stabilization layer.
    pub fn has_fresh_safe_quorum(&self) -> Result<bool, Error> {
        let Some(grade) = self.grade_2 else {
            return Ok(false);
        };
        if !self.blocks.contains_key(&grade.root) {
            return Ok(false);
        }
        let frame = self.get_fg_frame()?;
        self.is_compatible(grade, frame.forkchoice_root)
    }

    /// The spec's `get_confirmed_execution_block_hash`: the confirmed execution block hash for
    /// the Engine API.
    pub fn get_confirmed_execution_block_hash(&self) -> Result<ExecutionBlockHash, Error> {
        let confirmed = self.get_confirmed_node()?;
        let meta = self.block(&confirmed.root)?;
        Ok(if confirmed.payload_status == PAYLOAD_STATUS_FULL {
            meta.bid_block_hash
        } else {
            meta.bid_parent_hash
        })
    }

    /* ---------------- External API ---------------- */

    /// The spec's `get_confirmed_node`: the latest confirmed node, falling back to the finalized
    /// node when the record left the frame or conflicts with the current head.
    pub fn get_confirmed_node(&self) -> Result<FgNode, Error> {
        let frame = self.get_fg_frame()?;
        if !frame.tree_set.contains(&self.latest_confirmed)
            || self.latest_confirmed == frame.finalized_node
        {
            return Ok(frame.finalized_node);
        }
        let head = self.get_head()?;
        if !self.is_compatible(head, self.latest_confirmed)? {
            return Ok(frame.finalized_node);
        }
        Ok(self.latest_confirmed)
    }

    /// The spec's `get_head`: current-slot votes at or after the support cutoff, previous-slot
    /// votes before it; never returns a pending node.
    pub fn get_head(&self) -> Result<FgNode, Error> {
        let current_slot = self.get_current_slot();
        if current_slot == self.genesis_slot {
            return Ok(FgNode {
                root: self.finalized_checkpoint.root,
                payload_status: PAYLOAD_STATUS_EMPTY,
            });
        }
        let slot = if self.time_into_slot_ms() >= self.support_cutoff_ms() {
            current_slot
        } else {
            current_slot.saturating_sub(1u64)
        };
        let frame = self.get_fg_frame()?;
        let (votes, support_votes) =
            self.get_combined_local_view(slot, INFINITE_TIME_MS, INFINITE_TIME_MS);
        let head = self.get_head_in_tree(&frame, &frame.tree, &votes, &support_votes, slot)?;

        // Never return pending.
        if head.payload_status == PAYLOAD_STATUS_PENDING {
            Ok(FgNode {
                root: head.root,
                payload_status: PAYLOAD_STATUS_EMPTY,
            })
        } else {
            Ok(head)
        }
    }

    /* ---------------- Pruning ---------------- */

    /// Drop blocks that do not descend from the finalized block, blocks older than the finalized
    /// slot, and store entries no rule can read any more.
    pub fn prune(&mut self) -> Result<(), Error> {
        let finalized_root = self.finalized_checkpoint.root;
        let finalized_slot = self.finalized_slot;
        let mut retained: HashSet<Hash256> = HashSet::new();
        for (root, meta) in &self.blocks {
            if *root == finalized_root {
                retained.insert(*root);
                continue;
            }
            if meta.slot <= finalized_slot {
                continue;
            }
            if self
                .is_ancestor_root(finalized_root, *root)
                .unwrap_or(false)
            {
                retained.insert(*root);
            }
        }
        self.blocks.retain(|root, _| retained.contains(root));
        self.children.retain(|root, _| retained.contains(root));
        for children in self.children.values_mut() {
            children.retain(|child| retained.contains(child));
        }
        self.payload_verified.retain(|root| retained.contains(root));
        self.block_receipt_times
            .retain(|root, _| retained.contains(root));
        self.payload_receipt_times
            .retain(|root, _| retained.contains(root));
        self.fast_confirmed_path
            .retain(|node| retained.contains(&node.root));

        // Expired locks and locks on pruned branches are never read again.
        let time_ms = self.time_ms;
        self.locks
            .retain(|deadline, node| *deadline > time_ms && retained.contains(&node.root));

        // Head vote messages: only the current and previous slot are ever read.
        let current_slot = self.get_current_slot();
        let keep_from = current_slot.saturating_sub(1u64);
        self.head_vote_messages.retain(|slot, _| *slot >= keep_from);

        // Saved confirmations: only the slow-lock window is ever read.
        let keep_confirmations_from = current_slot.saturating_sub(SLOW_LOCK_WINDOW);
        self.saved_confirmations
            .retain(|slot, _| *slot >= keep_confirmations_from);

        // Safe votes: only the expiry window is ever read.
        let current_round = compute_round_at_slot::<E>(current_slot);
        let keep_from_round = current_round.saturating_sub(E::safe_vote_expiry_rounds());
        self.safe_votes.retain(|round, _| *round >= keep_from_round);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    //! Mirrors of the spec's `test_head_advance.py` regression tests (a fresh proposal is
    //! reachable by the vote walk in its own slot, and honest votes for the walk's output
    //! advance the next proposer's head) plus a lock-lifecycle exercise.

    use super::*;
    use types::MinimalEthSpec;

    const SLOT_MS: u64 = 6_000;
    const QUARTER_MS: u64 = 1_500;

    fn summary(height: u64) -> BlockSummary {
        BlockSummary {
            justified: HeightCheckpoint {
                height,
                root: Hash256::ZERO,
            },
            finalized: HeightCheckpoint {
                height,
                root: Hash256::ZERO,
            },
            finalized_slot: Slot::new(0),
            target_height: height,
        }
    }

    fn test_store() -> (FgForkChoiceStore<MinimalEthSpec>, Hash256) {
        let anchor_root = Hash256::repeat_byte(0x0a);
        let mut store = FgForkChoiceStore::<MinimalEthSpec>::from_anchor(
            anchor_root,
            Hash256::ZERO,
            Slot::new(0),
            0,
            ExecutionBlockHash::zero(),
            ExecutionBlockHash::zero(),
            summary(1),
            0,
            Slot::new(0),
            6,
        )
        .unwrap();
        let committee: Vec<u64> = (0..16).collect();
        for slot in 0..16 {
            store.insert_committee(Slot::new(slot), committee.clone());
        }
        (store, anchor_root)
    }

    /// Import a block at `slot` on the empty parent node.
    fn apply_block(
        store: &mut FgForkChoiceStore<MinimalEthSpec>,
        parent_root: Hash256,
        slot: u64,
        byte: u8,
    ) -> Hash256 {
        let root = Hash256::repeat_byte(byte);
        store.on_tick(slot.saturating_mul(SLOT_MS)).unwrap();
        store
            .on_block(
                root,
                parent_root,
                Slot::new(slot),
                0,
                ExecutionBlockHash::from_root(root),
                ExecutionBlockHash::from_root(Hash256::repeat_byte(0xee)),
                summary(1),
                Vec::new(),
                std::iter::empty(),
            )
            .unwrap();
        root
    }

    #[test]
    fn vote_head_reaches_fresh_proposal() {
        let (mut store, anchor_root) = test_store();
        let block_root = apply_block(&mut store, anchor_root, 1, 0x01);

        store.on_tick(SLOT_MS + QUARTER_MS).unwrap();
        let head = store.get_ac_vote_head().unwrap();
        assert_eq!(
            head.root, block_root,
            "the vote walk reaches a proposal in its own slot without any vote weight"
        );
    }

    #[test]
    fn honest_votes_advance_proposal_head() {
        let (mut store, anchor_root) = test_store();
        let block_root = apply_block(&mut store, anchor_root, 1, 0x01);

        store.on_tick(SLOT_MS + QUARTER_MS).unwrap();
        let head = store.get_ac_vote_head().unwrap();
        assert_eq!(head.root, block_root);

        // The committee honestly votes the walk's output, named pending per the same-slot rule.
        let data = HeadVoteData {
            slot: Slot::new(1),
            beacon_block_root: head.root,
            payload_status: PAYLOAD_STATUS_PENDING,
        };
        for validator_index in 0..16 {
            store.on_head_vote_message(validator_index, data);
        }

        store.on_tick(2 * SLOT_MS).unwrap();
        let (proposal_head, vote_messages, support_indices) = store.get_ac_proposal().unwrap();
        assert_eq!(
            proposal_head.root, block_root,
            "the next proposer builds on the voted block, not the anchor"
        );
        assert_ne!(proposal_head.payload_status, PAYLOAD_STATUS_PENDING);
        assert_eq!(vote_messages.len(), 16);
        assert_eq!(support_indices.len(), 16, "all honest votes resolved");
    }

    #[test]
    fn confirmation_locks_anchor_the_walk_until_expiry() {
        let (mut store, anchor_root) = test_store();
        let block_root = apply_block(&mut store, anchor_root, 1, 0x01);

        store.on_tick(SLOT_MS + QUARTER_MS).unwrap();
        let data = HeadVoteData {
            slot: Slot::new(1),
            beacon_block_root: block_root,
            payload_status: PAYLOAD_STATUS_PENDING,
        };
        for validator_index in 0..16 {
            store.on_head_vote_message(validator_index, data);
        }

        // The support cutoff of slot 2 confirms slot 1 and produces a fast lock.
        store.on_tick(2 * SLOT_MS + 2 * QUARTER_MS).unwrap();
        assert!(
            store.locks.values().any(|lock| lock.root == block_root),
            "a full committee's confirmation creates a fast lock on the block"
        );
        assert_eq!(store.latest_confirmed.root, block_root);
        assert!(!store.fast_confirmed_path.is_empty());

        // With no further votes, the walk stays anchored at the locked node.
        let head = store.get_head().unwrap();
        assert_eq!(head.root, block_root, "the lock anchors the head walk");
        assert_eq!(store.get_confirmed_node().unwrap().root, block_root);

        // Once every lock expires, the anchor falls back to the forkchoice root; the walk still
        // reaches the block from there on this unforked chain.
        let expiry = store.locks.keys().next_back().copied().unwrap();
        store.on_tick(expiry.saturating_add(SLOT_MS)).unwrap();
        let frame = store.get_fg_frame().unwrap();
        assert!(
            store.get_live_locks(&frame, store.time_ms).is_empty(),
            "all locks expired"
        );
    }
}
