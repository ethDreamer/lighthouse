//! EIP-8453 (decoupled consensus) fork choice: a standalone, spec-shaped implementation of
//! `specs/_features/eip8453/fork-choice.md` (branch `safe-votes`).
//!
//! This deliberately mirrors the pyspec `Store` rather than extending `proto_array`: head votes
//! carry weight for one slot only and safe votes for one expiry window, so there is nothing to
//! incrementalize — every score is recomputed per call over at most a committee of seats. The
//! store keeps per-block *metadata* instead of full blocks/states (Lighthouse's store owns
//! those); the caller folds each imported block's post-state FG summary via [`BlockSummary`].
//!
//! Prototype honesty notes (all greppable):
//! - EIP8453-STUB: PTC payload timeliness / data availability votes are not tracked; a payload
//!   counts as timely and available once verified.
//! - EIP8453-STUB: stabilization grades and confirmation records are carried but not yet
//!   evaluated (`on_tick` only advances the clock); `get_sg_root` therefore reduces to the FG
//!   root. Phase 4b implements `update_grades`/`update_confirmation`.
//! - Head-vote committees are served from a snapshot of the latest finalized block's post-state
//!   (the spec's dependent state), refreshed by the caller on finality advances.

use safe_arith::ArithError;
use std::collections::{BTreeMap, HashMap, HashSet};
use types::consts::eip8453::Round;
use types::consts::gloas::{
    PAYLOAD_STATUS_EMPTY, PAYLOAD_STATUS_FULL, PAYLOAD_STATUS_PENDING, PayloadStatus,
};
use types::eip8453::{
    HeadVoteData, HeightCheckpoint, SafeVoteData, compute_round_at_slot,
    compute_start_slot_at_round,
};
use types::{EthSpec, ExecutionBlockHash, Hash256, Slot};

#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    MissingBlock(Hash256),
    MissingCommittee(Slot),
    MissingArrival(Hash256),
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

/// The spec's `StoreHeadVote`: one voter, the signed data, and the arrival time.
#[derive(Debug, Clone, PartialEq)]
pub struct StoreHeadVote {
    pub validator_index: u64,
    pub data: HeadVoteData,
    pub arrival_ms: u64,
}

/// The spec's `StoreSafeVote`.
#[derive(Debug, Clone, PartialEq)]
pub struct StoreSafeVote {
    pub validator_index: u64,
    pub data: SafeVoteData,
    pub arrival_ms: u64,
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
    pub arrival_ms: u64,
    pub timeliness: [bool; 2],
    /// The head votes the block carried, with each voter's published support bit: the proposer's
    /// view, read by `get_voter_head`'s view merge.
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

    // Vote pools
    pub head_votes: BTreeMap<Slot, Vec<StoreHeadVote>>,
    pub safe_votes: BTreeMap<Round, Vec<StoreSafeVote>>,

    // Stabilization grades
    pub grade_2: Option<FgNode>,
    pub grade_1: Option<FgNode>,
    pub grade_0: Option<FgNode>,
    pub grade_2_round: Round,
    pub grade_1_round: Round,
    pub grade_0_round: Round,

    // Confirmation records
    pub confirmed: FgNode,
    pub latest_confirmed: FgNode,
    pub latest_stable: FgNode,
    pub confirmation_slot: Slot,

    /// Head-vote committees per slot, snapshotted from the latest finalized block's post-state
    /// (the spec's `get_head_vote_committee_at_slot` dependent state). Refreshed by the caller
    /// whenever the finalized block changes.
    committees: BTreeMap<Slot, Vec<u64>>,

    /// FG validator weights from the same dependent state as the committees (the spec reads the
    /// latest finalized block's post-state), for the stabilization rule's relative majority.
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
                arrival_ms: time_ms,
                timeliness: [true, true],
                carried_head_votes: Vec::new(),
            },
        );
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
            head_votes: BTreeMap::new(),
            safe_votes: BTreeMap::new(),
            grade_2: None,
            grade_1: None,
            grade_0: None,
            grade_2_round: compute_round_at_slot::<E>(anchor_slot),
            grade_1_round: compute_round_at_slot::<E>(anchor_slot),
            grade_0_round: compute_round_at_slot::<E>(anchor_slot),
            confirmed: anchor_node,
            latest_confirmed: anchor_node,
            latest_stable: anchor_node,
            confirmation_slot: anchor_slot,
            committees: BTreeMap::new(),
            finalized_weights: HashMap::new(),
            _phantom: std::marker::PhantomData,
        })
    }

    /* ---------------- Time ---------------- */

    pub fn get_current_slot(&self) -> Slot {
        let since_genesis_ms = self.time_ms.saturating_sub(self.genesis_time_ms);
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

    /// `get_head_vote_due_ms` (one quarter-slot).
    fn head_vote_due_ms(&self) -> u64 {
        self.slot_component_duration_ms(2500)
    }

    /// `get_head_vote_support_due_ms`.
    fn head_vote_support_due_ms(&self) -> u64 {
        self.slot_component_duration_ms(5000)
    }

    /// `get_view_freeze_due_ms`.
    fn view_freeze_due_ms(&self) -> u64 {
        self.slot_component_duration_ms(7500)
    }

    /// The spec's `on_tick`: advance the clock, evaluate due grades, and evaluate the
    /// confirmation of the most recent due slot (each slot at most once; the cutoffs are
    /// receipt-time comparisons, so a late evaluation reads the same sets a timely one would).
    pub fn on_tick(&mut self, time_ms: u64) -> Result<(), Error> {
        if time_ms > self.time_ms {
            self.time_ms = time_ms;
        }
        self.update_grades()?;
        let current_slot = self.get_current_slot();
        if current_slot <= self.genesis_slot.saturating_add(1u64) {
            return Ok(());
        }
        let confirmation_due_ms = self
            .slot_start_ms(current_slot)
            .saturating_add(self.head_vote_support_due_ms());
        let evaluation_slot = if self.time_ms >= confirmation_due_ms {
            current_slot.saturating_sub(1u64)
        } else {
            current_slot.saturating_sub(2u64)
        };
        if evaluation_slot > self.confirmation_slot {
            self.update_confirmation(evaluation_slot)?;
            self.confirmation_slot = evaluation_slot;
        }
        Ok(())
    }

    /// Advance the clock to the start of `slot`.
    ///
    /// Lighthouse's production fork choice ticks at slot granularity; sub-slot cutoffs are
    /// receipt-time comparisons, so grades and confirmations evaluated at the next slot start
    /// read the same sets a timely evaluation would have.
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

    /// The spec's `on_block`, minus the state transition (the caller has already imported and
    /// verified the block; carried votes were verified by the state transition).
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

        // `record_block_timeliness`: the head-vote deadline and the support cutoff stand in for
        // the pre-EIP-8453 attestation/PTC deadlines.
        let is_current_slot = self.get_current_slot() == slot;
        let time_into_slot_ms = self.time_ms.saturating_sub(self.slot_start_ms(slot));
        let timeliness = [
            is_current_slot && time_into_slot_ms < self.slot_component_duration_ms(2500),
            is_current_slot && time_into_slot_ms < self.slot_component_duration_ms(5000),
        ];

        self.blocks.insert(
            block_root,
            BlockMeta {
                parent_root,
                slot,
                proposer_index,
                state_target_height: summary.target_height,
                bid_block_hash,
                bid_parent_hash,
                arrival_ms: self.time_ms,
                timeliness,
                carried_head_votes: carried_head_votes.clone(),
            },
        );
        self.children
            .entry(parent_root)
            .or_default()
            .push(block_root);

        // Carried head votes enter the pool under the wire admission rule.
        for (validator_index, data, _support) in carried_head_votes {
            self.record_head_vote(validator_index, data);
        }

        // Fold the post-state's FG summary into the store.
        self.update_finality(summary)?;

        // Carried safe votes enter the pool after the finality update.
        for (validator_index, data) in carried_safe_votes {
            self.record_safe_vote(validator_index, data);
        }

        Ok(())
    }

    pub fn set_payload_verified(&mut self, root: Hash256) {
        self.payload_verified.insert(root);
    }

    pub fn is_payload_verified(&self, root: &Hash256) -> bool {
        self.payload_verified.contains(root)
    }

    /// The spec's `should_extend_payload`.
    ///
    /// EIP8453-STUB: inclusion-list satisfaction and the PTC timeliness/data-availability votes
    /// are not tracked yet, so they collapse to payload verification.
    pub fn should_extend_payload(&self, root: &Hash256) -> bool {
        self.is_payload_verified(root)
    }

    /* ---------------- Committees ---------------- */

    /// Replace the head-vote committee snapshot (from the latest finalized block's post-state).
    pub fn set_committees(&mut self, committees: BTreeMap<Slot, Vec<u64>>) {
        self.committees = committees;
    }

    /// Merge one slot's committee into the snapshot.
    pub fn insert_committee(&mut self, slot: Slot, committee: Vec<u64>) {
        self.committees.insert(slot, committee);
    }

    /// Replace the FG validator weights (from the same dependent state as the committees).
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

    /* ---------------- Vote pools ---------------- */

    /// The spec's `record_head_vote`. A refused vote is not consumed.
    pub fn record_head_vote(&mut self, validator_index: u64, data: HeadVoteData) {
        let current_slot = self.get_current_slot();
        if data.slot.saturating_add(1u64) < current_slot || data.slot > current_slot {
            return;
        }
        let Ok(committee) = self.committee_at_slot(data.slot) else {
            return;
        };
        if !committee.contains(&validator_index) {
            return;
        }
        let votes = self.head_votes.entry(data.slot).or_default();
        let mut distinct: Vec<&HeadVoteData> = Vec::new();
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
        votes.push(StoreHeadVote {
            validator_index,
            data,
            arrival_ms: self.time_ms,
        });
    }

    /// The spec's `on_head_vote` (payload-status check + record).
    pub fn on_head_vote(&mut self, validator_index: u64, data: HeadVoteData) {
        if ![
            PAYLOAD_STATUS_EMPTY,
            PAYLOAD_STATUS_FULL,
            PAYLOAD_STATUS_PENDING,
        ]
        .contains(&data.payload_status)
        {
            return;
        }
        self.record_head_vote(validator_index, data);
    }

    /// The spec's `record_safe_vote`. A refused vote is not consumed.
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

    fn slot_head_votes(&self, slot: Slot) -> &[StoreHeadVote] {
        self.head_votes.get(&slot).map(Vec::as_slice).unwrap_or(&[])
    }

    /// The spec's `get_resolution_time_ms`: when the vote's head became available, or `None`.
    fn resolution_time_ms(&self, vote: &StoreHeadVote) -> Option<u64> {
        let meta = self.blocks.get(&vote.data.beacon_block_root)?;
        if meta.slot > vote.data.slot {
            return None;
        }
        Some(std::cmp::max(vote.arrival_ms, meta.arrival_ms))
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
    // Used by `get_safe_vote_node` in Phase 4b.
    #[allow(dead_code)]
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

    /// The spec's `is_ancestor` over fork-choice nodes.
    pub fn is_ancestor(&self, node: FgNode, ancestor: FgNode) -> Result<bool, Error> {
        let ancestor_slot = self.block(&ancestor.root)?.slot;
        let node_ancestor = self.get_ancestor(node, ancestor_slot)?;
        if node_ancestor.root != ancestor.root {
            return Ok(false);
        }
        Ok(node_ancestor.payload_status == ancestor.payload_status
            || ancestor.payload_status == PAYLOAD_STATUS_PENDING)
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

    /// The spec's `get_payload_status_tiebreaker`.
    ///
    /// EIP8453-STUB: the previous-slot payload-decision refinement (which consults
    /// `should_extend_payload` over PTC votes) is reduced to the raw payload status; PTC vote
    /// tracking lands with the p2p work.
    fn payload_status_tiebreaker(&self, node: &FgNode) -> u8 {
        node.payload_status
    }

    /* ---------------- FG restrictions ---------------- */

    /// The spec's `get_fg_root`.
    pub fn get_fg_root(&self) -> Hash256 {
        if self.max_height == self.justified_checkpoint.height.saturating_add(1) {
            self.justified_checkpoint.root
        } else {
            self.finalized_checkpoint.root
        }
    }

    /// The spec's `is_viable_root`: has a descendant whose state height is within one of the
    /// maximum seen.
    pub fn is_viable_root(&self, root: Hash256) -> Result<bool, Error> {
        for (tip_root, tip_meta) in &self.blocks {
            if tip_meta.state_target_height.saturating_add(1) < self.max_height {
                continue;
            }
            if self.is_ancestor_root(root, *tip_root)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// The spec's `get_filtered_roots`: blocks descending the FG root with a viable descendant.
    pub fn get_filtered_roots(&self) -> Result<HashSet<Hash256>, Error> {
        let all: HashSet<Hash256> = self.blocks.keys().copied().collect();
        self.get_filtered_roots_from(&all)
    }

    /// The spec's `get_filtered_roots_from`, over a caller-supplied block view.
    pub fn get_filtered_roots_from(
        &self,
        roots: &HashSet<Hash256>,
    ) -> Result<HashSet<Hash256>, Error> {
        let fg_root = self.get_fg_root();
        let mut candidates = Vec::new();
        for root in roots {
            if self.is_ancestor_root(fg_root, *root)? {
                candidates.push(*root);
            }
        }
        let tips: Vec<Hash256> = candidates
            .iter()
            .copied()
            .filter(|root| {
                self.blocks.get(root).is_some_and(|meta| {
                    meta.state_target_height.saturating_add(1) >= self.max_height
                })
            })
            .collect();
        let mut filtered = HashSet::new();
        for root in candidates {
            for tip in &tips {
                if self.is_ancestor_root(root, *tip)? {
                    filtered.insert(root);
                    break;
                }
            }
        }
        Ok(filtered)
    }

    /* ---------------- Goldfish score and walk ---------------- */

    fn seat_count(committee: &[u64], validator_index: u64) -> u64 {
        committee
            .iter()
            .filter(|&&index| index == validator_index)
            .count() as u64
    }

    /// The spec's `get_equivocating_voters`.
    fn equivocating_voters(votes: &[StoreHeadVote]) -> HashSet<u64> {
        let mut first_votes: HashMap<u64, &HeadVoteData> = HashMap::new();
        let mut equivocating = HashSet::new();
        for vote in votes {
            if let Some(first) = first_votes.get(&vote.validator_index) {
                if **first != vote.data {
                    equivocating.insert(vote.validator_index);
                }
            } else {
                first_votes.insert(vote.validator_index, &vote.data);
            }
        }
        equivocating
    }

    /// The spec's `get_participating_seats`.
    fn participating_seats(committee: &[u64], votes: &[StoreHeadVote]) -> u64 {
        let voters: HashSet<u64> = votes.iter().map(|vote| vote.validator_index).collect();
        voters
            .iter()
            .map(|&voter| Self::seat_count(committee, voter))
            .sum()
    }

    /// The spec's `get_goldfish_weight`, in seats.
    fn goldfish_weight(
        &self,
        committee: &[u64],
        votes: &[StoreHeadVote],
        support_votes: &[&StoreHeadVote],
        node: &FgNode,
    ) -> Result<u64, Error> {
        let equivocating = Self::equivocating_voters(votes);
        let mut supporting: HashSet<u64> = HashSet::new();
        for vote in support_votes {
            if equivocating.contains(&vote.validator_index) {
                continue;
            }
            if !self.blocks.contains_key(&vote.data.beacon_block_root) {
                continue;
            }
            let vote_node = FgNode {
                root: vote.data.beacon_block_root,
                payload_status: vote.data.payload_status,
            };
            if self.is_ancestor(vote_node, *node)? {
                supporting.insert(vote.validator_index);
            }
        }
        let mut weight: u64 = 0;
        for &voter in &equivocating {
            weight = weight.saturating_add(Self::seat_count(committee, voter));
        }
        for &voter in &supporting {
            weight = weight.saturating_add(Self::seat_count(committee, voter));
        }
        Ok(weight)
    }

    /// The spec's `is_goldfish_eligible`.
    fn is_goldfish_eligible(
        &self,
        committee: &[u64],
        votes: &[StoreHeadVote],
        support_votes: &[&StoreHeadVote],
        node: &FgNode,
        majority_only: bool,
    ) -> Result<bool, Error> {
        let meta = self.block(&node.root)?;
        if !majority_only {
            if meta.slot == self.get_current_slot() {
                return Ok(true);
            }
            if let Some(parent) = self.blocks.get(&meta.parent_root)
                && parent.state_target_height.saturating_add(1) < self.max_height
            {
                return Ok(true);
            }
        }
        let weight = self.goldfish_weight(committee, votes, support_votes, node)?;
        Ok(weight.saturating_mul(2) > Self::participating_seats(committee, votes))
    }

    /// The spec's `compute_goldfish_head`: the GHOST walk.
    #[allow(clippy::too_many_arguments)]
    fn compute_goldfish_head(
        &self,
        anchor: FgNode,
        view: &HashSet<Hash256>,
        committee: &[u64],
        votes: &[StoreHeadVote],
        support_votes: &[&StoreHeadVote],
        majority_only: bool,
    ) -> Result<FgNode, Error> {
        let mut head = anchor;
        loop {
            let mut eligible_children = Vec::new();
            for child in self.node_children(&head)? {
                if !view.contains(&child.root) {
                    continue;
                }
                if self.is_goldfish_eligible(
                    committee,
                    votes,
                    support_votes,
                    &child,
                    majority_only,
                )? {
                    let weight = self.goldfish_weight(committee, votes, support_votes, &child)?;
                    eligible_children.push((weight, child));
                }
            }
            let Some(best) = eligible_children.into_iter().max_by_key(|(weight, child)| {
                (*weight, child.root, self.payload_status_tiebreaker(child))
            }) else {
                return Ok(head);
            };
            head = best.1;
        }
    }

    /// The spec's `get_sg_root`: the anchor of every walk — the grade-1 node's active prefix,
    /// or the FG root.
    pub fn get_sg_root(&self) -> Result<FgNode, Error> {
        if let Some(prefix) = self.get_active_prefix(self.grade_1)? {
            return Ok(prefix);
        }
        Ok(FgNode::pending(self.get_fg_root()))
    }

    /// The spec's `get_stable_root`.
    pub fn get_stable_root(&self) -> Result<FgNode, Error> {
        if let Some(prefix) = self.get_active_prefix(self.grade_2)? {
            return Ok(prefix);
        }
        Ok(FgNode::pending(self.get_fg_root()))
    }

    /// The spec's `is_grade_0_compatible`.
    pub fn is_grade_0_compatible(&self, node: FgNode) -> Result<bool, Error> {
        let Some(veto) = self.grade_0 else {
            return Ok(true);
        };
        if !self.blocks.contains_key(&veto.root) || !self.blocks.contains_key(&node.root) {
            return Ok(true);
        }
        Ok(self.is_ancestor(node, veto)? || self.is_ancestor(veto, node)?)
    }

    /// The spec's `has_fresh_safe_quorum`: whether a height pair may be emitted this round.
    pub fn has_fresh_safe_quorum(&self) -> Result<bool, Error> {
        Ok(self.get_active_prefix(self.grade_2)?.is_some())
    }

    /// The spec's `get_safe_vote_node`: the node an honest validator names in its safe vote.
    pub fn get_safe_vote_node(&self) -> Result<FgNode, Error> {
        let anchor = self.get_sg_root()?;
        let confirmed = self.confirmed;
        if self.blocks.contains_key(&confirmed.root)
            && self.blocks.contains_key(&anchor.root)
            && self.is_ancestor(confirmed, anchor)?
        {
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
        if let Some(prefix) = self.get_active_prefix(self.grade_2)? {
            return Ok(prefix);
        }
        if self.grade_2.is_some() {
            return Ok(FgNode::pending(self.get_fg_root()));
        }
        Ok(anchor)
    }

    /// The spec's `get_head`.
    ///
    /// `vote_slot` selects whose votes weigh the walk; `None` uses the previous slot's votes
    /// (the latest complete generation).
    pub fn get_head(&self, vote_slot: Option<Slot>) -> Result<FgNode, Error> {
        let current_slot = self.get_current_slot();
        let vote_slot = vote_slot.unwrap_or_else(|| {
            if current_slot > self.genesis_slot {
                current_slot.saturating_sub(1u64)
            } else {
                self.genesis_slot
            }
        });
        let committee = match self.committee_at_slot(vote_slot) {
            Ok(committee) => committee.as_slice(),
            // Without a committee for the slot no votes could have been admitted either; walk
            // with an empty committee rather than failing head computation.
            Err(_) => &[],
        };
        let votes = self.slot_head_votes(vote_slot);
        let support_votes: Vec<&StoreHeadVote> = votes
            .iter()
            .filter(|vote| self.resolution_time_ms(vote).is_some())
            .collect();
        let anchor = self.get_sg_root()?;
        let view = self.get_filtered_roots()?;
        self.compute_goldfish_head(anchor, &view, committee, votes, &support_votes, false)
    }

    /* ---------------- FG summary ---------------- */

    /// The spec's `update_finality`: fold a post-state summary into the store.
    pub fn update_finality(&mut self, summary: BlockSummary) -> Result<(), Error> {
        self.max_height = std::cmp::max(self.max_height, summary.target_height);

        let justified = summary.justified;
        if self.blocks.contains_key(&justified.root)
            && self.is_ancestor_root(self.finalized_checkpoint.root, justified.root)?
        {
            let store_justified = self.justified_checkpoint;
            if (justified.height, justified.root) > (store_justified.height, store_justified.root) {
                self.justified_checkpoint = justified;
            }
        }

        let finalized = summary.finalized;
        if self.blocks.contains_key(&finalized.root) {
            let is_advance = finalized.height > self.finalized_checkpoint.height
                && self.is_ancestor_root(self.finalized_checkpoint.root, finalized.root)?
                && self.is_ancestor_root(finalized.root, self.justified_checkpoint.root)?
                && self.is_viable_root(finalized.root)?;
            if is_advance {
                self.finalized_checkpoint = finalized;
                self.finalized_slot = summary.finalized_slot;
                // A finality advance cuts every saved grade back to its deepest ancestor
                // compatible with the new finalized block.
                self.grade_2 = self.clip_node_to_finalized(self.grade_2)?;
                self.grade_1 = self.clip_node_to_finalized(self.grade_1)?;
                self.grade_0 = self.clip_node_to_finalized(self.grade_0)?;
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
        let meta = self.blocks.get(&root)?;
        let finalized_root = self.finalized_checkpoint.root;
        let is_compatible = self.is_ancestor_root(finalized_root, root).unwrap_or(false)
            || self.is_ancestor_root(root, finalized_root).unwrap_or(false);
        if !is_compatible {
            return None;
        }
        Some(std::cmp::max(vote.arrival_ms, meta.arrival_ms))
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
        graded.into_iter().max_by_key(|node| {
            (
                self.blocks.get(&node.root).map(|meta| meta.slot),
                u64::from(node.payload_status != PAYLOAD_STATUS_PENDING),
                node.root,
            )
        })
    }

    /// The spec's `update_grades`: evaluate each due grade once, late evaluations reading the
    /// same receipt-time sets a timely one would have.
    pub fn update_grades(&mut self) -> Result<(), Error> {
        let quarter_ms = self.head_vote_due_ms();
        let current_round = compute_round_at_slot::<E>(self.get_current_slot());
        if current_round == 0 {
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

    /// The spec's `get_active_prefix`: the deepest ancestor of a saved grade inside the filtered
    /// tree.
    fn get_active_prefix(&self, node: Option<FgNode>) -> Result<Option<FgNode>, Error> {
        let Some(node) = node else {
            return Ok(None);
        };
        if !self.blocks.contains_key(&node.root) {
            return Ok(None);
        }
        let filtered = self.get_filtered_roots()?;
        let mut current = Some(node);
        while let Some(candidate) = current {
            if filtered.contains(&candidate.root) {
                return Ok(Some(candidate));
            }
            current = self.node_parent(&candidate)?;
        }
        Ok(None)
    }

    /// The spec's `clip_node_to_finalized`.
    fn clip_node_to_finalized(&self, node: Option<FgNode>) -> Result<Option<FgNode>, Error> {
        let Some(node) = node else {
            return Ok(None);
        };
        if !self.blocks.contains_key(&node.root) {
            return Ok(None);
        }
        let finalized_root = self.finalized_checkpoint.root;
        let mut current = Some(node);
        while let Some(candidate) = current {
            let is_compatible = self
                .is_ancestor_root(finalized_root, candidate.root)
                .unwrap_or(false)
                || self
                    .is_ancestor_root(candidate.root, finalized_root)
                    .unwrap_or(false);
            if is_compatible {
                return Ok(Some(candidate));
            }
            current = self.node_parent(&candidate)?;
        }
        Ok(None)
    }

    /* ---------------- Confirmation ---------------- */

    /// `is_ancestor` for the record logic, where a pruned root means "no longer an ancestor".
    fn is_ancestor_or_false(&self, node: FgNode, ancestor: FgNode) -> bool {
        self.is_ancestor(node, ancestor).unwrap_or(false)
    }

    /// The spec's `update_confirmation`, evaluated for `slot` at the support cutoff of the
    /// following slot.
    pub fn update_confirmation(&mut self, slot: Slot) -> Result<(), Error> {
        let early_cutoff_ms = self
            .slot_start_ms(slot)
            .saturating_add(self.head_vote_support_due_ms());
        let late_cutoff_ms = self
            .slot_start_ms(slot.saturating_add(1u64))
            .saturating_add(self.head_vote_support_due_ms());
        let committee: Vec<u64> = self.committee_at_slot(slot).cloned().unwrap_or_default();

        let late_votes: Vec<StoreHeadVote> = self
            .slot_head_votes(slot)
            .iter()
            .filter(|vote| vote.arrival_ms < late_cutoff_ms)
            .cloned()
            .collect();
        let equivocating = Self::equivocating_voters(&late_votes);
        let mut support_votes: Vec<&StoreHeadVote> = Vec::new();
        for vote in &late_votes {
            let Some(resolution_ms) = self.resolution_time_ms(vote) else {
                continue;
            };
            if resolution_ms >= early_cutoff_ms {
                continue;
            }
            if equivocating.contains(&vote.validator_index) {
                continue;
            }
            support_votes.push(vote);
        }

        let anchor = self.get_sg_root()?;
        let view = self.get_filtered_roots()?;
        let head = self.compute_goldfish_head(
            anchor,
            &view,
            &committee,
            &late_votes,
            &support_votes,
            true,
        )?;
        let head_weight = self.goldfish_weight(&committee, &late_votes, &support_votes, &head)?;
        let candidate =
            if head_weight.saturating_mul(2) > Self::participating_seats(&committee, &late_votes) {
                self.confirmed = head;
                Some(head)
            } else {
                self.confirmed = FgNode::pending(self.get_fg_root());
                // The saved grade-2 node stands in for the walk.
                self.get_active_prefix(self.grade_2)?
            };
        self.update_confirmation_records(candidate);
        Ok(())
    }

    /// The spec's `advance_record`.
    fn advance_record(&self, old: FgNode, candidate: Option<FgNode>) -> FgNode {
        match candidate {
            None => old,
            Some(candidate) if candidate == old || self.is_ancestor_or_false(old, candidate) => old,
            Some(candidate) => candidate,
        }
    }

    /// The spec's `update_confirmation_records`: the confirmation record floors on the stable
    /// record, keeping `latest_stable ⪯ latest_confirmed`.
    fn update_confirmation_records(&mut self, candidate: Option<FgNode>) {
        let new_confirmed = self.advance_record(self.latest_confirmed, candidate);
        let stable_candidate = self.get_stable_root().ok();
        self.latest_stable = self.advance_record(self.latest_stable, stable_candidate);
        let stable = self.latest_stable;
        if new_confirmed == stable || self.is_ancestor_or_false(new_confirmed, stable) {
            self.latest_confirmed = new_confirmed;
        } else if !(self.latest_confirmed == stable
            || self.is_ancestor_or_false(self.latest_confirmed, stable))
        {
            self.latest_confirmed = stable;
        }
    }

    /// The spec's `get_stable` (user-facing).
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

    /// The spec's `get_confirmed` (user-facing): `finalized ⪯ stable ⪯ confirmed ⪯ head`.
    pub fn get_confirmed(&self) -> FgNode {
        let stable = self.get_stable();
        let confirmed = self.latest_confirmed;
        if confirmed == stable || self.is_ancestor_or_false(confirmed, stable) {
            return confirmed;
        }
        stable
    }

    /* ---------------- Voter head (AC voting) ---------------- */

    /// The spec's `has_head_vote`.
    fn has_head_vote(votes: &[StoreHeadVote], vote: &StoreHeadVote) -> bool {
        votes.iter().any(|existing| {
            existing.validator_index == vote.validator_index && existing.data == vote.data
        })
    }

    /// The spec's `get_voter_head`: view merge with the current slot's proposals, then the
    /// restricted walk over a frozen view.
    pub fn get_voter_head(&self, slot: Slot) -> Result<FgNode, Error> {
        let vote_slot = slot.saturating_sub(1u64);
        let freeze_ms = self
            .slot_start_ms(vote_slot)
            .saturating_add(self.view_freeze_due_ms());
        let committee: Vec<u64> = self
            .committee_at_slot(vote_slot)
            .cloned()
            .unwrap_or_default();

        let mut votes: Vec<StoreHeadVote> = self
            .slot_head_votes(vote_slot)
            .iter()
            .filter(|vote| vote.arrival_ms < freeze_ms)
            .cloned()
            .collect();
        let mut support_votes: Vec<StoreHeadVote> = Vec::new();
        for vote in &votes {
            if let Some(resolution_ms) = self.resolution_time_ms(vote)
                && resolution_ms < freeze_ms
            {
                support_votes.push(vote.clone());
            }
        }

        // Merge the views published by the current slot's proposals.
        let proposals: Vec<Hash256> = self
            .blocks
            .iter()
            .filter(|(_, meta)| meta.slot == slot)
            .map(|(root, _)| *root)
            .collect();
        for proposal_root in &proposals {
            let carried = self
                .blocks
                .get(proposal_root)
                .map(|meta| meta.carried_head_votes.clone())
                .unwrap_or_default();
            for (validator_index, data, support_bit) in carried {
                if data.slot != vote_slot {
                    continue;
                }
                let vote = StoreHeadVote {
                    validator_index,
                    data,
                    arrival_ms: freeze_ms,
                };
                if !Self::has_head_vote(&votes, &vote) {
                    votes.push(vote.clone());
                }
                let is_resolvable = self
                    .blocks
                    .get(&data.beacon_block_root)
                    .is_some_and(|meta| meta.slot <= vote_slot);
                if support_bit && is_resolvable && !Self::has_head_vote(&support_votes, &vote) {
                    support_votes.push(vote);
                }
            }
        }

        // The vote-domain merge has a block-domain analogue.
        let mut blocks_view: HashSet<Hash256> = HashSet::new();
        for (root, meta) in &self.blocks {
            if meta.arrival_ms < freeze_ms {
                blocks_view.insert(*root);
            } else {
                for proposal_root in &proposals {
                    if self
                        .is_ancestor_root(*root, *proposal_root)
                        .unwrap_or(false)
                    {
                        blocks_view.insert(*root);
                        break;
                    }
                }
            }
        }

        let support_refs: Vec<&StoreHeadVote> = support_votes.iter().collect();
        let anchor = self.get_sg_root()?;
        let view = self.get_filtered_roots_from(&blocks_view)?;
        self.compute_goldfish_head(anchor, &view, &committee, &votes, &support_refs, false)
    }

    /* ---------------- FG attestation sources ---------------- */

    /// The spec's `get_fg_target_source`, as a root (the caller resolves the state).
    pub fn fg_target_source_root(&self) -> Hash256 {
        self.confirmed.root
    }

    /// The spec's `get_fg_finality_source`, as a node (the caller resolves the state).
    pub fn fg_finality_source(&self) -> Result<FgNode, Error> {
        self.get_head(Some(self.get_current_slot()))
    }

    /* ---------------- Pruning ---------------- */

    /// Drop blocks that do not descend from the finalized block, blocks older than the finalized
    /// slot, and vote-pool entries no rule can read any more.
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

        // Head votes: only the current and previous slot are ever read.
        let current_slot = self.get_current_slot();
        let keep_from = current_slot.saturating_sub(1u64);
        self.head_votes.retain(|slot, _| *slot >= keep_from);

        // Safe votes: only the expiry window is ever read.
        let current_round = compute_round_at_slot::<E>(current_slot);
        let keep_from_round = current_round.saturating_sub(E::safe_vote_expiry_rounds());
        self.safe_votes.retain(|round, _| *round >= keep_from_round);

        Ok(())
    }
}
