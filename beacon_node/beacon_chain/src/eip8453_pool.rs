//! A deliberately dumb operation pool for the EIP-8453 finality gadget objects.
//!
//! EIP8453-STUB: the real `operation_pool` is Casper-FFG-shaped (`CompactIndexedAttestation`,
//! epoch-keyed pruning, on-chain aggregation machinery), so the prototype keeps the new objects
//! in plain vectors: insert on verification (or from the test harness), drain at block
//! production, prune by staleness against the packing state. No dedup, no aggregation, no
//! persistence.

use parking_lot::Mutex;
use ssz::ProgressiveBitList;
use types::eip8453::{HeadVote, HeadVoteMessage, SafeVote, SafeVoteMessage};
use types::{EthSpec, eip8453};

#[derive(Default)]
pub struct Eip8453OperationPool<E: EthSpec> {
    /// Finality gadget attestation aggregates awaiting inclusion.
    pub fg_attestations: Mutex<Vec<eip8453::Attestation<E>>>,
    /// Head vote aggregates awaiting inclusion, with the support bits the proposer will publish.
    pub head_votes: Mutex<Vec<(HeadVote<E>, ProgressiveBitList)>>,
    /// Safe vote aggregates awaiting inclusion.
    pub safe_votes: Mutex<Vec<SafeVote<E>>>,
    /// Unaggregated FG attestations from validators, aggregated at block production.
    pub fg_singles: Mutex<Vec<eip8453::SingleAttestation>>,
    /// Unaggregated head votes from validators, aggregated at block production.
    pub head_vote_singles: Mutex<Vec<HeadVoteMessage>>,
    /// Unaggregated safe votes from validators, aggregated at block production.
    pub safe_vote_singles: Mutex<Vec<SafeVoteMessage>>,
}

impl<E: EthSpec> Eip8453OperationPool<E> {
    pub fn insert_fg_attestation(&self, attestation: eip8453::Attestation<E>) {
        self.fg_attestations.lock().push(attestation);
    }

    pub fn insert_head_vote(&self, vote: HeadVote<E>, support: ProgressiveBitList) {
        self.head_votes.lock().push((vote, support));
    }

    pub fn insert_safe_vote(&self, vote: SafeVote<E>) {
        self.safe_votes.lock().push(vote);
    }

    pub fn insert_fg_single(&self, attestation: eip8453::SingleAttestation) {
        let mut pool = self.fg_singles.lock();
        // Dedup on (attester, data): re-broadcasts are common and aggregation asserts
        // distinct signers.
        if !pool.iter().any(|existing| {
            existing.attester_index == attestation.attester_index
                && existing.data == attestation.data
        }) {
            pool.push(attestation);
        }
    }

    pub fn insert_head_vote_single(&self, message: HeadVoteMessage) {
        let mut pool = self.head_vote_singles.lock();
        if !pool.iter().any(|existing| {
            existing.validator_index == message.validator_index && existing.data == message.data
        }) {
            pool.push(message);
        }
    }

    pub fn insert_safe_vote_single(&self, message: SafeVoteMessage) {
        let mut pool = self.safe_vote_singles.lock();
        if !pool.iter().any(|existing| {
            existing.validator_index == message.validator_index && existing.data == message.data
        }) {
            pool.push(message);
        }
    }

    /// Drain the unaggregated FG attestations satisfying `filter`, dropping the rest.
    pub fn drain_fg_singles(
        &self,
        filter: impl Fn(&eip8453::SingleAttestation) -> bool,
    ) -> Vec<eip8453::SingleAttestation> {
        self.fg_singles
            .lock()
            .drain(..)
            .filter(|a| filter(a))
            .collect()
    }

    /// Drain the unaggregated head votes satisfying `filter`, dropping the rest.
    pub fn drain_head_vote_singles(
        &self,
        filter: impl Fn(&HeadVoteMessage) -> bool,
    ) -> Vec<HeadVoteMessage> {
        self.head_vote_singles
            .lock()
            .drain(..)
            .filter(|m| filter(m))
            .collect()
    }

    /// Drain the unaggregated safe votes satisfying `filter`, dropping the rest.
    pub fn drain_safe_vote_singles(
        &self,
        filter: impl Fn(&SafeVoteMessage) -> bool,
    ) -> Vec<SafeVoteMessage> {
        self.safe_vote_singles
            .lock()
            .drain(..)
            .filter(|m| filter(m))
            .collect()
    }

    /// Drain up to `max` attestations satisfying `filter`, dropping everything it rejects.
    pub fn drain_fg_attestations(
        &self,
        max: usize,
        filter: impl Fn(&eip8453::Attestation<E>) -> bool,
    ) -> Vec<eip8453::Attestation<E>> {
        let mut pool = self.fg_attestations.lock();
        let mut drained = Vec::new();
        for attestation in pool.drain(..) {
            if drained.len() < max && filter(&attestation) {
                drained.push(attestation);
            }
        }
        drained
    }

    /// Drain up to `max` head votes satisfying `filter`, dropping everything it rejects.
    pub fn drain_head_votes(
        &self,
        max: usize,
        filter: impl Fn(&HeadVote<E>) -> bool,
    ) -> Vec<(HeadVote<E>, ProgressiveBitList)> {
        let mut pool = self.head_votes.lock();
        let mut drained = Vec::new();
        for (vote, support) in pool.drain(..) {
            if drained.len() < max && filter(&vote) {
                drained.push((vote, support));
            }
        }
        drained
    }

    /// Drain up to `max` safe votes satisfying `filter`, dropping everything it rejects.
    pub fn drain_safe_votes(
        &self,
        max: usize,
        filter: impl Fn(&SafeVote<E>) -> bool,
    ) -> Vec<SafeVote<E>> {
        let mut pool = self.safe_votes.lock();
        let mut drained = Vec::new();
        for vote in pool.drain(..) {
            if drained.len() < max && filter(&vote) {
                drained.push(vote);
            }
        }
        drained
    }
}
