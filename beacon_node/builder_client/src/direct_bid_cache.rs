use eth2::types::{
    BoostedValue, EthSpec, ExecutionBlockHash, Hash256, SignedExecutionPayloadBid, Slot,
};
use parking_lot::RwLock;
use sensitive_url::SensitiveUrl;
use std::collections::{BTreeMap, HashMap, hash_map};
use std::sync::Arc;

/// A direct builder bid together with the provenance needed to act on it later.
#[derive(Clone)]
pub struct DirectBid<E: EthSpec> {
    /// The signed bid returned by the builder.
    pub signed_bid: Arc<SignedExecutionPayloadBid<E>>,
    /// URL of the builder that returned this bid.
    ///
    /// Retained so that, if this bid wins selection, the signed block can be forwarded to the
    /// builder via `submitSignedBeaconBlock`. Provenance is tracked here rather than inferred from
    /// `builder_index`, since a URL may serve bids signed by rotating builder keys.
    pub builder_url: SensitiveUrl,
    /// Whether the builder served this bid as SSZ.
    ///
    /// Reused to choose the request-body encoding for `submit_signed_beacon_block`, so we don't
    /// have to re-probe the builder's SSZ support.
    pub ssz_response: bool,
    /// The proposer's `max_execution_payment` cap for this bid's builder, from its `BuilderEntry`.
    ///
    /// The proposer trusts at most this much of the (off-chain) `execution_payment` from this builder
    /// when ranking the bid (see [`clamped_value`](Self::clamped_value) /
    /// [`boosted_value`](Self::boosted_value)). It does not affect the reported
    /// [`proposer_value`](Self::proposer_value).
    pub max_execution_payment: u64,
    /// The proposer's `builder_boost_factor` for this bid's builder, from its `BuilderEntry`.
    ///
    /// Scales the bid via [`boosted_value`](Self::boosted_value) — the cache's ranking key, and what
    /// the block producer compares against the locally-built payload at selection.
    pub builder_boost_factor: u64,
}

impl<E: EthSpec> DirectBid<E> {
    /// Build a [`DirectBid`] from a bid and the proposer's per-builder inputs.
    pub fn new(
        signed_bid: Arc<SignedExecutionPayloadBid<E>>,
        builder_url: SensitiveUrl,
        ssz_response: bool,
        max_execution_payment: u64,
        builder_boost_factor: u64,
    ) -> Self {
        Self {
            signed_bid,
            builder_url,
            ssz_response,
            max_execution_payment,
            builder_boost_factor,
        }
    }

    /// The proposer's honest (unclamped) value for this bid in gwei: `value + execution_payment`.
    ///
    /// Reported if the bid wins; **never used in ranking** — being unclamped, it must not influence
    /// selection.
    pub fn proposer_value(&self) -> u64 {
        self.signed_bid.message.proposer_value()
    }

    /// The bid's clamped (trusted) value in gwei under this builder's `max_execution_payment` — the
    /// `min_bid` acceptance floor, and the basis of [`boosted_value`](Self::boosted_value).
    pub fn clamped_value(&self) -> u64 {
        self.signed_bid
            .message
            .clamped_value(self.max_execution_payment)
    }

    /// The bid's boost-adjusted ranking key under this builder's preferences — the cache's ordering,
    /// and what the block producer compares against the locally-built payload at selection.
    pub fn boosted_value(&self) -> BoostedValue {
        self.signed_bid
            .message
            .boosted_value(self.max_execution_payment, self.builder_boost_factor)
    }
}

/// The highest-[`boosted_value`](DirectBid::boosted_value) direct bid seen per
/// `(slot, parent_block_hash, parent_block_root)` tuple.
///
/// Keyed first by `Slot` (in a `BTreeMap`, so stale slots can be pruned cheaply via `split_off`),
/// then by the `(parent_block_hash, parent_block_root)` of the block the bid builds on.
type HighestBidMap<E> = BTreeMap<Slot, HashMap<(ExecutionBlockHash, Hash256), DirectBid<E>>>;

/// A cache of direct builder bids, retaining the highest-[`boosted_value`](DirectBid::boosted_value)
/// bid (and its provenance) for each `(slot, parent_block_hash, parent_block_root)` tuple.
///
/// This mirrors the gossip bid cache, but caches bids fetched directly from builders via
/// `getExecutionPayloadBid` and additionally records each winning bid's provenance. Unlike the
/// gossip cache it does not track seen builders: direct bids are not subject to the gossip
/// one-bid-per-builder-per-slot rule.
pub struct DirectBidCache<E: EthSpec> {
    inner: RwLock<HighestBidMap<E>>,
}

impl<E: EthSpec> Default for DirectBidCache<E> {
    fn default() -> Self {
        Self {
            inner: RwLock::new(BTreeMap::new()),
        }
    }
}

impl<E: EthSpec> DirectBidCache<E> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the highest-value cached bid (and its provenance) for the tuple
    /// `(slot, parent_block_hash, parent_block_root)`, if one exists.
    pub fn get_highest_bid(
        &self,
        slot: Slot,
        parent_block_hash: ExecutionBlockHash,
        parent_block_root: Hash256,
    ) -> Option<DirectBid<E>> {
        self.inner
            .read()
            .get(&slot)
            .and_then(|map| map.get(&(parent_block_hash, parent_block_root)).cloned())
    }

    /// Record a direct `bid` in the cache.
    ///
    /// If the bid has a strictly higher [`boosted_value`](DirectBid::boosted_value) than the currently
    /// cached bid for its `(slot, parent_block_hash, parent_block_root)` tuple (or no bid is cached
    /// yet), it replaces the cached bid and its provenance. Ties retain the earlier bid.
    ///
    /// Returns `true` if the bid became the new highest bid for its tuple, or `false` if an
    /// existing cached bid ranked equal or higher and was retained.
    pub fn observe_bid(&self, bid: DirectBid<E>) -> bool {
        let slot = bid.signed_bid.message.slot;
        let key = (
            bid.signed_bid.message.parent_block_hash,
            bid.signed_bid.message.parent_block_root,
        );

        let mut inner = self.inner.write();
        match inner.entry(slot).or_default().entry(key) {
            hash_map::Entry::Vacant(entry) => {
                entry.insert(bid);
                true
            }
            // Rank on boosted value; ties retain the earlier bid.
            hash_map::Entry::Occupied(mut entry) => {
                if entry.get().boosted_value() >= bid.boosted_value() {
                    return false;
                }
                entry.insert(bid);
                true
            }
        }
    }

    /// Remove all cached bids for slots older than `current_slot`.
    ///
    /// Entries for `current_slot` and later are retained.
    pub fn prune(&self, current_slot: Slot) {
        let mut inner = self.inner.write();
        let retained = inner.split_off(&current_slot);
        *inner = retained;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bls::Signature;
    use eth2::types::{ExecutionPayloadBid, MainnetEthSpec};
    use std::str::FromStr;

    type E = MainnetEthSpec;

    #[allow(clippy::too_many_arguments)]
    fn direct_bid(
        slot: u64,
        value: u64,
        execution_payment: u64,
        max_execution_payment: u64,
        url: &str,
        ssz_response: bool,
    ) -> DirectBid<E> {
        let signed_bid = Arc::new(SignedExecutionPayloadBid {
            message: ExecutionPayloadBid {
                slot: Slot::new(slot),
                parent_block_hash: ExecutionBlockHash::zero(),
                parent_block_root: Hash256::ZERO,
                value,
                execution_payment,
                ..ExecutionPayloadBid::default()
            },
            signature: Signature::empty(),
        });
        DirectBid::new(
            signed_bid,
            SensitiveUrl::from_str(url).unwrap(),
            ssz_response,
            max_execution_payment,
            // Boost factor is not exercised by the cache-ranking tests.
            100,
        )
    }

    #[test]
    fn direct_bid_value_methods_apply_builder_policy() {
        // value 100, execution_payment 500, cap 200, boost 100 (neutral).
        let bid = direct_bid(1, 100, 500, 200, "http://a.com", false);
        // Unclamped (reported): 100 + 500.
        assert_eq!(bid.proposer_value(), 600);
        // Clamped to the cap (min_bid floor / ranking basis): 100 + min(500, 200).
        assert_eq!(bid.clamped_value(), 300);
        // Neutral boost leaves the clamped value unchanged.
        assert_eq!(bid.boosted_value(), BoostedValue::Boosted(300));
    }

    #[test]
    fn observe_keeps_highest_and_records_provenance() {
        let cache = DirectBidCache::<E>::new();
        let hash = ExecutionBlockHash::zero();
        let root = Hash256::ZERO;

        // Boosted value 100 (neutral boost, no payment).
        assert!(cache.observe_bid(direct_bid(1, 100, 0, 0, "http://a.com", false)));
        // Lower boosted value is rejected.
        assert!(!cache.observe_bid(direct_bid(1, 50, 0, 0, "http://b.com", false)));
        // Equal boosted value is rejected (ties retain the earlier bid).
        assert!(!cache.observe_bid(direct_bid(1, 100, 0, 0, "http://b.com", false)));
        // A bid that only wins via its (capped) execution_payment: 150 + min(100, 100) = 250.
        assert!(cache.observe_bid(direct_bid(1, 150, 100, 100, "http://c.com", true)));

        let highest = cache.get_highest_bid(Slot::new(1), hash, root).unwrap();
        assert_eq!(highest.proposer_value(), 250);
        assert!(highest.ssz_response);
        assert_eq!(
            highest.builder_url.expose_full(),
            SensitiveUrl::from_str("http://c.com")
                .unwrap()
                .expose_full()
        );
    }

    #[test]
    fn observe_ranks_by_boosted_value() {
        let cache = DirectBidCache::<E>::new();
        let hash = ExecutionBlockHash::zero();
        let root = Hash256::ZERO;

        // Bid A: clamped value 300, default boost 100 -> boosted (300 * 100) / 100 = 300.
        assert!(cache.observe_bid(direct_bid(1, 300, 0, 0, "http://a.com", false)));

        // Bid B: lower value 200, but boost 200 -> boosted (200 * 200) / 100 = 400, so it wins
        // despite the lower unboosted value.
        let mut bid_b = direct_bid(1, 200, 0, 0, "http://b.com", false);
        bid_b.builder_boost_factor = 200;
        assert!(cache.observe_bid(bid_b));

        let highest = cache.get_highest_bid(Slot::new(1), hash, root).unwrap();
        assert_eq!(highest.proposer_value(), 200);
        assert_eq!(highest.builder_boost_factor, 200);
    }

    #[test]
    fn distinct_parents_do_not_collide() {
        let cache = DirectBidCache::<E>::new();
        let root = Hash256::ZERO;

        let mut bid_a = direct_bid(1, 100, 0, 0, "http://a.com", false);
        // Give this bid a different parent hash so it occupies a separate cache slot.
        Arc::make_mut(&mut bid_a.signed_bid)
            .message
            .parent_block_hash = ExecutionBlockHash::repeat_byte(9);
        cache.observe_bid(bid_a);
        cache.observe_bid(direct_bid(1, 200, 0, 0, "http://b.com", false));

        assert_eq!(
            cache
                .get_highest_bid(Slot::new(1), ExecutionBlockHash::repeat_byte(9), root)
                .unwrap()
                .proposer_value(),
            100
        );
        assert_eq!(
            cache
                .get_highest_bid(Slot::new(1), ExecutionBlockHash::zero(), root)
                .unwrap()
                .proposer_value(),
            200
        );
    }

    #[test]
    fn prune_removes_old_retains_current() {
        let cache = DirectBidCache::<E>::new();
        let hash = ExecutionBlockHash::zero();
        let root = Hash256::ZERO;

        for slot in [1u64, 2, 3, 7, 8, 9, 10] {
            cache.observe_bid(direct_bid(slot, slot * 100, 0, 0, "http://a.com", false));
        }

        cache.prune(Slot::new(8));

        for slot in [1u64, 2, 3, 7] {
            assert!(cache.get_highest_bid(Slot::new(slot), hash, root).is_none());
        }
        for slot in [8u64, 9, 10] {
            assert!(cache.get_highest_bid(Slot::new(slot), hash, root).is_some());
        }
    }
}
