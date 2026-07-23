use eth2::types::{EthSpec, ExecutionBlockHash, Hash256, SignedExecutionPayloadBid, Slot};
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
    /// The proposer's value for this bid, used to rank bids for the same tuple.
    ///
    /// This is `bid.value + min(max_execution_payment, bid.execution_payment)`, where
    /// `max_execution_payment` is the proposer's cap for this builder from its
    /// [`BuilderPreferences`](https://github.com/ethereum/beacon-APIs/pull/625): the on-chain
    /// `value` plus however much of the (off-chain) `execution_payment` the proposer is willing to
    /// accept from this builder.
    pub proposer_value: u64,
}

impl<E: EthSpec> DirectBid<E> {
    /// Build a [`DirectBid`], computing its [`proposer_value`](Self::proposer_value) from the bid
    /// and the proposer's `max_execution_payment` cap for the builder that returned it.
    pub fn new(
        signed_bid: Arc<SignedExecutionPayloadBid<E>>,
        builder_url: SensitiveUrl,
        ssz_response: bool,
        max_execution_payment: u64,
    ) -> Self {
        let bid = &signed_bid.message;
        let proposer_value = bid
            .value
            .saturating_add(bid.execution_payment.min(max_execution_payment));
        Self {
            signed_bid,
            builder_url,
            ssz_response,
            proposer_value,
        }
    }
}

/// The highest-value direct bid seen per `(slot, parent_block_hash, parent_block_root)` tuple.
///
/// Keyed first by `Slot` (in a `BTreeMap`, so stale slots can be pruned cheaply via `split_off`),
/// then by the `(parent_block_hash, parent_block_root)` of the block the bid builds on.
type HighestBidMap<E> = BTreeMap<Slot, HashMap<(ExecutionBlockHash, Hash256), DirectBid<E>>>;

/// A cache of direct builder bids, retaining the highest-value bid (and its provenance) for each
/// `(slot, parent_block_hash, parent_block_root)` tuple.
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
    /// If the bid has a strictly higher [`proposer_value`](DirectBid::proposer_value) than the
    /// currently cached bid for its `(slot, parent_block_hash, parent_block_root)` tuple (or no bid
    /// is cached yet), it replaces the cached bid and its provenance.
    ///
    /// Returns `true` if the bid became the new highest bid for its tuple, or `false` if an
    /// existing cached bid had an equal or greater `proposer_value` and was retained.
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
            hash_map::Entry::Occupied(mut entry) => {
                if entry.get().proposer_value >= bid.proposer_value {
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
        )
    }

    #[test]
    fn proposer_value_caps_execution_payment() {
        // execution_payment above the cap is limited to the cap: 100 + min(200, 500) = 300.
        assert_eq!(
            direct_bid(1, 100, 500, 200, "http://a.com", false).proposer_value,
            300
        );
        // execution_payment below the cap is fully counted: 100 + min(200, 50) = 150.
        assert_eq!(
            direct_bid(1, 100, 50, 200, "http://a.com", false).proposer_value,
            150
        );
    }

    #[test]
    fn observe_keeps_highest_and_records_provenance() {
        let cache = DirectBidCache::<E>::new();
        let hash = ExecutionBlockHash::zero();
        let root = Hash256::ZERO;

        // proposer_value 100.
        assert!(cache.observe_bid(direct_bid(1, 100, 0, 0, "http://a.com", false)));
        // Lower proposer_value is rejected.
        assert!(!cache.observe_bid(direct_bid(1, 50, 0, 0, "http://b.com", false)));
        // Equal proposer_value is rejected.
        assert!(!cache.observe_bid(direct_bid(1, 100, 0, 0, "http://b.com", false)));
        // A bid that only wins via its (capped) execution_payment: 150 + min(100, 100) = 250.
        assert!(cache.observe_bid(direct_bid(1, 150, 100, 100, "http://c.com", true)));

        let highest = cache.get_highest_bid(Slot::new(1), hash, root).unwrap();
        assert_eq!(highest.proposer_value, 250);
        assert!(highest.ssz_response);
        assert_eq!(
            highest.builder_url.expose_full(),
            SensitiveUrl::from_str("http://c.com").unwrap().expose_full()
        );
    }

    #[test]
    fn distinct_parents_do_not_collide() {
        let cache = DirectBidCache::<E>::new();
        let root = Hash256::ZERO;

        let mut bid_a = direct_bid(1, 100, 0, 0, "http://a.com", false);
        // Give this bid a different parent hash so it occupies a separate cache slot.
        Arc::make_mut(&mut bid_a.signed_bid).message.parent_block_hash =
            ExecutionBlockHash::repeat_byte(9);
        cache.observe_bid(bid_a);
        cache.observe_bid(direct_bid(1, 200, 0, 0, "http://b.com", false));

        assert_eq!(
            cache
                .get_highest_bid(Slot::new(1), ExecutionBlockHash::repeat_byte(9), root)
                .unwrap()
                .proposer_value,
            100
        );
        assert_eq!(
            cache
                .get_highest_bid(Slot::new(1), ExecutionBlockHash::zero(), root)
                .unwrap()
                .proposer_value,
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
