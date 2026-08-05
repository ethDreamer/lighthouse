//! Fork-agnostic ePBS payload-bid selection.
//!
//! Given the candidate bids for a slot, pick the winner. Selection is **value-based**: it never
//! inspects payload contents, only each candidate's precomputed `rank`.
//!
//! Every candidate — the local self-build and each external bid — is one [`BidCandidate`], tagged by
//! its [`BidSource`]. The source is the single home for per-source data: `Local` carries the
//! [`ExecutionPayloadData`] needed to build the envelope, `Direct` carries the builder URL to route
//! the winning block back to (`Eth-Builder-Url`), and `Gossip` carries nothing. There is no separate
//! "winning bid" type — the winner *is* a [`BidCandidate`], and the caller matches on its `source`.
//!
//! Ranking unifies the local/external asymmetry (local competes unboosted, external boosted) at
//! *construction*: each candidate's [`rank`](BidCandidate) is a [`BoostedValue`] in gwei — the local
//! build's own block value, or an external bid's clamped value scaled by `builder_boost_factor` (or
//! the always-prefer class). Selection is then a single ordering: the EL's `shouldOverrideBuilder`
//! outranks everything, then highest `rank`, then ties go to the local build, then to the earlier
//! candidate.
//!
//! Consumed by `gloas.rs` block production via [`select_payload_bid`].

use std::sync::Arc;
use types::{
    BoostedValue, EthSpec, ExecutionPayloadGloas, ExecutionRequestsGloas,
    SignedExecutionPayloadBid, Slot, Uint256,
};

const GWEI_TO_WEI: u64 = 1_000_000_000;

/// Convert a gwei figure to wei. Saturating, though block values are nowhere near the ceiling.
fn gwei_to_wei(gwei: u64) -> Uint256 {
    Uint256::from(gwei).saturating_mul(Uint256::from(GWEI_TO_WEI))
}

/// Data needed to construct an `ExecutionPayloadEnvelope`, carried by the local candidate and
/// materialized only if it wins.
///
/// Fork-coupling seam: `payload`/`execution_requests` are concrete Gloas types. Selection never
/// inspects them.
pub struct ExecutionPayloadData<E: EthSpec> {
    pub payload: ExecutionPayloadGloas<E>,
    pub execution_requests: ExecutionRequestsGloas<E>,
    pub builder_index: u64,
    pub slot: Slot,
    pub blobs_and_proofs: (types::BlobsList<E>, types::KzgProofs<E>),
}

/// Where a payload bid came from, and the per-source data the winner needs.
pub enum BidSource<E: EthSpec> {
    /// The locally-built payload. Carries the data to construct the envelope (boxed to keep the enum
    /// small) and the EL's `shouldOverrideBuilder` signal.
    Local {
        payload_data: Box<ExecutionPayloadData<E>>,
        should_override_builder: bool,
    },
    /// A bid from the `execution_payload_bid` gossip topic.
    Gossip,
    /// A bid fetched directly from a builder. Carries its URL so a winning block can be routed back
    /// to it via `submitSignedBeaconBlock` (surfaced as the `Eth-Builder-Url` header).
    Direct { builder_url: String },
}

impl<E: EthSpec> BidSource<E> {
    fn overrides_builder(&self) -> bool {
        matches!(
            self,
            BidSource::Local {
                should_override_builder: true,
                ..
            }
        )
    }

    fn is_local(&self) -> bool {
        matches!(self, BidSource::Local { .. })
    }

    /// The winning builder's URL, if this bid came through the builder-API (direct) channel.
    pub fn builder_url(&self) -> Option<&str> {
        match self {
            BidSource::Direct { builder_url } => Some(builder_url),
            _ => None,
        }
    }
}

/// A payload-bid candidate: the committed bid, its ranking key, the value to report if it wins, and
/// its [`BidSource`].
pub struct BidCandidate<E: EthSpec> {
    pub signed_bid: Arc<SignedExecutionPayloadBid<E>>,
    /// Selection key (gwei). Local = its unboosted block value; external = boosted / always-prefer.
    /// Never leaves the selector.
    rank: BoostedValue,
    /// Wei value reported for the winner (`Eth-Execution-Payload-Value`) — the proposer's real,
    /// unboosted revenue. Never used in ranking.
    pub payload_value: Uint256,
    pub source: BidSource<E>,
}

impl<E: EthSpec> BidCandidate<E> {
    /// The local self-build candidate. It competes unboosted, so its `rank` is just its EL block
    /// value (gwei); `block_value` is the wei figure reported if it wins.
    pub fn local(
        signed_bid: SignedExecutionPayloadBid<E>,
        payload_data: ExecutionPayloadData<E>,
        block_value: Uint256,
        should_override_builder: bool,
    ) -> Self {
        let block_value_gwei =
            u64::try_from(block_value / Uint256::from(GWEI_TO_WEI)).unwrap_or(u64::MAX);
        Self {
            signed_bid: Arc::new(signed_bid),
            rank: BoostedValue::Boosted(block_value_gwei),
            payload_value: block_value,
            source: BidSource::Local {
                payload_data: Box::new(payload_data),
                should_override_builder,
            },
        }
    }

    /// An external (gossip or direct) candidate, ranked by the bid's boost-adjusted value under this
    /// builder's resolved policy.
    ///
    /// - `max_execution_payment`: the largest `execution_payment` (gwei) the proposer trusts from
    ///   this builder; `u64::MAX` = no clamp (e.g. gossip bids, whose `execution_payment` is zero),
    ///   `0` = untrusted. Over-cap `execution_payment` is clamped, not rejected (a deliberate
    ///   departure from #630's reject rule): the untrusted excess can't sway selection, but a bid
    ///   that wins on its trusted value still keeps the full payment it promised.
    /// - `builder_boost_factor`: `100` is neutral, `0` prefers local, `u64::MAX` is always-prefer.
    pub fn external(
        signed_bid: Arc<SignedExecutionPayloadBid<E>>,
        max_execution_payment: u64,
        builder_boost_factor: u64,
        source: BidSource<E>,
    ) -> Self {
        let bid = &signed_bid.message;
        let rank = bid.boosted_value(max_execution_payment, builder_boost_factor);
        let payload_value = gwei_to_wei(bid.proposer_value());
        Self {
            signed_bid,
            rank,
            payload_value,
            source,
        }
    }
}

/// Select the winning payload bid.
///
/// The total order (greater = better) is: `shouldOverrideBuilder` (local only) beats everything,
/// then higher [`rank`](BidCandidate), then the local build wins ties over externals. On a full tie
/// (two externals of equal rank) the earlier candidate is kept. Returns `None` only when there are
/// no candidates — the caller treats that as block-production failure.
pub fn select_payload_bid<E: EthSpec>(candidates: Vec<BidCandidate<E>>) -> Option<BidCandidate<E>> {
    // `reduce` keeps `best` unless `candidate` is *strictly* greater, so the earliest of any tied
    // maxima wins.
    candidates.into_iter().reduce(|best, candidate| {
        if rank_key(&candidate) > rank_key(&best) {
            candidate
        } else {
            best
        }
    })
}

/// The lexicographic selection key: override first, then rank, then local-over-external on ties.
fn rank_key<E: EthSpec>(candidate: &BidCandidate<E>) -> (bool, BoostedValue, bool) {
    (
        candidate.source.overrides_builder(),
        candidate.rank,
        candidate.source.is_local(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use bls::Signature;
    use ssz_types::VariableList;
    use types::{ExecutionPayloadBid, MainnetEthSpec};

    type TestSpec = MainnetEthSpec;

    const GOSSIP_BUILDER: u64 = 111;
    const DIRECT_BUILDER: u64 = 222;
    const LOCAL_BUILDER: u64 = 0;

    /// Neutral boost / no clamp, i.e. the "profit maximization" policy.
    const NEUTRAL_BOOST: u64 = 100;
    const NO_CLAMP: u64 = u64::MAX;
    const DIRECT_URL: &str = "http://builder.example.com";

    fn gwei(n: u64) -> Uint256 {
        Uint256::from(n).saturating_mul(Uint256::from(GWEI_TO_WEI))
    }

    fn signed_bid(
        builder_index: u64,
        value_gwei: u64,
        payment_gwei: u64,
    ) -> Arc<SignedExecutionPayloadBid<TestSpec>> {
        Arc::new(SignedExecutionPayloadBid {
            message: ExecutionPayloadBid {
                builder_index,
                value: value_gwei,
                execution_payment: payment_gwei,
                ..Default::default()
            },
            signature: Signature::empty(),
        })
    }

    fn gossip(value_gwei: u64, boost: u64) -> BidCandidate<TestSpec> {
        // Gossip bids carry no execution_payment, so the clamp is irrelevant (no cap).
        BidCandidate::external(
            signed_bid(GOSSIP_BUILDER, value_gwei, 0),
            NO_CLAMP,
            boost,
            BidSource::Gossip,
        )
    }

    fn direct(
        value_gwei: u64,
        payment_gwei: u64,
        boost: u64,
        max_payment: u64,
    ) -> BidCandidate<TestSpec> {
        BidCandidate::external(
            signed_bid(DIRECT_BUILDER, value_gwei, payment_gwei),
            max_payment,
            boost,
            BidSource::Direct {
                builder_url: DIRECT_URL.to_string(),
            },
        )
    }

    fn local(block_value_gwei: u64, should_override_builder: bool) -> BidCandidate<TestSpec> {
        BidCandidate::local(
            SignedExecutionPayloadBid {
                message: ExecutionPayloadBid {
                    builder_index: LOCAL_BUILDER,
                    ..Default::default()
                },
                signature: Signature::empty(),
            },
            ExecutionPayloadData {
                payload: ExecutionPayloadGloas::default(),
                execution_requests: ExecutionRequestsGloas::default(),
                builder_index: LOCAL_BUILDER,
                slot: Slot::new(0),
                blobs_and_proofs: (VariableList::empty(), VariableList::empty()),
            },
            gwei(block_value_gwei),
            should_override_builder,
        )
    }

    /// `(winning_builder_index, is_local, payload_value_wei, source_label)`.
    fn outcome(win: BidCandidate<TestSpec>) -> (u64, bool, Uint256, &'static str) {
        let source = match &win.source {
            BidSource::Local { .. } => "local",
            BidSource::Gossip => "gossip",
            BidSource::Direct { .. } => "direct",
        };
        (
            win.signed_bid.message.builder_index,
            win.source.is_local(),
            win.payload_value,
            source,
        )
    }

    #[test]
    fn local_only_wins() {
        let win = select_payload_bid(vec![local(7, false)]).unwrap();
        assert_eq!(outcome(win), (LOCAL_BUILDER, true, gwei(7), "local"));
    }

    #[test]
    fn external_only_wins_when_no_local() {
        let win = select_payload_bid(vec![gossip(5, NEUTRAL_BOOST)]).unwrap();
        assert_eq!(outcome(win), (GOSSIP_BUILDER, false, gwei(5), "gossip"));
    }

    #[test]
    fn nothing_viable_is_none() {
        assert!(select_payload_bid::<TestSpec>(vec![]).is_none());
    }

    #[test]
    fn el_override_beats_any_external() {
        let win = select_payload_bid(vec![local(1, true), direct(1000, 1000, u64::MAX, NO_CLAMP)])
            .unwrap();
        assert_eq!(outcome(win), (LOCAL_BUILDER, true, gwei(1), "local"));
    }

    #[test]
    fn local_wins_value_tie() {
        // Neutral boost, external rank == local block value ⇒ local wins ties.
        let win = select_payload_bid(vec![local(5, false), gossip(5, NEUTRAL_BOOST)]).unwrap();
        assert_eq!(outcome(win), (LOCAL_BUILDER, true, gwei(5), "local"));
    }

    #[test]
    fn external_wins_when_strictly_higher() {
        let win = select_payload_bid(vec![local(4, false), gossip(5, NEUTRAL_BOOST)]).unwrap();
        assert_eq!(outcome(win), (GOSSIP_BUILDER, false, gwei(5), "gossip"));
    }

    #[test]
    fn direct_bid_counts_execution_payment() {
        // value 2 + payment 4 = 6 ranked (neutral) ⇒ beats local 5, reported at 6.
        let win = select_payload_bid(vec![local(5, false), direct(2, 4, NEUTRAL_BOOST, NO_CLAMP)])
            .unwrap();
        assert_eq!(outcome(win), (DIRECT_BUILDER, false, gwei(6), "direct"));
    }

    #[test]
    fn max_execution_payment_clamps_ranking_but_not_reported_value() {
        // Unclamped: value 1 + payment 10 = 11 ranked (neutral) ⇒ beats local 5.
        let unclamped = select_payload_bid(vec![
            local(5, false),
            direct(1, 10, NEUTRAL_BOOST, NO_CLAMP),
        ])
        .unwrap();
        assert_eq!(
            outcome(unclamped),
            (DIRECT_BUILDER, false, gwei(11), "direct")
        );

        // Clamp payment to 3: ranked value = 1 + min(10, 3) = 4 < local 5 ⇒ local wins.
        let clamped =
            select_payload_bid(vec![local(5, false), direct(1, 10, NEUTRAL_BOOST, 3)]).unwrap();
        assert_eq!(outcome(clamped), (LOCAL_BUILDER, true, gwei(5), "local"));

        // Clamp still lets it win over local 3 (ranked 4 > 3) — but the *reported* value is the
        // unclamped proposer value 11, since the clamp is a ranking-only trust bound.
        let clamped_win =
            select_payload_bid(vec![local(3, false), direct(1, 10, NEUTRAL_BOOST, 3)]).unwrap();
        assert_eq!(
            outcome(clamped_win),
            (DIRECT_BUILDER, false, gwei(11), "direct")
        );
    }

    #[test]
    fn boost_amplifies_external() {
        // Ranked 3 < local 5 ⇒ local; boost 200 ⇒ ranked 6 > 5 ⇒ external wins, reported at 3.
        let no_boost = select_payload_bid(vec![local(5, false), gossip(3, NEUTRAL_BOOST)]).unwrap();
        assert_eq!(outcome(no_boost), (LOCAL_BUILDER, true, gwei(5), "local"));

        let boosted = select_payload_bid(vec![local(5, false), gossip(3, 200)]).unwrap();
        assert_eq!(outcome(boosted), (GOSSIP_BUILDER, false, gwei(3), "gossip"));
    }

    #[test]
    fn always_prefer_class_beats_higher_local() {
        // Local block value dwarfs the bid, but the builder is in the always-prefer class.
        let win =
            select_payload_bid(vec![local(1000, false), direct(1, 0, u64::MAX, NO_CLAMP)]).unwrap();
        assert_eq!(outcome(win), (DIRECT_BUILDER, false, gwei(1), "direct"));
    }

    #[test]
    fn two_always_prefer_ranked_by_clamped_value() {
        // Both always-prefer; the higher (clamped) value wins, reported at its unclamped value.
        let win = select_payload_bid(vec![
            direct(1, 0, u64::MAX, NO_CLAMP),
            direct(2, 0, u64::MAX, NO_CLAMP),
        ])
        .unwrap();
        assert_eq!(outcome(win), (DIRECT_BUILDER, false, gwei(2), "direct"));
    }

    #[test]
    fn ranks_highest_across_sources() {
        // Gossip ranked 10 (neutral) vs direct value 4 boosted 300 ⇒ ranked 12 ⇒ direct wins.
        let win = select_payload_bid(vec![gossip(10, NEUTRAL_BOOST), direct(4, 0, 300, NO_CLAMP)])
            .unwrap();
        assert_eq!(outcome(win), (DIRECT_BUILDER, false, gwei(4), "direct"));
    }

    #[test]
    fn direct_winner_carries_builder_url() {
        let win = select_payload_bid(vec![direct(5, 0, NEUTRAL_BOOST, NO_CLAMP)]).unwrap();
        assert_eq!(win.source.builder_url(), Some(DIRECT_URL));
    }
}
