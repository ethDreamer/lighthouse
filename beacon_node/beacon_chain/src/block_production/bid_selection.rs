//! Fork-agnostic ePBS payload-bid selection.
//!
//! This module is the reusable core of "given the candidate bids and the validator's policy, pick a
//! winner". It is **value-based**: the selection math never inspects payload contents, only each
//! candidate's comparable value. External bids expose two figures, both computed on
//! [`ExecutionPayloadBid`](types::ExecutionPayloadBid): `boosted_value` (clamp + `builder_boost_factor`,
//! the ranking key) and `proposer_value` (the unclamped `value + execution_payment`, reported for the
//! winner). The heavy `ExecutionPayloadData` rides along inside the local candidate as an opaque
//! attachment that's only materialized for the winner, so the selection signature stays free of
//! fork-specific payload types.
//!
//! External candidates come from two sources — the `execution_payload_bid` gossip topic and builders
//! contacted directly via `getExecutionPayloadBid` — both modelled as [`ExternalBidCandidate`]. Local
//! builds are the [`LocalBidCandidate`], handled distinctly because their value is the EL block value
//! (not a bid) and they carry the `shouldOverrideBuilder` short-circuit. A winning direct bid is
//! routed the signed block for envelope reveal; its builder URL is recovered from the
//! `DirectBidCache` at publication (by matching the block's committed bid), so it isn't tracked here.
//!
//! Bid `value`/`execution_payment` are gwei, so ranking is done in gwei; the local EL block value is
//! native wei, so the single local-vs-external comparison bridges the external figure to wei. The
//! winner's reported value is likewise wei (`Eth-Execution-Payload-Value`).
//!
//! Consumed by `gloas.rs` block production via `select_payload_bid`; it owns the fork-agnostic
//! selection types (`ExecutionPayloadData`, `WinningBid`, the candidate types) that block production
//! and future ePBS forks share.

use std::sync::Arc;
use tracing::debug;
use types::{
    BoostedValue, EthSpec, ExecutionPayloadGloas, ExecutionRequestsGloas,
    SignedExecutionPayloadBid, Slot, Uint256,
};

const GWEI_TO_WEI: u64 = 1_000_000_000;

/// Convert a gwei figure to wei. Saturating, though block values are nowhere near the ceiling.
fn gwei_to_wei(gwei: u64) -> Uint256 {
    Uint256::from(gwei).saturating_mul(Uint256::from(GWEI_TO_WEI))
}

/// Data needed to construct an `ExecutionPayloadEnvelope`, carried through selection as the local
/// candidate's opaque attachment (materialized only if local wins).
///
/// Fork-coupling seam: `payload`/`execution_requests` are concrete Gloas types. The selection logic
/// never inspects them — only [`LocalBidCandidate::block_value`] participates in ranking.
pub struct ExecutionPayloadData<E: EthSpec> {
    pub payload: ExecutionPayloadGloas<E>,
    pub execution_requests: ExecutionRequestsGloas<E>,
    pub builder_index: u64,
    pub slot: Slot,
    pub blobs_and_proofs: (types::BlobsList<E>, types::KzgProofs<E>),
}

/// An external (gossip or direct) payload-bid candidate — already consensus-validated and
/// policy-eligible — with its ranking/reporting figures pre-computed. Build via
/// [`ExternalBidCandidate::new`].
pub struct ExternalBidCandidate<E: EthSpec> {
    pub signed_bid: Arc<SignedExecutionPayloadBid<E>>,
    /// Boost-adjusted ranking key (gwei): the clamped value scaled by `builder_boost_factor`, or the
    /// reserved always-prefer class. This is what selection ranks on.
    pub boosted_value: BoostedValue,
    /// Unclamped `value + execution_payment` (gwei): the honest value reported if this bid wins.
    /// **Never used in ranking** — being unclamped, it must not influence selection (see
    /// [`pick_best_external`]).
    pub proposer_value: u64,
}

impl<E: EthSpec> ExternalBidCandidate<E> {
    /// Construct a candidate, computing its figures from the bid and this builder's resolved policy.
    ///
    /// - `max_execution_payment`: the largest `execution_payment` (gwei) the proposer trusts from this
    ///   builder; `u64::MAX` = no clamp (e.g. gossip bids, whose `execution_payment` is zero anyway),
    ///   `0` = untrusted. Affects only `boosted_value` (the block's committed bid is unchanged).
    ///   Over-cap `execution_payment` is clamped, not rejected (a deliberate departure from #630's
    ///   reject rule): the untrusted excess can't sway selection, but a bid that wins on its trusted
    ///   value still keeps the full payment it promised.
    /// - `builder_boost_factor`: `100` is neutral, `0` prefers local, `u64::MAX` is the always-prefer
    ///   class.
    pub fn new(
        signed_bid: Arc<SignedExecutionPayloadBid<E>>,
        max_execution_payment: u64,
        builder_boost_factor: u64,
    ) -> Self {
        let bid = &signed_bid.message;
        let boosted_value = bid.boosted_value(max_execution_payment, builder_boost_factor);
        let proposer_value = bid.proposer_value();
        Self {
            signed_bid,
            boosted_value,
            proposer_value,
        }
    }

    fn into_winning_bid(self) -> WinningBid<E> {
        WinningBid {
            bid: (*self.signed_bid).clone(),
            payload_data: None,
            payload_value: gwei_to_wei(self.proposer_value),
        }
    }
}

/// The locally-built payload candidate. Its comparable value is the EL block value, not the bid
/// (a self-build bid's `value`/`execution_payment` are zero).
pub struct LocalBidCandidate<E: EthSpec> {
    pub signed_bid: SignedExecutionPayloadBid<E>,
    pub payload_data: ExecutionPayloadData<E>,
    /// EL block value (wei) — the local candidate's proposer revenue, ranking value, and reported
    /// value. Local is never boosted, so it competes directly against an external's `boosted_value`.
    pub block_value: Uint256,
    /// EL `engine_getPayload` `shouldOverrideBuilder` signal; when set, local is chosen regardless
    /// of any external bid.
    pub should_override_builder: bool,
}

impl<E: EthSpec> LocalBidCandidate<E> {
    fn into_winning_bid(self) -> WinningBid<E> {
        WinningBid {
            bid: self.signed_bid,
            payload_data: Some(self.payload_data),
            payload_value: self.block_value,
        }
    }
}

/// The outcome of bid selection.
pub struct WinningBid<E: EthSpec> {
    pub bid: SignedExecutionPayloadBid<E>,
    /// `Some` when local wins (self-build); `None` when an external bid wins (the builder reveals
    /// the envelope).
    pub payload_data: Option<ExecutionPayloadData<E>>,
    /// Wei value of the winner: the proposer's real (unboosted, unclamped) revenue.
    pub payload_value: Uint256,
}

/// Select the winning payload bid from the local build and the external candidates.
///
/// Rules:
/// 1. If the EL signaled `shouldOverrideBuilder`, local wins.
/// 2. If the best external candidate is in the always-prefer class (`boost == u64::MAX`), it wins.
/// 3. Otherwise local wins iff its block value `>=` the best external's boosted value (ties to
///    local); the external wins when strictly greater.
///
/// With only one side viable, that side wins. Returns `None` only when nothing is viable (no local
/// build and no external candidates) — the caller treats that as block-production failure.
pub fn select_payload_bid<E: EthSpec>(
    local: Option<LocalBidCandidate<E>>,
    externals: Vec<ExternalBidCandidate<E>>,
) -> Option<WinningBid<E>> {
    let best_external = pick_best_external(externals);

    match (local, best_external) {
        (None, None) => None,
        (Some(local), None) => Some(local.into_winning_bid()),
        (None, Some(external)) => Some(external.into_winning_bid()),
        (Some(local), Some(external)) => {
            let slot = local.signed_bid.message.slot;

            if local.should_override_builder {
                debug!(%slot, "Using local payload: EL signaled shouldOverrideBuilder");
                return Some(local.into_winning_bid());
            }

            match external.boosted_value {
                BoostedValue::AlwaysPrefer(_) => {
                    debug!(
                        %slot,
                        builder_index = external.signed_bid.message.builder_index,
                        "Using external bid: builder is in the always-prefer class"
                    );
                    Some(external.into_winning_bid())
                }
                // Local is unboosted, so compare its (wei) block value against the external's boosted
                // value bridged to wei. Ties go to local (avoids unnecessary builder reliance).
                BoostedValue::Boosted(boosted_gwei) => {
                    let boosted_wei = gwei_to_wei(boosted_gwei);
                    if local.block_value >= boosted_wei {
                        debug!(
                            %slot,
                            local_block_value = %local.block_value,
                            %boosted_wei,
                            "Using local payload: at least as profitable as the best external bid"
                        );
                        Some(local.into_winning_bid())
                    } else {
                        debug!(
                            %slot,
                            local_block_value = %local.block_value,
                            %boosted_wei,
                            builder_index = external.signed_bid.message.builder_index,
                            "Using external bid: more profitable than local"
                        );
                        Some(external.into_winning_bid())
                    }
                }
            }
        }
    }
}

/// Pick the single best external candidate: the highest [`ExternalBidCandidate::boosted_value`], the
/// earlier candidate winning ties. The always-prefer class needs no special handling here —
/// [`BoostedValue`]'s ordering already sorts it above every finite bid, and orders the always-prefer
/// class internally by its clamped value.
///
/// Ranking is on `boosted_value` **only**. `proposer_value` is deliberately excluded: it's unclamped,
/// so ranking on it would let a builder inflate `execution_payment` past the trust cap to influence
/// selection — the exact manipulation the clamp prevents.
fn pick_best_external<E: EthSpec>(
    externals: Vec<ExternalBidCandidate<E>>,
) -> Option<ExternalBidCandidate<E>> {
    // `reduce` keeps `best` unless a candidate is *strictly* greater, so the earliest of any tied
    // maxima wins.
    externals.into_iter().reduce(|best, candidate| {
        if candidate.boosted_value > best.boosted_value {
            candidate
        } else {
            best
        }
    })
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

    fn gossip(value_gwei: u64, boost: u64) -> ExternalBidCandidate<TestSpec> {
        // Gossip bids carry no execution_payment, so the clamp is irrelevant (no cap).
        ExternalBidCandidate::new(signed_bid(GOSSIP_BUILDER, value_gwei, 0), NO_CLAMP, boost)
    }

    fn direct(
        value_gwei: u64,
        payment_gwei: u64,
        boost: u64,
        max_payment: u64,
    ) -> ExternalBidCandidate<TestSpec> {
        ExternalBidCandidate::new(
            signed_bid(DIRECT_BUILDER, value_gwei, payment_gwei),
            max_payment,
            boost,
        )
    }

    fn local(block_value_gwei: u64, should_override_builder: bool) -> LocalBidCandidate<TestSpec> {
        LocalBidCandidate {
            signed_bid: SignedExecutionPayloadBid {
                message: ExecutionPayloadBid {
                    builder_index: LOCAL_BUILDER,
                    ..Default::default()
                },
                signature: Signature::empty(),
            },
            payload_data: ExecutionPayloadData {
                payload: ExecutionPayloadGloas::default(),
                execution_requests: ExecutionRequestsGloas::default(),
                builder_index: LOCAL_BUILDER,
                slot: Slot::new(0),
                blobs_and_proofs: (VariableList::empty(), VariableList::empty()),
            },
            block_value: gwei(block_value_gwei),
            should_override_builder,
        }
    }

    /// `(winning_builder_index, has_local_payload, payload_value_wei, source_label)`. The source is
    /// derived from the winner's builder index (the helpers use a distinct index per source).
    fn outcome(win: WinningBid<TestSpec>) -> (u64, bool, Uint256, &'static str) {
        let source = match win.bid.message.builder_index {
            LOCAL_BUILDER => "local",
            GOSSIP_BUILDER => "gossip",
            DIRECT_BUILDER => "direct",
            _ => "unknown",
        };
        (
            win.bid.message.builder_index,
            win.payload_data.is_some(),
            win.payload_value,
            source,
        )
    }

    #[test]
    fn local_only_wins() {
        let win = select_payload_bid(Some(local(7, false)), vec![]).unwrap();
        assert_eq!(outcome(win), (LOCAL_BUILDER, true, gwei(7), "local"));
    }

    #[test]
    fn external_only_wins_when_no_local() {
        let win = select_payload_bid(None, vec![gossip(5, NEUTRAL_BOOST)]).unwrap();
        assert_eq!(outcome(win), (GOSSIP_BUILDER, false, gwei(5), "gossip"));
    }

    #[test]
    fn nothing_viable_is_none() {
        assert!(select_payload_bid::<TestSpec>(None, vec![]).is_none());
    }

    #[test]
    fn el_override_beats_any_external() {
        let win = select_payload_bid(
            Some(local(1, true)),
            vec![direct(1000, 1000, u64::MAX, NO_CLAMP)],
        )
        .unwrap();
        assert_eq!(outcome(win), (LOCAL_BUILDER, true, gwei(1), "local"));
    }

    #[test]
    fn local_wins_value_tie() {
        // Neutral boost, external boosted == local block value ⇒ local wins ties.
        let win =
            select_payload_bid(Some(local(5, false)), vec![gossip(5, NEUTRAL_BOOST)]).unwrap();
        assert_eq!(outcome(win), (LOCAL_BUILDER, true, gwei(5), "local"));
    }

    #[test]
    fn external_wins_when_strictly_higher() {
        let win =
            select_payload_bid(Some(local(4, false)), vec![gossip(5, NEUTRAL_BOOST)]).unwrap();
        assert_eq!(outcome(win), (GOSSIP_BUILDER, false, gwei(5), "gossip"));
    }

    #[test]
    fn direct_bid_counts_execution_payment() {
        // Direct bid: value 2 + payment 4 = 6 boosted (neutral) ⇒ beats local 5, reported at 6.
        let win = select_payload_bid(
            Some(local(5, false)),
            vec![direct(2, 4, NEUTRAL_BOOST, NO_CLAMP)],
        )
        .unwrap();
        assert_eq!(outcome(win), (DIRECT_BUILDER, false, gwei(6), "direct"));
    }

    #[test]
    fn max_execution_payment_clamps_ranking_but_not_reported_value() {
        // Unclamped: value 1 + payment 10 = 11 boosted (neutral) ⇒ beats local 5.
        let unclamped = select_payload_bid(
            Some(local(5, false)),
            vec![direct(1, 10, NEUTRAL_BOOST, NO_CLAMP)],
        )
        .unwrap();
        assert_eq!(
            outcome(unclamped),
            (DIRECT_BUILDER, false, gwei(11), "direct")
        );

        // Clamp payment to 3: ranked value = 1 + min(10, 3) = 4 < local 5 ⇒ local wins.
        let clamped =
            select_payload_bid(Some(local(5, false)), vec![direct(1, 10, NEUTRAL_BOOST, 3)])
                .unwrap();
        assert_eq!(outcome(clamped), (LOCAL_BUILDER, true, gwei(5), "local"));

        // Clamp still lets it win over local 3 (ranked 4 > 3) — but the *reported* value is the
        // unclamped proposer value 11, since the clamp is a ranking-only trust bound.
        let clamped_win =
            select_payload_bid(Some(local(3, false)), vec![direct(1, 10, NEUTRAL_BOOST, 3)])
                .unwrap();
        assert_eq!(
            outcome(clamped_win),
            (DIRECT_BUILDER, false, gwei(11), "direct")
        );
    }

    #[test]
    fn boost_amplifies_external() {
        // Boosted 3 < local 5 ⇒ local; boost 200 ⇒ boosted 6 > 5 ⇒ external wins, reported at 3.
        let no_boost =
            select_payload_bid(Some(local(5, false)), vec![gossip(3, NEUTRAL_BOOST)]).unwrap();
        assert_eq!(outcome(no_boost), (LOCAL_BUILDER, true, gwei(5), "local"));

        let boosted = select_payload_bid(Some(local(5, false)), vec![gossip(3, 200)]).unwrap();
        assert_eq!(outcome(boosted), (GOSSIP_BUILDER, false, gwei(3), "gossip"));
    }

    #[test]
    fn always_prefer_class_beats_higher_local() {
        // Local block value dwarfs the bid, but the builder is in the always-prefer class.
        let win = select_payload_bid(
            Some(local(1000, false)),
            vec![direct(1, 0, u64::MAX, NO_CLAMP)],
        )
        .unwrap();
        assert_eq!(outcome(win), (DIRECT_BUILDER, false, gwei(1), "direct"));
    }

    #[test]
    fn two_always_prefer_ranked_by_clamped_value() {
        // Both always-prefer; the higher (clamped) value wins, reported at its unclamped value.
        let win = select_payload_bid(
            None,
            vec![
                direct(1, 0, u64::MAX, NO_CLAMP),
                direct(2, 0, u64::MAX, NO_CLAMP),
            ],
        )
        .unwrap();
        assert_eq!(outcome(win), (DIRECT_BUILDER, false, gwei(2), "direct"));
    }

    #[test]
    fn ranks_highest_boosted_across_sources() {
        // Gossip boosted 10 (neutral) vs direct value 4 boosted 300 ⇒ boosted 12 ⇒ direct wins.
        let win = select_payload_bid(
            None,
            vec![gossip(10, NEUTRAL_BOOST), direct(4, 0, 300, NO_CLAMP)],
        )
        .unwrap();
        assert_eq!(outcome(win), (DIRECT_BUILDER, false, gwei(4), "direct"));
    }
}
