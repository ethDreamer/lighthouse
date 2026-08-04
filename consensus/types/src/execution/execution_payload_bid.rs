use crate::kzg_ext::ProgressiveKzgCommitments;
use crate::{Address, EthSpec, ExecutionBlockHash, ForkName, Hash256, SignedRoot, Slot};
use context_deserialize::context_deserialize;
use educe::Educe;
use serde::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use std::marker::PhantomData;
use tree_hash_derive::TreeHash;

#[derive(Default, Debug, Clone, Serialize, Encode, Decode, Deserialize, TreeHash, Educe)]
#[cfg_attr(
    feature = "arbitrary",
    derive(arbitrary::Arbitrary),
    arbitrary(bound = "E: EthSpec")
)]
#[educe(PartialEq, Hash)]
#[serde(bound = "E: EthSpec")]
#[context_deserialize(ForkName)]
// https://github.com/ethereum/consensus-specs/blob/master/specs/gloas/beacon-chain.md#executionpayloadbid
#[tree_hash(
    struct_behaviour = "progressive_container",
    active_fields(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
)]
pub struct ExecutionPayloadBid<E: EthSpec> {
    pub parent_block_hash: ExecutionBlockHash,
    pub parent_block_root: Hash256,
    pub block_hash: ExecutionBlockHash,
    pub prev_randao: Hash256,
    #[serde(with = "serde_utils::address_hex")]
    pub fee_recipient: Address,
    #[serde(with = "serde_utils::quoted_u64")]
    pub gas_limit: u64,
    #[serde(with = "serde_utils::quoted_u64")]
    pub builder_index: u64,
    pub slot: Slot,
    #[serde(with = "serde_utils::quoted_u64")]
    pub value: u64,
    #[serde(with = "serde_utils::quoted_u64")]
    pub execution_payment: u64,
    // [Modified in Gloas:EIP7688]
    pub blob_kzg_commitments: ProgressiveKzgCommitments,
    pub execution_requests_root: Hash256,
    #[ssz(skip_serializing, skip_deserializing)]
    #[tree_hash(skip_hashing)]
    #[serde(skip)]
    #[cfg_attr(feature = "arbitrary", arbitrary(default))]
    pub _phantom: PhantomData<E>,
}

/// Boost-adjusted ranking key for an [`ExecutionPayloadBid`], used to choose between competing
/// external bids (and against the local execution payload).
///
/// [`AlwaysPrefer`](Self::AlwaysPrefer) encodes the reserved `builder_boost_factor == u64::MAX`
/// class — "prefer the external builder unless it's unviable", per beacon-APIs `produceBlockV3` — as
/// a *category* rather than a number, so no finite [`Boosted`](Self::Boosted) value can ever reach
/// it. The derived `Ord` gives exactly the intended total order:
/// - every `AlwaysPrefer` outranks every `Boosted` (variant declaration order),
/// - two `Boosted`s compare by their boosted value,
/// - two `AlwaysPrefer`s compare by their (clamped, unboosted) value.
///
/// Both variants are denominated in gwei; the inner values are only ever compared within their own
/// class, so the unit is never observed across variants.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum BoostedValue {
    /// A finite boosted value `(clamped_value * builder_boost_factor) / 100`, in gwei.
    Boosted(u64),
    /// The reserved always-prefer class (`builder_boost_factor == u64::MAX`), carrying the clamped
    /// (unboosted) value in gwei so two always-prefer bids can still be ranked against each other.
    AlwaysPrefer(u64),
}

impl<E: EthSpec> ExecutionPayloadBid<E> {
    /// The value the proposer realizes from this bid, in **gwei**: `value + execution_payment`, with
    /// no `max_execution_payment` clamp and no `builder_boost_factor` applied.
    ///
    /// This is the honest, preference-free figure to report for a winning bid (converted to wei for
    /// `Eth-Execution-Payload-Value`). The per-builder clamp and boost are proposer *ranking* policy
    /// and are deliberately excluded here — see [`boosted_value`](Self::boosted_value).
    pub fn proposer_value(&self) -> u64 {
        self.value.saturating_add(self.execution_payment)
    }

    /// The trusted value of this bid in **gwei**: `value + min(execution_payment,
    /// max_execution_payment)`.
    ///
    /// The `execution_payment` is clamped to the most the proposer trusts this builder to pay
    /// (`u64::MAX` = unlimited, `0` = untrusted). This is the value ranking is based on — used as an
    /// acceptance floor (`min_bid`) and, scaled by the boost, as [`boosted_value`](Self::boosted_value).
    /// Unlike [`proposer_value`](Self::proposer_value) it excludes untrusted payment, so it's safe to
    /// use in comparisons.
    pub fn clamped_value(&self, max_execution_payment: u64) -> u64 {
        self.value
            .saturating_add(self.execution_payment.min(max_execution_payment))
    }

    /// The boost-adjusted ranking key for this bid under the proposer's per-builder preferences.
    ///
    /// Takes the [`clamped_value`](Self::clamped_value) and:
    /// - `builder_boost_factor == u64::MAX` yields [`BoostedValue::AlwaysPrefer`] (always prefer this
    ///   builder over the local payload), carrying the clamped value for tie-breaks;
    /// - otherwise the boosted value `(clamped * builder_boost_factor) / 100` (a factor of `100` is
    ///   neutral; `0` prefers the local payload).
    ///
    /// Multiplication precedes division so bids below 100 gwei aren't floored to zero, and uses
    /// saturating arithmetic so a large `builder_boost_factor` can't overflow (per beacon-APIs
    /// guidance). All arithmetic is in gwei.
    pub fn boosted_value(
        &self,
        max_execution_payment: u64,
        builder_boost_factor: u64,
    ) -> BoostedValue {
        let clamped = self.clamped_value(max_execution_payment);
        if builder_boost_factor == u64::MAX {
            return BoostedValue::AlwaysPrefer(clamped);
        }
        BoostedValue::Boosted(clamped.saturating_mul(builder_boost_factor) / 100)
    }
}

impl<E: EthSpec> SignedRoot for ExecutionPayloadBid<E> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MainnetEthSpec;

    ssz_and_tree_hash_tests!(ExecutionPayloadBid<MainnetEthSpec>);

    fn bid(value: u64, execution_payment: u64) -> ExecutionPayloadBid<MainnetEthSpec> {
        ExecutionPayloadBid {
            value,
            execution_payment,
            ..Default::default()
        }
    }

    #[test]
    fn proposer_value_is_unclamped_sum_and_ignores_preferences() {
        // `value + execution_payment`, in gwei, regardless of clamp/boost (which it doesn't take).
        assert_eq!(bid(3, 7).proposer_value(), 10);
        assert_eq!(bid(1_000_000_000, 0).proposer_value(), 1_000_000_000);
        // Saturates rather than overflowing on absurd inputs.
        assert_eq!(bid(u64::MAX, u64::MAX).proposer_value(), u64::MAX);
    }

    #[test]
    fn clamped_value_bounds_execution_payment_by_cap() {
        // payment above the cap is trusted only up to the cap: 10 + min(100, 30) = 40.
        assert_eq!(bid(10, 100).clamped_value(30), 40);
        // cap = u64::MAX => no clamp: 10 + 100 = 110.
        assert_eq!(bid(10, 100).clamped_value(u64::MAX), 110);
        // cap = 0 => payment untrusted: just the value.
        assert_eq!(bid(10, 100).clamped_value(0), 10);
    }

    #[test]
    fn boosted_value_clamps_execution_payment() {
        // execution_payment 100 clamped to cap 30 => clamped = value(10) + 30 = 40; neutral boost.
        assert_eq!(
            bid(10, 100).boosted_value(30, 100),
            BoostedValue::Boosted(40)
        );
        // cap = u64::MAX => no clamp: clamped = 10 + 100 = 110.
        assert_eq!(
            bid(10, 100).boosted_value(u64::MAX, 100),
            BoostedValue::Boosted(110)
        );
        // cap = 0 => execution_payment untrusted: clamped = 10.
        assert_eq!(
            bid(10, 100).boosted_value(0, 100),
            BoostedValue::Boosted(10)
        );
    }

    #[test]
    fn boosted_value_applies_boost() {
        // clamped 200, boost 150 => 200 * 150 / 100 = 300.
        assert_eq!(
            bid(200, 0).boosted_value(0, 150),
            BoostedValue::Boosted(300)
        );
        // boost 0 => prefer local (ranks at 0).
        assert_eq!(bid(200, 0).boosted_value(0, 0), BoostedValue::Boosted(0));
    }

    #[test]
    fn boosted_value_multiplies_before_dividing() {
        // A sub-100-gwei bid must survive a neutral boost, not floor to zero.
        assert_eq!(bid(50, 0).boosted_value(0, 100), BoostedValue::Boosted(50));
        // 50 * 150 / 100 = 75, not (50 / 100) * 150 = 0.
        assert_eq!(bid(50, 0).boosted_value(0, 150), BoostedValue::Boosted(75));
    }

    #[test]
    fn always_prefer_sentinel_carries_clamped_value() {
        // u64::MAX boost => AlwaysPrefer, still carrying the *clamped* value (payment 100 capped to 30).
        assert_eq!(
            bid(10, 100).boosted_value(30, u64::MAX),
            BoostedValue::AlwaysPrefer(40)
        );
    }

    #[test]
    fn boosted_value_saturates_instead_of_overflowing() {
        // A huge (non-sentinel) boost saturates rather than wrapping/panicking.
        assert!(matches!(
            bid(u64::MAX, 0).boosted_value(0, u64::MAX - 1),
            BoostedValue::Boosted(_)
        ));
    }

    #[test]
    fn ord_always_prefer_beats_boosted_and_orders_within_class() {
        use BoostedValue::*;
        // Any always-prefer outranks any finite boosted, even the largest possible.
        assert!(AlwaysPrefer(1) > Boosted(u64::MAX));
        // Finite bids order by boosted value.
        assert!(Boosted(300) > Boosted(200));
        // Always-prefer bids order by their clamped value.
        assert!(AlwaysPrefer(300) > AlwaysPrefer(200));
    }
}
