use crate::{BuilderEntryV1, MaxBuilderEntries};
use serde::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use ssz_types::VariableList;
use tree_hash_derive::TreeHash;

/// The resolved builder config the validator client sends on a block-production request, per
/// [beacon-APIs #630](https://github.com/ethereum/beacon-APIs/pull/630).
///
/// `builders` are the direct bid requests, each fully resolved. The top-level `min_bid` and
/// `builder_boost_factor` govern any bid that matches no entry — in practice, a bid received over
/// p2p.
///
/// SSZ container:
/// ```text
/// class BuilderConfigV1(Container):
///     builders: List[BuilderEntryV1, MAX_BUILDER_ENTRIES]
///     min_bid: Gwei
///     builder_boost_factor: uint64
/// ```
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, Encode, Decode, TreeHash)]
pub struct BuilderConfigV1 {
    /// The builders to request bids from directly. Empty means only p2p bids are considered.
    pub builders: VariableList<BuilderEntryV1, MaxBuilderEntries>,
    /// Minimum total payment (Gwei) accepted from a bid that matches no entry (a p2p bid).
    #[serde(with = "serde_utils::quoted_u64")]
    pub min_bid: u64,
    /// Percentage multiplier applied to a bid that matches no entry (a p2p bid).
    #[serde(with = "serde_utils::quoted_u64")]
    pub builder_boost_factor: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    ssz_and_tree_hash_tests!(BuilderConfigV1);

    #[test]
    fn json_shape() {
        let config = BuilderConfigV1 {
            builders: VariableList::default(),
            min_bid: 5,
            builder_boost_factor: 100,
        };
        let json = serde_json::to_value(&config).unwrap();
        let obj = json.as_object().unwrap();
        // `builders` is a JSON array; the Gwei/uint64 fields are quoted strings.
        assert!(obj["builders"].is_array());
        assert_eq!(obj["min_bid"], "5");
        assert_eq!(obj["builder_boost_factor"], "100");

        assert_eq!(
            serde_json::from_value::<BuilderConfigV1>(json).unwrap(),
            config
        );
    }
}
