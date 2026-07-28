use crate::builder::{BuilderUrl, SignedRequestAuthV1};
use serde::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use tree_hash_derive::TreeHash;

/// A per-builder preference a validator asks the beacon node to submit ahead of the bid request,
/// one entry per `submitBuilderPreferences` builder-API call the beacon node will make.
///
/// This is the beacon-API (validator -> beacon node) type from beacon-APIs #630. Unlike the
/// block-production `BuilderEntry`, it carries only what a builder is allowed to see: the routing
/// `url`, the forwarded `auth`, and the `max_execution_payment` cap. The proposer's private
/// bid-filtering knobs (`min_bid`, `builder_boost_factor`) are never sent to a builder.
///
/// The beacon node contacts the builder at `url`, forwards `auth` byte-for-byte unchanged, and
/// submits `max_execution_payment`.
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, Encode, Decode, TreeHash)]
pub struct BuilderPreferenceEntryV1 {
    /// The URL the beacon node submits these preferences to. Unsigned routing metadata.
    pub url: BuilderUrl,
    /// Authenticates the submission to the builder; forwarded byte-for-byte unchanged.
    pub auth: SignedRequestAuthV1,
    /// Maximum trusted execution-layer payment (Gwei) the proposer will accept from this builder.
    #[serde(with = "serde_utils::quoted_u64")]
    pub max_execution_payment: u64,
}

impl BuilderPreferenceEntryV1 {
    pub fn new(url: BuilderUrl, auth: SignedRequestAuthV1, max_execution_payment: u64) -> Self {
        Self {
            url,
            auth,
            max_execution_payment,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    ssz_and_tree_hash_tests!(BuilderPreferenceEntryV1);
}
