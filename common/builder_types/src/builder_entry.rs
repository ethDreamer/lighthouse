use crate::{BuilderUrl, SignedRequestAuthV1};
use bls::PublicKeyBytes;
use serde::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use tree_hash_derive::TreeHash;

/// A per-builder input the validator client supplies on a block-production request, per
/// [beacon-APIs #630](https://github.com/ethereum/beacon-APIs/pull/630).
///
/// At least one of `url` and `builder_pubkey` must be present:
/// - An entry with a `url` is a **bid request** (and must also carry an `auth`): the beacon node
///   calls `getExecutionPayloadBid` at that URL. One request is made per entry, so several entries
///   MAY share a `url` with different `auth`.
/// - An entry with no `url` supplies **p2p policy** for the builder identified by `builder_pubkey`:
///   no request is made, but its `min_bid`/`builder_boost_factor` apply to that builder's gossiped
///   bids.
///
/// SSZ cannot express absence, so each optional field carries a sentinel "unset" value: a
/// zero-length `url` ([`BuilderUrl::empty`]), an all-zero `builder_pubkey` (not a valid BLS key),
/// and an unset `auth` ([`SignedRequestAuthV1::unset`]). Prefer the [`url`](Self::url) and
/// [`builder_pubkey`](Self::builder_pubkey) accessors, which resolve the sentinels to `Option`s,
/// over inspecting the fields directly.
///
/// Field order matches the SSZ `BuilderEntryV1` container:
/// ```text
/// class BuilderEntryV1(Container):
///     url: ByteList[MAX_BUILDER_URL_SIZE]
///     auth: SignedRequestAuthV1
///     builder_pubkey: BLSPubkey
///     max_execution_payment: Gwei
///     min_bid: Gwei
///     builder_boost_factor: uint64
/// ```
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, Encode, Decode, TreeHash)]
pub struct BuilderEntryV1 {
    /// Where this entry's bid request is sent. Unset (zero-length) makes it a p2p-policy entry.
    #[serde(
        default = "BuilderUrl::empty",
        skip_serializing_if = "BuilderUrl::is_empty"
    )]
    pub url: BuilderUrl,
    /// Authenticates this entry's bid request. Required when `url` is present; unset otherwise.
    #[serde(
        default = "SignedRequestAuthV1::unset",
        skip_serializing_if = "SignedRequestAuthV1::is_unset"
    )]
    pub auth: SignedRequestAuthV1,
    /// On a `url` entry, the returned bid must be signed by this key; on a p2p entry it identifies
    /// the builder this policy applies to. Unset is all-zero.
    #[serde(
        default = "PublicKeyBytes::empty",
        skip_serializing_if = "pubkey_is_unset"
    )]
    pub builder_pubkey: PublicKeyBytes,
    /// Maximum trusted execution-layer payment (Gwei) accepted from this builder.
    #[serde(with = "serde_utils::quoted_u64")]
    pub max_execution_payment: u64,
    /// Minimum total payment (Gwei) for a bid from this builder to be accepted.
    #[serde(with = "serde_utils::quoted_u64")]
    pub min_bid: u64,
    /// Percentage multiplier applied to this builder's bid when comparing against the local payload.
    #[serde(with = "serde_utils::quoted_u64")]
    pub builder_boost_factor: u64,
}

impl BuilderEntryV1 {
    /// The builder URL for a bid-request entry, or `None` when unset (a p2p-policy entry).
    pub fn url(&self) -> Option<&BuilderUrl> {
        (!self.url.is_empty()).then_some(&self.url)
    }

    /// The builder pubkey this entry constrains — used to validate a `url` entry's bid response, or
    /// to identify the target of a p2p-policy entry — or `None` when unset.
    pub fn builder_pubkey(&self) -> Option<PublicKeyBytes> {
        (!pubkey_is_unset(&self.builder_pubkey)).then_some(self.builder_pubkey)
    }
}

/// Whether a `builder_pubkey` is unset (all-zero). A free function, used for the field's
/// `skip_serializing_if`, because `PublicKeyBytes` is a foreign type without an `is_empty` method
/// (unlike `BuilderUrl`/`SignedRequestAuthV1`, which carry their own predicates).
fn pubkey_is_unset(pubkey: &PublicKeyBytes) -> bool {
    *pubkey == PublicKeyBytes::empty()
}

#[cfg(test)]
mod tests {
    use super::*;

    ssz_and_tree_hash_tests!(BuilderEntryV1);

    fn entry() -> BuilderEntryV1 {
        BuilderEntryV1 {
            url: BuilderUrl::empty(),
            auth: SignedRequestAuthV1::unset(),
            builder_pubkey: PublicKeyBytes::empty(),
            max_execution_payment: 1,
            min_bid: 2,
            builder_boost_factor: 100,
        }
    }

    #[test]
    fn json_omits_unset_optional_fields() {
        // A p2p-style entry with every optional field unset serializes to only the required fields.
        let entry = entry();
        let json = serde_json::to_value(&entry).unwrap();
        let obj = json.as_object().unwrap();
        assert!(!obj.contains_key("url"));
        assert!(!obj.contains_key("auth"));
        assert!(!obj.contains_key("builder_pubkey"));
        assert!(obj.contains_key("max_execution_payment"));

        // Omitted fields deserialize back to their unset sentinels.
        assert_eq!(
            serde_json::from_value::<BuilderEntryV1>(json).unwrap(),
            entry
        );
    }

    #[test]
    fn json_includes_set_optional_fields() {
        let mut entry = entry();
        entry.url = "http://builder.example.com".parse().unwrap();
        let json = serde_json::to_value(&entry).unwrap();
        let obj = json.as_object().unwrap();
        // A set `url` is serialized; the still-unset `auth`/`builder_pubkey` are omitted.
        assert!(obj.contains_key("url"));
        assert!(!obj.contains_key("auth"));
        assert!(!obj.contains_key("builder_pubkey"));

        assert_eq!(
            serde_json::from_value::<BuilderEntryV1>(json).unwrap(),
            entry
        );
    }
}
