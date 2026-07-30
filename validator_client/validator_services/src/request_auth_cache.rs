use bls::PublicKeyBytes;
use builder_types::{RequestAuthData, SignedRequestAuthV1};
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use types::Slot;

/// Caches signed `RequestAuthV1` objects so a given proposer/auth-data/slot combination is only
/// signed once.
///
/// The signed authorization is a pure function of the proposer pubkey, the opaque `auth_data`, and
/// the proposal `slot`, so those form the cache key. The builder URL is deliberately *not* part of
/// the key: two builders configured with the same `auth_data` share one signature.
#[derive(Hash, PartialEq, Eq)]
struct RequestAuthInnerKey {
    pubkey: PublicKeyBytes,
    auth_data: RequestAuthData,
}

#[derive(Default)]
struct Inner {
    entries: BTreeMap<Slot, HashMap<RequestAuthInnerKey, SignedRequestAuthV1>>,
}

#[derive(Clone)]
pub struct RequestAuthCache {
    inner: Arc<RwLock<Inner>>,
}

impl Default for RequestAuthCache {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner::default())),
        }
    }
}

impl RequestAuthCache {
    pub fn get(
        &self,
        slot: Slot,
        pubkey: PublicKeyBytes,
        auth_data: &RequestAuthData,
    ) -> Option<SignedRequestAuthV1> {
        self.inner.read().entries.get(&slot).and_then(|entries| {
            let key = RequestAuthInnerKey {
                pubkey,
                auth_data: auth_data.clone(),
            };
            entries.get(&key).cloned()
        })
    }

    pub fn insert(
        &self,
        slot: Slot,
        pubkey: PublicKeyBytes,
        auth_data: RequestAuthData,
        signed_request_auth: SignedRequestAuthV1,
    ) {
        let key = RequestAuthInnerKey { pubkey, auth_data };

        self.inner
            .write()
            .entries
            .entry(slot)
            .or_default()
            .insert(key, signed_request_auth);
    }

    pub fn prune(&self, current_slot: Slot) {
        let mut guard = self.inner.write();
        guard.entries = guard.entries.split_off(&current_slot);
    }
}
