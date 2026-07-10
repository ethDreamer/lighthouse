use bls::PublicKeyBytes;
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use types::Slot;
use types::builder::{RequestAuthUrl, SignedRequestAuthV1};

#[derive(Hash, PartialEq, Eq)]
struct RequestAuthInnerKey {
    pubkey: PublicKeyBytes,
    builder_url: RequestAuthUrl,
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
        builder_url: &RequestAuthUrl,
    ) -> Option<SignedRequestAuthV1> {
        self.inner.read().entries.get(&slot).and_then(|entries| {
            let key = RequestAuthInnerKey {
                pubkey,
                builder_url: builder_url.clone(),
            };
            entries.get(&key).cloned()
        })
    }

    pub fn insert(
        &self,
        slot: Slot,
        pubkey: PublicKeyBytes,
        builder_url: RequestAuthUrl,
        signed_request_auth: SignedRequestAuthV1,
    ) {
        let key = RequestAuthInnerKey {
            pubkey,
            builder_url,
        };

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
