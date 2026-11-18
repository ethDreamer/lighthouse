use crate::BeaconChainTypes;
use crate::payload_envelope_verification::gossip_verified_envelope::GossipVerifiedEnvelope;
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashSet};
use types::{Hash256, Slot};

type SeenEnvelopeMap = BTreeMap<Slot, HashSet<Hash256>>;

pub struct ObservedPayloadEnvelopes {
    seen_envelopes: RwLock<SeenEnvelopeMap>,
}

impl Default for ObservedPayloadEnvelopes {
    fn default() -> Self {
        Self {
            seen_envelopes: RwLock::new(BTreeMap::new()),
        }
    }
}

impl ObservedPayloadEnvelopes {
    /// Observe the verified payload envelope for its `(slot, block_root)` pair
    ///
    /// Returns `true` if the envelope was newly observed, `false` if it had already been seen
    pub fn observe_envelope<T: BeaconChainTypes>(
        &self,
        envelope: &GossipVerifiedEnvelope<T>,
    ) -> bool {
        self.seen_envelopes
            .write()
            .entry(envelope.block.slot())
            .or_default()
            .insert(envelope.envelope.beacon_block_root)
    }

    /// Checks if a payload envelope at `block_root` was already seen for `slot`
    pub fn envelope_has_been_observed(&self, slot: Slot, block_root: Hash256) -> bool {
        self.seen_envelopes
            .read()
            .get(&slot)
            .is_some_and(|seen| seen.contains(&block_root))
    }

    /// Prune all entries prior to `finalized_slot`.
    pub fn prune(&self, finalized_slot: Slot) {
        self.seen_envelopes
            .write()
            .retain(|&slot, _| slot >= finalized_slot);
    }
}

#[cfg(test)]
mod tests {

    use super::ObservedPayloadEnvelopes;
    use crate::payload_envelope_verification::gossip_verified_envelope::GossipVerifiedEnvelope;
    use crate::test_utils::EphemeralHarnessType;
    use bls::Signature;
    use std::sync::Arc;
    use types::{
        BeaconBlock, EthSpec, ExecutionPayloadEnvelope, ExecutionPayloadGloas,
        ExecutionRequestsGloas, Hash256, SignedBeaconBlock, Slot, Spec,
    };

    type E = Spec;

    fn make_verified_envelope(
        slot: Slot,
        block_root: Hash256,
    ) -> GossipVerifiedEnvelope<EphemeralHarnessType<E>> {
        let envelope = ExecutionPayloadEnvelope {
            payload: ExecutionPayloadGloas {
                slot_number: slot,
                ..ExecutionPayloadGloas::default()
            },
            execution_requests: ExecutionRequestsGloas::default(),
            beacon_block_root: block_root,
            parent_beacon_block_root: Hash256::ZERO,
        };
        let mut block = BeaconBlock::empty(&E::default_spec());
        *block.slot_mut() = slot;

        GossipVerifiedEnvelope {
            envelope: Arc::new(envelope),
            block: Arc::new(SignedBeaconBlock::from_block(block, Signature::empty())),
        }
    }

    #[test]
    fn marks_envelope_as_seen() {
        let cache = ObservedPayloadEnvelopes::default();
        let slot = Slot::new(1);
        let block_root = Hash256::random();
        let envelope = make_verified_envelope(slot, block_root);

        assert!(cache.observe_envelope(&envelope));

        assert!(cache.envelope_has_been_observed(slot, block_root));
        // Marking the same envelope again reports it as already seen.
        assert!(!cache.observe_envelope(&envelope));
    }

    #[test]
    fn prune_removes_entries_prior_to_finalized_slot() {
        let cache = ObservedPayloadEnvelopes::default();
        let block_root = Hash256::random();
        let total_slots = 10;
        let finalized_slot = 8;

        for slot in 0..=total_slots {
            let envelope = make_verified_envelope(Slot::new(slot), block_root);
            cache.observe_envelope(&envelope);
        }

        cache.prune(Slot::new(finalized_slot));

        // Slots prior to the finalized slot are pruned.
        for slot in 0..finalized_slot {
            assert!(!cache.envelope_has_been_observed(Slot::new(slot), block_root));
        }

        // The finalized slot and later slots are retained.
        for slot in finalized_slot..=total_slots {
            assert!(cache.envelope_has_been_observed(Slot::new(slot), block_root));
        }
    }
}
