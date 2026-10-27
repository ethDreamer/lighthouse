use context_deserialize::context_deserialize;
use educe::Educe;
use serde::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use tree_hash_derive::TreeHash;

use super::{AttestationData, HeightCheckpoint, IndexedAttestation};
use crate::{core::EthSpec, fork::ForkName};

/// Attester slashing evidence under the finality gadget rules [Modified in EIP8453]:
/// double target votes and votes conflicting with a finality pair. Pre-fork (Casper FFG)
/// evidence is carried separately in `legacy_attester_slashings` using the unchanged
/// `AttesterSlashingGloas` type.
#[derive(TreeHash, Debug, Clone, Encode, Decode, Serialize, Deserialize, Educe)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(bound = "E: EthSpec", deny_unknown_fields)]
#[cfg_attr(feature = "arbitrary", arbitrary(bound = "E: EthSpec"))]
#[educe(PartialEq, Hash)]
#[context_deserialize(ForkName)]
pub struct AttesterSlashing<E: EthSpec> {
    pub attestation_1: IndexedAttestation<E>,
    pub attestation_2: IndexedAttestation<E>,
}

/// `is_double_target_vote` from the spec.
pub fn is_double_target_vote(data_1: &AttestationData, data_2: &AttestationData) -> bool {
    !data_1.timeout
        && !data_2.timeout
        && data_1.target.height == data_2.target.height
        && data_1.target.root != data_2.target.root
}

/// `is_conflicting_with_finality_pair` from the spec.
pub fn is_conflicting_with_finality_pair(
    data: &AttestationData,
    finality: &HeightCheckpoint,
) -> bool {
    if finality.is_empty() {
        return false;
    }
    if data.target.height != finality.height {
        return false;
    }
    data.timeout || data.target.root != finality.root
}

/// `is_slashable_attestation_data` from the spec, rewritten for the finality gadget.
pub fn is_slashable_attestation_data(data_1: &AttestationData, data_2: &AttestationData) -> bool {
    is_double_target_vote(data_1, data_2)
        || is_conflicting_with_finality_pair(data_1, &data_2.finality)
        || is_conflicting_with_finality_pair(data_2, &data_1.finality)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Hash256, Spec};

    ssz_and_tree_hash_tests!(AttesterSlashing<Spec>);

    #[test]
    fn slashing_predicates() {
        let target = |height, byte| HeightCheckpoint {
            height,
            root: Hash256::repeat_byte(byte),
        };
        let vote = |t: HeightCheckpoint, timeout: bool, f: HeightCheckpoint| AttestationData {
            target: t,
            timeout,
            finality: f,
        };

        // Double target vote: same height, different roots, no timeouts.
        let a = vote(target(5, 1), false, HeightCheckpoint::empty());
        let b = vote(target(5, 2), false, HeightCheckpoint::empty());
        assert!(is_slashable_attestation_data(&a, &b));

        // Same vote twice is not slashable.
        assert!(!is_slashable_attestation_data(&a, &a.clone()));

        // Timeout at the height of another vote's finality pair conflicts with it.
        let c = vote(target(7, 3), false, target(5, 1));
        let d = vote(target(5, 1), true, HeightCheckpoint::empty());
        assert!(is_slashable_attestation_data(&c, &d));

        // A target vote matching the finality pair's root does not conflict.
        let e = vote(target(5, 1), false, HeightCheckpoint::empty());
        assert!(!is_slashable_attestation_data(&c, &e));
    }
}
