use context_deserialize::context_deserialize;
use serde::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use tree_hash_derive::TreeHash;

use super::HeightCheckpoint;
use crate::{
    core::{ChainSpec, Domain, Epoch, Hash256, SignedRoot},
    fork::{Fork, ForkName},
};

/// The data of a finality gadget attestation [New in EIP8453].
///
/// The height pair is `(target, timeout)`: a target vote when `timeout` is false and a timeout
/// when it is true. The finality pair is `finality`, with `HeightCheckpoint::empty()` standing
/// for an absent pair.
///
/// Note the absence of `slot`, `index`, `beacon_block_root` and `source`: head votes have taken
/// over the LMD GHOST vote, and the finality gadget replaces Casper FFG. This type deliberately
/// does NOT implement `SlotData` so that it cannot be routed into slot-keyed attestation
/// infrastructure.
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    Hash,
    Serialize,
    Deserialize,
    Encode,
    Decode,
    TreeHash,
)]
#[context_deserialize(ForkName)]
pub struct AttestationData {
    pub target: HeightCheckpoint,
    pub timeout: bool,
    pub finality: HeightCheckpoint,
}

impl SignedRoot for AttestationData {}

impl AttestationData {
    /// `is_well_formed_attestation_data` from the spec.
    pub fn is_well_formed(&self) -> bool {
        if self.target.height == 0 || self.target.root.is_zero() {
            return false;
        }
        if self.finality.is_empty() {
            return true;
        }
        !self.finality.root.is_zero() && self.finality.height < self.target.height
    }

    /// Compute the signing root for this data.
    ///
    /// The domain epoch is the attestation's (unsigned) `epoch` field, NOT anything contained in
    /// the signed data. This helper is the only supported way to compute it.
    pub fn signing_root_at_epoch(
        &self,
        epoch: Epoch,
        fork: &Fork,
        genesis_validators_root: Hash256,
        spec: &ChainSpec,
    ) -> Hash256 {
        let domain = spec.get_domain(
            epoch,
            Domain::FinalityAttester,
            fork,
            genesis_validators_root,
        );
        self.signing_root(domain)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    ssz_and_tree_hash_tests!(AttestationData);

    #[test]
    fn well_formedness() {
        let target = |height, byte| HeightCheckpoint {
            height,
            root: Hash256::repeat_byte(byte),
        };
        let vote = |t: HeightCheckpoint, timeout: bool, f: HeightCheckpoint| AttestationData {
            target: t,
            timeout,
            finality: f,
        };

        assert!(vote(target(5, 1), false, HeightCheckpoint::empty()).is_well_formed());
        assert!(!vote(target(0, 1), false, HeightCheckpoint::empty()).is_well_formed());
        assert!(!vote(target(5, 0), false, HeightCheckpoint::empty()).is_well_formed());
        // Finality pair must be below the target height.
        assert!(!vote(target(5, 1), false, target(5, 2)).is_well_formed());
        assert!(vote(target(5, 1), false, target(4, 2)).is_well_formed());
    }
}
