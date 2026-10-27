//! EIP-8453 (decoupled consensus) attestation and head vote containers.
//!
//! These types are a complete departure from the pre-EIP-8453 attestation family: the
//! finality-gadget `AttestationData` has no slot, no LMD vote and no source/target epochs, so
//! there is no consumer that could meaningfully handle both the old and new containers. They are
//! therefore deliberately standalone types rather than new variants of the `Attestation`
//! superstruct, keeping the (still churning) wire format isolated in this one module.
//!
//! Spec: `specs/_features/eip8453/beacon-chain.md` on the `decoupled-consensus` branch.

use bls::{AggregateSignature, Signature};
use context_deserialize::context_deserialize;
use educe::Educe;
use serde::{Deserialize, Serialize};
use ssz::{BitVector, ProgressiveBitList};
use ssz_derive::{Decode, Encode};
use ssz_types::ProgressiveVariableList;
use tree_hash::TreeHash;
use tree_hash_derive::TreeHash;

use crate::{
    core::{ChainSpec, Domain, Epoch, EthSpec, Hash256, SignedRoot, Slot},
    fork::{Fork, ForkName},
};

/// A checkpoint of the finality gadget: a height and the root of its target block.
///
/// This is *not* the Casper FFG `Checkpoint`; heights are not epochs.
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
pub struct HeightCheckpoint {
    #[serde(with = "serde_utils::quoted_u64")]
    pub height: u64,
    pub root: Hash256,
}

impl HeightCheckpoint {
    /// The empty checkpoint stands for an absent finality pair.
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }
}

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
pub struct AttestationDataEip8453 {
    pub target: HeightCheckpoint,
    pub timeout: bool,
    pub finality: HeightCheckpoint,
}

impl SignedRoot for AttestationDataEip8453 {}

impl AttestationDataEip8453 {
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
        let domain = spec.get_domain(epoch, Domain::BeaconAttester, fork, genesis_validators_root);
        self.signing_root(domain)
    }
}

/// An aggregated finality gadget attestation [Modified in EIP8453].
///
/// `epoch` is NOT covered by the signature; it selects the attestation committees that
/// `committee_bits` and `aggregation_bits` refer to, and the signature domain. Deduplication and
/// identity must therefore use [`Self::observation_key`] (data root + epoch + signers), never the
/// container's own tree hash root.
#[derive(TreeHash, Debug, Clone, Encode, Decode, Serialize, Deserialize, Educe)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(bound = "E: EthSpec", deny_unknown_fields)]
#[cfg_attr(feature = "arbitrary", arbitrary(bound = "E: EthSpec"))]
#[educe(PartialEq, Hash)]
#[context_deserialize(ForkName)]
#[tree_hash(
    struct_behaviour = "progressive_container",
    active_fields(1, 1, 1, 1, 1)
)]
pub struct AttestationEip8453<E: EthSpec> {
    /// One bit for each member of the attestation committees marked in `committee_bits`, in
    /// committee order. Bounded by `MAX_VALIDATORS_PER_AGGREGATE` at validation time.
    pub aggregation_bits: ProgressiveBitList,
    pub data: AttestationDataEip8453,
    pub signature: AggregateSignature,
    pub committee_bits: BitVector<E::AttestationCommitteesPerRound>,
    /// [New in EIP8453] Unsigned; see the container doc comment.
    pub epoch: Epoch,
}

impl<E: EthSpec> AttestationEip8453<E> {
    /// The key under which this attestation should be observed/deduplicated.
    ///
    /// The signature binds `(data, epoch)` via the domain, so `(data_root, epoch)` identifies the
    /// signed message; signers are disambiguated by the aggregation bits separately.
    pub fn observation_key(&self) -> (Hash256, Epoch) {
        (self.data.tree_hash_root(), self.epoch)
    }

    /// Compute the signing root of this attestation's data under its (unsigned) epoch.
    pub fn signing_root(
        &self,
        fork: &Fork,
        genesis_validators_root: Hash256,
        spec: &ChainSpec,
    ) -> Hash256 {
        self.data
            .signing_root_at_epoch(self.epoch, fork, genesis_validators_root, spec)
    }
}

/// An indexed finality gadget attestation [Modified in EIP8453].
///
/// `epoch` is unsigned, exactly as on [`AttestationEip8453`].
#[derive(TreeHash, Debug, Clone, Encode, Decode, Serialize, Deserialize, Educe)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(bound = "E: EthSpec", deny_unknown_fields)]
#[cfg_attr(feature = "arbitrary", arbitrary(bound = "E: EthSpec"))]
#[educe(PartialEq, Hash)]
#[context_deserialize(ForkName)]
#[tree_hash(struct_behaviour = "progressive_container", active_fields(1, 1, 1, 1))]
pub struct IndexedAttestationEip8453<E: EthSpec> {
    /// Validator registry indices, sorted and unique. Bounded by `MAX_VALIDATORS_PER_AGGREGATE`
    /// at validation time.
    #[serde(with = "ssz_types::serde_utils::quoted_u64_var_list")]
    pub attesting_indices: ProgressiveVariableList<u64, E::MaxValidatorsPerAggregate>,
    pub data: AttestationDataEip8453,
    pub signature: AggregateSignature,
    /// [New in EIP8453] Unsigned; selects the signature domain.
    pub epoch: Epoch,
}

/// A single validator's finality gadget attestation, as sent on gossip
/// [Modified in EIP8453].
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Encode, Decode, TreeHash, Educe)]
#[educe(Hash)]
#[context_deserialize(ForkName)]
pub struct SingleAttestationEip8453 {
    #[serde(with = "serde_utils::quoted_u64")]
    pub committee_index: u64,
    #[serde(with = "serde_utils::quoted_u64")]
    pub attester_index: u64,
    pub data: AttestationDataEip8453,
    pub signature: AggregateSignature,
    /// [New in EIP8453] The epoch of the validator's assigned slot. Unsigned.
    pub epoch: Epoch,
}

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
pub struct AttesterSlashingEip8453<E: EthSpec> {
    pub attestation_1: IndexedAttestationEip8453<E>,
    pub attestation_2: IndexedAttestationEip8453<E>,
}

/// `is_double_target_vote` from the spec.
pub fn is_double_target_vote(
    data_1: &AttestationDataEip8453,
    data_2: &AttestationDataEip8453,
) -> bool {
    !data_1.timeout
        && !data_2.timeout
        && data_1.target.height == data_2.target.height
        && data_1.target.root != data_2.target.root
}

/// `is_conflicting_with_finality_pair` from the spec.
pub fn is_conflicting_with_finality_pair(
    data: &AttestationDataEip8453,
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
pub fn is_slashable_attestation_data_eip8453(
    data_1: &AttestationDataEip8453,
    data_2: &AttestationDataEip8453,
) -> bool {
    is_double_target_vote(data_1, data_2)
        || is_conflicting_with_finality_pair(data_1, &data_2.finality)
        || is_conflicting_with_finality_pair(data_2, &data_1.finality)
}

/// A justification of the finality gadget that is not yet finalized [New in EIP8453].
///
/// `participation` marks, per validator index, whether a finality pair for this checkpoint has
/// been counted. Note the nested per-validator list: lives inside the state's `pending_finality`
/// list and is mutated per attestation (a known tree-states performance concern to measure).
#[derive(TreeHash, Debug, Clone, Encode, Decode, Serialize, Deserialize, Educe)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(bound = "E: EthSpec", deny_unknown_fields)]
#[cfg_attr(feature = "arbitrary", arbitrary(bound = "E: EthSpec"))]
#[educe(PartialEq, Hash)]
#[context_deserialize(ForkName)]
pub struct PendingFinality<E: EthSpec> {
    pub checkpoint: HeightCheckpoint,
    pub slot: Slot,
    pub participation: ProgressiveVariableList<bool, E::ValidatorRegistryLimit>,
}

/// The data of a head vote [New in EIP8453]: the available chain's vote for the head block.
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
pub struct HeadVoteData {
    pub slot: Slot,
    pub beacon_block_root: Hash256,
}

impl SignedRoot for HeadVoteData {}

/// An aggregated head vote [New in EIP8453], as included in a beacon block.
#[derive(TreeHash, Debug, Clone, Encode, Decode, Serialize, Deserialize, Educe)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(bound = "E: EthSpec", deny_unknown_fields)]
#[cfg_attr(feature = "arbitrary", arbitrary(bound = "E: EthSpec"))]
#[educe(PartialEq, Hash)]
#[context_deserialize(ForkName)]
#[tree_hash(struct_behaviour = "progressive_container", active_fields(1, 1, 1))]
pub struct HeadVote<E: EthSpec> {
    /// The participation bits of the head vote committee, one bit per seat in committee order.
    pub aggregation_bits: BitVector<E::HeadVoteCommitteeSize>,
    pub data: HeadVoteData,
    pub signature: AggregateSignature,
}

/// A single validator's head vote, as sent on gossip [New in EIP8453].
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Encode, Decode, TreeHash, Educe)]
#[educe(Hash)]
#[context_deserialize(ForkName)]
pub struct HeadVoteMessage {
    #[serde(with = "serde_utils::quoted_u64")]
    pub validator_index: u64,
    pub data: HeadVoteData,
    pub signature: Signature,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Spec;

    macro_rules! ssz_tests_in_module {
        ($module: ident, $type: ty) => {
            mod $module {
                use super::*;
                ssz_and_tree_hash_tests!($type);
            }
        };
    }

    ssz_tests_in_module!(height_checkpoint, HeightCheckpoint);
    ssz_tests_in_module!(attestation_data, AttestationDataEip8453);
    ssz_tests_in_module!(attestation, AttestationEip8453<Spec>);
    ssz_tests_in_module!(indexed_attestation, IndexedAttestationEip8453<Spec>);
    ssz_tests_in_module!(single_attestation, SingleAttestationEip8453);
    ssz_tests_in_module!(attester_slashing, AttesterSlashingEip8453<Spec>);
    ssz_tests_in_module!(head_vote_data, HeadVoteData);
    ssz_tests_in_module!(head_vote, HeadVote<Spec>);
    ssz_tests_in_module!(head_vote_message, HeadVoteMessage);

    #[test]
    fn slashing_predicates() {
        let target = |height, byte| HeightCheckpoint {
            height,
            root: Hash256::repeat_byte(byte),
        };
        let vote =
            |t: HeightCheckpoint, timeout: bool, f: HeightCheckpoint| AttestationDataEip8453 {
                target: t,
                timeout,
                finality: f,
            };

        // Double target vote: same height, different roots, no timeouts.
        let a = vote(target(5, 1), false, HeightCheckpoint::empty());
        let b = vote(target(5, 2), false, HeightCheckpoint::empty());
        assert!(is_slashable_attestation_data_eip8453(&a, &b));

        // Same vote twice is not slashable.
        assert!(!is_slashable_attestation_data_eip8453(&a, &a.clone()));

        // Timeout at the height of another vote's finality pair conflicts with it.
        let c = vote(target(7, 3), false, target(5, 1));
        let d = vote(target(5, 1), true, HeightCheckpoint::empty());
        assert!(is_slashable_attestation_data_eip8453(&c, &d));

        // A target vote matching the finality pair's root does not conflict.
        let e = vote(target(5, 1), false, HeightCheckpoint::empty());
        assert!(!is_slashable_attestation_data_eip8453(&c, &e));

        // Well-formedness.
        assert!(a.is_well_formed());
        assert!(!vote(target(0, 1), false, HeightCheckpoint::empty()).is_well_formed());
        assert!(!vote(target(5, 0), false, HeightCheckpoint::empty()).is_well_formed());
        // Finality pair must be below the target height.
        assert!(!vote(target(5, 1), false, target(5, 2)).is_well_formed());
        assert!(vote(target(5, 1), false, target(4, 2)).is_well_formed());
    }
}
