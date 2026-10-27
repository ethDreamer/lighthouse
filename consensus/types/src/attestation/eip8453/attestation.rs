use bls::AggregateSignature;
use context_deserialize::context_deserialize;
use educe::Educe;
use serde::{Deserialize, Serialize};
use ssz::{BitVector, ProgressiveBitList};
use ssz_derive::{Decode, Encode};
use tree_hash::TreeHash;
use tree_hash_derive::TreeHash;

use super::AttestationData;
use crate::{
    core::{ChainSpec, Epoch, EthSpec, Hash256},
    fork::{Fork, ForkName},
};

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
pub struct Attestation<E: EthSpec> {
    /// One bit for each member of the attestation committees marked in `committee_bits`, in
    /// committee order. Bounded by `MAX_VALIDATORS_PER_AGGREGATE` at validation time.
    pub aggregation_bits: ProgressiveBitList,
    pub data: AttestationData,
    pub signature: AggregateSignature,
    pub committee_bits: BitVector<E::AttestationCommitteesPerRound>,
    /// [New in EIP8453] Unsigned; see the container doc comment.
    pub epoch: Epoch,
}

impl<E: EthSpec> Attestation<E> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Spec;

    ssz_and_tree_hash_tests!(Attestation<Spec>);
}
