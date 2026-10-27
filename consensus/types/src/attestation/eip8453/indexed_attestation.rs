use bls::AggregateSignature;
use context_deserialize::context_deserialize;
use educe::Educe;
use serde::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use ssz_types::ProgressiveVariableList;
use tree_hash_derive::TreeHash;

use super::AttestationData;
use crate::{
    core::{Epoch, EthSpec},
    fork::ForkName,
};

/// An indexed finality gadget attestation [Modified in EIP8453].
///
/// `epoch` is unsigned, exactly as on [`super::Attestation`].
#[derive(TreeHash, Debug, Clone, Encode, Decode, Serialize, Deserialize, Educe)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(bound = "E: EthSpec", deny_unknown_fields)]
#[cfg_attr(feature = "arbitrary", arbitrary(bound = "E: EthSpec"))]
#[educe(PartialEq, Hash)]
#[context_deserialize(ForkName)]
#[tree_hash(struct_behaviour = "progressive_container", active_fields(1, 1, 1, 1))]
pub struct IndexedAttestation<E: EthSpec> {
    /// Validator registry indices, sorted and unique. Bounded by `MAX_VALIDATORS_PER_AGGREGATE`
    /// at validation time.
    #[serde(with = "ssz_types::serde_utils::quoted_u64_var_list")]
    pub attesting_indices: ProgressiveVariableList<u64, E::MaxValidatorsPerAggregate>,
    pub data: AttestationData,
    pub signature: AggregateSignature,
    /// [New in EIP8453] Unsigned; selects the signature domain.
    pub epoch: Epoch,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Spec;

    ssz_and_tree_hash_tests!(IndexedAttestation<Spec>);
}
