use bls::AggregateSignature;
use context_deserialize::context_deserialize;
use educe::Educe;
use serde::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use ssz_types::ProgressiveVariableList;
use tree_hash_derive::TreeHash;

use super::SafeVoteData;
use crate::{core::EthSpec, fork::ForkName};

/// An aggregated safe vote [New in EIP8453], as included in a beacon block.
///
/// A safe vote aggregate combines validators naming the same node in the same round, regardless
/// of their attestation committees, so the spec leaves `voter_indices` unbounded
/// (`ProgressiveList[ValidatorIndex]`); the registry limit serves as the decode bound here.
#[derive(TreeHash, Debug, Clone, Encode, Decode, Serialize, Deserialize, Educe)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(bound = "E: EthSpec", deny_unknown_fields)]
#[cfg_attr(feature = "arbitrary", arbitrary(bound = "E: EthSpec"))]
#[educe(PartialEq, Hash)]
#[context_deserialize(ForkName)]
#[tree_hash(struct_behaviour = "progressive_container", active_fields(1, 1, 1))]
pub struct SafeVote<E: EthSpec> {
    /// The indices of the voting validators, sorted and unique.
    #[serde(with = "ssz_types::serde_utils::quoted_u64_var_list")]
    pub voter_indices: ProgressiveVariableList<u64, E::ValidatorRegistryLimit>,
    pub data: SafeVoteData,
    pub signature: AggregateSignature,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Spec;

    ssz_and_tree_hash_tests!(SafeVote<Spec>);
}
