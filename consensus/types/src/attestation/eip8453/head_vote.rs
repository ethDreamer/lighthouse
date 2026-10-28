use bls::AggregateSignature;
use context_deserialize::context_deserialize;
use educe::Educe;
use serde::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use ssz_types::VariableList;
use tree_hash_derive::TreeHash;

use super::HeadVoteData;
use crate::{core::EthSpec, fork::ForkName};

/// An aggregated head vote [New in EIP8453], as included in a beacon block.
#[derive(TreeHash, Debug, Clone, Encode, Decode, Serialize, Deserialize, Educe)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(bound = "E: EthSpec", deny_unknown_fields)]
#[cfg_attr(feature = "arbitrary", arbitrary(bound = "E: EthSpec"))]
#[educe(PartialEq, Hash)]
#[context_deserialize(ForkName)]
#[tree_hash(struct_behaviour = "progressive_container", active_fields(1, 1, 1))]
pub struct HeadVote<E: EthSpec> {
    /// The indices of the voting validators, sorted and unique. A validator holding several
    /// committee seats is listed (and signs) once; its vote counts for each of its seats.
    #[serde(with = "ssz_types::serde_utils::quoted_u64_var_list")]
    pub voter_indices: VariableList<u64, E::HeadVoteCommitteeSize>,
    pub data: HeadVoteData,
    pub signature: AggregateSignature,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Spec;

    ssz_and_tree_hash_tests!(HeadVote<Spec>);
}
