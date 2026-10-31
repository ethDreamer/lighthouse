use bls::Signature;
use context_deserialize::context_deserialize;
use serde::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use tree_hash_derive::TreeHash;

use super::HeadVote;
use crate::{core::EthSpec, fork::ForkName};

/// A head vote aggregate wrapped with its aggregator's identity and selection proof
/// [New in EIP8453], as broadcast on the `head_vote_aggregate` gossip topic.
#[derive(TreeHash, Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(bound = "E: EthSpec", deny_unknown_fields)]
#[cfg_attr(feature = "arbitrary", arbitrary(bound = "E: EthSpec"))]
#[context_deserialize(ForkName)]
pub struct HeadVoteAggregateAndProof<E: EthSpec> {
    #[serde(with = "serde_utils::quoted_u64")]
    pub aggregator_index: u64,
    pub aggregate: HeadVote<E>,
    pub selection_proof: Signature,
}

/// A signed [`HeadVoteAggregateAndProof`] [New in EIP8453]. The signature is made under
/// `DOMAIN_AGGREGATE_AND_PROOF`.
#[derive(TreeHash, Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(bound = "E: EthSpec", deny_unknown_fields)]
#[cfg_attr(feature = "arbitrary", arbitrary(bound = "E: EthSpec"))]
#[context_deserialize(ForkName)]
pub struct SignedHeadVoteAggregateAndProof<E: EthSpec> {
    pub message: HeadVoteAggregateAndProof<E>,
    pub signature: Signature,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Spec;

    ssz_and_tree_hash_tests!(SignedHeadVoteAggregateAndProof<Spec>);
}
