use bls::Signature;
use context_deserialize::context_deserialize;
use serde::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use tree_hash_derive::TreeHash;

use super::HeadVote;
use crate::core::{ChainSpec, Domain, Hash256, SignedRoot};
use crate::fork::Fork;
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

impl<E: EthSpec> SignedRoot for HeadVoteAggregateAndProof<E> {}

impl<E: EthSpec> HeadVoteAggregateAndProof<E> {
    /// The wrapper's signing root: `DOMAIN_AGGREGATE_AND_PROOF` at the vote slot's epoch.
    pub fn compute_signing_root(
        &self,
        fork: &Fork,
        genesis_validators_root: Hash256,
        spec: &ChainSpec,
    ) -> Hash256 {
        let epoch = self.aggregate.data.slot.epoch(E::slots_per_epoch());
        let domain = spec.get_domain(
            epoch,
            Domain::AggregateAndProof,
            fork,
            genesis_validators_root,
        );
        self.signing_root(domain)
    }
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
