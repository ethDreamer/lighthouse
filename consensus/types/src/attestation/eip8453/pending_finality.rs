use context_deserialize::context_deserialize;
use educe::Educe;
use serde::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use ssz_types::ProgressiveVariableList;
use tree_hash_derive::TreeHash;

use super::HeightCheckpoint;
use crate::{
    core::{EthSpec, Slot},
    fork::ForkName,
};

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Spec;

    ssz_and_tree_hash_tests!(PendingFinality<Spec>);
}
