use crate::kzg_ext::ProgressiveKzgCommitments;
use crate::{
    Address, BeaconStateError, EthSpec, ExecutionBlockHash, ForkName, ForkVersionDecode, Hash256,
    SignedRoot, Slot,
};
use context_deserialize::{ContextDeserialize, context_deserialize};
use educe::Educe;
use serde::{Deserialize, Deserializer, Serialize};
use ssz::Decode;
use ssz_derive::{Decode, Encode};
use std::marker::PhantomData;
use superstruct::superstruct;
use tree_hash::TreeHash as _;
use tree_hash_derive::TreeHash;

// https://github.com/ethereum/consensus-specs/blob/master/specs/gloas/beacon-chain.md#executionpayloadbid
// Heze adds the EIP-8142 chunk commitment:
// consensus-specs/specs/_features/eip8142/beacon-chain.md#executionpayloadbid
#[superstruct(
    variants(Gloas, Heze),
    variant_attributes(
        derive(
            Default,
            Debug,
            Clone,
            Serialize,
            Deserialize,
            Encode,
            Decode,
            TreeHash,
            Educe,
        ),
        context_deserialize(ForkName),
        educe(PartialEq, Hash(bound(E: EthSpec))),
        serde(bound = "E: EthSpec", deny_unknown_fields),
        cfg_attr(
            feature = "arbitrary",
            derive(arbitrary::Arbitrary),
            arbitrary(bound = "E: EthSpec"),
        ),
    ),
    specific_variant_attributes(
        Gloas(tree_hash(
            struct_behaviour = "progressive_container",
            active_fields(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
        )),
        Heze(tree_hash(
            struct_behaviour = "progressive_container",
            active_fields(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
        ))
    ),
    ref_attributes(derive(Debug, PartialEq, Hash)),
    cast_error(
        ty = "BeaconStateError",
        expr = "BeaconStateError::IncorrectStateVariant"
    ),
    partial_getter_error(
        ty = "BeaconStateError",
        expr = "BeaconStateError::IncorrectStateVariant"
    )
)]
#[cfg_attr(
    feature = "arbitrary",
    derive(arbitrary::Arbitrary),
    arbitrary(bound = "E: EthSpec")
)]
#[derive(Debug, Clone, Serialize, Deserialize, Encode, TreeHash, Educe)]
#[educe(PartialEq, Hash(bound(E: EthSpec)))]
#[serde(bound = "E: EthSpec", untagged)]
#[ssz(enum_behaviour = "transparent")]
#[tree_hash(enum_behaviour = "transparent")]
pub struct ExecutionPayloadBid<E: EthSpec> {
    #[superstruct(getter(copy))]
    pub parent_block_hash: ExecutionBlockHash,
    #[superstruct(getter(copy))]
    pub parent_block_root: Hash256,
    #[superstruct(getter(copy))]
    pub block_hash: ExecutionBlockHash,
    #[superstruct(getter(copy))]
    pub prev_randao: Hash256,
    #[superstruct(getter(copy))]
    #[serde(with = "serde_utils::address_hex")]
    pub fee_recipient: Address,
    #[superstruct(getter(copy))]
    #[serde(with = "serde_utils::quoted_u64")]
    pub gas_limit: u64,
    #[superstruct(getter(copy))]
    #[serde(with = "serde_utils::quoted_u64")]
    pub builder_index: u64,
    #[superstruct(getter(copy))]
    pub slot: Slot,
    #[superstruct(getter(copy))]
    #[serde(with = "serde_utils::quoted_u64")]
    pub value: u64,
    #[superstruct(getter(copy))]
    #[serde(with = "serde_utils::quoted_u64")]
    pub execution_payment: u64,
    // [Modified in Gloas:EIP7688]
    pub blob_kzg_commitments: ProgressiveKzgCommitments,
    #[superstruct(getter(copy))]
    pub execution_requests_root: Hash256,
    /// [New in Heze:EIP8142] `hash_tree_root` of the list of chunk hashes of the payload the
    /// builder will reveal, see `crypto/payload_chunks`.
    #[superstruct(only(Heze), partial_getter(copy))]
    pub payload_chunks_root: Hash256,
    /// [New in Heze:EIP8142] Length in bytes of the serialized `ExecutionPayloadContents` the
    /// chunks carry.
    #[superstruct(only(Heze), partial_getter(copy))]
    #[serde(with = "serde_utils::quoted_u64")]
    pub payload_length: u64,
    #[ssz(skip_serializing, skip_deserializing)]
    #[tree_hash(skip_hashing)]
    #[serde(skip)]
    #[cfg_attr(feature = "arbitrary", arbitrary(default))]
    pub _phantom: PhantomData<E>,
}

impl<E: EthSpec> SignedRoot for ExecutionPayloadBid<E> {}
impl<E: EthSpec> SignedRoot for ExecutionPayloadBidGloas<E> {}
impl<E: EthSpec> SignedRoot for ExecutionPayloadBidHeze<E> {}

impl<E: EthSpec> ExecutionPayloadBid<E> {
    /// The default (all-zero) bid of the given fork.
    pub fn default_at_fork(fork_name: ForkName) -> Result<Self, BeaconStateError> {
        match fork_name {
            ForkName::Gloas => Ok(Self::Gloas(ExecutionPayloadBidGloas::default())),
            ForkName::Heze => Ok(Self::Heze(ExecutionPayloadBidHeze::default())),
            _ => Err(BeaconStateError::IncorrectStateVariant),
        }
    }

    pub fn fork_name(&self) -> ForkName {
        match self {
            Self::Gloas(_) => ForkName::Gloas,
            Self::Heze(_) => ForkName::Heze,
        }
    }

    pub fn epoch(&self) -> crate::Epoch {
        self.slot().epoch(E::slots_per_epoch())
    }
}

impl<'a, E: EthSpec> ExecutionPayloadBidRef<'a, E> {
    pub fn fork_name(&self) -> ForkName {
        match self {
            Self::Gloas(_) => ForkName::Gloas,
            Self::Heze(_) => ForkName::Heze,
        }
    }

    pub fn epoch(&self) -> crate::Epoch {
        self.slot().epoch(E::slots_per_epoch())
    }

    /// An owned copy of the referenced bid.
    pub fn clone_from_ref(&self) -> ExecutionPayloadBid<E> {
        match self {
            Self::Gloas(bid) => ExecutionPayloadBid::Gloas((*bid).clone()),
            Self::Heze(bid) => ExecutionPayloadBid::Heze((*bid).clone()),
        }
    }

    pub fn tree_hash_root(&self) -> Hash256 {
        match self {
            Self::Gloas(bid) => bid.tree_hash_root(),
            Self::Heze(bid) => bid.tree_hash_root(),
        }
    }

    pub fn signing_root(&self, domain: Hash256) -> Hash256 {
        match self {
            Self::Gloas(bid) => bid.signing_root(domain),
            Self::Heze(bid) => bid.signing_root(domain),
        }
    }
}

impl<E: EthSpec> ExecutionPayloadBidRefMut<'_, E> {
    /// Overwrites the referenced bid with `bid`, which must be of the same fork.
    pub fn assign(&mut self, bid: ExecutionPayloadBidRef<'_, E>) -> Result<(), BeaconStateError> {
        match (self, bid) {
            (Self::Gloas(dest), ExecutionPayloadBidRef::Gloas(src)) => **dest = src.clone(),
            (Self::Heze(dest), ExecutionPayloadBidRef::Heze(src)) => **dest = src.clone(),
            _ => return Err(BeaconStateError::IncorrectStateVariant),
        }
        Ok(())
    }
}

impl<E: EthSpec> ExecutionPayloadBidGloas<E> {
    /// The Heze bid carrying the same commitments, with an empty chunk commitment. Used by
    /// `upgrade_to_heze` for the bid of the last pre-fork block, against which no chunks are
    /// ever validated.
    pub fn upgrade_to_heze(self) -> ExecutionPayloadBidHeze<E> {
        ExecutionPayloadBidHeze {
            parent_block_hash: self.parent_block_hash,
            parent_block_root: self.parent_block_root,
            block_hash: self.block_hash,
            prev_randao: self.prev_randao,
            fee_recipient: self.fee_recipient,
            gas_limit: self.gas_limit,
            builder_index: self.builder_index,
            slot: self.slot,
            value: self.value,
            execution_payment: self.execution_payment,
            blob_kzg_commitments: self.blob_kzg_commitments,
            execution_requests_root: self.execution_requests_root,
            payload_chunks_root: Hash256::ZERO,
            payload_length: 0,
            _phantom: PhantomData,
        }
    }
}

impl<E: EthSpec> ForkVersionDecode for ExecutionPayloadBid<E> {
    fn from_ssz_bytes_by_fork(bytes: &[u8], fork_name: ForkName) -> Result<Self, ssz::DecodeError> {
        match fork_name {
            ForkName::Gloas => ExecutionPayloadBidGloas::from_ssz_bytes(bytes).map(Self::Gloas),
            ForkName::Heze => ExecutionPayloadBidHeze::from_ssz_bytes(bytes).map(Self::Heze),
            _ => Err(ssz::DecodeError::BytesInvalid(format!(
                "unsupported fork for ExecutionPayloadBid: {fork_name}",
            ))),
        }
    }
}

impl<'de, E: EthSpec> ContextDeserialize<'de, ForkName> for ExecutionPayloadBid<E> {
    fn context_deserialize<D>(deserializer: D, context: ForkName) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let convert_err = |e| {
            serde::de::Error::custom(format!(
                "ExecutionPayloadBid failed to deserialize: {:?}",
                e
            ))
        };
        Ok(match context {
            ForkName::Gloas => {
                Self::Gloas(Deserialize::deserialize(deserializer).map_err(convert_err)?)
            }
            ForkName::Heze => {
                Self::Heze(Deserialize::deserialize(deserializer).map_err(convert_err)?)
            }
            _ => {
                return Err(serde::de::Error::custom(format!(
                    "ExecutionPayloadBid failed to deserialize: unsupported fork '{}'",
                    context
                )));
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MainnetEthSpec;

    mod gloas {
        use super::*;
        ssz_and_tree_hash_tests!(ExecutionPayloadBidGloas<MainnetEthSpec>);
    }

    mod heze {
        use super::*;
        ssz_and_tree_hash_tests!(ExecutionPayloadBidHeze<MainnetEthSpec>);
    }

    #[test]
    fn heze_bid_is_forty_bytes_longer() {
        use ssz::Encode;
        let gloas = ExecutionPayloadBidGloas::<MainnetEthSpec>::default();
        let heze = gloas.clone().upgrade_to_heze();
        assert_eq!(
            heze.as_ssz_bytes().len(),
            gloas.as_ssz_bytes().len() + 32 + 8
        );
    }
}
