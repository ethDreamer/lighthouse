use crate::execution::{
    ExecutionPayloadBid, ExecutionPayloadBidGloas, ExecutionPayloadBidHeze, ExecutionPayloadBidRef,
    ExecutionPayloadBidRefMut,
};
use crate::{BeaconStateError, EthSpec, ForkName, ForkVersionDecode};
use bls::Signature;
use context_deserialize::{ContextDeserialize, context_deserialize};
use educe::Educe;
use serde::{Deserialize, Deserializer, Serialize};
use ssz::Decode;
use ssz_derive::{Decode, Encode};
use superstruct::superstruct;
use tree_hash_derive::TreeHash;

// https://github.com/ethereum/consensus-specs/blob/master/specs/gloas/beacon-chain.md#signedexecutionpayloadbid
#[superstruct(
    variants(Gloas, Heze),
    variant_attributes(
        derive(
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
pub struct SignedExecutionPayloadBid<E: EthSpec> {
    #[superstruct(only(Gloas), partial_getter(rename = "message_gloas"))]
    pub message: ExecutionPayloadBidGloas<E>,
    #[superstruct(only(Heze), partial_getter(rename = "message_heze"))]
    pub message: ExecutionPayloadBidHeze<E>,
    pub signature: Signature,
}

impl<E: EthSpec> SignedExecutionPayloadBid<E> {
    /// The bid, whichever fork it belongs to.
    pub fn message(&self) -> ExecutionPayloadBidRef<'_, E> {
        self.to_ref().message()
    }

    pub fn message_mut(&mut self) -> ExecutionPayloadBidRefMut<'_, E> {
        match self {
            Self::Gloas(signed) => ExecutionPayloadBidRefMut::Gloas(&mut signed.message),
            Self::Heze(signed) => ExecutionPayloadBidRefMut::Heze(&mut signed.message),
        }
    }

    /// The bid as an owned `ExecutionPayloadBid`, consuming the signature.
    pub fn into_message(self) -> ExecutionPayloadBid<E> {
        match self {
            Self::Gloas(signed) => ExecutionPayloadBid::Gloas(signed.message),
            Self::Heze(signed) => ExecutionPayloadBid::Heze(signed.message),
        }
    }

    pub fn fork_name(&self) -> ForkName {
        match self {
            Self::Gloas(_) => ForkName::Gloas,
            Self::Heze(_) => ForkName::Heze,
        }
    }

    pub fn epoch(&self) -> crate::Epoch {
        self.message().epoch()
    }

    pub fn slot(&self) -> crate::Slot {
        self.message().slot()
    }

    /// A default bid with an empty signature, in the given fork.
    pub fn empty_at_fork(fork_name: ForkName) -> Result<Self, BeaconStateError> {
        Self::new(
            ExecutionPayloadBid::default_at_fork(fork_name)?,
            Signature::empty(),
        )
    }

    /// Pairs a bid with its signature. Never fails: every bid variant has a signed variant.
    pub fn new(
        message: ExecutionPayloadBid<E>,
        signature: Signature,
    ) -> Result<Self, BeaconStateError> {
        Ok(match message {
            ExecutionPayloadBid::Gloas(message) => {
                Self::Gloas(SignedExecutionPayloadBidGloas { message, signature })
            }
            ExecutionPayloadBid::Heze(message) => {
                Self::Heze(SignedExecutionPayloadBidHeze { message, signature })
            }
        })
    }

    pub fn num_blobs_expected(&self) -> usize {
        self.message().blob_kzg_commitments().len()
    }
}

impl<E: EthSpec> SignedExecutionPayloadBidGloas<E> {
    pub fn empty() -> Self {
        Self {
            message: ExecutionPayloadBidGloas::default(),
            signature: Signature::empty(),
        }
    }
}

impl<E: EthSpec> SignedExecutionPayloadBidHeze<E> {
    pub fn empty() -> Self {
        Self {
            message: ExecutionPayloadBidHeze::default(),
            signature: Signature::empty(),
        }
    }
}

impl<'a, E: EthSpec> SignedExecutionPayloadBidRef<'a, E> {
    pub fn message(&self) -> ExecutionPayloadBidRef<'a, E> {
        match self {
            Self::Gloas(signed) => ExecutionPayloadBidRef::Gloas(&signed.message),
            Self::Heze(signed) => ExecutionPayloadBidRef::Heze(&signed.message),
        }
    }

    pub fn fork_name(&self) -> ForkName {
        match self {
            Self::Gloas(_) => ForkName::Gloas,
            Self::Heze(_) => ForkName::Heze,
        }
    }

    pub fn epoch(&self) -> crate::Epoch {
        self.message().epoch()
    }

    pub fn slot(&self) -> crate::Slot {
        self.message().slot()
    }

    pub fn clone_from_ref(&self) -> SignedExecutionPayloadBid<E> {
        match self {
            Self::Gloas(signed) => SignedExecutionPayloadBid::Gloas((*signed).clone()),
            Self::Heze(signed) => SignedExecutionPayloadBid::Heze((*signed).clone()),
        }
    }
}

impl<E: EthSpec> ForkVersionDecode for SignedExecutionPayloadBid<E> {
    fn from_ssz_bytes_by_fork(bytes: &[u8], fork_name: ForkName) -> Result<Self, ssz::DecodeError> {
        match fork_name {
            ForkName::Gloas => {
                SignedExecutionPayloadBidGloas::from_ssz_bytes(bytes).map(Self::Gloas)
            }
            ForkName::Heze => SignedExecutionPayloadBidHeze::from_ssz_bytes(bytes).map(Self::Heze),
            _ => Err(ssz::DecodeError::BytesInvalid(format!(
                "unsupported fork for SignedExecutionPayloadBid: {fork_name}",
            ))),
        }
    }
}

impl<'de, E: EthSpec> ContextDeserialize<'de, ForkName> for SignedExecutionPayloadBid<E> {
    fn context_deserialize<D>(deserializer: D, context: ForkName) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let convert_err = |e| {
            serde::de::Error::custom(format!(
                "SignedExecutionPayloadBid failed to deserialize: {:?}",
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
                    "SignedExecutionPayloadBid failed to deserialize: unsupported fork '{}'",
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
        ssz_and_tree_hash_tests!(SignedExecutionPayloadBidGloas<MainnetEthSpec>);
    }

    mod heze {
        use super::*;
        ssz_and_tree_hash_tests!(SignedExecutionPayloadBidHeze<MainnetEthSpec>);
    }
}
