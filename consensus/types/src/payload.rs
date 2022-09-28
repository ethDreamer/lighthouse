use crate::{test_utils::TestRandom, *};
use derivative::Derivative;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use ssz::{Decode, DecodeError, Encode};
use ssz_derive::{Decode, Encode};
use std::convert::TryFrom;
use std::fmt::Debug;
use std::hash::Hash;
use test_random_derive::TestRandom;
use tree_hash::{PackedEncoding, TreeHash};
use tree_hash_derive::TreeHash;

#[derive(Debug)]
pub enum BlockType {
    Blinded,
    Full,
}

//  + TryFrom<ExecutionPayloadHeader<T>>
pub trait ExecPayload<T: EthSpec>:
    Debug + Clone + TreeHash + PartialEq + Serialize + DeserializeOwned + Hash + Send + 'static
{
    fn block_type() -> BlockType;

    /// Convert the payload into a payload header.
    fn to_execution_payload_header(&self) -> ExecutionPayloadHeader<T>;

    // We provide a subset of field accessors, for the fields used in `consensus`.
    //
    // More fields can be added here if you wish.
    fn parent_hash(&self) -> ExecutionBlockHash;
    fn prev_randao(&self) -> Hash256;
    fn block_number(&self) -> u64;
    fn timestamp(&self) -> u64;
    fn block_hash(&self) -> ExecutionBlockHash;
    fn fee_recipient(&self) -> Address;
    fn gas_limit(&self) -> u64;
}

pub trait AbstractExecPayload<T: EthSpec>:
    ExecPayload<T> + Sized + From<ExecutionPayload<T>> + TryFrom<ExecutionPayloadHeader<T>>
{
    type Merge: ExecPayload<T>
        + Into<Self>
        + AsRef<Self>
        + Default
        + Encode
        + Decode
        + TestRandom
        + From<ExecutionPayloadMerge<T>>
        + TryFrom<ExecutionPayloadHeaderMerge<T>>;
    type Capella: ExecPayload<T>
        + Into<Self>
        + AsRef<Self>
        + Default
        + Encode
        + Decode
        + TestRandom
        + From<ExecutionPayloadCapella<T>>
        + TryFrom<ExecutionPayloadHeaderCapella<T>>;
}

#[superstruct(
    variants(Merge, Capella),
    variant_attributes(
        derive(
            Debug,
            Clone,
            Serialize,
            Deserialize,
            Encode,
            Decode,
            TestRandom,
            TreeHash,
            Derivative,
        ),
        derivative(PartialEq, Hash(bound = "T: EthSpec")),
        serde(bound = "T: EthSpec", deny_unknown_fields),
        cfg_attr(feature = "arbitrary-fuzz", derive(arbitrary::Arbitrary))
    ),
    cast_error(ty = "Error", expr = "BeaconStateError::IncorrectStateVariant"),
    partial_getter_error(ty = "Error", expr = "BeaconStateError::IncorrectStateVariant")
)]
#[derive(Debug, Clone, Serialize, Deserialize, TreeHash, Derivative)]
#[derivative(PartialEq, Hash(bound = "T: EthSpec"))]
#[serde(bound = "T: EthSpec")]
#[tree_hash(enum_behaviour = "transparent")]
pub struct FullPayload<T: EthSpec> {
    #[superstruct(only(Merge), partial_getter(rename = "execution_payload_merge"))]
    pub execution_payload: ExecutionPayloadMerge<T>,
    #[superstruct(only(Capella), partial_getter(rename = "execution_payload_capella"))]
    pub execution_payload: ExecutionPayloadCapella<T>,
}

impl<T: EthSpec> ExecPayload<T> for FullPayloadMerge<T> {
    fn block_type() -> BlockType {
        BlockType::Full
    }

    fn to_execution_payload_header(&self) -> ExecutionPayloadHeader<T> {
        ExecutionPayloadHeader::Merge(ExecutionPayloadHeaderMerge::from(
            self.execution_payload.clone(),
        ))
    }

    fn parent_hash(&self) -> ExecutionBlockHash {
        self.execution_payload.parent_hash
    }

    fn prev_randao(&self) -> Hash256 {
        self.execution_payload.prev_randao
    }

    fn block_number(&self) -> u64 {
        self.execution_payload.block_number
    }

    fn timestamp(&self) -> u64 {
        self.execution_payload.timestamp
    }

    fn block_hash(&self) -> ExecutionBlockHash {
        self.execution_payload.block_hash
    }

    fn fee_recipient(&self) -> Address {
        self.execution_payload.fee_recipient
    }

    fn gas_limit(&self) -> u64 {
        self.execution_payload.gas_limit
    }
}
impl<T: EthSpec> ExecPayload<T> for FullPayloadCapella<T> {
    fn block_type() -> BlockType {
        BlockType::Full
    }

    fn to_execution_payload_header(&self) -> ExecutionPayloadHeader<T> {
        ExecutionPayloadHeader::Capella(ExecutionPayloadHeaderCapella::from(
            self.execution_payload.clone(),
        ))
    }

    fn parent_hash(&self) -> ExecutionBlockHash {
        self.execution_payload.parent_hash
    }

    fn prev_randao(&self) -> Hash256 {
        self.execution_payload.prev_randao
    }

    fn block_number(&self) -> u64 {
        self.execution_payload.block_number
    }

    fn timestamp(&self) -> u64 {
        self.execution_payload.timestamp
    }

    fn block_hash(&self) -> ExecutionBlockHash {
        self.execution_payload.block_hash
    }

    fn fee_recipient(&self) -> Address {
        self.execution_payload.fee_recipient
    }

    fn gas_limit(&self) -> u64 {
        self.execution_payload.gas_limit
    }
}

impl<T: EthSpec> ExecPayload<T> for FullPayload<T> {
    fn block_type() -> BlockType {
        BlockType::Full
    }

    fn to_execution_payload_header(&self) -> ExecutionPayloadHeader<T> {
        match self {
            Self::Merge(payload) => ExecutionPayloadHeader::Merge(
                ExecutionPayloadHeaderMerge::from(payload.execution_payload.clone()),
            ),
            Self::Capella(payload) => ExecutionPayloadHeader::Capella(
                ExecutionPayloadHeaderCapella::from(payload.execution_payload.clone()),
            ),
        }
    }

    fn parent_hash(&self) -> ExecutionBlockHash {
        match self {
            Self::Merge(payload) => payload.execution_payload.parent_hash,
            Self::Capella(payload) => payload.execution_payload.parent_hash,
        }
    }

    fn prev_randao(&self) -> Hash256 {
        match self {
            Self::Merge(payload) => payload.execution_payload.prev_randao,
            Self::Capella(payload) => payload.execution_payload.prev_randao,
        }
    }

    fn block_number(&self) -> u64 {
        match self {
            Self::Merge(payload) => payload.execution_payload.block_number,
            Self::Capella(payload) => payload.execution_payload.block_number,
        }
    }

    fn timestamp(&self) -> u64 {
        match self {
            Self::Merge(payload) => payload.execution_payload.timestamp,
            Self::Capella(payload) => payload.execution_payload.timestamp,
        }
    }

    fn block_hash(&self) -> ExecutionBlockHash {
        match self {
            Self::Merge(payload) => payload.execution_payload.block_hash,
            Self::Capella(payload) => payload.execution_payload.block_hash,
        }
    }

    fn fee_recipient(&self) -> Address {
        match self {
            Self::Merge(payload) => payload.execution_payload.fee_recipient,
            Self::Capella(payload) => payload.execution_payload.fee_recipient,
        }
    }

    fn gas_limit(&self) -> u64 {
        match self {
            Self::Merge(payload) => payload.execution_payload.gas_limit,
            Self::Capella(payload) => payload.execution_payload.gas_limit,
        }
    }
}

impl<T: EthSpec> AbstractExecPayload<T> for FullPayload<T> {
    type Merge = FullPayloadMerge<T>;
    type Capella = FullPayloadCapella<T>;
}

/*
// original implementation
impl<T: EthSpec> From<ExecutionPayload<T>> for FullPayload<T> {
    fn from(execution_payload: ExecutionPayload<T>) -> Self {
        Self { execution_payload }
    }
}

impl<T: EthSpec> TryFrom<ExecutionPayloadHeader<T>> for FullPayload<T> {
    type Error = ();

    fn try_from(_: ExecutionPayloadHeader<T>) -> Result<Self, Self::Error> {
        Err(())
    }
}
 */

impl<T: EthSpec> From<ExecutionPayload<T>> for FullPayload<T> {
    fn from(execution_payload: ExecutionPayload<T>) -> Self {
        match execution_payload {
            ExecutionPayload::Merge(execution_payload) => {
                Self::Merge(FullPayloadMerge { execution_payload })
            }
            ExecutionPayload::Capella(execution_payload) => {
                Self::Capella(FullPayloadCapella { execution_payload })
            }
        }
    }
}

impl<T: EthSpec> TryFrom<ExecutionPayloadHeader<T>> for FullPayload<T> {
    type Error = ();
    fn try_from(_: ExecutionPayloadHeader<T>) -> Result<Self, Self::Error> {
        Err(())
    }
}

impl<T: EthSpec> From<ExecutionPayloadMerge<T>> for FullPayloadMerge<T> {
    fn from(execution_payload: ExecutionPayloadMerge<T>) -> Self {
        Self { execution_payload }
    }
}
impl<T: EthSpec> From<ExecutionPayloadCapella<T>> for FullPayloadCapella<T> {
    fn from(execution_payload: ExecutionPayloadCapella<T>) -> Self {
        Self { execution_payload }
    }
}

impl<T: EthSpec> TryFrom<ExecutionPayloadHeader<T>> for FullPayloadMerge<T> {
    type Error = ();
    fn try_from(_: ExecutionPayloadHeader<T>) -> Result<Self, Self::Error> {
        Err(())
    }
}
impl<T: EthSpec> TryFrom<ExecutionPayloadHeader<T>> for FullPayloadCapella<T> {
    type Error = ();
    fn try_from(_: ExecutionPayloadHeader<T>) -> Result<Self, Self::Error> {
        Err(())
    }
}

impl<T: EthSpec> TryFrom<ExecutionPayloadHeaderMerge<T>> for FullPayloadMerge<T> {
    type Error = ();
    fn try_from(_: ExecutionPayloadHeaderMerge<T>) -> Result<Self, Self::Error> {
        Err(())
    }
}
impl<T: EthSpec> TryFrom<ExecutionPayloadHeaderCapella<T>> for FullPayloadCapella<T> {
    type Error = ();
    fn try_from(_: ExecutionPayloadHeaderCapella<T>) -> Result<Self, Self::Error> {
        Err(())
    }
}

#[superstruct(
    variants(Merge, Capella),
    variant_attributes(
        derive(
            Debug,
            Clone,
            Serialize,
            Deserialize,
            Encode,
            Decode,
            TestRandom,
            TreeHash,
            Derivative,
        ),
        derivative(PartialEq, Hash(bound = "T: EthSpec")),
        serde(bound = "T: EthSpec", deny_unknown_fields),
        cfg_attr(feature = "arbitrary-fuzz", derive(arbitrary::Arbitrary))
    ),
    cast_error(ty = "Error", expr = "BeaconStateError::IncorrectStateVariant"),
    partial_getter_error(ty = "Error", expr = "BeaconStateError::IncorrectStateVariant")
)]
#[derive(Debug, Clone, Serialize, Deserialize, TreeHash, Derivative)]
#[derivative(PartialEq, Hash(bound = "T: EthSpec"))]
#[serde(bound = "T: EthSpec")]
#[tree_hash(enum_behaviour = "transparent")]
pub struct BlindedPayload<T: EthSpec> {
    #[superstruct(only(Merge), partial_getter(rename = "execution_payload_merge"))]
    pub execution_payload_header: ExecutionPayloadHeaderMerge<T>,
    #[superstruct(only(Capella), partial_getter(rename = "execution_payload_capella"))]
    pub execution_payload_header: ExecutionPayloadHeaderCapella<T>,
}

impl<T: EthSpec> ExecPayload<T> for BlindedPayload<T> {
    fn block_type() -> BlockType {
        BlockType::Blinded
    }

    fn to_execution_payload_header(&self) -> ExecutionPayloadHeader<T> {
        match self {
            Self::Merge(payload) => {
                ExecutionPayloadHeader::Merge(payload.execution_payload_header.clone())
            }
            Self::Capella(payload) => {
                ExecutionPayloadHeader::Capella(payload.execution_payload_header.clone())
            }
        }
    }

    fn parent_hash(&self) -> ExecutionBlockHash {
        match self {
            Self::Merge(payload) => payload.execution_payload_header.parent_hash,
            Self::Capella(payload) => payload.execution_payload_header.parent_hash,
        }
    }

    fn prev_randao(&self) -> Hash256 {
        match self {
            Self::Merge(payload) => payload.execution_payload_header.prev_randao,
            Self::Capella(payload) => payload.execution_payload_header.prev_randao,
        }
    }

    fn block_number(&self) -> u64 {
        match self {
            Self::Merge(payload) => payload.execution_payload_header.block_number,
            Self::Capella(payload) => payload.execution_payload_header.block_number,
        }
    }

    fn timestamp(&self) -> u64 {
        match self {
            Self::Merge(payload) => payload.execution_payload_header.timestamp,
            Self::Capella(payload) => payload.execution_payload_header.timestamp,
        }
    }

    fn block_hash(&self) -> ExecutionBlockHash {
        match self {
            Self::Merge(payload) => payload.execution_payload_header.block_hash,
            Self::Capella(payload) => payload.execution_payload_header.block_hash,
        }
    }

    fn fee_recipient(&self) -> Address {
        match self {
            Self::Merge(payload) => payload.execution_payload_header.fee_recipient,
            Self::Capella(payload) => payload.execution_payload_header.fee_recipient,
        }
    }

    fn gas_limit(&self) -> u64 {
        match self {
            Self::Merge(payload) => payload.execution_payload_header.gas_limit,
            Self::Capella(payload) => payload.execution_payload_header.gas_limit,
        }
    }
}

impl<T: EthSpec> ExecPayload<T> for BlindedPayloadMerge<T> {
    fn block_type() -> BlockType {
        BlockType::Full
    }

    fn to_execution_payload_header(&self) -> ExecutionPayloadHeader<T> {
        ExecutionPayloadHeader::Merge(ExecutionPayloadHeaderMerge::from(
            self.execution_payload_header.clone(),
        ))
    }

    fn parent_hash(&self) -> ExecutionBlockHash {
        self.execution_payload_header.parent_hash
    }

    fn prev_randao(&self) -> Hash256 {
        self.execution_payload_header.prev_randao
    }

    fn block_number(&self) -> u64 {
        self.execution_payload_header.block_number
    }

    fn timestamp(&self) -> u64 {
        self.execution_payload_header.timestamp
    }

    fn block_hash(&self) -> ExecutionBlockHash {
        self.execution_payload_header.block_hash
    }

    fn fee_recipient(&self) -> Address {
        self.execution_payload_header.fee_recipient
    }

    fn gas_limit(&self) -> u64 {
        self.execution_payload_header.gas_limit
    }
}
impl<T: EthSpec> ExecPayload<T> for BlindedPayloadCapella<T> {
    fn block_type() -> BlockType {
        BlockType::Full
    }

    fn to_execution_payload_header(&self) -> ExecutionPayloadHeader<T> {
        ExecutionPayloadHeader::Capella(ExecutionPayloadHeaderCapella::from(
            self.execution_payload_header.clone(),
        ))
    }

    fn parent_hash(&self) -> ExecutionBlockHash {
        self.execution_payload_header.parent_hash
    }

    fn prev_randao(&self) -> Hash256 {
        self.execution_payload_header.prev_randao
    }

    fn block_number(&self) -> u64 {
        self.execution_payload_header.block_number
    }

    fn timestamp(&self) -> u64 {
        self.execution_payload_header.timestamp
    }

    fn block_hash(&self) -> ExecutionBlockHash {
        self.execution_payload_header.block_hash
    }

    fn fee_recipient(&self) -> Address {
        self.execution_payload_header.fee_recipient
    }

    fn gas_limit(&self) -> u64 {
        self.execution_payload_header.gas_limit
    }
}

impl<T: EthSpec> AbstractExecPayload<T> for BlindedPayload<T> {
    type Merge = BlindedPayloadMerge<T>;
    type Capella = BlindedPayloadCapella<T>;
}

impl<T: EthSpec> Default for FullPayloadMerge<T> {
    fn default() -> Self {
        Self {
            execution_payload: ExecutionPayloadMerge::default(),
        }
    }
}
impl<T: EthSpec> Default for FullPayloadCapella<T> {
    fn default() -> Self {
        Self {
            execution_payload: ExecutionPayloadCapella::default(),
        }
    }
}

// NOTE: the `Default` implementation for `BlindedPayload` needs to be different from the `Default`
// implementation for `ExecutionPayloadHeader` because payloads are checked for equality against the
// default payload in `is_merge_transition_block` to determine whether the merge has occurred.
//
// The default `BlindedPayload` is therefore the payload header that results from blinding the
// default `ExecutionPayload`, which differs from the default `ExecutionPayloadHeader` in that
// its `transactions_root` is the hash of the empty list rather than 0x0.
/*
impl<T: EthSpec> Default for BlindedPayload<T> {
    fn default() -> Self {
        Self {
            execution_payload_header: ExecutionPayloadHeader::from(&ExecutionPayload::default()),
        }
    }
}
*/

impl<T: EthSpec> Default for BlindedPayloadMerge<T> {
    fn default() -> Self {
        Self {
            execution_payload_header: ExecutionPayloadHeaderMerge::from(
                ExecutionPayloadMerge::default(),
            ),
        }
    }
}

impl<T: EthSpec> Default for BlindedPayloadCapella<T> {
    fn default() -> Self {
        Self {
            execution_payload_header: ExecutionPayloadHeaderCapella::from(
                ExecutionPayloadCapella::default(),
            ),
        }
    }
}

impl<T: EthSpec> From<ExecutionPayload<T>> for BlindedPayload<T> {
    fn from(payload: ExecutionPayload<T>) -> Self {
        match payload {
            ExecutionPayload::Merge(payload) => BlindedPayload::Merge(payload.into()),
            ExecutionPayload::Capella(payload) => BlindedPayload::Capella(payload.into()),
        }
    }
}

impl<T: EthSpec> From<ExecutionPayloadHeader<T>> for BlindedPayload<T> {
    fn from(execution_payload_header: ExecutionPayloadHeader<T>) -> Self {
        match execution_payload_header {
            ExecutionPayloadHeader::Merge(execution_payload_header) => {
                Self::Merge(BlindedPayloadMerge {
                    execution_payload_header,
                })
            }
            ExecutionPayloadHeader::Capella(execution_payload_header) => {
                Self::Capella(BlindedPayloadCapella {
                    execution_payload_header,
                })
            }
        }
    }
}

impl<T: EthSpec> From<ExecutionPayloadHeaderMerge<T>> for BlindedPayloadMerge<T> {
    fn from(execution_payload_header: ExecutionPayloadHeaderMerge<T>) -> Self {
        Self {
            execution_payload_header,
        }
    }
}
impl<T: EthSpec> From<ExecutionPayloadHeaderCapella<T>> for BlindedPayloadCapella<T> {
    fn from(execution_payload_header: ExecutionPayloadHeaderCapella<T>) -> Self {
        Self {
            execution_payload_header,
        }
    }
}

impl<T: EthSpec> From<BlindedPayload<T>> for ExecutionPayloadHeader<T> {
    fn from(blinded: BlindedPayload<T>) -> Self {
        match blinded {
            BlindedPayload::Merge(blinded_payload) => {
                ExecutionPayloadHeader::Merge(blinded_payload.execution_payload_header)
            }
            BlindedPayload::Capella(blinded_payload) => {
                ExecutionPayloadHeader::Capella(blinded_payload.execution_payload_header)
            }
        }
    }
}

impl<T: EthSpec> From<ExecutionPayloadMerge<T>> for BlindedPayloadMerge<T> {
    fn from(execution_payload: ExecutionPayloadMerge<T>) -> Self {
        Self {
            execution_payload_header: ExecutionPayloadHeaderMerge::from(execution_payload),
        }
    }
}
impl<T: EthSpec> From<ExecutionPayloadCapella<T>> for BlindedPayloadCapella<T> {
    fn from(execution_payload: ExecutionPayloadCapella<T>) -> Self {
        Self {
            execution_payload_header: ExecutionPayloadHeaderCapella::from(execution_payload),
        }
    }
}

impl<T: EthSpec> TryFrom<ExecutionPayloadHeader<T>> for BlindedPayloadMerge<T> {
    type Error = ();
    fn try_from(header: ExecutionPayloadHeader<T>) -> Result<Self, Self::Error> {
        match header {
            ExecutionPayloadHeader::Merge(execution_payload_header) => {
                Ok(execution_payload_header.into())
            }
            _ => Err(()),
        }
    }
}
impl<T: EthSpec> TryFrom<ExecutionPayloadHeader<T>> for BlindedPayloadCapella<T> {
    type Error = ();
    fn try_from(header: ExecutionPayloadHeader<T>) -> Result<Self, Self::Error> {
        match header {
            ExecutionPayloadHeader::Capella(execution_payload_header) => {
                Ok(execution_payload_header.into())
            }
            _ => Err(()),
        }
    }
}

/*
// TODO: implement this if it's needed
impl<T: EthSpec> From<ExecutionPayload<T>> for BlindedPayload<T> {
    fn from(execution_payload: ExecutionPayload<T>) -> Self {
        Self {
            execution_payload_header: ExecutionPayloadHeader::from(&execution_payload),
        }
    }
}
 */

/*
impl<T: EthSpec> TreeHash for BlindedPayload<T> {
    fn tree_hash_type() -> tree_hash::TreeHashType {
        <ExecutionPayloadHeader<T>>::tree_hash_type()
    }

    fn tree_hash_packed_encoding(&self) -> PackedEncoding {
        self.execution_payload_header.tree_hash_packed_encoding()
    }

    fn tree_hash_packing_factor() -> usize {
        <ExecutionPayloadHeader<T>>::tree_hash_packing_factor()
    }

    fn tree_hash_root(&self) -> tree_hash::Hash256 {
        self.execution_payload_header.tree_hash_root()
    }
}
 */

/*
impl<T: EthSpec> Decode for BlindedPayload<T> {
    fn is_ssz_fixed_len() -> bool {
        <ExecutionPayloadHeader<T> as Decode>::is_ssz_fixed_len()
    }

    fn ssz_fixed_len() -> usize {
        <ExecutionPayloadHeader<T> as Decode>::ssz_fixed_len()
    }

    fn from_ssz_bytes(bytes: &[u8]) -> Result<Self, DecodeError> {
        Ok(Self {
            execution_payload_header: ExecutionPayloadHeader::from_ssz_bytes(bytes)?,
        })
    }
}
 */

/*
impl<T: EthSpec> Encode for BlindedPayload<T> {
    fn is_ssz_fixed_len() -> bool {
        <ExecutionPayloadHeader<T> as Encode>::is_ssz_fixed_len()
    }

    fn ssz_append(&self, buf: &mut Vec<u8>) {
        self.execution_payload_header.ssz_append(buf)
    }

    fn ssz_bytes_len(&self) -> usize {
        self.execution_payload_header.ssz_bytes_len()
    }
}
*/

impl<T: EthSpec> From<FullPayloadMerge<T>> for FullPayload<T> {
    fn from(payload: FullPayloadMerge<T>) -> Self {
        Self::Merge(payload)
    }
}

impl<T: EthSpec> From<FullPayloadCapella<T>> for FullPayload<T> {
    fn from(payload: FullPayloadCapella<T>) -> Self {
        Self::Capella(payload)
    }
}

impl<T: EthSpec> From<BlindedPayloadMerge<T>> for BlindedPayload<T> {
    fn from(payload: BlindedPayloadMerge<T>) -> Self {
        Self::Merge(payload)
    }
}
impl<T: EthSpec> From<BlindedPayloadCapella<T>> for BlindedPayload<T> {
    fn from(payload: BlindedPayloadCapella<T>) -> Self {
        Self::Capella(payload)
    }
}


// more REF experiments

impl<'a, T: EthSpec> From<&FullPayloadMerge<T>> for FullPayloadRef<'a, T> {
    fn from(payload_ref: &FullPayloadMerge<T>) -> Self {
        FullPayloadRef::Merge(payload_ref)
    }
}
impl<'a, T: EthSpec> From<&FullPayloadCapella<T>> for FullPayloadRef<'a, T> {
    fn from(payload_ref: &FullPayloadCapella<T>) -> Self {
        FullPayloadRef::Capella(payload_ref)
    }
}
impl<'a, T: EthSpec> From<&BlindedPayloadMerge<T>> for BlindedPayloadRef<'a, T> {
    fn from(payload_ref: &BlindedPayloadMerge<T>) -> Self {
        BlindedPayloadRef::Merge(payload_ref)
    }
}
impl<'a, T: EthSpec> From<&BlindedPayloadCapella<T>> for BlindedPayloadRef<'a, T> {
    fn from(payload_ref: &BlindedPayloadCapella<T>) -> Self {
        BlindedPayloadRef::Capella(payload_ref)
    }
}

// AsRef<Self> experiments

impl<'a, T: EthSpec> AsRef<FullPayload<T>> for FullPayloadMerge<T> {
    fn as_ref(&self) -> &FullPayload<T> {
        &FullPayload::Merge(self)
    }
}
impl<'a, T: EthSpec> AsRef<FullPayload<T>> for FullPayloadCapella<T> {
    fn as_ref(&self) -> &FullPayload<T> {
        &FullPayload::Capella(self)
    }
}
impl<'a, T: EthSpec> AsRef<BlindedPayload<T>> for BlindedPayloadMerge<T> {
    fn as_ref(&self) -> &BlindedPayload<T> {
        &BlindedPayload::Merge(self)
    }
}
impl<'a, T: EthSpec> AsRef<BlindedPayload<T>> for BlindedPayloadCapella<T> {
    fn as_ref(&self) -> &BlindedPayload<T> {
        &BlindedPayload::Capella(self)
    }
}

// end REF experiments



/*
impl<T: EthSpec> TreeHash for FullPayload<T> {
    fn tree_hash_type() -> tree_hash::TreeHashType {
        <ExecutionPayload<T>>::tree_hash_type()
    }

    fn tree_hash_packed_encoding(&self) -> tree_hash::PackedEncoding {
        self.execution_payload.tree_hash_packed_encoding()
    }

    fn tree_hash_packing_factor() -> usize {
        <ExecutionPayload<T>>::tree_hash_packing_factor()
    }

    fn tree_hash_root(&self) -> tree_hash::Hash256 {
        self.execution_payload.tree_hash_root()
    }
}
*/

/*
impl<T: EthSpec> Decode for FullPayload<T> {
    fn is_ssz_fixed_len() -> bool {
        <ExecutionPayload<T> as Decode>::is_ssz_fixed_len()
    }

    fn from_ssz_bytes(bytes: &[u8]) -> Result<Self, DecodeError> {
        Ok(FullPayload {
            execution_payload: Decode::from_ssz_bytes(bytes)?,
        })
    }
}

impl<T: EthSpec> Encode for FullPayload<T> {
    fn is_ssz_fixed_len() -> bool {
        <ExecutionPayload<T> as Encode>::is_ssz_fixed_len()
    }

    fn ssz_append(&self, buf: &mut Vec<u8>) {
        self.execution_payload.ssz_append(buf)
    }

    fn ssz_bytes_len(&self) -> usize {
        self.execution_payload.ssz_bytes_len()
    }
}
*/
