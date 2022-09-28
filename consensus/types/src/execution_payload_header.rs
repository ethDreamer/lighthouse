use crate::{test_utils::TestRandom, *};
use BeaconStateError;
use derivative::Derivative;
use serde_derive::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use test_random_derive::TestRandom;
use tree_hash::TreeHash;
use tree_hash_derive::TreeHash;

#[superstruct(
    variants(Merge, Capella),
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
            TestRandom,
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
#[tree_hash(enum_behaviour = "transparent")]
#[serde(bound = "T: EthSpec")]
#[cfg_attr(feature = "arbitrary-fuzz", derive(arbitrary::Arbitrary))]
pub struct ExecutionPayloadHeader<T: EthSpec> {
    pub parent_hash: ExecutionBlockHash,
    pub fee_recipient: Address,
    pub state_root: Hash256,
    pub receipts_root: Hash256,
    #[serde(with = "ssz_types::serde_utils::hex_fixed_vec")]
    pub logs_bloom: FixedVector<u8, T::BytesPerLogsBloom>,
    pub prev_randao: Hash256,
    #[serde(with = "eth2_serde_utils::quoted_u64")]
    pub block_number: u64,
    #[serde(with = "eth2_serde_utils::quoted_u64")]
    pub gas_limit: u64,
    #[serde(with = "eth2_serde_utils::quoted_u64")]
    pub gas_used: u64,
    #[serde(with = "eth2_serde_utils::quoted_u64")]
    pub timestamp: u64,
    #[serde(with = "ssz_types::serde_utils::hex_var_list")]
    pub extra_data: VariableList<u8, T::MaxExtraDataBytes>,
    #[serde(with = "eth2_serde_utils::quoted_u256")]
    pub base_fee_per_gas: Uint256,
    pub block_hash: ExecutionBlockHash,
    pub transactions_root: Hash256,
    #[superstruct(only(Capella))]
    pub withdrawals_root: Hash256,
}

/*
// ref implementations (dangerious transparent clone)
impl<'a, T: EthSpec> From<&'a ExecutionPayload<T>> for ExecutionPayloadHeader<T> {
    fn from(payload_enum: &'a ExecutionPayload<T>) -> Self {
        match payload_enum {
            ExecutionPayload::Merge(payload) => {
                Self::Merge(ExecutionPayloadHeaderMerge {
                    parent_hash: payload.parent_hash,
                    fee_recipient: payload.fee_recipient,
                    state_root: payload.state_root,
                    receipts_root: payload.receipts_root,
                    logs_bloom: payload.logs_bloom.clone(),
                    prev_randao: payload.prev_randao,
                    block_number: payload.block_number,
                    gas_limit: payload.gas_limit,
                    gas_used: payload.gas_used,
                    timestamp: payload.timestamp,
                    extra_data: payload.extra_data.clone(),
                    base_fee_per_gas: payload.base_fee_per_gas,
                    block_hash: payload.block_hash,
                    transactions_root: payload.transactions.tree_hash_root(),
                })
            },
            ExecutionPayload::Capella(payload) => {
                Self::Capella(ExecutionPayloadHeaderCapella {
                    parent_hash: payload.parent_hash,
                    fee_recipient: payload.fee_recipient,
                    state_root: payload.state_root,
                    receipts_root: payload.receipts_root,
                    logs_bloom: payload.logs_bloom.clone(),
                    prev_randao: payload.prev_randao,
                    block_number: payload.block_number,
                    gas_limit: payload.gas_limit,
                    gas_used: payload.gas_used,
                    timestamp: payload.timestamp,
                    extra_data: payload.extra_data.clone(),
                    base_fee_per_gas: payload.base_fee_per_gas,
                    block_hash: payload.block_hash,
                    transactions_root: payload.transactions.tree_hash_root(),
                    withdrawals_root: payload.withdrawals.tree_hash_root(),
                })
            }
        }
    }
}

impl<'a, T: EthSpec> From<&'a ExecutionPayloadMerge<T>> for ExecutionPayloadHeaderMerge<T> {
    fn from(payload: &'a ExecutionPayloadMerge<T>) -> Self {
        Self {
            parent_hash: payload.parent_hash,
            fee_recipient: payload.fee_recipient,
            state_root: payload.state_root,
            receipts_root: payload.receipts_root,
            logs_bloom: payload.logs_bloom.clone(),
            prev_randao: payload.prev_randao,
            block_number: payload.block_number,
            gas_limit: payload.gas_limit,
            gas_used: payload.gas_used,
            timestamp: payload.timestamp,
            extra_data: payload.extra_data.clone(),
            base_fee_per_gas: payload.base_fee_per_gas,
            block_hash: payload.block_hash,
            transactions_root: payload.transactions.tree_hash_root(),
        }
    }
}

impl<'a, T: EthSpec> From<&'a ExecutionPayloadCapella<T>> for ExecutionPayloadHeaderCapella<T> {
    fn from(payload: &'a ExecutionPayloadCapella<T>) -> Self {
        Self {
            parent_hash: payload.parent_hash,
            fee_recipient: payload.fee_recipient,
            state_root: payload.state_root,
            receipts_root: payload.receipts_root,
            logs_bloom: payload.logs_bloom.clone(),
            prev_randao: payload.prev_randao,
            block_number: payload.block_number,
            gas_limit: payload.gas_limit,
            gas_used: payload.gas_used,
            timestamp: payload.timestamp,
            extra_data: payload.extra_data.clone(),
            base_fee_per_gas: payload.base_fee_per_gas,
            block_hash: payload.block_hash,
            transactions_root: payload.transactions.tree_hash_root(),
            withdrawals_root: payload.withdrawals.tree_hash_root(),
        }
    }
}
*/

impl<T: EthSpec> From<ExecutionPayloadMerge<T>> for ExecutionPayloadHeaderMerge<T> {
    fn from(payload: ExecutionPayloadMerge<T>) -> Self {
        Self {
            parent_hash: payload.parent_hash,
            fee_recipient: payload.fee_recipient,
            state_root: payload.state_root,
            receipts_root: payload.receipts_root,
            logs_bloom: payload.logs_bloom,
            prev_randao: payload.prev_randao,
            block_number: payload.block_number,
            gas_limit: payload.gas_limit,
            gas_used: payload.gas_used,
            timestamp: payload.timestamp,
            extra_data: payload.extra_data,
            base_fee_per_gas: payload.base_fee_per_gas,
            block_hash: payload.block_hash,
            transactions_root: payload.transactions.tree_hash_root(),
        }
    }
}

impl<T: EthSpec> From<ExecutionPayloadCapella<T>> for ExecutionPayloadHeaderCapella<T> {
    fn from(payload: ExecutionPayloadCapella<T>) -> Self {
        Self {
            parent_hash: payload.parent_hash,
            fee_recipient: payload.fee_recipient,
            state_root: payload.state_root,
            receipts_root: payload.receipts_root,
            logs_bloom: payload.logs_bloom,
            prev_randao: payload.prev_randao,
            block_number: payload.block_number,
            gas_limit: payload.gas_limit,
            gas_used: payload.gas_used,
            timestamp: payload.timestamp,
            extra_data: payload.extra_data,
            base_fee_per_gas: payload.base_fee_per_gas,
            block_hash: payload.block_hash,
            transactions_root: payload.transactions.tree_hash_root(),
            withdrawals_root: payload.withdrawals.tree_hash_root(),
        }
    }
}
