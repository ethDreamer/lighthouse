//! Vocabulary of the old `EthSpec` trait and the new `Spec` type.
//!
//! The associated-type and method names are taken from
//! `consensus/types/src/core/eth_spec.rs` (old) and the constant/method names
//! from `consensus/types/src/core/spec.rs` (new, from PR 9229).

use std::collections::HashSet;
use std::sync::OnceLock;

/// Constants defined on the new spec structs.
pub const CONSTS: &[&str] = &[
    "GENESIS_EPOCH",
    "JUSTIFICATION_BITS_LENGTH",
    "SUBNET_BITFIELD_LENGTH",
    "MAX_VALIDATORS_PER_COMMITTEE",
    "MAX_COMMITTEES_PER_SLOT",
    "MAX_VALIDATORS_PER_SLOT",
    "SLOTS_PER_EPOCH",
    "EPOCHS_PER_ETH1_VOTING_PERIOD",
    "SLOTS_PER_HISTORICAL_ROOT",
    "EPOCHS_PER_HISTORICAL_VECTOR",
    "EPOCHS_PER_SLASHINGS_VECTOR",
    "HISTORICAL_ROOTS_LIMIT",
    "VALIDATOR_REGISTRY_LIMIT",
    "BUILDER_PENDING_PAYMENTS_LIMIT",
    "MAX_PROPOSER_SLASHINGS",
    "MAX_ATTESTER_SLASHINGS",
    "MAX_ATTESTATIONS",
    "MAX_DEPOSITS",
    "MAX_VOLUNTARY_EXITS",
    "SYNC_COMMITTEE_SIZE",
    "SYNC_COMMITTEE_SUBNET_COUNT",
    "MAX_BYTES_PER_TRANSACTION",
    "MAX_TRANSACTIONS_PER_PAYLOAD",
    "BYTES_PER_LOGS_BLOOM",
    "GAS_LIMIT_DENOMINATOR",
    "MIN_GAS_LIMIT",
    "MAX_EXTRA_DATA_BYTES",
    "MAX_BLS_TO_EXECUTION_CHANGES",
    "MAX_WITHDRAWALS_PER_PAYLOAD",
    "MAX_BLOB_COMMITMENTS_PER_BLOCK",
    "BYTES_PER_FIELD_ELEMENT",
    "FIELD_ELEMENTS_PER_BLOB",
    "FIELD_ELEMENTS_PER_CELL",
    "FIELD_ELEMENTS_PER_EXT_BLOB",
    "BYTES_PER_BLOB",
    "BYTES_PER_CELL",
    "MAX_CELLS_PER_BLOCK",
    "KZG_COMMITMENT_INCLUSION_PROOF_DEPTH",
    "KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH",
    "CELLS_PER_EXT_BLOB",
    "NUMBER_OF_COLUMNS",
    "PROPOSER_LOOKAHEAD_SLOTS",
    "SYNC_SUBCOMMITTEE_SIZE",
    "MAX_PENDING_ATTESTATIONS",
    "SLOTS_PER_ETH1_VOTING_PERIOD",
    "PENDING_DEPOSITS_LIMIT",
    "PENDING_PARTIAL_WITHDRAWALS_LIMIT",
    "PENDING_CONSOLIDATIONS_LIMIT",
    "MAX_CONSOLIDATION_REQUESTS_PER_PAYLOAD",
    "MAX_DEPOSIT_REQUESTS_PER_PAYLOAD",
    "MAX_ATTESTER_SLASHINGS_ELECTRA",
    "MAX_ATTESTATIONS_ELECTRA",
    "MAX_WITHDRAWAL_REQUESTS_PER_PAYLOAD",
    "MAX_PENDING_DEPOSITS_PER_EPOCH",
    "PTC_SIZE",
    "PTC_WINDOW_LENGTH",
    "MAX_PAYLOAD_ATTESTATIONS",
    "MAX_BUILDERS_PER_WITHDRAWALS_SWEEP",
    "MAX_BUILDER_DEPOSIT_REQUESTS_PER_PAYLOAD",
    "MAX_BUILDER_EXIT_REQUESTS_PER_PAYLOAD",
    "INCLUSION_LIST_COMMITTEE_SIZE",
    "MAX_SIGNED_AGGREGATE_AND_PROOF_SIZE",
    "MAX_ATTESTER_SLASHING_SIZE",
    "MAX_DATA_COLUMN_SIDECAR_SIZE",
    "MAX_PARTIAL_DATA_COLUMN_SIDECAR_SIZE",
    "MAX_SIGNED_EXECUTION_PAYLOAD_BID_SIZE",
    "MAX_SIGNED_INCLUSION_LIST_SIZE",
    "PRESET_BASE",
    "SPEC_ID",
];

/// Methods that exist on the new spec structs (`impl_spec_methods!` and the
/// per-spec `default_spec`). Calls to these keep their call syntax.
pub const NEW_METHODS: &[&str] = &[
    "slots_per_epoch",
    "genesis_epoch",
    "slots_per_historical_root",
    "number_of_columns",
    "epochs_per_slashings_vector",
    "epochs_per_historical_vector",
    "sync_committee_size",
    "sync_subcommittee_size",
    "cells_per_ext_blob",
    "validator_registry_limit",
    "get_committee_count_per_slot",
    "minimum_validator_count",
    "kzg_commitments_tree_depth",
    "block_body_tree_depth",
    "payload_timely_threshold",
    "data_availability_timely_threshold",
    "default_spec",
];

/// Old trait methods whose name does not simply upper-case to a constant.
pub const METHOD_RENAMES: &[(&str, &str)] = &[
    ("kzg_proof_inclusion_proof_depth", "KZG_COMMITMENT_INCLUSION_PROOF_DEPTH"),
    ("spec_name", "SPEC_ID"),
    ("name", "PRESET_BASE"),
];

/// Old associated types whose upper-snake form does not match a constant.
pub const ASSOC_RENAMES: &[(&str, &str)] = &[("PTCSize", "PTC_SIZE")];

/// Old `EthSpec` associated type names (typenum types).
pub const ASSOC_TYPES: &[&str] = &[
    "GenesisEpoch",
    "JustificationBitsLength",
    "SubnetBitfieldLength",
    "MaxValidatorsPerCommittee",
    "MaxValidatorsPerSlot",
    "MaxCommitteesPerSlot",
    "SlotsPerEpoch",
    "SlotsPerHistoricalRoot",
    "EpochsPerHistoricalVector",
    "EpochsPerSlashingsVector",
    "HistoricalRootsLimit",
    "ValidatorRegistryLimit",
    "MaxProposerSlashings",
    "MaxAttesterSlashings",
    "MaxAttestations",
    "MaxDeposits",
    "MaxVoluntaryExits",
    "SyncCommitteeSize",
    "SyncCommitteeSubnetCount",
    "MaxBytesPerTransaction",
    "MaxTransactionsPerPayload",
    "BytesPerLogsBloom",
    "GasLimitDenominator",
    "MinGasLimit",
    "MaxExtraDataBytes",
    "MaxBlsToExecutionChanges",
    "MaxWithdrawalsPerPayload",
    "MaxBlobCommitmentsPerBlock",
    "FieldElementsPerBlob",
    "BytesPerFieldElement",
    "KzgCommitmentInclusionProofDepth",
    "FieldElementsPerCell",
    "FieldElementsPerExtBlob",
    "KzgCommitmentsInclusionProofDepth",
    "CellsPerExtBlob",
    "NumberOfColumns",
    "ProposerLookaheadSlots",
    "MaxPendingAttestations",
    "SyncSubcommitteeSize",
    "BytesPerBlob",
    "BytesPerCell",
    "MaxCellsPerBlock",
    "PendingDepositsLimit",
    "PendingPartialWithdrawalsLimit",
    "PendingConsolidationsLimit",
    "MaxConsolidationRequestsPerPayload",
    "MaxDepositRequestsPerPayload",
    "MaxAttesterSlashingsElectra",
    "MaxAttestationsElectra",
    "MaxWithdrawalRequestsPerPayload",
    "MaxPendingDepositsPerEpoch",
    "PTCSize",
    "PtcWindowLength",
    "MaxPayloadAttestations",
    "BuilderPendingPaymentsLimit",
    "MaxBuildersPerWithdrawalsSweep",
    "MaxBuilderDepositRequestsPerPayload",
    "MaxBuilderExitRequestsPerPayload",
    "InclusionListCommitteeSize",
    "EpochsPerEth1VotingPeriod",
    "SlotsPerEth1VotingPeriod",
];

/// Old concrete spec types, which become `Spec` (or vanish) in the new world.
pub const CONCRETE_SPECS: &[&str] = &["MainnetEthSpec", "MinimalEthSpec", "GnosisEthSpec"];

/// The conventional, unbounded spec type-parameter name used across Lighthouse.
pub const CONVENTIONAL_PARAM: &str = "E";

/// Old trait methods (taken from `eth_spec.rs`) — used only as evidence that an
/// unbounded parameter is a spec.
pub const OLD_METHODS: &[&str] = &[
    "default_spec",
    "spec_name",
    "genesis_epoch",
    "minimum_validator_count",
    "slots_per_epoch",
    "slots_per_historical_root",
    "epochs_per_historical_vector",
    "sync_committee_size",
    "sync_subcommittee_size",
    "max_bytes_per_transaction",
    "max_transactions_per_payload",
    "max_extra_data_bytes",
    "bytes_per_logs_bloom",
    "max_bls_to_execution_changes",
    "max_withdrawals_per_payload",
    "max_blob_commitments_per_block",
    "field_elements_per_blob",
    "field_elements_per_ext_blob",
    "field_elements_per_cell",
    "bytes_per_blob",
    "bytes_per_cell",
    "kzg_proof_inclusion_proof_depth",
    "kzg_commitments_tree_depth",
    "block_body_tree_depth",
    "pending_deposits_limit",
    "pending_partial_withdrawals_limit",
    "pending_consolidations_limit",
    "builder_pending_payments_limit",
    "max_consolidation_requests_per_payload",
    "max_deposit_requests_per_payload",
    "max_attester_slashings_electra",
    "max_attestations_electra",
    "max_withdrawal_requests_per_payload",
    "max_pending_deposits_per_epoch",
    "kzg_commitments_inclusion_proof_depth",
    "cells_per_ext_blob",
    "number_of_columns",
    "proposer_lookahead_slots",
    "ptc_size",
    "ptc_window_length",
    "max_payload_attestations",
    "max_builders_per_withdrawals_sweep",
    "max_builder_deposit_requests_per_payload",
    "max_builder_exit_requests_per_payload",
    "max_signed_aggregate_and_proof_size",
    "max_attester_slashing_size",
    "max_data_column_sidecar_size",
    "max_partial_data_column_sidecar_size",
    "max_signed_execution_payload_bid_size",
    "max_signed_inclusion_list_size",
    "payload_timely_threshold",
    "data_availability_timely_threshold",
    "inclusion_list_committee_size",
    "get_committee_count_per_slot",
];

fn consts() -> &'static HashSet<&'static str> {
    static S: OnceLock<HashSet<&'static str>> = OnceLock::new();
    S.get_or_init(|| CONSTS.iter().copied().collect())
}

fn assoc_types() -> &'static HashSet<&'static str> {
    static S: OnceLock<HashSet<&'static str>> = OnceLock::new();
    S.get_or_init(|| ASSOC_TYPES.iter().copied().collect())
}

fn new_methods() -> &'static HashSet<&'static str> {
    static S: OnceLock<HashSet<&'static str>> = OnceLock::new();
    S.get_or_init(|| NEW_METHODS.iter().copied().collect())
}

fn old_methods() -> &'static HashSet<&'static str> {
    static S: OnceLock<HashSet<&'static str>> = OnceLock::new();
    S.get_or_init(|| OLD_METHODS.iter().copied().collect())
}

pub fn is_assoc_type(name: &str) -> bool {
    assoc_types().contains(name)
}

pub fn is_old_method(name: &str) -> bool {
    old_methods().contains(name)
}

pub fn is_new_method(name: &str) -> bool {
    new_methods().contains(name)
}

pub fn is_concrete_spec(name: &str) -> bool {
    CONCRETE_SPECS.contains(&name)
}

/// `MaxValidatorsPerCommittee` -> `MAX_VALIDATORS_PER_COMMITTEE`.
pub fn camel_to_upper_snake(name: &str) -> String {
    let chars: Vec<char> = name.chars().collect();
    let mut out = String::new();
    for (i, c) in chars.iter().enumerate() {
        if c.is_uppercase() && i > 0 {
            let prev_lower = chars[i - 1].is_lowercase() || chars[i - 1].is_ascii_digit();
            let next_lower = chars.get(i + 1).map(|n| n.is_lowercase()).unwrap_or(false);
            if prev_lower || (chars[i - 1].is_uppercase() && next_lower) {
                out.push('_');
            }
        }
        out.push(c.to_ascii_uppercase());
    }
    out
}

/// Map an old associated type to the new constant name, if known.
pub fn assoc_to_const(name: &str) -> Option<String> {
    if let Some((_, c)) = ASSOC_RENAMES.iter().find(|(a, _)| *a == name) {
        return Some(c.to_string());
    }
    if !is_assoc_type(name) {
        return None;
    }
    let c = camel_to_upper_snake(name);
    if consts().contains(c.as_str()) {
        Some(c)
    } else {
        None
    }
}

/// What a call `E::method(...)` becomes.
pub enum MethodTarget {
    /// `Spec::method(...)` — a method exists on the new spec with a
    /// compatible return type.
    Method(String),
    /// `Spec::CONST` — a constant replaces the getter.
    Const(String),
    /// `Epoch::new(Spec::genesis_epoch())` — the old method returned `Epoch`.
    GenesisEpoch,
}

/// Old methods whose new counterpart has the same return type, so the call
/// syntax is kept.
const KEEP_CALL: &[&str] = &[
    "slots_per_epoch",
    "default_spec",
    "get_committee_count_per_slot",
    "minimum_validator_count",
    "kzg_commitments_tree_depth",
    "block_body_tree_depth",
    "payload_timely_threshold",
    "data_availability_timely_threshold",
];

/// What `E::method()` becomes, given the cast (if any) applied to the call.
/// `cast` is the target type of an enclosing `as` expression.
pub fn method_target_cast(method: &str, cast: Option<&str>) -> MethodTarget {
    if method == "genesis_epoch" {
        return MethodTarget::GenesisEpoch;
    }
    if method == "get_committee_count_per_slot_with" {
        return MethodTarget::Method("get_committee_count_per_slot".to_string());
    }
    if KEEP_CALL.contains(&method) {
        // `E::slots_per_epoch() as usize/u32/f64` -> the usize constant.
        if method == "slots_per_epoch" && cast.is_some() && cast != Some("u64") {
            return MethodTarget::Const("SLOTS_PER_EPOCH".to_string());
        }
        return MethodTarget::Method(method.to_string());
    }
    if let Some((_, c)) = METHOD_RENAMES.iter().find(|(m, _)| *m == method) {
        return MethodTarget::Const(c.to_string());
    }
    let upper = method.to_ascii_uppercase();
    if consts().contains(upper.as_str()) {
        // Old method returned usize. With `as u64` prefer the u64 method.
        if cast == Some("u64") && is_new_method(method) {
            return MethodTarget::Method(method.to_string());
        }
        MethodTarget::Const(upper)
    } else if is_new_method(method) {
        MethodTarget::Method(method.to_string())
    } else {
        // Unknown: keep as a method call and let the compiler complain.
        MethodTarget::Method(method.to_string())
    }
}

pub fn method_target(method: &str) -> MethodTarget {
    method_target_cast(method, None)
}

/// Methods on the new spec that return `u64`, used when translating
/// `E::Assoc::to_u64()`.
pub fn u64_method_for_const(konst: &str) -> Option<String> {
    let lower = konst.to_ascii_lowercase();
    if is_new_method(&lower) && lower != "default_spec" {
        Some(lower)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_assoc_types_map_to_consts() {
        for a in ASSOC_TYPES {
            assert!(assoc_to_const(a).is_some(), "no const for {a}");
        }
    }

    #[test]
    fn camel_case() {
        assert_eq!(camel_to_upper_snake("SlotsPerEpoch"), "SLOTS_PER_EPOCH");
        assert_eq!(camel_to_upper_snake("PTCSize"), "PTC_SIZE");
        assert_eq!(
            camel_to_upper_snake("KzgCommitmentInclusionProofDepth"),
            "KZG_COMMITMENT_INCLUSION_PROOF_DEPTH"
        );
        assert_eq!(camel_to_upper_snake("EpochsPerEth1VotingPeriod"), "EPOCHS_PER_ETH1_VOTING_PERIOD");
    }
}
