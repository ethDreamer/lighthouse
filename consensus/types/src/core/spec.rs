//! Compile-time spec selection via cfg features.
//!
//! - Default (no feature): `Spec = MainnetSpec`
//! - `--features spec-minimal`: `Spec = MinimalSpec`
//! - `--features spec-gnosis`: `Spec = GnosisSpec`

use crate::core::ChainSpec;
use safe_arith::{ArithError, SafeArith};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// Mainnet spec constants.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct MainnetSpec;

/// Minimal spec constants.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct MinimalSpec;

/// Gnosis spec constants.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct GnosisSpec;

const MAINNET: &str = "mainnet";
const MINIMAL: &str = "minimal";
const GNOSIS: &str = "gnosis";

/// Used to identify one of the `Spec` instances defined here.
#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(rename_all = "lowercase")]
pub enum SpecId {
    Mainnet,
    Minimal,
    Gnosis,
}

impl FromStr for SpecId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            MAINNET => Ok(SpecId::Mainnet),
            MINIMAL => Ok(SpecId::Minimal),
            GNOSIS => Ok(SpecId::Gnosis),
            _ => Err(format!("Unknown eth spec: {}", s)),
        }
    }
}

impl fmt::Display for SpecId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            SpecId::Mainnet => MAINNET,
            SpecId::Minimal => MINIMAL,
            SpecId::Gnosis => GNOSIS,
        };
        write!(f, "{}", s)
    }
}

/// All spec constants and their mainnet (default) values.
///
/// Single source of truth for the complete set of spec constants.
/// Each constant is listed exactly once with its mainnet value. Specs that
/// differ from mainnet provide overrides in `define_spec!`.
macro_rules! all_spec_consts {
    ($callback:ident, $($prefix:tt)*) => {
        $callback!(
            $($prefix)*
            GENESIS_EPOCH = 0,
            JUSTIFICATION_BITS_LENGTH = 4,
            SUBNET_BITFIELD_LENGTH = 64,
            MAX_VALIDATORS_PER_COMMITTEE = 2048,
            MAX_COMMITTEES_PER_SLOT = 64,
            MAX_VALIDATORS_PER_SLOT = 131072,
            SLOTS_PER_EPOCH = 32,
            EPOCHS_PER_ETH1_VOTING_PERIOD = 64,
            SLOTS_PER_HISTORICAL_ROOT = 8192,
            EPOCHS_PER_HISTORICAL_VECTOR = 65536,
            EPOCHS_PER_SLASHINGS_VECTOR = 8192,
            HISTORICAL_ROOTS_LIMIT = 16777216,
            VALIDATOR_REGISTRY_LIMIT = 1099511627776,
            BUILDER_PENDING_PAYMENTS_LIMIT = 64,
            MAX_PROPOSER_SLASHINGS = 16,
            MAX_ATTESTER_SLASHINGS = 2,
            MAX_ATTESTATIONS = 128,
            MAX_DEPOSITS = 16,
            MAX_VOLUNTARY_EXITS = 16,
            SYNC_COMMITTEE_SIZE = 512,
            SYNC_COMMITTEE_SUBNET_COUNT = 4,
            MAX_BYTES_PER_TRANSACTION = 1073741824,
            MAX_TRANSACTIONS_PER_PAYLOAD = 1048576,
            BYTES_PER_LOGS_BLOOM = 256,
            GAS_LIMIT_DENOMINATOR = 1024,
            MIN_GAS_LIMIT = 5000,
            MAX_EXTRA_DATA_BYTES = 32,
            MAX_BLS_TO_EXECUTION_CHANGES = 16,
            MAX_WITHDRAWALS_PER_PAYLOAD = 16,
            MAX_BLOB_COMMITMENTS_PER_BLOCK = 4096,
            BYTES_PER_FIELD_ELEMENT = 32,
            FIELD_ELEMENTS_PER_BLOB = 4096,
            FIELD_ELEMENTS_PER_CELL = 64,
            FIELD_ELEMENTS_PER_EXT_BLOB = 8192,
            BYTES_PER_BLOB = 131072,
            BYTES_PER_CELL = 2048,
            MAX_CELLS_PER_BLOCK = 33554432,
            KZG_COMMITMENT_INCLUSION_PROOF_DEPTH = 17,
            KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH = 4,
            CELLS_PER_EXT_BLOB = 128,
            NUMBER_OF_COLUMNS = 128,
            PROPOSER_LOOKAHEAD_SLOTS = 64,
            SYNC_SUBCOMMITTEE_SIZE = 128,
            MAX_PENDING_ATTESTATIONS = 4096,
            SLOTS_PER_ETH1_VOTING_PERIOD = 2048,
            PENDING_DEPOSITS_LIMIT = 134217728,
            PENDING_PARTIAL_WITHDRAWALS_LIMIT = 134217728,
            PENDING_CONSOLIDATIONS_LIMIT = 262144,
            MAX_CONSOLIDATION_REQUESTS_PER_PAYLOAD = 2,
            MAX_DEPOSIT_REQUESTS_PER_PAYLOAD = 8192,
            MAX_ATTESTER_SLASHINGS_ELECTRA = 1,
            MAX_ATTESTATIONS_ELECTRA = 8,
            MAX_WITHDRAWAL_REQUESTS_PER_PAYLOAD = 16,
            MAX_PENDING_DEPOSITS_PER_EPOCH = 16,
            PTC_SIZE = 512,
            PTC_WINDOW_LENGTH = 96,
            MAX_PAYLOAD_ATTESTATIONS = 4,
            MAX_BUILDERS_PER_WITHDRAWALS_SWEEP = 16384,
            MAX_BUILDER_DEPOSIT_REQUESTS_PER_PAYLOAD = 64,
            MAX_BUILDER_EXIT_REQUESTS_PER_PAYLOAD = 16,
            INCLUSION_LIST_COMMITTEE_SIZE = 16,
            MAX_SIGNED_AGGREGATE_AND_PROOF_SIZE = 16829,
            MAX_ATTESTER_SLASHING_SIZE = 2097616,
            MAX_SIGNED_EXECUTION_PAYLOAD_BID_SIZE = 196932,
            MAX_SIGNED_INCLUSION_LIST_SIZE = 41112,
        );
    };
}

/// For each constant, resolve its value by calling the `__value_for!`
/// macro (defined by `define_spec!`) which checks for an override
/// before falling back to the mainnet default.
macro_rules! __define_impl {
    (
        $name:ident, $spec_name:expr, $spec_id:expr,
        $($const_name:ident = $default:expr),* $(,)?
    ) => {
        impl $name {
            /// The preset base name (e.g. `"mainnet"`, `"minimal"`, `"gnosis"`).
            pub const PRESET_BASE: &'static str = $spec_name;

            /// The spec identifier enum variant.
            pub const SPEC_ID: SpecId = $spec_id;

            $(pub const $const_name: usize = __value_for!($const_name, $default);)*
        }
    };
}

/// Define all spec constants on a concrete struct.
///
/// Constants not listed in the overrides block inherit mainnet default values
/// from `all_spec_consts!`. For mainnet itself, the overrides block is empty.
macro_rules! define_spec {
    (
        $name:ident,
        $spec_name:expr,
        $spec_id:expr,
        { $($override_name:ident = $override_val:expr),* $(,)? }
    ) => {
        macro_rules! __value_for {
            // One arm per override: literal ident match takes priority.
            $(($override_name, $_d:expr) => { $override_val };)*
            // Fallback: use the mainnet default from all_spec_consts.
            ($other:ident, $default:expr) => { $default };
        }

        all_spec_consts!(__define_impl, $name, $spec_name, $spec_id,);
    };
}

// Spec definitions including overrides when const values differ from mainnet.
define_spec!(MainnetSpec, MAINNET, SpecId::Mainnet, {});

define_spec!(MinimalSpec, MINIMAL, SpecId::Minimal, {
    MAX_COMMITTEES_PER_SLOT = 4,
    MAX_VALIDATORS_PER_SLOT = 8192,
    SLOTS_PER_EPOCH = 8,
    EPOCHS_PER_ETH1_VOTING_PERIOD = 4,
    SLOTS_PER_HISTORICAL_ROOT = 64,
    EPOCHS_PER_HISTORICAL_VECTOR = 64,
    EPOCHS_PER_SLASHINGS_VECTOR = 64,
    SYNC_COMMITTEE_SIZE = 32,
    SYNC_SUBCOMMITTEE_SIZE = 8,
    MAX_PENDING_ATTESTATIONS = 1024,
    SLOTS_PER_ETH1_VOTING_PERIOD = 32,
    MAX_WITHDRAWALS_PER_PAYLOAD = 4,
    PENDING_PARTIAL_WITHDRAWALS_LIMIT = 64,
    PENDING_CONSOLIDATIONS_LIMIT = 64,
    PROPOSER_LOOKAHEAD_SLOTS = 16,
    BUILDER_PENDING_PAYMENTS_LIMIT = 16,
    PTC_SIZE = 16,
    PTC_WINDOW_LENGTH = 24,
    MAX_BUILDERS_PER_WITHDRAWALS_SWEEP = 16,
    MAX_SIGNED_AGGREGATE_AND_PROOF_SIZE = 1462,
    MAX_ATTESTER_SLASHING_SIZE = 131536,
});

define_spec!(GnosisSpec, GNOSIS, SpecId::Gnosis, {
    SLOTS_PER_EPOCH = 16,
    BUILDER_PENDING_PAYMENTS_LIMIT = 32,
    MAX_WITHDRAWALS_PER_PAYLOAD = 8,
    MAX_PENDING_ATTESTATIONS = 2048,
    SLOTS_PER_ETH1_VOTING_PERIOD = 1024,
    PROPOSER_LOOKAHEAD_SLOTS = 32,
    MAX_PAYLOAD_ATTESTATIONS = 2,
    PTC_WINDOW_LENGTH = 48,
});

/// Implement commonly-used derived helpers on a spec struct.
macro_rules! impl_spec_methods {
    ($name:ident) => {
        impl $name {
            /// `SLOTS_PER_EPOCH` as `u64`.
            pub const fn slots_per_epoch() -> u64 {
                Self::SLOTS_PER_EPOCH as u64
            }

            /// `GENESIS_EPOCH` as `u64`.
            pub const fn genesis_epoch() -> u64 {
                Self::GENESIS_EPOCH as u64
            }

            /// `SLOTS_PER_HISTORICAL_ROOT` as `u64`.
            pub const fn slots_per_historical_root() -> u64 {
                Self::SLOTS_PER_HISTORICAL_ROOT as u64
            }

            /// `NUMBER_OF_COLUMNS` as `u64`.
            pub const fn number_of_columns() -> u64 {
                Self::NUMBER_OF_COLUMNS as u64
            }

            /// `EPOCHS_PER_SLASHINGS_VECTOR` as `u64`.
            pub const fn epochs_per_slashings_vector() -> u64 {
                Self::EPOCHS_PER_SLASHINGS_VECTOR as u64
            }

            /// `EPOCHS_PER_HISTORICAL_VECTOR` as `u64`.
            pub const fn epochs_per_historical_vector() -> u64 {
                Self::EPOCHS_PER_HISTORICAL_VECTOR as u64
            }

            /// `SYNC_COMMITTEE_SIZE` as `u64`.
            pub const fn sync_committee_size() -> u64 {
                Self::SYNC_COMMITTEE_SIZE as u64
            }

            /// `SYNC_SUBCOMMITTEE_SIZE` as `u64`.
            pub const fn sync_subcommittee_size() -> u64 {
                Self::SYNC_SUBCOMMITTEE_SIZE as u64
            }

            /// `CELLS_PER_EXT_BLOB` as `u64`.
            pub const fn cells_per_ext_blob() -> u64 {
                Self::CELLS_PER_EXT_BLOB as u64
            }

            /// `VALIDATOR_REGISTRY_LIMIT` as `u64`.
            pub const fn validator_registry_limit() -> u64 {
                Self::VALIDATOR_REGISTRY_LIMIT as u64
            }

            /// Returns the number of committees per slot for the given parameters.
            pub fn get_committee_count_per_slot(
                active_validator_count: usize,
                max_committees_per_slot: usize,
                target_committee_size: usize,
            ) -> Result<usize, ArithError> {
                Ok(std::cmp::max(
                    1,
                    std::cmp::min(
                        max_committees_per_slot,
                        active_validator_count
                            .safe_div(Self::SLOTS_PER_EPOCH)?
                            .safe_div(target_committee_size)?,
                    ),
                ))
            }

            /// Minimum number of validators required.
            pub fn minimum_validator_count() -> usize {
                Self::SLOTS_PER_EPOCH
            }

            /// Depth of the subtree in block body for KZG commitments.
            pub fn kzg_commitments_tree_depth() -> usize {
                (Self::MAX_BLOB_COMMITMENTS_PER_BLOCK
                    .next_power_of_two()
                    .ilog2() as usize)
                    .safe_add(1)
                    .expect("The log of max_blob_commitments_per_block can not overflow")
            }

            /// Depth of the block body tree.
            pub fn block_body_tree_depth() -> usize {
                Self::KZG_COMMITMENT_INCLUSION_PROOF_DEPTH
                    .safe_sub(Self::kzg_commitments_tree_depth())
                    .expect("Preset values are not configurable and never result in non-positive block body depth")
            }

            /// Returns the `PAYLOAD_TIMELY_THRESHOLD` constant (PTC_SIZE / 2).
            pub fn payload_timely_threshold() -> usize {
                Self::PTC_SIZE / 2
            }

            /// Returns the `DATA_AVAILABILITY_TIMELY_THRESHOLD` constant (PTC_SIZE / 2).
            pub fn data_availability_timely_threshold() -> usize {
                Self::PTC_SIZE / 2
            }
        }
    };
}

impl_spec_methods!(MainnetSpec);
impl_spec_methods!(MinimalSpec);
impl_spec_methods!(GnosisSpec);

impl MainnetSpec {
    /// Returns the default `ChainSpec` for mainnet.
    pub fn default_spec() -> ChainSpec {
        ChainSpec::mainnet()
    }
}

impl MinimalSpec {
    /// Returns the default `ChainSpec` for minimal.
    pub fn default_spec() -> ChainSpec {
        ChainSpec::minimal()
    }
}

impl GnosisSpec {
    /// Returns the default `ChainSpec` for gnosis.
    pub fn default_spec() -> ChainSpec {
        ChainSpec::gnosis()
    }
}

// Ensure at most one spec feature is active.
#[cfg(all(feature = "spec-minimal", feature = "spec-gnosis"))]
compile_error!(
    "Features `spec-minimal` and `spec-gnosis` are mutually exclusive. \
     Please enable at most one."
);

#[cfg(all(
    feature = "spec-non-mainnet",
    not(any(feature = "spec-minimal", feature = "spec-gnosis"))
))]
compile_error!(
    "Feature `spec-non-mainnet` is an internal helper. Enable `spec-minimal` or \
     `spec-gnosis` to select a concrete non-mainnet spec."
);

/// The spec type selected at compile time.
///
/// - Default: `MainnetSpec`
/// - `--features spec-minimal`: `MinimalSpec`
/// - `--features spec-gnosis`: `GnosisSpec`
#[cfg(not(any(feature = "spec-minimal", feature = "spec-gnosis")))]
pub type Spec = MainnetSpec;

#[cfg(feature = "spec-minimal")]
pub type Spec = MinimalSpec;

#[cfg(feature = "spec-gnosis")]
pub type Spec = GnosisSpec;

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn assert_valid_spec() {
        let spec = Spec::default_spec();

        // Derived helpers must not panic.
        Spec::kzg_commitments_tree_depth();
        Spec::block_body_tree_depth();

        const {
            assert!(
                Spec::MAX_VALIDATORS_PER_SLOT >= Spec::MAX_VALIDATORS_PER_COMMITTEE,
                "MAX_VALIDATORS_PER_SLOT must be >= MAX_VALIDATORS_PER_COMMITTEE",
            );
        }

        assert_eq!(
            Spec::PROPOSER_LOOKAHEAD_SLOTS,
            (spec.min_seed_lookahead.as_usize() + 1) * Spec::SLOTS_PER_EPOCH,
            "PROPOSER_LOOKAHEAD_SLOTS must equal (MIN_SEED_LOOKAHEAD + 1) * SLOTS_PER_EPOCH"
        );

        assert_eq!(
            Spec::PTC_WINDOW_LENGTH,
            (spec.min_seed_lookahead.as_usize() + 2) * Spec::SLOTS_PER_EPOCH,
            "PTC_WINDOW_LENGTH must equal (2 + MIN_SEED_LOOKAHEAD) * SLOTS_PER_EPOCH"
        );
    }
}

/// While `EthSpec` and `Spec` coexist, the new constant table must agree with
/// the old trait for every preset. Removed together with `eth_spec.rs`.
#[cfg(test)]
mod equivalence {
    use super::*;
    use crate::core::{EthSpec, GnosisEthSpec, MainnetEthSpec, MinimalEthSpec};
    use ssz_types::typenum::Unsigned;

    macro_rules! check_table {
        ($old:ty, $new:ty) => {
            assert_eq!(<$old as EthSpec>::GenesisEpoch::to_usize(), <$new>::GENESIS_EPOCH, "GENESIS_EPOCH");
            assert_eq!(<$old as EthSpec>::JustificationBitsLength::to_usize(), <$new>::JUSTIFICATION_BITS_LENGTH, "JUSTIFICATION_BITS_LENGTH");
            assert_eq!(<$old as EthSpec>::SubnetBitfieldLength::to_usize(), <$new>::SUBNET_BITFIELD_LENGTH, "SUBNET_BITFIELD_LENGTH");
            assert_eq!(<$old as EthSpec>::MaxValidatorsPerCommittee::to_usize(), <$new>::MAX_VALIDATORS_PER_COMMITTEE, "MAX_VALIDATORS_PER_COMMITTEE");
            assert_eq!(<$old as EthSpec>::MaxValidatorsPerSlot::to_usize(), <$new>::MAX_VALIDATORS_PER_SLOT, "MAX_VALIDATORS_PER_SLOT");
            assert_eq!(<$old as EthSpec>::MaxCommitteesPerSlot::to_usize(), <$new>::MAX_COMMITTEES_PER_SLOT, "MAX_COMMITTEES_PER_SLOT");
            assert_eq!(<$old as EthSpec>::SlotsPerEpoch::to_usize(), <$new>::SLOTS_PER_EPOCH, "SLOTS_PER_EPOCH");
            assert_eq!(<$old as EthSpec>::SlotsPerHistoricalRoot::to_usize(), <$new>::SLOTS_PER_HISTORICAL_ROOT, "SLOTS_PER_HISTORICAL_ROOT");
            assert_eq!(<$old as EthSpec>::EpochsPerHistoricalVector::to_usize(), <$new>::EPOCHS_PER_HISTORICAL_VECTOR, "EPOCHS_PER_HISTORICAL_VECTOR");
            assert_eq!(<$old as EthSpec>::EpochsPerSlashingsVector::to_usize(), <$new>::EPOCHS_PER_SLASHINGS_VECTOR, "EPOCHS_PER_SLASHINGS_VECTOR");
            assert_eq!(<$old as EthSpec>::HistoricalRootsLimit::to_usize(), <$new>::HISTORICAL_ROOTS_LIMIT, "HISTORICAL_ROOTS_LIMIT");
            assert_eq!(<$old as EthSpec>::ValidatorRegistryLimit::to_usize(), <$new>::VALIDATOR_REGISTRY_LIMIT, "VALIDATOR_REGISTRY_LIMIT");
            assert_eq!(<$old as EthSpec>::MaxProposerSlashings::to_usize(), <$new>::MAX_PROPOSER_SLASHINGS, "MAX_PROPOSER_SLASHINGS");
            assert_eq!(<$old as EthSpec>::MaxAttesterSlashings::to_usize(), <$new>::MAX_ATTESTER_SLASHINGS, "MAX_ATTESTER_SLASHINGS");
            assert_eq!(<$old as EthSpec>::MaxAttestations::to_usize(), <$new>::MAX_ATTESTATIONS, "MAX_ATTESTATIONS");
            assert_eq!(<$old as EthSpec>::MaxDeposits::to_usize(), <$new>::MAX_DEPOSITS, "MAX_DEPOSITS");
            assert_eq!(<$old as EthSpec>::MaxVoluntaryExits::to_usize(), <$new>::MAX_VOLUNTARY_EXITS, "MAX_VOLUNTARY_EXITS");
            assert_eq!(<$old as EthSpec>::SyncCommitteeSize::to_usize(), <$new>::SYNC_COMMITTEE_SIZE, "SYNC_COMMITTEE_SIZE");
            assert_eq!(<$old as EthSpec>::SyncCommitteeSubnetCount::to_usize(), <$new>::SYNC_COMMITTEE_SUBNET_COUNT, "SYNC_COMMITTEE_SUBNET_COUNT");
            assert_eq!(<$old as EthSpec>::MaxBytesPerTransaction::to_usize(), <$new>::MAX_BYTES_PER_TRANSACTION, "MAX_BYTES_PER_TRANSACTION");
            assert_eq!(<$old as EthSpec>::MaxTransactionsPerPayload::to_usize(), <$new>::MAX_TRANSACTIONS_PER_PAYLOAD, "MAX_TRANSACTIONS_PER_PAYLOAD");
            assert_eq!(<$old as EthSpec>::BytesPerLogsBloom::to_usize(), <$new>::BYTES_PER_LOGS_BLOOM, "BYTES_PER_LOGS_BLOOM");
            assert_eq!(<$old as EthSpec>::GasLimitDenominator::to_usize(), <$new>::GAS_LIMIT_DENOMINATOR, "GAS_LIMIT_DENOMINATOR");
            assert_eq!(<$old as EthSpec>::MinGasLimit::to_usize(), <$new>::MIN_GAS_LIMIT, "MIN_GAS_LIMIT");
            assert_eq!(<$old as EthSpec>::MaxExtraDataBytes::to_usize(), <$new>::MAX_EXTRA_DATA_BYTES, "MAX_EXTRA_DATA_BYTES");
            assert_eq!(<$old as EthSpec>::MaxBlsToExecutionChanges::to_usize(), <$new>::MAX_BLS_TO_EXECUTION_CHANGES, "MAX_BLS_TO_EXECUTION_CHANGES");
            assert_eq!(<$old as EthSpec>::MaxWithdrawalsPerPayload::to_usize(), <$new>::MAX_WITHDRAWALS_PER_PAYLOAD, "MAX_WITHDRAWALS_PER_PAYLOAD");
            assert_eq!(<$old as EthSpec>::MaxBlobCommitmentsPerBlock::to_usize(), <$new>::MAX_BLOB_COMMITMENTS_PER_BLOCK, "MAX_BLOB_COMMITMENTS_PER_BLOCK");
            assert_eq!(<$old as EthSpec>::FieldElementsPerBlob::to_usize(), <$new>::FIELD_ELEMENTS_PER_BLOB, "FIELD_ELEMENTS_PER_BLOB");
            assert_eq!(<$old as EthSpec>::BytesPerFieldElement::to_usize(), <$new>::BYTES_PER_FIELD_ELEMENT, "BYTES_PER_FIELD_ELEMENT");
            assert_eq!(<$old as EthSpec>::KzgCommitmentInclusionProofDepth::to_usize(), <$new>::KZG_COMMITMENT_INCLUSION_PROOF_DEPTH, "KZG_COMMITMENT_INCLUSION_PROOF_DEPTH");
            assert_eq!(<$old as EthSpec>::FieldElementsPerCell::to_usize(), <$new>::FIELD_ELEMENTS_PER_CELL, "FIELD_ELEMENTS_PER_CELL");
            assert_eq!(<$old as EthSpec>::FieldElementsPerExtBlob::to_usize(), <$new>::FIELD_ELEMENTS_PER_EXT_BLOB, "FIELD_ELEMENTS_PER_EXT_BLOB");
            assert_eq!(<$old as EthSpec>::KzgCommitmentsInclusionProofDepth::to_usize(), <$new>::KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH, "KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH");
            assert_eq!(<$old as EthSpec>::CellsPerExtBlob::to_usize(), <$new>::CELLS_PER_EXT_BLOB, "CELLS_PER_EXT_BLOB");
            assert_eq!(<$old as EthSpec>::NumberOfColumns::to_usize(), <$new>::NUMBER_OF_COLUMNS, "NUMBER_OF_COLUMNS");
            assert_eq!(<$old as EthSpec>::ProposerLookaheadSlots::to_usize(), <$new>::PROPOSER_LOOKAHEAD_SLOTS, "PROPOSER_LOOKAHEAD_SLOTS");
            assert_eq!(<$old as EthSpec>::MaxPendingAttestations::to_usize(), <$new>::MAX_PENDING_ATTESTATIONS, "MAX_PENDING_ATTESTATIONS");
            assert_eq!(<$old as EthSpec>::SyncSubcommitteeSize::to_usize(), <$new>::SYNC_SUBCOMMITTEE_SIZE, "SYNC_SUBCOMMITTEE_SIZE");
            assert_eq!(<$old as EthSpec>::BytesPerBlob::to_usize(), <$new>::BYTES_PER_BLOB, "BYTES_PER_BLOB");
            assert_eq!(<$old as EthSpec>::BytesPerCell::to_usize(), <$new>::BYTES_PER_CELL, "BYTES_PER_CELL");
            assert_eq!(<$old as EthSpec>::MaxCellsPerBlock::to_usize(), <$new>::MAX_CELLS_PER_BLOCK, "MAX_CELLS_PER_BLOCK");
            assert_eq!(<$old as EthSpec>::PendingDepositsLimit::to_usize(), <$new>::PENDING_DEPOSITS_LIMIT, "PENDING_DEPOSITS_LIMIT");
            assert_eq!(<$old as EthSpec>::PendingPartialWithdrawalsLimit::to_usize(), <$new>::PENDING_PARTIAL_WITHDRAWALS_LIMIT, "PENDING_PARTIAL_WITHDRAWALS_LIMIT");
            assert_eq!(<$old as EthSpec>::PendingConsolidationsLimit::to_usize(), <$new>::PENDING_CONSOLIDATIONS_LIMIT, "PENDING_CONSOLIDATIONS_LIMIT");
            assert_eq!(<$old as EthSpec>::MaxConsolidationRequestsPerPayload::to_usize(), <$new>::MAX_CONSOLIDATION_REQUESTS_PER_PAYLOAD, "MAX_CONSOLIDATION_REQUESTS_PER_PAYLOAD");
            assert_eq!(<$old as EthSpec>::MaxDepositRequestsPerPayload::to_usize(), <$new>::MAX_DEPOSIT_REQUESTS_PER_PAYLOAD, "MAX_DEPOSIT_REQUESTS_PER_PAYLOAD");
            assert_eq!(<$old as EthSpec>::MaxAttesterSlashingsElectra::to_usize(), <$new>::MAX_ATTESTER_SLASHINGS_ELECTRA, "MAX_ATTESTER_SLASHINGS_ELECTRA");
            assert_eq!(<$old as EthSpec>::MaxAttestationsElectra::to_usize(), <$new>::MAX_ATTESTATIONS_ELECTRA, "MAX_ATTESTATIONS_ELECTRA");
            assert_eq!(<$old as EthSpec>::MaxWithdrawalRequestsPerPayload::to_usize(), <$new>::MAX_WITHDRAWAL_REQUESTS_PER_PAYLOAD, "MAX_WITHDRAWAL_REQUESTS_PER_PAYLOAD");
            assert_eq!(<$old as EthSpec>::MaxPendingDepositsPerEpoch::to_usize(), <$new>::MAX_PENDING_DEPOSITS_PER_EPOCH, "MAX_PENDING_DEPOSITS_PER_EPOCH");
            assert_eq!(<$old as EthSpec>::PTCSize::to_usize(), <$new>::PTC_SIZE, "PTC_SIZE");
            assert_eq!(<$old as EthSpec>::PtcWindowLength::to_usize(), <$new>::PTC_WINDOW_LENGTH, "PTC_WINDOW_LENGTH");
            assert_eq!(<$old as EthSpec>::MaxPayloadAttestations::to_usize(), <$new>::MAX_PAYLOAD_ATTESTATIONS, "MAX_PAYLOAD_ATTESTATIONS");
            assert_eq!(<$old as EthSpec>::BuilderPendingPaymentsLimit::to_usize(), <$new>::BUILDER_PENDING_PAYMENTS_LIMIT, "BUILDER_PENDING_PAYMENTS_LIMIT");
            assert_eq!(<$old as EthSpec>::MaxBuildersPerWithdrawalsSweep::to_usize(), <$new>::MAX_BUILDERS_PER_WITHDRAWALS_SWEEP, "MAX_BUILDERS_PER_WITHDRAWALS_SWEEP");
            assert_eq!(<$old as EthSpec>::MaxBuilderDepositRequestsPerPayload::to_usize(), <$new>::MAX_BUILDER_DEPOSIT_REQUESTS_PER_PAYLOAD, "MAX_BUILDER_DEPOSIT_REQUESTS_PER_PAYLOAD");
            assert_eq!(<$old as EthSpec>::MaxBuilderExitRequestsPerPayload::to_usize(), <$new>::MAX_BUILDER_EXIT_REQUESTS_PER_PAYLOAD, "MAX_BUILDER_EXIT_REQUESTS_PER_PAYLOAD");
            assert_eq!(<$old as EthSpec>::InclusionListCommitteeSize::to_usize(), <$new>::INCLUSION_LIST_COMMITTEE_SIZE, "INCLUSION_LIST_COMMITTEE_SIZE");
            assert_eq!(<$old as EthSpec>::EpochsPerEth1VotingPeriod::to_usize(), <$new>::EPOCHS_PER_ETH1_VOTING_PERIOD, "EPOCHS_PER_ETH1_VOTING_PERIOD");
            assert_eq!(<$old as EthSpec>::SlotsPerEth1VotingPeriod::to_usize(), <$new>::SLOTS_PER_ETH1_VOTING_PERIOD, "SLOTS_PER_ETH1_VOTING_PERIOD");
            assert_eq!(<$old as EthSpec>::builder_pending_payments_limit() as usize, <$new>::BUILDER_PENDING_PAYMENTS_LIMIT as usize, "builder_pending_payments_limit");
            assert_eq!(<$old as EthSpec>::bytes_per_blob() as usize, <$new>::BYTES_PER_BLOB as usize, "bytes_per_blob");
            assert_eq!(<$old as EthSpec>::bytes_per_cell() as usize, <$new>::BYTES_PER_CELL as usize, "bytes_per_cell");
            assert_eq!(<$old as EthSpec>::bytes_per_logs_bloom() as usize, <$new>::BYTES_PER_LOGS_BLOOM as usize, "bytes_per_logs_bloom");
            assert_eq!(<$old as EthSpec>::field_elements_per_blob() as usize, <$new>::FIELD_ELEMENTS_PER_BLOB as usize, "field_elements_per_blob");
            assert_eq!(<$old as EthSpec>::field_elements_per_cell() as usize, <$new>::FIELD_ELEMENTS_PER_CELL as usize, "field_elements_per_cell");
            assert_eq!(<$old as EthSpec>::field_elements_per_ext_blob() as usize, <$new>::FIELD_ELEMENTS_PER_EXT_BLOB as usize, "field_elements_per_ext_blob");
            assert_eq!(<$old as EthSpec>::inclusion_list_committee_size() as usize, <$new>::INCLUSION_LIST_COMMITTEE_SIZE as usize, "inclusion_list_committee_size");
            assert_eq!(<$old as EthSpec>::kzg_commitments_inclusion_proof_depth() as usize, <$new>::KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH as usize, "kzg_commitments_inclusion_proof_depth");
            assert_eq!(<$old as EthSpec>::max_attestations_electra() as usize, <$new>::MAX_ATTESTATIONS_ELECTRA as usize, "max_attestations_electra");
            assert_eq!(<$old as EthSpec>::max_attester_slashing_size() as usize, <$new>::MAX_ATTESTER_SLASHING_SIZE as usize, "max_attester_slashing_size");
            assert_eq!(<$old as EthSpec>::max_attester_slashings_electra() as usize, <$new>::MAX_ATTESTER_SLASHINGS_ELECTRA as usize, "max_attester_slashings_electra");
            assert_eq!(<$old as EthSpec>::max_blob_commitments_per_block() as usize, <$new>::MAX_BLOB_COMMITMENTS_PER_BLOCK as usize, "max_blob_commitments_per_block");
            assert_eq!(<$old as EthSpec>::max_bls_to_execution_changes() as usize, <$new>::MAX_BLS_TO_EXECUTION_CHANGES as usize, "max_bls_to_execution_changes");
            assert_eq!(<$old as EthSpec>::max_builder_deposit_requests_per_payload() as usize, <$new>::MAX_BUILDER_DEPOSIT_REQUESTS_PER_PAYLOAD as usize, "max_builder_deposit_requests_per_payload");
            assert_eq!(<$old as EthSpec>::max_builder_exit_requests_per_payload() as usize, <$new>::MAX_BUILDER_EXIT_REQUESTS_PER_PAYLOAD as usize, "max_builder_exit_requests_per_payload");
            assert_eq!(<$old as EthSpec>::max_builders_per_withdrawals_sweep() as usize, <$new>::MAX_BUILDERS_PER_WITHDRAWALS_SWEEP as usize, "max_builders_per_withdrawals_sweep");
            assert_eq!(<$old as EthSpec>::max_bytes_per_transaction() as usize, <$new>::MAX_BYTES_PER_TRANSACTION as usize, "max_bytes_per_transaction");
            assert_eq!(<$old as EthSpec>::max_consolidation_requests_per_payload() as usize, <$new>::MAX_CONSOLIDATION_REQUESTS_PER_PAYLOAD as usize, "max_consolidation_requests_per_payload");
            assert_eq!(<$old as EthSpec>::max_deposit_requests_per_payload() as usize, <$new>::MAX_DEPOSIT_REQUESTS_PER_PAYLOAD as usize, "max_deposit_requests_per_payload");
            assert_eq!(<$old as EthSpec>::max_extra_data_bytes() as usize, <$new>::MAX_EXTRA_DATA_BYTES as usize, "max_extra_data_bytes");
            assert_eq!(<$old as EthSpec>::max_payload_attestations() as usize, <$new>::MAX_PAYLOAD_ATTESTATIONS as usize, "max_payload_attestations");
            assert_eq!(<$old as EthSpec>::max_pending_deposits_per_epoch() as usize, <$new>::MAX_PENDING_DEPOSITS_PER_EPOCH as usize, "max_pending_deposits_per_epoch");
            assert_eq!(<$old as EthSpec>::max_signed_aggregate_and_proof_size() as usize, <$new>::MAX_SIGNED_AGGREGATE_AND_PROOF_SIZE as usize, "max_signed_aggregate_and_proof_size");
            assert_eq!(<$old as EthSpec>::max_signed_execution_payload_bid_size() as usize, <$new>::MAX_SIGNED_EXECUTION_PAYLOAD_BID_SIZE as usize, "max_signed_execution_payload_bid_size");
            assert_eq!(<$old as EthSpec>::max_signed_inclusion_list_size() as usize, <$new>::MAX_SIGNED_INCLUSION_LIST_SIZE as usize, "max_signed_inclusion_list_size");
            assert_eq!(<$old as EthSpec>::max_transactions_per_payload() as usize, <$new>::MAX_TRANSACTIONS_PER_PAYLOAD as usize, "max_transactions_per_payload");
            assert_eq!(<$old as EthSpec>::max_withdrawal_requests_per_payload() as usize, <$new>::MAX_WITHDRAWAL_REQUESTS_PER_PAYLOAD as usize, "max_withdrawal_requests_per_payload");
            assert_eq!(<$old as EthSpec>::max_withdrawals_per_payload() as usize, <$new>::MAX_WITHDRAWALS_PER_PAYLOAD as usize, "max_withdrawals_per_payload");
            assert_eq!(<$old as EthSpec>::pending_consolidations_limit() as usize, <$new>::PENDING_CONSOLIDATIONS_LIMIT as usize, "pending_consolidations_limit");
            assert_eq!(<$old as EthSpec>::pending_deposits_limit() as usize, <$new>::PENDING_DEPOSITS_LIMIT as usize, "pending_deposits_limit");
            assert_eq!(<$old as EthSpec>::pending_partial_withdrawals_limit() as usize, <$new>::PENDING_PARTIAL_WITHDRAWALS_LIMIT as usize, "pending_partial_withdrawals_limit");
            assert_eq!(<$old as EthSpec>::proposer_lookahead_slots() as usize, <$new>::PROPOSER_LOOKAHEAD_SLOTS as usize, "proposer_lookahead_slots");
            assert_eq!(<$old as EthSpec>::ptc_size() as usize, <$new>::PTC_SIZE as usize, "ptc_size");
            assert_eq!(<$old as EthSpec>::ptc_window_length() as usize, <$new>::PTC_WINDOW_LENGTH as usize, "ptc_window_length");
            assert_eq!(<$old as EthSpec>::slots_per_eth1_voting_period() as usize, <$new>::SLOTS_PER_ETH1_VOTING_PERIOD as usize, "slots_per_eth1_voting_period");
            assert_eq!(<$old as EthSpec>::epochs_per_historical_vector() as usize, <$new>::EPOCHS_PER_HISTORICAL_VECTOR as usize, "epochs_per_historical_vector");
            assert_eq!(<$old as EthSpec>::number_of_columns() as usize, <$new>::NUMBER_OF_COLUMNS as usize, "number_of_columns");
            assert_eq!(<$old as EthSpec>::cells_per_ext_blob() as usize, <$new>::CELLS_PER_EXT_BLOB as usize, "cells_per_ext_blob");
            assert_eq!(<$old as EthSpec>::sync_committee_size() as usize, <$new>::SYNC_COMMITTEE_SIZE as usize, "sync_committee_size");
            assert_eq!(<$old as EthSpec>::sync_subcommittee_size() as usize, <$new>::SYNC_SUBCOMMITTEE_SIZE as usize, "sync_subcommittee_size");
            assert_eq!(<$old as EthSpec>::slots_per_historical_root() as usize, <$new>::SLOTS_PER_HISTORICAL_ROOT as usize, "slots_per_historical_root");
            assert_eq!(<$old as EthSpec>::kzg_proof_inclusion_proof_depth() as usize, <$new>::KZG_COMMITMENT_INCLUSION_PROOF_DEPTH as usize, "kzg_proof_inclusion_proof_depth");
            assert_eq!(<$old as EthSpec>::minimum_validator_count(), <$new>::minimum_validator_count(), "minimum_validator_count");
            assert_eq!(<$old as EthSpec>::kzg_commitments_tree_depth(), <$new>::kzg_commitments_tree_depth(), "kzg_commitments_tree_depth");
            assert_eq!(<$old as EthSpec>::block_body_tree_depth(), <$new>::block_body_tree_depth(), "block_body_tree_depth");
            assert_eq!(<$old as EthSpec>::payload_timely_threshold(), <$new>::payload_timely_threshold(), "payload_timely_threshold");
            assert_eq!(<$old as EthSpec>::data_availability_timely_threshold(), <$new>::data_availability_timely_threshold(), "data_availability_timely_threshold");
            assert_eq!(<$old as EthSpec>::slots_per_epoch(), <$new>::slots_per_epoch());
            assert_eq!(<$old as EthSpec>::genesis_epoch().as_u64(), <$new>::genesis_epoch());
            assert_eq!(<$old as EthSpec>::spec_name().to_string(), <$new>::SPEC_ID.to_string());
            assert_eq!(<$old as EthSpec>::default_spec(), <$new>::default_spec());
        };
    }

    #[test]
    fn mainnet_matches_eth_spec() {
        check_table!(MainnetEthSpec, MainnetSpec);
    }

    #[test]
    fn minimal_matches_eth_spec() {
        check_table!(MinimalEthSpec, MinimalSpec);
    }

    #[test]
    fn gnosis_matches_eth_spec() {
        check_table!(GnosisEthSpec, GnosisSpec);
    }
}
