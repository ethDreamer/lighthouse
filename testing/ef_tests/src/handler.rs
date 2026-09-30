use crate::FeatureName;
use crate::cases::{self, Case, Cases, EpochTransition, LoadCase, Operation};
use crate::type_name::TypeName;
use context_deserialize::ContextDeserialize;
use educe::Educe;
use std::fs::{self, DirEntry};
use std::marker::PhantomData;
use std::path::PathBuf;
use types::{BeaconState, EthSpec, ForkName};

pub trait Handler {
    type Case: Case + LoadCase;

    fn config_name() -> &'static str {
        "general"
    }

    fn runner_name() -> &'static str;

    fn handler_name(&self) -> String;

    fn handler_path(&self, fork_or_feature_name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("consensus-spec-tests")
            .join("tests")
            .join(Self::config_name())
            .join(fork_or_feature_name)
            .join(Self::runner_name())
            .join(self.handler_name())
    }

    // Add forks here to exclude them from EF spec testing. Helpful for adding future or
    // unspecified forks.
    fn disabled_forks(&self) -> Vec<ForkName> {
        vec![]
    }

    fn is_enabled_for_fork(&self, fork_name: ForkName) -> bool {
        Self::Case::is_enabled_for_fork(fork_name)
    }

    fn is_enabled_for_feature(&self, feature_name: FeatureName) -> bool {
        Self::Case::is_enabled_for_feature(feature_name)
    }

    fn run(&self) {
        for fork_name in ForkName::list_all() {
            // TODO(heze): remove this skip once Heze spec test vectors are published in
            // consensus-spec-tests.
            if fork_name == ForkName::Heze {
                continue;
            }
            if !self.disabled_forks().contains(&fork_name) && self.is_enabled_for_fork(fork_name) {
                self.run_for_fork(fork_name);
            }
        }

        // Run feature tests for future forks that are not yet added to `ForkName`.
        // This runs tests in the directory named by the feature instead of the fork name.
        // e.g. consensus-spec-tests/tests/general/[feature_name]/[runner_name]
        // e.g. consensus-spec-tests/tests/general/peerdas/ssz_static
        for feature_name in FeatureName::list_all() {
            if self.is_enabled_for_feature(feature_name) {
                self.run_for_feature(feature_name);
            }
        }
    }

    // Do NOT override this function.
    // TODO: use default keyword when stable.
    fn rayon_enabled() -> bool {
        #[cfg(feature = "disable_rayon")]
        {
            false
        }
        #[cfg(not(feature = "disable_rayon"))]
        {
            Self::use_rayon()
        }
    }

    fn use_rayon() -> bool {
        true
    }

    fn run_for_fork(&self, fork_name: ForkName) {
        let fork_name_str = fork_name.to_string();
        let handler_path = self.handler_path(&fork_name_str);

        // Iterate through test suites
        let as_directory = |entry: Result<DirEntry, std::io::Error>| -> Option<DirEntry> {
            entry
                .ok()
                .filter(|e| e.file_type().map(|ty| ty.is_dir()).unwrap())
        };

        let test_cases = fs::read_dir(&handler_path)
            .unwrap_or_else(|e| panic!("handler dir {} exists: {:?}", handler_path.display(), e))
            .filter_map(as_directory)
            .flat_map(|suite| fs::read_dir(suite.path()).expect("suite dir exists"))
            .filter_map(as_directory)
            .map(|test_case_dir| {
                let path = test_case_dir.path();
                let case = Self::Case::load_from_dir(&path, fork_name).expect("test should load");
                (path, case)
            })
            .collect();

        let results = Cases { test_cases }.test_results(fork_name, Self::rayon_enabled());

        let name = format!(
            "{}/{}/{}",
            fork_name_str,
            Self::runner_name(),
            self.handler_name()
        );
        crate::results::assert_tests_pass(&name, &handler_path, &results);
    }

    fn run_for_feature(&self, feature_name: FeatureName) {
        let feature_name_str = feature_name.to_string();
        let fork_name = feature_name.fork_name();
        let handler_path = self.handler_path(&feature_name_str);

        // Iterate through test suites
        let as_directory = |entry: Result<DirEntry, std::io::Error>| -> Option<DirEntry> {
            entry
                .ok()
                .filter(|e| e.file_type().map(|ty| ty.is_dir()).unwrap())
        };

        let test_cases = fs::read_dir(&handler_path)
            .unwrap_or_else(|e| panic!("handler dir {} exists: {:?}", handler_path.display(), e))
            .filter_map(as_directory)
            .flat_map(|suite| fs::read_dir(suite.path()).expect("suite dir exists"))
            .filter_map(as_directory)
            .map(|test_case_dir| {
                let path = test_case_dir.path();
                let case = Self::Case::load_from_dir(&path, fork_name).expect("test should load");
                (path, case)
            })
            .collect();

        let results = Cases { test_cases }.test_results(fork_name, Self::rayon_enabled());

        let name = format!(
            "{}/{}/{}",
            feature_name_str,
            Self::runner_name(),
            self.handler_name()
        );
        crate::results::assert_tests_pass(&name, &handler_path, &results);
    }
}

macro_rules! bls_eth_handler {
    ($runner_name: ident, $case_name:ident, $handler_name:expr) => {
        #[derive(Educe)]
        #[educe(Default)]
        pub struct $runner_name;

        impl Handler for $runner_name {
            type Case = cases::$case_name;

            fn runner_name() -> &'static str {
                "bls"
            }

            fn handler_name(&self) -> String {
                $handler_name.into()
            }
        }
    };
}

macro_rules! bls_handler {
    ($runner_name: ident, $case_name:ident, $handler_name:expr) => {
        #[derive(Educe)]
        #[educe(Default)]
        pub struct $runner_name;

        impl Handler for $runner_name {
            type Case = cases::$case_name;

            fn runner_name() -> &'static str {
                "bls"
            }

            fn config_name() -> &'static str {
                "bls12-381-tests"
            }

            fn handler_name(&self) -> String {
                $handler_name.into()
            }

            fn run(&self) {
                let fork_name = ForkName::Base;
                let fork_name_str = fork_name.to_string();
                let handler_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("consensus-spec-tests")
                    .join(Self::config_name())
                    .join(self.handler_name());

                let as_file = |entry: Result<DirEntry, std::io::Error>| -> Option<DirEntry> {
                    entry
                        .ok()
                        .filter(|e| e.file_type().map(|ty| ty.is_file()).unwrap_or(false))
                };
                let test_cases: Vec<(PathBuf, Self::Case)> = fs::read_dir(&handler_path)
                    .expect("handler dir exists")
                    .filter_map(as_file)
                    .map(|test_case_path| {
                        let path = test_case_path.path();
                        let case =
                            Self::Case::load_from_dir(&path, fork_name).expect("test should load");

                        (path, case)
                    })
                    .collect();

                let results = Cases { test_cases }.test_results(fork_name, Self::rayon_enabled());

                let name = format!(
                    "{}/{}/{}",
                    fork_name_str,
                    Self::runner_name(),
                    self.handler_name()
                );
                crate::results::assert_tests_pass(&name, &handler_path, &results);
            }
        }
    };
}

bls_handler!(BlsAggregateSigsHandler, BlsAggregateSigs, "aggregate");
bls_handler!(BlsSignMsgHandler, BlsSign, "sign");
bls_handler!(BlsBatchVerifyHandler, BlsBatchVerify, "batch_verify");
bls_handler!(BlsVerifyMsgHandler, BlsVerify, "verify");
bls_handler!(
    BlsAggregateVerifyHandler,
    BlsAggregateVerify,
    "aggregate_verify"
);
bls_handler!(
    BlsFastAggregateVerifyHandler,
    BlsFastAggregateVerify,
    "fast_aggregate_verify"
);
bls_eth_handler!(
    BlsEthAggregatePubkeysHandler,
    BlsEthAggregatePubkeys,
    "eth_aggregate_pubkeys"
);
bls_eth_handler!(
    BlsEthFastAggregateVerifyHandler,
    BlsEthFastAggregateVerify,
    "eth_fast_aggregate_verify"
);

/// Handler for SSZ types.
pub struct SszStaticHandler<T> {
    supported_forks: Vec<ForkName>,
    _phantom: PhantomData<T>,
}

impl<T> Default for SszStaticHandler<T> {
    fn default() -> Self {
        Self::for_forks(ForkName::list_all())
    }
}

impl<T> SszStaticHandler<T> {
    pub fn for_forks(supported_forks: Vec<ForkName>) -> Self {
        SszStaticHandler {
            supported_forks,
            _phantom: PhantomData,
        }
    }

    pub fn base_only() -> Self {
        Self::for_forks(vec![ForkName::Base])
    }

    pub fn altair_only() -> Self {
        Self::for_forks(vec![ForkName::Altair])
    }

    pub fn bellatrix_only() -> Self {
        Self::for_forks(vec![ForkName::Bellatrix])
    }

    pub fn capella_only() -> Self {
        Self::for_forks(vec![ForkName::Capella])
    }

    pub fn deneb_only() -> Self {
        Self::for_forks(vec![ForkName::Deneb])
    }

    pub fn electra_only() -> Self {
        Self::for_forks(vec![ForkName::Electra])
    }

    pub fn fulu_only() -> Self {
        Self::for_forks(vec![ForkName::Fulu])
    }

    pub fn gloas_only() -> Self {
        Self::for_forks(vec![ForkName::Gloas])
    }

    pub fn heze_only() -> Self {
        Self::for_forks(vec![ForkName::Heze])
    }

    pub fn altair_and_later() -> Self {
        Self::for_forks(ForkName::list_all()[1..].to_vec())
    }

    pub fn merge_and_later() -> Self {
        Self::for_forks(ForkName::list_all()[2..].to_vec())
    }

    pub fn capella_and_later() -> Self {
        Self::for_forks(ForkName::list_all()[3..].to_vec())
    }

    pub fn deneb_and_later() -> Self {
        Self::for_forks(ForkName::list_all()[4..].to_vec())
    }

    pub fn electra_and_later() -> Self {
        Self::for_forks(ForkName::list_all()[5..].to_vec())
    }

    pub fn electra_through_fulu() -> Self {
        Self::for_forks(ForkName::list_all()[5..7].to_vec())
    }

    pub fn fulu_and_later() -> Self {
        Self::for_forks(ForkName::list_all()[6..].to_vec())
    }

    pub fn gloas_and_later() -> Self {
        Self::for_forks(ForkName::list_all()[7..].to_vec())
    }

    pub fn heze_and_later() -> Self {
        Self::for_forks(ForkName::list_all()[8..].to_vec())
    }

    pub fn pre_electra() -> Self {
        Self::for_forks(ForkName::list_all()[0..5].to_vec())
    }

    pub fn pre_capella() -> Self {
        Self::for_forks(ForkName::list_all()[0..3].to_vec())
    }
}

/// Handler for SSZ types that implement `CachedTreeHash`.
#[derive(Educe)]
#[educe(Default)]
pub struct SszStaticTHCHandler<T>(PhantomData<T>);

/// Handler for SSZ types that don't implement `ssz::Decode`.
pub struct SszStaticWithSpecHandler<T> {
    supported_forks: Vec<ForkName>,
    _phantom: PhantomData<T>,
}

impl<T> Default for SszStaticWithSpecHandler<T> {
    fn default() -> Self {
        Self::for_forks(ForkName::list_all())
    }
}

impl<T> SszStaticWithSpecHandler<T> {
    pub fn for_forks(supported_forks: Vec<ForkName>) -> Self {
        SszStaticWithSpecHandler {
            supported_forks,
            _phantom: PhantomData,
        }
    }

    pub fn fulu_and_later() -> Self {
        Self::for_forks(ForkName::list_all()[6..].to_vec())
    }
}

impl<T> Handler for SszStaticHandler<T>
where
    T: cases::SszStaticType
        + for<'de> ContextDeserialize<'de, ForkName>
        + tree_hash::TreeHash
        + ssz::Decode
        + TypeName
{
    type Case = cases::SszStatic<T>;

    fn config_name() -> &'static str {
        Spec::PRESET_BASE
    }

    fn runner_name() -> &'static str {
        "ssz_static"
    }

    fn handler_name(&self) -> String {
        T::name().into()
    }

    fn is_enabled_for_fork(&self, fork_name: ForkName) -> bool {
        self.supported_forks.contains(&fork_name)
    }
}

impl Handler for SszStaticTHCHandler<BeaconState>

{
    type Case = cases::SszStaticTHC<BeaconState>;

    fn config_name() -> &'static str {
        Spec::PRESET_BASE
    }

    fn runner_name() -> &'static str {
        "ssz_static"
    }

    fn handler_name(&self) -> String {
        BeaconState::name().into()
    }
}

impl<T> Handler for SszStaticWithSpecHandler<T>
where
    T: TypeName,
    cases::SszStaticWithSpec<T>: Case + LoadCase,
{
    type Case = cases::SszStaticWithSpec<T>;

    fn config_name() -> &'static str {
        Spec::PRESET_BASE
    }

    fn runner_name() -> &'static str {
        "ssz_static"
    }

    fn handler_name(&self) -> String {
        T::name().into()
    }

    fn is_enabled_for_fork(&self, fork_name: ForkName) -> bool {
        self.supported_forks.contains(&fork_name)
    }
}

#[derive(Educe)]
#[educe(Default)]
pub struct ShufflingHandler(PhantomData<E>);

impl Handler for ShufflingHandler {
    type Case = cases::Shuffling;

    fn config_name() -> &'static str {
        Spec::PRESET_BASE
    }

    fn runner_name() -> &'static str {
        "shuffling"
    }

    fn handler_name(&self) -> String {
        "core".into()
    }

    fn is_enabled_for_fork(&self, fork_name: ForkName) -> bool {
        fork_name == ForkName::Base
    }
}

#[derive(Educe)]
#[educe(Default)]
pub struct SanityBlocksHandler(PhantomData<E>);

impl Handler for SanityBlocksHandler {
    type Case = cases::SanityBlocks;

    fn config_name() -> &'static str {
        Spec::PRESET_BASE
    }

    fn runner_name() -> &'static str {
        "sanity"
    }

    fn handler_name(&self) -> String {
        "blocks".into()
    }

    fn is_enabled_for_fork(&self, _fork_name: ForkName) -> bool {
        // NOTE: v1.1.0-beta.4 doesn't mark the historical blocks test as requiring real crypto, so
        // only run these tests with real crypto for now.
        cfg!(not(feature = "fake_crypto"))
    }
}

#[derive(Educe)]
#[educe(Default)]
pub struct SanitySlotsHandler(PhantomData<E>);

impl Handler for SanitySlotsHandler {
    type Case = cases::SanitySlots;

    fn config_name() -> &'static str {
        Spec::PRESET_BASE
    }

    fn runner_name() -> &'static str {
        "sanity"
    }

    fn handler_name(&self) -> String {
        "slots".into()
    }

    fn is_enabled_for_fork(&self, fork_name: ForkName) -> bool {
        // Some sanity tests compute sync committees, which requires real crypto.
        fork_name == ForkName::Base || cfg!(not(feature = "fake_crypto"))
    }
}

#[derive(Educe)]
#[educe(Default)]
pub struct RandomHandler(PhantomData<E>);

impl Handler for RandomHandler {
    type Case = cases::SanityBlocks;

    fn config_name() -> &'static str {
        Spec::PRESET_BASE
    }

    fn runner_name() -> &'static str {
        "random"
    }

    fn handler_name(&self) -> String {
        "random".into()
    }
}

#[derive(Educe)]
#[educe(Default)]
pub struct EpochProcessingHandler<T>(PhantomData<T>);

impl<T: EpochTransition> Handler for EpochProcessingHandler<T> {
    type Case = cases::EpochProcessing<T>;

    fn config_name() -> &'static str {
        Spec::PRESET_BASE
    }

    fn runner_name() -> &'static str {
        "epoch_processing"
    }

    fn handler_name(&self) -> String {
        T::name().into()
    }
}

pub struct RewardsHandler {
    handler_name: &'static str,
    _phantom: PhantomData<E>,
}

impl RewardsHandler {
    pub fn new(handler_name: &'static str) -> Self {
        Self {
            handler_name,
            _phantom: PhantomData,
        }
    }
}

impl Handler for RewardsHandler {
    type Case = cases::RewardsTest;

    fn config_name() -> &'static str {
        Spec::PRESET_BASE
    }

    fn runner_name() -> &'static str {
        "rewards"
    }

    fn handler_name(&self) -> String {
        self.handler_name.to_string()
    }

    fn is_enabled_for_fork(&self, fork_name: ForkName) -> bool {
        if self.handler_name == "inactivity_scores" {
            // These tests were added in v1.7.0-alpha.2 and are available for Altair and later.
            fork_name.altair_enabled()
        } else {
            true
        }
    }
}

#[derive(Educe)]
#[educe(Default)]
pub struct ForkHandler(PhantomData<E>);

impl Handler for ForkHandler {
    type Case = cases::ForkTest;

    fn config_name() -> &'static str {
        Spec::PRESET_BASE
    }

    fn runner_name() -> &'static str {
        "fork"
    }

    fn handler_name(&self) -> String {
        "fork".into()
    }
}

#[derive(Educe)]
#[educe(Default)]
pub struct TransitionHandler(PhantomData<E>);

impl Handler for TransitionHandler {
    type Case = cases::TransitionTest;

    fn config_name() -> &'static str {
        Spec::PRESET_BASE
    }

    fn runner_name() -> &'static str {
        "transition"
    }

    fn handler_name(&self) -> String {
        "core".into()
    }
}

#[derive(Educe)]
#[educe(Default)]
pub struct FinalityHandler(PhantomData<E>);

impl Handler for FinalityHandler {
    // Reuse the blocks case runner.
    type Case = cases::SanityBlocks;

    fn config_name() -> &'static str {
        Spec::PRESET_BASE
    }

    fn runner_name() -> &'static str {
        "finality"
    }

    fn handler_name(&self) -> String {
        "finality".into()
    }
}

pub struct ForkChoiceHandler {
    handler_name: String,
    _phantom: PhantomData<E>,
}

impl ForkChoiceHandler {
    pub fn new(handler_name: &str) -> Self {
        Self {
            handler_name: handler_name.into(),
            _phantom: PhantomData,
        }
    }
}

impl Handler for ForkChoiceHandler {
    type Case = cases::ForkChoiceTest;

    fn config_name() -> &'static str {
        Spec::PRESET_BASE
    }

    fn runner_name() -> &'static str {
        "fork_choice"
    }

    fn handler_name(&self) -> String {
        self.handler_name.clone()
    }

    fn use_rayon() -> bool {
        // The fork choice tests use `block_on` which can cause panics with rayon.
        false
    }

    fn is_enabled_for_fork(&self, fork_name: ForkName) -> bool {
        // We no longer run on_merge_block tests since removing merge support.
        if self.handler_name == "on_merge_block" {
            return false;
        }

        // Tests are no longer generated for the base/phase0 specification.
        if fork_name == ForkName::Base {
            return false;
        }

        // Deposit tests exist only for Electra and later.
        if self.handler_name == "deposit_with_reorg" && !fork_name.electra_enabled() {
            return false;
        }

        // Proposer head tests removed in Gloas.
        if self.handler_name == "get_proposer_head" && fork_name.gloas_enabled() {
            return false;
        }

        // on_attestation, on_execution_payload_envelope, get_parent_payload_status,
        // on_payload_attestation_message, payload_timeliness, payload_data_availability,
        // and should_apply_proposer_boost tests exist only for Gloas and later.
        if (self.handler_name == "on_attestation"
            || self.handler_name == "on_execution_payload_envelope"
            || self.handler_name == "get_parent_payload_status"
            || self.handler_name == "on_payload_attestation_message"
            || self.handler_name == "payload_timeliness"
            || self.handler_name == "payload_data_availability"
            || self.handler_name == "should_apply_proposer_boost")
            && !fork_name.gloas_enabled()
        {
            return false;
        }

        // These tests check block validity (which may include signatures) and there is no need to
        // run them with fake crypto.
        cfg!(not(feature = "fake_crypto"))
    }

    fn disabled_forks(&self) -> Vec<ForkName> {
        vec![]
    }
}

pub struct ForkChoiceComplianceHandler {
    handler_name: String,
    _phantom: PhantomData<E>,
}

impl ForkChoiceComplianceHandler {
    pub fn new(handler_name: &str) -> Self {
        Self {
            handler_name: handler_name.into(),
            _phantom: PhantomData,
        }
    }
}

impl Handler for ForkChoiceComplianceHandler {
    type Case = cases::ForkChoiceTest;

    fn config_name() -> &'static str {
        Spec::PRESET_BASE
    }

    fn runner_name() -> &'static str {
        "fork_choice_compliance"
    }

    fn handler_name(&self) -> String {
        self.handler_name.clone()
    }

    fn use_rayon() -> bool {
        false
    }

    fn is_enabled_for_fork(&self, fork_name: ForkName) -> bool {
        cfg!(feature = "fake_crypto") && fork_name.fulu_enabled()
    }

    fn disabled_forks(&self) -> Vec<ForkName> {
        vec![]
    }
}

pub struct FastConfirmationHandler {
    handler_name: String,
    _phantom: PhantomData<E>,
}

impl FastConfirmationHandler {
    pub fn new(handler_name: &str) -> Self {
        Self {
            handler_name: handler_name.into(),
            _phantom: PhantomData,
        }
    }
}

impl Handler for FastConfirmationHandler {
    type Case = cases::ForkChoiceTest;

    fn config_name() -> &'static str {
        Spec::PRESET_BASE
    }

    fn runner_name() -> &'static str {
        "fast_confirmation"
    }

    fn handler_name(&self) -> String {
        self.handler_name.clone()
    }

    fn use_rayon() -> bool {
        false
    }

    fn is_enabled_for_fork(&self, fork_name: ForkName) -> bool {
        // FCR vectors carry `bls_setting: 2` (fake signatures), so they must run under fake_crypto.
        fork_name != ForkName::Base && cfg!(feature = "fake_crypto")
    }

    fn disabled_forks(&self) -> Vec<ForkName> {
        vec![]
    }
}

#[derive(Educe)]
#[educe(Default)]
pub struct OptimisticSyncHandler(PhantomData<E>);

impl Handler for OptimisticSyncHandler {
    type Case = cases::ForkChoiceTest;

    fn config_name() -> &'static str {
        Spec::PRESET_BASE
    }

    fn runner_name() -> &'static str {
        "sync"
    }

    fn handler_name(&self) -> String {
        "optimistic".into()
    }

    fn use_rayon() -> bool {
        // The opt sync tests use `block_on` which can cause panics with rayon.
        false
    }

    fn is_enabled_for_fork(&self, fork_name: ForkName) -> bool {
        fork_name.bellatrix_enabled() && cfg!(not(feature = "fake_crypto"))
    }

    fn disabled_forks(&self) -> Vec<ForkName> {
        // TODO(gloas): remove once we have Gloas optimistic sync tests
        vec![ForkName::Gloas, ForkName::Heze]
    }
}

#[derive(Educe)]
#[educe(Default)]
pub struct GenesisValidityHandler(PhantomData<E>);

impl Handler for GenesisValidityHandler {
    type Case = cases::GenesisValidity;

    fn config_name() -> &'static str {
        Spec::PRESET_BASE
    }

    fn runner_name() -> &'static str {
        "genesis"
    }

    fn handler_name(&self) -> String {
        "validity".into()
    }
}

#[derive(Educe)]
#[educe(Default)]
pub struct GenesisInitializationHandler(PhantomData<E>);

impl Handler for GenesisInitializationHandler {
    type Case = cases::GenesisInitialization;

    fn config_name() -> &'static str {
        Spec::PRESET_BASE
    }

    fn runner_name() -> &'static str {
        "genesis"
    }

    fn handler_name(&self) -> String {
        "initialization".into()
    }
}

#[derive(Educe)]
#[educe(Default)]
pub struct KZGBlobToKZGCommitmentHandler(PhantomData<E>);

impl Handler for KZGBlobToKZGCommitmentHandler {
    type Case = cases::KZGBlobToKZGCommitment;

    fn config_name() -> &'static str {
        "general"
    }

    fn runner_name() -> &'static str {
        "kzg"
    }

    fn handler_name(&self) -> String {
        "blob_to_kzg_commitment".into()
    }
}

#[derive(Educe)]
#[educe(Default)]
pub struct KZGComputeBlobKZGProofHandler(PhantomData<E>);

impl Handler for KZGComputeBlobKZGProofHandler {
    type Case = cases::KZGComputeBlobKZGProof;

    fn config_name() -> &'static str {
        "general"
    }

    fn runner_name() -> &'static str {
        "kzg"
    }

    fn handler_name(&self) -> String {
        "compute_blob_kzg_proof".into()
    }
}

#[derive(Educe)]
#[educe(Default)]
pub struct KZGComputeKZGProofHandler(PhantomData<E>);

impl Handler for KZGComputeKZGProofHandler {
    type Case = cases::KZGComputeKZGProof;

    fn config_name() -> &'static str {
        "general"
    }

    fn runner_name() -> &'static str {
        "kzg"
    }

    fn handler_name(&self) -> String {
        "compute_kzg_proof".into()
    }
}

#[derive(Educe)]
#[educe(Default)]
pub struct KZGVerifyBlobKZGProofHandler(PhantomData<E>);

impl Handler for KZGVerifyBlobKZGProofHandler {
    type Case = cases::KZGVerifyBlobKZGProof;

    fn config_name() -> &'static str {
        "general"
    }

    fn runner_name() -> &'static str {
        "kzg"
    }

    fn handler_name(&self) -> String {
        "verify_blob_kzg_proof".into()
    }
}

#[derive(Educe)]
#[educe(Default)]
pub struct KZGVerifyBlobKZGProofBatchHandler(PhantomData<E>);

impl Handler for KZGVerifyBlobKZGProofBatchHandler {
    type Case = cases::KZGVerifyBlobKZGProofBatch;

    fn config_name() -> &'static str {
        "general"
    }

    fn runner_name() -> &'static str {
        "kzg"
    }

    fn handler_name(&self) -> String {
        "verify_blob_kzg_proof_batch".into()
    }
}

#[derive(Educe)]
#[educe(Default)]
pub struct KZGVerifyKZGProofHandler(PhantomData<E>);

impl Handler for KZGVerifyKZGProofHandler {
    type Case = cases::KZGVerifyKZGProof;

    fn config_name() -> &'static str {
        "general"
    }

    fn runner_name() -> &'static str {
        "kzg"
    }

    fn handler_name(&self) -> String {
        "verify_kzg_proof".into()
    }
}

#[derive(Educe)]
#[educe(Default)]
pub struct GetCustodyGroupsHandler(PhantomData<E>);

impl Handler for GetCustodyGroupsHandler {
    type Case = cases::GetCustodyGroups;

    fn config_name() -> &'static str {
        Spec::PRESET_BASE
    }

    fn runner_name() -> &'static str {
        "networking"
    }

    fn handler_name(&self) -> String {
        "get_custody_groups".into()
    }
}

#[derive(Educe)]
#[educe(Default)]
pub struct ComputeColumnsForCustodyGroupHandler(PhantomData<E>);

impl Handler for ComputeColumnsForCustodyGroupHandler {
    type Case = cases::ComputeColumnsForCustodyGroups;

    fn config_name() -> &'static str {
        Spec::PRESET_BASE
    }

    fn runner_name() -> &'static str {
        "networking"
    }

    fn handler_name(&self) -> String {
        "compute_columns_for_custody_group".into()
    }
}

pub struct GossipValidationHandler {
    handler_name: &'static str,
    supported_forks: Vec<ForkName>,
    _phantom: PhantomData<E>,
}

impl GossipValidationHandler {
    pub fn new(handler_name: &'static str) -> Self {
        Self::for_forks(handler_name, ForkName::list_all())
    }

    pub fn for_forks(handler_name: &'static str, supported_forks: Vec<ForkName>) -> Self {
        Self {
            handler_name,
            supported_forks,
            _phantom: PhantomData,
        }
    }

    pub fn latest_stable(handler_name: &'static str) -> Self {
        Self::for_forks(handler_name, vec![ForkName::latest_stable()])
    }
}

impl Handler for GossipValidationHandler {
    type Case = cases::GossipValidation;

    fn use_rayon() -> bool {
        // Some gossip validation tests use `block_on` which can cause panics with rayon.
        false
    }

    fn config_name() -> &'static str {
        Spec::PRESET_BASE
    }

    fn runner_name() -> &'static str {
        "networking"
    }

    fn handler_name(&self) -> String {
        self.handler_name.into()
    }

    fn is_enabled_for_fork(&self, fork_name: ForkName) -> bool {
        let fork_name_str = fork_name.to_string();
        self.supported_forks.contains(&fork_name) && self.handler_path(&fork_name_str).exists()
    }

    fn disabled_forks(&self) -> Vec<ForkName> {
        vec![ForkName::Gloas]
    }
}

#[derive(Educe)]
#[educe(Default)]
pub struct KZGComputeCellsHandler(PhantomData<E>);

impl Handler for KZGComputeCellsHandler {
    type Case = cases::KZGComputeCells;

    fn config_name() -> &'static str {
        "general"
    }

    fn runner_name() -> &'static str {
        "kzg"
    }

    fn handler_name(&self) -> String {
        "compute_cells".into()
    }

    fn disabled_forks(&self) -> Vec<ForkName> {
        // TODO(gloas): remove once we have Gloas KZG tests
        vec![ForkName::Gloas, ForkName::Heze]
    }
}

#[derive(Educe)]
#[educe(Default)]
pub struct KZGComputeCellsAndKZGProofHandler(PhantomData<E>);

impl Handler for KZGComputeCellsAndKZGProofHandler {
    type Case = cases::KZGComputeCellsAndKZGProofs;

    fn config_name() -> &'static str {
        "general"
    }

    fn runner_name() -> &'static str {
        "kzg"
    }

    fn handler_name(&self) -> String {
        "compute_cells_and_kzg_proofs".into()
    }

    fn disabled_forks(&self) -> Vec<ForkName> {
        // TODO(gloas): remove once we have Gloas KZG tests
        vec![ForkName::Gloas, ForkName::Heze]
    }
}

#[derive(Educe)]
#[educe(Default)]
pub struct KZGVerifyCellKZGProofBatchHandler(PhantomData<E>);

impl Handler for KZGVerifyCellKZGProofBatchHandler {
    type Case = cases::KZGVerifyCellKZGProofBatch;

    fn config_name() -> &'static str {
        "general"
    }

    fn runner_name() -> &'static str {
        "kzg"
    }

    fn handler_name(&self) -> String {
        "verify_cell_kzg_proof_batch".into()
    }

    fn disabled_forks(&self) -> Vec<ForkName> {
        // TODO(gloas): remove once we have Gloas KZG tests
        vec![ForkName::Gloas, ForkName::Heze]
    }
}

#[derive(Educe)]
#[educe(Default)]
pub struct KZGRecoverCellsAndKZGProofHandler(PhantomData<E>);

impl Handler for KZGRecoverCellsAndKZGProofHandler {
    type Case = cases::KZGRecoverCellsAndKZGProofs;

    fn config_name() -> &'static str {
        "general"
    }

    fn runner_name() -> &'static str {
        "kzg"
    }

    fn handler_name(&self) -> String {
        "recover_cells_and_kzg_proofs".into()
    }

    fn disabled_forks(&self) -> Vec<ForkName> {
        // TODO(gloas): remove once we have Gloas KZG tests
        vec![ForkName::Gloas, ForkName::Heze]
    }
}

#[derive(Educe)]
#[educe(Default)]
pub struct KzgInclusionMerkleProofValidityHandler(PhantomData<E>);

impl Handler for KzgInclusionMerkleProofValidityHandler {
    type Case = cases::KzgInclusionMerkleProofValidity;

    fn config_name() -> &'static str {
        Spec::PRESET_BASE
    }

    fn runner_name() -> &'static str {
        "merkle_proof"
    }

    fn handler_name(&self) -> String {
        "single_merkle_proof".into()
    }

    fn is_enabled_for_fork(&self, fork_name: ForkName) -> bool {
        fork_name.deneb_enabled()
    }

    fn disabled_forks(&self) -> Vec<ForkName> {
        // TODO(gloas): remove once we have Gloas KZG merkle proof tests
        vec![ForkName::Gloas, ForkName::Heze]
    }
}

#[derive(Educe)]
#[educe(Default)]
pub struct MerkleProofValidityHandler(PhantomData<E>);

impl Handler for MerkleProofValidityHandler {
    type Case = cases::GenericMerkleProofValidity;

    fn config_name() -> &'static str {
        Spec::PRESET_BASE
    }

    fn runner_name() -> &'static str {
        "light_client"
    }

    fn handler_name(&self) -> String {
        "single_merkle_proof".into()
    }

    fn is_enabled_for_fork(&self, fork_name: ForkName) -> bool {
        fork_name.altair_enabled()
    }

    fn disabled_forks(&self) -> Vec<ForkName> {
        // TODO(gloas): remove once we have Gloas light client tests
        vec![ForkName::Gloas, ForkName::Heze]
    }
}

#[derive(Educe)]
#[educe(Default)]
pub struct LightClientUpdateHandler(PhantomData<E>);

impl Handler for LightClientUpdateHandler {
    type Case = cases::LightClientVerifyIsBetterUpdate;

    fn config_name() -> &'static str {
        Spec::PRESET_BASE
    }

    fn runner_name() -> &'static str {
        "light_client"
    }

    fn handler_name(&self) -> String {
        "update_ranking".into()
    }

    fn is_enabled_for_fork(&self, fork_name: ForkName) -> bool {
        // Enabled in Altair
        fork_name.altair_enabled()
    }

    fn disabled_forks(&self) -> Vec<ForkName> {
        // TODO(gloas): remove once we have Gloas light client tests
        vec![ForkName::Gloas, ForkName::Heze]
    }
}

#[derive(Educe)]
#[educe(Default)]
pub struct OperationsHandler<O>(PhantomData<O>);

impl<O: Operation> Handler for OperationsHandler<O> {
    type Case = cases::Operations<O>;

    fn config_name() -> &'static str {
        Spec::PRESET_BASE
    }

    fn runner_name() -> &'static str {
        "operations"
    }

    fn handler_name(&self) -> String {
        O::handler_name()
    }
}
