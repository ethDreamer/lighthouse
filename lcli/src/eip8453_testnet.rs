//! Writes a testnet directory for an EIP-8453-at-genesis devnet (prototype tooling).
//!
//! Only this codebase can build an EIP-8453 genesis state (the public genesis generators have no
//! idea of the fork), so the devnet scripts call this instead of the ethereum-genesis-generator.
//! The genesis validators' keys derive from a mnemonic so that `lcli mnemonic-validators` (same
//! mnemonic) produces the matching keystores, and the genesis execution payload header is the
//! mock-EL's deterministic genesis block, so `lcli mock-el` agrees with the state.

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use account_utils::eth2_keystore::keypair_from_secret;
use clap::ArgMatches;
use clap_utils::{parse_optional, parse_required};
use eth2_wallet::bip39::{Language, Mnemonic, Seed};
use eth2_wallet::{KeyType, recover_validator_secret_from_mnemonic};
use execution_layer::test_utils::generate_genesis_header;
use genesis::{DEFAULT_ETH1_BLOCK_HASH, interop_genesis_state_with_eth1};
use ssz::Encode;
use types::{Config, EthSpec, ForkName, Hash256, Uint256};

pub fn run<E: EthSpec>(matches: &ArgMatches) -> Result<(), String> {
    let testnet_dir: PathBuf = parse_required(matches, "testnet-dir")?;
    let validator_count: usize = parse_required(matches, "validator-count")?;
    let genesis_delay: u64 = parse_required(matches, "genesis-delay")?;
    let mnemonic_phrase: String = parse_required(matches, "mnemonic-phrase")?;
    let genesis_time: Option<u64> = parse_optional(matches, "genesis-time")?;

    let mut spec = ForkName::Eip8453.make_genesis_spec(E::default_spec());
    // The chain starts merged: the terminal difficulty machinery must never engage.
    spec.terminal_total_difficulty = Uint256::ZERO;
    spec.config_name = Some("eip8453-devnet".to_string());

    // Derive the genesis validators' voting keypairs from the mnemonic, matching
    // `lcli mnemonic-validators`.
    let mnemonic = Mnemonic::from_phrase(&mnemonic_phrase, Language::English)
        .map_err(|e| format!("Invalid mnemonic: {e:?}"))?;
    let seed = Seed::new(&mnemonic, "");
    let mut keypairs = Vec::with_capacity(validator_count);
    for index in 0..validator_count {
        let (secret, _) =
            recover_validator_secret_from_mnemonic(seed.as_bytes(), index as u32, KeyType::Voting)
                .map_err(|e| format!("Unable to derive validator {index}: {e:?}"))?;
        keypairs.push(
            keypair_from_secret(secret.as_bytes())
                .map_err(|e| format!("Unable to build keypair {index}: {e:?}"))?,
        );
    }

    let genesis_time = if let Some(genesis_time) = genesis_time {
        genesis_time
    } else {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| format!("Clock error: {e:?}"))?
            .as_secs()
            .saturating_add(genesis_delay)
    };

    // The mock EL's deterministic genesis block: the state's bid/latest block hash must name it
    // so the engine-API chain and the beacon chain agree from slot 0.
    let execution_payload_header = generate_genesis_header::<E>(&spec);
    let state = interop_genesis_state_with_eth1::<E>(
        &keypairs,
        genesis_time,
        Hash256::from_slice(DEFAULT_ETH1_BLOCK_HASH),
        execution_payload_header,
        &spec,
    )?;

    std::fs::create_dir_all(&testnet_dir)
        .map_err(|e| format!("Unable to create testnet dir: {e:?}"))?;
    let write = |name: &str, bytes: &[u8]| {
        std::fs::write(testnet_dir.join(name), bytes)
            .map_err(|e| format!("Unable to write {name}: {e:?}"))
    };
    let config = Config::from_chain_spec::<E>(&spec);
    let config_yaml =
        yaml_serde::to_string(&config).map_err(|e| format!("Unable to serialize config: {e:?}"))?;
    write("config.yaml", config_yaml.as_bytes())?;
    write("deposit_contract_block.txt", b"0")?;
    write("bootstrap_nodes.yaml", b"[]")?;
    write("genesis.ssz", &state.as_ssz_bytes())?;

    eprintln!(
        "Wrote an EIP-8453 testnet to {} ({} validators, genesis time {genesis_time})",
        testnet_dir.display(),
        validator_count,
    );
    Ok(())
}
