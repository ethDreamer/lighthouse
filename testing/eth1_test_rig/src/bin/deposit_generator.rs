use eth1_test_rig::{generate_deterministic_deposit, GanacheEth1Instance};
use serde_derive::{Deserialize, Serialize};
use serde_yaml;
use ssz::Encode;
use ssz_derive::{Decode, Encode};
use state_processing::common::DepositDataTree;
use std::future::Future;
use tree_hash::TreeHash;
use types::{DepositData, DepositTreeSnapshot, FinalizedExecutionBlock, Hash256, MainnetEthSpec};

#[tokio::main]
pub async fn block_on(
    future: impl Future<Output = Result<Vec<DepositTestCase>, String>>,
) -> Result<Vec<DepositTestCase>, String> {
    future.await
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct DepositTestCase {
    pub deposit_data: DepositData,
    pub deposit_data_root: Hash256,
    pub finalized_execution_block: FinalizedExecutionBlock,
    pub snapshot: DepositTreeSnapshot,
}

impl DepositTestCase {
    fn from_components(
        deposit: DepositData,
        finalized_execution_block: FinalizedExecutionBlock,
        snapshot: DepositTreeSnapshot,
    ) -> Self {
        let bytes = deposit.tree_hash_root().as_ssz_bytes();
        Self {
            deposit_data: deposit,
            deposit_data_root: Hash256::from_slice(&bytes),
            finalized_execution_block,
            snapshot,
        }
    }
}

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() < 2 {
        eprintln!("Usage: {} [NUM_DEPOSITS]", args[0]);
        std::process::exit(1);
    }
    let total_validator_count = match args[1].parse::<usize>() {
        Ok(n) => n,
        Err(e) => {
            eprintln!("Unable to parse '{}' as uint: {}", args[1], e);
            std::process::exit(1);
        }
    };

    let main_future = async move {
        /*
         * Deploy the deposit contract, spawn tasks to keep creating new blocks and deposit
         * validators.
         */
        let ganache_eth1_instance = GanacheEth1Instance::new(1).await?;
        let deposit_contract = ganache_eth1_instance.deposit_contract;
        // let network_id = ganache_eth1_instance.ganache.network_id();
        // let chain_id = ganache_eth1_instance.ganache.chain_id();
        // let eth1_endpoint = SensitiveUrl::parse(ganache.endpoint().as_str()).expect("Unable to parse ganache endpoint.");
        // let deposit_contract_address = deposit_contract.address();

        let deposit_amount = u64::pow(2, 5) * u64::pow(10, 9);
        let ganache = ganache_eth1_instance.ganache;

        let zero_root = deposit_contract.get_deposit_root().await.unwrap();
        assert_eq!(
            format!("{:?}", zero_root).as_str(),
            "0xd70a234731285c6804c2a4f56711ddb8c82c99740f207854891028af34e27e5e",
            "Empty Deposit Tree Root Mismatch"
        );

        // Submit deposits to the deposit contract.
        let mut deposit_tree = DepositDataTree::create(&[], 0, 32);
        let mut result = vec![];
        for i in 0..total_validator_count {
            let deposit = generate_deterministic_deposit::<MainnetEthSpec>(i, deposit_amount);
            deposit_contract
                .deposit_async(deposit.clone())
                .await
                .unwrap();
            // mine block
            let (block_hash, block_height) = ganache
                .web3
                .eth()
                .block(web3::types::BlockNumber::Latest.into())
                .await
                .unwrap()
                .map(|block| {
                    (
                        Hash256::from_slice(block.hash.unwrap().as_bytes()),
                        block.number.unwrap().as_u64(),
                    )
                })
                .unwrap();
            // get deposit tree root
            let deposit_tree_root = match deposit_contract.get_deposit_root().await {
                Ok(hash) => hash,
                Err(_) => Hash256::zero(),
            };

            let finalized_execution_block = FinalizedExecutionBlock {
                deposit_count: (i + 1) as u64,
                deposit_root: deposit_tree_root,
                block_hash,
                block_height,
            };

            // get deposit data root
            let deposit_data_root = Hash256::from_slice(&deposit.tree_hash_root().as_ssz_bytes());
            deposit_tree.push_leaf(deposit_data_root).unwrap();
            deposit_tree
                .finalize(finalized_execution_block.clone())
                .unwrap();
            let snapshot = deposit_tree.get_snapshot().unwrap();
            assert_eq!(snapshot.deposit_root, snapshot.calculate_root().unwrap());

            result.push(DepositTestCase::from_components(
                deposit,
                finalized_execution_block,
                deposit_tree.get_snapshot().unwrap(),
            ));
        }

        Ok(result)
    };

    match block_on(main_future) {
        Ok(results) => {
            println!("{}", serde_yaml::to_string(&results).unwrap())
        }
        Err(e) => eprintln!("Errors occurred: {}", e),
    };
}
