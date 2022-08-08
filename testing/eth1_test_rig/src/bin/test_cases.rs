use serde_derive::{Deserialize, Serialize};
use serde_yaml;
use ssz_derive::{Decode, Encode};
use state_processing::common::DepositDataTree;
use std::fs::File;
use std::io::Read;
use types::{DepositData, DepositTreeSnapshot, Eth1Data, FinalizedExecutionBlock, Hash256};

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct SnapshotData {
    pub finalized_eth1_data: Eth1Data,
    pub snapshot: DepositTreeSnapshot,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct DepositTestCase {
    pub deposit_data: DepositData,
    pub deposit_data_root: Hash256,
    pub finalized_execution_block: FinalizedExecutionBlock,
    pub snapshot: DepositTreeSnapshot,
}

fn load_file(file: &str) -> Result<String, String> {
    let mut file =
        File::open(file).map_err(|e| format!("Unable to open file for reading: {:?}", e))?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .map_err(|e| format!("Unable to read file: {:?}", e))?;
    Ok(contents)
}

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() < 2 {
        eprintln!("Usage: {} TEST_CASE_YAML_FILE", args[0]);
        std::process::exit(1);
    }
    let yaml_string = load_file(&args[1])
        .map_err(|err| {
            eprintln!("{}", err);
            std::process::exit(1);
        })
        .unwrap();

    let deposit_cases: Vec<DepositTestCase> = serde_yaml::from_str(&yaml_string)
        .map_err(|e| {
            eprintln!("Unable to deserialize yaml: {:?}", e);
            std::process::exit(1);
        })
        .unwrap();

    let mut deposit_tree = DepositDataTree::create(&[], 0, 32);
    assert_eq!(
        format!("{:?}", deposit_tree.root()).as_str(),
        "0xd70a234731285c6804c2a4f56711ddb8c82c99740f207854891028af34e27e5e",
        "Empty Deposit Tree Root Mismatch"
    );

    for i in 0..deposit_cases.len() {
        let mut full_tree = DepositDataTree::create(&[], 0, 32);
        for j in 0..i + 1 {
            full_tree
                .push_leaf(deposit_cases[j].deposit_data_root)
                .expect("should push leaf");
        }
        deposit_tree
            .push_leaf(deposit_cases[i].deposit_data_root)
            .expect("should push leaf");
        assert_eq!(
            deposit_tree.root(),
            deposit_cases[i].finalized_execution_block.deposit_root,
            "Deposit Tree Root Mismatch for deposit {}!",
            i
        );
        assert_eq!(
            full_tree.root(),
            deposit_cases[i].finalized_execution_block.deposit_root,
            "Full Tree Root Mismatch for deposit {}!",
            i
        );
        deposit_tree
            .finalize(deposit_cases[i].finalized_execution_block.clone())
            .expect("should finalize");
        assert_eq!(
            deposit_tree.get_snapshot(),
            Some(deposit_cases[i].snapshot.clone()),
            "Deposit Tree Snapshot Mismatch for deposit {}!",
            i
        );
        full_tree
            .finalize(deposit_cases[i].finalized_execution_block.clone())
            .expect("should finalize");
        assert_eq!(
            full_tree.get_snapshot(),
            Some(deposit_cases[i].snapshot.clone()),
            "Full Tree Snapshot Mismatch for deposit {}!",
            i
        );
    }

    for skip_count in 2..200 {
        let mut skip_tree = DepositDataTree::create(&[], 0, 32);
        for i in 0..deposit_cases.len() {
            skip_tree
                .push_leaf(deposit_cases[i].deposit_data_root)
                .expect("should push leaf");
            assert_eq!(
                skip_tree.root(),
                deposit_cases[i].finalized_execution_block.deposit_root,
                "Full Tree Root Mismatch for deposit {}!",
                i
            );
            if i != 0 && i % skip_count == 0 {
                skip_tree
                    .finalize(deposit_cases[i].finalized_execution_block.clone())
                    .expect("should finalize");
                assert_eq!(
                    skip_tree.get_snapshot(),
                    Some(deposit_cases[i].snapshot.clone()),
                    "Full Tree Snapshot Mismatch for deposit {}!",
                    i
                );
            } else if i > skip_count {
                let mut recovered =
                    DepositDataTree::from_snapshot(&skip_tree.get_snapshot().unwrap(), 32)
                        .expect("Should recover tree");
                assert_eq!(
                    recovered.root(),
                    deposit_cases[((i / skip_count) * skip_count)]
                        .finalized_execution_block
                        .deposit_root,
                    "Recovered Tree Root Mismatch for deposit {} i[{}]!",
                    ((i / skip_count) * skip_count),
                    i
                );

                for j in ((i / skip_count) * skip_count) + 1..i + 1 {
                    recovered
                        .push_leaf(deposit_cases[j].deposit_data_root)
                        .expect("should push leaf");
                    assert_eq!(
                        recovered.get_snapshot(),
                        Some(
                            deposit_cases[((i / skip_count) * skip_count)]
                                .snapshot
                                .clone()
                        ),
                        "Recovered Tree Snapshot Mismatch for deposit {}!",
                        ((i / skip_count) * skip_count)
                    );
                    assert_eq!(
                        recovered.root(),
                        deposit_cases[j].finalized_execution_block.deposit_root,
                        "Recovered Tree Root Mismatch for deposit {}!",
                        j
                    );
                }
                recovered
                    .finalize(deposit_cases[i].finalized_execution_block.clone())
                    .expect("should finalize");
                assert_eq!(
                    recovered.get_snapshot(),
                    Some(deposit_cases[i].snapshot.clone()),
                    "Recovered Tree Snapshot Mismatch for current deposit {}!",
                    i
                );
            }
        }
    }
    println!("All tests passed! :)");
}
