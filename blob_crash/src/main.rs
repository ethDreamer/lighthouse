use rand::{thread_rng, Rng};
use c_kzg::KzgSettings;

mod kzg_commitment;
mod kzg_proof;
mod trusted_setup;

use kzg_commitment::KzgCommitment;
use kzg_proof::KzgProof;
use trusted_setup::TrustedSetup;

const TRUSTED_SETUP: &[u8] =
    include_bytes!("../../common/eth2_network_config/built_in_network_configs/testing_trusted_setups.json");

fn parse_iterations_arg_or_default() -> usize {
    // Get command line arguments.
    let args: Vec<String> = std::env::args().collect();

    // If no argument is provided, return the default value.
    if args.len() <= 1 {
        return 128;
    }

    // Try to parse the first argument as an integer.
    match args[1].parse::<usize>() {
        Ok(iterations) => iterations,
        Err(_) => {
            print_usage_and_exit();
            unreachable!() // This line won't be executed since the function above exits the process.
        }
    }
}

fn print_usage_and_exit() {
    eprintln!("Usage: {} <number_of_iterations>", std::env::args().next().unwrap());
    std::process::exit(1);
}

pub fn random_valid_blob<R: Rng>(rng: &mut R) -> Result<c_kzg::Blob, String> {
    let mut blob_bytes = vec![0u8; c_kzg::BYTES_PER_BLOB];
    rng.fill_bytes(&mut blob_bytes);

    // Ensure that the blob is canonical by ensuring that
    // each field element contained in the blob is < BLS_MODULUS
    for i in 0..c_kzg::FIELD_ELEMENTS_PER_BLOB {
        let Some(byte) = blob_bytes.get_mut(i.checked_mul(c_kzg::BYTES_PER_FIELD_ELEMENT).ok_or("overflow".to_string())?)  else {
            return Err(format!("blob byte index out of bounds: {:?}", i));
        };
        *byte = 0;
    }
    c_kzg::Blob::from_bytes(&blob_bytes)
        .map_err(|e| format!("failed to create blob: {:?}", e))
}

fn random_valid_blob_components<R: Rng>(rng: &mut R, kzg_settings: &KzgSettings) -> Result<(c_kzg::Blob, KzgCommitment, KzgProof), String> {
    let blob = random_valid_blob(rng)
        .map_err(|e| format!("error generating valid blob: {:?}", e))?;
    let c_kzg_blob = &blob;

    let commitment = c_kzg::KzgCommitment::blob_to_kzg_commitment(c_kzg_blob.clone(), kzg_settings)
        .map(|com| KzgCommitment(com.to_bytes().into_inner()))
        .map_err(|e| format!("error computing kzg commitment: {:?}", e))?;

    let proof = c_kzg::KzgProof::compute_blob_kzg_proof(
        c_kzg_blob,
        commitment.into(),
        kzg_settings,
    )
        .map(|proof| KzgProof(proof.to_bytes().into_inner()))
        .map_err(|e| format!("error computing kzg proof: {:?}", e))?;

    Ok((blob, commitment, proof))
}

fn main() {
    // Get command line arguments.
    let iterations = parse_iterations_arg_or_default();
    println!("Number of iterations: {}", iterations);

    let trusted_setup: TrustedSetup =
        serde_json::from_reader(TRUSTED_SETUP)
            .map_err(|e| format!("Unable to read trusted setup file: {}", e))
            .expect("should get trusted setup");

    let kzg_settings = c_kzg::KzgSettings::load_trusted_setup(
        trusted_setup.g1_points(),
        trusted_setup.g2_points(),
    ).expect("should load trusted setup");

    for i in 0..iterations {
        let (blob, commitment, proof) = random_valid_blob_components(&mut thread_rng(), &kzg_settings).expect("should get blob components");
        let result = c_kzg::KzgProof::verify_blob_kzg_proof(
            &blob,
            commitment.into(),
            proof.into(),
            &kzg_settings,
        );

        match result {
            Ok(valid) => println!("Iteration {} validation result: {}", i, valid),
            Err(e) => println!("Iteration {} failed: {:?}", i, e), 
        }
    }
}
