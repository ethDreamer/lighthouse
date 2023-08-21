use kzg::TrustedSetup;
use rand::thread_rng;
use types::{BlobSidecar, EthSpec, KzgCommitment, KzgProof, MainnetEthSpec, SigpBlob};

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


pub fn random_valid<R: Rng>(rng: &mut R, kzg: &Kzg<T::Kzg>) -> Result<Self, String> {
    let blob = SigpBlob::<T>::random_valid(rng)?;
    let kzg_blob = blob.c_kzg_blob();

    let commitment = kzg
        .blob_to_kzg_commitment(kzg_blob.clone())
        .map_err(|e| format!("error computing kzg commitment: {:?}", e))?;

    let proof = kzg
        .compute_blob_kzg_proof(kzg_blob, commitment)
        .map_err(|e| format!("error computing kzg proof: {:?}", e))?;

    Ok(Self {
        blob,
        kzg_commitment: commitment,
        kzg_proof: proof,
        ..Default::default()
    })
}


fn main() {
    // Get command line arguments.
    let iterations = parse_iterations_arg_or_default();
    println!("Number of iterations: {}", iterations);

    type E = MainnetEthSpec;
    let trusted_setup: TrustedSetup =
        serde_json::from_reader(TRUSTED_SETUP)
            .map_err(|e| format!("Unable to read trusted setup file: {}", e))
            .expect("should get trusted setup");

    let kzg_settings = c_kzg::KzgSettings::load_trusted_setup(
        trusted_setup.g1_points(),
        trusted_setup.g2_points(),
    ).expect("should load trusted setup");

    for i in 0..iterations {
        let sidecar = BlobSidecar::<E>::random_valid(&mut thread_rng(), &kzg).expect("should get random valid sidecar");
        let result = c_kzg::KzgProof::verify_blob_kzg_proof(
            sidecar.blob.c_kzg_blob(),
            sidecar.kzg_commitment.into(),
            sidecar.kzg_proof.into(),
            &kzg_settings,
        );

        match result {
            Ok(valid) => println!("Iteration {} validation result: {}", i, valid),
            Err(e) => println!("Iteration {} failed: {:?}", i, e), 
        }
    }
}