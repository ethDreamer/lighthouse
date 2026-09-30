use ssz::{Decode, Encode};
use ssz_types::{ProgressiveVariableList, typenum::Unsigned};
use types::{test_utils::test_arbitrary_instance, *};

fn round_trip<T: Encode + Decode>(value: &T) -> Result<T, ssz::DecodeError> {
    T::from_ssz_bytes(&value.as_ssz_bytes())
}

fn check_over_limit<T: Clone + Encode + Decode + 'static, N: Unsigned>(
    list: &ProgressiveVariableList<T, N>,
    item: T,
) {
    let mut values = list.clone().into_vec();
    values.push(item);
    // Encode the oversized list as a Vec: the bounded list cannot be constructed anymore.
    assert!(ProgressiveVariableList::<T, N>::from_ssz_bytes(&values.as_ssz_bytes()).is_err());
    assert!(ProgressiveVariableList::<T, N>::new(values).is_err());
}

fn check_limits() {
    // Exercise the enclosing wire containers, including nested variable-length lists.
    macro_rules! check {
        ($container:expr, $($field:ident).+, $limit:ty) => {{
            let mut value = $container.clone();
            let item = test_arbitrary_instance();
            value.$($field).+ = ProgressiveVariableList::new(vec![item; <$limit>::to_usize()]).unwrap();
            assert!(round_trip(&value).is_ok(), stringify!($($field).+));
            check_over_limit(&value.$($field).+, test_arbitrary_instance());
            assert!(value.$($field).+.push(test_arbitrary_instance()).is_err());
            assert_eq!(value.$($field).+.len(), <$limit>::to_usize());
            assert!(round_trip(&value).is_ok(), stringify!($($field).+));
        }};
    }

    let spec = ForkName::Gloas.make_genesis_spec(Spec::default_spec());
    let block = BeaconBlockGloas::<FullPayload>::empty(&spec);
    check!(block, body.proposer_slashings, Spec::MAX_PROPOSER_SLASHINGS);
    check!(
        block,
        body.attester_slashings,
        Spec::MAX_ATTESTER_SLASHINGS_ELECTRA
    );
    check!(block, body.attestations, Spec::MAX_ATTESTATIONS_ELECTRA);
    check!(block, body.deposits, Spec::MAX_DEPOSITS);
    check!(block, body.voluntary_exits, Spec::MAX_VOLUNTARY_EXITS);
    check!(
        block,
        body.bls_to_execution_changes,
        Spec::MAX_BLS_TO_EXECUTION_CHANGES
    );
    check!(block, body.payload_attestations, Spec::MAX_PAYLOAD_ATTESTATIONS);
    check!(
        block,
        body.signed_execution_payload_bid
            .message
            .blob_kzg_commitments,
        Spec::MAX_BLOB_COMMITMENTS_PER_BLOCK
    );

    let envelope = ExecutionPayloadEnvelope::empty();
    check!(envelope, payload.withdrawals, Spec::MAX_WITHDRAWALS_PER_PAYLOAD);
    check!(
        envelope,
        execution_requests.withdrawals,
        Spec::MAX_WITHDRAWAL_REQUESTS_PER_PAYLOAD
    );
    check!(
        envelope,
        execution_requests.consolidations,
        Spec::MAX_CONSOLIDATION_REQUESTS_PER_PAYLOAD
    );
    check!(
        envelope,
        execution_requests.builder_deposits,
        Spec::MAX_BUILDER_DEPOSIT_REQUESTS_PER_PAYLOAD
    );
    check!(
        envelope,
        execution_requests.builder_exits,
        Spec::MAX_BUILDER_EXIT_REQUESTS_PER_PAYLOAD
    );

    // Gloas deliberately removed the Electra deposit-request limit.
    let mut envelope = envelope;
    envelope.execution_requests.deposits = ProgressiveVariableList::new(vec![
        test_arbitrary_instance(); Spec::MAX_DEPOSIT_REQUESTS_PER_PAYLOAD + 1
    ])
    .unwrap();
    assert!(round_trip(&envelope).is_ok());

    let attestation: IndexedAttestationGloas = test_arbitrary_instance();
    check!(attestation, attesting_indices, Spec::MAX_VALIDATORS_PER_SLOT);
    let column: DataColumnSidecarGloas = test_arbitrary_instance();
    check!(column, column, Spec::MAX_BLOB_COMMITMENTS_PER_BLOCK);
    check!(column, kzg_proofs, Spec::MAX_BLOB_COMMITMENTS_PER_BLOCK);
    let partial_column: PartialDataColumnSidecarGloas = test_arbitrary_instance();
    check!(partial_column, column, Spec::MAX_BLOB_COMMITMENTS_PER_BLOCK);
    check!(partial_column, kzg_proofs, Spec::MAX_BLOB_COMMITMENTS_PER_BLOCK);
}

#[test]
fn progressive_list_limits() {
    check_limits();
    check_limits();
    check_limits();
}

#[test]
fn progressive_block_body_errors_propagate() {
    for fork in [ForkName::Gloas, ForkName::Heze] {
        let spec = fork.make_genesis_spec(Spec::default_spec());
        let mut block = BeaconBlock::<FullPayload>::empty(&spec);
        let mut body = block.body_mut();
        body.set_deposits_from_iter(vec![
            test_arbitrary_instance();
            Spec::MAX_DEPOSITS
        ])
        .unwrap();
        assert!(matches!(
            body.deposits_push(test_arbitrary_instance()),
            Err(BeaconStateError::SszTypesError(_))
        ));
        assert!(matches!(
            body.set_deposits_from_iter(vec![
                test_arbitrary_instance();
                Spec::MAX_DEPOSITS + 1
            ]),
            Err(BeaconStateError::SszTypesError(_))
        ));

        for _ in 0..Spec::MAX_PROPOSER_SLASHINGS {
            body.proposer_slashings_push(test_arbitrary_instance())
                .unwrap();
        }
        assert!(matches!(
            body.proposer_slashings_push(test_arbitrary_instance()),
            Err(BeaconStateError::SszTypesError(_))
        ));

        for _ in 0..Spec::MAX_VOLUNTARY_EXITS {
            body.voluntary_exits_push(test_arbitrary_instance())
                .unwrap();
        }
        assert!(matches!(
            body.voluntary_exits_push(test_arbitrary_instance()),
            Err(BeaconStateError::SszTypesError(_))
        ));
        assert_eq!(
            block.body().deposits().len(),
            Spec::MAX_DEPOSITS
        );
    }
}

#[test]
fn progressive_column_min_size_matches_encoding() {
    let sidecar = DataColumnSidecarGloas {
        index: 0,
        column: ProgressiveVariableList::new(vec![Cell::default()]).unwrap(),
        kzg_proofs: ProgressiveVariableList::new(vec![KzgProof::empty()]).unwrap(),
        slot: Slot::new(0),
        beacon_block_root: Hash256::ZERO,
    };
    assert_eq!(
        DataColumnSidecarGloas::min_size(),
        sidecar.as_ssz_bytes().len()
    );
}
