//! Gate test for EIP-8453 Phase 4a: a single-node chain from eip8453 genesis produces and
//! imports blocks across rounds, head votes drive the finality gadget fork choice, FG
//! attestations flow through the dumb pool into blocks, and finality advances.

use beacon_chain::test_utils::BeaconChainHarness;
use ssz::{BitVector, ProgressiveBitList};
use state_processing::per_slot_processing;
use state_processing::upgrade::gloas::GloasVerificationContext;
use std::collections::BTreeSet;
use std::sync::Arc;
use types::consts::gloas::PAYLOAD_STATUS_PENDING;
use types::{
    BeaconState, ChainSpec, EthSpec, ForkName, Hash256, MinimalEthSpec, Slot,
    eip8453::{
        self, HeadVoteData, HeightCheckpoint, compute_attestation_committee_slot,
        compute_round_at_slot,
    },
};

type E = MinimalEthSpec;

const VALIDATOR_COUNT: usize = 32;

/// Sign a full-committee FG attestation for every attestation committee scheduled at the
/// state's slot.
fn make_fg_attestations(
    harness: &BeaconChainHarness<beacon_chain::test_utils::EphemeralHarnessType<E>>,
    state: &BeaconState<E>,
    spec: &ChainSpec,
) -> Vec<eip8453::Attestation<E>> {
    let slot = state.slot();
    let epoch = state.current_epoch();
    let round = compute_round_at_slot::<E>(slot);
    let target = state.target_checkpoint().unwrap();
    assert!(!target.root.is_zero(), "target root is back-filled");

    // The finality pair: vote to finalize the justified checkpoint while it is pending
    // (the validator guide reads this from the signing record; full participation makes the
    // naive rule safe here).
    let justified = state.justified_checkpoint().unwrap();
    let finality = if !justified.root.is_zero()
        && justified.height < target.height
        && state
            .pending_finality()
            .unwrap()
            .iter()
            .any(|pending| pending.checkpoint == justified)
    {
        justified
    } else {
        HeightCheckpoint::empty()
    };

    let data = eip8453::AttestationData {
        target,
        timeout: false,
        finality,
    };
    let signing_root =
        data.signing_root_at_epoch(epoch, &state.fork(), state.genesis_validators_root(), spec);

    let mut attestations = Vec::new();
    for committee_index in 0..E::attestation_committees_per_round() as u64 {
        if compute_attestation_committee_slot::<E>(round, committee_index) != slot {
            continue;
        }
        let committee = state
            .get_attestation_committee(epoch, committee_index, spec)
            .unwrap();
        if committee.is_empty() {
            continue;
        }

        let mut signature = bls::AggregateSignature::infinity();
        for &validator_index in &committee {
            signature.add_assign(
                &harness.validator_keypairs[validator_index]
                    .sk
                    .sign(signing_root),
            );
        }
        let mut aggregation_bits = ProgressiveBitList::with_capacity(committee.len());
        for i in 0..committee.len() {
            aggregation_bits.set(i, true).unwrap();
        }
        let mut committee_bits = BitVector::new();
        committee_bits.set(committee_index as usize, true).unwrap();

        attestations.push(eip8453::Attestation {
            aggregation_bits,
            data,
            signature,
            committee_bits,
            epoch,
        });
    }
    attestations
}

#[tokio::test]
async fn eip8453_single_node_chain_finalizes() {
    let spec = Arc::new(ForkName::Eip8453.make_genesis_spec(E::default_spec()));
    let harness = BeaconChainHarness::builder(E::default())
        .spec(spec.clone())
        .deterministic_keypairs(VALIDATOR_COUNT)
        .fresh_ephemeral_store()
        .mock_execution_layer()
        .build();

    let num_rounds = 6;
    let num_slots = num_rounds * E::slots_per_round();

    let (mut state, _) = harness.get_current_state_and_root();
    let mut tip_root = harness.chain.genesis_block_root;

    for slot_number in 1..=num_slots {
        let slot = Slot::new(slot_number);
        harness.advance_slot();

        // The slot-start fork choice run: the previous slot's head votes are now the complete
        // generation the walk reads, so the cached head advances to the voted tip before
        // production reads it.
        harness.chain.recompute_head_at_current_slot().await;
        if slot_number > 1 {
            assert_eq!(
                harness.chain.canonical_head.cached_head().head_block_root(),
                tip_root,
                "the walk follows the previous slot's votes at slot {slot}"
            );
        }

        // Craft FG attestations against the packing state (the previous post-state advanced to
        // this slot) and queue them in the dumb pool for the block we are about to produce.
        let mut attestation_state = state.clone();
        while attestation_state.slot() < slot {
            per_slot_processing(
                &mut attestation_state,
                None,
                GloasVerificationContext::FullVerification,
                &spec,
            )
            .unwrap();
        }
        for attestation in make_fg_attestations(&harness, &attestation_state, &spec) {
            harness
                .chain
                .eip8453_pool
                .insert_fg_attestation(attestation);
        }

        // Produce and import the block for this slot.
        let (block_hash, _block_contents, new_state) =
            harness.add_block_at_slot(slot, state).await.unwrap();
        tip_root = Hash256::from(block_hash);

        // The block carries the queued FG attestations.
        let block = harness
            .chain
            .get_blinded_block(&tip_root)
            .unwrap()
            .expect("block is stored");
        assert!(
            !block
                .message()
                .body()
                .attestations_eip8453()
                .unwrap()
                .is_empty(),
            "block at slot {slot} packs FG attestations"
        );

        // Honest head voting: the slot's head-vote committee votes for the new proposal
        // (same-slot votes carry PAYLOAD_STATUS_PENDING). This is the gossip path the harness
        // simulates by injecting directly into fork choice.
        let committee = new_state.get_head_vote_committee(slot, &spec).unwrap();
        let unique_voters: BTreeSet<usize> = committee.into_iter().collect();
        {
            let mut fork_choice = harness.chain.canonical_head.fork_choice_write_lock();
            for validator_index in unique_voters {
                fork_choice.on_head_vote(
                    validator_index as u64,
                    HeadVoteData {
                        slot,
                        beacon_block_root: tip_root,
                        payload_status: PAYLOAD_STATUS_PENDING,
                    },
                );
            }
        }

        state = new_state;
    }

    // One more slot so the final slot's head votes become the "previous slot" generation the
    // head walk reads.
    harness.advance_slot();
    harness.chain.recompute_head_at_current_slot().await;

    // The goldfish walk follows the voted tip.
    let head_root = harness.chain.canonical_head.cached_head().head_block_root();
    assert_eq!(head_root, tip_root, "head follows the voted tip");

    // The finality gadget advanced: with full participation each round justifies its target and
    // finalizes a pending justification shortly after.
    let (fg_finalized, fg_justified, fg_max_height) = {
        let fork_choice = harness.chain.canonical_head.fork_choice_read_lock();
        let fg = fork_choice.fg().expect("fg fork choice is active");
        (
            fg.finalized_checkpoint,
            fg.justified_checkpoint,
            fg.max_height,
        )
    };
    assert!(
        fg_max_height > 3,
        "heights advance across rounds (max height {fg_max_height})"
    );
    assert!(
        fg_justified.height > 2,
        "justification advances (justified height {})",
        fg_justified.height
    );
    assert!(
        fg_finalized.height > 1,
        "finalization advances (finalized height {})",
        fg_finalized.height
    );

    // The state agrees with the store.
    assert_eq!(
        state.finalized_checkpoint_eip8453().unwrap().height,
        fg_finalized.height,
        "store and state agree on finality"
    );

    // The synthesized epoch-based view (pruning/migration compat) also advanced.
    let finalized_epoch = harness
        .chain
        .canonical_head
        .cached_head()
        .finalized_checkpoint()
        .epoch;
    assert!(
        finalized_epoch > 0,
        "synthesized finalized epoch advances (epoch {finalized_epoch})"
    );
}
