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
        self, HeadVoteData, HeightCheckpoint, SafeVoteData, compute_attestation_committee_slot,
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

        // Advance the finality gadget clock past the slot's support cutoff (real nodes tick
        // continuously): the confirmation of the previous slot evaluates, so safe votes cast
        // below name a confirmed node only one slot behind — which keeps the stabilization
        // grades on the FG root in the healthy case.
        {
            let slot_duration_ms = spec.get_slot_duration().as_millis() as u64;
            let genesis_time_ms = harness.chain.genesis_time * 1000;
            let mid_slot_ms = genesis_time_ms
                + slot.as_u64() * slot_duration_ms
                + slot_duration_ms * 2600 / 10_000;
            harness
                .chain
                .canonical_head
                .fork_choice_write_lock()
                .fg_on_tick_ms(mid_slot_ms)
                .unwrap();
        }

        // Honest safe voting: each validator casts one safe vote per round in its sub-round,
        // naming the node the fork choice's safe-vote selection reports.
        let round = compute_round_at_slot::<E>(slot);
        let sub_round = slot.as_u64() % E::slots_per_round();
        if sub_round < E::slots_per_round().saturating_sub(2) {
            let mut fork_choice = harness.chain.canonical_head.fork_choice_write_lock();
            let safe_vote_node = fork_choice
                .fg()
                .expect("fg active")
                .get_safe_vote_node()
                .expect("safe vote node");
            for validator_index in 0..VALIDATOR_COUNT as u64 {
                if validator_index % (E::slots_per_round().saturating_sub(2)) == sub_round {
                    fork_choice.on_safe_vote(
                        validator_index,
                        SafeVoteData {
                            round,
                            beacon_block_root: safe_vote_node.root,
                            payload_status: safe_vote_node.payload_status,
                        },
                    );
                }
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

    // Phase 4b: the stabilization grades formed from the safe votes, the confirmation records
    // advanced, and the four tips nest.
    {
        let fork_choice = harness.chain.canonical_head.fork_choice_read_lock();
        let fg = fork_choice.fg().expect("fg fork choice is active");

        assert!(fg.grade_2.is_some(), "grade 2 is evaluated");
        assert!(fg.grade_1.is_some(), "grade 1 is evaluated");
        assert!(fg.grade_0.is_some(), "grade 0 is evaluated");
        // Spec commit `280200b41` (fixing a finding from this prototype): the quorum test is
        // root *compatibility* with the FG root, not an active prefix — on a healthy chain
        // the grade-2 node trails the justified root as its ancestor, so height pairs flow.
        assert!(
            fg.has_fresh_safe_quorum().unwrap(),
            "an ancestor grade-2 node is a fresh safe quorum on the healthy chain"
        );

        let genesis_root = harness.chain.genesis_block_root;
        let stable = fg.get_stable();
        let confirmed = fg.get_confirmed();
        assert_ne!(stable.root, genesis_root, "the stable tip advanced");
        assert_ne!(confirmed.root, genesis_root, "the confirmed tip advanced");

        // finalized ⪯ stable ⪯ confirmed ⪯ head.
        let finalized = fg.finalized_checkpoint.root;
        assert!(
            fg.is_ancestor_root(finalized, stable.root).unwrap(),
            "finalized ⪯ stable"
        );
        assert!(
            fg.is_ancestor_root(stable.root, confirmed.root).unwrap(),
            "stable ⪯ confirmed"
        );
        assert!(
            fg.is_ancestor_root(confirmed.root, head_root).unwrap(),
            "confirmed ⪯ head"
        );

        // The AC voting view agrees with the walk at the final slot.
        let voter_head = fg.get_voter_head(fg.get_current_slot()).unwrap();
        assert_eq!(voter_head.root, tip_root, "voter head follows the tip");

        // The FG attestation sources resolve.
        assert!(
            fg.is_ancestor_root(fg.fg_target_source_root(), head_root)
                .unwrap(),
            "target source on the canonical chain"
        );
        // The finality source reads the *current* slot's votes (head voting precedes FG
        // attesting within a slot); with no slot-25 votes cast, the walk conservatively stays
        // on the canonical chain behind the tip.
        let finality_source = fg.fg_finality_source().unwrap();
        assert!(
            fg.is_ancestor_root(finality_source.root, head_root)
                .unwrap(),
            "finality source is on the canonical chain"
        );
    }

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
