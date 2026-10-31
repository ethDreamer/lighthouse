//! EIP-8453 validator-duty round trip over the HTTP API (minimal preset).
//!
//! Drives a single-node eip8453-genesis chain the way a validator client would: duties come from
//! the duty endpoints, every vote reads the fork choice through the data endpoints at its
//! deadline, messages are signed with the real domains and published through the pool endpoints,
//! and blocks pack them via the proposer-as-aggregator path. The only direct harness touches are
//! clock control (slot advance, fg ticks) and block production itself.

use std::collections::HashSet;

use http_api::test_utils::InteractiveTester;
use types::consts::gloas::PAYLOAD_STATUS_PENDING;
use types::eip8453::{
    self, HeadVoteMessage, HeightCheckpoint, SafeVoteMessage, compute_round_at_slot,
    compute_safe_vote_sub_round,
};
use types::{EthSpec, ForkName, Hash256, Slot, Spec};

type E = Spec;
const VALIDATOR_COUNT: usize = 64;

/// The naive honest finality pair: vote to finalize the justified checkpoint while it is above
/// the finalized height and below the target height. (The real VC reads this from the signing
/// record; full participation makes the naive rule safe here, matching the beacon_chain gate
/// test.)
fn naive_finality(
    justified: HeightCheckpoint,
    finalized_height: u64,
    target: HeightCheckpoint,
) -> HeightCheckpoint {
    if !justified.root.is_zero()
        && justified.height > finalized_height
        && justified.height < target.height
    {
        justified
    } else {
        HeightCheckpoint::empty()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eip8453_http_duty_round_trip() {
    let spec = ForkName::Eip8453.make_genesis_spec(E::default_spec());
    let tester = InteractiveTester::<E>::new(Some(spec.clone()), VALIDATOR_COUNT).await;
    let harness = &tester.harness;
    let client = &tester.client;
    let spec = harness.chain.spec.clone();

    let all_indices: Vec<u64> = (0..VALIDATOR_COUNT as u64).collect();
    let num_rounds = 6;
    let num_slots = num_rounds * E::slots_per_round();
    let slot_duration_ms = spec.get_slot_duration().as_millis() as u64;
    let genesis_time_ms = harness.chain.genesis_time * 1000;

    let (mut state, _) = harness.get_current_state_and_root();
    let mut tip_root = harness.chain.genesis_block_root;
    // The client-side stand-in for the FinalitySigningRecord's entry suppression: one height
    // vote per (validator, height).
    let mut signed_heights: HashSet<(u64, u64)> = HashSet::new();
    let mut packed_fg_blocks = 0usize;
    let mut packed_head_vote_blocks = 0usize;
    let mut packed_safe_vote_blocks = 0usize;

    for slot_number in 1..=num_slots {
        let slot = Slot::new(slot_number);
        let epoch = slot.epoch(E::slots_per_epoch());
        harness.advance_slot();
        harness.chain.recompute_head_at_current_slot().await;

        let head_state = harness.chain.head_snapshot().beacon_state.clone();
        let fork = head_state.fork();
        let genesis_validators_root = head_state.genesis_validators_root();

        // FG attestations: duties and the single fork-choice observation, over HTTP.
        let fg_duties = client
            .post_validator_duties_fg_attester(epoch, &all_indices)
            .await
            .expect("fg attester duties");
        let components = client
            .get_validator_eip8453_fg_attestation_components()
            .await
            .expect("fg attestation components")
            .data;
        let mut singles = Vec::new();
        for duty in fg_duties.data.iter().filter(|duty| duty.slot == slot) {
            let target = components.target;
            if target.root.is_zero()
                || !signed_heights.insert((duty.validator_index, target.height))
            {
                continue;
            }
            let data = eip8453::AttestationData {
                target,
                timeout: false,
                finality: naive_finality(components.justified, components.finalized_height, target),
            };
            let signing_root =
                data.signing_root_at_epoch(epoch, &fork, genesis_validators_root, &spec);
            let signature = harness.validator_keypairs[duty.validator_index as usize]
                .sk
                .sign(signing_root);
            singles.push(eip8453::SingleAttestation {
                committee_index: duty.committee_index,
                attester_index: duty.validator_index,
                data,
                signature: bls::AggregateSignature::from(&signature),
                epoch,
            });
        }
        if !singles.is_empty() {
            client
                .post_beacon_pool_eip8453_fg_attestations(&singles)
                .await
                .expect("publish fg attestations");
        }

        // Produce and import the block: packing aggregates the published singles.
        let (block_hash, _block_contents, new_state) =
            harness.add_block_at_slot(slot, state).await.unwrap();
        tip_root = Hash256::from(block_hash);
        let block = harness
            .chain
            .get_blinded_block(&tip_root)
            .unwrap()
            .expect("block is stored");
        if !block
            .message()
            .body()
            .attestations_eip8453()
            .unwrap()
            .is_empty()
        {
            packed_fg_blocks += 1;
        }
        if !block.message().body().head_votes().unwrap().is_empty() {
            packed_head_vote_blocks += 1;
        }
        if !block.message().body().safe_votes().unwrap().is_empty() {
            packed_safe_vote_blocks += 1;
        }

        // Head voting at the quarter: duties and `get_voter_head` over HTTP.
        let head_vote_duties = client
            .post_validator_duties_head_vote(epoch, &all_indices)
            .await
            .expect("head vote duties");
        let voter_head = client
            .get_validator_eip8453_head_vote_data(slot)
            .await
            .expect("head vote data")
            .data;
        assert_eq!(voter_head.slot, slot);
        // EIP8453-STUB (SPEC FINDING, friction #13): honest voting per `get_voter_head` never
        // names a fresh proposal (the walk stalls behind the parent's payload-flavor node), so
        // a chain voted that way is not live. Like the spec's own fork-choice tests, vote the
        // tip directly until the spec resolves the walk's flavor-node eligibility.
        let vote_data = eip8453::HeadVoteData {
            slot,
            beacon_block_root: tip_root,
            payload_status: PAYLOAD_STATUS_PENDING,
        };
        let messages: Vec<HeadVoteMessage> = head_vote_duties
            .data
            .iter()
            .filter(|duty| duty.slot == slot)
            .map(|duty| {
                let signing_root =
                    vote_data.compute_signing_root::<E>(&fork, genesis_validators_root, &spec);
                HeadVoteMessage {
                    validator_index: duty.validator_index,
                    data: vote_data,
                    signature: harness.validator_keypairs[duty.validator_index as usize]
                        .sk
                        .sign(signing_root),
                }
            })
            .collect();
        assert!(
            !messages.is_empty(),
            "the head vote committee of slot {slot} has local duties"
        );
        client
            .post_beacon_pool_eip8453_head_votes(&messages)
            .await
            .expect("publish head votes");

        // Advance the FG clock past the slot's cutoffs (grades and the previous slot's
        // confirmation evaluate), as in the beacon_chain gate test.
        let mid_slot_ms =
            genesis_time_ms + slot.as_u64() * slot_duration_ms + slot_duration_ms * 2600 / 10_000;
        harness
            .chain
            .canonical_head
            .fork_choice_write_lock()
            .fg_on_tick_ms(mid_slot_ms)
            .unwrap();

        // Safe voting in the assigned sub-round: `get_safe_vote_node` over HTTP.
        let round = compute_round_at_slot::<E>(slot);
        let sub_round = slot.as_u64() % E::slots_per_round();
        if sub_round < E::slots_per_round().saturating_sub(2) {
            let safe_vote_data = client
                .get_validator_eip8453_safe_vote_data()
                .await
                .expect("safe vote data")
                .data;
            assert_eq!(safe_vote_data.round, round);
            let messages: Vec<SafeVoteMessage> = all_indices
                .iter()
                .filter(|&&index| compute_safe_vote_sub_round::<E>(index) == sub_round)
                .map(|&index| {
                    let signing_root = safe_vote_data.compute_signing_root::<E>(
                        &fork,
                        genesis_validators_root,
                        &spec,
                    );
                    SafeVoteMessage {
                        validator_index: index,
                        data: safe_vote_data,
                        signature: harness.validator_keypairs[index as usize]
                            .sk
                            .sign(signing_root),
                    }
                })
                .collect();
            client
                .post_beacon_pool_eip8453_safe_votes(&messages)
                .await
                .expect("publish safe votes");
        }

        state = new_state;
    }

    // One more slot so the final slot's votes become the generation the walk reads.
    harness.advance_slot();
    harness.chain.recompute_head_at_current_slot().await;
    assert_eq!(
        harness.chain.canonical_head.cached_head().head_block_root(),
        tip_root,
        "head follows the voted tip"
    );

    // The finality gadget advanced on HTTP-fed votes alone, and the stabilization layer
    // reports a fresh safe quorum (grade-2 compatible with the FG root).
    let (fg_finalized, fg_justified, fresh_quorum) = {
        let fork_choice = harness.chain.canonical_head.fork_choice_read_lock();
        let fg = fork_choice.fg().expect("fg active");
        (
            fg.finalized_checkpoint,
            fg.justified_checkpoint,
            fg.has_fresh_safe_quorum().unwrap(),
        )
    };
    assert!(fresh_quorum, "fresh safe quorum on the healthy chain");
    assert!(
        fg_finalized.height > 1,
        "finalized height advanced: {fg_finalized:?}"
    );
    assert!(
        fg_justified.height > 2,
        "justified height advanced: {fg_justified:?}"
    );

    // The blocks packed the HTTP-published objects.
    assert!(
        packed_fg_blocks > num_slots as usize / 2,
        "most blocks pack FG attestations ({packed_fg_blocks}/{num_slots})"
    );
    assert!(
        packed_head_vote_blocks > num_slots as usize / 2,
        "most blocks pack head votes ({packed_head_vote_blocks}/{num_slots})"
    );
    assert!(
        packed_safe_vote_blocks > 0,
        "some blocks pack safe votes ({packed_safe_vote_blocks}/{num_slots})"
    );

    // An invalid publish is rejected: a vote from a validator outside the committee.
    let bogus = HeadVoteMessage {
        validator_index: u64::MAX,
        data: eip8453::HeadVoteData {
            slot: Slot::new(num_slots),
            beacon_block_root: tip_root,
            payload_status: PAYLOAD_STATUS_PENDING,
        },
        signature: bls::Signature::empty(),
    };
    client
        .post_beacon_pool_eip8453_head_votes(&[bogus])
        .await
        .expect_err("a non-member head vote is rejected");
}
