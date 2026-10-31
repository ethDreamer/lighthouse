//! Gate test for EIP-8453: a single-node chain from eip8453 genesis produces and imports blocks
//! across rounds, the head-vote committee honestly votes the vote walk's own output
//! (`get_ac_vote_head`), FG attestations flow through the dumb pool into blocks, and finality
//! advances.

use beacon_chain::eip8453_gossip::Eip8453GossipError;
use beacon_chain::test_utils::BeaconChainHarness;
use ssz::{BitVector, ProgressiveBitList};
use ssz_types::{ProgressiveVariableList, VariableList};
use state_processing::per_slot_processing;
use state_processing::upgrade::gloas::GloasVerificationContext;
use std::collections::BTreeSet;
use std::sync::Arc;
use types::Domain;
use types::consts::gloas::{PAYLOAD_STATUS_EMPTY, PAYLOAD_STATUS_FULL, PAYLOAD_STATUS_PENDING};
use types::{
    BeaconState, ChainSpec, EthSpec, ForkName, Hash256, MinimalEthSpec, Slot,
    eip8453::{
        self, HeadVoteData, HeadVoteMessage, HeightCheckpoint, SafeVoteData, SafeVoteMessage,
        compute_attestation_committee_slot, compute_round_at_slot,
        compute_subnet_for_fg_attestation, head_vote_aggregator_selection_root,
        is_selected_aggregator, round_aggregator_selection_root,
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

        // Honest head voting at the head-vote deadline: tick the finality gadget to the slot's
        // +Δ and have each committee member vote the node the vote walk itself names
        // (`get_ac_vote_head`; same-slot heads are named pending). This is the gossip path the
        // harness simulates by injecting directly into fork choice.
        let slot_duration_ms = spec.get_slot_duration().as_millis() as u64;
        let genesis_time_ms = harness.chain.genesis_time * 1000;
        let slot_start_ms = genesis_time_ms + slot.as_u64() * slot_duration_ms;
        let committee = new_state.get_head_vote_committee(slot, &spec).unwrap();
        let unique_voters: BTreeSet<usize> = committee.into_iter().collect();
        {
            let mut fork_choice = harness.chain.canonical_head.fork_choice_write_lock();
            fork_choice
                .fg_on_tick_ms(slot_start_ms + spec.get_head_vote_due().as_millis() as u64)
                .unwrap();
            let fg = fork_choice.fg().expect("fg active");
            let vote_head = fg.get_ac_vote_head().expect("vote head resolves");
            assert_eq!(
                vote_head.root, tip_root,
                "the vote walk reaches the fresh proposal at slot {slot}"
            );
            let payload_status = if fg.block_slot(&vote_head.root) == Some(slot) {
                PAYLOAD_STATUS_PENDING
            } else {
                vote_head.payload_status
            };
            for validator_index in unique_voters {
                fork_choice.on_head_vote(
                    validator_index as u64,
                    HeadVoteData {
                        slot,
                        beacon_block_root: vote_head.root,
                        payload_status,
                    },
                );
            }
        }

        // Honest safe voting (also a +Δ duty): each validator casts one safe vote per round in
        // its sub-round, naming the node the fork choice's safe-vote selection reports.
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

        // Advance the finality gadget clock to the slot's support cutoff (real nodes tick
        // continuously): the confirmation of the previous slot evaluates.
        harness
            .chain
            .canonical_head
            .fork_choice_write_lock()
            .fg_on_tick_ms(slot_start_ms + spec.get_support_cutoff().as_millis() as u64)
            .unwrap();

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

    // The stabilization grades formed from the safe votes, the confirmation records advanced,
    // and the four tips nest.
    {
        // Tick to the final slot's head-vote deadline so the duty reads below run at their
        // scheduled offset.
        let slot_duration_ms = spec.get_slot_duration().as_millis() as u64;
        let genesis_time_ms = harness.chain.genesis_time * 1000;
        let vote_due_ms = genesis_time_ms
            + (num_slots + 1) * slot_duration_ms
            + spec.get_head_vote_due().as_millis() as u64;
        harness
            .chain
            .canonical_head
            .fork_choice_write_lock()
            .fg_on_tick_ms(vote_due_ms)
            .unwrap();

        let fork_choice = harness.chain.canonical_head.fork_choice_read_lock();
        let fg = fork_choice.fg().expect("fg fork choice is active");

        assert!(fg.grade_2.is_some(), "grade 2 is evaluated");
        assert!(fg.grade_1.is_some(), "grade 1 is evaluated");
        assert!(fg.grade_0.is_some(), "grade 0 is evaluated");
        // The quorum test is *compatibility* of the grade-2 node with the FG root: on a healthy
        // chain the grade-2 node trails the justified root as its ancestor, so height pairs flow.
        assert!(
            fg.has_fresh_safe_quorum().unwrap(),
            "an ancestor grade-2 node is a fresh safe quorum on the healthy chain"
        );

        let genesis_root = harness.chain.genesis_block_root;
        let stable = fg.get_stable();
        let confirmed = fg.get_confirmed_node().unwrap();
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

        // The AC vote walk agrees with the head at the final slot.
        let voter_head = fg.get_ac_vote_head().unwrap();
        assert_eq!(voter_head.root, tip_root, "voter head follows the tip");

        // The FG attestation sources resolve.
        assert!(
            fg.is_ancestor_root(fg.fg_target_source_root().unwrap(), head_root)
                .unwrap(),
            "target source on the canonical chain"
        );
        // The finality source is `get_head`, which before the support cutoff reads the previous
        // slot's votes; those votes name the tip.
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

/// Gate test for Phase 6 stage 6b: the p2p spec's gossip validation conditions, exercised by
/// injecting valid and invalid head votes, safe votes, FG attestation singles, and attester
/// slashings against a live chain.
#[tokio::test]
async fn eip8453_gossip_validation() {
    let spec = Arc::new(ForkName::Eip8453.make_genesis_spec(E::default_spec()));
    let harness = BeaconChainHarness::builder(E::default())
        .spec(spec.clone())
        .deterministic_keypairs(VALIDATOR_COUNT)
        .fresh_ephemeral_store()
        .mock_execution_layer()
        .build();

    // One block at slot 1 so the clock, the fork choice, and the head state agree on a tip.
    let (state, _) = harness.get_current_state_and_root();
    harness.advance_slot();
    harness.chain.recompute_head_at_current_slot().await;
    let slot = Slot::new(1);
    let (block_hash, _, state) = harness.add_block_at_slot(slot, state).await.unwrap();
    let tip_root = Hash256::from(block_hash);
    let genesis_root = harness.chain.genesis_block_root;

    let sign_head_vote = |validator_index: usize, data: HeadVoteData| -> HeadVoteMessage {
        let signing_root =
            data.compute_signing_root::<E>(&state.fork(), state.genesis_validators_root(), &spec);
        HeadVoteMessage {
            validator_index: validator_index as u64,
            data,
            signature: harness.validator_keypairs[validator_index]
                .sk
                .sign(signing_root),
        }
    };
    let sign_safe_vote = |validator_index: usize, data: SafeVoteData| -> SafeVoteMessage {
        let signing_root =
            data.compute_signing_root::<E>(&state.fork(), state.genesis_validators_root(), &spec);
        SafeVoteMessage {
            validator_index: validator_index as u64,
            data,
            signature: harness.validator_keypairs[validator_index]
                .sk
                .sign(signing_root),
        }
    };

    // === head_vote topic ===
    let committee: BTreeSet<usize> = state
        .get_head_vote_committee(slot, &spec)
        .unwrap()
        .into_iter()
        .collect();
    let mut members = committee.iter().copied();
    let voter_a = members.next().expect("committee has a first member");
    let voter_b = members.next().expect("committee has a second member");
    let non_member = (0..VALIDATOR_COUNT)
        .find(|index| !committee.contains(index))
        .expect("some validator is off the committee");
    let same_slot_vote = HeadVoteData {
        slot,
        beacon_block_root: tip_root,
        payload_status: PAYLOAD_STATUS_PENDING,
    };

    // A valid same-slot vote passes, and its replay is ignored as already seen.
    harness
        .chain
        .eip8453_verify_head_vote_for_gossip(&sign_head_vote(voter_a, same_slot_vote))
        .expect("valid head vote");
    assert!(matches!(
        harness
            .chain
            .eip8453_verify_head_vote_for_gossip(&sign_head_vote(voter_a, same_slot_vote)),
        Err(Eip8453GossipError::PriorHeadVoteKnown { .. })
    ));

    // A same-slot vote must name a pending payload status.
    assert!(matches!(
        harness
            .chain
            .eip8453_verify_head_vote_for_gossip(&sign_head_vote(
                voter_b,
                HeadVoteData {
                    payload_status: PAYLOAD_STATUS_FULL,
                    ..same_slot_vote
                }
            )),
        Err(Eip8453GossipError::InvalidPayloadStatus(_))
    ));

    // A vote for an earlier block must carry a payload decision, not PENDING.
    assert!(matches!(
        harness
            .chain
            .eip8453_verify_head_vote_for_gossip(&sign_head_vote(
                voter_b,
                HeadVoteData {
                    beacon_block_root: genesis_root,
                    ..same_slot_vote
                }
            )),
        Err(Eip8453GossipError::InvalidPayloadStatus(_))
    ));

    // A vote for an unseen block is ignored (and may be queued for lookup).
    assert!(matches!(
        harness
            .chain
            .eip8453_verify_head_vote_for_gossip(&sign_head_vote(
                voter_b,
                HeadVoteData {
                    beacon_block_root: Hash256::repeat_byte(0xab),
                    ..same_slot_vote
                }
            )),
        Err(Eip8453GossipError::UnknownBlock(_))
    ));

    // A vote outside the current slot (with disparity) is ignored.
    assert!(matches!(
        harness
            .chain
            .eip8453_verify_head_vote_for_gossip(&sign_head_vote(
                voter_b,
                HeadVoteData {
                    slot: Slot::new(5),
                    ..same_slot_vote
                }
            )),
        Err(Eip8453GossipError::NotCurrentSlot { .. })
    ));

    // A non-member is rejected.
    assert!(matches!(
        harness
            .chain
            .eip8453_verify_head_vote_for_gossip(&sign_head_vote(non_member, same_slot_vote)),
        Err(Eip8453GossipError::NotInCommittee { .. })
    ));

    // A bad signature is rejected: valid data signed by the wrong key.
    let mut forged = sign_head_vote(non_member, same_slot_vote);
    forged.validator_index = voter_b as u64;
    assert!(matches!(
        harness.chain.eip8453_verify_head_vote_for_gossip(&forged),
        Err(Eip8453GossipError::InvalidSignature(_))
    ));

    // === safe_vote topic ===
    let round = compute_round_at_slot::<E>(slot);
    let safe_voter = 0usize;
    let first_vote = SafeVoteData {
        round,
        beacon_block_root: tip_root,
        payload_status: PAYLOAD_STATUS_PENDING,
    };

    // The first vote passes; its exact replay is ignored.
    harness
        .chain
        .eip8453_verify_safe_vote_for_gossip(&sign_safe_vote(safe_voter, first_vote))
        .expect("valid safe vote");
    assert!(matches!(
        harness
            .chain
            .eip8453_verify_safe_vote_for_gossip(&sign_safe_vote(safe_voter, first_vote)),
        Err(Eip8453GossipError::DuplicateSafeVote { .. })
    ));

    // A second distinct vote from the same validator propagates: it is equivocation evidence.
    let second_vote = SafeVoteData {
        beacon_block_root: genesis_root,
        ..first_vote
    };
    harness
        .chain
        .eip8453_verify_safe_vote_for_gossip(&sign_safe_vote(safe_voter, second_vote))
        .expect("the second distinct safe vote propagates as equivocation evidence");

    // A third distinct vote is suppressed.
    assert!(matches!(
        harness
            .chain
            .eip8453_verify_safe_vote_for_gossip(&sign_safe_vote(
                safe_voter,
                SafeVoteData {
                    beacon_block_root: genesis_root,
                    payload_status: PAYLOAD_STATUS_EMPTY,
                    ..first_vote
                }
            )),
        Err(Eip8453GossipError::SafeVoteEquivocationLimit { .. })
    ));

    // A vote for another round is ignored; an unassigned payload status is rejected.
    assert!(matches!(
        harness
            .chain
            .eip8453_verify_safe_vote_for_gossip(&sign_safe_vote(
                1,
                SafeVoteData {
                    round: round + 5,
                    ..first_vote
                }
            )),
        Err(Eip8453GossipError::NotCurrentRound { .. })
    ));
    assert!(matches!(
        harness
            .chain
            .eip8453_verify_safe_vote_for_gossip(&sign_safe_vote(
                1,
                SafeVoteData {
                    payload_status: 7,
                    ..first_vote
                }
            )),
        Err(Eip8453GossipError::InvalidPayloadStatus(_))
    ));

    // A bad signature is rejected.
    let mut forged = sign_safe_vote(2, first_vote);
    forged.validator_index = 3;
    assert!(matches!(
        harness.chain.eip8453_verify_safe_vote_for_gossip(&forged),
        Err(Eip8453GossipError::InvalidSignature(_))
    ));

    // === attestation_{subnet_id} topic ===
    let epoch = state.current_epoch();
    let committee_index = 0u64;
    let attestation_committee = state
        .get_attestation_committee(epoch, committee_index, &spec)
        .unwrap();
    let attester = attestation_committee[0] as u64;
    let subnet_id =
        compute_subnet_for_fg_attestation(committee_index, spec.attestation_subnet_count);
    let data = eip8453::AttestationData {
        target: state.target_checkpoint().unwrap(),
        timeout: false,
        finality: HeightCheckpoint::empty(),
    };
    let sign_fg_attestation = |attester_index: u64, signer: usize| -> eip8453::SingleAttestation {
        let signing_root = data.signing_root_at_epoch(
            epoch,
            &state.fork(),
            state.genesis_validators_root(),
            &spec,
        );
        let mut signature = bls::AggregateSignature::infinity();
        signature.add_assign(&harness.validator_keypairs[signer].sk.sign(signing_root));
        eip8453::SingleAttestation {
            committee_index,
            attester_index,
            data,
            signature,
            epoch,
        }
    };

    // The wrong subnet is rejected before anything else.
    assert!(matches!(
        harness.chain.eip8453_verify_fg_attestation_for_gossip(
            &sign_fg_attestation(attester, attester as usize),
            subnet_id + 1
        ),
        Err(Eip8453GossipError::WrongSubnet { .. })
    ));

    // A valid single passes; a second from the same validator this round is ignored.
    harness
        .chain
        .eip8453_verify_fg_attestation_for_gossip(
            &sign_fg_attestation(attester, attester as usize),
            subnet_id,
        )
        .expect("valid FG attestation");
    assert!(matches!(
        harness.chain.eip8453_verify_fg_attestation_for_gossip(
            &sign_fg_attestation(attester, attester as usize),
            subnet_id
        ),
        Err(Eip8453GossipError::PriorAttestationKnown { .. })
    ));

    // An out-of-range committee index is rejected.
    let mut out_of_range = sign_fg_attestation(attester, attester as usize);
    out_of_range.committee_index = E::attestation_committees_per_round() as u64;
    assert!(matches!(
        harness
            .chain
            .eip8453_verify_fg_attestation_for_gossip(&out_of_range, subnet_id),
        Err(Eip8453GossipError::CommitteeIndexOutOfRange(_))
    ));

    // A bad signature is rejected: another committee member's index, signed by the wrong key.
    let other_attester = attestation_committee[1] as u64;
    assert!(matches!(
        harness.chain.eip8453_verify_fg_attestation_for_gossip(
            &sign_fg_attestation(other_attester, attester as usize),
            subnet_id
        ),
        Err(Eip8453GossipError::InvalidSignature(_))
    ));

    // === attester_slashing topic ===
    let make_indexed = |target_root: Hash256| -> eip8453::IndexedAttestation<E> {
        let data = eip8453::AttestationData {
            target: HeightCheckpoint {
                height: 1,
                root: target_root,
            },
            timeout: false,
            finality: HeightCheckpoint::empty(),
        };
        let signing_root = data.signing_root_at_epoch(
            epoch,
            &state.fork(),
            state.genesis_validators_root(),
            &spec,
        );
        let mut signature = bls::AggregateSignature::infinity();
        for index in [0usize, 1] {
            signature.add_assign(&harness.validator_keypairs[index].sk.sign(signing_root));
        }
        eip8453::IndexedAttestation {
            attesting_indices: ProgressiveVariableList::new(vec![0u64, 1]).unwrap(),
            data,
            signature,
            epoch,
        }
    };

    // A double target vote at the same height is slashable and fully verifies.
    let slashing = eip8453::AttesterSlashing {
        attestation_1: make_indexed(tip_root),
        attestation_2: make_indexed(genesis_root),
    };
    harness
        .chain
        .eip8453_verify_attester_slashing_for_gossip(&slashing)
        .expect("valid attester slashing");
    harness
        .chain
        .eip8453_pool
        .insert_attester_slashing(slashing);

    // The same vote twice is not slashable.
    assert!(matches!(
        harness
            .chain
            .eip8453_verify_attester_slashing_for_gossip(&eip8453::AttesterSlashing {
                attestation_1: make_indexed(tip_root),
                attestation_2: make_indexed(tip_root),
            }),
        Err(Eip8453GossipError::InvalidSlashing(_))
    ));

    // A bad signature on otherwise-slashable evidence is rejected by the full verification.
    let mut bad_signature = eip8453::AttesterSlashing {
        attestation_1: make_indexed(tip_root),
        attestation_2: make_indexed(genesis_root),
    };
    bad_signature.attestation_2.signature = bls::AggregateSignature::infinity();
    assert!(matches!(
        harness
            .chain
            .eip8453_verify_attester_slashing_for_gossip(&bad_signature),
        Err(Eip8453GossipError::InvalidSlashing(_))
    ));

    // === head_vote_aggregate topic ===
    let round = compute_round_at_slot::<E>(slot);
    let head_vote_selection_root = head_vote_aggregator_selection_root::<E>(
        slot,
        &state.fork(),
        state.genesis_validators_root(),
        &spec,
    );
    let make_head_vote_aggregate = |voters: Vec<usize>, aggregator: usize| {
        let mut sorted = voters.clone();
        sorted.sort_unstable();
        sorted.dedup();
        let mut signature = bls::AggregateSignature::infinity();
        for &voter in &sorted {
            signature.add_assign(&sign_head_vote(voter, same_slot_vote).signature);
        }
        let message = eip8453::HeadVoteAggregateAndProof::<E> {
            aggregator_index: aggregator as u64,
            aggregate: eip8453::HeadVote {
                voter_indices: VariableList::new(voters.iter().map(|&v| v as u64).collect())
                    .unwrap(),
                data: same_slot_vote,
                signature,
            },
            selection_proof: harness.validator_keypairs[aggregator]
                .sk
                .sign(head_vote_selection_root),
        };
        let wrapper_root =
            message.compute_signing_root(&state.fork(), state.genesis_validators_root(), &spec);
        eip8453::SignedHeadVoteAggregateAndProof {
            signature: harness.validator_keypairs[aggregator].sk.sign(wrapper_root),
            message,
        }
    };

    // At minimal scale every committee member's proof selects it (modulo 1).
    assert!(is_selected_aggregator(
        &harness.validator_keypairs[voter_b]
            .sk
            .sign(head_vote_selection_root),
        E::head_vote_committee_size() as u64,
        spec.target_aggregators_per_committee,
    ));

    // A valid aggregate passes; the aggregator's second aggregate this slot is ignored.
    harness
        .chain
        .eip8453_verify_head_vote_aggregate_for_gossip(&make_head_vote_aggregate(
            vec![voter_a, voter_b],
            voter_b,
        ))
        .expect("valid head vote aggregate");
    assert!(matches!(
        harness
            .chain
            .eip8453_verify_head_vote_aggregate_for_gossip(&make_head_vote_aggregate(
                vec![voter_a],
                voter_b
            )),
        Err(Eip8453GossipError::PriorAggregateKnown { .. })
    ));

    // Duplicate voters are rejected; a non-member aggregator is rejected; a selection proof for
    // the wrong slot is rejected.
    assert!(matches!(
        harness
            .chain
            .eip8453_verify_head_vote_aggregate_for_gossip(&make_head_vote_aggregate(
                vec![voter_a, voter_a],
                voter_a
            )),
        Err(Eip8453GossipError::UnsortedVoters)
    ));
    assert!(matches!(
        harness
            .chain
            .eip8453_verify_head_vote_aggregate_for_gossip(&make_head_vote_aggregate(
                vec![voter_a],
                non_member
            )),
        Err(Eip8453GossipError::NotInCommittee { .. })
    ));
    let mut wrong_proof = make_head_vote_aggregate(vec![voter_a], voter_a);
    wrong_proof.message.selection_proof =
        harness.validator_keypairs[voter_a]
            .sk
            .sign(head_vote_aggregator_selection_root::<E>(
                Slot::new(2),
                &state.fork(),
                state.genesis_validators_root(),
                &spec,
            ));
    assert!(matches!(
        harness
            .chain
            .eip8453_verify_head_vote_aggregate_for_gossip(&wrong_proof),
        Err(Eip8453GossipError::InvalidSelectionProof(_))
    ));

    // === safe_vote_aggregate topic ===
    let safe_vote_selection_root = round_aggregator_selection_root::<E>(
        round,
        Domain::SafeVoteAggregator,
        &state.fork(),
        state.genesis_validators_root(),
        &spec,
    );
    let make_safe_vote_aggregate = |voters: Vec<usize>, data: SafeVoteData, aggregator: usize| {
        let mut signature = bls::AggregateSignature::infinity();
        for &voter in &voters {
            signature.add_assign(&sign_safe_vote(voter, data).signature);
        }
        let message = eip8453::SafeVoteAggregateAndProof::<E> {
            aggregator_index: aggregator as u64,
            aggregate: eip8453::SafeVote {
                voter_indices: ProgressiveVariableList::new(
                    voters.iter().map(|&v| v as u64).collect(),
                )
                .unwrap(),
                data,
                signature,
            },
            selection_proof: harness.validator_keypairs[aggregator]
                .sk
                .sign(safe_vote_selection_root),
        };
        let wrapper_root =
            message.compute_signing_root(&state.fork(), state.genesis_validators_root(), &spec);
        eip8453::SignedSafeVoteAggregateAndProof {
            signature: harness.validator_keypairs[aggregator].sk.sign(wrapper_root),
            message,
        }
    };

    // A valid aggregate passes; the same aggregator passes a second DISTINCT data (equivocation
    // evidence aggregates too) but not the same data twice.
    harness
        .chain
        .eip8453_verify_safe_vote_aggregate_for_gossip(&make_safe_vote_aggregate(
            vec![4, 5],
            first_vote,
            5,
        ))
        .expect("valid safe vote aggregate");
    harness
        .chain
        .eip8453_verify_safe_vote_aggregate_for_gossip(&make_safe_vote_aggregate(
            vec![4, 5],
            second_vote,
            5,
        ))
        .expect("a second distinct-data aggregate from the same aggregator propagates");
    assert!(matches!(
        harness
            .chain
            .eip8453_verify_safe_vote_aggregate_for_gossip(&make_safe_vote_aggregate(
                vec![4],
                first_vote,
                5
            )),
        Err(Eip8453GossipError::PriorAggregateKnown { .. })
    ));

    // A bad wrapper signature is rejected.
    let mut bad_wrapper = make_safe_vote_aggregate(vec![4], first_vote, 6);
    bad_wrapper.signature = harness.validator_keypairs[7]
        .sk
        .sign(Hash256::repeat_byte(0x01));
    assert!(matches!(
        harness
            .chain
            .eip8453_verify_safe_vote_aggregate_for_gossip(&bad_wrapper),
        Err(Eip8453GossipError::InvalidSignature(_))
    ));

    // === attestation_aggregate topic ===
    // Feed two correctly signed singles and let the chain build the spec-shaped aggregate.
    harness
        .chain
        .eip8453_import_verified_fg_attestation(sign_fg_attestation(attester, attester as usize));
    harness
        .chain
        .eip8453_import_verified_fg_attestation(sign_fg_attestation(
            other_attester,
            other_attester as usize,
        ));
    let fg_aggregate = harness
        .chain
        .eip8453_build_fg_attestation_aggregates()
        .expect("fg aggregates build")
        .into_iter()
        .next()
        .expect("one aggregate");
    let fg_selection_root = round_aggregator_selection_root::<E>(
        round,
        Domain::AttestationAggregator,
        &state.fork(),
        state.genesis_validators_root(),
        &spec,
    );
    let fg_aggregator = attestation_committee
        .iter()
        .map(|&index| index as u64)
        .find(|&index| {
            let proof = harness.validator_keypairs[index as usize]
                .sk
                .sign(fg_selection_root);
            is_selected_aggregator(
                &proof,
                attestation_committee.len() as u64,
                spec.target_aggregators_per_committee,
            )
        })
        .expect("some committee member is a selected FG aggregator");
    let fg_message = eip8453::AttestationAggregateAndProof::<E> {
        aggregator_index: fg_aggregator,
        aggregate: fg_aggregate,
        selection_proof: harness.validator_keypairs[fg_aggregator as usize]
            .sk
            .sign(fg_selection_root),
    };
    let aggregation_epoch = Slot::new(round * E::slots_per_round()).epoch(E::slots_per_epoch());
    let fg_wrapper_root = fg_message.compute_signing_root(
        aggregation_epoch,
        &state.fork(),
        state.genesis_validators_root(),
        &spec,
    );
    let signed_fg_aggregate = eip8453::SignedAttestationAggregateAndProof {
        signature: harness.validator_keypairs[fg_aggregator as usize]
            .sk
            .sign(fg_wrapper_root),
        message: fg_message,
    };
    harness
        .chain
        .eip8453_verify_attestation_aggregate_for_gossip(&signed_fg_aggregate)
        .expect("valid FG attestation aggregate");
    assert!(matches!(
        harness
            .chain
            .eip8453_verify_attestation_aggregate_for_gossip(&signed_fg_aggregate),
        Err(Eip8453GossipError::PriorAggregateKnown { .. })
    ));
    let mut bad_fg_wrapper = signed_fg_aggregate;
    bad_fg_wrapper.signature = harness.validator_keypairs[0]
        .sk
        .sign(Hash256::repeat_byte(0x01));
    bad_fg_wrapper.message.aggregator_index = other_attester;
    assert!(matches!(
        harness
            .chain
            .eip8453_verify_attestation_aggregate_for_gossip(&bad_fg_wrapper),
        Err(Eip8453GossipError::InvalidSelectionProof(_) | Eip8453GossipError::InvalidSignature(_))
    ));

    // The pooled slashing packs into the next block.
    harness.advance_slot();
    harness.chain.recompute_head_at_current_slot().await;
    let (block_hash, _, _) = harness
        .add_block_at_slot(Slot::new(2), state.clone())
        .await
        .unwrap();
    let block = harness
        .chain
        .get_blinded_block(&Hash256::from(block_hash))
        .unwrap()
        .expect("block is stored");
    assert_eq!(
        block
            .message()
            .body()
            .attester_slashings_eip8453()
            .unwrap()
            .len(),
        1,
        "the gossip-verified slashing packs into the block"
    );
}
