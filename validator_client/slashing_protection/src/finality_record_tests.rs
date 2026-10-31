#![cfg(test)]

//! Tests for the EIP-8453 `FinalitySigningRecord` store, following the spec rules in
//! `specs/_features/eip8453/validator.md`.

use crate::test_utils::pubkey;
use crate::*;
use fixed_bytes::FixedBytesExtended;
use tempfile::tempdir;
use types::eip8453::HeightCheckpoint;

fn checkpoint(height: u64, byte: u8) -> HeightCheckpoint {
    HeightCheckpoint {
        height,
        root: Hash256::repeat_byte(byte),
    }
}

fn fresh_db(dir: &tempfile::TempDir) -> SlashingDatabase {
    let db = SlashingDatabase::create(&dir.path().join("slashing.sqlite")).unwrap();
    db.register_validator(pubkey(0)).unwrap();
    db
}

#[test]
fn head_vote_lock_per_slot() {
    let dir = tempdir().unwrap();
    let db = fresh_db(&dir);
    let pk = pubkey(0);
    let root_a = Hash256::repeat_byte(0xaa);
    let root_b = Hash256::repeat_byte(0xbb);

    assert_eq!(
        db.check_and_insert_eip8453_head_vote(&pk, 3, root_a)
            .unwrap(),
        Safe::Valid
    );
    // The same data may be re-signed.
    assert_eq!(
        db.check_and_insert_eip8453_head_vote(&pk, 3, root_a)
            .unwrap(),
        Safe::SameData
    );
    // A different head for the same slot must not be signed.
    db.check_and_insert_eip8453_head_vote(&pk, 3, root_b)
        .unwrap_err();
    // Another slot is fine.
    assert_eq!(
        db.check_and_insert_eip8453_head_vote(&pk, 4, root_b)
            .unwrap(),
        Safe::Valid
    );
}

#[test]
fn safe_vote_lock_per_round() {
    let dir = tempdir().unwrap();
    let db = fresh_db(&dir);
    let pk = pubkey(0);
    let root_a = Hash256::repeat_byte(0xaa);
    let root_b = Hash256::repeat_byte(0xbb);

    assert_eq!(
        db.check_and_insert_eip8453_safe_vote(&pk, 1, root_a)
            .unwrap(),
        Safe::Valid
    );
    assert_eq!(
        db.check_and_insert_eip8453_safe_vote(&pk, 1, root_a)
            .unwrap(),
        Safe::SameData
    );
    db.check_and_insert_eip8453_safe_vote(&pk, 1, root_b)
        .unwrap_err();
}

#[test]
fn fg_attestation_healthy_path() {
    let dir = tempdir().unwrap();
    let db = fresh_db(&dir);
    let pk = pubkey(0);

    // Height 3 target, justified at height 2 above finalized height 1: both pairs emitted.
    let target = checkpoint(3, 0x03);
    let justified = checkpoint(2, 0x02);
    let data = db
        .eip8453_fg_attestation_data(&pk, target, justified, 1)
        .unwrap()
        .expect("fresh history signs");
    assert_eq!(data.target, target);
    assert!(!data.timeout);
    assert_eq!(data.finality, justified);

    // The exact same observation is suppressed (an entry exists for the height).
    assert!(
        db.eip8453_fg_attestation_data(&pk, target, justified, 1)
            .unwrap()
            .is_none()
    );

    // A conflicting target at the same height becomes a timeout vote (second distinct entry).
    let conflicting = checkpoint(3, 0x33);
    let data = db
        .eip8453_fg_attestation_data(&pk, conflicting, justified, 1)
        .unwrap()
        .expect("conflicting target times out");
    assert!(data.timeout);
    assert_eq!(data.target, conflicting);
    // The finality pair is for height 2 — below the target height, matching the recorded
    // finality lock — so it still rides along with the timeout vote.
    assert_eq!(data.finality, justified);
}

#[test]
fn fg_attestation_finality_lock_constrains() {
    let dir = tempdir().unwrap();
    let db = fresh_db(&dir);
    let pk = pubkey(0);

    // Vote height 3 with a finality pair locking height 2 to root 0x02.
    let data = db
        .eip8453_fg_attestation_data(&pk, checkpoint(3, 0x03), checkpoint(2, 0x02), 1)
        .unwrap()
        .unwrap();
    assert_eq!(data.finality, checkpoint(2, 0x02));

    // A later height-2 target conflicting with the lock is refused outright (the lock names a
    // different root and the entry set for height 2 is empty, so no timeout escape).
    assert!(
        db.eip8453_fg_attestation_data(&pk, checkpoint(2, 0x22), HeightCheckpoint::empty(), 1)
            .unwrap()
            .is_none()
    );
    // The locked root itself is signable.
    let data = db
        .eip8453_fg_attestation_data(&pk, checkpoint(2, 0x02), HeightCheckpoint::empty(), 1)
        .unwrap()
        .unwrap();
    assert!(!data.timeout);
}

#[test]
fn fg_attestation_finality_pair_rules() {
    let dir = tempdir().unwrap();
    let db = fresh_db(&dir);
    let pk = pubkey(0);

    // Justified at or below the finalized height yields no finality pair.
    let data = db
        .eip8453_fg_attestation_data(&pk, checkpoint(3, 0x03), checkpoint(2, 0x02), 2)
        .unwrap()
        .unwrap();
    assert!(data.finality.is_empty());

    // A finality pair at or above the target height is dropped from the data.
    let data = db
        .eip8453_fg_attestation_data(&pk, checkpoint(4, 0x04), checkpoint(4, 0x04), 1)
        .unwrap()
        .unwrap();
    assert!(data.finality.is_empty());
    assert_eq!(data.target, checkpoint(4, 0x04));
}

#[test]
fn record_survives_restart() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("slashing.sqlite");
    let pk = pubkey(0);

    {
        let db = SlashingDatabase::create(&path).unwrap();
        db.register_validator(pk).unwrap();
        let data = db
            .eip8453_fg_attestation_data(&pk, checkpoint(3, 0x03), checkpoint(2, 0x02), 1)
            .unwrap()
            .unwrap();
        assert!(!data.timeout);
        assert_eq!(
            db.check_and_insert_eip8453_head_vote(&pk, 7, Hash256::repeat_byte(0x77))
                .unwrap(),
            Safe::Valid
        );
    }

    // Reopen (a VC restart): the record persists and still constrains signing.
    let db = SlashingDatabase::open(&path).unwrap();
    assert!(
        db.eip8453_fg_attestation_data(&pk, checkpoint(3, 0x03), checkpoint(2, 0x02), 1)
            .unwrap()
            .is_none(),
        "the height-3 entry survives the restart"
    );
    db.check_and_insert_eip8453_head_vote(&pk, 7, Hash256::repeat_byte(0x88))
        .unwrap_err();
    assert_eq!(
        db.check_and_insert_eip8453_head_vote(&pk, 7, Hash256::repeat_byte(0x77))
            .unwrap(),
        Safe::SameData
    );
}

#[test]
fn zero_target_suppresses() {
    let dir = tempdir().unwrap();
    let db = fresh_db(&dir);
    let pk = pubkey(0);

    // Height 0 or zero root: no height vote, no attestation.
    assert!(
        db.eip8453_fg_attestation_data(&pk, HeightCheckpoint::empty(), checkpoint(2, 0x02), 1)
            .unwrap()
            .is_none()
    );
}
