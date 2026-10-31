//! EIP-8453 finality signing protection: the spec's `FinalitySigningRecord`, plus one-per-slot
//! head vote and one-per-round safe vote locks.
//!
//! This is a separate, height-keyed store living in the same SQLite database as the legacy
//! slashing protection (sharing its EXCLUSIVE lock and transactions), NOT a migration of it:
//! EIP-3076 interchange has no representation for heights, timeouts, finality locks or entries,
//! so records created here cannot be exported — a known spec friction, acceptable for the
//! prototype.
//!
//! Spec: `specs/_features/eip8453/validator.md` — `FinalitySigningRecord`, `get_finality_vote`,
//! `get_height_vote`, `get_finality_attestation_data`, `record_finality_attestation`. Selecting
//! and recording the attestation data is one exclusive transaction, and the record is persisted
//! (committed) before the caller signs.

use bls::PublicKeyBytes;
use rusqlite::{OptionalExtension, Transaction, params};
use types::Hash256;
use types::eip8453::{AttestationData, HeightCheckpoint};

use crate::{NotSafe, Safe, SlashingDatabase};

/// Create the EIP-8453 tables. Called from the schema migrations so both fresh and existing
/// databases converge.
pub(crate) fn apply_eip8453_schema(txn: &Transaction) -> Result<(), NotSafe> {
    txn.execute(
        "CREATE TABLE IF NOT EXISTS fg_targets (
            validator_id INTEGER NOT NULL,
            height INTEGER NOT NULL,
            root BLOB NOT NULL,
            FOREIGN KEY(validator_id) REFERENCES validators(id)
            UNIQUE (validator_id, height)
        )",
        params![],
    )?;
    txn.execute(
        "CREATE TABLE IF NOT EXISTS fg_timeouts (
            validator_id INTEGER NOT NULL,
            height INTEGER NOT NULL,
            FOREIGN KEY(validator_id) REFERENCES validators(id)
            UNIQUE (validator_id, height)
        )",
        params![],
    )?;
    txn.execute(
        "CREATE TABLE IF NOT EXISTS fg_finality_locks (
            validator_id INTEGER NOT NULL,
            height INTEGER NOT NULL,
            root BLOB NOT NULL,
            FOREIGN KEY(validator_id) REFERENCES validators(id)
            UNIQUE (validator_id, height)
        )",
        params![],
    )?;
    txn.execute(
        "CREATE TABLE IF NOT EXISTS fg_entries (
            validator_id INTEGER NOT NULL,
            height INTEGER NOT NULL,
            root BLOB NOT NULL,
            FOREIGN KEY(validator_id) REFERENCES validators(id)
            UNIQUE (validator_id, height, root)
        )",
        params![],
    )?;
    txn.execute(
        "CREATE TABLE IF NOT EXISTS eip8453_signed_head_votes (
            validator_id INTEGER NOT NULL,
            slot INTEGER NOT NULL,
            data_root BLOB NOT NULL,
            FOREIGN KEY(validator_id) REFERENCES validators(id)
            UNIQUE (validator_id, slot)
        )",
        params![],
    )?;
    txn.execute(
        "CREATE TABLE IF NOT EXISTS eip8453_signed_safe_votes (
            validator_id INTEGER NOT NULL,
            round INTEGER NOT NULL,
            data_root BLOB NOT NULL,
            FOREIGN KEY(validator_id) REFERENCES validators(id)
            UNIQUE (validator_id, round)
        )",
        params![],
    )?;
    Ok(())
}

fn root_from_blob(blob: Vec<u8>) -> Result<Hash256, NotSafe> {
    if blob.len() != 32 {
        return Err(NotSafe::ConsistencyError);
    }
    Ok(Hash256::from_slice(&blob))
}

impl SlashingDatabase {
    /// "The validator must record the selected root before signing and must not sign another
    /// head for the same slot."
    pub fn check_and_insert_eip8453_head_vote(
        &self,
        validator_pk: &PublicKeyBytes,
        slot: u64,
        data_root: Hash256,
    ) -> Result<Safe, NotSafe> {
        self.with_transaction(|txn| {
            let validator_id = self.get_validator_id_in_txn(txn, validator_pk)?;
            let existing: Option<Vec<u8>> = txn
                .query_row(
                    "SELECT data_root FROM eip8453_signed_head_votes
                     WHERE validator_id = ?1 AND slot = ?2",
                    params![validator_id, slot as i64],
                    |row| row.get(0),
                )
                .optional()?;
            if let Some(blob) = existing {
                return if root_from_blob(blob)? == data_root {
                    Ok(Safe::SameData)
                } else {
                    Err(NotSafe::ConsistencyError)
                };
            }
            txn.execute(
                "INSERT INTO eip8453_signed_head_votes (validator_id, slot, data_root)
                 VALUES (?1, ?2, ?3)",
                params![validator_id, slot as i64, data_root.as_slice()],
            )?;
            Ok(Safe::Valid)
        })
    }

    /// "The validator must record the selected node before signing and must not sign another
    /// safe vote for the same round."
    pub fn check_and_insert_eip8453_safe_vote(
        &self,
        validator_pk: &PublicKeyBytes,
        round: u64,
        data_root: Hash256,
    ) -> Result<Safe, NotSafe> {
        self.with_transaction(|txn| {
            let validator_id = self.get_validator_id_in_txn(txn, validator_pk)?;
            let existing: Option<Vec<u8>> = txn
                .query_row(
                    "SELECT data_root FROM eip8453_signed_safe_votes
                     WHERE validator_id = ?1 AND round = ?2",
                    params![validator_id, round as i64],
                    |row| row.get(0),
                )
                .optional()?;
            if let Some(blob) = existing {
                return if root_from_blob(blob)? == data_root {
                    Ok(Safe::SameData)
                } else {
                    Err(NotSafe::ConsistencyError)
                };
            }
            txn.execute(
                "INSERT INTO eip8453_signed_safe_votes (validator_id, round, data_root)
                 VALUES (?1, ?2, ?3)",
                params![validator_id, round as i64, data_root.as_slice()],
            )?;
            Ok(Safe::Valid)
        })
    }

    /// Select (and record, before signing) the FG attestation data for one observation of the
    /// fork choice: `get_finality_attestation_data` + `record_finality_attestation` in one
    /// exclusive transaction. Returns `None` when the signing history suppresses the vote.
    pub fn eip8453_fg_attestation_data(
        &self,
        validator_pk: &PublicKeyBytes,
        target: HeightCheckpoint,
        justified: HeightCheckpoint,
        finalized_height: u64,
    ) -> Result<Option<AttestationData>, NotSafe> {
        self.with_transaction(|txn| {
            let validator_id = self.get_validator_id_in_txn(txn, validator_pk)?;

            let finality = get_finality_vote(txn, validator_id, justified, finalized_height)?;
            let Some((target, timeout)) = get_height_vote(txn, validator_id, target, finality)?
            else {
                return Ok(None);
            };

            // The duplicate-vote checks: at most one non-timeout entry per height, and never
            // the same entry twice.
            let entries = get_entries(txn, validator_id, target.height)?;
            if (!timeout && !entries.is_empty()) || entries.contains(&target.root) {
                return Ok(None);
            }

            let finality = if !finality.is_empty() && target.height <= finality.height {
                HeightCheckpoint::empty()
            } else {
                finality
            };
            let data = AttestationData {
                target,
                timeout,
                finality,
            };

            // `record_finality_attestation`, committed before the caller signs.
            txn.execute(
                "INSERT OR IGNORE INTO fg_entries (validator_id, height, root)
                 VALUES (?1, ?2, ?3)",
                params![validator_id, target.height as i64, target.root.as_slice()],
            )?;
            if !data.finality.is_empty() {
                txn.execute(
                    "INSERT INTO fg_finality_locks (validator_id, height, root)
                     VALUES (?1, ?2, ?3)
                     ON CONFLICT (validator_id, height) DO UPDATE SET root = excluded.root",
                    params![
                        validator_id,
                        data.finality.height as i64,
                        data.finality.root.as_slice()
                    ],
                )?;
            }
            if timeout {
                txn.execute(
                    "INSERT OR IGNORE INTO fg_timeouts (validator_id, height) VALUES (?1, ?2)",
                    params![validator_id, target.height as i64],
                )?;
            } else {
                txn.execute(
                    "INSERT OR IGNORE INTO fg_targets (validator_id, height, root)
                     VALUES (?1, ?2, ?3)",
                    params![validator_id, target.height as i64, target.root.as_slice()],
                )?;
            }

            Ok(Some(data))
        })
    }
}

/// The spec's `get_finality_vote`.
fn get_finality_vote(
    txn: &Transaction,
    validator_id: i64,
    justified: HeightCheckpoint,
    finalized_height: u64,
) -> Result<HeightCheckpoint, NotSafe> {
    let (height, root) = (justified.height, justified.root);
    if height <= finalized_height {
        return Ok(HeightCheckpoint::empty());
    }
    if let Some(recorded) = get_target(txn, validator_id, height)?
        && recorded != root
    {
        return Ok(HeightCheckpoint::empty());
    }
    if has_timeout(txn, validator_id, height)? {
        return Ok(HeightCheckpoint::empty());
    }
    if let Some(lock) = get_finality_lock(txn, validator_id, height)?
        && lock != root
    {
        return Ok(HeightCheckpoint::empty());
    }
    Ok(justified)
}

/// The spec's `get_height_vote`: `Some((target, timeout))`, or `None` to suppress the vote.
fn get_height_vote(
    txn: &Transaction,
    validator_id: i64,
    target: HeightCheckpoint,
    finality: HeightCheckpoint,
) -> Result<Option<(HeightCheckpoint, bool)>, NotSafe> {
    let (height, root) = (target.height, target.root);
    if height == 0 || root.is_zero() {
        return Ok(None);
    }

    let mut lock = get_finality_lock(txn, validator_id, height)?;
    if !finality.is_empty() && finality.height == height {
        lock = Some(finality.root);
    }

    if has_timeout(txn, validator_id, height)? {
        return Ok(Some((target, true)));
    }
    if let Some(lock) = lock {
        if lock == root {
            return Ok(Some((target, false)));
        }
        return Ok(None);
    }
    match get_target(txn, validator_id, height)? {
        None => Ok(Some((target, false))),
        Some(recorded) if recorded == root => Ok(Some((target, false))),
        Some(_) => Ok(Some((target, true))),
    }
}

fn get_target(
    txn: &Transaction,
    validator_id: i64,
    height: u64,
) -> Result<Option<Hash256>, NotSafe> {
    txn.query_row(
        "SELECT root FROM fg_targets WHERE validator_id = ?1 AND height = ?2",
        params![validator_id, height as i64],
        |row| row.get::<_, Vec<u8>>(0),
    )
    .optional()?
    .map(root_from_blob)
    .transpose()
}

fn has_timeout(txn: &Transaction, validator_id: i64, height: u64) -> Result<bool, NotSafe> {
    Ok(txn
        .query_row(
            "SELECT 1 FROM fg_timeouts WHERE validator_id = ?1 AND height = ?2",
            params![validator_id, height as i64],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .is_some())
}

fn get_finality_lock(
    txn: &Transaction,
    validator_id: i64,
    height: u64,
) -> Result<Option<Hash256>, NotSafe> {
    txn.query_row(
        "SELECT root FROM fg_finality_locks WHERE validator_id = ?1 AND height = ?2",
        params![validator_id, height as i64],
        |row| row.get::<_, Vec<u8>>(0),
    )
    .optional()?
    .map(root_from_blob)
    .transpose()
}

fn get_entries(txn: &Transaction, validator_id: i64, height: u64) -> Result<Vec<Hash256>, NotSafe> {
    let mut stmt =
        txn.prepare("SELECT root FROM fg_entries WHERE validator_id = ?1 AND height = ?2")?;
    let rows = stmt
        .query_map(params![validator_id, height as i64], |row| {
            row.get::<_, Vec<u8>>(0)
        })?
        .collect::<Result<Vec<_>, _>>()?;
    rows.into_iter().map(root_from_blob).collect()
}
