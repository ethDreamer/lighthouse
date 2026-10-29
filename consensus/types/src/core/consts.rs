pub mod altair {
    pub const TIMELY_SOURCE_FLAG_INDEX: usize = 0;
    pub const TIMELY_TARGET_FLAG_INDEX: usize = 1;
    pub const TIMELY_HEAD_FLAG_INDEX: usize = 2;
    pub const TIMELY_SOURCE_WEIGHT: u64 = 14;
    pub const TIMELY_TARGET_WEIGHT: u64 = 26;
    pub const TIMELY_HEAD_WEIGHT: u64 = 14;
    pub const SYNC_REWARD_WEIGHT: u64 = 2;
    pub const PROPOSER_WEIGHT: u64 = 8;
    pub const WEIGHT_DENOMINATOR: u64 = 64;
    pub const SYNC_COMMITTEE_SUBNET_COUNT: u64 = 4;
    pub const TARGET_AGGREGATORS_PER_SYNC_SUBCOMMITTEE: u64 = 16;

    pub const PARTICIPATION_FLAG_WEIGHTS: [u64; NUM_FLAG_INDICES] = [
        TIMELY_SOURCE_WEIGHT,
        TIMELY_TARGET_WEIGHT,
        TIMELY_HEAD_WEIGHT,
    ];

    pub const NUM_FLAG_INDICES: usize = 3;
}
pub mod bellatrix {
    pub const BASIS_POINTS: u64 = 10_000;
}
pub mod deneb {
    pub use kzg::VERSIONED_HASH_VERSION_KZG;
}
pub mod gloas {
    pub const BUILDER_INDEX_SELF_BUILD: u64 = u64::MAX;
    pub const BUILDER_INDEX_FLAG: u64 = 1 << 40;

    /// Version for an execution payload builder.
    pub const PAYLOAD_BUILDER_VERSION: u8 = 0;

    // Fork choice constants
    pub type PayloadStatus = u8;
    pub const PAYLOAD_STATUS_EMPTY: PayloadStatus = 0;
    pub const PAYLOAD_STATUS_FULL: PayloadStatus = 1;
    pub const PAYLOAD_STATUS_PENDING: PayloadStatus = 2;

    pub const ATTESTATION_TIMELINESS_INDEX: usize = 0;
    pub const PTC_TIMELINESS_INDEX: usize = 1;
    pub const NUM_BLOCK_TIMELINESS_DEADLINES: usize = 2;
}
pub mod eip8453 {
    /// A round number: a round spans `SLOTS_PER_ROUND` slots, in which each active validator
    /// casts one FG attestation and after which the FG settles rewards and inactivity.
    pub type Round = u64;
    pub const GENESIS_ROUND: Round = 0;

    pub const PROGRESS_FLAG_INDEX: usize = 0;
    pub const FINALITY_FLAG_INDEX: usize = 1;
    pub const SAFE_VOTE_FLAG_INDEX: usize = 2;
    // [Modified in safe-votes] the two new duties (safe votes, head votes) are paid by
    // reallocating the fixed WEIGHT_DENOMINATOR budget; with the inherited sync (2) and
    // proposer (8) weights the total is still 64.
    pub const PROGRESS_WEIGHT: u64 = 32;
    pub const FINALITY_WEIGHT: u64 = 12;
    pub const SAFE_VOTE_WEIGHT: u64 = 6;
    pub const HEAD_VOTE_WEIGHT: u64 = 4;

    pub const PARTICIPATION_FLAG_WEIGHTS: [u64; NUM_FLAG_INDICES] =
        [PROGRESS_WEIGHT, FINALITY_WEIGHT, SAFE_VOTE_WEIGHT];

    pub const NUM_FLAG_INDICES: usize = 3;
}
