pub mod altair;
pub mod capella;
pub mod deneb;
pub mod merge;
pub mod whisk;

pub use altair::upgrade_to_altair;
pub use capella::upgrade_to_capella;
pub use deneb::upgrade_to_deneb;
pub use merge::upgrade_to_bellatrix;
pub use whisk::upgrade_to_whisk;
