mod builder_definitions;
use builder_definitions::BuilderConfigFile;
pub use builder_definitions::{BuilderDefinition, Error};
use builder_types::{BuilderUrl, RequestAuthData};
use parking_lot::RwLock;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Contains the precursors for constructing a `BuilderPreferenceEntry`.
#[derive(Clone)]
pub struct DirectBuilder {
    /// The builder URL: routing identity, dedup key, and (via `to_sensitive_url`) request target.
    pub url: BuilderUrl,
    /// The opaque authentication data to sign for this builder.
    pub auth_data: RequestAuthData,
    /// The maximum trusted execution payment accepted from this builder.
    pub max_execution_payment: u64,
}

#[derive(Clone)]
pub struct BuilderStore {
    config: Arc<RwLock<BuilderConfigFile>>,
    validators_dir: PathBuf,
}

impl BuilderStore {
    pub fn open_or_create<P: AsRef<Path>>(validators_dir: P) -> Result<Self, Error> {
        let validators_dir = validators_dir.as_ref().to_path_buf();

        Ok(Self {
            config: Arc::new(RwLock::new(BuilderConfigFile::open_or_create(
                &validators_dir,
            )?)),
            validators_dir,
        })
    }

    /// Returns the precursors for constructing a BuilderPreferenceEntry for
    /// the enabled builders that have a URL defined.
    pub fn direct_builders(&self) -> Vec<DirectBuilder> {
        self.config
            .read()
            .into_iter()
            .filter(|entry| entry.enabled)
            // we only care about builders where the URL is defined
            .map(|entry| DirectBuilder {
                url: entry.url.clone(),
                auth_data: entry
                    .auth_data
                    .clone()
                    .unwrap_or_else(|| entry.url.to_default_auth_data()),
                max_execution_payment: entry.max_execution_payment,
            })
            .collect()
    }

    pub fn builder_definitions(&self) -> Vec<BuilderDefinition> {
        self.config.read().as_slice().to_vec()
    }

    pub fn insert(&self, builder: BuilderDefinition) -> Result<(), Error> {
        let mut config = self.config.write();
        // Validate a candidate copy before committing, so a bad insert leaves the config unchanged
        // (and the global bid-policy defaults are preserved).
        let mut candidate = config.clone();
        candidate.push(builder);
        candidate.validate()?;

        *config = candidate;
        config.save(&self.validators_dir)
    }
}
