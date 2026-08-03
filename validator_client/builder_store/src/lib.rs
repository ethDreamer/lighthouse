mod builder_definitions;
use builder_definitions::BuilderDefinitions;
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
    definitions: Arc<RwLock<BuilderDefinitions>>,
    validators_dir: PathBuf,
}

impl BuilderStore {
    pub fn open_or_create<P: AsRef<Path>>(validators_dir: P) -> Result<Self, Error> {
        let validators_dir = validators_dir.as_ref().to_path_buf();

        Ok(Self {
            definitions: Arc::new(RwLock::new(BuilderDefinitions::open_or_create(
                &validators_dir,
            )?)),
            validators_dir,
        })
    }

    /// Returns the precursors for constructing a BuilderPreferenceEntry for
    /// the enabled builders that have a URL defined.
    pub fn direct_builders(&self) -> Vec<DirectBuilder> {
        self.definitions
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
        self.definitions.read().as_slice().to_vec()
    }

    pub fn insert(&self, builder: BuilderDefinition) -> Result<(), Error> {
        let mut definitions = self.definitions.write();
        let mut copied_vec = definitions.as_slice().to_vec();
        copied_vec.push(builder);

        let new_definitions = BuilderDefinitions::try_from_vec(copied_vec)?;

        *definitions = new_definitions;
        definitions.save(&self.validators_dir)
    }
}
