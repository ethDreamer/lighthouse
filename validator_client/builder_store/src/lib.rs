mod builder_definitions;
use builder_definitions::BuilderDefinitions;
pub use builder_definitions::{BuilderEntry, Error};
use parking_lot::RwLock;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use types::builder::RequestAuthUrl;

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

    pub fn enabled_builders(&self) -> Vec<(RequestAuthUrl, u64)> {
        self.definitions
            .read()
            .into_iter()
            .filter(|entry| entry.enabled)
            .map(|entry| (entry.builder_url.clone(), entry.max_execution_payment))
            .collect()
    }

    pub fn builders(&self) -> Vec<BuilderEntry> {
        self.definitions.read().as_slice().to_vec()
    }

    pub fn insert(&self, builder: BuilderEntry) -> Result<(), Error> {
        let mut definitions = self.definitions.write();

        if definitions
            .as_slice()
            .iter()
            .any(|existing| existing.builder_url == builder.builder_url)
        {
            return Err(Error::DuplicateBuilder);
        }

        definitions.push(builder);
        definitions.save(&self.validators_dir)
    }

    pub fn remove(&self, builder_url: &RequestAuthUrl) -> Result<(), Error> {
        let mut definitions = self.definitions.write();
        let original_len = definitions.as_slice().len();

        definitions.retain(|definition| &definition.builder_url != builder_url);

        if definitions.as_slice().len() == original_len {
            return Err(Error::UnknownBuilder);
        }

        definitions.save(&self.validators_dir)
    }

    pub fn set_enabled(&self, builder_url: &RequestAuthUrl, enabled: bool) -> Result<(), Error> {
        let mut definitions = self.definitions.write();
        let definition = definitions
            .iter_mut()
            .find(|definition| &definition.builder_url == builder_url)
            .ok_or(Error::UnknownBuilder)?;

        definition.enabled = enabled;
        definitions.save(&self.validators_dir)
    }

    pub fn set_max_execution_payment(
        &self,
        builder_url: &RequestAuthUrl,
        max_execution_payment: u64,
    ) -> Result<(), Error> {
        let mut definitions = self.definitions.write();
        let definition = definitions
            .iter_mut()
            .find(|definition| &definition.builder_url == builder_url)
            .ok_or(Error::UnknownBuilder)?;

        definition.max_execution_payment = max_execution_payment;
        definitions.save(&self.validators_dir)
    }
}
