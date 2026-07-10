use account_utils::write_file_via_temporary;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs::{File, create_dir_all};
use std::io;
use std::path::{Path, PathBuf};
use types::builder::RequestAuthUrl;

/// The file name for the serialized `BuilderDefinitions` struct.
pub const BUILDERS_FILENAME: &str = "builder_definitions.yml";
/// The temporary file name for the serialized `BuilderDefinitions` struct.
///
/// This is used to achieve an atomic update of the contents on disk, without truncation.
pub const BUILDERS_TEMP_FILENAME: &str = ".builder_definitions.yml.tmp";

#[derive(Debug)]
pub enum Error {
    /// The config file could not be opened.
    UnableToOpenFile(io::Error),
    /// The config file could not be parsed as YAML.
    UnableToParseFile(yaml_serde::Error),
    /// The builders file could not be serialized as YAML.
    UnableToEncodeFile(yaml_serde::Error),
    /// The builders file or temp file could not be written to the filesystem.
    UnableToWriteFile(filesystem::Error),
    /// The validator directory could not be created.
    UnableToCreateValidatorDir(PathBuf),
    /// A builder with the given URL already exists.
    DuplicateBuilder,
    /// A builder with the given URL does not exist.
    UnknownBuilder,
}

/// A single entry in the builders file.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct BuilderEntry {
    pub enabled: bool,
    pub builder_url: RequestAuthUrl,
    pub max_execution_payment: u64,
}

/// A list of `BuilderEntry` that serves as a serde-able configuration file which defines a
/// list of builders that the validator client will request bids from.
#[derive(Default, Serialize, Deserialize)]
pub struct BuilderDefinitions(Vec<BuilderEntry>);

impl BuilderDefinitions {
    /// Open an existing file or create a new, empty one if it does not exist.
    pub fn open_or_create<P: AsRef<Path>>(validators_dir: P) -> Result<Self, Error> {
        create_dir_all(validators_dir.as_ref()).map_err(|_| {
            Error::UnableToCreateValidatorDir(PathBuf::from(validators_dir.as_ref()))
        })?;
        let builders_file_path = validators_dir.as_ref().join(BUILDERS_FILENAME);
        if !builders_file_path.exists() {
            let this = Self::default();
            this.save(&validators_dir)?;
        }
        Self::open(validators_dir)
    }

    /// Open an existing file, returning an error if the file does not exist.
    pub fn open<P: AsRef<Path>>(validators_dir: P) -> Result<Self, Error> {
        let config_path = validators_dir.as_ref().join(BUILDERS_FILENAME);
        let file = File::options()
            .write(true)
            .read(true)
            .create_new(false)
            .open(config_path)
            .map_err(Error::UnableToOpenFile)?;
        let definitions: Self = yaml_serde::from_reader(file).map_err(Error::UnableToParseFile)?;
        definitions.validate_no_duplicates()?;
        Ok(definitions)
    }

    /// Encodes `self` as a YAML string and atomically writes it to the `CONFIG_FILENAME` file in
    /// the `validators_dir` directory.
    ///
    /// Will create a new file if it does not exist or overwrite any existing file.
    pub fn save<P: AsRef<Path>>(&self, validators_dir: P) -> Result<(), Error> {
        let config_path = validators_dir.as_ref().join(BUILDERS_FILENAME);
        let temp_path = validators_dir.as_ref().join(BUILDERS_TEMP_FILENAME);
        let mut bytes = vec![];
        yaml_serde::to_writer(&mut bytes, self).map_err(Error::UnableToEncodeFile)?;

        write_file_via_temporary(&config_path, &temp_path, &bytes)
            .map_err(Error::UnableToWriteFile)?;

        Ok(())
    }

    pub fn as_slice(&self) -> &[BuilderEntry] {
        &self.0
    }

    pub fn push(&mut self, definition: BuilderEntry) {
        self.0.push(definition);
    }

    pub fn retain(&mut self, f: impl FnMut(&BuilderEntry) -> bool) {
        self.0.retain(f);
    }

    pub fn iter_mut(&mut self) -> std::slice::IterMut<'_, BuilderEntry> {
        self.0.iter_mut()
    }

    fn validate_no_duplicates(&self) -> Result<(), Error> {
        let mut seen = HashSet::new();

        for definition in &self.0 {
            if !seen.insert(definition.builder_url.clone()) {
                return Err(Error::DuplicateBuilder);
            }
        }

        Ok(())
    }
}

impl<'a> IntoIterator for &'a BuilderDefinitions {
    type Item = &'a BuilderEntry;
    type IntoIter = std::slice::Iter<'a, BuilderEntry>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}
