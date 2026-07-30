use account_utils::write_file_via_temporary;
use bls::PublicKeyBytes;
use builder_types::{BuilderUrl, RequestAuthData};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs::{File, create_dir_all};
use std::io;
use std::path::{Path, PathBuf};

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
    DuplicateBuilderAuth(BuilderUrl),
    /// A builder with the given URL does not exist.
    UnknownBuilder,
    /// A builder with the given URL did not supply a builder pubkey
    BuilderPubkeyAndURLNotSpecified,
    /// A builder with the given pubkey already exists.
    DuplicateBuilderPubkey(PublicKeyBytes),
}

/// A single definition in the builders file.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct BuilderDefinition {
    /// Indicates whether this definition is enabled or disabled.
    pub enabled: bool,
    /// The URL the beacon node uses to contact this builder. Routing metadata; never signed.
    #[serde(default)]
    pub url: Option<BuilderUrl>,
    /// Opaque authentication data signed into `RequestAuthV1.data`, agreed with the builder out of
    /// band. When unset, it defaults to the UTF-8 bytes of `url` (the builder-specs #165 default).
    #[serde(default)]
    pub auth_data: Option<RequestAuthData>,
    /// The builder's BLS public key.
    #[serde(default)]
    pub builder_pubkey: Option<PublicKeyBytes>,
    /// The maximum execution payment, in gwei, that we're willing to accept from this builder.
    pub max_execution_payment: u64,
    /// The minimum total payment, in gwei, for us to accept a bid from this builder.
    #[serde(default)]
    pub min_bid: u64,
    /// Percentage multiplier applied to this bid when comparing against a local payload
    #[serde(default = "default_builder_boost_factor")]
    pub builder_boost_factor: u64,
}

fn default_builder_boost_factor() -> u64 {
    100
}

/// A list of `BuilderDefinition` that serves as a serde-able configuration file which defines a
/// list of builders that the validator client will request bids from.
#[derive(Default, Serialize, Deserialize)]
pub struct BuilderDefinitions(Vec<BuilderDefinition>);

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
        definitions.validate()?;
        Ok(definitions)
    }

    /// Initialize a new `BuilderDefinitions` instance from a Vec<BuilderDefinition>.
    pub fn try_from_vec(definitions: Vec<BuilderDefinition>) -> Result<Self, Error> {
        let definitions = Self(definitions);
        definitions.validate()?;
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

    pub fn as_slice(&self) -> &[BuilderDefinition] {
        &self.0
    }

    pub fn push(&mut self, definition: BuilderDefinition) {
        self.0.push(definition);
    }

    pub fn retain(&mut self, f: impl FnMut(&BuilderDefinition) -> bool) {
        self.0.retain(f);
    }

    pub fn iter_mut(&mut self) -> std::slice::IterMut<'_, BuilderDefinition> {
        self.0.iter_mut()
    }

    pub fn validate(&self) -> Result<(), Error> {
        let mut p2p_policy_builder_pubkeys = HashSet::new();
        let mut direct_bid_auth_urls = HashSet::new();

        for definition in &self.0 {
            if !definition.enabled {
                // ignore disabled builders
                continue;
            }
            if let Some(url) = &definition.url {
                let auth = definition
                    .auth_data
                    .clone()
                    .unwrap_or_else(|| url.to_default_auth_data());
                // two entries cannot contain the same url and auth data
                let key = (url.clone(), auth);
                if !direct_bid_auth_urls.insert(key) {
                    return Err(Error::DuplicateBuilderAuth(url.clone()));
                }
            } else {
                // Entry specifies P2P policy
                // builder pubkey MUST be specified
                let Some(pubkey) = &definition.builder_pubkey else {
                    return Err(Error::BuilderPubkeyAndURLNotSpecified);
                };

                if !p2p_policy_builder_pubkeys.insert(*pubkey) {
                    return Err(Error::DuplicateBuilderPubkey(*pubkey));
                }
            }
        }

        Ok(())
    }
}

impl<'a> IntoIterator for &'a BuilderDefinitions {
    type Item = &'a BuilderDefinition;
    type IntoIter = std::slice::Iter<'a, BuilderDefinition>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}
