use crate::test_utils::TestRandom;
use crate::*;
use derivative::Derivative;
use serde::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use superstruct::superstruct;
use test_random_derive::TestRandom;
use tree_hash_derive::TreeHash;

// in all likelihood, this will be superstructed so might as well start early eh?
#[superstruct(
    variants(EIP7732, NextFork),
    variant_attributes(
        derive(
            Debug,
            Clone,
            Serialize,
            Deserialize,
            Encode,
            Decode,
            TreeHash,
            TestRandom,
            Derivative,
            arbitrary::Arbitrary
        ),
        derivative(PartialEq, Hash(bound = "E: EthSpec")),
        serde(bound = "E: EthSpec", deny_unknown_fields),
        arbitrary(bound = "E: EthSpec")
    ),
    cast_error(ty = "Error", expr = "BeaconStateError::IncorrectStateVariant"),
    partial_getter_error(ty = "Error", expr = "BeaconStateError::IncorrectStateVariant")
)]
#[derive(
    Debug, Clone, Serialize, Encode, Deserialize, TreeHash, Derivative, arbitrary::Arbitrary,
)]
#[derivative(PartialEq, Hash(bound = "E: EthSpec"))]
#[serde(bound = "E: EthSpec", untagged)]
#[arbitrary(bound = "E: EthSpec")]
#[ssz(enum_behaviour = "transparent")]
#[tree_hash(enum_behaviour = "transparent")]
pub struct SignedExecutionEnvelope<E: EthSpec> {
    #[superstruct(only(EIP7732), partial_getter(rename = "message_eip7732"))]
    pub message: ExecutionEnvelopeEIP7732<E>,
    #[superstruct(only(NextFork), partial_getter(rename = "message_next_fork"))]
    pub message: crate::execution_envelope::ExecutionEnvelopeNextFork<E>,
    pub signature: Signature,
}

impl<E: EthSpec> SignedExecutionEnvelope<E> {
    pub fn message(&self) -> ExecutionEnvelopeRef<E> {
        match self {
            SignedExecutionEnvelope::EIP7732(ref signed) => {
                ExecutionEnvelopeRef::EIP7732(&signed.message)
            }
            SignedExecutionEnvelope::NextFork(ref signed) => {
                ExecutionEnvelopeRef::NextFork(&signed.message)
            }
        }
    }

    /// Verify `self.signature`.
    ///
    /// The `parent_state` is the post-state of the beacon block with
    /// block_root = self.message.beacon_block_root
    pub fn verify_signature(
        &self,
        parent_state: &BeaconState<E>,
        spec: &ChainSpec,
    ) -> Result<bool, BeaconStateError> {
        let domain = spec.get_domain(
            parent_state.current_epoch(),
            Domain::BeaconBuilder,
            &parent_state.fork(),
            parent_state.genesis_validators_root(),
        );
        let pubkey = parent_state
            .validators()
            .get(self.message().builder_index() as usize)
            .and_then(|v| {
                let pk: Option<PublicKey> = v.pubkey.decompress().ok();
                pk
            })
            .ok_or_else(|| {
                BeaconStateError::UnknownValidator(self.message().builder_index() as usize)
            })?;
        let message = self.message().signing_root(domain);

        Ok(self.signature().verify(&pubkey, message))
    }
}

/// This module can be used to encode and decode a `SignedExecutionEnvelope` the same way it
/// would be done if we had tagged the superstruct enum with
/// `#[ssz(enum_behaviour = "union")]`
/// This should _only_ be used *some* cases when storing these objects in the database
/// and _NEVER_ for encoding / decoding blocks sent over the network!
pub mod ssz_tagged_signed_execution_envelope {
    use super::*;

    #[derive(Debug, Clone, Encode, Decode, PartialEq)]
    #[ssz(enum_behaviour = "union")]
    pub enum SignedExecutionEnvelopeOnDisk<E: EthSpec> {
        EIP7732(SignedExecutionEnvelopeEIP7732<E>),
        NextFork(SignedExecutionEnvelopeNextFork<E>),
    }

    #[derive(Debug, Clone, Encode)]
    #[ssz(enum_behaviour = "union")]
    pub enum SignedExecutionEnvelopeRefOnDisk<'a, E: EthSpec> {
        EIP7732(&'a SignedExecutionEnvelopeEIP7732<E>),
        NextFork(&'a SignedExecutionEnvelopeNextFork<E>),
    }

    impl<E: EthSpec> From<SignedExecutionEnvelopeOnDisk<E>> for SignedExecutionEnvelope<E> {
        fn from(e: SignedExecutionEnvelopeOnDisk<E>) -> Self {
            match e {
                SignedExecutionEnvelopeOnDisk::EIP7732(e) => SignedExecutionEnvelope::EIP7732(e),
                SignedExecutionEnvelopeOnDisk::NextFork(e) => SignedExecutionEnvelope::NextFork(e),
            }
        }
    }

    impl<'a, E: EthSpec> From<SignedExecutionEnvelopeRef<'a, E>>
        for SignedExecutionEnvelopeRefOnDisk<'a, E>
    {
        fn from(envelope: SignedExecutionEnvelopeRef<'a, E>) -> Self {
            match envelope {
                SignedExecutionEnvelopeRef::EIP7732(e) => Self::EIP7732(e),
                SignedExecutionEnvelopeRef::NextFork(e) => Self::NextFork(e),
            }
        }
    }

    pub mod encode {
        use super::*;
        #[allow(unused_imports)]
        use ssz::*;

        pub fn is_ssz_fixed_len() -> bool {
            false
        }

        pub fn ssz_fixed_len() -> usize {
            BYTES_PER_LENGTH_OFFSET
        }

        pub fn ssz_bytes_len<E: EthSpec>(envelope: &SignedExecutionEnvelope<E>) -> usize {
            SignedExecutionEnvelopeRefOnDisk::from(envelope.to_ref()).ssz_bytes_len()
        }

        pub fn ssz_append<E: EthSpec>(envelope: &SignedExecutionEnvelope<E>, buf: &mut Vec<u8>) {
            SignedExecutionEnvelopeRefOnDisk::from(envelope.to_ref()).ssz_append(buf);
        }

        pub fn as_ssz_bytes<E: EthSpec>(envelope: &SignedExecutionEnvelope<E>) -> Vec<u8> {
            let mut buf = vec![];
            ssz_append(envelope, &mut buf);

            buf
        }
    }

    pub mod decode {
        use super::*;
        #[allow(unused_imports)]
        use ssz::*;

        pub fn is_ssz_fixed_len() -> bool {
            false
        }

        pub fn ssz_fixed_len() -> usize {
            BYTES_PER_LENGTH_OFFSET
        }

        pub fn from_ssz_bytes<E: EthSpec>(
            bytes: &[u8],
        ) -> Result<SignedExecutionEnvelope<E>, DecodeError> {
            SignedExecutionEnvelopeOnDisk::from_ssz_bytes(bytes).map(Into::into)
        }
    }
}

pub mod ssz_tagged_signed_execution_envelope_arc {
    use super::*;
    pub mod encode {
        pub use super::ssz_tagged_signed_execution_envelope::encode::*;
    }

    pub mod decode {
        pub use super::ssz_tagged_signed_execution_envelope::decode::{
            is_ssz_fixed_len, ssz_fixed_len,
        };
        use super::*;
        #[allow(unused_imports)]
        use ssz::*;
        use std::sync::Arc;

        pub fn from_ssz_bytes<E: EthSpec>(
            bytes: &[u8],
        ) -> Result<Arc<SignedExecutionEnvelope<E>>, DecodeError> {
            ssz_tagged_signed_execution_envelope::decode::from_ssz_bytes(bytes).map(Arc::new)
        }
    }
}
