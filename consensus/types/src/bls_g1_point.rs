use serde::{de::Error, Deserialize, Deserializer, Serialize, Serializer};
use ssz::{Decode, DecodeError};
use ssz_derive::Encode;

pub const BLS_G1_BYTES_LEN: usize = 48;

#[derive(Debug, PartialEq, Hash, Clone, Copy, Encode, Serialize, Deserialize)]
#[serde(transparent)]
#[derive(arbitrary::Arbitrary)]
#[ssz(struct_behaviour = "transparent")]
pub struct BLSG1Point(#[serde(with = "serde_bls_g1_point")] pub [u8; BLS_G1_BYTES_LEN]);

impl Decode for BLSG1Point {
    fn is_ssz_fixed_len() -> bool {
        true
    }

    fn ssz_fixed_len() -> usize {
        BLS_G1_BYTES_LEN
    }

    fn from_ssz_bytes(bytes: &[u8]) -> Result<Self, DecodeError> {
        if bytes.len() != BLS_G1_BYTES_LEN {
            return Err(DecodeError::InvalidByteLength {
                len: bytes.len(),
                expected: BLS_G1_BYTES_LEN,
            });
        }

        let mut array = [0; BLS_G1_BYTES_LEN];
        array.copy_from_slice(bytes);

        // TODO: spec says we should do subgroup check here
        //       but we must figure out the best way to do this\
        Ok(Self(array))
    }
}

pub mod serde_bls_g1_point {
    use super::*;

    pub fn serialize<S>(bytes: &[u8; BLS_G1_BYTES_LEN], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&eth2_serde_utils::hex::encode(bytes))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<[u8; BLS_G1_BYTES_LEN], D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = Deserialize::deserialize(deserializer)?;

        let bytes = eth2_serde_utils::hex::decode(&s).map_err(D::Error::custom)?;

        if bytes.len() != BLS_G1_BYTES_LEN {
            return Err(D::Error::custom(format!(
                "incorrect byte length {}, expected {}",
                bytes.len(),
                BLS_G1_BYTES_LEN
            )));
        }

        let mut array = [0; BLS_G1_BYTES_LEN];
        array[..].copy_from_slice(&bytes);

        Ok(array)
    }
}
