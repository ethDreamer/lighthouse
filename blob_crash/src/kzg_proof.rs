#[derive(PartialEq, Hash, Clone, Copy)]
pub struct KzgProof(pub [u8; c_kzg::BYTES_PER_PROOF]);

impl From<KzgProof> for c_kzg::Bytes48 {
    fn from(value: KzgProof) -> Self {
        value.0.into()
    }
}

