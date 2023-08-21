#[derive(Clone, Copy)]
pub struct KzgCommitment(pub [u8; c_kzg::BYTES_PER_COMMITMENT]);

impl From<KzgCommitment> for c_kzg::Bytes48 {
    fn from(value: KzgCommitment) -> Self {
        value.0.into()
    }
}
