//! GF(2^16) arithmetic and the evaluation points of the payload chunk code, as defined in
//! `specs/_features/eip8142/beacon-chain.md`.
//!
//! Field elements are represented as integers whose bits are the coefficients of a polynomial
//! over GF(2), reduced modulo [`FIELD_MODULUS`]. Symbols on the wire are the little-endian bytes
//! of those integers.
//!
//! `reed-solomon-simd` represents the same field by coordinates in the Cantor basis
//! [`FIELD_BASIS`] instead. [`Translation`] holds the two lookup tables that convert between the
//! representations, one symbol at a time.

use std::sync::OnceLock;

/// `PAYLOAD_CHUNK_FIELD_MODULUS`: the smallest primitive polynomial of degree 16,
/// `x^16 + x^5 + x^3 + x^2 + 1`.
pub const FIELD_MODULUS: u32 = 0x1002D;

/// `PAYLOAD_CHUNK_FIELD_BASIS`: the Cantor basis of GF(2^16) that spans the evaluation points.
///
/// Its first element is `1` and each later element is the even one of the two roots of
/// `x^2 + x = c`, where `c` is the element before it. See [`derive_basis`] in the tests.
pub const FIELD_BASIS: [u16; 16] = [
    0x0001, 0xACCA, 0x3C0E, 0x163E, 0xC582, 0xED2E, 0x914C, 0x4012, 0x6C98, 0x10D8, 0x6A72, 0xB900,
    0xFDB8, 0xFB34, 0xFF38, 0x991E,
];

/// `PAYLOAD_CHUNK_SYMBOL_SIZE`: bytes per GF(2^16) symbol.
pub const SYMBOL_SIZE: usize = 2;

/// Number of elements of GF(2^16).
pub const FIELD_ORDER: usize = 1 << 16;

/// `gf16_multiply` from the spec.
pub fn gf16_multiply(a: u16, b: u16) -> u16 {
    let mut a = a as u32;
    let mut b = b as u32;
    let mut product = 0u32;
    while b > 0 {
        if b & 1 == 1 {
            product ^= a;
        }
        a <<= 1;
        if a >= 1 << 16 {
            a ^= FIELD_MODULUS;
        }
        b >>= 1;
    }
    product as u16
}

/// `gf16_inverse` from the spec. Returns `None` for zero, which has no inverse.
pub fn gf16_inverse(a: u16) -> Option<u16> {
    if a == 0 {
        return None;
    }
    let mut result = 1u16;
    let mut base = a;
    let mut exponent = (1u32 << 16) - 2;
    while exponent > 0 {
        if exponent & 1 == 1 {
            result = gf16_multiply(result, base);
        }
        base = gf16_multiply(base, base);
        exponent >>= 1;
    }
    Some(result)
}

/// `get_payload_chunk_point` from the spec: the sum of the basis elements selected by the bits of
/// `position`.
///
/// `position` must be below [`FIELD_ORDER`].
pub fn chunk_point(position: usize) -> u16 {
    debug_assert!(position < FIELD_ORDER);
    FIELD_BASIS
        .iter()
        .enumerate()
        .filter(|(bit, _)| (position >> bit) & 1 == 1)
        .fold(0, |point, (_, basis_element)| point ^ basis_element)
}

/// Lookup tables between the spec's polynomial-basis representation of field elements and the
/// Cantor-basis coordinates that `reed-solomon-simd` uses for shard symbols.
pub struct Translation {
    /// Indexed by a polynomial-basis element, gives its Cantor coordinates.
    to_cantor: Box<[u16]>,
    /// Indexed by Cantor coordinates, gives the polynomial-basis element.
    to_poly: Box<[u16]>,
}

impl Translation {
    fn build() -> Self {
        let mut to_poly = vec![0u16; FIELD_ORDER].into_boxed_slice();
        let mut to_cantor = vec![0u16; FIELD_ORDER].into_boxed_slice();
        for coordinates in 0..FIELD_ORDER {
            let element = chunk_point(coordinates);
            to_poly[coordinates] = element;
            to_cantor[element as usize] = coordinates as u16;
        }
        Self { to_cantor, to_poly }
    }

    /// The process-wide tables, built on first use.
    pub fn get() -> &'static Self {
        static TABLES: OnceLock<Translation> = OnceLock::new();
        TABLES.get_or_init(Self::build)
    }

    #[inline]
    pub fn to_cantor(&self, element: u16) -> u16 {
        self.to_cantor[element as usize]
    }

    #[inline]
    pub fn to_poly(&self, coordinates: u16) -> u16 {
        self.to_poly[coordinates as usize]
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// `x^2 + x`, the map whose iterated kernels are the Cantor subspaces.
    fn phi(x: u16) -> u16 {
        gf16_multiply(x, x) ^ x
    }

    /// Derives the basis from the rule in the spec: start at one, then take the even root of
    /// `x^2 + x = previous` at every step.
    pub(crate) fn derive_basis() -> [u16; 16] {
        let mut basis = [0u16; 16];
        basis[0] = 1;
        for i in 1..16 {
            let previous = basis[i - 1];
            basis[i] = (0..FIELD_ORDER as u32)
                .map(|x| x as u16)
                .find(|&x| x & 1 == 0 && phi(x) == previous)
                .expect("x^2 + x = c has a root for every basis element");
        }
        basis
    }

    #[test]
    fn modulus_is_primitive() {
        // The powers of x visit every non-zero element exactly once.
        let mut state = 1u16;
        for i in 1..FIELD_ORDER {
            state = gf16_multiply(state, 2);
            assert_eq!(state == 1, i == FIELD_ORDER - 1, "period is not 2^16 - 1");
        }
    }

    #[test]
    fn basis_follows_the_rule() {
        assert_eq!(derive_basis(), FIELD_BASIS);
    }

    #[test]
    fn basis_spans_kernels_of_phi() {
        // The first `t` basis elements span the kernel of the `t`-fold iterate of `phi`, so the
        // evaluation set is canonical whatever tie-break the rule used.
        for t in 1..=5 {
            let span: std::collections::BTreeSet<u16> = (0..1usize << t).map(chunk_point).collect();
            let kernel: std::collections::BTreeSet<u16> = (0..FIELD_ORDER as u32)
                .map(|x| x as u16)
                .filter(|&x| (0..t).fold(x, |y, _| phi(y)) == 0)
                .collect();
            assert_eq!(span, kernel);
        }
    }

    #[test]
    fn inverse_inverts() {
        for a in [1u16, 2, 3, 0x1234, 0xACCA, 0xFFFF] {
            assert_eq!(gf16_multiply(a, gf16_inverse(a).unwrap()), 1);
        }
        assert_eq!(gf16_inverse(0), None);
    }

    #[test]
    fn translation_is_a_bijection() {
        let t = Translation::get();
        for v in 0..FIELD_ORDER as u32 {
            let v = v as u16;
            assert_eq!(t.to_cantor(t.to_poly(v)), v);
            assert_eq!(t.to_poly(t.to_cantor(v)), v);
        }
        assert_eq!(t.to_poly(0), 0);
        assert_eq!(t.to_poly(1), 1);
        assert_eq!(t.to_poly(2), 0xACCA);
    }
}
