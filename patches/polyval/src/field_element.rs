//! POLYVAL field element implementation.
//!
//! This module contains a portable pure Rust implementation which can computes carryless POLYVAL
//! multiplication over GF (2^128) in constant time. Both 32-bit and 64-bit backends are available.
//!
//! Method described at: <https://www.bearssl.org/constanttime.html#ghash-for-gcm>
//!
//! POLYVAL multiplication is effectively the little endian equivalent of GHASH multiplication,
//! aside from one small detail described here:
//!
//! <https://crypto.stackexchange.com/questions/66448/how-does-bearssls-gcm-modular-reduction-work/66462#66462>
//!
//! > The product of two bit-reversed 128-bit polynomials yields the
//! > bit-reversed result over 255 bits, not 256. The BearSSL code ends up
//! > with a 256-bit result in zw[], and that value is shifted by one bit,
//! > because of that reversed convention issue. Thus, the code must
//! > include a shifting step to put it back where it should
//!
//! This shift is unnecessary for POLYVAL (it is in fact what distinguishes POLYVAL from GHASH) and
//! has been removed.

cpubits::cpubits! {
    16 | 32 => {
        #[path = "field_element/mul32.rs"]
        mod mul;
    }
    64 => {
        #[path = "field_element/mul64.rs"]
        mod mul;
    }
}

mod mulx;

use crate::{BLOCK_SIZE, Block};
use core::{
    fmt::{self, Debug},
    num::Wrapping,
    ops::{Add, BitAnd, BitOr, BitXor, Mul, MulAssign, Shl},
};

#[cfg(feature = "zeroize")]
use zeroize::Zeroize;

/// An element in POLYVAL's field.
///
/// This type represents an element of the binary field GF(2^128) modulo the irreducible polynomial
/// `x^128 + x^127 + x^126 + x^121 + 1` as described in [RFC8452 §3].
///
/// Arithmetic in POLYVAL's field has the following properties:
/// - All arithmetic operations are performed modulo the polynomial above.
/// - Addition is equivalent to the XOR operation applied to the two field elements
/// - Multiplication is carryless
///
/// [RFC8452 §3]: https://tools.ietf.org/html/rfc8452#section-3
#[derive(Clone, Copy, Default)]
#[cfg_attr(test, derive(Eq, PartialEq))]
#[repr(align(16))] // Alignment-friendly for SIMD registers
pub struct FieldElement([u8; BLOCK_SIZE]);

impl FieldElement {
    /// Reverse this field element at a byte-level of granularity.
    ///
    /// This is useful when implementing GHASH in terms of POLYVAL.
    #[inline]
    #[must_use]
    pub fn reverse(mut self) -> Self {
        self.0.reverse();
        self
    }
}

impl Debug for FieldElement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FieldElement(")?;
        for byte in self.0 {
            write!(f, "{:02x}", byte)?;
        }
        write!(f, ")")
    }
}

impl From<Block> for FieldElement {
    #[inline]
    fn from(block: Block) -> Self {
        Self(block.into())
    }
}

impl From<&Block> for FieldElement {
    #[inline]
    fn from(block: &Block) -> Self {
        Self::from(*block)
    }
}

impl From<FieldElement> for Block {
    #[inline]
    fn from(fe: FieldElement) -> Self {
        fe.0.into()
    }
}

impl From<&FieldElement> for Block {
    #[inline]
    fn from(fe: &FieldElement) -> Self {
        Self::from(*fe)
    }
}

impl From<[u8; BLOCK_SIZE]> for FieldElement {
    #[inline]
    fn from(bytes: [u8; BLOCK_SIZE]) -> Self {
        Self(bytes)
    }
}

impl From<FieldElement> for [u8; BLOCK_SIZE] {
    #[inline]
    fn from(fe: FieldElement) -> [u8; BLOCK_SIZE] {
        fe.0
    }
}

impl From<u128> for FieldElement {
    #[inline]
    fn from(x: u128) -> Self {
        Self(x.to_le_bytes())
    }
}

impl From<FieldElement> for u128 {
    #[inline]
    fn from(fe: FieldElement) -> Self {
        u128::from_le_bytes(fe.0)
    }
}

impl Add for FieldElement {
    type Output = Self;

    /// Adds two POLYVAL field elements.
    ///
    /// In POLYVAL's field, addition is the equivalent operation to XOR.
    #[inline]
    fn add(self, rhs: Self) -> Self::Output {
        (u128::from(self) ^ u128::from(rhs)).into()
    }
}

impl Mul for FieldElement {
    type Output = Self;

    /// Perform carryless multiplication within POLYVAL's field modulo its polynomial.
    #[inline]
    fn mul(self, rhs: Self) -> Self {
        self.karatsuba_mul(rhs).mont_reduce()
    }
}

impl MulAssign for FieldElement {
    #[inline]
    fn mul_assign(&mut self, rhs: Self) {
        *self = *self * rhs;
    }
}

#[cfg(feature = "zeroize")]
impl Zeroize for FieldElement {
    fn zeroize(&mut self) {
        self.0.zeroize();
    }
}

/// Multiplication in GF(2)[X], implemented generically for use with `u32` and `u64`.
///
/// Uses "holes" (sequences of zeroes) to avoid carry spilling, as specified in the mask operand
/// `m0` which should have a full-width value with the following bit pattern:
///
/// `0b100010001...0001` (e.g. `0x1111_1111u32`)
///
/// When carries do occur, they wind up in a "hole" and are subsequently masked out of the result.
#[inline]
fn bmul<T>(x: T, y: T, m0: T) -> T
where
    T: BitAnd<Output = T> + BitOr<Output = T> + Copy + Shl<u32, Output = T>,
    Wrapping<T>: BitXor<Output = Wrapping<T>> + Mul<Output = Wrapping<T>>,
{
    let m1 = m0 << 1;
    let m2 = m1 << 1;
    let m3 = m2 << 1;

    let x0 = Wrapping(x & m0);
    let x1 = Wrapping(x & m1);
    let x2 = Wrapping(x & m2);
    let x3 = Wrapping(x & m3);

    let y0 = Wrapping(y & m0);
    let y1 = Wrapping(y & m1);
    let y2 = Wrapping(y & m2);
    let y3 = Wrapping(y & m3);

    let z0 = (x0 * y0) ^ (x1 * y3) ^ (x2 * y2) ^ (x3 * y1);
    let z1 = (x0 * y1) ^ (x1 * y0) ^ (x2 * y3) ^ (x3 * y2);
    let z2 = (x0 * y2) ^ (x1 * y1) ^ (x2 * y0) ^ (x3 * y3);
    let z3 = (x0 * y3) ^ (x1 * y2) ^ (x2 * y1) ^ (x3 * y0);

    (z0.0 & m0) | (z1.0 & m1) | (z2.0 & m2) | (z3.0 & m3)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hex_literal::hex;

    const A: [u8; 16] = hex!("66e94bd4ef8a2c3b884cfa59ca342b2e");
    const B: [u8; 16] = hex!("ff000000000000000000000000000000");

    #[test]
    fn fe_add() {
        let a = FieldElement::from(A);
        let b = FieldElement::from(B);

        let expected = FieldElement::from(hex!("99e94bd4ef8a2c3b884cfa59ca342b2e"));
        assert_eq!(a + b, expected);
        assert_eq!(b + a, expected);
    }

    #[test]
    fn fe_mul() {
        let a = FieldElement::from(A);
        let b = FieldElement::from(B);

        let expected = FieldElement::from(hex!("ebe563401e7e91ea3ad6426b8140c394"));
        assert_eq!(a * b, expected);
        assert_eq!(b * a, expected);
    }
}
