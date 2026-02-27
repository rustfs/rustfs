//! Constant-time software implementation of POLYVAL for 64-bit architectures.
//! Adapted from BearSSL's `ghash_ctmul64.c`:
//!
//! <https://bearssl.org/gitweb/?p=BearSSL;a=blob;f=src/hash/ghash_ctmul64.c;hb=4b6046412>
//!
//! Copyright (c) 2016 Thomas Pornin <pornin@bolet.org>

use crate::field_element::FieldElement;
use core::{array, ops::BitXor};

impl FieldElement {
    /// Compute the unreduced 256-bit carryless product of two 128-bit field elements.
    ///
    /// Uses a Karatsuba decomposition in which the 128x128 multiplication is reduced to three 64x64
    /// multiplications together with a bit-reversal trick to efficiently recover the high half.
    #[inline]
    pub(crate) fn karatsuba_mul(self, rhs: Self) -> Product {
        // Karatsuba input decomposition for H
        let (h0, h1) = self.to_u64x2();
        let h0r = h0.reverse_bits();
        let h1r = h1.reverse_bits();
        let h2 = h0 ^ h1;
        let h2r = h0r ^ h1r;

        // Karatsuba input decomposition for Y
        let (y0, y1) = rhs.to_u64x2();
        let y0r = y0.reverse_bits();
        let y1r = y1.reverse_bits();
        let y2 = y0 ^ y1;
        let y2r = y0r ^ y1r;

        /// Carryless multiplication in GF(2)[X], truncated to the low 64-bits.
        #[inline]
        fn bmul64(x: u64, y: u64) -> u64 {
            super::bmul(x, y, 0x1111_1111_1111_1111)
        }

        // Perform carryless multiplications
        let z0 = bmul64(y0, h0);
        let z1 = bmul64(y1, h1);
        let mut z2 = bmul64(y2, h2);
        let mut z0h = bmul64(y0r, h0r);
        let mut z1h = bmul64(y1r, h1r);
        let mut z2h = bmul64(y2r, h2r);

        // Karatsuba recombination
        z2 ^= z0 ^ z1;
        z2h ^= z0h ^ z1h;
        z0h = z0h.reverse_bits() >> 1;
        z1h = z1h.reverse_bits() >> 1;
        z2h = z2h.reverse_bits() >> 1;

        // Assemble the final 256-bit product as `u64` x 4.
        let v0 = z0;
        let v1 = z0h ^ z2;
        let v2 = z1 ^ z2h;
        let v3 = z1h;
        Product([v0, v1, v2, v3])
    }

    /// Splits into `(lo, hi)`.
    #[inline]
    fn to_u64x2(self) -> (u64, u64) {
        let x = u128::from(self);
        ((x & 0xFFFF_FFFF_FFFF_FFFF) as u64, (x >> 64) as u64)
    }
}

/// Unreduced 256-bit carryless product.
pub(crate) struct Product([u64; 4]);

impl Product {
    /// Reduce the 256-bit carryless product of Karatsuba modulo the POLYVAL polynomial.
    ///
    /// This performs constant-time folding using shifts and XORs corresponding to the irreducible
    /// polynomial `x^128 + x^127 + x^126 + x^121 + 1`.
    ///
    /// This is closely related to GHASH reduction but the bit order is reversed in POLYVAL.
    #[inline]
    pub(crate) fn mont_reduce(self) -> FieldElement {
        let (v0, mut v1, mut v2, mut v3) = (self.0[0], self.0[1], self.0[2], self.0[3]);
        v2 ^= v0 ^ (v0 >> 1) ^ (v0 >> 2) ^ (v0 >> 7);
        v1 ^= (v0 << 63) ^ (v0 << 62) ^ (v0 << 57);
        v3 ^= v1 ^ (v1 >> 1) ^ (v1 >> 2) ^ (v1 >> 7);
        v2 ^= (v1 << 63) ^ (v1 << 62) ^ (v1 << 57);
        (u128::from(v2) | u128::from(v3) << 64).into()
    }
}

impl BitXor for Product {
    type Output = Self;

    #[inline]
    fn bitxor(self, rhs: Self) -> Self::Output {
        Self(array::from_fn(|n| self.0[n] ^ rhs.0[n]))
    }
}
