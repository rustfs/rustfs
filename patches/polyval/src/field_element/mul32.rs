//! Constant-time software implementation of POLYVAL for 32-bit architectures
//! Adapted from BearSSL's `ghash_ctmul32.c`:
//!
//! <https://bearssl.org/gitweb/?p=BearSSL;a=blob;f=src/hash/ghash_ctmul32.c;hb=4b6046412>
//!
//! Copyright (c) 2016 Thomas Pornin <pornin@bolet.org>
//!
//! This implementation is designed for 32-bit CPUs which lack a widening multiply instruction such
//! as the ARM Cortex M0 and M0+, whose multiplication opcode does not yield the upper 32-bits.
//! We use the `cpubits` crate to distinguish CPUs which do support widening multiply such as ARMv7
//! and promote them to use the 64-bit implementation (please open an issue on `cpubits` if you feel
//! an architecture should receive such a promotion).
//!
//! It might also be useful on architectures where access to the upper 32-bits requires use of
//! specific registers that create contention (e.g. on i386, "mul" necessarily outputs the result
//! in `edx:eax`, while `imul` can use any registers but is limited to the low 32 bits).
//!
//! The implementation trick that is used here is bit-reversing (bit 0 is swapped with bit 31, bit 1
//! with bit 30, and so on). In GF(2)[X], for all values x and y, we have:
//!
//! ```text
//! x.reverse_bits() * y.reverse_bits() = (x * y).reverse_bits()
//! ```
//!
//! In other words, if we bit-reverse (over 32-bits) the operands then we bit-reverse (over 64-bits)
//! the result.

use super::bmul;
use crate::field_element::FieldElement;
use core::{array, ops::BitXor};

impl FieldElement {
    /// Compute the unreduced 256-bit carryless product of two 128-bit field elements using 32-bit
    /// limbs.
    ///
    /// Uses a Karatsuba decomposition in which the 128x128 multiplication is reduced to three 64x64
    /// multiplications, hence nine 32x32 multiplications. With the bit-reversal trick, we have to
    /// perform 18 32x32 multiplications.
    #[inline]
    pub(crate) fn karatsuba_mul(self, rhs: FieldElement) -> Product {
        let hw = self.to_u32x4();
        let yw = rhs.to_u32x4();
        let hwr = [
            hw[0].reverse_bits(),
            hw[1].reverse_bits(),
            hw[2].reverse_bits(),
            hw[3].reverse_bits(),
        ];

        // Karatsuba input decomposition for H
        let mut a = [0u32; 18];
        a[0] = yw[0];
        a[1] = yw[1];
        a[2] = yw[2];
        a[3] = yw[3];
        a[4] = a[0] ^ a[1];
        a[5] = a[2] ^ a[3];
        a[6] = a[0] ^ a[2];
        a[7] = a[1] ^ a[3];
        a[8] = a[6] ^ a[7];
        a[9] = yw[0].reverse_bits();
        a[10] = yw[1].reverse_bits();
        a[11] = yw[2].reverse_bits();
        a[12] = yw[3].reverse_bits();
        a[13] = a[9] ^ a[10];
        a[14] = a[11] ^ a[12];
        a[15] = a[9] ^ a[11];
        a[16] = a[10] ^ a[12];
        a[17] = a[15] ^ a[16];

        // Karatsuba input decomposition for Y
        let mut b = [0u32; 18];
        b[0] = hw[0];
        b[1] = hw[1];
        b[2] = hw[2];
        b[3] = hw[3];
        b[4] = b[0] ^ b[1];
        b[5] = b[2] ^ b[3];
        b[6] = b[0] ^ b[2];
        b[7] = b[1] ^ b[3];
        b[8] = b[6] ^ b[7];
        b[9] = hwr[0];
        b[10] = hwr[1];
        b[11] = hwr[2];
        b[12] = hwr[3];
        b[13] = b[9] ^ b[10];
        b[14] = b[11] ^ b[12];
        b[15] = b[9] ^ b[11];
        b[16] = b[10] ^ b[12];
        b[17] = b[15] ^ b[16];

        // 18 carryless 32x32 multiplications
        let mut c = [0u32; 18];
        for i in 0..18 {
            c[i] = bmul(a[i], b[i], 0x1111_1111);
        }

        // Karatsuba recombination (normal)
        c[4] ^= c[0] ^ c[1];
        c[5] ^= c[2] ^ c[3];
        c[8] ^= c[6] ^ c[7];

        // Karatsuba recombination (bit-reversed)
        c[13] ^= c[9] ^ c[10];
        c[14] ^= c[11] ^ c[12];
        c[17] ^= c[15] ^ c[16];

        // Assemble the final 256-bit product as `u32` x 8.
        let zw0 = c[0];
        let zw1 = c[4] ^ c[9].reverse_bits() >> 1;
        let zw2 = c[1] ^ c[0] ^ c[2] ^ c[6] ^ c[13].reverse_bits() >> 1;
        let zw3 = c[4] ^ c[5] ^ c[8] ^ (c[10] ^ c[9] ^ c[11] ^ c[15]).reverse_bits() >> 1;
        let zw4 = c[2] ^ c[1] ^ c[3] ^ c[7] ^ (c[13] ^ c[14] ^ c[17]).reverse_bits() >> 1;
        let zw5 = c[5] ^ (c[11] ^ c[10] ^ c[12] ^ c[16]).reverse_bits() >> 1;
        let zw6 = c[3] ^ c[14].reverse_bits() >> 1;
        let zw7 = c[12].reverse_bits() >> 1;
        Product([zw0, zw1, zw2, zw3, zw4, zw5, zw6, zw7])
    }

    #[inline]
    fn from_u32x4(v: [u32; 4]) -> FieldElement {
        let mut bytes = [0u8; 16];
        bytes[0..4].copy_from_slice(&v[0].to_le_bytes());
        bytes[4..8].copy_from_slice(&v[1].to_le_bytes());
        bytes[8..12].copy_from_slice(&v[2].to_le_bytes());
        bytes[12..16].copy_from_slice(&v[3].to_le_bytes());
        FieldElement(bytes)
    }

    #[inline]
    fn to_u32x4(self) -> [u32; 4] {
        [
            u32::from_le_bytes([self.0[0], self.0[1], self.0[2], self.0[3]]),
            u32::from_le_bytes([self.0[4], self.0[5], self.0[6], self.0[7]]),
            u32::from_le_bytes([self.0[8], self.0[9], self.0[10], self.0[11]]),
            u32::from_le_bytes([self.0[12], self.0[13], self.0[14], self.0[15]]),
        ]
    }
}

/// Unreduced 256-bit carryless product.
pub(crate) struct Product([u32; 8]);

impl Product {
    /// Reduce the 256-bit carryless product of Karatsuba modulo the POLYVAL polynomial.
    ///
    /// This performs constant-time folding using shifts and XORs corresponding to the irreducible
    /// polynomial `x^128 + x^127 + x^126 + x^121 + 1`.
    ///
    /// This is closely related to GHASH reduction but the bit order is reversed in POLYVAL.
    #[inline]
    pub(crate) fn mont_reduce(self) -> FieldElement {
        let mut zw = self.0;

        for i in 0..4 {
            let lw = zw[i];
            zw[i + 4] ^= lw ^ (lw >> 1) ^ (lw >> 2) ^ (lw >> 7);
            zw[i + 3] ^= (lw << 31) ^ (lw << 30) ^ (lw << 25);
        }

        FieldElement::from_u32x4([zw[4], zw[5], zw[6], zw[7]])
    }
}

impl BitXor for Product {
    type Output = Self;

    #[inline]
    fn bitxor(self, rhs: Self) -> Self::Output {
        Self(array::from_fn(|n| self.0[n] ^ rhs.0[n]))
    }
}
