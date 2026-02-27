//! ARM NEON + PMULL optimized POLYVAL implementation using R/F Algorithm
//! Adapted from the implementation in the Apache 2.0+MIT-licensed HPCrypt library
//! Copyright (c) 2024 HPCrypt Contributors
//!
//! This implementation uses the R/F (Reduction/Field) algorithm:
//! - 4 PMULL per block for R and F terms
//! - PMULL-based reduction (1 PMULL) instead of scalar shifts
//! - 4-block aggregated processing with single reduction
//!
//! Key equations:
//! - D = swap(H) ⊕ (H0 × P1)
//! - R = M0×D1 ⊕ M1×H1
//! - F = M0×D0 ⊕ M1×H0
//! - Result = R ⊕ F1 ⊕ (x^64×F0) ⊕ (P1×F0)
//!
//! POLYVAL operates in GF(2^128) with polynomial x^128 + x^127 + x^126 + x^121 + 1
//! Unlike GHASH, POLYVAL uses little-endian byte ordering (no byte swap needed).
//!
//! <https://eprint.iacr.org/2025/2171.pdf>

#![allow(unsafe_op_in_unsafe_fn)]

use super::ExpandedKey;
use crate::{Block, ParBlocks, field_element::FieldElement};
use core::arch::aarch64::*;

/// P1 polynomial: x^63 + x^62 + x^57 = 0xC200000000000000
const P1: u64 = 0xC200000000000000;

// `aes` implies PMULL
cpufeatures::new!(pmull, "aes");
pub(crate) use pmull::InitToken;

/// Byte array which is the inner type of `FieldElement`
type ByteArray = [u8; 16];

impl FieldElement {
    #[target_feature(enable = "neon", enable = "aes")]
    #[inline]
    unsafe fn from_uint64x2_t(reg: uint64x2_t) -> Self {
        let mut out = ByteArray::default();
        vst1q_u8(out.as_mut_ptr(), vreinterpretq_u8_u64(reg));
        out.into()
    }

    #[target_feature(enable = "neon", enable = "aes")]
    #[inline]
    unsafe fn to_uint64x2_t(self) -> uint64x2_t {
        load_bytes(&self.into())
    }
}

/// Convert 16 bytes into `uint64x2_t`.
///
/// # Safety
/// Requires NEON and AES/PMULL support
#[target_feature(enable = "neon", enable = "aes")]
#[inline]
unsafe fn load_bytes(bytes: &ByteArray) -> uint64x2_t {
    vreinterpretq_u64_u8(vld1q_u8(bytes.as_ptr()))
}

/// Process a single block (5 PMULLs)
///
/// # Safety
/// Requires NEON and AES/PMULL support
#[target_feature(enable = "neon", enable = "aes")]
#[inline]
pub(super) unsafe fn proc_block(key: &ExpandedKey, y: FieldElement, block: &Block) -> FieldElement {
    let data = load_bytes(&block.0);

    // XOR with accumulator
    let y = veorq_u64(y.to_uint64x2_t(), data);

    // Multiply by H using R/F algorithm
    FieldElement::from_uint64x2_t(gf128_mul_rf(
        y,
        key.h1.to_uint64x2_t(),
        key.d1.to_uint64x2_t(),
    ))
}

/// Process 4 blocks with R/F algorithm and aggregated reduction
///
/// Uses 16 PMULLs for multiplication (4 per block) + 1 PMULL for reduction = 17 PMULLs total
#[target_feature(enable = "neon", enable = "aes")]
#[inline]
pub(super) unsafe fn proc_par_blocks(
    key: &ExpandedKey,
    acc: FieldElement,
    par_blocks: &ParBlocks,
) -> FieldElement {
    // Load all 4 blocks (no byte swap for POLYVAL)
    let m0 = load_bytes(&par_blocks[0].0);
    let m1 = load_bytes(&par_blocks[1].0);
    let m2 = load_bytes(&par_blocks[2].0);
    let m3 = load_bytes(&par_blocks[3].0);

    // XOR first block with accumulator
    let y0 = veorq_u64(acc.to_uint64x2_t(), m0);

    // R/F multiply all 4 blocks (16 PMULLs)
    let (r0, f0) = rf_mul_unreduced(y0, key.h4.to_uint64x2_t(), key.d4.to_uint64x2_t());
    let (r1, f1) = rf_mul_unreduced(m1, key.h3.to_uint64x2_t(), key.d3.to_uint64x2_t());
    let (r2, f2) = rf_mul_unreduced(m2, key.h2.to_uint64x2_t(), key.d2.to_uint64x2_t());
    let (r3, f3) = rf_mul_unreduced(m3, key.h1.to_uint64x2_t(), key.d1.to_uint64x2_t());

    // Aggregate R and F values
    let r = veorq_u64(veorq_u64(r0, r1), veorq_u64(r2, r3));
    let f = veorq_u64(veorq_u64(f0, f1), veorq_u64(f2, f3));

    // Single reduction (1 PMULL)
    FieldElement::from_uint64x2_t(reduce_rf(r, f))
}

/// Create a new POLYVAL key with R/F algorithm
///
/// # Safety
/// Requires NEON and AES/PMULL support
#[target_feature(enable = "neon", enable = "aes")]
pub(super) unsafe fn expand_key(h: &[u8; 16]) -> ExpandedKey {
    let h1 = load_bytes(h);
    let d1 = compute_d(h1);

    // Compute powers using R/F multiplication
    let h2 = gf128_mul_rf(h1, h1, d1);
    let d2 = compute_d(h2);

    let h3 = gf128_mul_rf(h2, h1, d1);
    let d3 = compute_d(h3);

    let h4 = gf128_mul_rf(h2, h2, d2);
    let d4 = compute_d(h4);

    ExpandedKey {
        h1: FieldElement::from_uint64x2_t(h1),
        d1: FieldElement::from_uint64x2_t(d1),
        h2: FieldElement::from_uint64x2_t(h2),
        d2: FieldElement::from_uint64x2_t(d2),
        h3: FieldElement::from_uint64x2_t(h3),
        d3: FieldElement::from_uint64x2_t(d3),
        h4: FieldElement::from_uint64x2_t(h4),
        d4: FieldElement::from_uint64x2_t(d4),
    }
}

/// Compute D from H using R/F algorithm
///
/// D = swap(H) ⊕ (H0 × P1)
#[target_feature(enable = "neon", enable = "aes")]
#[inline]
unsafe fn compute_d(h: uint64x2_t) -> uint64x2_t {
    // Swap halves: [H1 : H0] -> [H0 : H1]
    let h_swap = vextq_u64(h, h, 1);

    // T = H0 × P1 (polynomial multiply)
    let h0 = vgetq_lane_u64(h, 0);
    let t = vreinterpretq_u64_p128(vmull_p64(h0, P1));

    // D = swap(H) ⊕ T
    veorq_u64(h_swap, t)
}

/// Complete R/F multiplication with reduction (5 PMULLs total)
#[target_feature(enable = "neon", enable = "aes")]
#[inline]
unsafe fn gf128_mul_rf(m: uint64x2_t, h: uint64x2_t, d: uint64x2_t) -> uint64x2_t {
    let (r, f) = rf_mul_unreduced(m, h, d);
    reduce_rf(r, f)
}

/// R/F multiplication: compute R and F terms (4 PMULLs)
///
/// R = M0×D1 ⊕ M1×H1
/// F = M0×D0 ⊕ M1×H0
#[target_feature(enable = "neon", enable = "aes")]
#[inline]
unsafe fn rf_mul_unreduced(
    m: uint64x2_t,
    h: uint64x2_t,
    d: uint64x2_t,
) -> (uint64x2_t, uint64x2_t) {
    let m0 = vgetq_lane_u64(m, 0);
    let m1 = vgetq_lane_u64(m, 1);
    let h0 = vgetq_lane_u64(h, 0);
    let h1 = vgetq_lane_u64(h, 1);
    let d0 = vgetq_lane_u64(d, 0);
    let d1 = vgetq_lane_u64(d, 1);

    // R = M0×D1 ⊕ M1×H1
    let r0 = vmull_p64(m0, d1);
    let r1 = vmull_p64(m1, h1);
    let r = veorq_u64(vreinterpretq_u64_p128(r0), vreinterpretq_u64_p128(r1));

    // F = M0×D0 ⊕ M1×H0
    let f0 = vmull_p64(m0, d0);
    let f1 = vmull_p64(m1, h0);
    let f = veorq_u64(vreinterpretq_u64_p128(f0), vreinterpretq_u64_p128(f1));

    (r, f)
}

/// Reduction using Lemma 3: Result = R ⊕ F1 ⊕ (x^64×F0) ⊕ (P1×F0)
///
/// Uses 1 PMULL for reduction
#[target_feature(enable = "neon", enable = "aes")]
#[inline]
unsafe fn reduce_rf(r: uint64x2_t, f: uint64x2_t) -> uint64x2_t {
    // F1 (high 64 bits of f)
    let f1 = vgetq_lane_u64(f, 1);
    let f1_vec = vcombine_u64(vcreate_u64(f1), vcreate_u64(0));

    // x^64×F0 (shift F0 to high position)
    let f0 = vgetq_lane_u64(f, 0);
    let f0_shifted = vcombine_u64(vcreate_u64(0), vcreate_u64(f0));

    // P1×F0
    let p1_f0 = vmull_p64(f0, P1);

    // Result = R ⊕ F1 ⊕ (x^64×F0) ⊕ (P1×F0)
    let result = veorq_u64(r, f1_vec);
    let result = veorq_u64(result, f0_shifted);
    veorq_u64(result, vreinterpretq_u64_p128(p1_f0))
}
