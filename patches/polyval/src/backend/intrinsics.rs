//! Support for CPU feature autodetection with a portable pure Rust fallback.

use crate::{Block, Key, ParBlocks, Tag, field_element::FieldElement};

#[cfg_attr(target_arch = "aarch64", path = "intrinsics/neon.rs")]
#[cfg_attr(
    any(target_arch = "x86_64", target_arch = "x86"),
    path = "intrinsics/avx2.rs"
)]
mod intrinsics_impl;
use intrinsics_impl::InitToken;

#[cfg(feature = "zeroize")]
use zeroize::Zeroize;

/// State of a POLYVAL hash operation.
#[derive(Clone)]
pub(crate) struct State {
    /// Expanded key.
    expanded_key: ExpandedKey,

    /// Accumulator for the POLYVAL computation in progress (a.k.a. `y`).
    acc: FieldElement,

    /// CPU feature detection initialization token.
    init_token: InitToken,
}

impl State {
    pub(crate) fn new(h: &Key) -> Self {
        let (init_token, has_intrinsics) = InitToken::init_get();

        let expanded_key = if has_intrinsics {
            // SAFETY: we have just used CPU feature detection to ensure intrinsics are available
            unsafe { intrinsics_impl::expand_key(&h.0) }
        } else {
            // Fallback to software-only implementation
            // TODO(tarcieri): use `ExpandedKey` space to store powers-of-H
            ExpandedKey {
                h1: FieldElement::from(*h),
                ..Default::default()
            }
        };

        let y = FieldElement::default();

        Self {
            expanded_key,
            acc: y,
            init_token,
        }
    }

    pub(crate) fn proc_block(&mut self, block: &Block) {
        self.acc = if self.has_intrinsics() {
            // SAFETY: we have just used CPU feature detection to ensure intrinsics are available
            unsafe { intrinsics_impl::proc_block(&self.expanded_key, self.acc, block) }
        } else {
            (self.acc + block.into()) * self.expanded_key.h1
        };
    }

    pub(crate) fn proc_par_blocks(&mut self, par_blocks: &ParBlocks) {
        if self.has_intrinsics() {
            // SAFETY: we have just used CPU feature detection to ensure intrinsics are available
            self.acc = unsafe {
                intrinsics_impl::proc_par_blocks(&self.expanded_key, self.acc, par_blocks)
            };
        } else {
            // TODO(tarcieri): use powers-of-H since we have the space in `ExpandedKey`
            for block in par_blocks {
                self.proc_block(block);
            }
        }
    }

    pub(crate) fn finalize(&self) -> Tag {
        self.acc.into()
    }

    pub(crate) fn reset(&mut self) {
        self.acc = FieldElement::default();
    }

    #[inline]
    fn has_intrinsics(&self) -> bool {
        self.init_token.get()
    }
}

#[cfg(feature = "zeroize")]
impl Zeroize for State {
    fn zeroize(&mut self) {
        self.expanded_key.zeroize();
        self.acc.zeroize();
    }
}

/// Precomputed key material for POLYVAL using R/F algorithm
///
/// Stores H and D values for each power, where D = swap(H) ⊕ (H0 × P1)
#[derive(Clone, Default)]
pub(crate) struct ExpandedKey {
    /// H^1 packed as [h1_hi : h1_lo]
    h1: FieldElement,
    /// D^1 = computed from H^1
    d1: FieldElement,
    /// H^2
    h2: FieldElement,
    /// D^2
    d2: FieldElement,
    /// H^3
    h3: FieldElement,
    /// D^3
    d3: FieldElement,
    /// H^4
    h4: FieldElement,
    /// D^4
    d4: FieldElement,
}

#[cfg(feature = "zeroize")]
impl Zeroize for ExpandedKey {
    fn zeroize(&mut self) {
        self.h1.zeroize();
        self.d1.zeroize();
        self.h2.zeroize();
        self.d2.zeroize();
        self.h3.zeroize();
        self.d3.zeroize();
        self.h4.zeroize();
        self.d4.zeroize();
    }
}
