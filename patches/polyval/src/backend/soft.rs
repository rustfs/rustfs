//! Portable software implementation written in terms of [`FieldElement`].
//!
//! This implementation is deliberately compact and simple, avoiding a large powers-of-H key state
//! in favor of reducing the memory footprint.

use crate::{Block, Key, ParBlocks, Tag, field_element::FieldElement};

#[cfg(feature = "zeroize")]
use zeroize::Zeroize;

/// State of a POLYVAL hash operation.
#[derive(Clone, Copy)]
pub(crate) struct State {
    /// Hash key: fixed element of GF(2^128) that parameterizes the POLYVAL universal hash function.
    ///
    /// This is the multiplier that advances POLYVAL's state. Each message block is XORed into the
    /// accumulator and then multiplied by `H`.
    h: FieldElement,

    /// Accumulator for POLYVAL computation.
    y: FieldElement,
}

impl State {
    pub(crate) fn new(h: &Key) -> Self {
        Self {
            h: FieldElement::from(*h),
            y: FieldElement::default(),
        }
    }

    pub(crate) fn proc_block(&mut self, block: &Block) {
        self.y = (self.y + block.into()) * self.h;
    }

    pub(crate) fn proc_par_blocks(&mut self, par_blocks: &ParBlocks) {
        // Just process them in sequence since we don't support anything fancy
        for block in par_blocks {
            self.proc_block(block);
        }
    }

    pub(crate) fn finalize(&self) -> Tag {
        self.y.into()
    }

    pub(crate) fn reset(&mut self) {
        self.y = FieldElement::default();
    }
}

#[cfg(feature = "zeroize")]
impl Zeroize for State {
    fn zeroize(&mut self) {
        self.h.zeroize();
        self.y.zeroize();
    }
}
