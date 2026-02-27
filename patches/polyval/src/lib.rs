#![no_std]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/RustCrypto/media/8f1a9894/logo.svg",
    html_favicon_url = "https://raw.githubusercontent.com/RustCrypto/media/8f1a9894/logo.svg"
)]

#[cfg(feature = "hazmat")]
pub mod hazmat;

mod backend;
mod field_element;

pub use universal_hash;

use crate::backend::State;
use core::fmt::{self, Debug};
use universal_hash::{
    KeyInit, Reset, UhfBackend, UhfClosure, UniversalHash,
    common::{BlockSizeUser, KeySizeUser, ParBlocksSizeUser},
    consts::{U4, U16},
};

#[cfg(feature = "zeroize")]
use zeroize::Zeroize;

/// Size of a POLYVAL block in bytes
pub const BLOCK_SIZE: usize = 16;

/// Size of a POLYVAL key in bytes
pub const KEY_SIZE: usize = 16;

/// POLYVAL keys (16-bytes)
pub type Key = universal_hash::Key<Polyval>;

/// POLYVAL blocks (16-bytes)
pub type Block = universal_hash::Block<Polyval>;

/// POLYVAL parallel blocks (4 x 16-bytes)
pub type ParBlocks = universal_hash::ParBlocks<Polyval>;

/// POLYVAL tags (16-bytes)
pub type Tag = universal_hash::Block<Polyval>;

/// **POLYVAL**: GHASH-like universal hash over GF(2^128), but optimized for little-endian
/// architectures.
#[derive(Clone)]
pub struct Polyval {
    /// State of the internal hash being computed.
    state: State,
}

impl Polyval {
    /// Initialize POLYVAL with the given `H` field element (i.e. hash key).
    #[must_use]
    pub fn new(h: &Key) -> Self {
        Self {
            state: State::new(h),
        }
    }
}

impl KeyInit for Polyval {
    fn new(h: &Key) -> Self {
        Self::new(h)
    }
}

impl KeySizeUser for Polyval {
    type KeySize = U16;
}

impl BlockSizeUser for Polyval {
    type BlockSize = U16;
}

impl ParBlocksSizeUser for Polyval {
    type ParBlocksSize = U4;
}

impl UniversalHash for Polyval {
    fn update_with_backend(&mut self, f: impl UhfClosure<BlockSize = Self::BlockSize>) {
        f.call(self);
    }

    fn finalize(self) -> Tag {
        self.state.finalize()
    }
}

impl UhfBackend for Polyval {
    fn proc_block(&mut self, block: &Block) {
        self.state.proc_block(block);
    }

    fn proc_par_blocks(&mut self, blocks: &ParBlocks) {
        self.state.proc_par_blocks(blocks);
    }
}

impl Reset for Polyval {
    fn reset(&mut self) {
        self.state.reset();
    }
}

impl Debug for Polyval {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.debug_struct("Polyval").finish_non_exhaustive()
    }
}

impl Drop for Polyval {
    fn drop(&mut self) {
        #[cfg(feature = "zeroize")]
        self.state.zeroize();
    }
}

#[cfg(test)]
mod tests {
    use crate::{BLOCK_SIZE, Polyval, universal_hash::UniversalHash};
    use hex_literal::hex;

    //
    // Test vectors for POLYVAL from RFC 8452 Appendix A
    // <https://tools.ietf.org/html/rfc8452#appendix-A>
    //

    const H: [u8; BLOCK_SIZE] = hex!("25629347589242761d31f826ba4b757b");
    const X_1: [u8; BLOCK_SIZE] = hex!("4f4f95668c83dfb6401762bb2d01a262");
    const X_2: [u8; BLOCK_SIZE] = hex!("d1a24ddd2721d006bbe45f20d3c9f362");

    /// POLYVAL(H, X_1, X_2)
    const POLYVAL_RESULT: [u8; BLOCK_SIZE] = hex!("f7a3b47b846119fae5b7866cf5e5b77e");

    #[test]
    fn polyval_test_vector() {
        let mut poly = Polyval::new(&H.into());
        poly.update(&[X_1.into(), X_2.into()]);

        let result = poly.finalize();
        assert_eq!(&POLYVAL_RESULT[..], result.as_slice());
    }
}
