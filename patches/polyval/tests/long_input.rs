//! Longer test cases to ensure that long-input optimizations behave correctly.

use hex_literal::hex;
use polyval::{BLOCK_SIZE, Polyval, universal_hash::UniversalHash};

//
// Test vectors for POLYVAL from RFC 8452 Appendix A
// <https://tools.ietf.org/html/rfc8452#appendix-A>
//

const H: [u8; BLOCK_SIZE] = hex!("25629347589242761d31f826ba4b757b");

#[test]
fn longer_test() {
    let inp = (1u16..=4096)
        .map(|n| ((n * 3) % 0xFF) as u8)
        .collect::<Vec<u8>>();

    // Try computing polyval all at once.
    let mut poly = Polyval::new(&H.into());
    poly.update_padded(&inp);
    let result1 = poly.finalize_reset();

    // Try computing polyval one block at a time.
    for block in inp.chunks(BLOCK_SIZE) {
        poly.update(&[block.try_into().unwrap()]);
    }
    let result2 = poly.finalize();

    // Make sure the results are the same.
    assert_eq!(result1, result2);
}
