// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 RustFS Team

use std::hint::black_box;

fn main() {
    match std::env::args().nth(1).as_deref() {
        Some("control") => {}
        Some("sha256") => {
            black_box(rustfs_utils::crypto::hex_sha256(b"canonical request", str::len));
        }
        Some("fragments") => {
            let fragments = [b"first".as_slice().into(), b"second".as_slice().into()];
            black_box(rustfs_utils::crypto::hex_sha256_chunk(&fragments, str::len));
        }
        Some("hmac") => {
            black_box(rustfs_utils::crypto::hmac_sha256([0x31; 32], b"string to sign"));
        }
        _ => panic!("unknown SigV4 probe"),
    }
}
