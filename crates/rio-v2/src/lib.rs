// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! rio_v2 selectively replaces legacy rio components while keeping the
//! remaining API surface stable for feature-gated integration.

mod compress_reader;
mod encrypt_reader;

pub use compress_reader::{CompressReader, DecompressReader};
pub use encrypt_reader::{DecryptReader, EncryptReader, derive_part_key};
pub use rustfs_rio::DEFAULT_ENCRYPTION_BLOCK_SIZE;
pub use rustfs_rio::DynReader;
pub use rustfs_rio::EtagReader;
pub use rustfs_rio::EtagResolvable;
pub use rustfs_rio::HardLimitReader;
pub use rustfs_rio::HashReader;
pub use rustfs_rio::HashReaderDetector;
pub use rustfs_rio::HashReaderMut;
pub use rustfs_rio::Index;
pub use rustfs_rio::LimitReader;
pub use rustfs_rio::ReadStream;
pub use rustfs_rio::Reader;
pub use rustfs_rio::ReaderCapabilities;
pub use rustfs_rio::TryGetIndex;
pub use rustfs_rio::WarpReader;
pub use rustfs_rio::boxed_reader;
pub use rustfs_rio::read_checksums;
pub use rustfs_rio::resolve_etag_generic;
pub use rustfs_rio::wrap_reader;

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tokio::io::AsyncReadExt;

    const DARE_VERSION_20: u8 = 0x20;
    const DARE_HEADER_SIZE: usize = 16;
    const DARE_TAG_SIZE: usize = 16;
    const DARE_PAYLOAD_SIZE: usize = 64 * 1024;
    const DARE_PACKAGE_SIZE: usize = DARE_HEADER_SIZE + DARE_PAYLOAD_SIZE + DARE_TAG_SIZE;

    #[tokio::test]
    async fn encrypt_reader_emits_dare_v2_packages() {
        let plaintext = vec![0x5Au8; DARE_PAYLOAD_SIZE + 17];
        let key_bytes = [0x11u8; 32];
        let base_nonce = [0x22u8; 12];

        let mut encrypted = Vec::new();
        EncryptReader::new(Cursor::new(plaintext), key_bytes, base_nonce)
            .read_to_end(&mut encrypted)
            .await
            .expect("encrypt plaintext into rio_v2 stream");

        assert!(
            encrypted.len() > DARE_PACKAGE_SIZE,
            "expected at least one full DARE package and one final package"
        );
        assert_eq!(encrypted[0], DARE_VERSION_20, "rio_v2 encrypted streams must start with a DARE V2 header");
        assert_eq!(
            &encrypted[4..16],
            &base_nonce,
            "rio_v2 should preserve the configured nonce in the first DARE header"
        );

        let second_header_offset = DARE_PACKAGE_SIZE;
        assert_eq!(
            encrypted[second_header_offset], DARE_VERSION_20,
            "rio_v2 should emit subsequent DARE V2 package headers at 64KiB boundaries"
        );
        assert_ne!(
            encrypted[second_header_offset + 4] & 0x80,
            0,
            "the final DARE package must set the final flag"
        );
    }
}
