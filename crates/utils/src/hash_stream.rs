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

//! Streaming (incremental) hash interface.
//!
//! [`hash`](crate::hash) owns the one-shot interface (`HashAlgorithm::hash_encode`).
//! This module owns the streaming shape: feed bytes over time, take the digest
//! at the end. Both are the *only* places RustFS touches a concrete MD5
//! implementation; every other crate goes through [`Md5Stream`] or
//! `HashAlgorithm::Md5`, never through the backing crate directly.
//!
//! Backend selection lives in the private [`backend`] module. Exactly one
//! backend is compiled in; today that is RustCrypto `md-5`. Adding another
//! backend means adding one `Md5Inner` behind a cargo feature there and
//! nothing else in the tree changes.
//!
//! # Why `finalize` consumes `self` and there is no `Clone`
//!
//! Not every MD5 implementation can snapshot its state (an assembly backend may
//! keep an uninitialised block buffer, for example). The interface is therefore
//! the intersection of what any backend can offer: append bytes, consume the
//! hasher for the digest, or take the digest and start over. Code that needs a
//! digest "so far" while continuing to feed bytes must restructure, not clone;
//! keeping that constraint on the default backend means switching backends can
//! never turn into a compile error somewhere else in the tree.

use std::fmt;
use std::io;

/// Incremental MD5 hasher: the single streaming MD5 entry point for RustFS.
///
/// Used for the S3 ETag of every PUT / UploadPart (`rustfs_rio::EtagReader`),
/// for `Content-MD5` verification, and for the MD5 additional checksum.
/// The digest is plain MD5 (RFC 1321); this type never changes what bytes
/// mean, only which implementation computes them.
pub struct Md5Stream(backend::Md5Inner);

impl Md5Stream {
    /// Digest length in bytes.
    pub const OUTPUT_SIZE: usize = 16;

    /// Create an empty hasher.
    #[inline]
    pub fn new() -> Self {
        Self(backend::Md5Inner::new())
    }

    /// Append bytes. Any slice length is accepted; splitting the input across
    /// calls never changes the digest.
    #[inline]
    pub fn update(&mut self, data: &[u8]) {
        self.0.update(data);
    }

    /// Consume the hasher and return the digest.
    #[inline]
    pub fn finalize(self) -> [u8; Self::OUTPUT_SIZE] {
        self.0.finalize()
    }

    /// Return the digest of everything fed so far and reset this hasher to the
    /// empty state, as if freshly constructed.
    #[inline]
    pub fn finalize_reset(&mut self) -> [u8; Self::OUTPUT_SIZE] {
        std::mem::replace(&mut self.0, backend::Md5Inner::new()).finalize()
    }

    /// One-shot digest of `data`.
    #[inline]
    pub fn digest(data: &[u8]) -> [u8; Self::OUTPUT_SIZE] {
        let mut hasher = Self::new();
        hasher.update(data);
        hasher.finalize()
    }
}

impl Default for Md5Stream {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Md5Stream {
    /// Deliberately opaque: never print internal state or partial digests.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Md5Stream")
    }
}

impl io::Write for Md5Stream {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.update(buf);
        Ok(buf.len())
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Concrete MD5 backends. Exactly one `Md5Inner` is compiled in.
///
/// Every backend exposes the same three inherent methods
/// (`new` / `update` / `finalize(self)`); [`Md5Stream`] is written against
/// that shape only, so a backend does not need `Clone`, `Default`, or any
/// trait from the backing crate.
mod backend {
    /// RustCrypto `md-5`: the default backend.
    pub(super) struct Md5Inner(md5::Md5);

    impl Md5Inner {
        #[inline]
        pub(super) fn new() -> Self {
            use md5::Digest as _;
            Self(md5::Md5::new())
        }

        #[inline]
        pub(super) fn update(&mut self, data: &[u8]) {
            use md5::Digest as _;
            self.0.update(data);
        }

        #[inline]
        pub(super) fn finalize(self) -> [u8; 16] {
            use md5::Digest as _;
            self.0.finalize().into()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;

    fn hex(digest: &[u8]) -> String {
        hex_simd::encode_to_string(digest, hex_simd::AsciiCase::Lower)
    }

    /// RFC 1321 appendix A.5 test suite.
    const RFC1321_VECTORS: [(&[u8], &str); 7] = [
        (b"", "d41d8cd98f00b204e9800998ecf8427e"),
        (b"a", "0cc175b9c0f1b6a831c399e269772661"),
        (b"abc", "900150983cd24fb0d6963f7d28e17f72"),
        (b"message digest", "f96b697d7cb7938d525a2f31aaf161d0"),
        (b"abcdefghijklmnopqrstuvwxyz", "c3fcd3d76192e4007dfb496cca67e13b"),
        (
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789",
            "d174ab98d277d9f5a5611c2c9f419d9f",
        ),
        (
            b"12345678901234567890123456789012345678901234567890123456789012345678901234567890",
            "57edf4a22be3c955ac49da2e2107b67a",
        ),
    ];

    /// Deterministic pseudo-random payload; no `rand` dependency needed.
    fn payload(len: usize, seed: u64) -> Vec<u8> {
        let mut x = seed | 1;
        (0..len)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                (x >> 24) as u8
            })
            .collect()
    }

    #[test]
    fn rfc1321_known_answers_one_shot() {
        for (input, want) in RFC1321_VECTORS {
            assert_eq!(hex(&Md5Stream::digest(input)), want, "input {:?}", String::from_utf8_lossy(input));
        }
    }

    #[test]
    fn rfc1321_known_answers_streaming_byte_at_a_time() {
        for (input, want) in RFC1321_VECTORS {
            let mut hasher = Md5Stream::new();
            for byte in input {
                hasher.update(std::slice::from_ref(byte));
            }
            assert_eq!(hex(&hasher.finalize()), want);
        }
    }

    #[test]
    fn chunked_updates_match_one_shot_across_block_boundaries() {
        // MD5 works on 64-byte blocks with a 56-byte padding threshold; cover
        // every split near those edges plus splits inside a multi-megabyte body.
        let small = payload(300, 7);
        let want = Md5Stream::digest(&small);
        for split in [0usize, 1, 2, 55, 56, 57, 63, 64, 65, 119, 120, 127, 128, 129, 200, 299, 300] {
            let mut hasher = Md5Stream::new();
            hasher.update(&small[..split]);
            hasher.update(&small[split..]);
            assert_eq!(hasher.finalize(), want, "split at {split}");
        }

        let large = payload((1 << 20) + 37, 11);
        let want = Md5Stream::digest(&large);
        let mut hasher = Md5Stream::new();
        let mut offset = 0usize;
        let mut chunk = 1usize;
        while offset < large.len() {
            let end = (offset + chunk).min(large.len());
            hasher.update(&large[offset..end]);
            offset = end;
            chunk = (chunk * 3 + 1) % 200_003 + 1;
        }
        assert_eq!(hasher.finalize(), want);
    }

    #[test]
    fn finalize_reset_yields_digest_and_restarts_from_empty() {
        let mut hasher = Md5Stream::new();
        hasher.update(b"message digest");
        assert_eq!(hex(&hasher.finalize_reset()), "f96b697d7cb7938d525a2f31aaf161d0");
        // After the reset the hasher must behave like a fresh one...
        assert_eq!(hex(&hasher.finalize_reset()), "d41d8cd98f00b204e9800998ecf8427e");
        // ...including for a new message that spans a block boundary.
        let body = payload(1000, 3);
        hasher.update(&body[..500]);
        hasher.update(&body[500..]);
        assert_eq!(hasher.finalize(), Md5Stream::digest(&body));
    }

    #[test]
    fn write_impl_feeds_the_hasher() {
        let body = payload(70_000, 5);
        let mut hasher = Md5Stream::default();
        hasher.write_all(&body[..1234]).expect("in-memory write cannot fail");
        hasher.write_all(&body[1234..]).expect("in-memory write cannot fail");
        hasher.flush().expect("flush is a no-op");
        assert_eq!(hasher.finalize(), Md5Stream::digest(&body));
    }

    #[test]
    fn debug_output_is_opaque() {
        let mut hasher = Md5Stream::new();
        hasher.update(b"secret payload");
        assert_eq!(format!("{hasher:?}"), "Md5Stream");
    }

    #[test]
    fn output_size_matches_md5() {
        assert_eq!(Md5Stream::OUTPUT_SIZE, 16);
        assert_eq!(Md5Stream::digest(b"").len(), Md5Stream::OUTPUT_SIZE);
    }

    #[test]
    fn finalize_reset_sequence_matches_independent_digests() {
        // One hasher reused for several messages must produce exactly the
        // digests of several fresh hashers; nothing may leak between messages.
        let messages = [payload(0, 1), payload(1, 2), payload(63, 3), payload(64, 4), payload(4097, 5)];
        let mut hasher = Md5Stream::new();
        for message in &messages {
            let (head, tail) = message.split_at(message.len() / 2);
            hasher.update(head);
            hasher.update(tail);
            assert_eq!(hasher.finalize_reset(), Md5Stream::digest(message), "len {}", message.len());
        }
    }

    /// Differential oracle against RustCrypto `md-5` called directly, i.e. not
    /// through the backend module. With the default backend this is the same
    /// implementation reached two ways; once another backend is compiled in
    /// it becomes the independent reference that gates the switch.
    mod differential {
        use super::super::Md5Stream;
        use proptest::prelude::*;

        fn reference(data: &[u8]) -> [u8; 16] {
            use md5::Digest as _;
            md5::Md5::digest(data).into()
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(256))]

            #[test]
            fn random_input_and_random_splits_match_reference(
                data in proptest::collection::vec(any::<u8>(), 0..20_000),
                splits in proptest::collection::vec(0usize..20_000, 0..8),
            ) {
                let want = reference(&data);
                prop_assert_eq!(Md5Stream::digest(&data), want);

                let mut cuts: Vec<usize> = splits.into_iter().map(|s| s % (data.len() + 1)).collect();
                cuts.sort_unstable();
                let mut hasher = Md5Stream::new();
                let mut start = 0usize;
                for cut in cuts {
                    hasher.update(&data[start..cut]);
                    start = cut;
                }
                hasher.update(&data[start..]);
                prop_assert_eq!(hasher.finalize(), want);
            }

            #[test]
            fn finalize_reset_matches_reference_for_each_message(
                first in proptest::collection::vec(any::<u8>(), 0..3_000),
                second in proptest::collection::vec(any::<u8>(), 0..3_000),
            ) {
                let mut hasher = Md5Stream::new();
                hasher.update(&first);
                prop_assert_eq!(hasher.finalize_reset(), reference(&first));
                hasher.update(&second);
                prop_assert_eq!(hasher.finalize(), reference(&second));
            }
        }
    }
}
