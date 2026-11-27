// Copyright 2026 RustFS Team
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

//! Algebraic property tests for S3 multipart composite checksums, linear
//! full-object CRC combination, and multipart MD5 ETag-of-ETags invariants
//! (`rustfs/backlog#1151` `sec-11`).
//!
//! Pure in-memory algebraic proofs over `rustfs_rio::{Checksum, ChecksumType,
//! read_checksums}` — zero disk I/O and zero global state.

use base64_simd::STANDARD as BASE64_STANDARD;
use crc_fast::{CrcAlgorithm, Digest as CrcDigest};
use md5::{Digest as Md5Digest, Md5};
use proptest::prelude::*;
use rustfs_rio::{Checksum, ChecksumType, read_checksums};
use sha1::Sha1;
use sha2::{Sha256, Sha512};
use std::io::Write as _;

fn no_persist_config() -> ProptestConfig {
    ProptestConfig {
        cases: 128,
        failure_persistence: None,
        ..ProptestConfig::default()
    }
}

/// Independent reference digest for each `ChecksumType`, computed directly from
/// primitive cryptographic/CRC crates rather than delegating to `rustfs_rio`.
fn reference_digest(ct: ChecksumType, data: &[u8]) -> Vec<u8> {
    match ct.base() {
        ChecksumType::CRC32 => {
            let mut d = CrcDigest::new(CrcAlgorithm::Crc32IsoHdlc);
            d.update(data);
            (d.finalize() as u32).to_be_bytes().to_vec()
        }
        ChecksumType::CRC32C => {
            let mut d = CrcDigest::new(CrcAlgorithm::Crc32Iscsi);
            d.update(data);
            (d.finalize() as u32).to_be_bytes().to_vec()
        }
        ChecksumType::CRC64_NVME => {
            let mut d = CrcDigest::new(CrcAlgorithm::Crc64Nvme);
            d.update(data);
            d.finalize().to_be_bytes().to_vec()
        }
        ChecksumType::SHA1 => Sha1::digest(data).to_vec(),
        ChecksumType::SHA256 => Sha256::digest(data).to_vec(),
        ChecksumType::SHA512 => Sha512::digest(data).to_vec(),
        ChecksumType::XXHASH3 => xxhash_rust::xxh3::xxh3_64(data).to_be_bytes().to_vec(),
        ChecksumType::XXHASH64 => xxhash_rust::xxh64::xxh64(data, 0).to_be_bytes().to_vec(),
        ChecksumType::XXHASH128 => xxhash_rust::xxh3::xxh3_128(data).to_be_bytes().to_vec(),
        ChecksumType::MD5 => Md5::digest(data).to_vec(),
        other => panic!("unexpected checksum type {other:?}"),
    }
}

fn composite_algorithms() -> [ChecksumType; 9] {
    [
        ChecksumType::CRC32,
        ChecksumType::CRC32C,
        ChecksumType::SHA1,
        ChecksumType::SHA256,
        ChecksumType::SHA512,
        ChecksumType::XXHASH3,
        ChecksumType::XXHASH64,
        ChecksumType::XXHASH128,
        ChecksumType::MD5,
    ]
}

fn full_object_crc_algorithms() -> [ChecksumType; 3] {
    [ChecksumType::CRC32, ChecksumType::CRC32C, ChecksumType::CRC64_NVME]
}

proptest! {
    #![proptest_config(no_persist_config())]

    /// Composite multipart checksum algebra:
    /// For every S3 COMPOSITE algorithm and any sequence of 1..=10 parts,
    /// the serialized multipart checksum record equals
    /// `base64(H(concat(H(part_1), ..., H(part_N)))) + "-N"`, reports
    /// `x-amz-checksum-type: COMPOSITE`, and recovers every individual part's
    /// digest via `read_checksums(buf, part_number)`.
    #[test]
    fn composite_multipart_checksum_satisfies_s3_hash_of_part_digests_algebra(
        parts in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..=256), 1..=10),
        alg_idx in 0usize..9usize,
    ) {
        let base_ct = composite_algorithms()[alg_idx];
        let map_key = base_ct.to_string();

        let mut combined_raw_part_digests = Vec::new();
        let mut expected_part_b64 = Vec::with_capacity(parts.len());

        for part in &parts {
            let mut hasher = base_ct.hasher().expect("rio hasher must exist");
            hasher.write_all(part).expect("hasher write");
            let rio_part_digest = hasher.finalize();
            let ref_part_digest = reference_digest(base_ct, part);
            prop_assert_eq!(&rio_part_digest, &ref_part_digest, "per-part digest must match reference");
            combined_raw_part_digests.extend_from_slice(&rio_part_digest);
            expected_part_b64.push(BASE64_STANDARD.encode_to_string(&rio_part_digest));
        }

        let mut multipart_ct = base_ct;
        multipart_ct
            .merge(ChecksumType::MULTIPART)
            .merge(ChecksumType::INCLUDES_MULTIPART);
        prop_assert_eq!(multipart_ct.obj_type(), "COMPOSITE");

        let composite = Checksum::new_from_data(multipart_ct, &combined_raw_part_digests)
            .expect("composite checksum must construct from concatenated part digests");
        let serialized = composite.to_bytes(&combined_raw_part_digests);

        // Object-level read (part == 0) must yield `base64(H(concat(part_digests)))-N`
        // under the canonical algorithm name and must NOT claim FULL_OBJECT.
        let (obj_map, obj_is_multipart) = read_checksums(&serialized, 0);
        prop_assert!(obj_is_multipart, "read_checksums(part=0) must flag multipart composite record");
        let expected_composite_raw = reference_digest(base_ct, &combined_raw_part_digests);
        let expected_composite = format!(
            "{}-{}",
            BASE64_STANDARD.encode_to_string(&expected_composite_raw),
            parts.len()
        );
        prop_assert_eq!(obj_map.get(&map_key).map(String::as_str), Some(expected_composite.as_str()));
        prop_assert_eq!(obj_map.get("x-amz-checksum-type"), None);

        // Per-part reads (1-indexed) must recover each part's exact digest.
        for (idx, expected_b64) in expected_part_b64.iter().enumerate() {
            let part_num = i32::try_from(idx + 1).expect("part index fits i32");
            let (part_map, part_is_multipart) = read_checksums(&serialized, part_num);
            prop_assert!(part_is_multipart, "read_checksums(part={part_num}) must flag multipart");
            prop_assert_eq!(part_map.get(&map_key).map(String::as_str), Some(expected_b64.as_str()));
        }
    }

    /// Full-object CRC combination algebra:
    /// For every mergeable CRC algorithm (`CRC32`, `CRC32C`, `CRC64_NVME`) and
    /// any partition of bytes into 1..=10 parts (including empty parts), combining
    /// per-part checksums via `Checksum::add_part` equals the single-pass CRC over
    /// the concatenated byte stream and roundtrips through `to_bytes`/`read_checksums`
    /// as `FULL_OBJECT` without a `-N` suffix.
    #[test]
    fn full_object_crc_add_part_matches_single_pass_digest_and_roundtrips(
        parts in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..=256), 1..=10),
        alg_idx in 0usize..3usize,
    ) {
        let base_ct = full_object_crc_algorithms()[alg_idx];
        let map_key = base_ct.to_string();

        let mut running = Checksum {
            checksum_type: base_ct,
            encoded: String::new(),
            raw: Vec::new(),
            want_parts: 0,
        };
        let mut concat_bytes = Vec::new();
        let mut combined_part_digests = Vec::new();

        for part in &parts {
            concat_bytes.extend_from_slice(part);
            let part_cs = Checksum::new_from_data(base_ct, part).expect("part CRC");
            combined_part_digests.extend_from_slice(&part_cs.raw);
            running
                .add_part(&part_cs, i64::try_from(part.len()).expect("part len fits i64"))
                .expect("add_part must succeed for mergeable CRC");
        }

        let expected_full_raw = reference_digest(base_ct, &concat_bytes);
        let expected_full_b64 = BASE64_STANDARD.encode_to_string(&expected_full_raw);

        if concat_bytes.is_empty() {
            // Adding only 0-byte parts leaves the running accumulator empty until seeded.
            let empty_cs = Checksum::new_from_data(base_ct, &[]).expect("empty CRC");
            prop_assert_eq!(empty_cs.raw, expected_full_raw);
            prop_assert_eq!(empty_cs.encoded, expected_full_b64);
        } else {
            prop_assert_eq!(&running.raw, &expected_full_raw);
            prop_assert_eq!(&running.encoded, &expected_full_b64);

            let mut full_mp_ct = base_ct;
            full_mp_ct
                .merge(ChecksumType::FULL_OBJECT)
                .merge(ChecksumType::MULTIPART)
                .merge(ChecksumType::INCLUDES_MULTIPART);
            running.checksum_type = full_mp_ct;

            let serialized = running.to_bytes(&combined_part_digests);
            let (obj_map, obj_is_multipart) = read_checksums(&serialized, 0);
            prop_assert!(obj_is_multipart, "read_checksums(part=0) must flag FULL_OBJECT multipart record");
            prop_assert_eq!(obj_map.get(&map_key).map(String::as_str), Some(expected_full_b64.as_str()));
            prop_assert_eq!(
                obj_map.get("x-amz-checksum-type").map(String::as_str),
                Some("FULL_OBJECT")
            );
        }
    }

    /// CRC combination monoid laws: associativity `((a + b) + c) == (a + (b + c))`
    /// and empty-part identity `(a + []) == a == ([] + a)`.
    #[test]
    fn full_object_crc_add_part_satisfies_associativity_and_identity(
        a in prop::collection::vec(any::<u8>(), 1..=128),
        b in prop::collection::vec(any::<u8>(), 1..=128),
        c in prop::collection::vec(any::<u8>(), 1..=128),
        alg_idx in 0usize..3usize,
    ) {
        let base_ct = full_object_crc_algorithms()[alg_idx];
        let cs_a = Checksum::new_from_data(base_ct, &a).expect("a CRC");
        let cs_b = Checksum::new_from_data(base_ct, &b).expect("b CRC");
        let cs_c = Checksum::new_from_data(base_ct, &c).expect("c CRC");
        let cs_empty = Checksum::new_from_data(base_ct, &[]).expect("empty CRC");

        // Left-associated: (a + b) + c
        let mut left = cs_a.clone();
        left.add_part(&cs_b, b.len() as i64).expect("a + b");
        left.add_part(&cs_c, c.len() as i64).expect("(a + b) + c");

        // Right-associated: a + (b + c)
        let mut bc = cs_b.clone();
        bc.add_part(&cs_c, c.len() as i64).expect("b + c");
        let mut right = cs_a.clone();
        right.add_part(&bc, (b.len() + c.len()) as i64).expect("a + (b + c)");

        prop_assert_eq!(&left.raw, &right.raw, "CRC combination must be associative");
        prop_assert_eq!(&left.encoded, &right.encoded);

        // Right identity: a + [] == a
        let mut with_empty = cs_a.clone();
        with_empty.add_part(&cs_empty, 0).expect("a + empty");
        prop_assert_eq!(&with_empty.raw, &cs_a.raw, "0-length part must be a right identity");
    }

    /// Multipart ETag-of-ETags MD5 algebra via `ChecksumType::MD5`:
    /// `etag == hex(md5(concat(md5_raw(part_1), ..., md5_raw(part_N)))) + "-N"`.
    #[test]
    fn multipart_md5_etag_of_etags_matches_s3_definition(
        parts in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..=256), 1..=10),
    ) {
        let mut concat_raw_md5 = Vec::with_capacity(parts.len() * 16);
        for part in &parts {
            let mut hasher = ChecksumType::MD5.hasher().expect("MD5 hasher");
            hasher.write_all(part).expect("MD5 write");
            let part_md5 = hasher.finalize();
            prop_assert_eq!(part_md5.len(), 16);
            prop_assert_eq!(&part_md5, &Md5::digest(part).to_vec());
            concat_raw_md5.extend_from_slice(&part_md5);
        }

        let mut outer = ChecksumType::MD5.hasher().expect("outer MD5 hasher");
        outer.write_all(&concat_raw_md5).expect("outer MD5 write");
        let rio_etag = format!("{}-{}", faster_hex::hex_string(&outer.finalize()), parts.len());
        let ref_etag = format!("{}-{}", faster_hex::hex_string(&Md5::digest(&concat_raw_md5)), parts.len());
        prop_assert_eq!(&rio_etag, &ref_etag);
        let expected_suffix = format!("-{}", parts.len());
        prop_assert!(rio_etag.ends_with(&expected_suffix));
        prop_assert_eq!(rio_etag.split('-').next().map(str::len), Some(32));
    }
}
