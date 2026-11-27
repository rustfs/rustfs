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

//! Tests of what only an `m-compatible` build compiles.

use super::*;

/// MinIO records no size for a single-part SSE object: the stored bytes are
/// DARE packages, each up to 64 KiB of payload behind 32 bytes of framing.
/// These are the stored sizes MinIO produced for 1 B, 4 KiB, 300 KiB,
/// 1 MiB + 1 and 9 MiB objects.
#[test]
fn get_actual_size_derives_the_plaintext_size_of_a_minio_sealed_object() {
    for (stored, plaintext) in [
        (0, 0),
        (33, 1),
        (4128, 4096),
        (65_568, 65_536),
        (307_360, 307_200),
        (1_049_121, 1_048_577),
        (9_441_792, 9_437_184),
    ] {
        let info = ObjectInfo {
            size: stored,
            user_defined: minio_sealed_object_metadata(false),
            ..Default::default()
        };
        assert_eq!(info.get_actual_size().unwrap(), plaintext, "stored {stored}");
        assert_eq!(info.decrypted_size().unwrap(), plaintext, "stored {stored}");
        assert_eq!(info.get_actual_size_or_physical(), plaintext, "stored {stored}");
    }

    let part = |number: usize, size: usize| ObjectPartInfo {
        number,
        size,
        ..Default::default()
    };

    // A read hands back the object with `size` rewritten to the length it
    // delivers — the whole plaintext, or one byte of a ranged read — and the
    // response length is then computed from that value. The parts still
    // hold what is stored.
    for delivered in [2_097_152, 1] {
        let resized_by_a_read = ObjectInfo {
            size: delivered,
            parts: Arc::new(vec![part(1, 2_098_176)]),
            user_defined: minio_sealed_object_metadata(false),
            ..Default::default()
        };
        assert_eq!(resized_by_a_read.get_actual_size().unwrap(), 2_097_152, "delivered {delivered}");
    }

    // A multipart upload is one DARE stream per part.
    let multipart = ObjectInfo {
        size: 5_245_440 + 5_245_440 + 1_049_137,
        parts: Arc::new(vec![part(1, 5_245_440), part(2, 5_245_440), part(3, 1_049_137)]),
        user_defined: minio_sealed_object_metadata(true),
        ..Default::default()
    };
    assert_eq!(multipart.get_actual_size().unwrap(), 5_242_880 + 5_242_880 + 1_048_593);

    // A stored size that is not a whole number of packages is damage, not a
    // size to report: 20 bytes cannot even hold one package's framing.
    let damaged = ObjectInfo {
        size: 20,
        user_defined: minio_sealed_object_metadata(false),
        ..Default::default()
    };
    assert!(damaged.get_actual_size().is_err());
    assert_eq!(damaged.get_actual_size_or_physical(), 20, "listings fall back to the stored size");
}

/// A size the object recorded always wins over one derived from its layout.
#[test]
fn get_actual_size_prefers_a_recorded_size_over_the_dare_layout() {
    let mut metadata = (*minio_sealed_object_metadata(false)).clone();
    metadata.insert("x-rustfs-encryption-original-size".to_string(), "77".to_string());
    let info = ObjectInfo {
        size: 4128,
        user_defined: Arc::new(metadata),
        ..Default::default()
    };

    assert_eq!(info.get_actual_size().unwrap(), 77);
}
