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

//! Object-metadata map keys persisted inside xl.meta (`MetaObject::meta_user`).
//!
//! These strings are part of the on-disk format, not HTTP header names. They
//! happen to spell S3 header names, and some were historically taken from
//! `s3s::header` or duplicated in `rustfs_utils::http::headers`, which let an
//! HTTP-library or header-table edit silently change what is written to and
//! looked up in xl.meta. Map lookups are exact, so every byte (including ASCII
//! case) is load-bearing and existing xl.meta cannot be rewritten in bulk:
//!
//! - the object-lock keys unreadable => retention reads back as "no lock" and
//!   WORM objects become deletable;
//! - [`RESTORE`] unreadable => `MetaObject::uses_data_dir` reports a restored
//!   object's data dir as unused, making live data reclaimable.
//!
//! The literals are pinned by tests below against both a literal table and
//! xl.meta bytes captured from the code that predates this module.

/// Object-lock legal hold (`ON`/`OFF`).
pub const OBJECT_LOCK_LEGAL_HOLD: &str = "x-amz-object-lock-legal-hold";
/// Object-lock retention mode (`GOVERNANCE`/`COMPLIANCE`).
pub const OBJECT_LOCK_MODE: &str = "x-amz-object-lock-mode";
/// Object-lock retain-until date.
pub const OBJECT_LOCK_RETAIN_UNTIL_DATE: &str = "x-amz-object-lock-retain-until-date";
/// Restore status of a transitioned object restored to local storage.
pub const RESTORE: &str = "x-amz-restore";
/// Requested restore duration. Persisted in this mixed case.
pub const RESTORE_EXPIRY_DAYS: &str = "X-Amz-Restore-Expiry-Days";
/// Restore request date. Persisted in this mixed case.
pub const RESTORE_REQUEST_DATE: &str = "X-Amz-Restore-Request-Date";
/// Server-side encryption algorithm.
pub const SERVER_SIDE_ENCRYPTION: &str = "x-amz-server-side-encryption";
/// Storage class.
pub const STORAGE_CLASS: &str = "x-amz-storage-class";
/// Composite replication status. Persisted in this mixed case.
pub const REPLICATION_STATUS: &str = "X-Amz-Replication-Status";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_data::create_pre_metadata_keys_xlmeta;
    use crate::{FileMeta, FileMetaVersion, MetaObject, is_restored_object_on_disk};
    use std::collections::HashMap;

    /// Every persisted key with its exact on-disk spelling, written out as an
    /// independent literal so a change to a constant cannot also change the
    /// expectation.
    const PINNED: [(&str, &str); 9] = [
        (OBJECT_LOCK_LEGAL_HOLD, "x-amz-object-lock-legal-hold"),
        (OBJECT_LOCK_MODE, "x-amz-object-lock-mode"),
        (OBJECT_LOCK_RETAIN_UNTIL_DATE, "x-amz-object-lock-retain-until-date"),
        (RESTORE, "x-amz-restore"),
        (RESTORE_EXPIRY_DAYS, "X-Amz-Restore-Expiry-Days"),
        (RESTORE_REQUEST_DATE, "X-Amz-Restore-Request-Date"),
        (SERVER_SIDE_ENCRYPTION, "x-amz-server-side-encryption"),
        (STORAGE_CLASS, "x-amz-storage-class"),
        (REPLICATION_STATUS, "X-Amz-Replication-Status"),
    ];

    /// Values stored under each key in the pre-module fixture.
    const FIXTURE_VALUES: [(&str, &str); 9] = [
        ("x-amz-object-lock-legal-hold", "ON"),
        ("x-amz-object-lock-mode", "COMPLIANCE"),
        ("x-amz-object-lock-retain-until-date", "2099-01-01T00:00:00Z"),
        ("x-amz-restore", "ongoing-request=\"false\", expiry-date=\"9999-01-01T00:00:00Z\""),
        ("X-Amz-Restore-Expiry-Days", "7"),
        ("X-Amz-Restore-Request-Date", "Thu, 16 Jul 2026 00:00:00 GMT"),
        ("x-amz-server-side-encryption", "AES256"),
        ("x-amz-storage-class", "GLACIER"),
        ("X-Amz-Replication-Status", "COMPLETED"),
    ];

    const FIXTURE_VERSION_ID: &str = "0b1e5a3a-1735-4a3a-8000-00000000a3a0";

    fn fixture_object() -> MetaObject {
        let fm = FileMeta::load(&create_pre_metadata_keys_xlmeta().expect("decode fixture hex")).expect("load fixture xl.meta");
        assert_eq!(fm.versions.len(), 1);
        FileMetaVersion::try_from(fm.versions[0].meta.as_slice())
            .expect("decode fixture version")
            .object
            .expect("fixture version is an object")
    }

    fn fixture_metadata() -> HashMap<String, String> {
        let fm = FileMeta::load(&create_pre_metadata_keys_xlmeta().expect("decode fixture hex")).expect("load fixture xl.meta");
        fm.into_fileinfo("bucket", "object", FIXTURE_VERSION_ID, false, false, false)
            .expect("fixture version to FileInfo")
            .metadata
    }

    /// Replaces the byte at `idx` with a different one of the same class:
    /// ASCII letters flip case (lookups are case-sensitive), anything else
    /// becomes `_`.
    fn mutate_at(key: &str, idx: usize) -> String {
        let mut bytes = key.as_bytes().to_vec();
        let b = bytes[idx];
        bytes[idx] = if b.is_ascii_lowercase() {
            b.to_ascii_uppercase()
        } else if b.is_ascii_uppercase() {
            b.to_ascii_lowercase()
        } else {
            b'_'
        };
        String::from_utf8(bytes).expect("ascii mutation stays utf-8")
    }

    #[test]
    fn persisted_metadata_keys_are_pinned_byte_for_byte() {
        for (key, literal) in PINNED {
            assert_eq!(key.as_bytes(), literal.as_bytes(), "persisted key {literal:?} drifted");
        }
        let fixture_keys: Vec<&str> = FIXTURE_VALUES.iter().map(|(k, _)| *k).collect();
        let pinned_keys: Vec<&str> = PINNED.iter().map(|(_, l)| *l).collect();
        assert_eq!(fixture_keys, pinned_keys, "fixture table must cover every pinned key");
    }

    /// Migration-period cross-check: the constants must equal the historical
    /// `rustfs_utils` sources callers used before this module existed. The
    /// `s3s::header` half was dropped with filemeta's `s3s` dependency (A3c);
    /// `PINNED` and the pre-module fixture keep pinning those bytes.
    #[test]
    fn persisted_metadata_keys_match_their_historical_sources() {
        use rustfs_utils::http::AMZ_BUCKET_REPLICATION_STATUS;
        use rustfs_utils::http::headers::{
            AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER, AMZ_OBJECT_LOCK_MODE_LOWER, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER, AMZ_RESTORE,
            AMZ_RESTORE_EXPIRY_DAYS, AMZ_RESTORE_REQUEST_DATE, AMZ_STORAGE_CLASS,
        };
        assert_eq!(OBJECT_LOCK_LEGAL_HOLD, AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER);
        assert_eq!(OBJECT_LOCK_MODE, AMZ_OBJECT_LOCK_MODE_LOWER);
        assert_eq!(OBJECT_LOCK_RETAIN_UNTIL_DATE, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER);
        assert_eq!(RESTORE, AMZ_RESTORE);
        assert_eq!(RESTORE_EXPIRY_DAYS, AMZ_RESTORE_EXPIRY_DAYS);
        assert_eq!(RESTORE_REQUEST_DATE, AMZ_RESTORE_REQUEST_DATE);
        assert_eq!(STORAGE_CLASS, AMZ_STORAGE_CLASS);
        assert_eq!(REPLICATION_STATUS, AMZ_BUCKET_REPLICATION_STATUS);
    }

    /// The fixture bytes were written by the code that predates this module,
    /// with each key taken from its historical source. Every key must appear
    /// verbatim in the raw bytes and read back through the new constants.
    #[test]
    fn pre_module_xlmeta_reads_back_every_persisted_key() {
        let raw = create_pre_metadata_keys_xlmeta().expect("decode fixture hex");
        for (key, _) in PINNED {
            assert!(
                raw.windows(key.len()).any(|w| w == key.as_bytes()),
                "fixture bytes must contain persisted key {key:?} verbatim"
            );
        }

        let obj = fixture_object();
        let metadata = fixture_metadata();
        for ((key, _), (fixture_key, value)) in PINNED.iter().zip(FIXTURE_VALUES) {
            assert_eq!(*key, fixture_key);
            assert_eq!(obj.meta_user.get(*key).map(String::as_str), Some(value), "meta_user[{key:?}]");
            assert_eq!(metadata.get(*key).map(String::as_str), Some(value), "FileInfo.metadata[{key:?}]");
        }

        assert!(is_restored_object_on_disk(&metadata), "restore marker must read back as on disk");
        assert!(obj.uses_data_dir(), "restored object's data dir must stay in use");
    }

    /// Removing restore headers from the pre-module object must drop exactly
    /// the restore keys and flip `uses_data_dir`, proving the remover and the
    /// reader address the same persisted bytes.
    #[test]
    fn remove_restore_hdrs_strips_pre_module_restore_keys() {
        let mut obj = fixture_object();
        obj.remove_restore_hdrs();
        for key in [RESTORE, RESTORE_EXPIRY_DAYS, RESTORE_REQUEST_DATE] {
            assert!(!obj.meta_user.contains_key(key), "{key:?} must be removed");
        }
        for key in [
            OBJECT_LOCK_LEGAL_HOLD,
            OBJECT_LOCK_MODE,
            OBJECT_LOCK_RETAIN_UNTIL_DATE,
            SERVER_SIDE_ENCRYPTION,
            STORAGE_CLASS,
            REPLICATION_STATUS,
        ] {
            assert!(obj.meta_user.contains_key(key), "{key:?} must survive restore cleanup");
        }
        assert!(!is_restored_object_on_disk(&obj.meta_user));
        assert!(!obj.uses_data_dir());
    }

    /// Writing through the new constants must produce the same persisted key
    /// set as the pre-module bytes, so a rollback reads new objects too.
    #[test]
    fn new_writes_persist_the_same_keys_as_pre_module_bytes() {
        let mut fi = FileMeta::load(&create_pre_metadata_keys_xlmeta().expect("decode fixture hex"))
            .expect("load fixture xl.meta")
            .into_fileinfo("bucket", "object", FIXTURE_VERSION_ID, false, false, false)
            .expect("fixture version to FileInfo");
        fi.metadata = PINNED
            .iter()
            .zip(FIXTURE_VALUES)
            .map(|((key, _), (_, value))| (key.to_string(), value.to_string()))
            .collect();
        fi.metadata
            .insert("etag".to_string(), "d41d8cd98f00b204e9800998ecf8427e".to_string());

        let mut fm = FileMeta::new();
        fm.add_version(fi).expect("add version");
        let rewritten = FileMeta::load(&fm.marshal_msg().expect("marshal")).expect("reload");
        let rewritten = FileMetaVersion::try_from(rewritten.versions[0].meta.as_slice())
            .expect("decode rewritten version")
            .object
            .expect("rewritten version is an object");

        assert_eq!(rewritten.meta_user, fixture_object().meta_user);
    }

    /// Per-character mutation: a constant that differs from the persisted key
    /// by any single byte (including ASCII case) finds nothing in pre-module
    /// xl.meta, and a restore marker stored under such a key is not honored.
    /// This is the drift the pins above turn into a test failure.
    #[test]
    fn single_character_key_mutation_misses_pre_module_data() {
        let metadata = fixture_metadata();
        let mut mutations = 0usize;
        for (key, _) in PINNED {
            for idx in 0..key.len() {
                let mutated = mutate_at(key, idx);
                assert_ne!(mutated, key);
                assert!(
                    !metadata.contains_key(&mutated),
                    "mutation {mutated:?} of {key:?} must not match pre-module data"
                );
                mutations += 1;
            }
        }
        assert_eq!(mutations, PINNED.iter().map(|(k, _)| k.len()).sum::<usize>());

        let marker = metadata.get(RESTORE).expect("fixture restore marker").clone();
        for idx in 0..RESTORE.len() {
            let meta = HashMap::from([(mutate_at(RESTORE, idx), marker.clone())]);
            assert!(
                !is_restored_object_on_disk(&meta),
                "restore marker under {:?} must not count as on disk",
                mutate_at(RESTORE, idx)
            );
        }
    }
}
