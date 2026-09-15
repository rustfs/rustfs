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

//! Old-bytes coverage for the ecstore/lifecycle readers of the persisted
//! `meta_user` keys (backlog#1735 A3b).
//!
//! The fixture is xl.meta written by the code that predates
//! `rustfs_filemeta::metadata_keys`, with each key taken from its historical
//! source. Every ecstore reader migrated to the authority must still find its
//! key in those bytes, and a single-character drift of any key (ASCII case
//! included, because these lookups are exact) must make the reader miss.

use super::*;
use crate::bucket::object_lock::objectlock::{get_object_legalhold_meta, get_object_retention_meta};
use crate::bucket::object_lock::objectlock_sys::{ObjectLockBlockReason, check_object_lock_for_deletion_with_default_retention};
use crate::bucket::object_lock::types::{LegalHoldStatus, RetentionMode};
use crate::bucket::replication::ReplicationStatusType;
use crate::store::utils::remove_standard_storage_class;
use rustfs_filemeta::FileMeta;
use rustfs_filemeta::metadata_keys;
use rustfs_filemeta::test_data::create_pre_metadata_keys_xlmeta;

const FIXTURE_VERSION_ID: &str = "0b1e5a3a-1735-4a3a-8000-00000000a3a0";

fn fixture_file_info() -> FileInfo {
    FileMeta::load(&create_pre_metadata_keys_xlmeta().expect("decode fixture hex"))
        .expect("load fixture xl.meta")
        .into_fileinfo("bucket", "object", FIXTURE_VERSION_ID, false, false, false)
        .expect("fixture version to FileInfo")
}

fn object_info(fi: &FileInfo) -> ObjectInfo {
    ObjectInfo::from_file_info(fi, "bucket", "object", true)
}

/// Replaces the byte at `idx` with a different one of the same class:
/// ASCII letters flip case, anything else becomes `_`.
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

/// The fixture version with `key` stored under `mutated` instead.
fn file_info_with_key_renamed(key: &str, mutated: &str) -> FileInfo {
    let mut fi = fixture_file_info();
    let value = fi.metadata.remove(key).expect("fixture carries the key");
    fi.metadata.insert(mutated.to_string(), value);
    fi
}

fn locked_by_object_lock(oi: &ObjectInfo) -> bool {
    let retention = get_object_retention_meta(&oi.user_defined);
    let hold = get_object_legalhold_meta(&oi.user_defined);
    retention.mode.is_some()
        || hold.status.is_some()
        || rustfs_lifecycle::object_lock::is_object_locked_by_metadata(&oi.user_defined, false)
        || check_object_lock_for_deletion_with_default_retention(None, oi, false)
            .expect("object-lock check")
            .is_some()
}

#[test]
fn pre_module_xlmeta_object_lock_is_enforced_by_ecstore_and_lifecycle() {
    let oi = object_info(&fixture_file_info());

    let retention = get_object_retention_meta(&oi.user_defined);
    assert!(matches!(retention.mode, Some(RetentionMode::Compliance)));
    assert_eq!(retention.retain_until_date.map(|date| date.year()), Some(2099));
    assert!(matches!(get_object_legalhold_meta(&oi.user_defined).status, Some(LegalHoldStatus::On)));
    assert!(rustfs_lifecycle::object_lock::is_object_locked_by_metadata(&oi.user_defined, false));
    assert!(matches!(
        check_object_lock_for_deletion_with_default_retention(None, &oi, true),
        Ok(Some(ObjectLockBlockReason::LegalHold))
    ));

    let mut without_hold = fixture_file_info();
    without_hold.metadata.remove(metadata_keys::OBJECT_LOCK_LEGAL_HOLD);
    assert!(matches!(
        check_object_lock_for_deletion_with_default_retention(None, &object_info(&without_hold), true),
        Ok(Some(ObjectLockBlockReason::Retention {
            mode: RetentionMode::Compliance,
            ..
        }))
    ));
}

#[test]
fn pre_module_xlmeta_restore_replication_and_storage_class_read_back() {
    let fi = fixture_file_info();
    let oi = object_info(&fi);
    assert!(!oi.restore_ongoing);
    assert_eq!(oi.restore_expires.map(|date| date.year()), Some(9999));

    let mut replica = fixture_file_info();
    replica.metadata.insert(
        metadata_keys::REPLICATION_STATUS.to_string(),
        ReplicationStatusType::Replica.as_str().to_string(),
    );
    assert_eq!(object_info(&replica).replication_status, ReplicationStatusType::Replica);

    let mut metadata = fi.metadata;
    assert_eq!(metadata.get(metadata_keys::STORAGE_CLASS).map(String::as_str), Some("GLACIER"));
    remove_standard_storage_class(&mut metadata);
    assert!(metadata.contains_key(metadata_keys::STORAGE_CLASS), "non-STANDARD class must stay");
    metadata.insert(
        metadata_keys::STORAGE_CLASS.to_string(),
        crate::config::storageclass::STANDARD.to_string(),
    );
    remove_standard_storage_class(&mut metadata);
    assert!(!metadata.contains_key(metadata_keys::STORAGE_CLASS), "STANDARD must be dropped");
}

/// Per-character mutation over the persisted keys the migrated ecstore and
/// lifecycle readers look up exactly: each drifted spelling must be missed.
#[test]
fn single_character_key_mutation_is_missed_by_ecstore_readers() {
    // Keep a single object-lock category per object so each mutation is
    // judged on its own: the legal hold alone, then the retention pair alone.
    for key in [
        metadata_keys::OBJECT_LOCK_LEGAL_HOLD,
        metadata_keys::OBJECT_LOCK_MODE,
        metadata_keys::OBJECT_LOCK_RETAIN_UNTIL_DATE,
    ] {
        for idx in 0..key.len() {
            let mutated = mutate_at(key, idx);
            let mut fi = file_info_with_key_renamed(key, &mutated);
            if key == metadata_keys::OBJECT_LOCK_LEGAL_HOLD {
                fi.metadata.remove(metadata_keys::OBJECT_LOCK_MODE);
                fi.metadata.remove(metadata_keys::OBJECT_LOCK_RETAIN_UNTIL_DATE);
                assert!(!locked_by_object_lock(&object_info(&fi)), "legal hold under {mutated:?} must not lock");
            } else {
                fi.metadata.remove(metadata_keys::OBJECT_LOCK_LEGAL_HOLD);
                let oi = object_info(&fi);
                let retention = get_object_retention_meta(&oi.user_defined);
                if key == metadata_keys::OBJECT_LOCK_MODE {
                    assert!(retention.mode.is_none(), "mode under {mutated:?} must not be read");
                } else {
                    assert!(retention.retain_until_date.is_none(), "retain-until under {mutated:?} must not be read");
                }
                assert!(
                    !rustfs_lifecycle::object_lock::is_object_locked_by_metadata(&oi.user_defined, false),
                    "retention with {mutated:?} must not read as locked"
                );
            }
        }
    }

    for idx in 0..metadata_keys::RESTORE.len() {
        let mutated = mutate_at(metadata_keys::RESTORE, idx);
        let oi = object_info(&file_info_with_key_renamed(metadata_keys::RESTORE, &mutated));
        assert!(oi.restore_expires.is_none(), "restore status under {mutated:?} must not be read");
    }

    for idx in 0..metadata_keys::REPLICATION_STATUS.len() {
        let mutated = mutate_at(metadata_keys::REPLICATION_STATUS, idx);
        let mut fi = fixture_file_info();
        fi.metadata.remove(metadata_keys::REPLICATION_STATUS);
        fi.metadata
            .insert(mutated.clone(), ReplicationStatusType::Replica.as_str().to_string());
        assert_ne!(
            object_info(&fi).replication_status,
            ReplicationStatusType::Replica,
            "replica status under {mutated:?} must not be read"
        );
    }

    for idx in 0..metadata_keys::STORAGE_CLASS.len() {
        let mutated = mutate_at(metadata_keys::STORAGE_CLASS, idx);
        let mut metadata = HashMap::from([(mutated.clone(), crate::config::storageclass::STANDARD.to_string())]);
        remove_standard_storage_class(&mut metadata);
        assert!(metadata.contains_key(&mutated), "storage class under {mutated:?} must not be matched");
    }
}
