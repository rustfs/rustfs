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

//! Migration of bucket metadata and IAM config from legacy format to RustFS format.

use crate::bucket::metadata::BUCKET_METADATA_FILE;
use crate::bucket::replication::{decode_resync_file, encode_resync_file};
use crate::disk::{BUCKET_META_PREFIX, MIGRATING_META_BUCKET, RUSTFS_META_BUCKET};
use crate::store_api::{BucketOptions, ObjectOptions, PutObjReader, StorageAPI};
use http::HeaderMap;
use rustfs_policy::auth::UserIdentity;
use rustfs_policy::policy::PolicyDoc;
use rustfs_utils::path::SLASH_SEPARATOR;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use time::OffsetDateTime;
use tracing::{debug, info, warn};

/// IAM config prefix under meta bucket (e.g. config/iam/).
const IAM_CONFIG_PREFIX: &str = "config/iam";
const IAM_FORMAT_FILE_PATH: &str = "config/iam/format.json";
const IAM_USERS_PREFIX: &str = "config/iam/users/";
const IAM_SERVICE_ACCOUNTS_PREFIX: &str = "config/iam/service-accounts/";
const IAM_STS_PREFIX: &str = "config/iam/sts/";
const IAM_GROUPS_PREFIX: &str = "config/iam/groups/";
const IAM_POLICIES_PREFIX: &str = "config/iam/policies/";
const IAM_POLICY_DB_PREFIX: &str = "config/iam/policydb/";
const REPLICATION_META_DIR: &str = ".replication";
const RESYNC_META_FILE: &str = "resync.bin";

#[derive(Debug, Serialize, Deserialize)]
struct CompatIamFormat {
    #[serde(default)]
    version: i64,
}

#[derive(Debug, Serialize, Deserialize)]
struct CompatGroupInfo {
    #[serde(default)]
    version: i64,
    #[serde(default = "default_group_status")]
    status: String,
    #[serde(default)]
    members: Vec<String>,
    #[serde(
        rename = "updatedAt",
        alias = "update_at",
        default,
        with = "rustfs_policy::serde_datetime::option"
    )]
    update_at: Option<OffsetDateTime>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CompatMappedPolicy {
    #[serde(default)]
    version: i64,
    #[serde(rename = "policy", alias = "policies", default)]
    policy: String,
    #[serde(
        rename = "updatedAt",
        alias = "update_at",
        default,
        with = "rustfs_policy::serde_datetime::option"
    )]
    update_at: Option<OffsetDateTime>,
}

fn default_group_status() -> String {
    "enabled".to_string()
}

fn normalize_iam_config_blob(path: &str, data: &[u8]) -> std::result::Result<Option<Vec<u8>>, String> {
    if path == IAM_FORMAT_FILE_PATH {
        let mut format: CompatIamFormat =
            serde_json::from_slice(data).map_err(|err| format!("parse IAM format failed: {err}"))?;
        if format.version <= 0 {
            format.version = 1;
        }
        return serde_json::to_vec(&format)
            .map(Some)
            .map_err(|err| format!("serialize IAM format failed: {err}"));
    }

    if is_identity_path(path) {
        let mut identity: UserIdentity =
            serde_json::from_slice(data).map_err(|err| format!("parse IAM identity failed: {err}"))?;
        if identity.update_at.is_none() {
            identity.update_at = Some(OffsetDateTime::now_utc());
        }
        return serde_json::to_vec(&identity)
            .map(Some)
            .map_err(|err| format!("serialize IAM identity failed: {err}"));
    }

    if is_group_path(path) {
        let mut group: CompatGroupInfo = serde_json::from_slice(data).map_err(|err| format!("parse IAM group failed: {err}"))?;
        if group.update_at.is_none() {
            group.update_at = Some(OffsetDateTime::now_utc());
        }
        return serde_json::to_vec(&group)
            .map(Some)
            .map_err(|err| format!("serialize IAM group failed: {err}"));
    }

    if is_policy_doc_path(path) {
        let mut doc = PolicyDoc::try_from(data.to_vec()).map_err(|err| format!("parse IAM policy doc failed: {err}"))?;
        if doc.create_date.is_none() {
            doc.create_date = doc.update_date;
        }
        if doc.update_date.is_none() {
            doc.update_date = doc.create_date;
        }
        return serde_json::to_vec(&doc)
            .map(Some)
            .map_err(|err| format!("serialize IAM policy doc failed: {err}"));
    }

    if is_policy_mapping_path(path) {
        let mut mapped: CompatMappedPolicy =
            serde_json::from_slice(data).map_err(|err| format!("parse IAM policy mapping failed: {err}"))?;
        if mapped.update_at.is_none() {
            mapped.update_at = Some(OffsetDateTime::now_utc());
        }
        return serde_json::to_vec(&mapped)
            .map(Some)
            .map_err(|err| format!("serialize IAM policy mapping failed: {err}"));
    }

    Ok(None)
}

fn is_identity_path(path: &str) -> bool {
    (path.starts_with(IAM_USERS_PREFIX) || path.starts_with(IAM_SERVICE_ACCOUNTS_PREFIX) || path.starts_with(IAM_STS_PREFIX))
        && path.ends_with("/identity.json")
}

fn is_group_path(path: &str) -> bool {
    path.starts_with(IAM_GROUPS_PREFIX) && path.ends_with("/members.json")
}

fn is_policy_doc_path(path: &str) -> bool {
    path.starts_with(IAM_POLICIES_PREFIX) && path.ends_with("/policy.json")
}

fn is_policy_mapping_path(path: &str) -> bool {
    path.starts_with(IAM_POLICY_DB_PREFIX) && path.ends_with(".json")
}

fn is_resync_meta_path(path: &str) -> bool {
    path.ends_with(&format!("{REPLICATION_META_DIR}/{RESYNC_META_FILE}"))
}

fn normalize_bucket_meta_blob(path: &str, data: &[u8]) -> std::result::Result<Option<Vec<u8>>, String> {
    if !is_resync_meta_path(path) {
        return Ok(None);
    }
    let status = decode_resync_file(data).map_err(|err| format!("decode resync meta failed: {err}"))?;
    encode_resync_file(&status)
        .map(Some)
        .map_err(|err| format!("encode resync meta failed: {err}"))
}

/// Migrates bucket metadata from legacy format to RustFS.
/// Uses list_bucket (from disk volumes) to get bucket names, since list_objects_v2 on the legacy
/// meta bucket may not work (legacy format differs from object layer expectations).
/// Skips buckets that already exist in RustFS (idempotent).
pub async fn try_migrate_bucket_metadata<S: StorageAPI>(store: Arc<S>) {
    let buckets_list = match store
        .list_bucket(&BucketOptions {
            no_metadata: true,
            ..Default::default()
        })
        .await
    {
        Ok(b) => b,
        Err(e) => {
            warn!("list buckets failed (skip migration): {e}");
            return;
        }
    };

    let buckets: Vec<String> = buckets_list.into_iter().map(|b| b.name).collect();

    if buckets.is_empty() {
        debug!("No migrating bucket metadata found");
        return;
    }

    debug!("Found {} migrating bucket metadata, migrating...", buckets.len());

    let opts = ObjectOptions {
        max_parity: true,
        no_lock: true,
        ..Default::default()
    };
    let h = HeaderMap::new();

    for bucket in buckets {
        let meta_path = format!("{BUCKET_META_PREFIX}{SLASH_SEPARATOR}{bucket}{SLASH_SEPARATOR}{BUCKET_METADATA_FILE}");
        migrate_one_if_missing(store.clone(), &opts, &h, &meta_path, &format!("bucket metadata: {bucket}")).await;

        let resync_path = format!(
            "{BUCKET_META_PREFIX}{SLASH_SEPARATOR}{bucket}{SLASH_SEPARATOR}{REPLICATION_META_DIR}{SLASH_SEPARATOR}{RESYNC_META_FILE}"
        );
        migrate_one_if_missing(store.clone(), &opts, &h, &resync_path, &format!("bucket replication resync: {bucket}")).await;
    }
}

async fn migrate_one_if_missing<S: StorageAPI>(
    store: Arc<S>,
    opts: &ObjectOptions,
    headers: &HeaderMap,
    path: &str,
    label: &str,
) {
    if store
        .get_object_info(RUSTFS_META_BUCKET, path, &ObjectOptions::default())
        .await
        .is_ok()
    {
        debug!("{label} already exists in RustFS, skip");
        return;
    }

    let mut rd = match store
        .get_object_reader(MIGRATING_META_BUCKET, path, None, headers.clone(), opts)
        .await
    {
        Ok(r) => r,
        Err(e) => {
            debug!("read migrating {label}: {e}");
            return;
        }
    };

    let data = match rd.read_all().await {
        Ok(d) if !d.is_empty() => d,
        Ok(_) => return,
        Err(e) => {
            debug!("read migrating {label} body: {e}");
            return;
        }
    };

    let data = match normalize_bucket_meta_blob(path, &data) {
        Ok(Some(normalized)) => normalized,
        Ok(None) => data,
        Err(e) => {
            warn!("skip {label} migration due to incompatible format: {e}");
            return;
        }
    };

    let mut put_data = PutObjReader::from_vec(data);
    if let Err(e) = store.put_object(RUSTFS_META_BUCKET, path, &mut put_data, opts).await {
        warn!("write {label}: {e}");
    } else {
        info!("Migrated {label}");
    }
}

/// Migrates IAM config from legacy meta bucket `config/iam/` to RustFS meta bucket.
/// Lists all objects under the IAM prefix in the source, copies each to the target if not present.
/// Skips objects that already exist in RustFS (idempotent).
/// If list_objects_v2 on the legacy bucket fails (e.g. format differs), migration is skipped.
pub async fn try_migrate_iam_config<S: StorageAPI>(store: Arc<S>) {
    let opts = ObjectOptions {
        max_parity: true,
        no_lock: true,
        ..Default::default()
    };
    let h = HeaderMap::new();
    let prefix = format!("{IAM_CONFIG_PREFIX}/");
    let mut continuation: Option<String> = None;
    let mut total_migrated = 0usize;

    loop {
        let list_result = match store
            .clone()
            .list_objects_v2(MIGRATING_META_BUCKET, &prefix, continuation, None, 500, false, None, false)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                debug!("list IAM config from legacy bucket failed (skip migration): {e}");
                return;
            }
        };

        for obj in list_result.objects {
            let path = &obj.name;
            if path.is_empty() || path.ends_with('/') {
                continue;
            }
            if store
                .get_object_info(RUSTFS_META_BUCKET, path, &ObjectOptions::default())
                .await
                .is_ok()
            {
                debug!("IAM config already exists in RustFS, skip: {path}");
                continue;
            }
            let mut rd = match store
                .get_object_reader(MIGRATING_META_BUCKET, path, None, h.clone(), &opts)
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    debug!("read migrating IAM config {path}: {e}");
                    continue;
                }
            };
            let data = match rd.read_all().await {
                Ok(d) if !d.is_empty() => d,
                Ok(_) => continue,
                Err(e) => {
                    debug!("read migrating IAM config {path} body: {e}");
                    continue;
                }
            };
            let data = match normalize_iam_config_blob(path, &data) {
                Ok(Some(normalized)) => normalized,
                Ok(None) => {
                    debug!("skip unsupported IAM config path during migration: {path}");
                    continue;
                }
                Err(e) => {
                    warn!("skip IAM config migration due to incompatible format, path: {path}, err: {e}");
                    continue;
                }
            };
            let mut put_data = PutObjReader::from_vec(data);
            if let Err(e) = store.put_object(RUSTFS_META_BUCKET, path, &mut put_data, &opts).await {
                warn!("write IAM config {path}: {e}");
            } else {
                info!("Migrated IAM config: {path}");
                total_migrated += 1;
            }
        }

        continuation = list_result.next_continuation_token.or(list_result.continuation_token);
        if !list_result.is_truncated || continuation.is_none() {
            break;
        }
    }

    if total_migrated > 0 {
        info!("IAM migration complete: {} object(s) migrated", total_migrated);
    }
}

#[cfg(test)]
mod tests {
    use super::{normalize_bucket_meta_blob, normalize_iam_config_blob};
    use crate::bucket::replication::{
        BucketReplicationResyncStatus, ResyncStatusType, TargetReplicationResyncStatus, decode_resync_file, encode_resync_file,
    };
    use std::collections::HashMap;

    #[test]
    fn test_normalize_policy_mapping_legacy_timestamp_and_fields() {
        let path = "config/iam/policydb/users/alice.json";
        let input = r#"{"version":1,"policies":"readwrite","update_at":"2026-03-09 02:22:44.998954 +00:00:00"}"#;

        let output = normalize_iam_config_blob(path, input.as_bytes())
            .expect("normalize should succeed")
            .expect("path should be supported");

        let v: serde_json::Value = serde_json::from_slice(&output).expect("output should be valid JSON");
        assert_eq!(v.get("policy").and_then(|x| x.as_str()), Some("readwrite"));
        assert!(v.get("policies").is_none(), "legacy field should be normalized");

        let updated_at = v
            .get("updatedAt")
            .and_then(|x| x.as_str())
            .expect("updatedAt should exist as string");
        assert!(updated_at.contains('T'), "updatedAt should be RFC3339-like");
    }

    #[test]
    fn test_normalize_bucket_meta_blob_resync_reencode() {
        let path = ".buckets/test/.replication/resync.bin";
        let mut status = BucketReplicationResyncStatus::new();
        status.id = 123;
        status.targets_map = HashMap::from([(
            "arn:replication::1:dest".to_string(),
            TargetReplicationResyncStatus {
                resync_id: "reset-1".to_string(),
                resync_status: ResyncStatusType::ResyncStarted,
                replicated_count: 1,
                ..Default::default()
            },
        )]);

        let input = encode_resync_file(&status).expect("encode should succeed");
        let output = normalize_bucket_meta_blob(path, &input)
            .expect("normalize should succeed")
            .expect("resync path should be normalized");

        let decoded = decode_resync_file(&output).expect("decode should succeed");
        assert_eq!(decoded.id, 123);
        assert_eq!(decoded.targets_map["arn:replication::1:dest"].resync_id, "reset-1");
    }
}
