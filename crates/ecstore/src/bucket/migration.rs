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
use crate::bucket::replication::ReplicationMigrationBridge;
use crate::disk::{BUCKET_META_PREFIX, MIGRATING_META_BUCKET, RUSTFS_META_BUCKET};
use crate::error::Error;
use crate::object_api::{GetObjectReader, ObjectInfo, ObjectOptions, PutObjReader};
use crate::storage_api_contracts::{
    bucket::{BucketOperations, BucketOptions},
    list::{ListOperations, StorageListObjectVersionsInfo, StorageListObjectsV2Info, StorageObjectInfoOrErr, StorageWalkOptions},
    object::{DeletedObject, EcstoreObjectIO, EcstoreObjectOperations, ObjectIO, ObjectOperations, ObjectToDelete},
    range::HTTPRangeSpec,
};
use http::HeaderMap;
use rustfs_filemeta::FileInfo;
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

type ListObjectsV2Info = StorageListObjectsV2Info<ObjectInfo>;
type ListObjectVersionsInfo = StorageListObjectVersionsInfo<ObjectInfo>;
type ObjectInfoOrErr = StorageObjectInfoOrErr<ObjectInfo, Error>;
type WalkOptions = StorageWalkOptions<fn(&FileInfo) -> bool>;

/// Callback used to decrypt an at-rest config blob during MinIO -> RustFS migration.
///
/// MinIO encrypts IAM identity/service-account files and the server config at rest
/// with a key derived from the root credentials. The migration paths live in
/// `ecstore`, which cannot depend on the IAM crate that owns the decryption keys,
/// so the caller injects the decryption logic. Given raw bytes read from the
/// legacy meta bucket, it returns the plaintext when a key succeeds, or `None`
/// when the blob cannot be decrypted (in which case the raw bytes are used as-is,
/// preserving the original plaintext-only behavior).
pub type LegacyBlobDecryptFn = Arc<dyn Fn(&[u8]) -> Option<Vec<u8>> + Send + Sync>;

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
    let status =
        ReplicationMigrationBridge::decode_resync_status(data).map_err(|err| format!("decode resync meta failed: {err}"))?;
    ReplicationMigrationBridge::encode_resync_status(&status)
        .map(Some)
        .map_err(|err| format!("encode resync meta failed: {err}"))
}

/// Migrates bucket metadata from legacy format to RustFS.
/// Uses list_bucket (from disk volumes) to get bucket names, since list_objects_v2 on the legacy
/// meta bucket may not work (legacy format differs from object layer expectations).
/// Skips buckets that already exist in RustFS (idempotent).
pub async fn try_migrate_bucket_metadata<S>(store: Arc<S>)
where
    S: BucketOperations<Error = crate::error::Error>
        + ObjectIO<
            Error = crate::error::Error,
            RangeSpec = HTTPRangeSpec,
            HeaderMap = HeaderMap,
            ObjectOptions = ObjectOptions,
            ObjectInfo = ObjectInfo,
            GetObjectReader = GetObjectReader,
            PutObjectReader = PutObjReader,
        > + ObjectOperations<
            Error = crate::error::Error,
            ObjectInfo = ObjectInfo,
            ObjectOptions = ObjectOptions,
            FileInfo = FileInfo,
            ObjectToDelete = ObjectToDelete,
            DeletedObject = DeletedObject,
        >,
{
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

async fn migrate_one_if_missing<S>(store: Arc<S>, opts: &ObjectOptions, headers: &HeaderMap, path: &str, label: &str)
where
    S: EcstoreObjectIO + EcstoreObjectOperations,
{
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
pub async fn try_migrate_iam_config<S>(store: Arc<S>, decrypt_fn: Option<LegacyBlobDecryptFn>)
where
    S: ListOperations<
            Error = crate::error::Error,
            ListObjectsV2Info = ListObjectsV2Info,
            ListObjectVersionsInfo = ListObjectVersionsInfo,
            ObjectInfoOrErr = ObjectInfoOrErr,
            WalkOptions = WalkOptions,
            WalkCancellation = tokio_util::sync::CancellationToken,
            WalkResultSender = tokio::sync::mpsc::Sender<ObjectInfoOrErr>,
        > + ObjectIO<
            Error = crate::error::Error,
            RangeSpec = HTTPRangeSpec,
            HeaderMap = HeaderMap,
            ObjectOptions = ObjectOptions,
            ObjectInfo = ObjectInfo,
            GetObjectReader = GetObjectReader,
            PutObjectReader = PutObjReader,
        > + ObjectOperations<
            Error = crate::error::Error,
            ObjectInfo = ObjectInfo,
            ObjectOptions = ObjectOptions,
            FileInfo = FileInfo,
            ObjectToDelete = ObjectToDelete,
            DeletedObject = DeletedObject,
        >,
{
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
            // MinIO encrypts IAM identity/service-account files at rest. Decrypt
            // before normalizing; fall back to the raw bytes when no key applies
            // (plaintext blobs, or nothing to decrypt) so existing behavior holds.
            let data = match &decrypt_fn {
                Some(decrypt) => decrypt(&data).unwrap_or(data),
                None => data,
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
        BucketReplicationResyncStatus, ReplicationMigrationBridge, ResyncStatusType, TargetReplicationResyncStatus,
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
    fn test_encrypted_identity_requires_decrypt_before_normalize() {
        // Reproduces the MinIO drop-in migration gap: an IAM identity file that
        // MinIO encrypted at rest is NOT valid JSON, so normalize fails outright.
        // The migration's decrypt callback must run first to recover it.
        let path = "config/iam/users/alice/identity.json";
        let plaintext = br#"{"version":1,"credentials":{"accessKey":"alice","secretKey":"alicesecret"}}"#;

        // Simulate MinIO's at-rest encryption of the identity blob.
        let ciphertext = rustfs_crypto::encrypt_data(b"root-secret-key", plaintext).expect("encrypt identity blob");

        // Without decryption (old behavior): normalize can't parse ciphertext -> Err -> skipped.
        assert!(
            normalize_iam_config_blob(path, &ciphertext).is_err(),
            "ciphertext must not parse as a JSON identity"
        );

        // With the decrypt callback recovering plaintext first: normalize succeeds.
        let decrypt_fn: super::LegacyBlobDecryptFn =
            std::sync::Arc::new(|data: &[u8]| rustfs_crypto::decrypt_data(b"root-secret-key", data).ok());
        let recovered = decrypt_fn(&ciphertext).expect("callback should decrypt the identity blob");
        assert_eq!(recovered, plaintext);

        let normalized = normalize_iam_config_blob(path, &recovered)
            .expect("normalize should succeed on decrypted plaintext")
            .expect("identity path should be supported");
        let v: serde_json::Value = serde_json::from_slice(&normalized).expect("output should be valid JSON");
        assert!(v.get("updatedAt").is_some(), "normalize should backfill updatedAt");
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

        let input = ReplicationMigrationBridge::encode_resync_status(&status).expect("encode should succeed");
        let output = normalize_bucket_meta_blob(path, &input)
            .expect("normalize should succeed")
            .expect("resync path should be normalized");

        let decoded = ReplicationMigrationBridge::decode_resync_status(&output).expect("decode should succeed");
        assert_eq!(decoded.id, 123);
        assert_eq!(decoded.targets_map["arn:replication::1:dest"].resync_id, "reset-1");
    }

    fn decode_hex(s: &str) -> Vec<u8> {
        let s: String = s.chars().filter(|c| !c.is_whitespace()).collect();
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("valid hex fixture"))
            .collect()
    }

    /// backlog#580 Phase 2/4: end-to-end proof that `try_migrate_bucket_metadata`
    /// pulls a real MinIO-written bucket-metadata blob from a `.minio.sys` layout
    /// into `.rustfs.sys`, and that the migrated blob loads every config. Uses a
    /// throwaway 4-drive local ECStore. Fixture: `tests/fixtures/minio/README.md`.
    #[tokio::test]
    async fn migrates_real_minio_bucket_metadata_end_to_end() {
        use crate::bucket::metadata::{BUCKET_METADATA_FILE, BucketMetadata};
        use crate::config::com::read_config;
        use crate::disk::endpoint::Endpoint;
        use crate::disk::{BUCKET_META_PREFIX, MIGRATING_META_BUCKET, RUSTFS_META_BUCKET};
        use crate::layout::endpoints::{EndpointServerPools, Endpoints, PoolEndpoints};
        use crate::object_api::{ObjectOptions, PutObjReader};
        use crate::storage_api_contracts::bucket::{BucketOperations, BucketOptions, MakeBucketOptions};
        use crate::storage_api_contracts::object::{ObjectIO, ObjectOperations};
        use crate::store::{ECStore, init_local_disks};
        use rustfs_utils::path::SLASH_SEPARATOR;
        use tokio::fs;
        use tokio_util::sync::CancellationToken;
        use uuid::Uuid;

        let blob = decode_hex(include_str!("../../tests/fixtures/minio/bucket_metadata.blob.hex"));

        // --- Stand up a throwaway 4-drive local ECStore. ---
        let base = std::path::PathBuf::from(format!("/tmp/rustfs_minio_migrate_test_{}", Uuid::new_v4()));
        let disk_paths: Vec<_> = (1..=4).map(|i| base.join(format!("disk{i}"))).collect();
        for p in &disk_paths {
            fs::create_dir_all(p).await.unwrap();
        }
        let mut endpoints = Vec::new();
        for (i, p) in disk_paths.iter().enumerate() {
            let mut ep = Endpoint::try_from(p.to_str().unwrap()).unwrap();
            ep.set_pool_index(0);
            ep.set_set_index(0);
            ep.set_disk_index(i);
            endpoints.push(ep);
        }
        let endpoint_pools = EndpointServerPools(vec![PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 4,
            endpoints: Endpoints::from(endpoints),
            cmd_line: "minio-migrate-test".to_string(),
            platform: format!("OS: {} | Arch: {}", std::env::consts::OS, std::env::consts::ARCH),
        }]);
        init_local_disks(endpoint_pools.clone()).await.unwrap();
        let ecstore = ECStore::new("127.0.0.1:0".parse().unwrap(), endpoint_pools, CancellationToken::new())
            .await
            .unwrap();
        let existing: Vec<String> = ecstore
            .list_bucket(&BucketOptions {
                no_metadata: true,
                ..Default::default()
            })
            .await
            .unwrap()
            .into_iter()
            .map(|b| b.name)
            .collect();
        crate::bucket::metadata_sys::init_bucket_metadata_sys(ecstore.clone(), existing).await;

        let meta_path = format!("{BUCKET_META_PREFIX}{SLASH_SEPARATOR}interop{SLASH_SEPARATOR}{BUCKET_METADATA_FILE}");
        let put_opts = ObjectOptions::default();

        // --- Arrange the pre-migration state a MinIO import starts from: the ---
        // bucket exists and its config lives under `.minio.sys`, while `.rustfs.sys`
        // has no metadata for it yet.
        ecstore.make_bucket("interop", &MakeBucketOptions::default()).await.unwrap();
        let _ = ecstore
            .delete_object(RUSTFS_META_BUCKET, &meta_path, ObjectOptions::default())
            .await;
        // MinIO leaves its `.minio.sys` meta volume on every drive; recreate it so
        // the source object can be seeded through the object layer.
        for p in &disk_paths {
            fs::create_dir_all(p.join(MIGRATING_META_BUCKET)).await.ok();
        }
        let mut src = PutObjReader::from_vec(blob.clone());
        ecstore
            .put_object(MIGRATING_META_BUCKET, &meta_path, &mut src, &put_opts)
            .await
            .expect("seed .minio.sys bucket metadata");

        // --- Run the real startup migration. ---
        super::try_migrate_bucket_metadata(ecstore.clone()).await;

        // --- The migrated `.rustfs.sys` blob must carry every MinIO config, ---
        // byte-identical to the source (typed XML/JSON parsing of these fields is
        // covered by the bucket-metadata parse-parity test).
        let migrated = read_config(ecstore.clone(), &meta_path)
            .await
            .expect("read migrated bucket metadata");
        BucketMetadata::check_header(&migrated).expect("migrated blob has a valid header");
        let bm = BucketMetadata::unmarshal(&migrated[4..]).expect("unmarshal migrated bucket metadata");
        let src = BucketMetadata::unmarshal(&blob[4..]).expect("unmarshal source bucket metadata");

        assert_eq!(bm.name, "interop");
        assert_eq!(bm.policy_config_json, src.policy_config_json, "policy migrated intact");
        assert_eq!(bm.lifecycle_config_xml, src.lifecycle_config_xml, "lifecycle migrated intact");
        assert_eq!(bm.object_lock_config_xml, src.object_lock_config_xml, "object-lock migrated intact");
        assert_eq!(bm.versioning_config_xml, src.versioning_config_xml, "versioning migrated intact");
        assert_eq!(bm.tagging_config_xml, src.tagging_config_xml, "tagging migrated intact");
        assert_eq!(bm.quota_config_json, src.quota_config_json, "quota migrated intact");
        assert_eq!(bm.notification_config_xml, src.notification_config_xml, "notification migrated intact");
        assert_eq!(bm.encryption_config_xml, src.encryption_config_xml, "encryption migrated intact");
        assert_eq!(bm.replication_config_xml, src.replication_config_xml, "replication migrated intact");
        assert!(!bm.lifecycle_config_xml.is_empty(), "lifecycle present");
        assert!(!bm.replication_config_xml.is_empty(), "replication present");

        fs::remove_dir_all(&base).await.ok();
    }
}
