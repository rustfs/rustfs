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

use super::{BucketVersioningSys, ReplicationStatusType, Result, StorageError};
use crate::storage::storage_api::options_consumer::contract::{object::HTTPPreconditions, range::HTTPRangeSpec};
use http::header::{IF_MATCH, IF_NONE_MATCH};
use http::{HeaderMap, HeaderValue};
use rustfs_utils::http::{
    AMZ_BUCKET_REPLICATION_STATUS, SUFFIX_FORCE_DELETE, SUFFIX_OBJECTLOCK_LEGALHOLD_TIMESTAMP,
    SUFFIX_OBJECTLOCK_RETENTION_TIMESTAMP, SUFFIX_REPLICATION_ACTUAL_OBJECT_SIZE, SUFFIX_REPLICATION_SSEC_CRC,
    SUFFIX_SOURCE_DELETEMARKER, SUFFIX_SOURCE_ETAG, SUFFIX_SOURCE_MTIME, SUFFIX_SOURCE_PROXY_REQUEST,
    SUFFIX_SOURCE_REPLICATION_LEGALHOLD_TIMESTAMP, SUFFIX_SOURCE_REPLICATION_REQUEST,
    SUFFIX_SOURCE_REPLICATION_RETENTION_TIMESTAMP, SUFFIX_SOURCE_REPLICATION_TAGGING_TIMESTAMP, SUFFIX_SOURCE_VERSION_ID,
    SUFFIX_TAGGING_TIMESTAMP, get_header,
    header_compat::{MINIO_ENCRYPTION_PREFIX, RUSTFS_ENCRYPTION_PREFIX},
    insert_header_map, insert_str,
    metadata_compat::{MINIO_INTERNAL_PREFIX, RUSTFS_INTERNAL_PREFIX},
};
use rustfs_utils::http::{
    AMZ_META_UNENCRYPTED_CONTENT_LENGTH, AMZ_META_UNENCRYPTED_CONTENT_MD5, AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER,
    AMZ_OBJECT_LOCK_MODE_LOWER, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER,
};
use s3s::header::X_AMZ_OBJECT_LOCK_MODE;
use s3s::header::X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE;

use crate::auth::UNSIGNED_PAYLOAD;
use crate::auth::UNSIGNED_PAYLOAD_TRAILER;
use rustfs_policy::service_type::ServiceType;
use rustfs_utils::hash::EMPTY_STRING_SHA256_HASH;
use rustfs_utils::http::AMZ_CONTENT_SHA256;
use rustfs_utils::path::is_dir_object;
use s3s::{S3Error, S3ErrorCode, S3Result, s3_error};
use std::collections::HashMap;
use std::sync::LazyLock;
use tracing::error;
use uuid::Uuid;

use crate::auth::AuthType;
use crate::auth::get_query_param;
use crate::auth::get_request_auth_type_with_query;
use crate::auth::is_request_presigned_signature_v4_with_query;
use crate::storage::storage_api::ecstore_bucket::versioning::VersioningApi as _;
use crate::storage::storage_api::options_consumer::StorageObjectOptions as ObjectOptions;
use s3s::dto::VersioningConfiguration;

/// Test-only counter of versioning-config fetches, used to pin that batch
/// handlers resolve the configuration once per request, not once per key
/// (same counting pattern as MUST_REPLICATE_OBJECT_CALLS).
///
/// Blind spot: only fetches routed through [`bucket_versioning_config`] are
/// counted — handler code that calls `BucketVersioningSys` directly bypasses
/// this seam and its regression pins.
#[cfg(test)]
pub(crate) static VERSIONING_CONFIG_LOOKUPS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

#[cfg(test)]
type VersioningConfigTestHook = (String, std::sync::Arc<tokio::sync::Barrier>, std::sync::Arc<tokio::sync::Barrier>);

#[cfg(test)]
static VERSIONING_CONFIG_TEST_HOOK: std::sync::OnceLock<std::sync::Mutex<Option<VersioningConfigTestHook>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
pub(crate) fn install_versioning_config_test_hook(
    bucket: String,
    entered: std::sync::Arc<tokio::sync::Barrier>,
    resume: std::sync::Arc<tokio::sync::Barrier>,
) {
    *VERSIONING_CONFIG_TEST_HOOK
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("versioning config test hook lock should not be poisoned") = Some((bucket, entered, resume));
}

#[cfg(test)]
async fn wait_for_versioning_config_test_hook(bucket: &str) {
    let hook = {
        let mut slot = VERSIONING_CONFIG_TEST_HOOK
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("versioning config test hook lock should not be poisoned");
        if slot.as_ref().is_some_and(|(expected_bucket, _, _)| expected_bucket == bucket) {
            slot.take()
        } else {
            None
        }
    };
    if let Some((_bucket, entered, resume)) = hook {
        entered.wait().await;
        resume.wait().await;
    }
}

/// Fetch the bucket's versioning configuration once so callers can derive
/// enabled/suspended state without repeated metadata-sys lookups per request.
pub(crate) async fn bucket_versioning_config(bucket: &str) -> VersioningConfiguration {
    #[cfg(test)]
    VERSIONING_CONFIG_LOOKUPS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    #[cfg(test)]
    wait_for_versioning_config_test_hook(bucket).await;
    match BucketVersioningSys::get(bucket).await {
        Ok(cfg) => cfg,
        Err(err) => {
            tracing::warn!(
                bucket = %bucket,
                error = ?err,
                "failed to load bucket versioning configuration; using default configuration"
            );
            VersioningConfiguration::default()
        }
    }
}

/// Whether GET should skip per-shard bitrot verification. Read once: the env
/// flag is consulted on every GET and `std::env::var` takes a process-global
/// lock. In tests the env is read directly so `temp_env` overrides apply.
fn get_skip_verify_bitrot() -> bool {
    #[cfg(test)]
    {
        rustfs_utils::get_env_bool(
            rustfs_config::ENV_OBJECT_GET_SKIP_BITROT_VERIFY,
            rustfs_config::DEFAULT_OBJECT_GET_SKIP_BITROT_VERIFY,
        )
    }
    #[cfg(not(test))]
    {
        static CACHED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        *CACHED.get_or_init(|| {
            rustfs_utils::get_env_bool(
                rustfs_config::ENV_OBJECT_GET_SKIP_BITROT_VERIFY,
                rustfs_config::DEFAULT_OBJECT_GET_SKIP_BITROT_VERIFY,
            )
        })
    }
}

/// Creates options for deleting an object in a bucket.
pub async fn del_opts(
    bucket: &str,
    object: &str,
    vid: Option<String>,
    headers: &HeaderMap<HeaderValue>,
    metadata: HashMap<String, String>,
) -> Result<ObjectOptions> {
    let versioning_cfg = bucket_versioning_config(bucket).await;
    del_opts_with_versioning(bucket, object, vid, headers, metadata, &versioning_cfg, false)
}

/// Like [`del_opts`], but derives versioning state from an already-fetched
/// configuration so batch callers (DeleteObjects) resolve the bucket's
/// versioning once per request instead of once per key.
pub fn del_opts_with_versioning(
    bucket: &str,
    object: &str,
    vid: Option<String>,
    headers: &HeaderMap<HeaderValue>,
    metadata: HashMap<String, String>,
    versioning_cfg: &VersioningConfiguration,
    replication_request_authorized: bool,
) -> Result<ObjectOptions> {
    let (versioned, version_suspended) = versioning_cfg.delete_state(object);

    let vid = if vid.is_none() && replication_request_authorized {
        get_header(headers, SUFFIX_SOURCE_VERSION_ID).map(|s| s.into_owned())
    } else {
        vid
    };
    let synthetic_version_id = is_dir_object(object) && vid.is_none();

    let vid = vid.map(|v| v.as_str().trim().to_owned());

    // Handle AWS S3 special case: "null" string represents null version ID
    // When VersionId='null' is specified, it means delete the object with null version ID
    let vid = if let Some(ref id) = vid {
        if id.eq_ignore_ascii_case("null") {
            // Convert "null" to Uuid::nil() string representation
            Some(Uuid::nil().to_string())
        } else {
            // Validate UUID format for other version IDs
            if *id != Uuid::nil().to_string() && Uuid::parse_str(id.as_str()).is_err() {
                error!("del_opts: invalid version id: {} error: invalid UUID format", id);
                return Err(StorageError::InvalidVersionID(bucket.to_owned(), object.to_owned(), id.clone()));
            }
            Some(id.clone())
        }
    } else {
        None
    };

    let mut opts = if replication_request_authorized {
        put_opts_from_headers_with_replication_authorization(headers, metadata, replication_request_authorized)
    } else {
        get_default_opts(headers, metadata, false)
    }
    .map_err(|err| {
        error!("del_opts: invalid argument: {} error: {}", object, err);
        StorageError::InvalidArgument(bucket.to_owned(), object.to_owned(), err.to_string())
    })?;

    opts.delete_prefix = get_header(headers, SUFFIX_FORCE_DELETE)
        .map(|v| v.as_ref() == "true")
        .unwrap_or_default();

    opts.version_id = synthetic_version_id.then(|| Uuid::nil().to_string()).or(vid);
    opts.synthetic_version_id = synthetic_version_id;
    opts.version_suspended = version_suspended;
    opts.versioned = versioned;

    opts.delete_marker = replication_request_authorized
        && get_header(headers, SUFFIX_SOURCE_DELETEMARKER)
            .map(|v| v.as_ref() == "true")
            .unwrap_or_default();

    fill_conditional_writes_opts_from_header(headers, &mut opts)?;

    Ok(opts)
}

/// Creates options for getting an object from a bucket.
pub async fn get_opts(
    bucket: &str,
    object: &str,
    vid: Option<String>,
    part_num: Option<usize>,
    headers: &HeaderMap<HeaderValue>,
) -> Result<ObjectOptions> {
    let versioning_cfg = bucket_versioning_config(bucket).await;
    let versioned = versioning_cfg.prefix_enabled(object);
    let version_suspended = versioning_cfg.prefix_suspended(object);

    let vid = vid.map(|v| v.as_str().trim().to_owned());

    let nil_uuid_str = Uuid::nil().to_string();

    let vid = match vid {
        Some(ref id) => {
            if id.eq_ignore_ascii_case("null") {
                Some(nil_uuid_str.clone())
            } else {
                if id.as_str() != nil_uuid_str.as_str() && Uuid::parse_str(id).is_err() {
                    return Err(StorageError::InvalidVersionID(bucket.to_owned(), object.to_owned(), id.clone()));
                }
                Some(id.clone())
            }
        }
        None => None,
    };

    let mut opts = get_default_opts(headers, HashMap::new(), false)
        .map_err(|err| StorageError::InvalidArgument(bucket.to_owned(), object.to_owned(), err.to_string()))?;

    opts.version_id = {
        if is_dir_object(object) && vid.is_none() {
            Some(nil_uuid_str)
        } else {
            vid
        }
    };

    opts.part_number = part_num;

    opts.version_suspended = version_suspended;
    opts.versioned = versioned;

    // Optionally skip per-shard bitrot hash verification on reads to save CPU.
    // Background scanner still performs full integrity checks asynchronously.
    opts.skip_verify_bitrot = get_skip_verify_bitrot();

    // Anti-loop markers for the replication read proxy
    // (`{x-rustfs-,x-minio-}source-proxy-request` header family).
    // MinIO semantics: the header being PRESENT at all (`ProxyHeaderSet`)
    // disables proxying, whatever its value — a peer's replication worker
    // sends "false" on its convergence HEADs so the receiver answers locally
    // instead of proxying the miss back (a proxied echo would fake
    // convergence and the object would never replicate). Deliberately not
    // gated on replication authorization: the header only disables proxying
    // (it grants nothing).
    let proxy_header = get_header(headers, SUFFIX_SOURCE_PROXY_REQUEST);
    opts.proxy_header_set = proxy_header.is_some();
    opts.proxy_request = proxy_header.map(|v| v.as_ref() == "true").unwrap_or_default();

    fill_conditional_writes_opts_from_header(headers, &mut opts)?;

    Ok(opts)
}

fn fill_conditional_writes_opts_from_header(headers: &HeaderMap<HeaderValue>, opts: &mut ObjectOptions) -> std::io::Result<()> {
    let if_none_match = conditional_etag_header(headers, IF_NONE_MATCH, "If-None-Match")?;
    let if_match = conditional_etag_header(headers, IF_MATCH, "If-Match")?;

    if if_none_match.is_some() || if_match.is_some() {
        opts.http_preconditions = Some(HTTPPreconditions {
            if_match,
            if_none_match,
            ..Default::default()
        });
    }

    Ok(())
}

fn conditional_etag_header(
    headers: &HeaderMap<HeaderValue>,
    name: http::header::HeaderName,
    display_name: &str,
) -> std::io::Result<Option<String>> {
    let Some(value) = headers.get(name) else {
        return Ok(None);
    };

    let value = value
        .to_str()
        .map_err(|_| std::io::Error::other(format!("Invalid {display_name} header")))?
        .trim();

    if value.is_empty() {
        Ok(None)
    } else {
        Ok(Some(value.to_owned()))
    }
}

/// Creates options for putting an object in a bucket.
pub async fn put_opts(
    bucket: &str,
    object: &str,
    vid: Option<String>,
    headers: &HeaderMap<HeaderValue>,
    metadata: HashMap<String, String>,
) -> Result<ObjectOptions> {
    put_opts_with_replication_authorization(bucket, object, vid, headers, metadata, false).await
}

pub async fn put_opts_with_replication_authorization(
    bucket: &str,
    object: &str,
    vid: Option<String>,
    headers: &HeaderMap<HeaderValue>,
    metadata: HashMap<String, String>,
    replication_request_authorized: bool,
) -> Result<ObjectOptions> {
    let versioning_cfg = bucket_versioning_config(bucket).await;
    let versioned = versioning_cfg.prefix_enabled(object);
    let version_suspended = versioning_cfg.prefix_suspended(object);

    let vid = if vid.is_none() && replication_request_authorized {
        get_header(headers, SUFFIX_SOURCE_VERSION_ID).map(|s| s.into_owned())
    } else {
        vid
    };

    // The S3 API addresses the null version as the literal "null"
    // (MinIO-compatible replication senders, including RustFS itself, put it
    // in the versionId query); normalize it to the internal nil-UUID
    // representation exactly like get_opts / del_opts do.
    let vid = vid.map(|v| {
        let id = v.as_str().trim();
        if id.eq_ignore_ascii_case("null") {
            Uuid::nil().to_string()
        } else {
            id.to_owned()
        }
    });

    if let Some(ref id) = vid
        && *id != Uuid::nil().to_string()
        && let Err(_err) = Uuid::parse_str(id.as_str())
    {
        return Err(StorageError::InvalidVersionID(bucket.to_owned(), object.to_owned(), id.clone()));
    }

    let mut opts = put_opts_from_headers_with_replication_authorization(headers, metadata, replication_request_authorized)
        .map_err(|err| StorageError::InvalidArgument(bucket.to_owned(), object.to_owned(), err.to_string()))?;

    opts.version_id = {
        if is_dir_object(object) && vid.is_none() {
            Some(Uuid::nil().to_string())
        } else {
            vid
        }
    };
    opts.version_suspended = version_suspended;
    opts.versioned = versioned;

    fill_conditional_writes_opts_from_header(headers, &mut opts)?;

    Ok(opts)
}

pub fn get_complete_multipart_upload_opts(headers: &HeaderMap<HeaderValue>) -> std::io::Result<ObjectOptions> {
    get_complete_multipart_upload_opts_with_replication_authorization(headers, false)
}

pub fn get_complete_multipart_upload_opts_with_replication_authorization(
    headers: &HeaderMap<HeaderValue>,
    replication_request_authorized: bool,
) -> std::io::Result<ObjectOptions> {
    let mut user_defined = HashMap::new();

    let mut replication_request = false;
    let mut mod_time = None;
    let mut preserve_etag = None;
    if replication_request_authorized && get_header(headers, SUFFIX_SOURCE_REPLICATION_REQUEST).as_deref() == Some("true") {
        replication_request = true;
        mod_time = replication_source_mtime(headers);
        preserve_etag = replication_source_etag(headers);
        if let Some(actual_size_str) = get_header(headers, SUFFIX_REPLICATION_ACTUAL_OBJECT_SIZE) {
            rustfs_utils::http::insert_str(
                &mut user_defined,
                rustfs_utils::http::SUFFIX_ACTUAL_OBJECT_SIZE_CAP,
                actual_size_str.into_owned(),
            );
        } else {
            tracing::warn!("Failed to get or parse replication actual object size header (x-rustfs-* or x-minio-*)");
        }
    }

    if replication_request_authorized && let Some(v) = get_header(headers, SUFFIX_REPLICATION_SSEC_CRC) {
        insert_header_map(&mut user_defined, SUFFIX_REPLICATION_SSEC_CRC, v.into_owned());
    }

    let mut opts = ObjectOptions {
        want_checksum: rustfs_rio::get_content_checksum(headers)?,
        user_defined,
        replication_request,
        mod_time,
        preserve_etag,
        ..Default::default()
    };
    if replication_request {
        apply_replication_timestamps_from_headers(headers, &mut opts);
    }
    apply_replica_status_from_headers(headers, &mut opts, replication_request_authorized);

    fill_conditional_writes_opts_from_header(headers, &mut opts)?;
    Ok(opts)
}

/// Creates options for copying an object in a bucket.
pub async fn copy_dst_opts(
    bucket: &str,
    object: &str,
    vid: Option<String>,
    headers: &HeaderMap<HeaderValue>,
    metadata: HashMap<String, String>,
) -> Result<ObjectOptions> {
    copy_dst_opts_with_replication_authorization(bucket, object, vid, headers, metadata, false).await
}

pub async fn copy_dst_opts_with_replication_authorization(
    bucket: &str,
    object: &str,
    vid: Option<String>,
    headers: &HeaderMap<HeaderValue>,
    metadata: HashMap<String, String>,
    replication_request_authorized: bool,
) -> Result<ObjectOptions> {
    put_opts_with_replication_authorization(bucket, object, vid, headers, metadata, replication_request_authorized).await
}

pub fn copy_src_opts(_bucket: &str, _object: &str, headers: &HeaderMap<HeaderValue>) -> Result<ObjectOptions> {
    get_default_opts(headers, HashMap::new(), false)
}

pub fn put_opts_from_headers(headers: &HeaderMap<HeaderValue>, metadata: HashMap<String, String>) -> Result<ObjectOptions> {
    put_opts_from_headers_with_replication_authorization(headers, metadata, false)
}

pub(crate) fn has_replication_retention_update(headers: &HeaderMap<HeaderValue>, replication_request_authorized: bool) -> bool {
    replication_request_authorized
        && get_header(headers, SUFFIX_SOURCE_REPLICATION_REQUEST).as_deref() == Some("true")
        && replication_timestamp_header(headers, SUFFIX_SOURCE_REPLICATION_RETENTION_TIMESTAMP).is_some()
}

pub fn put_opts_from_headers_with_replication_authorization(
    headers: &HeaderMap<HeaderValue>,
    metadata: HashMap<String, String>,
    replication_request_authorized: bool,
) -> Result<ObjectOptions> {
    let mut opts = get_default_opts(headers, metadata, false)?;
    apply_replica_status_from_headers(headers, &mut opts, replication_request_authorized);
    if replication_request_authorized && get_header(headers, SUFFIX_SOURCE_REPLICATION_REQUEST).as_deref() == Some("true") {
        opts.replication_request = true;
        opts.mod_time = replication_source_mtime(headers);
        opts.preserve_etag = replication_source_etag(headers);
        // SSE-C ciphertext passthrough: restore the stored encryption metadata
        // from the transport headers and mark the body as already encrypted so
        // the write path stores it verbatim.
        if let Some(restored) = rustfs_utils::http::ssec_transport_to_stored_metadata(headers) {
            opts.user_defined.extend(restored);
            opts.preserve_ciphertext = true;
        }
        if let Some(crc) = get_header(headers, SUFFIX_REPLICATION_SSEC_CRC) {
            insert_header_map(&mut opts.user_defined, SUFFIX_REPLICATION_SSEC_CRC, crc.into_owned());
        }
        apply_replication_timestamps_from_headers(headers, &mut opts);
    }
    Ok(opts)
}

/// Replicas must keep the source object's ETag: managed-SSE replication
/// re-encrypts on the target, so a recomputed ETag would differ from the
/// source and every HEAD comparison would re-schedule the object forever.
fn replication_source_etag(headers: &HeaderMap<HeaderValue>) -> Option<String> {
    let value = get_header(headers, SUFFIX_SOURCE_ETAG)?;
    let value = value.trim().trim_matches('"');
    (!value.is_empty()).then(|| value.to_string())
}

fn replication_source_mtime(headers: &HeaderMap<HeaderValue>) -> Option<time::OffsetDateTime> {
    let value = get_header(headers, SUFFIX_SOURCE_MTIME)?;
    let value = value.trim();
    match time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339) {
        Ok(mtime) => Some(mtime),
        Err(err) => {
            tracing::warn!("Invalid source-mtime value '{}' (replication request=true): {}", value, err);
            None
        }
    }
}

/// Parses one replication LWW timestamp header. Invalid values are dropped
/// with a warning (same tolerance as [`replication_source_mtime`]) so a
/// malformed source header cannot wedge the replication queue.
fn replication_timestamp_header(headers: &HeaderMap<HeaderValue>, suffix: &str) -> Option<time::OffsetDateTime> {
    let value = get_header(headers, suffix)?;
    let value = value.trim();
    match time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339) {
        Ok(timestamp) => Some(timestamp),
        Err(err) => {
            tracing::warn!("Invalid {} value '{}' (replication request=true): {}", suffix, value, err);
            None
        }
    }
}

/// Callers must gate on an authorized replication request: these headers are
/// trusted source-cluster state, not client input.
fn apply_replication_timestamps_from_headers(headers: &HeaderMap<HeaderValue>, opts: &mut ObjectOptions) {
    opts.replication_tagging_timestamp = replication_timestamp_header(headers, SUFFIX_SOURCE_REPLICATION_TAGGING_TIMESTAMP);
    opts.replication_retention_timestamp = replication_timestamp_header(headers, SUFFIX_SOURCE_REPLICATION_RETENTION_TIMESTAMP);
    opts.replication_legalhold_timestamp = replication_timestamp_header(headers, SUFFIX_SOURCE_REPLICATION_LEGALHOLD_TIMESTAMP);

    // Persist into the internal metadata keys so a later outbound replication
    // pass (replication_target_boundary) reads the source's modification
    // times instead of falling back to mod_time.
    // TODO(P1-6): receiver-side LWW is still missing — when the stored
    // per-category timestamp is newer than the inbound one, the existing
    // tags/retention/legal-hold should win instead of being overwritten.
    for (timestamp, suffix) in [
        (opts.replication_tagging_timestamp, SUFFIX_TAGGING_TIMESTAMP),
        (opts.replication_retention_timestamp, SUFFIX_OBJECTLOCK_RETENTION_TIMESTAMP),
        (opts.replication_legalhold_timestamp, SUFFIX_OBJECTLOCK_LEGALHOLD_TIMESTAMP),
    ] {
        if let Some(timestamp) = timestamp
            && let Ok(value) = timestamp.format(&time::format_description::well_known::Rfc3339)
        {
            insert_str(&mut opts.user_defined, suffix, value);
        }
    }
}

fn apply_replica_status_from_headers(headers: &HeaderMap<HeaderValue>, opts: &mut ObjectOptions, authorized: bool) {
    if !authorized {
        return;
    }

    if headers
        .get(AMZ_BUCKET_REPLICATION_STATUS)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|status| status.eq_ignore_ascii_case(ReplicationStatusType::Replica.as_str()))
    {
        opts.set_replica_status(ReplicationStatusType::Replica);
        // Persist REPLICA into the object metadata as well (mirrors the
        // Snowball inbound path). The read side derives
        // ObjectInfo::replication_status from this key, and must_replicate's
        // anti-loop guard consumes it — without it, HEAD/GET report no
        // status and the scanner's existing-object pass cascades the replica
        // onward. `opts.delete_replication` alone only reaches delete flows.
        opts.user_defined
            .retain(|key, _| !key.eq_ignore_ascii_case(AMZ_BUCKET_REPLICATION_STATUS));
        opts.user_defined.insert(
            AMZ_BUCKET_REPLICATION_STATUS.to_string(),
            ReplicationStatusType::Replica.as_str().to_string(),
        );
    }
}

/// Creates default options for getting an object from a bucket.
pub fn get_default_opts(
    _headers: &HeaderMap<HeaderValue>,
    metadata: HashMap<String, String>,
    _copy_source: bool,
) -> Result<ObjectOptions> {
    Ok(ObjectOptions {
        user_defined: metadata,
        ..Default::default()
    })
}

/// Extracts metadata from headers and returns it as a HashMap.
pub fn extract_metadata(headers: &HeaderMap<HeaderValue>) -> HashMap<String, String> {
    let mut metadata = HashMap::new();

    extract_metadata_from_mime(headers, &mut metadata);

    metadata
}

/// Extracts metadata from headers and returns it as a HashMap.
pub fn extract_metadata_from_mime(headers: &HeaderMap<HeaderValue>, metadata: &mut HashMap<String, String>) {
    extract_metadata_from_mime_with_object_name(headers, metadata, false, None);
}

/// Normalizes Content-Encoding for storage per AWS S3 behavior: "aws-chunked" is a
/// request-side transfer encoding for SigV4 streaming and must not be stored or returned.
/// If the only value is "aws-chunked", returns None (do not persist). Otherwise returns
/// the value with "aws-chunked" stripped, or None if nothing remains.
pub(crate) fn normalize_content_encoding_for_storage(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    let normalized: String = trimmed
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.eq_ignore_ascii_case("aws-chunked"))
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join(", ");
    if normalized.is_empty() { None } else { Some(normalized) }
}

const ENV_REJECT_ARCHIVE_CONTENT_ENCODING: &str = "RUSTFS_REJECT_ARCHIVE_CONTENT_ENCODING";

const ARCHIVE_CONTENT_ENCODING_BLOCKED_SUFFIXES: &[&str] = &[
    ".zip",
    ".tar",
    ".tar.gz",
    ".tgz",
    ".tar.bz2",
    ".tbz",
    ".tbz2",
    ".tar.xz",
    ".txz",
    ".tar.zst",
    ".tar.zstd",
    ".tzst",
];

const ARCHIVE_CONTENT_ENCODING_BLOCKED_CONTENT_TYPES: &[&str] =
    &["application/zip", "application/x-zip-compressed", "application/x-tar"];

fn is_archive_object_name_for_content_encoding(object_name: &str) -> bool {
    let object_name = object_name.to_ascii_lowercase();
    ARCHIVE_CONTENT_ENCODING_BLOCKED_SUFFIXES
        .iter()
        .any(|suffix| object_name.ends_with(suffix))
}

fn is_archive_content_type_for_content_encoding(content_type: &str) -> bool {
    let main_type = content_type
        .split(';')
        .next()
        .unwrap_or(content_type)
        .trim()
        .to_ascii_lowercase();

    ARCHIVE_CONTENT_ENCODING_BLOCKED_CONTENT_TYPES
        .iter()
        .any(|candidate| main_type == *candidate)
}

pub(crate) fn validate_archive_content_encoding(
    object_name: &str,
    content_type: Option<&str>,
    content_encoding: Option<&str>,
) -> S3Result<()> {
    if !archive_content_encoding_strict_mode() {
        return Ok(());
    }

    let Some(content_encoding) = content_encoding.and_then(normalize_content_encoding_for_storage) else {
        return Ok(());
    };

    let is_archive_like = is_archive_object_name_for_content_encoding(object_name)
        || content_type.is_some_and(is_archive_content_type_for_content_encoding);
    if !is_archive_like {
        return Ok(());
    }

    Err(S3Error::with_message(
        S3ErrorCode::InvalidArgument,
        format!(
            "Content-Encoding '{content_encoding}' is not allowed for archive objects when {ENV_REJECT_ARCHIVE_CONTENT_ENCODING}=true; unset {ENV_REJECT_ARCHIVE_CONTENT_ENCODING} or set it to false to restore compatibility-first behavior"
        ),
    ))
}

fn archive_content_encoding_strict_mode() -> bool {
    rustfs_utils::get_env_bool(ENV_REJECT_ARCHIVE_CONTENT_ENCODING, false)
}

const USER_METADATA_PREFIXES: &[&str] = &["x-amz-meta-", "x-rustfs-meta-", "x-minio-meta-"];
const CANONICAL_USER_METADATA_PREFIX: &str = "x-amz-meta-";

/// Keys a client must not be able to materialize as bare stored metadata.
///
/// Must stay symmetric with [`should_skip_object_metadata_key`]: every prefix the
/// read side strips as internal must be namespaced here on the write side, or a
/// client PUT of `x-amz-meta-<internal-key>` lands on disk as the internal key
/// itself (e.g. `x-rustfs-encryption-algorithm`, which the KMS
/// `headers_to_metadata` path treats as the cipher selector).
fn is_reserved_user_metadata_key(key: &str) -> bool {
    SUPPORTED_HEADERS.iter().any(|header| key.eq_ignore_ascii_case(header))
        || starts_with_ignore_ascii_case(key, "x-amz-")
        || starts_with_ignore_ascii_case(key, RUSTFS_INTERNAL_PREFIX)
        || starts_with_ignore_ascii_case(key, MINIO_INTERNAL_PREFIX)
        || starts_with_ignore_ascii_case(key, RUSTFS_ENCRYPTION_PREFIX)
        || starts_with_ignore_ascii_case(key, MINIO_ENCRYPTION_PREFIX)
        // Replication transport names (source-replication timestamps,
        // source-mtime/-etag/-version-id, ...). A bare stored key with one of
        // these names is forwarded verbatim by the outbound replication
        // header builder on a server-authorized request, so the receiver
        // would persist attacker-chosen values as trusted internal LWW state.
        || starts_with_ignore_ascii_case(key, "x-rustfs-source-")
        || starts_with_ignore_ascii_case(key, "x-minio-source-")
}

fn stored_user_metadata_key(key: &str) -> String {
    if is_reserved_user_metadata_key(key) {
        format!("{CANONICAL_USER_METADATA_PREFIX}{key}")
    } else {
        key.to_owned()
    }
}

pub(crate) fn namespace_reserved_user_metadata(metadata: &mut HashMap<String, String>) {
    *metadata = std::mem::take(metadata)
        .into_iter()
        .map(|(key, value)| (stored_user_metadata_key(&key), value))
        .collect();
}

pub(crate) fn preserve_unclassified_user_metadata(metadata: &mut HashMap<String, String>, key: &str, value: &str) {
    let classified_user_metadata = USER_METADATA_PREFIXES
        .iter()
        .any(|prefix| key.strip_prefix(prefix).is_some_and(|suffix| !suffix.is_empty()));
    if classified_user_metadata || SUPPORTED_HEADERS.iter().any(|header| key.eq_ignore_ascii_case(header)) {
        return;
    }

    metadata.insert(stored_user_metadata_key(key), value.to_owned());
}

/// Extracts metadata from headers and returns it as a HashMap with object name for MIME type detection.
pub fn extract_metadata_from_mime_with_object_name(
    headers: &HeaderMap<HeaderValue>,
    metadata: &mut HashMap<String, String>,
    skip_content_type: bool,
    object_name: Option<&str>,
) {
    for (k, v) in headers.iter() {
        if k.as_str() == "content-type" && skip_content_type {
            continue;
        }

        if let Some(key) = USER_METADATA_PREFIXES
            .iter()
            .find_map(|prefix| k.as_str().strip_prefix(prefix))
        {
            if key.is_empty() {
                continue;
            }

            metadata.insert(stored_user_metadata_key(key), String::from_utf8_lossy(v.as_bytes()).to_string());
            continue;
        }

        for hd in SUPPORTED_HEADERS.iter() {
            if k.as_str() == *hd {
                let raw = String::from_utf8_lossy(v.as_bytes()).to_string();
                if *hd == "content-encoding" {
                    if let Some(normalized) = normalize_content_encoding_for_storage(&raw) {
                        metadata.insert(k.to_string(), normalized);
                    }
                } else {
                    metadata.insert(k.to_string(), raw);
                }
                continue;
            }
        }
    }

    if !metadata.contains_key("content-type") {
        let default_content_type = if let Some(obj_name) = object_name {
            detect_content_type_from_object_name(obj_name)
        } else {
            "binary/octet-stream".to_owned()
        };
        metadata.insert("content-type".to_owned(), default_content_type);
    }
}

fn starts_with_ignore_ascii_case(value: &str, prefix: &str) -> bool {
    value
        .get(..prefix.len())
        .is_some_and(|head| head.eq_ignore_ascii_case(prefix))
}

fn should_skip_object_metadata_key(key: &str, value: &str, excluded_headers: &[&str]) -> bool {
    const X_AMZ_PREFIX: &str = "x-amz-";

    // Skip internal/reserved metadata (x-rustfs-internal-* or x-minio-internal-*)
    if starts_with_ignore_ascii_case(key, RUSTFS_INTERNAL_PREFIX) || starts_with_ignore_ascii_case(key, MINIO_INTERNAL_PREFIX) {
        return true;
    }

    // Skip internal encryption metadata (x-rustfs-encryption-* or x-minio-encryption-*)
    if starts_with_ignore_ascii_case(key, RUSTFS_ENCRYPTION_PREFIX) || starts_with_ignore_ascii_case(key, MINIO_ENCRYPTION_PREFIX)
    {
        return true;
    }

    // Skip empty object lock values
    if value.is_empty()
        && (key.eq_ignore_ascii_case(X_AMZ_OBJECT_LOCK_MODE.as_str())
            || key.eq_ignore_ascii_case(X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.as_str()))
    {
        return true;
    }

    if key.eq_ignore_ascii_case(AMZ_META_UNENCRYPTED_CONTENT_MD5) || key.eq_ignore_ascii_case(AMZ_META_UNENCRYPTED_CONTENT_LENGTH)
    {
        return true;
    }

    if excluded_headers.iter().any(|excluded| key.eq_ignore_ascii_case(excluded)) {
        return true;
    }

    // User metadata is stored without the x-amz-meta- prefix by extract_metadata_from_mime.
    starts_with_ignore_ascii_case(key, X_AMZ_PREFIX)
}

pub(crate) fn filter_object_metadata(metadata: &HashMap<String, String>) -> Option<HashMap<String, String>> {
    // HTTP headers that should NOT be returned in the Metadata field.
    // These headers are returned as separate response headers, not user metadata.
    const EXCLUDED_HEADERS: &[&str] = &[
        "content-type",
        "content-disposition",
        "content-encoding",
        "content-language",
        "cache-control",
        "expires",
        "etag",
        "x-amz-storage-class",
        "x-amz-tagging",
        "x-amz-replication-status",
        "x-amz-server-side-encryption",
        "x-amz-server-side-encryption-customer-algorithm",
        "x-amz-server-side-encryption-customer-key-md5",
        "x-amz-server-side-encryption-aws-kms-key-id",
    ];

    let mut filtered_metadata = None;
    for (k, v) in metadata {
        if starts_with_ignore_ascii_case(k, "x-amz-meta-internal-")
            || k.eq_ignore_ascii_case(AMZ_META_UNENCRYPTED_CONTENT_MD5)
            || k.eq_ignore_ascii_case(AMZ_META_UNENCRYPTED_CONTENT_LENGTH)
        {
            continue;
        }

        if let Some(key) = USER_METADATA_PREFIXES.iter().find_map(|prefix| {
            k.get(..prefix.len())
                .filter(|head| head.eq_ignore_ascii_case(prefix))
                .map(|_| &k[prefix.len()..])
        }) {
            if !key.is_empty() {
                filtered_metadata
                    .get_or_insert_with(HashMap::new)
                    .insert(key.to_owned(), v.clone());
            }
            continue;
        }

        if should_skip_object_metadata_key(k, v, EXCLUDED_HEADERS) {
            continue;
        }

        // Include user-defined metadata (keys like "meta1", "custom-key", etc.)
        filtered_metadata
            .get_or_insert_with(HashMap::new)
            .insert(k.clone(), v.clone());
    }

    filtered_metadata
}

/// Detects content type from object name based on file extension.
pub(crate) fn detect_content_type_from_object_name(object_name: &str) -> String {
    let lower_name = object_name.to_lowercase();

    // Check for Parquet files specifically
    if lower_name.ends_with(".parquet") {
        return "application/vnd.apache.parquet".to_owned();
    }

    // Special handling for other data formats that mime_guess doesn't know
    if lower_name.ends_with(".avro") {
        return "application/avro".to_owned();
    }
    if lower_name.ends_with(".orc") {
        return "application/orc".to_owned();
    }
    if lower_name.ends_with(".feather") {
        return "application/feather".to_owned();
    }
    if lower_name.ends_with(".arrow") {
        return "application/arrow".to_owned();
    }

    // Use mime_guess for standard file types
    mime_guess::from_path(object_name).first_or_octet_stream().to_string()
}

/// List of supported headers.
static SUPPORTED_HEADERS: LazyLock<Vec<&'static str>> = LazyLock::new(|| {
    vec![
        "content-type",
        "cache-control",
        "content-language",
        "content-encoding",
        "content-disposition",
        "x-amz-storage-class",
        "x-amz-tagging",
        "expires",
        "x-amz-replication-status",
        // Object Lock headers - required for S3 Object Lock functionality
        AMZ_OBJECT_LOCK_MODE_LOWER,
        AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER,
        AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER,
    ]
});

/// Parse copy source range string in format "bytes=start-end"
pub fn parse_copy_source_range(range_str: &str) -> S3Result<HTTPRangeSpec> {
    if !range_str.starts_with("bytes=") {
        return Err(s3_error!(InvalidArgument, "Invalid range format"));
    }

    let range_part = &range_str[6..]; // Remove "bytes=" prefix

    if let Some(dash_pos) = range_part.find('-') {
        let start_str = &range_part[..dash_pos];
        let end_str = &range_part[dash_pos + 1..];

        if start_str.is_empty() && end_str.is_empty() {
            return Err(s3_error!(InvalidArgument, "Invalid range format"));
        }

        if start_str.is_empty() {
            // Suffix range: bytes=-500 (last 500 bytes)
            let length = end_str
                .parse::<i64>()
                .map_err(|_| s3_error!(InvalidArgument, "Invalid range format"))?;

            if length <= 0 {
                return Err(s3_error!(InvalidArgument, "Invalid range format"));
            }

            let start = length
                .checked_neg()
                .ok_or_else(|| s3_error!(InvalidArgument, "Invalid range format"))?;

            Ok(HTTPRangeSpec {
                is_suffix_length: true,
                start,
                end: -1,
            })
        } else {
            let start = start_str
                .parse::<i64>()
                .map_err(|_| s3_error!(InvalidArgument, "Invalid range format"))?;

            let end = if end_str.is_empty() {
                -1 // Open-ended range: bytes=500-
            } else {
                end_str
                    .parse::<i64>()
                    .map_err(|_| s3_error!(InvalidArgument, "Invalid range format"))?
            };

            if start < 0 || (end != -1 && end < start) {
                return Err(s3_error!(InvalidArgument, "Invalid range format"));
            }

            Ok(HTTPRangeSpec {
                is_suffix_length: false,
                start,
                end,
            })
        }
    } else {
        Err(s3_error!(InvalidArgument, "Invalid range format"))
    }
}
pub(crate) fn get_content_sha256_with_query(headers: &HeaderMap<HeaderValue>, query: Option<&str>) -> Option<String> {
    match get_request_auth_type_with_query(headers, query) {
        AuthType::Presigned | AuthType::Signed => {
            if skip_content_sha256_cksum_with_query(headers, query) {
                None
            } else {
                Some(get_content_sha256_cksum_with_query(headers, query, ServiceType::S3))
            }
        }
        _ => None,
    }
}
fn skip_content_sha256_cksum_with_query(headers: &HeaderMap<HeaderValue>, query: Option<&str>) -> bool {
    let include_query_values = matches!(get_request_auth_type_with_query(headers, query), AuthType::Presigned);
    let content_sha256 = get_content_sha256_value(headers, query, include_query_values);

    // Skip if no checksum value was set in header/query for query-presigned requests.
    let Some(header_value) = content_sha256 else {
        return true;
    };

    let value = header_value;

    // If x-amz-content-sha256 is set and the value is not
    // 'UNSIGNED-PAYLOAD' we should validate the content sha256.
    match value {
        v if v == UNSIGNED_PAYLOAD || v == UNSIGNED_PAYLOAD_TRAILER => true,
        v if v == EMPTY_STRING_SHA256_HASH => {
            // some broken clients set empty-sha256
            // with > 0 content-length in the body,
            // we should skip such clients and allow
            // blindly such insecure clients only if
            // S3 strict compatibility is disabled.

            // We return true only in situations when
            // deployment has asked RustFS to allow for
            // such broken clients and content-length > 0.
            // For now, we'll assume strict compatibility is disabled
            // In a real implementation, you would check a global config
            if let Some(content_length) = headers.get("content-length")
                && let Ok(length_str) = content_length.to_str()
                && let Ok(length) = length_str.parse::<i64>()
            {
                return length > 0; // && !global_server_ctxt.strict_s3_compat
            }
            false
        }
        _ => false,
    }
}

/// Returns SHA256 for calculating canonical-request.
fn get_content_sha256_cksum_with_query(
    headers: &HeaderMap<HeaderValue>,
    query: Option<&str>,
    service_type: ServiceType,
) -> String {
    if service_type == ServiceType::STS {
        // For STS requests, we would need to read the body and calculate SHA256
        // This is a simplified implementation - in practice you'd need access to the request body
        // For now, we'll return a placeholder
        return "sts-body-sha256-placeholder".to_string();
    }

    let (default_sha256_cksum, content_sha256) = if is_request_presigned_signature_v4_with_query(headers, query) {
        // For a presigned request we look at the query param for sha256.
        // X-Amz-Content-Sha256, if not set in presigned requests, checksum
        // will default to 'UNSIGNED-PAYLOAD'.
        (UNSIGNED_PAYLOAD.to_string(), get_content_sha256_value(headers, query, true))
    } else {
        // X-Amz-Content-Sha256, if not set in signed requests, checksum
        // will default to sha256([]byte("")).
        (
            EMPTY_STRING_SHA256_HASH.to_string(),
            headers
                .get(AMZ_CONTENT_SHA256)
                .and_then(|v| v.to_str().ok().map(str::to_owned)),
        )
    };

    // We found 'X-Amz-Content-Sha256' return the captured value.
    if let Some(header_value) = content_sha256 {
        return header_value;
    }

    // We couldn't find 'X-Amz-Content-Sha256'.
    default_sha256_cksum
}

fn get_content_sha256_value(
    headers: &HeaderMap<HeaderValue>,
    query: Option<&str>,
    include_query_for_presigned: bool,
) -> Option<String> {
    if include_query_for_presigned && is_request_presigned_signature_v4_with_query(headers, query) {
        return query
            .and_then(|q| get_query_param(q, "x-amz-content-sha256"))
            .or_else(|| headers.get(AMZ_CONTENT_SHA256).and_then(|v| v.to_str().ok()))
            .map(str::to_owned);
    }

    headers
        .get(AMZ_CONTENT_SHA256)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
}
#[cfg(test)]
#[allow(unused_imports)]
mod tests {
    use proptest::prelude::*;
    use temp_env;

    use super::super::StorageError;
    use super::{
        ENV_REJECT_ARCHIVE_CONTENT_ENCODING, ReplicationStatusType, SUPPORTED_HEADERS, copy_dst_opts, copy_src_opts, del_opts,
        del_opts_with_versioning, detect_content_type_from_object_name, extract_metadata, extract_metadata_from_mime,
        extract_metadata_from_mime_with_object_name, filter_object_metadata, get_complete_multipart_upload_opts,
        get_complete_multipart_upload_opts_with_replication_authorization, get_default_opts, get_opts,
        has_replication_retention_update, namespace_reserved_user_metadata, parse_copy_source_range, put_opts,
        put_opts_from_headers, put_opts_from_headers_with_replication_authorization, put_opts_with_replication_authorization,
        validate_archive_content_encoding,
    };
    use http::{HeaderMap, HeaderValue};
    use rustfs_utils::http::{
        AMZ_BUCKET_REPLICATION_STATUS, AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER, AMZ_OBJECT_LOCK_MODE_LOWER,
        AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER, SUFFIX_FORCE_DELETE, SUFFIX_SOURCE_DELETEMARKER, SUFFIX_SOURCE_ETAG,
        SUFFIX_SOURCE_MTIME, SUFFIX_SOURCE_REPLICATION_REQUEST, SUFFIX_SOURCE_REPLICATION_RETENTION_TIMESTAMP,
        SUFFIX_SOURCE_VERSION_ID, insert_header,
    };
    use s3s::S3ErrorCode;
    use s3s::dto::{BucketVersioningStatus, ExcludedPrefix, VersioningConfiguration};
    use std::collections::HashMap;
    use uuid::Uuid;

    fn create_test_headers() -> HeaderMap<HeaderValue> {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/json"));
        headers.insert("x-amz-meta-custom", HeaderValue::from_static("custom-value"));
        headers.insert("x-rustfs-meta-internal", HeaderValue::from_static("internal-value"));
        headers.insert("cache-control", HeaderValue::from_static("no-cache"));
        headers
    }

    fn create_test_metadata() -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        metadata.insert("key1".to_string(), "value1".to_string());
        metadata.insert("key2".to_string(), "value2".to_string());
        metadata
    }

    #[tokio::test]
    async fn test_del_opts_basic() {
        let headers = create_test_headers();
        let metadata = create_test_metadata();

        let result = del_opts("test-bucket", "test-object", None, &headers, metadata).await;

        assert!(result.is_ok());
        let opts = result.unwrap();
        assert!(!opts.user_defined.is_empty());
        assert_eq!(opts.version_id, None);
    }

    #[tokio::test]
    async fn test_del_opts_with_directory_object() {
        let headers = create_test_headers();

        let result = del_opts("test-bucket", "test-dir/", None, &headers, HashMap::new()).await;

        assert!(result.is_ok());
        let opts = result.unwrap();
        assert_eq!(opts.version_id, Some(Uuid::nil().to_string()));
    }

    #[tokio::test]
    async fn test_del_opts_preserves_explicit_null_directory_version() {
        let headers = create_test_headers();

        let opts = del_opts("test-bucket", "test-dir/", Some("null".to_string()), &headers, HashMap::new())
            .await
            .expect("explicit null directory version should be accepted");

        assert_eq!(opts.version_id, Some(Uuid::nil().to_string()));
    }

    #[tokio::test]
    async fn test_del_opts_with_valid_version_id() {
        let headers = create_test_headers();
        let valid_uuid = Uuid::new_v4().to_string();

        let result = del_opts("test-bucket", "test-object", Some(valid_uuid.clone()), &headers, HashMap::new()).await;

        // This test may fail if versioning is not enabled for the bucket
        // In a real test environment, you would mock BucketVersioningSys
        match result {
            Ok(opts) => {
                assert_eq!(opts.version_id, Some(valid_uuid));
            }
            Err(_) => {
                // Expected if versioning is not enabled
            }
        }
    }

    #[test]
    fn test_del_opts_only_trusts_replication_headers_after_authorization() {
        let source_version_id = Uuid::new_v4().to_string();
        let source_mtime = "2024-05-20T10:30:00+08:00";
        let mut headers = HeaderMap::new();
        insert_header(&mut headers, SUFFIX_SOURCE_VERSION_ID, source_version_id.clone());
        insert_header(&mut headers, SUFFIX_SOURCE_REPLICATION_REQUEST, "true");
        insert_header(&mut headers, SUFFIX_SOURCE_MTIME, source_mtime);
        insert_header(&mut headers, SUFFIX_SOURCE_DELETEMARKER, "true");
        headers.insert(
            AMZ_BUCKET_REPLICATION_STATUS,
            HeaderValue::from_static(ReplicationStatusType::Replica.as_str()),
        );

        let untrusted = del_opts_with_versioning(
            "test-bucket",
            "test-object",
            None,
            &headers,
            HashMap::new(),
            &VersioningConfiguration::default(),
            false,
        )
        .expect("ordinary delete options should ignore internal replication headers");

        assert_eq!(untrusted.version_id, None);
        assert!(!untrusted.replication_request);
        assert!(untrusted.mod_time.is_none());
        assert!(!untrusted.delete_marker);
        assert!(untrusted.delete_replication.is_none());

        let trusted = del_opts_with_versioning(
            "test-bucket",
            "test-object",
            None,
            &headers,
            HashMap::new(),
            &VersioningConfiguration::default(),
            true,
        )
        .expect("authorized replication delete options should accept internal headers");

        assert_eq!(trusted.version_id.as_deref(), Some(source_version_id.as_str()));
        assert!(trusted.replication_request);
        assert!(trusted.mod_time.is_some());
        assert!(trusted.delete_marker);
        assert_eq!(trusted.delete_marker_replication_status(), ReplicationStatusType::Replica);
    }

    #[test]
    fn test_del_opts_treats_excluded_prefix_as_unversioned() {
        let versioning = VersioningConfiguration {
            status: Some(BucketVersioningStatus::from_static(BucketVersioningStatus::ENABLED)),
            excluded_prefixes: Some(vec![ExcludedPrefix {
                prefix: Some("archive/".to_string()),
            }]),
            ..Default::default()
        };

        let excluded = del_opts_with_versioning(
            "test-bucket",
            "archive/object",
            None,
            &HeaderMap::new(),
            HashMap::new(),
            &versioning,
            false,
        )
        .expect("excluded-prefix delete options should be derived");
        assert!(!excluded.versioned);
        assert!(!excluded.version_suspended);

        let included =
            del_opts_with_versioning("test-bucket", "live/object", None, &HeaderMap::new(), HashMap::new(), &versioning, false)
                .expect("included-prefix delete options should be derived");
        assert!(included.versioned);
        assert!(!included.version_suspended);
    }

    #[tokio::test]
    async fn test_del_opts_with_invalid_version_id() {
        let headers = create_test_headers();
        let invalid_uuid = "invalid-uuid".to_string();

        let result = del_opts("test-bucket", "test-object", Some(invalid_uuid), &headers, HashMap::new()).await;

        assert!(result.is_err());
        if let Err(err) = result {
            match err {
                StorageError::InvalidVersionID(bucket, object, version) => {
                    assert_eq!(bucket, "test-bucket");
                    assert_eq!(object, "test-object");
                    assert_eq!(version, "invalid-uuid");
                }
                _ => panic!("Expected InvalidVersionID error"),
            }
        }
    }

    #[tokio::test]
    async fn test_del_opts_with_delete_prefix() {
        let mut headers = create_test_headers();
        let metadata = create_test_metadata();

        // Test without force-delete header - should default to false
        let result = del_opts("test-bucket", "test-object", None, &headers, metadata.clone()).await;
        assert!(result.is_ok());
        let opts = result.unwrap();
        assert!(!opts.delete_prefix);

        // Test with RUSTFS_FORCE_DELETE header set to "true"
        insert_header(&mut headers, SUFFIX_FORCE_DELETE, "true");
        let result = del_opts("test-bucket", "test-object", None, &headers, metadata.clone()).await;
        assert!(result.is_ok());
        let opts = result.unwrap();
        assert!(opts.delete_prefix);

        // Test with RUSTFS_FORCE_DELETE header set to "false"
        insert_header(&mut headers, SUFFIX_FORCE_DELETE, "false");
        let result = del_opts("test-bucket", "test-object", None, &headers, metadata.clone()).await;
        assert!(result.is_ok());
        let opts = result.unwrap();
        assert!(!opts.delete_prefix);

        // Test with RUSTFS_FORCE_DELETE header set to other value
        insert_header(&mut headers, SUFFIX_FORCE_DELETE, "maybe");
        let result = del_opts("test-bucket", "test-object", None, &headers, metadata).await;
        assert!(result.is_ok());
        let opts = result.unwrap();
        assert!(!opts.delete_prefix);
    }

    #[tokio::test]
    async fn test_del_opts_with_null_version_id() {
        let headers = create_test_headers();
        let metadata = create_test_metadata();
        let result = del_opts("test-bucket", "test-object", Some("null".to_string()), &headers, metadata.clone()).await;
        assert!(result.is_ok());
        let result = del_opts("test-bucket", "test-object", Some("NULL".to_string()), &headers, metadata.clone()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_ops_with_null_version_id() {
        let headers = create_test_headers();
        let result = get_opts("test-bucket", "test-object", Some("null".to_string()), None, &headers).await;
        assert!(result.is_ok());
        let opts = result.unwrap();
        assert_eq!(opts.version_id, Some(Uuid::nil().to_string()));
        let result = get_opts("test-bucket", "test-object", Some("NULL".to_string()), None, &headers).await;
        assert!(result.is_ok());
        let opts = result.unwrap();
        assert_eq!(opts.version_id, Some(Uuid::nil().to_string()));
    }

    #[tokio::test]
    async fn test_get_opts_basic() {
        let headers = create_test_headers();

        let result = get_opts("test-bucket", "test-object", None, None, &headers).await;

        assert!(result.is_ok());
        let opts = result.unwrap();
        assert_eq!(opts.part_number, None);
        assert_eq!(opts.version_id, None);
    }

    #[tokio::test]
    async fn test_get_opts_ignores_empty_conditional_headers() {
        let mut headers = create_test_headers();
        headers.insert(http::header::IF_MATCH, HeaderValue::from_static(""));
        headers.insert(http::header::IF_NONE_MATCH, HeaderValue::from_static(" "));

        let result = get_opts("test-bucket", "test-object", None, None, &headers).await;

        assert!(result.is_ok());
        assert!(result.unwrap().http_preconditions.is_none());
    }

    #[tokio::test]
    async fn test_get_opts_keeps_non_empty_conditional_headers() {
        let mut headers = create_test_headers();
        headers.insert(http::header::IF_MATCH, HeaderValue::from_static(" \"etag-a\" "));
        headers.insert(http::header::IF_NONE_MATCH, HeaderValue::from_static("\"etag-b\""));

        let result = get_opts("test-bucket", "test-object", None, None, &headers).await;

        assert!(result.is_ok());
        let preconditions = result.unwrap().http_preconditions.expect("conditional headers");
        assert_eq!(preconditions.if_match.as_deref(), Some("\"etag-a\""));
        assert_eq!(preconditions.if_none_match.as_deref(), Some("\"etag-b\""));
    }

    #[tokio::test]
    async fn test_get_opts_with_part_number() {
        let headers = create_test_headers();

        let result = get_opts("test-bucket", "test-object", None, Some(5), &headers).await;

        assert!(result.is_ok());
        let opts = result.unwrap();
        assert_eq!(opts.part_number, Some(5));
    }

    #[tokio::test]
    async fn test_get_opts_with_directory_object() {
        let headers = create_test_headers();

        let result = get_opts("test-bucket", "test-dir/", None, None, &headers).await;

        assert!(result.is_ok());
        let opts = result.unwrap();
        assert_eq!(opts.version_id, Some(Uuid::nil().to_string()));
    }

    #[tokio::test]
    async fn test_get_opts_with_invalid_version_id() {
        let headers = create_test_headers();
        let invalid_uuid = "invalid-uuid".to_string();

        let result = get_opts("test-bucket", "test-object", Some(invalid_uuid), None, &headers).await;

        assert!(result.is_err());
        if let Err(err) = result {
            match err {
                StorageError::InvalidVersionID(bucket, object, version) => {
                    assert_eq!(bucket, "test-bucket");
                    assert_eq!(object, "test-object");
                    assert_eq!(version, "invalid-uuid");
                }
                _ => panic!("Expected InvalidVersionID error"),
            }
        }
    }

    #[tokio::test]
    async fn test_put_opts_basic() {
        let headers = create_test_headers();
        let metadata = create_test_metadata();

        let result = put_opts("test-bucket", "test-object", None, &headers, metadata).await;

        assert!(result.is_ok());
        let opts = result.unwrap();
        assert!(!opts.user_defined.is_empty());
        assert_eq!(opts.version_id, None);
    }

    #[tokio::test]
    async fn test_put_opts_with_directory_object() {
        let headers = create_test_headers();

        let result = put_opts("test-bucket", "test-dir/", None, &headers, HashMap::new()).await;

        assert!(result.is_ok());
        let opts = result.unwrap();
        assert_eq!(opts.version_id, Some(Uuid::nil().to_string()));
    }

    #[tokio::test]
    async fn test_put_opts_with_invalid_version_id() {
        let headers = create_test_headers();
        let invalid_uuid = "invalid-uuid".to_string();

        let result = put_opts("test-bucket", "test-object", Some(invalid_uuid), &headers, HashMap::new()).await;

        assert!(result.is_err());
        if let Err(err) = result {
            match err {
                StorageError::InvalidVersionID(bucket, object, version) => {
                    assert_eq!(bucket, "test-bucket");
                    assert_eq!(object, "test-object");
                    assert_eq!(version, "invalid-uuid");
                }
                _ => panic!("Expected InvalidVersionID error"),
            }
        }
    }

    #[tokio::test]
    async fn test_put_opts_normalizes_null_version_id() {
        // MinIO-compatible replication senders (including RustFS itself since
        // the P0-5 fix) address the null version as the literal "null" in the
        // versionId query; the PUT / CreateMultipartUpload receive path must
        // normalize it to the internal nil-UUID representation, exactly like
        // get_opts / del_opts already do.
        let headers = create_test_headers();

        let opts = put_opts("test-bucket", "test-object", Some("null".to_string()), &headers, HashMap::new())
            .await
            .expect("PUT with versionId=null must be accepted as the null version");

        assert_eq!(opts.version_id, Some(Uuid::nil().to_string()));
    }

    #[tokio::test]
    async fn test_copy_dst_opts() {
        let headers = create_test_headers();
        let metadata = create_test_metadata();

        let result = copy_dst_opts("test-bucket", "test-object", None, &headers, metadata).await;

        assert!(result.is_ok());
        let opts = result.unwrap();
        assert!(!opts.user_defined.is_empty());
    }

    #[test]
    fn test_copy_src_opts() {
        let headers = create_test_headers();

        let result = copy_src_opts("test-bucket", "test-object", &headers);

        assert!(result.is_ok());
        let opts = result.unwrap();
        assert!(opts.user_defined.is_empty());
    }

    #[test]
    fn test_put_opts_from_headers() {
        let headers = create_test_headers();
        let metadata = create_test_metadata();

        let result = put_opts_from_headers(&headers, metadata);

        assert!(result.is_ok());
        let opts = result.unwrap();
        assert!(!opts.user_defined.is_empty());
        let user_defined = opts.user_defined;
        assert_eq!(user_defined.get("key1"), Some(&"value1".to_string()));
        assert_eq!(user_defined.get("key2"), Some(&"value2".to_string()));
    }

    #[test]
    fn test_put_opts_from_headers_ignores_replication_request_without_authorization() {
        let mut headers = HeaderMap::new();
        insert_header(&mut headers, SUFFIX_SOURCE_REPLICATION_REQUEST, "true");
        let valid_mtime = "2024-05-20T10:30:00+08:00";
        insert_header(&mut headers, SUFFIX_SOURCE_MTIME, valid_mtime);
        insert_header(&mut headers, SUFFIX_SOURCE_ETAG, "0123456789abcdef0123456789abcdef");

        let metadata = HashMap::new();

        let result = put_opts_from_headers(&headers, metadata);

        assert!(result.is_ok());
        let opts = result.unwrap();

        assert!(!opts.replication_request);
        assert!(opts.mod_time.is_none());
        assert!(opts.preserve_etag.is_none());
    }

    #[test]
    fn test_replication_retention_update_requires_authorization() {
        let mut headers = HeaderMap::new();
        insert_header(&mut headers, SUFFIX_SOURCE_REPLICATION_REQUEST, "true");
        insert_header(&mut headers, SUFFIX_SOURCE_REPLICATION_RETENTION_TIMESTAMP, "2026-01-01T00:00:00Z");

        assert!(!has_replication_retention_update(&headers, false));
        assert!(has_replication_retention_update(&headers, true));

        let mut missing_request = HeaderMap::new();
        insert_header(
            &mut missing_request,
            SUFFIX_SOURCE_REPLICATION_RETENTION_TIMESTAMP,
            "2026-01-01T00:00:00Z",
        );
        assert!(!has_replication_retention_update(&missing_request, true));
    }

    #[test]
    fn test_put_opts_from_headers_gates_ssec_passthrough_on_authorization() {
        use rustfs_utils::http::object_encryption_keys::{
            INTERNAL_ENCRYPTION_IV_HEADER, REPLICATION_ENCRYPTION_IV_HEADER, REPLICATION_SSEC_ALGORITHM_HEADER,
        };

        let mut headers = HeaderMap::new();
        insert_header(&mut headers, SUFFIX_SOURCE_REPLICATION_REQUEST, "true");
        headers.insert(
            REPLICATION_SSEC_ALGORITHM_HEADER.parse::<http::HeaderName>().unwrap(),
            HeaderValue::from_static("AES256"),
        );
        headers.insert(
            REPLICATION_ENCRYPTION_IV_HEADER.parse::<http::HeaderName>().unwrap(),
            HeaderValue::from_static("iv-value"),
        );

        // Unauthorized: the transport headers must be inert — no restored
        // encryption metadata, no ciphertext-passthrough flag.
        let untrusted = put_opts_from_headers(&headers, HashMap::new()).expect("ordinary PUT options should be created");
        assert!(!untrusted.preserve_ciphertext);
        assert!(!untrusted.user_defined.contains_key(INTERNAL_ENCRYPTION_IV_HEADER));
        assert!(
            !untrusted
                .user_defined
                .contains_key("x-amz-server-side-encryption-customer-algorithm")
        );

        // Authorized: the stored keys are restored and the write path is told
        // the body is already ciphertext.
        let trusted = put_opts_from_headers_with_replication_authorization(&headers, HashMap::new(), true)
            .expect("authorized replication request should parse");
        assert!(trusted.preserve_ciphertext);
        assert_eq!(
            trusted.user_defined.get(INTERNAL_ENCRYPTION_IV_HEADER).map(String::as_str),
            Some("iv-value")
        );
        assert_eq!(
            trusted
                .user_defined
                .get("x-amz-server-side-encryption-customer-algorithm")
                .map(String::as_str),
            Some("AES256")
        );
        assert_eq!(
            trusted.user_defined.get("x-amz-server-side-encryption").map(String::as_str),
            Some("AES256")
        );
    }

    #[test]
    fn test_put_opts_from_headers_accepts_replication_request_after_authorization() {
        let mut headers = HeaderMap::new();
        insert_header(&mut headers, SUFFIX_SOURCE_REPLICATION_REQUEST, "true");
        let valid_mtime = "2024-05-20T10:30:00+08:00";
        insert_header(&mut headers, SUFFIX_SOURCE_MTIME, valid_mtime);
        insert_header(&mut headers, SUFFIX_SOURCE_ETAG, "\"0123456789abcdef0123456789abcdef-3\"");

        let opts = put_opts_from_headers_with_replication_authorization(&headers, HashMap::new(), true)
            .expect("authorized replication request should parse");

        assert!(opts.replication_request);
        // The replica keeps the source ETag verbatim (quotes trimmed) so the
        // replication HEAD comparison converges after target-side re-encryption.
        assert_eq!(opts.preserve_etag.as_deref(), Some("0123456789abcdef0123456789abcdef-3"));

        let expected_mtime = time::OffsetDateTime::parse(valid_mtime, &time::format_description::well_known::Rfc3339).unwrap();
        assert_eq!(opts.mod_time, Some(expected_mtime));

        let mut headers_invalid_mtime = HeaderMap::new();
        insert_header(&mut headers_invalid_mtime, SUFFIX_SOURCE_REPLICATION_REQUEST, "true");
        insert_header(&mut headers_invalid_mtime, SUFFIX_SOURCE_MTIME, "invalid-time");
        let result_invalid = put_opts_from_headers_with_replication_authorization(&headers_invalid_mtime, HashMap::new(), true);
        assert!(result_invalid.is_ok());
        let opts_invalid = result_invalid.unwrap();
        assert!(opts_invalid.replication_request);
        assert!(opts_invalid.mod_time.is_none());
    }

    /// A client PUT must not materialize the replication transport names as
    /// bare stored user-metadata keys: the outbound replication header
    /// builder forwards user metadata verbatim on a server-authorized
    /// request, so a bare `x-rustfs-source-replication-*-timestamp` key would
    /// deliver an attacker-chosen value into the replica's trusted internal
    /// LWW state (for a tagless object nothing later overwrites it).
    #[test]
    fn test_replication_transport_names_cannot_be_forged_via_user_metadata() {
        let mut headers = HeaderMap::new();
        for name in [
            "x-amz-meta-x-rustfs-source-replication-tagging-timestamp",
            "x-amz-meta-x-minio-source-replication-legalhold-timestamp",
            "x-rustfs-meta-x-rustfs-source-replication-retention-timestamp",
            "x-amz-meta-x-rustfs-source-mtime",
        ] {
            headers.insert(
                http::header::HeaderName::from_static(name),
                HeaderValue::from_static("2026-01-02T03:04:05Z"),
            );
        }

        let metadata = extract_metadata(&headers);

        for forged in [
            "x-rustfs-source-replication-tagging-timestamp",
            "x-minio-source-replication-legalhold-timestamp",
            "x-rustfs-source-replication-retention-timestamp",
            "x-rustfs-source-mtime",
        ] {
            assert!(
                !metadata.contains_key(forged),
                "{forged} must not be storable as a bare user-metadata key"
            );
        }
        // The values survive, namespaced back under the user-metadata prefix.
        assert_eq!(
            metadata
                .get("x-amz-meta-x-rustfs-source-replication-tagging-timestamp")
                .map(String::as_str),
            Some("2026-01-02T03:04:05Z")
        );
    }

    #[test]
    fn test_put_opts_from_headers_gates_replication_timestamp_persistence_on_authorization() {
        // Sender-side LWW state (replication_target_boundary.rs) is read back
        // from these internal metadata keys, so an authorized replication PUT
        // must persist the inbound timestamp headers; an unauthorized client
        // must not be able to forge them.
        let mut headers = HeaderMap::new();
        insert_header(&mut headers, SUFFIX_SOURCE_REPLICATION_REQUEST, "true");
        insert_header(&mut headers, "source-replication-tagging-timestamp", "2026-01-02T03:04:05Z");
        insert_header(&mut headers, "source-replication-retention-timestamp", "2026-01-02T03:04:06Z");
        insert_header(&mut headers, "source-replication-legalhold-timestamp", "2026-01-02T03:04:07Z");

        let untrusted = put_opts_from_headers(&headers, HashMap::new()).expect("ordinary PUT options should be created");
        for suffix in [
            "tagging-timestamp",
            "objectlock-retention-timestamp",
            "objectlock-legalhold-timestamp",
        ] {
            assert!(
                rustfs_utils::http::get_str(&untrusted.user_defined, suffix).is_none(),
                "unauthorized clients must not persist the {suffix} internal key"
            );
        }
        assert!(untrusted.replication_tagging_timestamp.is_none());
        assert!(untrusted.replication_retention_timestamp.is_none());
        assert!(untrusted.replication_legalhold_timestamp.is_none());

        let trusted = put_opts_from_headers_with_replication_authorization(&headers, HashMap::new(), true)
            .expect("authorized replication request should parse");
        let parse = |value: &str| {
            time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339).expect("valid RFC3339")
        };
        assert_eq!(trusted.replication_tagging_timestamp, Some(parse("2026-01-02T03:04:05Z")));
        assert_eq!(trusted.replication_retention_timestamp, Some(parse("2026-01-02T03:04:06Z")));
        assert_eq!(trusted.replication_legalhold_timestamp, Some(parse("2026-01-02T03:04:07Z")));
        for (suffix, expected) in [
            ("tagging-timestamp", "2026-01-02T03:04:05Z"),
            ("objectlock-retention-timestamp", "2026-01-02T03:04:06Z"),
            ("objectlock-legalhold-timestamp", "2026-01-02T03:04:07Z"),
        ] {
            assert_eq!(
                rustfs_utils::http::get_str(&trusted.user_defined, suffix).as_deref(),
                Some(expected),
                "authorized replication must persist the {suffix} internal key"
            );
        }
    }

    #[test]
    fn test_complete_multipart_opts_persist_replication_timestamps_when_authorized() {
        let mut headers = HeaderMap::new();
        insert_header(&mut headers, SUFFIX_SOURCE_REPLICATION_REQUEST, "true");
        insert_header(&mut headers, "replication-actual-object-size", "1");
        insert_header(&mut headers, "source-replication-tagging-timestamp", "2026-01-02T03:04:05Z");
        insert_header(&mut headers, "source-replication-retention-timestamp", "2026-01-02T03:04:06Z");
        insert_header(&mut headers, "source-replication-legalhold-timestamp", "2026-01-02T03:04:07Z");

        let untrusted = get_complete_multipart_upload_opts(&headers).expect("ordinary multipart options should be created");
        for suffix in [
            "tagging-timestamp",
            "objectlock-retention-timestamp",
            "objectlock-legalhold-timestamp",
        ] {
            assert!(
                rustfs_utils::http::get_str(&untrusted.user_defined, suffix).is_none(),
                "unauthorized multipart completes must not persist the {suffix} internal key"
            );
        }

        let trusted = get_complete_multipart_upload_opts_with_replication_authorization(&headers, true)
            .expect("authorized multipart complete should parse");
        for (suffix, expected) in [
            ("tagging-timestamp", "2026-01-02T03:04:05Z"),
            ("objectlock-retention-timestamp", "2026-01-02T03:04:06Z"),
            ("objectlock-legalhold-timestamp", "2026-01-02T03:04:07Z"),
        ] {
            assert_eq!(
                rustfs_utils::http::get_str(&trusted.user_defined, suffix).as_deref(),
                Some(expected),
                "authorized multipart completes must persist the {suffix} internal key"
            );
        }
        assert!(trusted.replication_tagging_timestamp.is_some());
        assert!(trusted.replication_retention_timestamp.is_some());
        assert!(trusted.replication_legalhold_timestamp.is_some());
    }

    #[test]
    fn test_put_opts_from_headers_with_replica_status() {
        let mut headers = HeaderMap::new();
        headers.insert(
            AMZ_BUCKET_REPLICATION_STATUS,
            HeaderValue::from_static(ReplicationStatusType::Replica.as_str()),
        );

        let opts = put_opts_from_headers(&headers, HashMap::new()).expect("replica status header should be ignored");

        assert_eq!(opts.delete_marker_replication_status(), ReplicationStatusType::Empty);
        assert!(
            !opts
                .user_defined
                .keys()
                .any(|key| key.eq_ignore_ascii_case(AMZ_BUCKET_REPLICATION_STATUS)),
            "an unauthorized client must not forge a persisted REPLICA status"
        );

        let authorized = put_opts_from_headers_with_replication_authorization(&headers, HashMap::new(), true)
            .expect("authorized replica status header should parse");
        assert_eq!(authorized.delete_marker_replication_status(), ReplicationStatusType::Replica);
        // The status must reach the object metadata: the read side derives
        // ObjectInfo::replication_status from this key and the scanner's
        // anti-cascade guard consumes it.
        assert_eq!(
            authorized.user_defined.get(AMZ_BUCKET_REPLICATION_STATUS).map(String::as_str),
            Some(ReplicationStatusType::Replica.as_str()),
            "authorized inbound replication must persist REPLICA into object metadata"
        );
    }

    #[tokio::test]
    async fn test_put_opts_only_accepts_replication_source_headers_after_authorization() {
        let source_version_id = Uuid::new_v4().to_string();
        let mut headers = HeaderMap::new();
        insert_header(&mut headers, SUFFIX_SOURCE_VERSION_ID, source_version_id.clone());
        insert_header(&mut headers, SUFFIX_SOURCE_REPLICATION_REQUEST, "true");
        headers.insert(
            AMZ_BUCKET_REPLICATION_STATUS,
            HeaderValue::from_static(ReplicationStatusType::Replica.as_str()),
        );

        let untrusted = put_opts("test-bucket", "test-object", None, &headers, HashMap::new())
            .await
            .expect("ordinary PUT options should be created");
        assert_eq!(untrusted.version_id, None);
        assert!(!untrusted.replication_request);
        assert_eq!(untrusted.delete_marker_replication_status(), ReplicationStatusType::Empty);

        let trusted = put_opts_with_replication_authorization("test-bucket", "test-object", None, &headers, HashMap::new(), true)
            .await
            .expect("authorized replication PUT options should be created");
        assert_eq!(trusted.version_id.as_deref(), Some(source_version_id.as_str()));
        assert!(trusted.replication_request);
        assert_eq!(trusted.delete_marker_replication_status(), ReplicationStatusType::Replica);
    }

    #[test]
    fn test_complete_multipart_opts_with_replica_status() {
        let mut headers = HeaderMap::new();
        headers.insert(
            AMZ_BUCKET_REPLICATION_STATUS,
            HeaderValue::from_static(ReplicationStatusType::Replica.as_str()),
        );

        let opts = get_complete_multipart_upload_opts(&headers).expect("replica status header should be ignored");

        assert_eq!(opts.delete_marker_replication_status(), ReplicationStatusType::Empty);
        assert!(
            !opts
                .user_defined
                .keys()
                .any(|key| key.eq_ignore_ascii_case(AMZ_BUCKET_REPLICATION_STATUS)),
            "an unauthorized client must not forge a persisted REPLICA status"
        );

        let authorized = get_complete_multipart_upload_opts_with_replication_authorization(&headers, true)
            .expect("authorized replica status header should parse");
        assert_eq!(authorized.delete_marker_replication_status(), ReplicationStatusType::Replica);
        // For multipart the on-disk stamp actually comes from the SAME header
        // at initiate time (create-multipart builds its options through
        // put_opts_with_replication_authorization and persists user_defined
        // into the upload's metadata); the completion-time insert asserted
        // here feeds the anti-cascade must_replicate check on completion.
        assert_eq!(
            authorized.user_defined.get(AMZ_BUCKET_REPLICATION_STATUS).map(String::as_str),
            Some(ReplicationStatusType::Replica.as_str()),
            "authorized inbound multipart completion must carry REPLICA in its metadata"
        );
    }

    #[test]
    fn test_complete_multipart_opts_preserves_authorized_replication_mtime() {
        let source_mtime = "2024-05-20T10:30:00+08:00";
        let mut headers = HeaderMap::new();
        insert_header(&mut headers, SUFFIX_SOURCE_REPLICATION_REQUEST, "true");
        insert_header(&mut headers, SUFFIX_SOURCE_MTIME, source_mtime);
        insert_header(&mut headers, SUFFIX_SOURCE_ETAG, "\"0123456789abcdef0123456789abcdef-3\"");

        let untrusted = get_complete_multipart_upload_opts(&headers)
            .expect("ordinary multipart completion options should ignore replication headers");
        assert!(!untrusted.replication_request);
        assert!(untrusted.mod_time.is_none());
        assert!(untrusted.preserve_etag.is_none());

        let authorized = get_complete_multipart_upload_opts_with_replication_authorization(&headers, true)
            .expect("authorized multipart replication options should parse");
        let expected = time::OffsetDateTime::parse(source_mtime, &time::format_description::well_known::Rfc3339)
            .expect("test source mtime should be valid");
        assert!(authorized.replication_request);
        assert_eq!(authorized.mod_time, Some(expected));
        assert_eq!(authorized.preserve_etag.as_deref(), Some("0123456789abcdef0123456789abcdef-3"));

        insert_header(&mut headers, SUFFIX_SOURCE_MTIME, "invalid-time");
        let invalid = get_complete_multipart_upload_opts_with_replication_authorization(&headers, true)
            .expect("invalid replication mtime should keep existing fallback behavior");
        assert!(invalid.replication_request);
        assert!(invalid.mod_time.is_none());
    }

    #[test]
    fn test_get_default_opts_with_metadata() {
        let headers = create_test_headers();
        let metadata = create_test_metadata();

        let result = get_default_opts(&headers, metadata, false);

        assert!(result.is_ok());
        let opts = result.unwrap();
        assert!(!opts.user_defined.is_empty());
        let user_defined = opts.user_defined;
        assert_eq!(user_defined.get("key1"), Some(&"value1".to_string()));
        assert_eq!(user_defined.get("key2"), Some(&"value2".to_string()));
    }

    #[test]
    fn test_get_default_opts_without_metadata() {
        let headers = create_test_headers();

        let result = get_default_opts(&headers, HashMap::new(), false);

        assert!(result.is_ok());
        let opts = result.unwrap();
        assert!(opts.user_defined.is_empty());
    }

    #[test]
    fn test_extract_metadata_basic() {
        let headers = create_test_headers();

        let metadata = extract_metadata(&headers);

        assert!(metadata.contains_key("content-type"));
        assert_eq!(metadata.get("content-type"), Some(&"application/json".to_string()));
        assert!(metadata.contains_key("cache-control"));
        assert_eq!(metadata.get("cache-control"), Some(&"no-cache".to_string()));
        assert!(metadata.contains_key("custom"));
        assert_eq!(metadata.get("custom"), Some(&"custom-value".to_string()));
        assert!(metadata.contains_key("internal"));
        assert_eq!(metadata.get("internal"), Some(&"internal-value".to_string()));
    }

    #[test]
    fn test_extract_metadata_from_mime_amz_meta() {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-meta-user-id", HeaderValue::from_static("12345"));
        headers.insert("x-amz-meta-project", HeaderValue::from_static("test-project"));
        headers.insert("x-amz-meta-", HeaderValue::from_static("empty-key")); // Should be ignored

        let mut metadata = HashMap::new();
        extract_metadata_from_mime(&headers, &mut metadata);

        assert_eq!(metadata.get("user-id"), Some(&"12345".to_string()));
        assert_eq!(metadata.get("project"), Some(&"test-project".to_string()));
        assert!(!metadata.contains_key(""));
    }

    #[test]
    fn test_extract_metadata_from_mime_rustfs_meta() {
        let mut headers = HeaderMap::new();
        headers.insert("x-rustfs-meta-internal-id", HeaderValue::from_static("67890"));
        headers.insert("x-rustfs-meta-category", HeaderValue::from_static("documents"));

        let mut metadata = HashMap::new();
        extract_metadata_from_mime(&headers, &mut metadata);

        assert_eq!(metadata.get("internal-id"), Some(&"67890".to_string()));
        assert_eq!(metadata.get("category"), Some(&"documents".to_string()));
    }

    #[test]
    fn test_extract_metadata_from_mime_minio_meta() {
        let mut headers = HeaderMap::new();
        headers.insert("x-minio-meta-origin", HeaderValue::from_static("gateway"));
        headers.insert("x-minio-meta-source-id", HeaderValue::from_static("abc123"));
        headers.insert("x-minio-meta-", HeaderValue::from_static("empty-key"));

        let mut metadata = HashMap::new();
        extract_metadata_from_mime(&headers, &mut metadata);

        assert_eq!(metadata.get("origin"), Some(&"gateway".to_string()));
        assert_eq!(metadata.get("source-id"), Some(&"abc123".to_string()));
        assert!(!metadata.contains_key(""));
    }

    #[test]
    fn test_extract_metadata_from_mime_supported_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("text/plain"));
        headers.insert("cache-control", HeaderValue::from_static("max-age=3600"));
        headers.insert("content-language", HeaderValue::from_static("en-US"));
        headers.insert("content-encoding", HeaderValue::from_static("gzip"));
        headers.insert("content-disposition", HeaderValue::from_static("attachment"));
        headers.insert("x-amz-storage-class", HeaderValue::from_static("STANDARD"));
        headers.insert("x-amz-tagging", HeaderValue::from_static("key1=value1&key2=value2"));
        headers.insert("expires", HeaderValue::from_static("Wed, 21 Oct 2015 07:28:00 GMT"));
        headers.insert("x-amz-replication-status", HeaderValue::from_static("COMPLETED"));

        let mut metadata = HashMap::new();
        extract_metadata_from_mime(&headers, &mut metadata);

        assert_eq!(metadata.get("content-type"), Some(&"text/plain".to_string()));
        assert_eq!(metadata.get("cache-control"), Some(&"max-age=3600".to_string()));
        assert_eq!(metadata.get("content-language"), Some(&"en-US".to_string()));
        assert_eq!(metadata.get("content-encoding"), Some(&"gzip".to_string()));
        assert_eq!(metadata.get("content-disposition"), Some(&"attachment".to_string()));
        assert_eq!(metadata.get("x-amz-storage-class"), Some(&"STANDARD".to_string()));
        assert_eq!(metadata.get("x-amz-tagging"), Some(&"key1=value1&key2=value2".to_string()));
        assert_eq!(metadata.get("expires"), Some(&"Wed, 21 Oct 2015 07:28:00 GMT".to_string()));
        assert_eq!(metadata.get("x-amz-replication-status"), Some(&"COMPLETED".to_string()));
    }

    /// Issue #1857: SigV4 streaming sends Content-Encoding: aws-chunked. Per AWS S3,
    /// this is a request-side transfer encoding and must not be stored or returned.
    /// This test verifies: (1) "aws-chunked" alone is not persisted;
    /// (2) when combined with real encoding (e.g. gzip), only the real encoding is stored;
    /// (3) case-insensitive stripping of aws-chunked.
    #[test]
    fn test_content_encoding_aws_chunked_not_persisted_issue_1857() {
        let cases: &[(&str, Option<&str>)] = &[
            ("aws-chunked", None),
            ("AWS-CHUNKED", None),
            ("aws-chunked ", None),
            ("gzip, aws-chunked", Some("gzip")),
            ("aws-chunked, gzip", Some("gzip")),
            ("gzip", Some("gzip")),
            ("zstd", Some("zstd")),
        ];

        for (header_value, expected) in cases {
            let mut headers = HeaderMap::new();
            headers.insert("content-encoding", HeaderValue::from_static(header_value));

            let mut metadata = HashMap::new();
            extract_metadata_from_mime(&headers, &mut metadata);

            match expected {
                None => assert!(
                    !metadata.contains_key("content-encoding"),
                    "content-encoding {:?} should not be persisted, got metadata keys: {:?}",
                    header_value,
                    metadata.keys().collect::<Vec<_>>()
                ),
                Some(exp) => assert_eq!(
                    metadata.get("content-encoding"),
                    Some(&exp.to_string()),
                    "content-encoding {:?} should be normalized to {:?}",
                    header_value,
                    exp
                ),
            }
        }
    }

    #[test]
    fn test_extract_metadata_from_mime_default_content_type() {
        let headers = HeaderMap::new();

        let mut metadata = HashMap::new();
        extract_metadata_from_mime(&headers, &mut metadata);

        assert_eq!(metadata.get("content-type"), Some(&"binary/octet-stream".to_string()));
    }

    #[test]
    fn test_extract_metadata_from_mime_existing_content_type() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/json"));

        let mut metadata = HashMap::new();
        extract_metadata_from_mime(&headers, &mut metadata);

        assert_eq!(metadata.get("content-type"), Some(&"application/json".to_string()));
    }

    #[test]
    fn test_extract_metadata_from_mime_unicode_values() {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-meta-chinese", HeaderValue::from_bytes("test-value".as_bytes()).unwrap());
        headers.insert("x-rustfs-meta-emoji", HeaderValue::from_bytes("🚀".as_bytes()).unwrap());

        let mut metadata = HashMap::new();
        extract_metadata_from_mime(&headers, &mut metadata);

        assert_eq!(metadata.get("chinese"), Some(&"test-value".to_string()));
        assert_eq!(metadata.get("emoji"), Some(&"🚀".to_string()));
    }

    #[test]
    fn test_extract_metadata_from_mime_unsupported_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", HeaderValue::from_static("Bearer token"));
        headers.insert("host", HeaderValue::from_static("example.com"));
        headers.insert("user-agent", HeaderValue::from_static("test-agent"));

        let mut metadata = HashMap::new();
        extract_metadata_from_mime(&headers, &mut metadata);

        // These headers should not be included in metadata
        assert!(!metadata.contains_key("authorization"));
        assert!(!metadata.contains_key("host"));
        assert!(!metadata.contains_key("user-agent"));
        // But default content-type should be added
        assert_eq!(metadata.get("content-type"), Some(&"binary/octet-stream".to_string()));
    }

    #[test]
    fn test_supported_headers_constant() {
        let expected_headers = vec![
            "content-type",
            "cache-control",
            "content-language",
            "content-encoding",
            "content-disposition",
            "x-amz-storage-class",
            "x-amz-tagging",
            "expires",
            "x-amz-replication-status",
            AMZ_OBJECT_LOCK_MODE_LOWER,
            AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER,
            AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER,
        ];

        assert_eq!(*SUPPORTED_HEADERS, expected_headers);
        assert_eq!(SUPPORTED_HEADERS.len(), 12);
    }

    #[test]
    fn test_extract_metadata_empty_headers() {
        let headers = HeaderMap::new();

        let metadata = extract_metadata(&headers);

        // Should only contain default content-type
        assert_eq!(metadata.len(), 1);
        assert_eq!(metadata.get("content-type"), Some(&"binary/octet-stream".to_string()));
    }

    #[test]
    fn test_extract_metadata_mixed_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/xml"));
        headers.insert("x-amz-meta-version", HeaderValue::from_static("1.0"));
        headers.insert("x-rustfs-meta-source", HeaderValue::from_static("upload"));
        headers.insert("x-minio-meta-origin", HeaderValue::from_static("replication"));
        headers.insert("cache-control", HeaderValue::from_static("public"));
        headers.insert("authorization", HeaderValue::from_static("Bearer xyz")); // Should be ignored

        let metadata = extract_metadata(&headers);

        assert_eq!(metadata.get("content-type"), Some(&"application/xml".to_string()));
        assert_eq!(metadata.get("version"), Some(&"1.0".to_string()));
        assert_eq!(metadata.get("source"), Some(&"upload".to_string()));
        assert_eq!(metadata.get("origin"), Some(&"replication".to_string()));
        assert_eq!(metadata.get("cache-control"), Some(&"public".to_string()));
        assert!(!metadata.contains_key("authorization"));
    }

    #[test]
    fn test_extract_metadata_from_mime_with_parquet_object_name() {
        let headers = HeaderMap::new();
        let mut metadata = HashMap::new();

        extract_metadata_from_mime_with_object_name(&headers, &mut metadata, false, Some("data/test.parquet"));

        assert_eq!(metadata.get("content-type"), Some(&"application/vnd.apache.parquet".to_string()));
    }

    #[test]
    fn test_extract_metadata_from_mime_with_various_data_formats() {
        let test_cases = vec![
            ("data.parquet", "application/vnd.apache.parquet"),
            ("data.PARQUET", "application/vnd.apache.parquet"), // Test case insensitive
            ("file.avro", "application/avro"),
            ("file.orc", "application/orc"),
            ("file.feather", "application/feather"),
            ("file.arrow", "application/arrow"),
            ("file.json", "application/json"),
            ("file.csv", "text/csv"),
            ("file.txt", "text/plain"),
            ("file.unknownext", "application/octet-stream"), // Use truly unknown extension
        ];

        for (filename, expected_content_type) in test_cases {
            let headers = HeaderMap::new();
            let mut metadata = HashMap::new();

            extract_metadata_from_mime_with_object_name(&headers, &mut metadata, false, Some(filename));

            assert_eq!(
                metadata.get("content-type"),
                Some(&expected_content_type.to_string()),
                "Failed for filename: {filename}"
            );
        }
    }

    #[test]
    fn test_extract_metadata_from_mime_with_existing_content_type() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("custom/type"));

        let mut metadata = HashMap::new();
        extract_metadata_from_mime_with_object_name(&headers, &mut metadata, false, Some("test.parquet"));

        // Should preserve existing content-type, not overwrite
        assert_eq!(metadata.get("content-type"), Some(&"custom/type".to_string()));
    }

    #[test]
    fn test_filter_object_metadata_excludes_standard_headers() {
        let mut metadata = HashMap::new();
        metadata.insert("content-type".to_string(), "application/octet-stream".to_string());
        metadata.insert("content-disposition".to_string(), "inline".to_string());
        metadata.insert("cache-control".to_string(), "no-cache".to_string());
        metadata.insert("x-amz-storage-class".to_string(), "STANDARD".to_string());
        metadata.insert("custom-key".to_string(), "custom-value".to_string());

        let filtered = filter_object_metadata(&metadata).unwrap();

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered.get("custom-key"), Some(&"custom-value".to_string()));
        assert!(!filtered.contains_key("content-type"));
        assert!(!filtered.contains_key("content-disposition"));
        assert!(!filtered.contains_key("cache-control"));
        assert!(!filtered.contains_key("x-amz-storage-class"));
    }

    #[test]
    fn test_filter_object_metadata_returns_none_for_only_content_type() {
        let mut metadata = HashMap::new();
        metadata.insert("content-type".to_string(), "application/octet-stream".to_string());

        let filtered = filter_object_metadata(&metadata);
        assert!(filtered.is_none(), "content-type must not be exposed as user metadata");
    }

    #[test]
    fn test_filter_object_metadata_excludes_case_insensitive_system_headers() {
        let mut metadata = HashMap::new();
        metadata.insert("Content-Type".to_string(), "application/octet-stream".to_string());
        metadata.insert("X-Amz-Storage-Class".to_string(), "STANDARD".to_string());
        metadata.insert("X-RustFS-Internal-Healing".to_string(), "true".to_string());
        metadata.insert("X-Minio-Encryption-Iv".to_string(), "secret".to_string());
        metadata.insert("custom-key".to_string(), "custom-value".to_string());

        let filtered = filter_object_metadata(&metadata).expect("user metadata should remain");

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered.get("custom-key"), Some(&"custom-value".to_string()));
    }

    #[test]
    fn test_user_metadata_cannot_shadow_standard_or_internal_metadata() {
        let mut headers = HeaderMap::new();
        headers.insert("content-encoding", HeaderValue::from_static("br"));
        headers.insert("x-amz-meta-content-encoding", HeaderValue::from_static("user-encoding"));
        headers.insert("x-amz-meta-x-rustfs-internal-healing", HeaderValue::from_static("user-value"));

        let mut metadata = HashMap::new();
        extract_metadata_from_mime(&headers, &mut metadata);

        assert_eq!(metadata.get("content-encoding"), Some(&"br".to_string()));
        assert_eq!(metadata.get("x-amz-meta-content-encoding"), Some(&"user-encoding".to_string()));
        assert_eq!(metadata.get("x-amz-meta-x-rustfs-internal-healing"), Some(&"user-value".to_string()));
        let filtered = filter_object_metadata(&metadata).expect("user metadata should remain");
        assert_eq!(filtered.get("content-encoding"), Some(&"user-encoding".to_string()));
        assert_eq!(filtered.get("x-rustfs-internal-healing"), Some(&"user-value".to_string()));

        let mut copied_metadata = HashMap::from([
            ("content-type".to_string(), "user-type".to_string()),
            ("x-amz-meta-content-type".to_string(), "nested-user-type".to_string()),
            ("x-amz-storage-class".to_string(), "user-class".to_string()),
        ]);
        namespace_reserved_user_metadata(&mut copied_metadata);
        assert_eq!(copied_metadata.get("x-amz-meta-content-type"), Some(&"user-type".to_string()));
        assert_eq!(
            copied_metadata.get("x-amz-meta-x-amz-meta-content-type"),
            Some(&"nested-user-type".to_string())
        );
        assert_eq!(copied_metadata.get("x-amz-meta-x-amz-storage-class"), Some(&"user-class".to_string()));

        let legacy_metadata = HashMap::from([
            ("x-amz-meta-internal-secret".to_string(), "must-not-leak".to_string()),
            ("x-amz-meta-x-amz-unencrypted-content-md5".to_string(), "must-not-leak".to_string()),
            ("x-amz-meta-project".to_string(), "rustfs".to_string()),
        ]);
        let filtered_legacy = filter_object_metadata(&legacy_metadata).expect("safe user metadata should remain");
        assert_eq!(filtered_legacy, HashMap::from([("project".to_string(), "rustfs".to_string())]));
    }

    #[test]
    fn test_client_cannot_inject_internal_encryption_metadata_keys() {
        // Attack form: a client PUT smuggles internal encryption keys through the
        // x-amz-meta- user-metadata prefix. Stripping the prefix must NOT produce a
        // bare internal key (`x-rustfs-encryption-*` / `x-minio-encryption-*`) in
        // stored metadata, otherwise the KMS `headers_to_metadata` path would read
        // attacker-chosen values (e.g. the cipher algorithm) as trusted state.
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-meta-x-rustfs-encryption-algorithm", HeaderValue::from_static("ChaCha20Poly1305"));
        headers.insert("x-amz-meta-x-minio-encryption-key", HeaderValue::from_static("attacker-key"));
        headers.insert("x-amz-meta-X-Rustfs-Encryption-Iv", HeaderValue::from_static("attacker-iv"));

        let mut metadata = HashMap::new();
        extract_metadata_from_mime(&headers, &mut metadata);

        for injected in [
            "x-rustfs-encryption-algorithm",
            "x-minio-encryption-key",
            "X-Rustfs-Encryption-Iv",
        ] {
            assert!(
                !metadata.keys().any(|k| k.eq_ignore_ascii_case(injected)),
                "client-injected internal encryption key must not be stored bare: {injected}"
            );
        }
        // The values survive only inside the user-metadata namespace.
        assert_eq!(
            metadata.get("x-amz-meta-x-rustfs-encryption-algorithm"),
            Some(&"ChaCha20Poly1305".to_string())
        );
        assert_eq!(metadata.get("x-amz-meta-x-minio-encryption-key"), Some(&"attacker-key".to_string()));

        // CopyObject REPLACE path: DTO metadata keys take the same namespacing.
        let mut copied = HashMap::from([
            ("x-rustfs-encryption-algorithm".to_string(), "ChaCha20Poly1305".to_string()),
            ("X-Minio-Encryption-Key".to_string(), "attacker-key".to_string()),
        ]);
        namespace_reserved_user_metadata(&mut copied);
        assert_eq!(
            copied.get("x-amz-meta-x-rustfs-encryption-algorithm"),
            Some(&"ChaCha20Poly1305".to_string())
        );
        assert_eq!(copied.get("x-amz-meta-X-Minio-Encryption-Key"), Some(&"attacker-key".to_string()));
        assert!(!copied.contains_key("x-rustfs-encryption-algorithm"));
        assert!(!copied.contains_key("X-Minio-Encryption-Key"));
    }

    #[test]
    fn test_bare_encryption_headers_are_not_ingested_as_metadata() {
        // A client sending the internal header directly (no x-amz-meta- prefix) must
        // not have it ingested into stored metadata either: it matches neither the
        // user-metadata prefixes nor SUPPORTED_HEADERS.
        let mut headers = HeaderMap::new();
        headers.insert("x-rustfs-encryption-algorithm", HeaderValue::from_static("ChaCha20Poly1305"));
        headers.insert("x-minio-encryption-iv", HeaderValue::from_static("attacker-iv"));

        let mut metadata = HashMap::new();
        extract_metadata_from_mime(&headers, &mut metadata);

        assert!(!metadata.contains_key("x-rustfs-encryption-algorithm"));
        assert!(!metadata.contains_key("x-minio-encryption-iv"));
    }

    #[test]
    fn test_server_written_encryption_metadata_stays_internal() {
        // Legitimate SSE flow: the server inserts bare internal encryption keys into
        // stored metadata after user-metadata namespacing. They must remain hidden
        // from the client-visible Metadata map, while namespaced user metadata that
        // merely resembles them round-trips back to the client.
        let metadata = HashMap::from([
            ("x-rustfs-encryption-iv".to_string(), "server-iv".to_string()),
            ("x-rustfs-encryption-key".to_string(), "wrapped-key".to_string()),
            ("x-minio-encryption-original-size".to_string(), "1024".to_string()),
            ("x-amz-meta-x-rustfs-encryption-algorithm".to_string(), "user-value".to_string()),
            ("x-amz-meta-project".to_string(), "rustfs".to_string()),
        ]);

        let filtered = filter_object_metadata(&metadata).expect("user metadata should remain");
        assert_eq!(
            filtered,
            HashMap::from([
                ("x-rustfs-encryption-algorithm".to_string(), "user-value".to_string()),
                ("project".to_string(), "rustfs".to_string()),
            ])
        );
    }

    #[test]
    fn test_detect_content_type_from_object_name() {
        // Test Parquet files (our custom handling)
        assert_eq!(detect_content_type_from_object_name("test.parquet"), "application/vnd.apache.parquet");
        assert_eq!(detect_content_type_from_object_name("TEST.PARQUET"), "application/vnd.apache.parquet");

        // Test other custom data formats
        assert_eq!(detect_content_type_from_object_name("data.avro"), "application/avro");
        assert_eq!(detect_content_type_from_object_name("data.orc"), "application/orc");
        assert_eq!(detect_content_type_from_object_name("data.feather"), "application/feather");
        assert_eq!(detect_content_type_from_object_name("data.arrow"), "application/arrow");

        // Test standard formats (mime_guess handling)
        assert_eq!(detect_content_type_from_object_name("data.json"), "application/json");
        assert_eq!(detect_content_type_from_object_name("data.csv"), "text/csv");
        assert_eq!(detect_content_type_from_object_name("data.txt"), "text/plain");

        // Test truly unknown format (using extension mime_guess doesn't recognize)
        assert_eq!(detect_content_type_from_object_name("unknown.unknownext"), "application/octet-stream");

        // Test files without extension
        assert_eq!(detect_content_type_from_object_name("noextension"), "application/octet-stream");
    }

    #[test]
    fn test_validate_archive_content_encoding_allows_archive_suffix_by_default() {
        validate_archive_content_encoding("bundle.tar.gz", Some("application/gzip"), Some("gzip")).expect("default allow");
    }

    #[test]
    fn test_validate_archive_content_encoding_allows_archive_mime_by_default() {
        validate_archive_content_encoding("bundle", Some("application/zip"), Some("gzip")).expect("default allow");
    }

    #[test]
    fn test_validate_archive_content_encoding_allows_non_archive_precompressed_object() {
        validate_archive_content_encoding("logs/app.log.zst", Some("text/plain"), Some("zstd")).expect("non-archive");
    }

    #[test]
    fn test_validate_archive_content_encoding_allows_archive_sigv4_streaming_encoding_by_default() {
        validate_archive_content_encoding("bundle.tar.gz", Some("application/gzip"), Some("aws-chunked"))
            .expect("aws-chunked is request-side only");
    }

    #[test]
    fn test_validate_archive_content_encoding_allows_archive_sigv4_streaming_encoding_case_insensitive() {
        validate_archive_content_encoding("bundle.zip", Some("application/zip"), Some("AWS-CHUNKED"))
            .expect("aws-chunked stripping should be case-insensitive");
    }

    #[test]
    fn test_validate_archive_content_encoding_allows_effective_archive_encoding_after_aws_chunked_stripped_by_default() {
        validate_archive_content_encoding("bundle.zip", Some("application/zip"), Some("aws-chunked, gzip"))
            .expect("default allow after stripping aws-chunked");
    }

    #[test]
    fn test_validate_archive_content_encoding_rejects_archive_suffix_in_strict_mode() {
        temp_env::with_var(ENV_REJECT_ARCHIVE_CONTENT_ENCODING, Some("true"), || {
            let err = validate_archive_content_encoding("bundle.tar.gz", Some("application/gzip"), Some("gzip")).unwrap_err();
            assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        });
    }

    #[test]
    fn test_validate_archive_content_encoding_rejects_archive_mime_in_strict_mode() {
        temp_env::with_var(ENV_REJECT_ARCHIVE_CONTENT_ENCODING, Some("true"), || {
            let err = validate_archive_content_encoding("bundle", Some("application/zip"), Some("gzip")).unwrap_err();
            assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        });
    }

    #[test]
    fn test_validate_archive_content_encoding_rejects_effective_archive_encoding_after_aws_chunked_stripped_in_strict_mode() {
        temp_env::with_var(ENV_REJECT_ARCHIVE_CONTENT_ENCODING, Some("true"), || {
            let err =
                validate_archive_content_encoding("bundle.zip", Some("application/zip"), Some("aws-chunked, gzip")).unwrap_err();
            assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
            assert_eq!(
                err.message(),
                Some(
                    "Content-Encoding 'gzip' is not allowed for archive objects when RUSTFS_REJECT_ARCHIVE_CONTENT_ENCODING=true; unset RUSTFS_REJECT_ARCHIVE_CONTENT_ENCODING or set it to false to restore compatibility-first behavior"
                )
            );
        });
    }

    #[test]
    fn test_parse_copy_source_range() {
        // Test complete range: bytes=0-1023
        let result = parse_copy_source_range("bytes=0-1023").unwrap();
        assert!(!result.is_suffix_length);
        assert_eq!(result.start, 0);
        assert_eq!(result.end, 1023);

        // Test open-ended range: bytes=500-
        let result = parse_copy_source_range("bytes=500-").unwrap();
        assert!(!result.is_suffix_length);
        assert_eq!(result.start, 500);
        assert_eq!(result.end, -1);

        // Test suffix range: bytes=-500 (last 500 bytes)
        let result = parse_copy_source_range("bytes=-500").unwrap();
        assert!(result.is_suffix_length);
        assert_eq!(result.start, -500);
        assert_eq!(result.end, -1);

        // Test invalid format
        assert!(parse_copy_source_range("invalid").is_err());
        assert!(parse_copy_source_range("bytes=").is_err());
        assert!(parse_copy_source_range("bytes=abc-def").is_err());
        assert!(parse_copy_source_range("bytes=100-50").is_err()); // start > end
        assert!(parse_copy_source_range("bytes=-0").is_err());
        assert!(parse_copy_source_range("bytes=--9223372036854775808").is_err());
    }

    proptest! {
        #[test]
        fn parse_copy_source_range_never_panics_and_preserves_output_invariants(
            input in any::<String>(),
        ) {
            match std::panic::catch_unwind(|| parse_copy_source_range(&input)) {
                Ok(Ok(spec)) => {
                    if spec.is_suffix_length {
                        prop_assert_eq!(spec.end, -1);
                        prop_assert!(spec.start < 0);
                    } else {
                        prop_assert!(spec.start >= 0);
                        prop_assert!(spec.end == -1 || spec.end >= spec.start);
                    }
                }
                Ok(Err(_)) => {}
                Err(_) => prop_assert!(false, "parse_copy_source_range panicked for input {:?}", input),
            }
        }
    }

    /// The replication read-proxy anti-loop markers must be honored under
    /// both interop prefixes (a MinIO peer sends x-minio-, a RustFS peer
    /// sends both). `proxy_request` is set only for the literal value
    /// "true", while `proxy_header_set` (MinIO `ProxyHeaderSet`) is set by
    /// the header's mere presence — "false" (the replication worker's
    /// convergence-HEAD marker) and arbitrary values included — so the
    /// selector refuses to proxy either way.
    #[tokio::test]
    async fn test_get_opts_parses_source_proxy_request_under_both_prefixes() {
        for header_name in ["x-rustfs-source-proxy-request", "x-minio-source-proxy-request"] {
            let mut headers = HeaderMap::new();
            headers.insert(header_name, HeaderValue::from_static("true"));
            let opts = get_opts("test-bucket", "test-object", None, None, &headers)
                .await
                .expect("get_opts should succeed");
            assert!(opts.proxy_request, "{header_name} must set opts.proxy_request");
            assert!(opts.proxy_header_set, "{header_name} must set opts.proxy_header_set");
        }

        let opts = get_opts("test-bucket", "test-object", None, None, &HeaderMap::new())
            .await
            .expect("get_opts should succeed");
        assert!(!opts.proxy_request, "absent header must leave proxy_request off");
        assert!(!opts.proxy_header_set, "absent header must leave proxy_header_set off");

        for (header_name, value) in [
            ("x-minio-source-proxy-request", "false"),
            ("x-rustfs-source-proxy-request", "false"),
            ("x-minio-source-proxy-request", "anything-else"),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(header_name, HeaderValue::from_static(value));
            let opts = get_opts("test-bucket", "test-object", None, None, &headers)
                .await
                .expect("get_opts should succeed");
            assert!(!opts.proxy_request, "{header_name}: non-'true' value must leave proxy_request off");
            assert!(
                opts.proxy_header_set,
                "{header_name}: value {value:?} must still set proxy_header_set (presence disables proxying)"
            );
        }
    }

    /// Pin that the source-proxy-request transport family cannot be
    /// materialized as bare stored metadata via an `x-*-meta-` disguise: the
    /// reserved-key namespacing (`x-rustfs-source-` / `x-minio-source-`
    /// prefixes in `is_reserved_user_metadata_key`) must keep covering it.
    #[test]
    fn test_source_proxy_request_family_is_reserved_user_metadata() {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-meta-x-minio-source-proxy-request", HeaderValue::from_static("true"));
        headers.insert("x-rustfs-meta-x-rustfs-source-proxy-request", HeaderValue::from_static("true"));
        // The bare transport header itself is not a user-metadata prefix and
        // must never land in stored metadata at all.
        headers.insert("x-minio-source-proxy-request", HeaderValue::from_static("true"));

        let metadata = extract_metadata(&headers);

        assert!(
            !metadata.contains_key("x-minio-source-proxy-request"),
            "bare source-proxy-request key must not be storable: {metadata:?}"
        );
        assert!(
            !metadata.contains_key("x-rustfs-source-proxy-request"),
            "bare source-proxy-request key must not be storable: {metadata:?}"
        );
        assert!(
            metadata.contains_key("x-amz-meta-x-minio-source-proxy-request"),
            "disguised key must be namespaced back under x-amz-meta-: {metadata:?}"
        );
        assert!(
            metadata.contains_key("x-amz-meta-x-rustfs-source-proxy-request"),
            "disguised key must be namespaced back under x-amz-meta-: {metadata:?}"
        );
    }
}
