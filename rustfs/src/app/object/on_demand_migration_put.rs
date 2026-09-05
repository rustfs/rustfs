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

//! On-demand migration write-back (rustfs/backlog#2153): the app-layer
//! [`OdmWriteBack`] the ecstore pull pipeline stores source objects with.
//!
//! Every write goes through the internal put entry points, so a pulled
//! object is indistinguishable from a client PUT: bucket default SSE, quota,
//! versioning, Object Lock defaults, replication scheduling and creation
//! events all apply. This module only maps a [`WriteBackRequest`] onto an
//! [`InternalPutContext`]: the content-header allowlist, the `x-amz-meta-*`
//! copy, optional tags, the five `odm-*` provenance keys (dual prefix), and
//! the ETag policy.
//!
//! ETag policy: a single-part source ETag (32 hex digits) of an unencrypted
//! source object is the plaintext MD5 and doubles as the integrity check;
//! `policy.preserve_etag` keeps the source ETag (multipart ETags included,
//! display only) unless the bucket encrypts by default, where the override
//! is dropped and the SSE write path decides the ETag, exactly like
//! replication receive. The source ETag is always recorded under
//! `odm-source-etag`.

use super::*;

use crate::app::storage_api::multipart_usecase::contract::multipart::CompletePart;
use crate::app::storage_api::object_usecase::on_demand_migration::{
    LocalObject, OdmWriteBack, SourceHead, WriteBackBody, WriteBackError, WriteBackOutcome, WriteBackPart, WriteBackRequest,
    is_multipart_etag,
};
use rustfs_utils::http::{
    SUFFIX_ODM_PULLED_AT, SUFFIX_ODM_SOURCE, SUFFIX_ODM_SOURCE_ETAG, SUFFIX_ODM_SOURCE_LAST_MODIFIED,
    SUFFIX_ODM_SOURCE_VERSION_ID,
};

/// `userIdentity.principalId` of every creation event a write-back emits.
pub(crate) const ON_DEMAND_MIGRATION_PRINCIPAL_ID: &str = "rustfs-on-demand-migration";

/// [`OdmWriteBack`] over [`DefaultObjectUsecase`]'s internal put entry
/// points. The ambient app context is resolved per call: the write-back is
/// installed at startup, before the context is published.
#[derive(Debug, Default)]
pub(crate) struct OnDemandMigrationWriteBack;

impl OnDemandMigrationWriteBack {
    pub(crate) fn new() -> Self {
        Self
    }

    fn usecase(&self) -> DefaultObjectUsecase {
        DefaultObjectUsecase::from_global()
    }

    fn store(&self) -> Result<Arc<ECStore>, WriteBackError> {
        self.usecase()
            .object_store()
            .ok_or_else(|| WriteBackError::Local("object store is not initialized".to_string()))
    }
}

fn rfc3339(time: OffsetDateTime) -> String {
    time.format(&Rfc3339).unwrap_or_default()
}

/// Standard object headers copied from the source. `storage_class` and the
/// source `Last-Modified` are deliberately absent: the local class follows
/// the bucket and the local mtime is the write time.
pub(super) fn content_headers(head: &SourceHead) -> HashMap<String, String> {
    let mut headers = HashMap::with_capacity(6);
    for (name, value) in [
        ("Content-Type", &head.content_type),
        ("Content-Encoding", &head.content_encoding),
        ("Content-Disposition", &head.content_disposition),
        ("Content-Language", &head.content_language),
        ("Cache-Control", &head.cache_control),
        ("Expires", &head.expires),
    ] {
        if let Some(value) = value.as_deref().map(str::trim).filter(|value| !value.is_empty()) {
            headers.insert(name.to_string(), value.to_string());
        }
    }
    headers
}

/// The five `odm-*` provenance keys under both internal prefixes. Absent
/// source values are stored as empty strings so the key set is constant.
pub(super) fn provenance_metadata(request: &WriteBackRequest) -> HashMap<String, String> {
    let head = &request.head;
    let mut metadata = HashMap::with_capacity(10);
    insert_str(&mut metadata, SUFFIX_ODM_SOURCE, request.source_label.clone());
    insert_str(&mut metadata, SUFFIX_ODM_SOURCE_ETAG, head.etag.clone().unwrap_or_default());
    insert_str(
        &mut metadata,
        SUFFIX_ODM_SOURCE_LAST_MODIFIED,
        head.last_modified.map(OffsetDateTime::from).map(rfc3339).unwrap_or_default(),
    );
    insert_str(&mut metadata, SUFFIX_ODM_SOURCE_VERSION_ID, head.version_id.clone().unwrap_or_default());
    insert_str(&mut metadata, SUFFIX_ODM_PULLED_AT, rfc3339(request.pulled_at));
    metadata
}

/// The source ETag as the expected plaintext MD5: only a bare 32-digit hex
/// ETag of an unencrypted source object is one.
pub(super) fn expected_md5_hex(head: &SourceHead) -> Option<String> {
    if head.sse.is_some() {
        return None;
    }
    // Azure stamps an opaque concurrency token in the ETag slot. It is
    // recorded as provenance, but reading it as a digest would compare the
    // pulled bytes against a value that never described them.
    if head.etag_is_opaque {
        return None;
    }
    let etag = head.etag.as_deref()?;
    if etag.len() != 32 || is_multipart_etag(etag) || !etag.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    Some(etag.to_ascii_lowercase())
}

/// `x-amz-tagging` form of the source tags, sorted by key for a stable
/// stored value.
pub(super) fn encode_tags(tags: &HashMap<String, String>) -> Option<String> {
    if tags.is_empty() {
        return None;
    }
    let mut pairs: Vec<(&String, &String)> = tags.iter().collect();
    pairs.sort();
    let mut encoded = url::form_urlencoded::Serializer::new(String::new());
    for (key, value) in pairs {
        encoded.append_pair(key, value);
    }
    Some(encoded.finish())
}

async fn bucket_encrypts_by_default(bucket: &str) -> bool {
    metadata_sys::get_sse_config(bucket).await.is_ok_and(|(config, _)| {
        config
            .rules
            .iter()
            .any(|rule| rule.apply_server_side_encryption_by_default.is_some())
    })
}

/// Builds the internal put context of a write-back. `single_part` enables
/// the MD5 integrity check, which only the single-object path can honor.
pub(super) async fn write_back_context(request: &WriteBackRequest, single_part: bool) -> InternalPutContext {
    let head = &request.head;
    let preserve_etag = if request.preserve_etag && head.etag.is_some() && !bucket_encrypts_by_default(&request.bucket).await {
        head.etag.clone()
    } else {
        None
    };
    InternalPutContext {
        bucket: request.bucket.clone(),
        key: request.key.clone(),
        size: Some(head.size),
        expected_md5_hex: single_part.then(|| expected_md5_hex(head)).flatten(),
        preserve_etag,
        content_headers: content_headers(head),
        user_metadata: head.user_metadata.clone(),
        tags: request.tags.as_ref().and_then(encode_tags),
        internal_metadata: provenance_metadata(request),
        emit_events: request.emit_events,
        principal_id: ON_DEMAND_MIGRATION_PRINCIPAL_ID,
    }
}

/// Maps an internal put failure onto the write-back error classes. A
/// digest mismatch is the only integrity signal; both quota producers
/// (admission and the durable reservation) say "quota exceeded".
pub(super) fn write_back_error(err: ApiError) -> WriteBackError {
    if err.code == S3ErrorCode::BadDigest {
        return WriteBackError::Integrity;
    }
    if err.message.to_ascii_lowercase().contains("quota exceeded") {
        return WriteBackError::Quota(err.message);
    }
    WriteBackError::Local(format!("{}: {}", err.code.as_str(), err.message))
}

fn outcome(info: ObjectInfo) -> WriteBackOutcome {
    WriteBackOutcome {
        etag: info.etag,
        size: u64::try_from(info.size).unwrap_or(0),
        version_id: info.version_id.map(|version_id| version_id.to_string()),
    }
}

#[async_trait::async_trait]
impl OdmWriteBack for OnDemandMigrationWriteBack {
    async fn local_object(&self, bucket: &str, key: &str) -> Result<Option<LocalObject>, WriteBackError> {
        let store = self.store()?;
        match store.get_object_info(bucket, key, &ObjectOptions::default()).await {
            Ok(info) => Ok(Some(LocalObject {
                etag: info.etag.clone(),
                size: u64::try_from(info.size).unwrap_or(0),
                delete_marker: info.delete_marker,
            })),
            Err(err) if is_err_object_not_found(&err) || is_err_version_not_found(&err) => Ok(None),
            Err(err) => Err(WriteBackError::Local(err.to_string())),
        }
    }

    async fn put_object(&self, request: &WriteBackRequest, body: WriteBackBody) -> Result<WriteBackOutcome, WriteBackError> {
        let ctx = write_back_context(request, true).await;
        self.usecase()
            .internal_put_object(ctx, body)
            .await
            .map(outcome)
            .map_err(write_back_error)
    }

    async fn create_multipart_upload(&self, request: &WriteBackRequest) -> Result<String, WriteBackError> {
        let ctx = write_back_context(request, false).await;
        self.usecase()
            .internal_create_multipart_upload(&ctx)
            .await
            .map_err(write_back_error)
    }

    async fn upload_part(
        &self,
        request: &WriteBackRequest,
        upload_id: &str,
        part_number: usize,
        size: u64,
        body: WriteBackBody,
    ) -> Result<WriteBackPart, WriteBackError> {
        let ctx = write_back_context(request, false).await;
        let part = self
            .usecase()
            .internal_upload_part(&ctx, upload_id, part_number, size, None, body)
            .await
            .map_err(write_back_error)?;
        Ok(WriteBackPart {
            part_number: part.part_num,
            etag: part.etag.unwrap_or_default(),
        })
    }

    async fn complete_multipart_upload(
        &self,
        request: &WriteBackRequest,
        upload_id: &str,
        parts: Vec<WriteBackPart>,
    ) -> Result<WriteBackOutcome, WriteBackError> {
        let ctx = write_back_context(request, false).await;
        let parts = parts
            .into_iter()
            .map(|part| CompletePart {
                part_num: part.part_number,
                etag: Some(part.etag),
                ..Default::default()
            })
            .collect();
        self.usecase()
            .internal_complete_multipart_upload(&ctx, upload_id, parts)
            .await
            .map(outcome)
            .map_err(write_back_error)
    }

    async fn abort_multipart_upload(&self, bucket: &str, key: &str, upload_id: &str) -> Result<(), WriteBackError> {
        self.usecase()
            .internal_abort_multipart_upload(bucket, key, upload_id)
            .await
            .map_err(write_back_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::storage_api::multipart_usecase::contract::multipart::MultipartOperations as _;
    use crate::app::storage_api::object_usecase::on_demand_migration::{PullFailureReason, SourceSse};
    use crate::app::storage_api::s3::{
        BucketVersioningStatus, DeleteMarkerReplication, DeleteMarkerReplicationStatus, Destination, ReplicationConfiguration,
        ReplicationRule, ReplicationRuleFilter, ReplicationRuleStatus, ServerSideEncryptionByDefault,
        ServerSideEncryptionConfiguration, ServerSideEncryptionRule, Tag, VersioningConfiguration,
    };
    use crate::app::storage_api::test::bucket::utils::serialize;
    use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};
    use crate::app::storage_api::test::{get_global_bucket_metadata_sys, set_bucket_metadata};
    use http::Method;
    use rustfs_utils::http::{MINIO_INTERNAL_PREFIX, RUSTFS_INTERNAL_PREFIX, contains_key_str, get_str};
    use sha2::{Digest as Sha256Digest, Sha256};
    use std::time::SystemTime;
    use tokio::io::AsyncReadExt;

    const SOURCE_LABEL: &str = "s3:legacy-bucket";

    fn md5_hex(body: &[u8]) -> String {
        hex_simd::encode_to_string(Md5::digest(body), hex_simd::AsciiCase::Lower)
    }

    fn sha256_hex(body: &[u8]) -> String {
        hex_simd::encode_to_string(Sha256::digest(body), hex_simd::AsciiCase::Lower)
    }

    fn stream(chunks: Vec<io::Result<Bytes>>) -> WriteBackBody {
        Box::pin(futures::stream::iter(chunks))
    }

    fn body_stream(body: &[u8]) -> WriteBackBody {
        stream(body.chunks(1 << 20).map(|chunk| Ok(Bytes::copy_from_slice(chunk))).collect())
    }

    fn source_head(body: &[u8]) -> SourceHead {
        SourceHead {
            etag: Some(md5_hex(body)),
            size: body.len() as u64,
            last_modified: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000)),
            content_type: Some("text/plain; charset=utf-8".to_string()),
            cache_control: Some("max-age=60".to_string()),
            content_language: Some("en".to_string()),
            user_metadata: HashMap::from([("origin".to_string(), "legacy".to_string())]),
            version_id: Some("src-v1".to_string()),
            storage_class: Some("STANDARD_IA".to_string()),
            ..Default::default()
        }
    }

    fn request(bucket: &str, key: &str, head: SourceHead) -> WriteBackRequest {
        WriteBackRequest {
            bucket: bucket.to_string(),
            key: key.to_string(),
            head,
            source_label: SOURCE_LABEL.to_string(),
            pulled_at: OffsetDateTime::from_unix_timestamp(1_756_800_000).expect("valid timestamp"),
            preserve_etag: true,
            emit_events: true,
            tags: Some(HashMap::from([
                ("team".to_string(), "storage".to_string()),
                ("env".to_string(), "prod".to_string()),
            ])),
        }
    }

    async fn write_back_test_bucket(prefix: &str, versioned: bool) -> (Arc<ECStore>, String) {
        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        crate::app::runtime_sources::install_test_app_context(Arc::clone(&store)).await;
        let bucket = format!("{prefix}-{}", Uuid::new_v4().simple());
        store
            .make_bucket(
                &bucket,
                &MakeBucketOptions {
                    versioning_enabled: versioned,
                    ..Default::default()
                },
            )
            .await
            .expect("create write-back test bucket");
        (store, bucket)
    }

    async fn stored_object(store: &Arc<ECStore>, bucket: &str, key: &str) -> ObjectInfo {
        store
            .get_object_info(bucket, key, &ObjectOptions::default())
            .await
            .expect("write-back must leave a readable object")
    }

    async fn assert_nothing_left(store: &Arc<ECStore>, bucket: &str, key: &str) {
        let lookup = store.get_object_info(bucket, key, &ObjectOptions::default()).await;
        assert!(
            lookup.as_ref().is_err_and(is_err_object_not_found),
            "a failed write-back must not leave an object: {lookup:?}"
        );
        let uploads = store
            .list_multipart_uploads(bucket, key, None, None, None, 100)
            .await
            .expect("list multipart uploads");
        assert!(
            uploads.uploads.is_empty(),
            "a failed write-back must not leave uploads: {:?}",
            uploads.uploads
        );
    }

    async fn raw_object_bytes(store: &Arc<ECStore>, bucket: &str, key: &str) -> Vec<u8> {
        let mut reader = (**store)
            .get_object_reader(bucket, key, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("read object");
        let mut buf = Vec::new();
        reader.stream.read_to_end(&mut buf).await.expect("drain object reader");
        buf
    }

    async fn get_via_app(bucket: &str, key: &str) -> Vec<u8> {
        let input = GetObjectInput::builder()
            .bucket(bucket.to_string())
            .key(key.to_string())
            .build()
            .expect("GET input must build");
        let req = build_request(input, Method::GET);
        let mut response = DefaultObjectUsecase::from_global()
            .execute_get_object(req)
            .await
            .expect("app-layer GET must succeed");
        let mut body = response.output.body.take().expect("GET response must include a body");
        let mut actual = Vec::new();
        while let Some(chunk) = body.next().await {
            actual.extend_from_slice(&chunk.expect("GET body chunk"));
        }
        actual
    }

    fn assert_provenance(metadata: &HashMap<String, String>, head: &SourceHead) {
        for suffix in [
            SUFFIX_ODM_SOURCE,
            SUFFIX_ODM_SOURCE_ETAG,
            SUFFIX_ODM_SOURCE_LAST_MODIFIED,
            SUFFIX_ODM_SOURCE_VERSION_ID,
            SUFFIX_ODM_PULLED_AT,
        ] {
            assert!(
                metadata.contains_key(&format!("{RUSTFS_INTERNAL_PREFIX}{suffix}")),
                "missing rustfs {suffix}"
            );
            assert!(
                metadata.contains_key(&format!("{MINIO_INTERNAL_PREFIX}{suffix}")),
                "missing minio {suffix}"
            );
        }
        assert_eq!(get_str(metadata, SUFFIX_ODM_SOURCE).as_deref(), Some(SOURCE_LABEL));
        assert_eq!(get_str(metadata, SUFFIX_ODM_SOURCE_ETAG), head.etag);
        assert_eq!(
            get_str(metadata, SUFFIX_ODM_SOURCE_LAST_MODIFIED).as_deref(),
            Some("2023-11-14T22:13:20Z")
        );
        assert_eq!(get_str(metadata, SUFFIX_ODM_SOURCE_VERSION_ID).as_deref(), Some("src-v1"));
        assert_eq!(get_str(metadata, SUFFIX_ODM_PULLED_AT).as_deref(), Some("2025-09-02T08:00:00Z"));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn write_back_commits_the_source_object_with_provenance_and_source_etag() {
        let (store, bucket) = write_back_test_bucket("odm-wb", false).await;
        let write_back = OnDemandMigrationWriteBack::new();
        assert!(
            write_back
                .local_object(&bucket, "dir/obj.txt")
                .await
                .expect("lookup")
                .is_none()
        );

        let body = b"pulled from the legacy bucket".to_vec();
        let head = source_head(&body);
        let outcome = write_back
            .put_object(&request(&bucket, "dir/obj.txt", head.clone()), body_stream(&body))
            .await
            .expect("write-back must commit");
        assert_eq!(outcome.etag, head.etag, "single-part source ETag is preserved");
        assert_eq!(outcome.size, body.len() as u64);

        let stored = stored_object(&store, &bucket, "dir/obj.txt").await;
        assert_eq!(stored.etag, head.etag);
        assert_eq!(stored.size, body.len() as i64);
        let metadata = &stored.user_defined;
        assert_provenance(metadata, &head);
        assert_eq!(metadata.get("content-type").map(String::as_str), Some("text/plain; charset=utf-8"));
        assert_eq!(metadata.get("cache-control").map(String::as_str), Some("max-age=60"));
        assert_eq!(metadata.get("content-language").map(String::as_str), Some("en"));
        assert_eq!(metadata.get("origin").map(String::as_str), Some("legacy"));
        assert!(
            !metadata.keys().any(|key| key.eq_ignore_ascii_case("x-amz-storage-class")),
            "source storage class is not copied: {metadata:?}"
        );
        assert_eq!(stored.user_tags.as_str(), "env=prod&team=storage");
        assert_eq!(raw_object_bytes(&store, &bucket, "dir/obj.txt").await, body);

        let local = write_back
            .local_object(&bucket, "dir/obj.txt")
            .await
            .expect("lookup")
            .expect("object now exists");
        assert_eq!(local.etag, head.etag);
        assert_eq!(local.size, body.len() as u64);
        assert!(!local.delete_marker);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn write_back_integrity_failure_leaves_nothing_behind() {
        let (store, bucket) = write_back_test_bucket("odm-wb-etag", false).await;
        let body = b"the source lied about this body".to_vec();
        let mut head = source_head(&body);
        head.etag = Some(md5_hex(b"a different body"));

        let err = OnDemandMigrationWriteBack::new()
            .put_object(&request(&bucket, "wrong.bin", head), body_stream(&body))
            .await
            .expect_err("an ETag mismatch must fail the write-back");
        assert_eq!(err, WriteBackError::Integrity);
        assert_eq!(err.reason(), PullFailureReason::EtagMismatch);
        assert_nothing_left(&store, &bucket, "wrong.bin").await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn write_back_truncated_stream_leaves_nothing_behind() {
        let (store, bucket) = write_back_test_bucket("odm-wb-trunc", false).await;
        let body = vec![0x5a; 200 * 1024];
        let head = source_head(&body);

        // The tee secondary reports the source failure mid-stream.
        let torn = stream(vec![
            Ok(Bytes::copy_from_slice(&body[..64 * 1024])),
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "tee primary dropped before EOF")),
        ]);
        let err = OnDemandMigrationWriteBack::new()
            .put_object(&request(&bucket, "torn.bin", head.clone()), torn)
            .await
            .expect_err("a broken stream must fail the write-back");
        assert_ne!(err, WriteBackError::Integrity, "{err}");
        assert_nothing_left(&store, &bucket, "torn.bin").await;

        // A clean EOF short of the advertised size is just as fatal.
        let short = stream(vec![Ok(Bytes::copy_from_slice(&body[..64 * 1024]))]);
        let err = OnDemandMigrationWriteBack::new()
            .put_object(&request(&bucket, "short.bin", head), short)
            .await
            .expect_err("a short body must fail the write-back");
        assert!(matches!(err, WriteBackError::Local(_) | WriteBackError::Integrity), "{err}");
        assert_nothing_left(&store, &bucket, "short.bin").await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn write_back_multipart_path_matches_the_source_digest() {
        const PART_SIZE: usize = 5 * 1024 * 1024;
        let (store, bucket) = write_back_test_bucket("odm-wb-mpu", false).await;
        let body: Vec<u8> = (0..PART_SIZE + 4096).map(|i| (i % 253) as u8).collect();
        let mut head = source_head(&body);
        head.etag = Some(format!("{}-2", md5_hex(&body)));
        head.is_multipart_etag = true;
        let request = request(&bucket, "big/object.bin", head.clone());
        let write_back = OnDemandMigrationWriteBack::new();

        let upload_id = write_back.create_multipart_upload(&request).await.expect("create");
        let mut parts = Vec::new();
        for (index, chunk) in body.chunks(PART_SIZE).enumerate() {
            let part = write_back
                .upload_part(&request, &upload_id, index + 1, chunk.len() as u64, body_stream(chunk))
                .await
                .expect("stage part");
            assert_eq!(part.part_number, index + 1);
            assert!(!part.etag.is_empty());
            parts.push(part);
        }
        let outcome = write_back
            .complete_multipart_upload(&request, &upload_id, parts)
            .await
            .expect("complete");
        assert_eq!(outcome.size, body.len() as u64);
        assert_eq!(outcome.etag, head.etag, "multipart source ETag is preserved for display");

        let stored = stored_object(&store, &bucket, "big/object.bin").await;
        assert_eq!(stored.parts.len(), 2, "HEAD parts_count");
        assert_eq!(stored.size, body.len() as i64);
        assert_eq!(stored.etag, head.etag);
        assert_provenance(&stored.user_defined, &head);
        assert_eq!(
            stored.user_defined.get("content-type").map(String::as_str),
            Some("text/plain; charset=utf-8")
        );
        assert_eq!(sha256_hex(&raw_object_bytes(&store, &bucket, "big/object.bin").await), sha256_hex(&body));

        // A failed upload is aborted and leaves no residue.
        let aborted = write_back.create_multipart_upload(&request).await.expect("create");
        write_back
            .upload_part(&request, &aborted, 1, 4096, body_stream(&body[..4096]))
            .await
            .expect("stage part");
        write_back
            .abort_multipart_upload(&bucket, "big/object.bin", &aborted)
            .await
            .expect("abort");
        let uploads = store
            .list_multipart_uploads(&bucket, "big/object.bin", None, None, None, 100)
            .await
            .expect("list uploads");
        assert!(uploads.uploads.iter().all(|upload| upload.upload_id != aborted));
    }

    async fn install_bucket_default_sse(bucket: &str) {
        let sys = get_global_bucket_metadata_sys().expect("bucket metadata system");
        let metadata = {
            let sys = sys.read().await;
            sys.get(bucket).await.expect("bucket metadata cached")
        };
        let mut metadata = (*metadata).clone();
        let config = ServerSideEncryptionConfiguration {
            rules: vec![ServerSideEncryptionRule {
                apply_server_side_encryption_by_default: Some(ServerSideEncryptionByDefault {
                    sse_algorithm: ServerSideEncryption::from_static(ServerSideEncryption::AES256),
                    kms_master_key_id: None,
                }),
                blocked_encryption_types: None,
                bucket_key_enabled: None,
            }],
        };
        metadata.encryption_config_xml = serialize(&config).expect("sse config serializes");
        metadata.sse_config = Some(config);
        set_bucket_metadata(bucket.to_string(), metadata)
            .await
            .expect("install bucket default SSE");
    }

    #[test]
    #[serial_test::serial]
    fn write_back_under_bucket_default_sse_stores_ciphertext_and_records_source_etag() {
        crate::app::gating_test_env::run_large_stack_test(
            "odm-write-back-under-bucket-default-sse",
            write_back_under_bucket_default_sse_stores_ciphertext_and_records_source_etag_inner,
        );
    }

    async fn write_back_under_bucket_default_sse_stores_ciphertext_and_records_source_etag_inner() {
        let local_sse_master_key = base64_simd::STANDARD.encode_to_string([0x42u8; 32]);
        temp_env::async_with_vars([("RUSTFS_SSE_S3_MASTER_KEY", Some(local_sse_master_key))], async {
            let (store, bucket) = write_back_test_bucket("odm-wb-sse", false).await;
            // The gating store adopts the bootstrap context; server startup is
            // what normally installs the read-side decryption resolver on it.
            let _ = crate::app::storage_api::test::bootstrap_instance_ctx();
            install_bucket_default_sse(&bucket).await;
            assert!(bucket_encrypts_by_default(&bucket).await);

            let body = b"plaintext that must be encrypted at rest".to_vec();
            let head = source_head(&body);
            let request = request(&bucket, "secret.txt", head.clone());
            // The source ETag is not forced onto an encrypted object; the
            // local ETag is whatever the SSE write path computes.
            assert_eq!(write_back_context(&request, true).await.preserve_etag, None);
            let outcome = OnDemandMigrationWriteBack::new()
                .put_object(&request, body_stream(&body))
                .await
                .expect("write-back under SSE must commit");
            assert!(outcome.etag.is_some());

            let stored = stored_object(&store, &bucket, "secret.txt").await;
            assert_eq!(stored.etag, outcome.etag);
            assert!(
                stored.user_defined.contains_key("x-amz-server-side-encryption"),
                "{:?}",
                stored.user_defined
            );
            assert_eq!(get_str(&stored.user_defined, SUFFIX_ODM_SOURCE_ETAG), head.etag);
            assert_provenance(&stored.user_defined, &head);
            assert!(
                stored
                    .user_defined
                    .keys()
                    .any(|key| key.starts_with("x-rustfs-encryption-") || key.starts_with("x-minio-encryption-")),
                "disk holds ciphertext under a managed key: {:?}",
                stored.user_defined
            );
            assert_eq!(get_via_app(&bucket, "secret.txt").await, body, "GET returns the plaintext");
        })
        .await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn write_back_reports_a_full_bucket_quota() {
        let (store, bucket) = crate::app::gating_test_env::durable_quota_test_bucket("odm-wb-quota", 64).await;
        let body = vec![0x71; 4096];
        let err = OnDemandMigrationWriteBack::new()
            .put_object(&request(&bucket, "over.bin", source_head(&body)), body_stream(&body))
            .await
            .expect_err("a full quota must reject the write-back");
        assert!(matches!(err, WriteBackError::Quota(_)), "{err}");
        assert_eq!(err.reason(), PullFailureReason::Quota);
        assert_nothing_left(&store, &bucket, "over.bin").await;
    }

    async fn install_replication_rule(bucket: &str) {
        install_replication_rule_with_tag(bucket, None).await;
    }

    async fn install_replication_rule_with_tag(bucket: &str, required_tag: Option<(&str, &str)>) {
        let sys = get_global_bucket_metadata_sys().expect("bucket metadata system");
        let metadata = {
            let sys = sys.read().await;
            sys.get(bucket).await.expect("bucket metadata cached")
        };
        let mut metadata = (*metadata).clone();
        metadata.versioning_config_xml = b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>".to_vec();
        metadata.versioning_config = Some(VersioningConfiguration {
            status: Some(BucketVersioningStatus::from_static(BucketVersioningStatus::ENABLED)),
            ..Default::default()
        });
        let config = ReplicationConfiguration {
            role: String::new(),
            rules: vec![ReplicationRule {
                delete_marker_replication: Some(DeleteMarkerReplication {
                    status: Some(DeleteMarkerReplicationStatus::from_static(DeleteMarkerReplicationStatus::DISABLED)),
                }),
                delete_replication: None,
                destination: Destination {
                    bucket: "arn:aws:s3:::target-bucket".to_string(),
                    ..Default::default()
                },
                existing_object_replication: None,
                filter: required_tag.map(|(key, value)| ReplicationRuleFilter {
                    tag: Some(Tag {
                        key: Some(key.to_string()),
                        value: Some(value.to_string()),
                    }),
                    ..Default::default()
                }),
                id: Some("odm".to_string()),
                prefix: Some(String::new()),
                priority: Some(1),
                source_selection_criteria: None,
                status: ReplicationRuleStatus::from_static(ReplicationRuleStatus::ENABLED),
            }],
        };
        metadata.replication_config_xml = serialize(&config).expect("replication config serializes");
        metadata.replication_config = Some(config);
        set_bucket_metadata(bucket.to_string(), metadata)
            .await
            .expect("install replication rule");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn write_back_schedules_replication_and_names_the_migration_principal() {
        let (store, bucket) = write_back_test_bucket("odm-wb-repl", true).await;
        install_replication_rule(&bucket).await;

        let body = b"replicate me".to_vec();
        let head = source_head(&body);
        let request = request(&bucket, "replicated.txt", head);
        let ctx = write_back_context(&request, true).await;
        assert!(ctx.emit_events, "policy.emit_events reaches the creation event");
        assert_eq!(ctx.principal_id, ON_DEMAND_MIGRATION_PRINCIPAL_ID);
        assert_eq!(ctx.principal_id, "rustfs-on-demand-migration");
        let event =
            InternalPutObjectEvent::builder(EventName::ObjectCreatedPut, &bucket, "replicated.txt", ctx.principal_id).build();
        assert_eq!(event.event_name, EventName::ObjectCreatedPut);
        assert_eq!(
            event.req_params.get("principalId").map(String::as_str),
            Some("rustfs-on-demand-migration")
        );

        let outcome = OnDemandMigrationWriteBack::new()
            .put_object(&request, body_stream(&body))
            .await
            .expect("write-back must commit");
        assert!(outcome.version_id.is_some(), "versioned bucket yields a version id");
        let stored = stored_object(&store, &bucket, "replicated.txt").await;
        // Stored per target as `<arn>=<status>;`; the S3 header is derived from it.
        let status = get_str(&stored.user_defined, SUFFIX_REPLICATION_STATUS).unwrap_or_default();
        assert!(
            status.contains("arn:aws:s3:::target-bucket=PENDING;") || status.contains("arn:aws:s3:::target-bucket=COMPLETED;"),
            "write-back must enter the replication queue: {status:?}"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn write_back_namespaces_source_user_metadata_that_looks_like_replication_bookkeeping() {
        let (store, bucket) = write_back_test_bucket("odm-wb-user-metadata", true).await;
        let target = "arn:aws:s3:::target-bucket";
        install_replication_rule_with_tag(&bucket, Some(("replicate", "yes"))).await;

        let body = b"initially unadmitted ODM object".to_vec();
        let mut head = source_head(&body);
        let forged_status_key = "X-Minio-Internal-Replication-Status";
        let forged_generation_key = "X-RuStFs-InTeRnAl-RePlIcAtIoN-GeNeRaTiOn";
        let forged_replica_key = "X-Minio-Internal-Replica-Status";
        let forged_status = format!("{target}=COMPLETED;");
        let forged_generation = Uuid::from_u128(9003).to_string();
        head.user_metadata
            .insert(forged_status_key.to_string(), forged_status.clone());
        head.user_metadata
            .insert(forged_generation_key.to_string(), forged_generation.clone());
        head.user_metadata
            .insert(forged_replica_key.to_string(), ReplicationStatusType::Replica.as_str().to_string());

        let mut write_request = request(&bucket, "unadmitted.txt", head);
        write_request.tags = Some(HashMap::from([("replicate".to_string(), "no".to_string())]));
        OnDemandMigrationWriteBack::new()
            .put_object(&write_request, body_stream(&body))
            .await
            .expect("ODM write-back with reserved-looking user metadata must commit safely");

        let stored = stored_object(&store, &bucket, "unadmitted.txt").await;
        for suffix in [
            SUFFIX_REPLICATION_STATUS,
            SUFFIX_REPLICATION_GENERATION,
            SUFFIX_REPLICA_STATUS,
        ] {
            assert!(
                !contains_key_str(&stored.user_defined, suffix),
                "source user metadata must not become trusted {suffix} bookkeeping"
            );
        }
        for (key, value) in [
            (forged_status_key, forged_status.as_str()),
            (forged_generation_key, forged_generation.as_str()),
            (forged_replica_key, ReplicationStatusType::Replica.as_str()),
        ] {
            let namespaced = format!("x-amz-meta-{key}");
            assert!(
                stored
                    .user_defined
                    .iter()
                    .any(|(stored_key, stored_value)| stored_key.eq_ignore_ascii_case(&namespaced) && stored_value == value),
                "reserved-looking source metadata must survive only in the user namespace: {namespaced}"
            );
        }

        let later_matching = crate::app::storage_api::object_usecase::bucket::replication::must_replicate_metadata(
            &bucket,
            "unadmitted.txt",
            &stored.user_defined,
            "replicate=yes".to_string(),
            stored.replication_status.clone(),
            ObjectOptions::default(),
        )
        .await;
        assert!(
            !later_matching.replicate_any(),
            "source user metadata must not forge historical target admission after a later matching tag"
        );
    }

    #[test]
    fn content_headers_follow_the_allowlist() {
        let head = SourceHead {
            content_type: Some("image/png".to_string()),
            content_encoding: Some("gzip".to_string()),
            content_disposition: Some("attachment".to_string()),
            content_language: Some(" ".to_string()),
            cache_control: None,
            expires: Some("Thu, 01 Jan 2026 00:00:00 GMT".to_string()),
            storage_class: Some("GLACIER".to_string()),
            ..Default::default()
        };
        let headers = content_headers(&head);
        assert_eq!(
            headers,
            HashMap::from([
                ("Content-Type".to_string(), "image/png".to_string()),
                ("Content-Encoding".to_string(), "gzip".to_string()),
                ("Content-Disposition".to_string(), "attachment".to_string()),
                ("Expires".to_string(), "Thu, 01 Jan 2026 00:00:00 GMT".to_string()),
            ])
        );
    }

    #[test]
    fn expected_md5_only_for_bare_single_part_unencrypted_etags() {
        let mut head = source_head(b"abc");
        assert_eq!(expected_md5_hex(&head), Some(md5_hex(b"abc")));
        head.etag = Some(md5_hex(b"abc").to_ascii_uppercase());
        assert_eq!(expected_md5_hex(&head), Some(md5_hex(b"abc")), "normalized to lowercase");
        head.etag = Some(format!("{}-2", md5_hex(b"abc")));
        assert_eq!(expected_md5_hex(&head), None, "multipart ETag");
        head.etag = Some("not-hex-not-hex-not-hex-not-hex-".to_string());
        assert_eq!(expected_md5_hex(&head), None, "non-hex");
        head.etag = Some(md5_hex(b"abc"));
        head.sse = Some(SourceSse::S3);
        assert_eq!(expected_md5_hex(&head), None, "encrypted source");
        head.sse = None;
        head.etag = None;
        assert_eq!(expected_md5_hex(&head), None);

        // An Azure ETag can be any string the service chooses; even one that
        // happens to look like an MD5 must not be checked against the bytes.
        let mut head = source_head(b"abc");
        head.etag_is_opaque = true;
        assert_eq!(expected_md5_hex(&head), None, "opaque provider ETag");
    }

    #[test]
    fn provenance_and_tags_are_stable() {
        let mut request = request("b", "k", source_head(b"x"));
        let metadata = provenance_metadata(&request);
        assert_eq!(metadata.len(), 10, "five keys under two prefixes");
        assert_provenance(&metadata, &request.head);
        request.head.etag = None;
        request.head.version_id = None;
        request.head.last_modified = None;
        let metadata = provenance_metadata(&request);
        assert_eq!(metadata.len(), 10, "absent values keep the key set constant");
        assert_eq!(get_str(&metadata, SUFFIX_ODM_SOURCE_ETAG).as_deref(), Some(""));
        assert_eq!(get_str(&metadata, SUFFIX_ODM_SOURCE_VERSION_ID).as_deref(), Some(""));
        assert_eq!(get_str(&metadata, SUFFIX_ODM_SOURCE_LAST_MODIFIED).as_deref(), Some(""));

        assert_eq!(encode_tags(&HashMap::new()), None);
        let tags = HashMap::from([("b key".to_string(), "v&2".to_string()), ("a".to_string(), "1".to_string())]);
        assert_eq!(encode_tags(&tags).as_deref(), Some("a=1&b+key=v%262"));
    }

    #[tokio::test]
    async fn write_back_context_applies_the_etag_and_event_policy() {
        let body = b"context".to_vec();
        let mut request = request("no-such-bucket", "k", source_head(&body));
        let ctx = write_back_context(&request, true).await;
        assert_eq!(ctx.expected_md5_hex, Some(md5_hex(&body)));
        assert_eq!(ctx.preserve_etag, Some(md5_hex(&body)));
        assert_eq!(ctx.size, Some(body.len() as u64));
        assert_eq!(ctx.tags.as_deref(), Some("env=prod&team=storage"));
        assert_eq!(ctx.user_metadata.get("origin").map(String::as_str), Some("legacy"));
        assert!(ctx.emit_events);
        assert_eq!(ctx.principal_id, ON_DEMAND_MIGRATION_PRINCIPAL_ID);

        let multipart = write_back_context(&request, false).await;
        assert_eq!(multipart.expected_md5_hex, None, "parts cannot be checked against the object ETag");
        assert_eq!(multipart.preserve_etag, Some(md5_hex(&body)));

        request.preserve_etag = false;
        request.emit_events = false;
        request.tags = None;
        let ctx = write_back_context(&request, true).await;
        assert_eq!(ctx.preserve_etag, None);
        assert_eq!(
            ctx.expected_md5_hex,
            Some(md5_hex(&body)),
            "integrity check is independent of preservation"
        );
        assert!(!ctx.emit_events);
        assert_eq!(ctx.tags, None);
    }

    #[test]
    fn write_back_error_classes_follow_the_api_error() {
        let bad_digest = ApiError {
            code: S3ErrorCode::BadDigest,
            message: "digest".to_string(),
            source: None,
        };
        assert_eq!(write_back_error(bad_digest), WriteBackError::Integrity);
        let quota = ApiError::invalid_request("Bucket quota exceeded. Current usage: 1 bytes, limit: 1 bytes");
        assert!(matches!(write_back_error(quota), WriteBackError::Quota(_)));
        let other = ApiError {
            code: S3ErrorCode::InternalError,
            message: "disk".to_string(),
            source: None,
        };
        assert_eq!(write_back_error(other), WriteBackError::Local("InternalError: disk".to_string()));
    }
}
