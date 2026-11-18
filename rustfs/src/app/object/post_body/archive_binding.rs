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

//! Binds a fully preflighted archive replay to its authenticated POST request.

use super::*;
use rustfs_gateway::{Req, dto};

/// Key rules copied only from the gateway context of the same authenticated form.
pub(crate) struct VerifiedPostArchivePolicy {
    bucket: String,
    key: String,
    permits: Arc<PostArchiveKeyPolicy>,
}

#[cfg(test)]
tokio::task_local! {
    static TEST_ARCHIVE_RESOURCES: Arc<StagingResources>;
}

pub(crate) fn post_archive_admission_closed() -> bool {
    #[cfg(test)]
    if TEST_ARCHIVE_RESOURCES.try_with(|_| ()).is_ok() {
        return false;
    }
    POST_ARCHIVE_SNAPSHOT_LIMIT == 0
}

impl VerifiedPostArchivePolicy {
    pub(crate) fn from_request(request: &Req<dto::PostObject>) -> Option<Self> {
        let context = request.context();
        let input = request.input();
        if context.operation() != "PostObject"
            || context.bucket().map(|bucket| bucket.as_str()) != Some(input.bucket.as_str())
            || context.key().map(|key| key.as_str()) != Some(input.key.as_str())
        {
            return None;
        }
        let constraints = context.post_object_key_constraints()?.clone();
        if !constraints.permits(input.key.as_str()) {
            return None;
        }
        Some(Self {
            bucket: input.bucket.as_str().to_owned(),
            key: input.key.as_str().to_owned(),
            permits: Arc::new(move |key| constraints.permits(key)),
        })
    }
}

/// Owns the exact immutable bytes that passed the signed-key and archive preflight.
pub(crate) struct PreflightedPostArchive {
    snapshot: PreflightedArchiveSnapshot,
    request_identity: Arc<()>,
    bucket: String,
    key: String,
    options: super::super::extract::PutObjectExtractOptions,
    length: i64,
}

impl PreflightedPostArchive {
    pub(crate) fn into_replay(self, envelope: &RequestEnvelope, bucket: &str, key: &str) -> S3Result<(Body, i64)> {
        let identity = envelope
            .gateway_post_identity()
            .ok_or_else(|| s3_error!(AccessDenied, "The POST archive request identity is missing"))?;
        let effective_headers = super::super::post_headers::post_effective_headers(envelope);
        let options = super::super::extract::resolve_put_object_extract_options(effective_headers)
            .map_err(rustfs_s3_types::S3Error::from)?;
        if !Arc::ptr_eq(identity, &self.request_identity)
            || bucket != self.bucket
            || key != self.key
            || options != self.options
            || !super::super::extract::is_put_object_extract_requested(effective_headers)
        {
            return Err(s3_error!(AccessDenied, "The POST archive does not match its preflight"));
        }
        Ok((self.snapshot.into_replay(), self.length))
    }
}

/// Stages, verifies and freezes a POST archive before any member can be written.
pub(crate) async fn stage_post_archive(
    envelope: &RequestEnvelope,
    bucket: &str,
    key: &str,
    body: Body,
    integrity: PostBodyIntegrity,
    policy: VerifiedPostArchivePolicy,
) -> S3Result<PreflightedPostArchive> {
    #[cfg(test)]
    if let Ok(resources) = TEST_ARCHIVE_RESOURCES.try_with(Arc::clone) {
        return stage_post_archive_with_resources(envelope, bucket, key, body, integrity, policy, &resources).await;
    }
    let resources = POST_STAGING
        .get_or_init(StagingResources::new)
        .as_ref()
        .map_err(|_| staging_io_error())?;
    stage_post_archive_with_resources(envelope, bucket, key, body, integrity, policy, resources).await
}

async fn stage_post_archive_with_resources(
    envelope: &RequestEnvelope,
    bucket: &str,
    key: &str,
    body: Body,
    integrity: PostBodyIntegrity,
    policy: VerifiedPostArchivePolicy,
    resources: &StagingResources,
) -> S3Result<PreflightedPostArchive> {
    let effective_headers = super::super::post_headers::post_effective_headers(envelope);
    if bucket != policy.bucket || key != policy.key || !super::super::extract::is_put_object_extract_requested(effective_headers)
    {
        return Err(s3_error!(AccessDenied, "The POST archive does not match its verified policy"));
    }
    let identity = Arc::clone(
        envelope
            .gateway_post_identity()
            .ok_or_else(|| s3_error!(AccessDenied, "The POST archive request identity is missing"))?,
    );
    let options =
        super::super::extract::resolve_put_object_extract_options(effective_headers).map_err(rustfs_s3_types::S3Error::from)?;
    if resources.archive_snapshot_budget.is_closed() {
        return Err(staging_capacity_error());
    }
    let permit = acquire_staging_slot(&resources.slots)?;
    with_staging_deadline(POST_STAGING_TIMEOUT, async {
        let (file, length) = ingest_admitted_post_body(
            envelope,
            &policy.bucket,
            &policy.key,
            (body, Some(integrity)),
            permit,
            Arc::clone(&resources.budget),
            resources.closer.clone(),
        )
        .await?;
        let replay_length = u64::try_from(length).map_err(|_| staging_io_error())?;
        let snapshot = ArchiveSnapshot::capture(file, replay_length, Arc::clone(&resources.archive_snapshot_budget)).await?;
        let snapshot = snapshot
            .preflight(&policy.key, effective_headers, policy.permits.as_ref())
            .await?;
        Ok(PreflightedPostArchive {
            snapshot,
            request_identity: identity,
            bucket: policy.bucket,
            key: policy.key,
            options,
            length,
        })
    })
    .await
}

#[cfg(test)]
fn test_resources() -> S3Result<Arc<StagingResources>> {
    Ok(Arc::new(StagingResources {
        slots: Arc::new(Semaphore::new(1)),
        budget: Arc::new(StagingBudget::new(16 * 1024 * 1024, staging_space)),
        archive_snapshot_budget: ArchiveSnapshotBudget::test(16 * 1024 * 1024),
        closer: staging_closer(1).map_err(|_| staging_io_error())?,
    }))
}

#[cfg(test)]
pub(crate) async fn stage_post_archive_for_test(
    envelope: &RequestEnvelope,
    bucket: &str,
    key: &str,
    body: Body,
    integrity: PostBodyIntegrity,
    policy: VerifiedPostArchivePolicy,
) -> S3Result<PreflightedPostArchive> {
    let resources = test_resources()?;
    stage_post_archive_with_resources(envelope, bucket, key, body, integrity, policy, &resources).await
}

#[cfg(test)]
pub(crate) async fn with_archive_resources_for_test<F: Future>(future: F) -> S3Result<F::Output> {
    let resources = test_resources()?;
    Ok(TEST_ARCHIVE_RESOURCES.scope(resources, future).await)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::RequestHead;
    use crate::app::storage_api::object_usecase::access::AuthorizationRequest as _;
    use http::{Extensions, HeaderValue, Method, Uri};
    use tokio_tar::{Builder, Header};

    fn envelope(prefix: Option<&str>) -> RequestEnvelope {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-meta-snowball-auto-extract", HeaderValue::from_static("true"));
        if let Some(prefix) = prefix {
            headers.insert("x-amz-meta-snowball-prefix", HeaderValue::from_str(prefix).expect("prefix header"));
        }
        let mut envelope = RequestEnvelope::from_parts(
            RequestHead {
                method: Method::POST,
                uri: Uri::from_static("/bucket"),
                headers,
                credentials: None,
                region: None,
                service: None,
            },
            Extensions::new(),
        );
        envelope.mark_gateway_post();
        envelope.bind_gateway_post_archive_identity().expect("gateway POST identity");
        envelope
    }

    fn resources() -> StagingResources {
        StagingResources {
            slots: Arc::new(Semaphore::new(1)),
            budget: Arc::new(StagingBudget::new(1024 * 1024, |_| {
                Ok(StagingSpace {
                    available: 8 * 1024 * 1024 * 1024,
                    total: 8 * 1024 * 1024 * 1024,
                    block_size: 1,
                })
            })),
            archive_snapshot_budget: ArchiveSnapshotBudget::test(1024 * 1024),
            closer: staging_closer(1).expect("staging closer"),
        }
    }

    async fn archive() -> Vec<u8> {
        let mut builder = Builder::new(Vec::new());
        let mut header = Header::new_ustar();
        header.set_size(1);
        header.set_mode(0o644);
        builder
            .append_data(&mut header, "allowed/member", &b"x"[..])
            .await
            .expect("archive member");
        builder.into_inner().await.expect("complete archive")
    }

    async fn preflight(envelope: &RequestEnvelope, resources: &StagingResources) -> PreflightedPostArchive {
        let bytes = archive().await;
        let body = from_io_stream(stream::iter([Ok::<_, io::Error>(Bytes::from(bytes))]));
        let policy = VerifiedPostArchivePolicy {
            bucket: "bucket".to_owned(),
            key: "archive.tar".to_owned(),
            permits: Arc::new(|key| key.starts_with("allowed/")),
        };
        stage_post_archive_with_resources(
            envelope,
            "bucket",
            "archive.tar",
            body,
            PostBodyIntegrity::for_archive(HeaderMap::new(), None, None),
            policy,
            resources,
        )
        .await
        .expect("archive preflight")
    }

    #[tokio::test]
    async fn post_archive_binding_requires_same_request_resource_and_options() {
        for mismatch in ["request", "bucket", "key", "options"] {
            let original = envelope(None);
            let resources = resources();
            let proof = preflight(&original, &resources).await;
            let mut other = original.fork_for_authorization();
            let (bucket, key) = match mismatch {
                "request" => {
                    other = envelope(None);
                    ("bucket", "archive.tar")
                }
                "bucket" => ("other", "archive.tar"),
                "key" => ("bucket", "other.tar"),
                "options" => {
                    other
                        .headers_mut()
                        .insert("x-amz-meta-snowball-prefix", HeaderValue::from_static("other"));
                    ("bucket", "archive.tar")
                }
                _ => unreachable!(),
            };
            let result = proof.into_replay(&other, bucket, key);
            let error = match result {
                Ok(_) => panic!("{mismatch} must not yield archive bytes"),
                Err(error) => error,
            };
            assert_eq!(error.code(), &S3ErrorCode::AccessDenied, "{mismatch}");
        }

        let original = envelope(None);
        let resources = resources();
        let proof = preflight(&original, &resources).await;
        let (body, length) = proof
            .into_replay(&original.fork_for_authorization(), "bucket", "archive.tar")
            .expect("same request and options");
        let (chunks, _) = BodyChunks::with_trailers(body, None);
        let mut reader = StreamReader::new(chunks.map(|chunk| chunk.map_err(s3s_body_error_to_io)));
        let mut actual = Vec::new();
        reader.read_to_end(&mut actual).await.expect("read verified replay");
        assert_eq!(i64::try_from(actual.len()).expect("replay length"), length);
        assert_eq!(actual, archive().await);
    }

    #[tokio::test]
    async fn post_archive_binding_rejects_disallowed_member_before_replay() {
        let envelope = envelope(Some("outside"));
        let resources = resources();
        let body = from_io_stream(stream::iter([Ok::<_, io::Error>(Bytes::from(archive().await))]));
        let policy = VerifiedPostArchivePolicy {
            bucket: "bucket".to_owned(),
            key: "archive.tar".to_owned(),
            permits: Arc::new(|key| key.starts_with("allowed/")),
        };
        let result = stage_post_archive_with_resources(
            &envelope,
            "bucket",
            "archive.tar",
            body,
            PostBodyIntegrity::for_archive(HeaderMap::new(), None, None),
            policy,
            &resources,
        )
        .await;
        let error = match result {
            Ok(_) => panic!("disallowed member must not yield a proof"),
            Err(error) => error,
        };
        assert_eq!(error.code(), &S3ErrorCode::AccessDenied);
    }

    #[tokio::test]
    async fn post_archive_binding_verifies_payload_sha256_before_preflight() {
        use sha2::{Digest as _, Sha256};

        let bytes = archive().await;
        let good = hex_simd::encode_to_string(Sha256::digest(&bytes), hex_simd::AsciiCase::Lower);
        for wrong in [false, true] {
            let envelope = envelope(None);
            let resources = resources();
            let body = from_io_stream(stream::iter([Ok::<_, io::Error>(Bytes::copy_from_slice(&bytes))]));
            let policy = VerifiedPostArchivePolicy {
                bucket: "bucket".to_owned(),
                key: "archive.tar".to_owned(),
                permits: Arc::new(|key| key.starts_with("allowed/")),
            };
            let sha = if wrong { "00".repeat(32) } else { good.clone() };
            let result = stage_post_archive_with_resources(
                &envelope,
                "bucket",
                "archive.tar",
                body,
                PostBodyIntegrity::for_archive(HeaderMap::new(), None, Some(sha)),
                policy,
                &resources,
            )
            .await;
            if wrong {
                let error = match result {
                    Ok(_) => panic!("wrong payload digest must not yield a proof"),
                    Err(error) => error,
                };
                assert_eq!(error.code().as_str(), "XAmzContentSHA256Mismatch");
            } else {
                result.expect("matching payload digest permits preflight");
            }
        }
    }

    #[tokio::test]
    async fn post_archive_binding_refuses_an_independent_body_before_polling() {
        use crate::app::object_usecase::{DefaultObjectUsecase, PutObjectWriteInput};
        use std::sync::atomic::{AtomicBool, Ordering};

        let envelope = envelope(None);
        let resources = resources();
        let proof = preflight(&envelope, &resources).await;
        let polled = Arc::new(AtomicBool::new(false));
        let observed = Arc::clone(&polled);
        let source = stream::iter([Ok::<_, io::Error>(Bytes::from_static(b"replacement"))]).inspect(move |_| {
            observed.store(true, Ordering::SeqCst);
        });
        let input = PutObjectWriteInput {
            bucket: "bucket".to_owned(),
            key: "archive.tar".to_owned(),
            body: Some(from_io_stream(source)),
            post_archive: Some(proof),
            ..Default::default()
        };
        let result = Box::pin(DefaultObjectUsecase::without_context().execute_put_object_extract_owned(envelope, input)).await;
        let error = match result {
            Ok(_) => panic!("an independent body must not accompany the proof"),
            Err(error) => error,
        };
        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
        assert!(!polled.load(Ordering::SeqCst), "the unverified body must not be polled");
    }

    #[tokio::test]
    async fn post_archive_binding_refuses_a_mismatched_policy_before_polling() {
        use std::sync::atomic::{AtomicBool, Ordering};

        for mismatch in ["bucket", "key", "extract"] {
            let mut envelope = envelope(None);
            let (bucket, key) = match mismatch {
                "bucket" => ("other", "archive.tar"),
                "key" => ("bucket", "other.tar"),
                "extract" => {
                    envelope.headers_mut().remove("x-amz-meta-snowball-auto-extract");
                    ("bucket", "archive.tar")
                }
                _ => unreachable!(),
            };
            let polled = Arc::new(AtomicBool::new(false));
            let observed = Arc::clone(&polled);
            let source = stream::iter([Ok::<_, io::Error>(Bytes::from_static(b"untrusted"))]).inspect(move |_| {
                observed.store(true, Ordering::SeqCst);
            });
            let policy = VerifiedPostArchivePolicy {
                bucket: "bucket".to_owned(),
                key: "archive.tar".to_owned(),
                permits: Arc::new(|key| key.starts_with("allowed/")),
            };
            let resources = resources();
            let result = stage_post_archive_with_resources(
                &envelope,
                bucket,
                key,
                from_io_stream(source),
                PostBodyIntegrity::for_archive(HeaderMap::new(), None, None),
                policy,
                &resources,
            )
            .await;
            let error = match result {
                Ok(_) => panic!("{mismatch} must not yield a proof"),
                Err(error) => error,
            };
            assert_eq!(error.code(), &S3ErrorCode::AccessDenied, "{mismatch}");
            assert!(!polled.load(Ordering::SeqCst), "{mismatch} must not poll the body");
        }
    }

    #[tokio::test]
    async fn post_archive_binding_replays_the_preflighted_snapshot_after_disk_changes() {
        let bytes = archive().await;
        let resources = resources();
        let file = Arc::new(StagedFile::new(
            tempfile::tempfile().expect("temporary file"),
            staging_cipher().expect("staging cipher"),
            Arc::clone(&resources.budget),
            acquire_staging_slot(&resources.slots).expect("staging slot"),
            resources.closer.clone(),
        ));
        let chunks: UploadChunks = Box::new(stream::iter([Ok(Bytes::copy_from_slice(&bytes))]));
        copy_post_file(&file, chunks, u64::try_from(bytes.len()).expect("archive length"))
            .await
            .expect("stage archive");
        let snapshot = ArchiveSnapshot::capture(
            Arc::clone(&file),
            u64::try_from(bytes.len()).expect("archive length"),
            Arc::clone(&resources.archive_snapshot_budget),
        )
        .await
        .expect("capture snapshot")
        .preflight("archive.tar", &HeaderMap::new(), &|key| key.starts_with("allowed/"))
        .await
        .expect("preflight snapshot");
        file_io(Arc::clone(&file), |file| {
            file.seek(SeekFrom::Start(4))?;
            file.write_all(&[0; 16])?;
            Ok(())
        })
        .await
        .expect("change the staged file after preflight");
        let (chunks, _) = BodyChunks::with_trailers(snapshot.into_replay(), None);
        let mut reader = StreamReader::new(chunks.map(|chunk| chunk.map_err(s3s_body_error_to_io)));
        let mut replay = Vec::new();
        reader.read_to_end(&mut replay).await.expect("read immutable replay");
        assert_eq!(replay, bytes);
    }

    #[tokio::test]
    async fn post_archive_binding_cancellation_keeps_snapshot_budget_until_io_finishes() {
        let bytes = archive().await;
        let resources = resources();
        let file = Arc::new(StagedFile::new(
            tempfile::tempfile().expect("temporary file"),
            staging_cipher().expect("staging cipher"),
            Arc::clone(&resources.budget),
            acquire_staging_slot(&resources.slots).expect("staging slot"),
            resources.closer.clone(),
        ));
        let chunks: UploadChunks = Box::new(stream::iter([Ok(Bytes::copy_from_slice(&bytes))]));
        let length = u64::try_from(bytes.len()).expect("archive length");
        copy_post_file(&file, chunks, length).await.expect("stage archive");
        let budget = Arc::clone(&resources.archive_snapshot_budget);
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel::<()>(1);
        let worker_file = Arc::clone(&file);
        let worker_budget = Arc::clone(&budget);
        let task = tokio::spawn(async move {
            ArchiveSnapshot::capture_with_hook(worker_file, length, worker_budget, move || {
                started_tx
                    .send(())
                    .map_err(|_| io::Error::other("snapshot observer closed"))?;
                release_rx.recv_timeout(Duration::from_secs(5)).map_err(io::Error::other)?;
                Ok(())
            })
            .await
        });
        tokio::time::timeout(Duration::from_secs(5), started_rx)
            .await
            .expect("snapshot reached the blocking hook")
            .expect("snapshot worker started");
        let reserved = budget.reserved();
        assert!(reserved > 0, "snapshot I/O must reserve memory");
        task.abort();
        let cancellation = match task.await {
            Ok(_) => panic!("snapshot task must be cancelled"),
            Err(error) => error,
        };
        assert!(cancellation.is_cancelled());
        assert_eq!(budget.reserved(), reserved, "blocking I/O still owns the reservation");
        release_tx.send(()).expect("release snapshot I/O");
        tokio::time::timeout(Duration::from_secs(5), async {
            while budget.reserved() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("reservation returns after I/O completes");
    }
}
