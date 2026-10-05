// Copyright 2024 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::model::{Protection, fingerprint, protection, sha256_bytes, stored_sha256, unsupported};
use super::{IntegrityError, ItemState, Job, JobMode, JobState, Result, load, readiness, save};
use crate::object_api::{
    ObjectInfo, ObjectOptions, PutObjReader, ShardIntegrityWriteMode, WriteCompletion, without_get_object_body_cache_hook,
};
use crate::storage_api_contracts::object::{HTTPPreconditions, ObjectIO, ObjectOperations};
use crate::store::ECStore;
use rustfs_rio::{Checksum, HashReader};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

const MARKER: &str = "integrity-migration-job-v1";

pub(super) async fn run(
    api: &Arc<ECStore>,
    job: &mut Job,
    mut etag: String,
    guard: &rustfs_lock::NamespaceLockGuard,
) -> Result<()> {
    without_get_object_body_cache_hook(async {
        for index in 0..job.results.len() {
            ensure_running(api, job, guard).await?;
            if !matches!(
                job.results[index].state,
                ItemState::Pending | ItemState::Prepared | ItemState::Unavailable
            ) {
                continue;
            }
            if job.request.mode == JobMode::Migrate {
                validate_destination(api, job).await?;
            }
            if job.results[index].state == ItemState::Prepared
                && job.request.mode == JobMode::Migrate
                && reconcile_target(api, job, index, guard).await?
            {
                etag = checkpoint(api, job, &etag, guard).await?;
                continue;
            }
            let request = job.request.items[index].clone();
            let opts = ObjectOptions {
                version_id: request.version_id.as_ref().map(|version| {
                    if version == "null" {
                        uuid::Uuid::nil().to_string()
                    } else {
                        version.clone()
                    }
                }),
                include_part_checksums: true,
                suppress_read_repair: true,
                ..Default::default()
            };
            let info = match api.get_object_info(&job.bucket, &request.key, &opts).await {
                Ok(info) => info,
                Err(_) => {
                    job.results[index].state = ItemState::Unavailable;
                    job.results[index].detail = Some("source_metadata_unavailable".into());
                    etag = checkpoint(api, job, &etag, guard).await?;
                    continue;
                }
            };
            if let Some(reason) = unsupported(&info) {
                job.results[index].state = ItemState::Unsupported;
                job.results[index].detail = Some(reason.into());
                etag = checkpoint(api, job, &etag, guard).await?;
                continue;
            }
            let identity = fingerprint(&info);
            if job.results[index]
                .source_fingerprint
                .as_ref()
                .is_some_and(|old| old != &identity)
            {
                job.results[index].state = ItemState::Stale;
                etag = checkpoint(api, job, &etag, guard).await?;
                continue;
            }
            let expected = request.expected_sha256.clone().or_else(|| stored_sha256(&info));
            let Some(expected) = expected else {
                job.results[index].state = ItemState::Unsupported;
                job.results[index].detail = Some("no_canonical_single_part_sha256".into());
                etag = checkpoint(api, job, &etag, guard).await?;
                continue;
            };
            let size = u64::try_from(info.size).map_err(|_| IntegrityError::Invalid("negative source size"))?;
            if size > job.request.max_object_bytes {
                job.results[index].state = ItemState::Unsupported;
                job.results[index].detail = Some("object_exceeds_staging_budget".into());
                etag = checkpoint(api, job, &etag, guard).await?;
                continue;
            }
            let result = &mut job.results[index];
            result.source_fingerprint = Some(identity);
            result.expected_sha256 = Some(expected.clone());
            result.evidence_source = Some(
                if request.expected_sha256.is_some() {
                    "administrator_supplied_sha256"
                } else {
                    "stored_sha256"
                }
                .into(),
            );
            result.source_size = Some(size);
            result.state = ItemState::Prepared;
            result.detail = None;
            etag = checkpoint(api, job, &etag, guard).await?;
            let mut file = match stage(api, job, index, &info, &opts, guard).await {
                Ok(file) => file,
                Err(IntegrityError::Conflict) => {
                    job.results[index].state = ItemState::Stale;
                    etag = checkpoint(api, job, &etag, guard).await?;
                    continue;
                }
                Err(IntegrityError::Invalid("content_sha256_mismatch")) => {
                    job.results[index].state = ItemState::Mismatch;
                    etag = checkpoint(api, job, &etag, guard).await?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            ensure_running(api, job, guard).await?;
            if job.request.mode == JobMode::Audit {
                job.results[index].state = ItemState::Verified;
            } else {
                let snapshot = validate_destination(api, job).await?;
                file.rewind().await?;
                let raw = sha256_bytes(&expected)?;
                let expected_hex = hex_simd::encode_to_string(raw, hex_simd::AsciiCase::Lower);
                let mut reader = HashReader::from_stream(
                    ThrottledFile::new(file, job.request.bytes_per_second),
                    info.size,
                    info.size,
                    None,
                    Some(expected_hex),
                    false,
                )?;
                reader.add_non_trailing_checksum(Checksum::new_from_string("SHA256", &expected), false)?;
                let mut body = PutObjReader::new(reader);
                let mut metadata: HashMap<String, String> = info
                    .user_defined
                    .iter()
                    .filter(|(key, _)| {
                        key.starts_with("x-amz-meta-")
                            || matches!(key.as_str(), "cache-control" | "content-disposition" | "content-language" | "expires")
                    })
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect();
                if let Some(content_type) = &info.content_type {
                    metadata.insert("content-type".into(), content_type.clone());
                }
                if let Some(content_encoding) = &info.content_encoding {
                    metadata.insert("content-encoding".into(), content_encoding.clone());
                }
                if let Some(expires) = info.expires {
                    metadata.insert(
                        "expires".into(),
                        expires
                            .format(&time::format_description::well_known::Rfc3339)
                            .map_err(|_| IntegrityError::Invalid("invalid source expiration"))?,
                    );
                }
                if let Some(storage_class) = &info.storage_class {
                    metadata.insert("x-amz-storage-class".into(), storage_class.clone());
                }
                if !info.user_tags.is_empty() {
                    metadata.insert(rustfs_utils::http::headers::AMZ_OBJECT_TAGGING.into(), info.user_tags.to_string());
                }
                rustfs_utils::http::insert_str(&mut metadata, MARKER, marker(job, index)?);
                let mut write = ObjectOptions {
                    user_defined: metadata,
                    preserve_delete_marker: true,
                    shard_integrity_write_mode: Some(ShardIntegrityWriteMode::Protected),
                    write_completion: WriteCompletion::TailDrained,
                    http_preconditions: Some(HTTPPreconditions {
                        if_none_match: Some("*".into()),
                        ..Default::default()
                    }),
                    ..Default::default()
                };
                write.add_namespace_lock_guard(guard);
                write.expected_bucket_incarnation_id = Some(job.bucket_incarnation);
                snapshot.add_lock_fences(&mut write);
                write.object_lock_config_snapshot = Some(snapshot.clone());
                let target = request
                    .target_key
                    .as_deref()
                    .ok_or(IntegrityError::Invalid("missing target"))?;
                // Even an error may follow a committed PUT. Reconcile before retrying; never overwrite.
                let put = api.put_object(&job.bucket, target, &mut body, &write).await;
                drop(write);
                drop(snapshot);
                if !reconcile_target(api, job, index, guard).await? {
                    put?;
                    return Err(IntegrityError::Conflict);
                }
            }
            etag = checkpoint(api, job, &etag, guard).await?;
        }
        job.state = if job.results.iter().any(|item| item.state == ItemState::Unavailable) {
            job.last_error = Some("source_unavailable_retry_requires_resume".into());
            JobState::Failed
        } else {
            JobState::Complete
        };
        checkpoint(api, job, &etag, guard).await?;
        Ok(())
    })
    .await
}

async fn checkpoint(api: &Arc<ECStore>, job: &mut Job, etag: &str, guard: &rustfs_lock::NamespaceLockGuard) -> Result<String> {
    ensure_running(api, job, guard).await?;
    job.revision = job.revision.checked_add(1).ok_or(IntegrityError::Conflict)?;
    save(api, job, Some(etag), Some(guard)).await
}

async fn ensure_running(api: &Arc<ECStore>, job: &Job, guard: &rustfs_lock::NamespaceLockGuard) -> Result<()> {
    if guard.lock_lost_signal().is_some_and(|signal| signal.is_lost()) {
        return Err(IntegrityError::Stopped);
    }
    let (current, _) = load(api, &job.bucket, job.id).await?;
    if current.state != JobState::Running {
        return Err(IntegrityError::Stopped);
    }
    if current.revision != job.revision {
        return Err(IntegrityError::Conflict);
    }
    Ok(())
}

async fn stage(
    api: &Arc<ECStore>,
    job: &Job,
    index: usize,
    info: &ObjectInfo,
    opts: &ObjectOptions,
    guard: &rustfs_lock::NamespaceLockGuard,
) -> Result<tokio::fs::File> {
    let mut reader = api
        .get_object_reader(&job.bucket, &job.request.items[index].key, None, Default::default(), opts)
        .await?;
    if fingerprint(&reader.object_info) != fingerprint(info) {
        return Err(IntegrityError::Conflict);
    }
    let std_file = tokio::task::spawn_blocking(tempfile::tempfile)
        .await
        .map_err(|_| IntegrityError::Invalid("staging task failed"))??;
    let mut file = tokio::fs::File::from_std(std_file);
    let expected = job.results[index]
        .expected_sha256
        .as_deref()
        .ok_or(IntegrityError::Invalid("missing expected digest"))?;
    copy_verified(api, job, &mut reader.stream, Some(&mut file), info.size, expected, guard).await?;
    file.flush().await?;
    let after = api.get_object_info(&job.bucket, &job.request.items[index].key, opts).await?;
    if fingerprint(&after) != fingerprint(info) {
        return Err(IntegrityError::Conflict);
    }
    Ok(file)
}

async fn copy_verified(
    api: &Arc<ECStore>,
    job: &Job,
    input: &mut (dyn tokio::io::AsyncRead + Unpin + Send + Sync),
    mut output: Option<&mut tokio::fs::File>,
    size: i64,
    expected: &str,
    guard: &rustfs_lock::NamespaceLockGuard,
) -> Result<()> {
    let expected = sha256_bytes(expected)?;
    let expected_size = u64::try_from(size).map_err(|_| IntegrityError::Invalid("negative object size"))?;
    let mut buffer = vec![0u8; 64 * 1024];
    let mut count = 0u64;
    let mut hash = Sha256::new();
    let started = tokio::time::Instant::now();
    let mut checked = started;
    loop {
        if checked.elapsed() >= Duration::from_secs(1) {
            ensure_running(api, job, guard).await?;
            checked = tokio::time::Instant::now();
        }
        let n = tokio::time::timeout(Duration::from_secs(30), input.read(&mut buffer))
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "integrity read timed out"))??;
        if n == 0 {
            break;
        }
        count = count
            .checked_add(u64::try_from(n).map_err(|_| IntegrityError::Invalid("read size overflow"))?)
            .ok_or(IntegrityError::Invalid("read size overflow"))?;
        if count > expected_size || count > job.request.max_object_bytes {
            return Err(IntegrityError::Invalid("content_sha256_mismatch"));
        }
        hash.update(&buffer[..n]);
        if let Some(file) = output.as_deref_mut() {
            file.write_all(&buffer[..n]).await?;
        }
        let delay = Duration::from_secs_f64(count as f64 / job.request.bytes_per_second as f64);
        tokio::time::sleep_until(started + delay).await;
    }
    if count != expected_size || hash.finalize().as_slice() != expected {
        return Err(IntegrityError::Invalid("content_sha256_mismatch"));
    }
    Ok(())
}

fn marker(job: &Job, index: usize) -> Result<String> {
    let result = &job.results[index];
    Ok(format!(
        "{}:{}:{}:{}",
        job.id,
        index,
        result
            .source_fingerprint
            .as_deref()
            .ok_or(IntegrityError::Invalid("missing source identity"))?,
        result
            .expected_sha256
            .as_deref()
            .ok_or(IntegrityError::Invalid("missing source digest"))?
    ))
}

async fn reconcile_target(
    api: &Arc<ECStore>,
    job: &mut Job,
    index: usize,
    guard: &rustfs_lock::NamespaceLockGuard,
) -> Result<bool> {
    let key = job.request.items[index]
        .target_key
        .as_deref()
        .ok_or(IntegrityError::Invalid("missing target"))?;
    let opts = ObjectOptions {
        include_part_checksums: true,
        suppress_read_repair: true,
        ..Default::default()
    };
    let target = match api.get_object_info(&job.bucket, key, &opts).await {
        Ok(info) => info,
        Err(crate::error::Error::FileNotFound | crate::error::Error::ObjectNotFound(_, _)) => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    let expected_marker = marker(job, index)?;
    if rustfs_utils::http::get_consistent_str(&target.user_defined, MARKER) != Some(expected_marker.as_str()) {
        job.results[index].state = ItemState::Conflict;
        job.results[index].detail = Some("target_owned_by_another_operation".into());
        return Ok(true);
    }
    if protection(&target) != Protection::IndependentCommitment {
        return Err(IntegrityError::Invalid("target_not_protected"));
    }
    let mut reader = api
        .get_object_reader(&job.bucket, key, None, Default::default(), &opts)
        .await?;
    if fingerprint(&target) != fingerprint(&reader.object_info) {
        return Err(IntegrityError::Conflict);
    }
    let size = job.results[index]
        .source_size
        .ok_or(IntegrityError::Invalid("missing source size"))?;
    let expected = job.results[index]
        .expected_sha256
        .clone()
        .ok_or(IntegrityError::Invalid("missing source digest"))?;
    copy_verified(
        api,
        job,
        &mut reader.stream,
        None,
        i64::try_from(size).map_err(|_| IntegrityError::Invalid("source size overflow"))?,
        &expected,
        guard,
    )
    .await?;
    let after = api.get_object_info(&job.bucket, key, &opts).await?;
    if fingerprint(&after) != fingerprint(&target) {
        return Err(IntegrityError::Conflict);
    }
    job.results[index].state = ItemState::Migrated;
    job.results[index].target_version_id = target.version_id;
    Ok(true)
}

async fn validate_destination(api: &Arc<ECStore>, job: &Job) -> Result<Arc<crate::object_api::ObjectLockConfigSnapshot>> {
    if !readiness().new_writes_enabled_here {
        return Err(IntegrityError::NotActivated);
    }
    let snapshot = api.object_lock_config_snapshot(&job.bucket).await?;
    if snapshot
        .metadata_transaction_guard_for(api.id, &job.bucket, Some(job.bucket_incarnation))
        .is_none()
    {
        return Err(IntegrityError::Conflict);
    }
    let metadata = crate::bucket::metadata_sys::integrity_migration_metadata_in(&api.ctx, &job.bucket).await?;
    // The direct storage writer is not the S3 orchestration layer. Reject configurations it cannot preserve.
    if metadata.bucket_incarnation_id != job.bucket_incarnation {
        return Err(IntegrityError::Conflict);
    }
    if !metadata.versioning_config_xml.is_empty()
        || !metadata.encryption_config_xml.is_empty()
        || !metadata.object_lock_config_xml.is_empty()
        || !metadata.replication_config_xml.is_empty()
        || !metadata.notification_config_xml.is_empty()
        || !metadata.quota_config_json.is_empty()
        || !metadata.on_demand_migration_config_json.is_empty()
        || !metadata.lifecycle_config_xml.is_empty()
        || !metadata.table_bucket_config_json.is_empty()
        || !metadata.bucket_acl_config_json.is_empty()
        || !metadata.logging_config_xml.is_empty()
    {
        return Err(IntegrityError::UnsupportedBucket);
    }
    Ok(snapshot)
}

/// Bound publication throughput as well as the source and destination verification reads.
struct ThrottledFile {
    file: tokio::fs::File,
    started: tokio::time::Instant,
    count: u64,
    rate: u64,
    sleep: std::pin::Pin<Box<tokio::time::Sleep>>,
}

impl ThrottledFile {
    fn new(file: tokio::fs::File, rate: u64) -> Self {
        let started = tokio::time::Instant::now();
        Self {
            file,
            started,
            count: 0,
            rate,
            sleep: Box::pin(tokio::time::sleep_until(started)),
        }
    }
}

impl tokio::io::AsyncRead for ThrottledFile {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        output: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use std::future::Future;
        if self.sleep.as_mut().poll(cx).is_pending() {
            return std::task::Poll::Pending;
        }
        let capacity = output.remaining().min(64 * 1024);
        let mut buffer = tokio::io::ReadBuf::new(output.initialize_unfilled_to(capacity));
        match std::pin::Pin::new(&mut self.file).poll_read(cx, &mut buffer) {
            std::task::Poll::Ready(Ok(())) => {
                let count = buffer.filled().len();
                output.advance(count);
                self.count += count as u64;
                let deadline = self.started + Duration::from_secs_f64(self.count as f64 / self.rate as f64);
                self.sleep.as_mut().reset(deadline);
                std::task::Poll::Ready(Ok(()))
            }
            result => result,
        }
    }
}
