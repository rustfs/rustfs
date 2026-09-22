// Copyright 2024 RustFS Team
// Licensed under the Apache License, Version 2.0.

//! Explicit, bounded integrity operations. This service does not participate in ordinary GET or Heal.
//! Lock order: cluster worker lock, bucket incarnation fence, object commit lock.
//! Control requests use checkpoint CAS without taking the worker lock; a worker that loses CAS stops.

mod model;
mod runner;
#[cfg(test)]
mod tests;

pub use model::{
    InventoryItem, InventoryPage, ItemRequest, ItemResult, ItemState, Job, JobMode, JobRequest, JobState, Protection, Readiness,
    readiness,
};

use crate::config::com::{read_config_limited_preserve_empty_with_metadata, save_config_with_opts_quiet};
use crate::disk::RUSTFS_META_BUCKET;
use crate::error::Error as StorageError;
use crate::object_api::{ObjectOptions, WriteCompletion};
use crate::storage_api_contracts::{
    list::ListOperations,
    namespace::NamespaceLocking,
    object::{HTTPPreconditions, ObjectOperations},
};
use crate::store::ECStore;
use model::InventoryItem as Item;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

type Result<T> = std::result::Result<T, IntegrityError>;

#[derive(Debug, thiserror::Error)]
pub enum IntegrityError {
    #[error("invalid integrity request: {0}")]
    Invalid(&'static str),
    #[error("integrity job or target changed concurrently")]
    Conflict,
    #[error("an integrity worker is already active")]
    Busy,
    #[error("integrity job not found")]
    NotFound,
    #[error("integrity operation was stopped")]
    Stopped,
    #[error("protected writes are not enabled on this coordinator")]
    NotActivated,
    #[error("unsupported migration bucket configuration")]
    UnsupportedBucket,
    #[error("integrity storage operation failed: {0}")]
    Storage(#[from] StorageError),
    #[error("integrity stream operation failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("integrity checkpoint is malformed: {0}")]
    Json(#[from] serde_json::Error),
}

fn check_bucket(bucket: &str) -> Result<()> {
    crate::bucket::utils::check_valid_bucket_name_strict(bucket).map_err(|_| IntegrityError::Invalid("invalid source bucket"))
}

fn path(bucket: &str, id: Uuid) -> String {
    format!("buckets/{bucket}/integrity-jobs/{id}.json")
}

pub async fn inventory(
    api: Arc<ECStore>,
    bucket: &str,
    prefix: &str,
    key_marker: Option<String>,
    version_marker: Option<String>,
    limit: i32,
) -> Result<InventoryPage> {
    check_bucket(bucket)?;
    if !(1..=100).contains(&limit) || prefix.len() > 1024 || (version_marker.is_some() && key_marker.is_none()) {
        return Err(IntegrityError::Invalid("invalid inventory page"));
    }
    let incarnation = api.bucket_incarnation_id_from_disk(bucket).await?;
    let page = api
        .clone()
        .list_object_versions(bucket, prefix, key_marker.clone(), version_marker.clone(), None, limit)
        .await?;
    if page.is_truncated && (page.next_marker.clone(), page.next_version_idmarker.clone()) == (key_marker, version_marker) {
        return Err(IntegrityError::Invalid("inventory cursor did not advance"));
    }
    let mut items = Vec::with_capacity(page.objects.len());
    for listed in page.objects {
        if listed.delete_marker {
            items.push(Item::from_info(&listed));
            continue;
        }
        let opts = ObjectOptions {
            version_id: Some(listed.version_id.unwrap_or_else(Uuid::nil).to_string()),
            include_part_checksums: true,
            suppress_read_repair: true,
            ..Default::default()
        };
        match api.get_object_info(bucket, &listed.name, &opts).await {
            Ok(info) => items.push(Item::from_info(&info)),
            Err(_) => {
                let mut item = Item::from_info(&listed);
                item.observation_error = Some("metadata_unavailable_or_changed".into());
                item.protection = Protection::Unknown;
                item.audit_unsupported = Some("metadata_unavailable_or_changed".into());
                items.push(item);
            }
        }
    }
    if api.bucket_incarnation_id_from_disk(bucket).await? != incarnation {
        return Err(IntegrityError::Conflict);
    }
    Ok(InventoryPage {
        observed_at: time::OffsetDateTime::now_utc().to_string(),
        bucket_incarnation: incarnation,
        items,
        is_truncated: page.is_truncated,
        next_key_marker: page.next_marker,
        next_version_marker: page.next_version_idmarker,
    })
}

pub async fn create_job(api: Arc<ECStore>, bucket: &str, request: JobRequest) -> Result<Job> {
    check_bucket(bucket)?;
    request.validate()?;
    if request.mode == JobMode::Migrate && !readiness().new_writes_enabled_here {
        return Err(IntegrityError::NotActivated);
    }
    let incarnation = api.bucket_incarnation_id_from_disk(bucket).await?;
    let job = Job {
        format_version: 1,
        id: Uuid::new_v4(),
        bucket: bucket.into(),
        bucket_incarnation: incarnation,
        results: vec![ItemResult::default(); request.items.len()],
        request,
        state: JobState::Paused,
        revision: 0,
        last_error: None,
    };
    save(&api, &job, None, None).await?;
    Ok(job)
}

pub async fn get_job(api: Arc<ECStore>, bucket: &str, id: Uuid) -> Result<Job> {
    Ok(load(&api, bucket, id).await?.0)
}

async fn load(api: &Arc<ECStore>, bucket: &str, id: Uuid) -> Result<(Job, String)> {
    check_bucket(bucket)?;
    let (bytes, info) = match read_config_limited_preserve_empty_with_metadata(api.clone(), &path(bucket, id), 1024 * 1024).await
    {
        Ok(value) => value,
        Err(StorageError::ConfigNotFound | StorageError::FileNotFound) => return Err(IntegrityError::NotFound),
        Err(error) => return Err(error.into()),
    };
    let job: Job = serde_json::from_slice(&bytes)?;
    job.request.validate()?;
    if job.format_version != 1
        || job.bucket != bucket
        || job.id != id
        || job.results.len() != job.request.items.len()
        || api.bucket_incarnation_id_from_disk(bucket).await? != job.bucket_incarnation
    {
        return Err(IntegrityError::Conflict);
    }
    Ok((job, info.etag.ok_or(IntegrityError::Conflict)?))
}

async fn save(
    api: &Arc<ECStore>,
    job: &Job,
    etag: Option<&str>,
    worker: Option<&rustfs_lock::NamespaceLockGuard>,
) -> Result<String> {
    let fence = api
        .acquire_bucket_incarnation_fence(&job.bucket, job.bucket_incarnation)
        .await?;
    let mut opts = ObjectOptions {
        max_parity: true,
        write_completion: WriteCompletion::TailDrained,
        http_preconditions: Some(match etag {
            Some(value) => HTTPPreconditions {
                if_match: Some(value.into()),
                ..Default::default()
            },
            None => HTTPPreconditions {
                if_none_match: Some("*".into()),
                ..Default::default()
            },
        }),
        ..Default::default()
    };
    fence.attach_to_object_options(&mut opts);
    if let Some(guard) = worker {
        opts.add_namespace_lock_guard(guard);
    }
    let api = api.clone();
    let job = job.clone();
    // Keep the lifecycle guard through the drained commit even if the HTTP waiter is cancelled.
    tokio::spawn(async move {
        save_config_with_opts_quiet(api.clone(), &path(&job.bucket, job.id), serde_json::to_vec(&job)?, &opts).await?;
        let (stored, next) = load(&api, &job.bucket, job.id).await?;
        if stored.revision != job.revision {
            return Err(IntegrityError::Conflict);
        }
        drop(fence);
        Ok(next)
    })
    .await
    .map_err(|_| IntegrityError::Invalid("checkpoint commit task failed"))?
}

/// Pause/cancel acknowledge intent. An in-flight conditional publication is allowed to drain.
pub async fn control_job(api: Arc<ECStore>, bucket: &str, id: Uuid, cancel: bool) -> Result<Job> {
    let (mut job, etag) = load(&api, bucket, id).await?;
    match job.state {
        JobState::Complete | JobState::Cancelled | JobState::CancelRequested => return Ok(job),
        JobState::Paused | JobState::Failed => job.state = if cancel { JobState::Cancelled } else { JobState::Paused },
        _ => {
            job.state = if cancel {
                JobState::CancelRequested
            } else {
                JobState::PauseRequested
            }
        }
    }
    job.revision = job.revision.checked_add(1).ok_or(IntegrityError::Conflict)?;
    save(&api, &job, Some(&etag), None).await?;
    Ok(job)
}

/// Resume is explicit after restart. The distributed lock excludes another worker, including another node.
pub async fn resume_job(api: Arc<ECStore>, bucket: &str, id: Uuid) -> Result<Job> {
    check_bucket(bucket)?;
    let lock = api.new_ns_lock(RUSTFS_META_BUCKET, "integrity-jobs/worker.lock").await?;
    let guard = lock
        .get_write_lock_quiet(Duration::from_secs(1))
        .await
        .map_err(|_| IntegrityError::Busy)?;
    let (mut job, etag) = load(&api, bucket, id).await?;
    if matches!(job.state, JobState::Complete | JobState::Cancelled) {
        return Ok(job);
    }
    if job.state == JobState::CancelRequested {
        job.state = JobState::Cancelled;
        job.revision = job.revision.checked_add(1).ok_or(IntegrityError::Conflict)?;
        save(&api, &job, Some(&etag), Some(&guard)).await?;
        return Ok(job);
    }
    if job.request.mode == JobMode::Migrate && !readiness().new_writes_enabled_here {
        return Err(IntegrityError::NotActivated);
    }
    job.state = JobState::Running;
    job.last_error = None;
    job.revision = job.revision.checked_add(1).ok_or(IntegrityError::Conflict)?;
    let etag = save(&api, &job, Some(&etag), Some(&guard)).await?;
    let response = job.clone();
    tokio::spawn(async move {
        let result = runner::run(&api, &mut job, etag, &guard).await;
        if let Err(error) = result {
            // A failed write/CAS stops publication. Never overwrite a concurrent control request.
            if let Ok((mut latest, etag)) = load(&api, &job.bucket, job.id).await {
                match latest.state {
                    JobState::PauseRequested => latest.state = JobState::Paused,
                    JobState::CancelRequested => latest.state = JobState::Cancelled,
                    JobState::Running if latest.revision <= job.revision => {
                        latest.state = JobState::Failed;
                        latest.last_error = Some(
                            match error {
                                IntegrityError::Conflict => "concurrent_change",
                                IntegrityError::Stopped => "worker_stopped",
                                IntegrityError::UnsupportedBucket => "unsupported_destination_bucket_configuration",
                                IntegrityError::NotActivated => "protected_writes_not_activated",
                                _ => "operation_failed_retry_requires_resume",
                            }
                            .into(),
                        );
                    }
                    _ => return,
                }
                if let Some(revision) = latest.revision.checked_add(1) {
                    latest.revision = revision;
                    let _ = save(&api, &latest, Some(&etag), Some(&guard)).await;
                }
            }
        }
    });
    Ok(response)
}
