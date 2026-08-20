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

use super::*;
use std::sync::Arc;

pub(super) enum DataUsageCacheLoadAttempt {
    Loaded {
        cache: Box<DataUsageCache>,
        revision: Option<DataUsageCacheRevision>,
    },
    Missing {
        revision: Option<DataUsageCacheRevision>,
    },
    Corrupt {
        revision: Option<DataUsageCacheRevision>,
    },
    Retryable(Error),
}

struct DataUsageCacheLoadResult {
    cache: DataUsageCache,
    main_revision: DataUsageCacheRevision,
    backup_revision: Option<DataUsageCacheRevision>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DataUsageCacheLoadState {
    Loaded,
    Missing,
    Corrupt,
    Retryable,
}

impl DataUsageCacheLoadAttempt {
    fn revision(&self) -> Option<DataUsageCacheRevision> {
        match self {
            Self::Loaded { revision, .. } | Self::Missing { revision } | Self::Corrupt { revision } => revision.clone(),
            Self::Retryable(_) => None,
        }
    }
}

// Data usage paths (computed at runtime)

impl DataUsageCache {
    /// Only backend errors are returned as errors.
    /// The loader is optimistic and has no locking, but tries 5 times before giving up.
    /// If the object is not found, a nil error with empty data usage cache is returned.
    pub async fn load<S: ScannerObjectIO>(&mut self, store: Arc<S>, name: &str) -> StorageResult<()> {
        let loaded = Self::load_cache(store, name).await?;
        *self = loaded.cache;
        Ok(())
    }

    pub(crate) async fn load_with_revisions<S: ScannerObjectIO>(
        &mut self,
        store: Arc<S>,
        name: &str,
    ) -> StorageResult<DataUsageCacheRevisions> {
        let backup_name = format!("{name}.bkp");
        let backup_path = path_join_buf(&[BUCKET_META_PREFIX, &backup_name]);
        let loaded = Self::load_cache(store.clone(), name).await?;
        let backup = match loaded.backup_revision {
            Some(revision) => Some(revision),
            None => match Self::revision_for_path(store, &backup_path).await {
                Ok(revision) => Some(revision),
                Err(err) => {
                    counter!(METRIC_CACHE_BACKUP_REVISION_FAILURE_TOTAL).increment(1);
                    debug!(
                        target: "rustfs::scanner::data_usage",
                        event = EVENT_SCANNER_CACHE_LOAD_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_CACHE,
                        cache_name = %name,
                        backup_path = %backup_path,
                        state = "backup_revision_unavailable",
                        error = %err,
                        "Scanner cache backup revision lookup failed"
                    );
                    None
                }
            },
        };
        let main = loaded.main_revision;
        *self = loaded.cache;

        Ok(DataUsageCacheRevisions { main, backup })
    }

    async fn load_cache<S: ScannerObjectIO>(store: Arc<S>, name: &str) -> StorageResult<DataUsageCacheLoadResult> {
        let mut last_retryable = None;

        for attempt in 0..5 {
            let main_attempt = Self::try_load_inner(store.clone(), name, Duration::from_secs(60)).await?;
            let main_revision = main_attempt.revision();
            let main_state = match main_attempt {
                DataUsageCacheLoadAttempt::Loaded {
                    cache,
                    revision: Some(main_revision),
                } => {
                    return Ok(DataUsageCacheLoadResult {
                        cache: *cache,
                        main_revision,
                        backup_revision: None,
                    });
                }
                DataUsageCacheLoadAttempt::Loaded { revision: None, .. } => {
                    last_retryable = Some(Error::other(format!("scanner cache object has no revision: {name}")));
                    DataUsageCacheLoadState::Retryable
                }
                DataUsageCacheLoadAttempt::Missing { .. } => DataUsageCacheLoadState::Missing,
                DataUsageCacheLoadAttempt::Corrupt { .. } => DataUsageCacheLoadState::Corrupt,
                DataUsageCacheLoadAttempt::Retryable(err) => {
                    last_retryable = Some(err);
                    DataUsageCacheLoadState::Retryable
                }
            };
            if main_state == DataUsageCacheLoadState::Retryable {
                if attempt < 4 {
                    let sleep_ms: u64 = rand::random::<u64>() % 1000;
                    sleep(Duration::from_millis(sleep_ms)).await;
                }
                continue;
            }

            let backup_name = format!("{name}.bkp");
            let backup_attempt = Self::try_load_inner(store.clone(), &backup_name, Duration::from_secs(30)).await?;
            let backup_revision = backup_attempt.revision();
            let backup_state = match backup_attempt {
                DataUsageCacheLoadAttempt::Loaded {
                    cache,
                    revision: Some(backup_revision),
                } => {
                    if matches!(main_state, DataUsageCacheLoadState::Missing | DataUsageCacheLoadState::Corrupt) {
                        let main_revision = main_revision.ok_or_else(|| {
                            Error::other(format!("scanner cache main revision is unavailable while loading backup: {name}"))
                        })?;
                        return Ok(DataUsageCacheLoadResult {
                            cache: *cache,
                            main_revision,
                            backup_revision: Some(backup_revision),
                        });
                    }
                    DataUsageCacheLoadState::Loaded
                }
                DataUsageCacheLoadAttempt::Loaded { revision: None, .. } => {
                    last_retryable = Some(Error::other(format!("scanner cache backup object has no revision: {backup_name}")));
                    DataUsageCacheLoadState::Retryable
                }
                DataUsageCacheLoadAttempt::Missing { .. } => DataUsageCacheLoadState::Missing,
                DataUsageCacheLoadAttempt::Corrupt { .. } => DataUsageCacheLoadState::Corrupt,
                DataUsageCacheLoadAttempt::Retryable(err) => {
                    last_retryable = Some(err);
                    DataUsageCacheLoadState::Retryable
                }
            };

            match (main_state, backup_state) {
                (DataUsageCacheLoadState::Missing, DataUsageCacheLoadState::Missing) => {
                    return Ok(DataUsageCacheLoadResult {
                        cache: DataUsageCache::default(),
                        main_revision: main_revision
                            .ok_or_else(|| Error::other(format!("scanner cache missing state has no revision: {name}")))?,
                        backup_revision,
                    });
                }
                (DataUsageCacheLoadState::Corrupt, DataUsageCacheLoadState::Missing)
                | (DataUsageCacheLoadState::Missing, DataUsageCacheLoadState::Corrupt)
                | (DataUsageCacheLoadState::Corrupt, DataUsageCacheLoadState::Corrupt) => {
                    warn!(
                        target: "rustfs::scanner::data_usage",
                        event = EVENT_SCANNER_CACHE_LOAD_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_CACHE,
                        cache_name = %name,
                        state = "corrupt_cache_rebuild",
                        "Scanner cache is corrupt and will be rebuilt"
                    );
                    return Ok(DataUsageCacheLoadResult {
                        cache: DataUsageCache::default(),
                        main_revision: main_revision
                            .ok_or_else(|| Error::other(format!("scanner cache corrupt state has no revision: {name}")))?,
                        backup_revision,
                    });
                }
                _ => {}
            }

            if attempt < 4 {
                let sleep_ms: u64 = rand::random::<u64>() % 1000;
                sleep(Duration::from_millis(sleep_ms)).await;
            }
        }

        warn!(
            target: "rustfs::scanner::data_usage",
            event = EVENT_SCANNER_CACHE_LOAD_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_CACHE,
            cache_name = %name,
            retries = 5,
            state = "max_retries_reached",
            "Scanner cache load reached retry limit"
        );
        Err(last_retryable.unwrap_or_else(|| Error::other(format!("scanner cache could not be loaded: {name}"))))
    }

    pub(super) async fn try_load_inner<S: ScannerObjectIO>(
        store: Arc<S>,
        load_name: &str,
        timeout_duration: Duration,
    ) -> StorageResult<DataUsageCacheLoadAttempt> {
        // Abandon if more than time.Minute, so we don't hold up scanner.
        // drive timeout by default is 2 minutes, we do not need to wait longer.
        let load_fut = async {
            // First try: RUSTFS_META_BUCKET + BUCKET_META_PREFIX/name
            let path = path_join_buf(&[BUCKET_META_PREFIX, load_name]);
            match store
                .get_object_reader(
                    RUSTFS_META_BUCKET,
                    &path,
                    None,
                    HeaderMap::new(),
                    &ObjectOptions {
                        no_lock: true,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(mut reader) => {
                    let revision = reader
                        .object_info
                        .etag
                        .as_ref()
                        .filter(|etag| !etag.is_empty())
                        .cloned()
                        .map(DataUsageCacheRevision::Etag);
                    match reader.read_all().await {
                        Ok(data) => match DataUsageCache::unmarshal(&data) {
                            Ok(cache) => Ok((Some(cache), revision, false)),
                            Err(_) => Ok((None, revision, true)),
                        },
                        Err(e) => {
                            // Read error
                            Err(e)
                        }
                    }
                }
                Err(err) => {
                    match err {
                        Error::FileNotFound | Error::VolumeNotFound | Error::ObjectNotFound(_, _) | Error::BucketNotFound(_) => {
                            // Try second location: DATA_USAGE_BUCKET/name
                            match store
                                .get_object_reader(
                                    &DATA_USAGE_BUCKET,
                                    load_name,
                                    None,
                                    HeaderMap::new(),
                                    &ObjectOptions {
                                        no_lock: true,
                                        ..Default::default()
                                    },
                                )
                                .await
                            {
                                Ok(mut reader) => match reader.read_all().await {
                                    Ok(data) => match DataUsageCache::unmarshal(&data) {
                                        Ok(cache) => Ok((Some(cache), Some(DataUsageCacheRevision::Missing), false)),
                                        Err(_) => Ok((None, Some(DataUsageCacheRevision::Missing), true)),
                                    },
                                    Err(e) => Err(e),
                                },
                                Err(inner_err) => match inner_err {
                                    Error::FileNotFound
                                    | Error::VolumeNotFound
                                    | Error::ObjectNotFound(_, _)
                                    | Error::BucketNotFound(_) => {
                                        // Object not found in both locations
                                        Ok((None, Some(DataUsageCacheRevision::Missing), false))
                                    }
                                    Error::ErasureReadQuorum => {
                                        // InsufficientReadQuorum - retry
                                        Err(Error::ErasureReadQuorum)
                                    }
                                    _ => {
                                        // Other storage errors - retry
                                        if matches!(
                                            inner_err,
                                            Error::FaultyDisk | Error::DiskFull | Error::StorageFull | Error::SlowDown
                                        ) {
                                            return Err(inner_err);
                                        }
                                        Err(inner_err)
                                    }
                                },
                            }
                        }
                        Error::ErasureReadQuorum => {
                            // InsufficientReadQuorum - retry
                            Err(Error::ErasureReadQuorum)
                        }
                        _ => {
                            // Other storage errors - retry
                            if matches!(err, Error::FaultyDisk | Error::DiskFull | Error::StorageFull | Error::SlowDown) {
                                return Err(err);
                            }
                            Err(err)
                        }
                    }
                }
            }
        };

        match timeout(timeout_duration, load_fut).await {
            Ok(Ok((Some(cache), revision, _))) => Ok(DataUsageCacheLoadAttempt::Loaded {
                cache: Box::new(cache),
                revision,
            }),
            Ok(Ok((None, revision, true))) => Ok(DataUsageCacheLoadAttempt::Corrupt { revision }),
            Ok(Ok((None, revision, false))) => Ok(DataUsageCacheLoadAttempt::Missing { revision }),
            Ok(Err(err)) => Ok(DataUsageCacheLoadAttempt::Retryable(err)),
            Err(_) => Ok(DataUsageCacheLoadAttempt::Retryable(Error::other("scanner cache load timed out"))),
        }
    }

    async fn revision_for_path<S: ScannerObjectIO>(store: Arc<S>, path: &str) -> StorageResult<DataUsageCacheRevision> {
        match store
            .get_object_reader(
                RUSTFS_META_BUCKET,
                path,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(reader) => reader
                .object_info
                .etag
                .filter(|etag| !etag.is_empty())
                .map(DataUsageCacheRevision::Etag)
                .ok_or_else(|| StorageError::other(format!("scanner cache object {path} has no ETag"))),
            Err(Error::FileNotFound | Error::VolumeNotFound | Error::ObjectNotFound(_, _) | Error::BucketNotFound(_)) => {
                Ok(DataUsageCacheRevision::Missing)
            }
            Err(err) => Err(err),
        }
    }

    pub(super) fn cache_save_timeout() -> Duration {
        crate::runtime_config::scanner_cache_save_timeout()
    }

    pub(crate) fn persistence_timeout() -> Duration {
        Self::cache_save_timeout()
            .saturating_mul(DATA_USAGE_CACHE_SAVE_RETRIES + 1)
            .saturating_add(DATA_USAGE_CACHE_SAVE_RETRY_BACKOFF_MAX)
            .saturating_add(Self::backup_cache_save_timeout(Self::cache_save_timeout()))
            .saturating_add(DATA_USAGE_CACHE_PERSISTENCE_MARGIN)
    }

    fn backup_cache_save_timeout(timeout_duration: Duration) -> Duration {
        timeout_duration.min(Duration::from_secs(DATA_USAGE_CACHE_BACKUP_SAVE_TIMEOUT_SECS_MAX))
    }

    fn record_save_attempt(path_type: &'static str, result: &'static str, duration: Duration) {
        histogram!(METRIC_CACHE_SAVE_DURATION_SECONDS, "cache" => path_type).record(duration.as_secs_f64());
        counter!(
            METRIC_CACHE_SAVE_ATTEMPT_TOTAL,
            "cache" => path_type,
            "result" => result
        )
        .increment(1);
        if result == "timeout" {
            counter!(METRIC_CACHE_SAVE_TIMEOUT_TOTAL, "cache" => path_type).increment(1);
        }
    }

    fn should_retry_save_error(err: &StorageError) -> bool {
        // Usage-cache files are best-effort scanner checkpoints. Retrying namespace
        // lock failures immediately only adds more lock traffic to the same hot object.
        !matches!(
            err,
            StorageError::Lock(_)
                | StorageError::NamespaceLockQuorumUnavailable { .. }
                | StorageError::PreconditionFailed
                | StorageError::ObjectNotFound(_, _)
        )
    }

    pub(super) async fn retry_save_op<F, Fut>(
        path_type: &'static str,
        timeout_duration: Duration,
        max_retries: u32,
        mut save_op: F,
    ) -> StorageResult<()>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = StorageResult<()>>,
    {
        let mut last_err: Option<StorageError> = None;

        for attempt in 0..=max_retries {
            let attempt_start = Instant::now();
            let timeout_res = timeout(timeout_duration, save_op()).await;
            let duration = attempt_start.elapsed();

            match timeout_res {
                Ok(Ok(())) => {
                    Self::record_save_attempt(path_type, "success", duration);
                    return Ok(());
                }
                Err(e) => {
                    Self::record_save_attempt(path_type, "timeout", duration);
                    last_err = Some(StorageError::other(format!("{e} after {timeout_duration:?}")));
                }
                Ok(Err(e)) => {
                    let should_retry = Self::should_retry_save_error(&e);
                    Self::record_save_attempt(path_type, if should_retry { "error" } else { "lock_error" }, duration);
                    last_err = Some(e);
                    if !should_retry {
                        break;
                    }
                }
            }

            if last_err.is_some() && attempt < max_retries {
                counter!(METRIC_CACHE_SAVE_RETRY_TOTAL, "cache" => path_type).increment(1);
                let backoff_ms = 50_u64 * (1_u64 << attempt) + (rand::random::<u64>() % 100);
                sleep(Duration::from_millis(backoff_ms)).await;
            }
        }

        Err(last_err.unwrap_or_else(|| StorageError::other("Failed to save data usage cache".to_string())))
    }

    async fn save_path_with_retry<S: ScannerObjectIO>(
        store: Arc<S>,
        path: &str,
        buf: &[u8],
        timeout_duration: Duration,
        max_retries: u32,
        revision: Option<DataUsageCacheRevision>,
    ) -> StorageResult<()> {
        Self::ensure_cache_save_metrics_registered();
        let path_type = Self::cache_path_type(path);
        let path = path.to_string();

        let save_result = Self::retry_save_op(path_type, timeout_duration, max_retries, || {
            let store_clone = store.clone();
            let path_clone = path.clone();
            let buf_clone = buf.to_vec();
            let revision = revision.clone();
            async move {
                if let Some(revision) = revision {
                    save_config_with_preconditions(store_clone, &path_clone, buf_clone, revision.preconditions()).await?;
                } else {
                    save_config(store_clone, &path_clone, buf_clone).await?;
                }
                Ok::<(), StorageError>(())
            }
        })
        .await;
        let Err(save_err) = save_result else {
            return Ok(());
        };

        for attempt in 0..=max_retries {
            let reconcile = timeout(timeout_duration, async {
                let mut reader = store
                    .get_object_reader(
                        RUSTFS_META_BUCKET,
                        &path,
                        None,
                        HeaderMap::new(),
                        &ObjectOptions {
                            no_lock: true,
                            ..Default::default()
                        },
                    )
                    .await?;
                Ok::<bool, StorageError>(reader.read_all().await? == buf)
            })
            .await;
            if matches!(reconcile, Ok(Ok(true))) {
                Self::record_save_attempt(path_type, "reconciled", Duration::ZERO);
                return Ok(());
            }
            if matches!(reconcile, Ok(Ok(false))) {
                break;
            }
            if attempt < max_retries {
                sleep(Duration::from_millis(50_u64 * (u64::from(attempt) + 1))).await;
            }
        }

        Err(save_err)
    }

    pub async fn save<S: ScannerObjectIO>(&self, store: Arc<S>, name: &str) -> StorageResult<()> {
        self.save_inner(store, name, None).await
    }

    pub(crate) async fn save_with_revisions<S: ScannerObjectIO>(
        &self,
        store: Arc<S>,
        name: &str,
        revisions: &DataUsageCacheRevisions,
    ) -> StorageResult<()> {
        self.save_inner(store, name, Some(revisions)).await
    }

    async fn save_inner<S: ScannerObjectIO>(
        &self,
        store: Arc<S>,
        name: &str,
        revisions: Option<&DataUsageCacheRevisions>,
    ) -> StorageResult<()> {
        let mut buf = Vec::new();
        self.serialize(&mut rmp_serde::Serializer::new(&mut buf))?;
        let timeout_duration = Self::cache_save_timeout();

        let path = path_join_buf(&[BUCKET_META_PREFIX, name]);
        Self::save_path_with_retry(
            store.clone(),
            &path,
            &buf,
            timeout_duration,
            DATA_USAGE_CACHE_SAVE_RETRIES,
            revisions.map(|revisions| revisions.main.clone()),
        )
        .await?;

        let backup_name = format!("{name}.bkp");
        let backup_path = path_join_buf(&[BUCKET_META_PREFIX, &backup_name]);
        let backup_timeout_duration = Self::backup_cache_save_timeout(timeout_duration);
        let backup_revision = revisions.and_then(|revisions| revisions.backup.clone());
        if revisions.is_some() && backup_revision.is_none() {
            return Ok(());
        }
        if let Err(e) = Self::save_path_with_retry(
            store,
            &backup_path,
            &buf,
            backup_timeout_duration,
            DATA_USAGE_CACHE_BACKUP_SAVE_RETRIES,
            backup_revision,
        )
        .await
        {
            warn!(
                target: "rustfs::scanner::data_usage",
                event = EVENT_SCANNER_CACHE_SAVE_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_CACHE,
                cache_name = %name,
                backup_path = %backup_path,
                state = "backup_save_failed",
                error = %e,
                "Scanner cache backup save failed"
            );
        }
        Ok(())
    }
}
