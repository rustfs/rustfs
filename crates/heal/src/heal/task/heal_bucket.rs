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
/// bucket/cluster/prefix heal: the recursive bucket-objects sweep and the erasure-set usage baseline
use super::*;
use crate::heal::progress::{add_bytes, increment_counter, stable_generation};
use crate::heal::storage::HealListItem;
use crate::heal::utils::format_set_disk_id;

const MAX_DEFERRED_OBJECTS: usize = 256;
const MAX_DEFERRED_BYTES: usize = 256 * 1024;
const MAX_DEFERRED_FORWARD_PAGES: u64 = 2;
const MAX_DEFERRED_AGE: Duration = Duration::from_secs(30);

struct DeferredObject {
    name: String,
    version_id: Option<String>,
    attempt: u32,
    page: u64,
    first_failure: Option<tokio::time::Instant>,
    due: tokio::time::Instant,
}

impl DeferredObject {
    fn new(item: HealListItem, page: u64) -> Self {
        Self {
            name: item.name,
            version_id: item.version_id,
            attempt: 0,
            page,
            first_failure: None,
            due: tokio::time::Instant::now(),
        }
    }

    fn payload_bytes(&self) -> usize {
        self.name
            .capacity()
            .saturating_add(self.version_id.as_ref().map_or(0, String::capacity))
    }

    fn expired(&self) -> bool {
        self.first_failure.is_some_and(|first| first.elapsed() >= MAX_DEFERRED_AGE)
    }

    fn defer(&mut self, delay: Duration) {
        let now = tokio::time::Instant::now();
        let first = *self.first_failure.get_or_insert(now);
        self.attempt += 1;
        self.due = (now + delay).min(first + MAX_DEFERRED_AGE);
    }
}

// Only failed identities are retained. The current listing page remains owned
// by the caller; capacity pressure stops fetching, never discards that page.
struct DeferredWindow {
    objects: VecDeque<DeferredObject>,
    bytes: usize,
}

impl Default for DeferredWindow {
    fn default() -> Self {
        Self {
            objects: VecDeque::new(),
            // Charge every possible slot up front, including spare capacity.
            bytes: MAX_DEFERRED_OBJECTS * size_of::<DeferredObject>(),
        }
    }
}

impl DeferredWindow {
    fn push(&mut self, item: DeferredObject) -> std::result::Result<(), DeferredObject> {
        let bytes = item.payload_bytes();
        if self.objects.len() >= MAX_DEFERRED_OBJECTS || bytes > MAX_DEFERRED_BYTES.saturating_sub(self.bytes) {
            return Err(item);
        }
        self.bytes += bytes;
        self.objects.push_back(item);
        Ok(())
    }

    fn pop_due(&mut self) -> Option<DeferredObject> {
        let now = tokio::time::Instant::now();
        let index = self.objects.iter().position(|item| item.due <= now)?;
        let item = self.objects.remove(index)?;
        self.bytes -= item.payload_bytes();
        Some(item)
    }

    fn next_due(&self) -> Option<tokio::time::Instant> {
        self.objects.iter().map(|item| item.due).min()
    }

    fn can_advance(&self, page: u64) -> bool {
        self.objects.len() < MAX_DEFERRED_OBJECTS
            && self.bytes < MAX_DEFERRED_BYTES
            && self
                .objects
                .iter()
                .all(|item| page.saturating_sub(item.page) < MAX_DEFERRED_FORWARD_PAGES)
    }
}

#[cfg(test)]
#[path = "tests/deferred_retry_window.rs"]
mod deferred_retry_window;

fn unavailable_recreate_error(result: &HealResultItem, opts: &HealOpts) -> Option<Error> {
    if opts.dry_run || !opts.recreate {
        return None;
    }

    let mut offline = false;
    for drive in &result.after.drives {
        if drive.state == DriveState::Faulty.to_str() {
            return Some(Error::Disk(DiskError::FaultyDisk));
        }
        offline |= drive.state == DriveState::Offline.to_str();
    }

    offline.then_some(Error::Disk(DiskError::DiskNotFound))
}

impl HealTask {
    pub(super) async fn heal_bucket(&self, bucket: &str) -> Result<()> {
        self.pace_mainline().await?;
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_BUCKET_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            bucket,
            stage = "start",
            recursive = self.options.recursive,
            "Heal bucket started"
        );

        // update progress
        {
            let mut progress = self.progress.write().await;
            progress.set_current_object(Some(format!("bucket: {bucket}")));
            progress.update_stage(0, 3);
        }

        // Step 1: Check if bucket exists
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_BUCKET_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            bucket,
            stage = "check_existence",
            "Heal bucket stage entered"
        );
        self.check_control_flags().await?;
        let bucket_exists = self.await_with_control(self.storage.get_bucket_info(bucket)).await?.is_some();
        if !bucket_exists {
            warn!(
                target: "rustfs::heal::task",
                event = EVENT_HEAL_BUCKET_RESULT,
                component = LOG_COMPONENT_HEAL,
                subsystem = LOG_SUBSYSTEM_TASK,
                task_id = %self.id,
                bucket,
                result = "missing",
                "Heal bucket failed because the bucket does not exist"
            );
            return Err(Error::TaskExecutionFailed {
                message: format!("Bucket not found: {bucket}"),
            });
        }

        {
            let mut progress = self.progress.write().await;
            progress.update_stage(1, 3);
        }

        // Step 2: Perform bucket heal using ecstore
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_BUCKET_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            bucket,
            stage = "heal_with_ecstore",
            dry_run = self.options.dry_run,
            "Heal bucket stage entered"
        );
        let heal_opts = HealOpts {
            recursive: self.options.recursive,
            dry_run: self.options.dry_run,
            remove: if self.options.recursive {
                false
            } else {
                self.options.remove_corrupted
            },
            recreate: self.options.recreate_missing,
            scan_mode: self.options.scan_mode,
            update_parity: self.options.update_parity,
            no_lock: self.options.no_lock,
            read_repair: false,
            pool: self.options.pool_index,
            set: self.options.set_index,
        };

        let heal_result = self.await_with_control(self.storage.heal_bucket(bucket, &heal_opts)).await;

        match heal_result {
            Ok(result) => {
                debug!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_BUCKET_RESULT,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_TASK,
                    task_id = %self.id,
                    bucket,
                    drives_healed = result.drives_healed(),
                    drives_total = result.drives_reported(),
                    recursive = self.options.recursive,
                    result = "ok",
                    "Heal bucket completed"
                );
                self.record_result_item(result).await;

                if self.options.recursive {
                    self.heal_bucket_objects(bucket, "").await?;
                }

                if !self.options.recursive {
                    let mut progress = self.progress.write().await;
                    progress.update_stage(3, 3);
                }
                Ok(())
            }
            Err(Error::TaskCancelled) => Err(Error::TaskCancelled),
            Err(Error::TaskTimeout) => Err(Error::TaskTimeout),
            Err(e) => {
                error!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_BUCKET_RESULT,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_TASK,
                    task_id = %self.id,
                    bucket,
                    result = "failed",
                    error = %e,
                    "Heal bucket failed"
                );
                {
                    let mut progress = self.progress.write().await;
                    progress.update_stage(3, 3);
                }
                Err(Error::TaskExecutionFailed {
                    message: format!("Failed to heal bucket {bucket}: {e}"),
                })
            }
        }
    }

    pub(super) async fn heal_cluster(&self) -> Result<()> {
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_BUCKET_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            stage = "cluster_recursive",
            "Heal cluster started"
        );

        let bucket_infos = self.await_with_control(self.storage.list_buckets()).await?;
        let mut failed = 0_u64;
        let mut retryable = 0_u64;
        let mut permanent = 0_u64;
        let mut first_object = None;
        let mut first_error = None;
        for bucket_info in bucket_infos {
            self.check_control_flags().await?;
            let mut retry_attempt = 0_u32;
            loop {
                match self.heal_bucket(&bucket_info.name).await {
                    Ok(()) => break,
                    Err(Error::TaskCancelled) => return Err(Error::TaskCancelled),
                    Err(Error::TaskTimeout) => return Err(Error::TaskTimeout),
                    Err(err) => {
                        if let Some(failure) = self.take_batch_failure().await {
                            failed = failed.saturating_add(failure.failed);
                            retryable = retryable.saturating_add(failure.retryable);
                            permanent = permanent.saturating_add(failure.permanent);
                            first_object.get_or_insert(failure.first_object);
                            first_error.get_or_insert(failure.first_error);
                            break;
                        }
                        if err.is_recoverable_heal() && retry_attempt < MAX_BUCKET_OBJECT_HEAL_RETRIES {
                            retry_attempt = retry_attempt.saturating_add(1);
                            self.await_with_control(async {
                                tokio::time::sleep(self.bucket_object_retry_delay(retry_attempt)).await;
                                Ok(())
                            })
                            .await?;
                            continue;
                        }
                        failed = failed.saturating_add(1);
                        self.outcome.write().await.mark_untraversable();
                        if err.is_recoverable_heal() {
                            retryable = retryable.saturating_add(1);
                        } else {
                            permanent = permanent.saturating_add(1);
                        }
                        first_object.get_or_insert(bucket_info.name.clone());
                        first_error.get_or_insert_with(|| err.to_string());
                        break;
                    }
                }
            }
        }

        if failed > 0 {
            let failure = BatchHealFailure {
                scope: "cluster".to_string(),
                failed,
                retryable,
                permanent,
                first_object: first_object.unwrap_or_default(),
                first_error: first_error.unwrap_or_default(),
            };
            return Err(self.record_batch_failure(failure).await);
        }

        Ok(())
    }

    pub(super) async fn heal_prefix(&self, bucket: &str, prefix: &str) -> Result<()> {
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_BUCKET_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            bucket,
            prefix,
            stage = "prefix_recursive",
            "Heal prefix started"
        );

        self.heal_bucket_objects(bucket, prefix).await
    }

    #[hotpath::measure]
    async fn heal_bucket_objects(&self, bucket: &str, prefix: &str) -> Result<()> {
        let previous_progress = self.get_progress().await;
        let mut scanned = 0u64;
        let mut healed = 0u64;
        let mut failed = 0u64;
        let mut skipped = 0u64;
        let mut retryable_failed = 0u64;
        let mut permanent_failed = 0u64;
        let mut bytes = 0u64;
        let mut first_failed_object = None;
        let mut first_error = None;
        let mut failure_samples_logged = 0_u64;

        let heal_opts = HealOpts {
            recursive: false,
            dry_run: self.options.dry_run,
            remove: self.options.remove_corrupted,
            recreate: self.options.recreate_missing,
            scan_mode: self.options.scan_mode,
            update_parity: self.options.update_parity,
            no_lock: self.options.no_lock,
            read_repair: false,
            pool: self.options.pool_index,
            set: self.options.set_index,
        };

        let erasure_set_scopes = self
            .await_with_control(self.storage.heal_erasure_set_scopes(&heal_opts))
            .await?;
        let listing_scopes = match erasure_set_scopes {
            None => vec![(None, heal_opts)],
            Some(erasure_set_scopes) => erasure_set_scopes
                .into_iter()
                .map(|(pool_idx, set_idx)| {
                    let mut scoped_opts = heal_opts;
                    scoped_opts.pool = Some(pool_idx);
                    scoped_opts.set = Some(set_idx);
                    (Some(format_set_disk_id(pool_idx, set_idx)), scoped_opts)
                })
                .collect(),
        };

        for (set_disk_id, heal_opts) in listing_scopes {
            let mut continuation_token: Option<String> = None;
            let mut deferred = DeferredWindow::default();
            let mut inline_retry: Option<DeferredObject> = None;
            let mut page_number = 0_u64;
            let mut aborted_progress_unknown = false;
            let mut pending = Vec::<HealListItem>::new().into_iter();
            let mut listing_finished = false;
            let mut listing_attempt = 0;
            let mut listing_due = tokio::time::Instant::now();
            let scope_result: Result<()> = async {
                loop {
                    self.check_control_flags().await?;
                    if listing_finished && pending.as_slice().is_empty() && deferred.objects.is_empty() && inline_retry.is_none()
                    {
                        break;
                    }
                    self.pace_mainline().await?;
                    // Listing and object retries share this safe boundary. A
                    // failed listing never hides an already-due object retry.
                    let item = deferred.pop_due().or_else(|| {
                        if inline_retry
                            .as_ref()
                            .is_some_and(|item| item.due <= tokio::time::Instant::now())
                        {
                            inline_retry.take()
                        } else if inline_retry.is_none() {
                            pending.next().map(|item| DeferredObject::new(item, page_number))
                        } else {
                            None
                        }
                    });
                    let Some(mut item) = item else {
                        let can_list = !listing_finished && inline_retry.is_none() && deferred.can_advance(page_number);
                        if can_list && listing_due <= tokio::time::Instant::now() {
                            let page = if let Some(set_disk_id) = set_disk_id.as_deref() {
                                self.await_with_control(self.storage.list_versions_for_heal_page_disk_walk(
                                    set_disk_id,
                                    bucket,
                                    prefix,
                                    continuation_token.as_deref(),
                                    false,
                                ))
                                .await
                            } else {
                                self.await_with_control(self.storage.list_objects_for_heal_page(
                                    bucket,
                                    prefix,
                                    continuation_token.as_deref(),
                                    false,
                                ))
                                .await
                            };
                            match page {
                                Ok((objects, next_token, is_truncated)) => {
                                    page_number = page_number.saturating_add(1);
                                    continuation_token = next_heal_listing_token(bucket, prefix, next_token, is_truncated)?;
                                    listing_finished = continuation_token.is_none();
                                    listing_attempt = 0;
                                    listing_due = tokio::time::Instant::now();
                                    pending = objects.into_iter();
                                }
                                Err(error @ (Error::TaskCancelled | Error::TaskTimeout)) => return Err(error),
                                Err(error) => {
                                    self.outcome.write().await.attempt_failed();
                                    if error.is_recoverable_heal() && listing_attempt < MAX_BUCKET_OBJECT_HEAL_RETRIES {
                                        listing_attempt += 1;
                                        listing_due =
                                            tokio::time::Instant::now() + self.bucket_object_retry_delay(listing_attempt);
                                        continue;
                                    }
                                    self.outcome.write().await.mark_untraversable();
                                    return Err(Error::HealListingFailed {
                                        bucket: bucket.to_string(),
                                        source: Box::new(error),
                                    });
                                }
                            }
                            continue;
                        } else {
                            let due = deferred
                                .next_due()
                                .into_iter()
                                .chain(inline_retry.as_ref().map(|item| item.due))
                                .chain(can_list.then_some(listing_due))
                                .min();
                            if let Some(due) = due {
                                self.await_with_control(async {
                                    tokio::time::sleep_until(due).await;
                                    Ok(())
                                })
                                .await?;
                            }
                        }
                        continue;
                    };
                    let retry_attempt = item.attempt;
                    let mut telemetry_unknown = false;
                    let object = item.name.as_str();
                    let identity =
                        self.outcome_identity(bucket, object, item.version_id.as_deref(), heal_opts.pool, heal_opts.set);
                    let mut disposition = if heal_opts.dry_run {
                        HealObjectDisposition::DryRunObserved
                    } else {
                        HealObjectDisposition::Unknown
                    };
                    let mut detail = None;
                    {
                        let mut progress = self.progress.write().await;
                        progress.set_current_object(Some(format!("{bucket}/{object}")));
                    }

                    let mut terminal_outcome = true;
                    let age_exhausted = item.expired();
                    let error = if age_exhausted {
                        Some(Error::other("heal object retry age exhausted"))
                    } else {
                        match self
                            .await_with_control(
                                self.storage
                                    .heal_object(bucket, object, item.version_id.as_deref(), &heal_opts),
                            )
                            .await
                        {
                            Ok((result, None)) => match unavailable_recreate_error(&result, &heal_opts) {
                                Some(error) => Some(error),
                                None => {
                                    telemetry_unknown |= !increment_counter(&mut healed);
                                    telemetry_unknown |=
                                        !add_bytes(&mut bytes, u64::try_from(result.object_size).unwrap_or(u64::MAX));
                                    self.record_result_item(result).await;
                                    None
                                }
                            },
                            Ok((_, Some(err))) if is_missing_object_dir_heal_result(object, &err) => {
                                telemetry_unknown |= !increment_counter(&mut healed);
                                debug!(
                                    target: "rustfs::heal::task",
                                    event = EVENT_HEAL_BUCKET_RESULT,
                                    component = LOG_COMPONENT_HEAL,
                                    subsystem = LOG_SUBSYSTEM_TASK,
                                    task_id = %self.id,
                                    bucket,
                                    object,
                                    result = "object_dir_not_found_skipped",
                                    "Heal bucket object-dir candidate skipped after not-found result"
                                );
                                None
                            }
                            Ok((_, Some(err))) | Err(err) => Some(err),
                        }
                    };

                    if let Some(err) = error {
                        match err {
                            Error::TaskCancelled | Error::TaskTimeout => {
                                let disposition = if matches!(err, Error::TaskCancelled) {
                                    HealObjectDisposition::Cancelled
                                } else {
                                    HealObjectDisposition::Deferred {
                                        reason: HealDeferredReason::Deadline,
                                        retry_not_before: None,
                                    }
                                };
                                self.outcome.write().await.record(HealObjectOutcome {
                                    identity,
                                    disposition,
                                    detail: None,
                                });
                                aborted_progress_unknown |= !increment_counter(&mut scanned);
                                aborted_progress_unknown |= !increment_counter(&mut skipped);
                                return Err(err);
                            }
                            _ if !age_exhausted => self.outcome.write().await.attempt_failed(),
                            _ => {}
                        }
                        detail = Some(err.to_string());
                        if Self::is_dangling_delete_grace_error(&err) {
                            disposition = HealObjectDisposition::Deferred {
                                reason: HealDeferredReason::DanglingDeleteGrace,
                                retry_not_before: None,
                            };
                            telemetry_unknown |= !increment_counter(&mut skipped);
                            warn!(
                                target: "rustfs::heal::task",
                                event = EVENT_HEAL_BUCKET_RESULT,
                                component = LOG_COMPONENT_HEAL,
                                subsystem = LOG_SUBSYSTEM_TASK,
                                task_id = %self.id,
                                bucket,
                                object,
                                result = "dangling_delete_grace_skip",
                                error = %err,
                                "Heal bucket object dangling cleanup deferred by grace window"
                            );
                        } else if Self::should_skip_data_usage_cache_heal_error(bucket, object, &err) {
                            disposition = HealObjectDisposition::Deferred {
                                reason: HealDeferredReason::TransientUsageCache,
                                retry_not_before: None,
                            };
                            telemetry_unknown |= !increment_counter(&mut skipped);
                            warn!(
                                target: "rustfs::heal::task",
                                event = EVENT_HEAL_BUCKET_RESULT,
                                component = LOG_COMPONENT_HEAL,
                                subsystem = LOG_SUBSYSTEM_TASK,
                                task_id = %self.id,
                                bucket,
                                object,
                                result = "transient_skip",
                                error = %err,
                                "Heal bucket object repair skipped due to transient metadata error"
                            );
                        } else if !age_exhausted && err.is_recoverable_heal() && retry_attempt < MAX_BUCKET_OBJECT_HEAL_RETRIES {
                            terminal_outcome = false;
                            debug!(
                                target: "rustfs::heal::task",
                                event = EVENT_HEAL_BUCKET_RESULT,
                                component = LOG_COMPONENT_HEAL,
                                subsystem = LOG_SUBSYSTEM_TASK,
                                task_id = %self.id,
                                bucket,
                                object,
                                retry_attempt = retry_attempt.saturating_add(1),
                                error = %err,
                                result = "object_retry_scheduled",
                                "Heal bucket object retry scheduled"
                            );
                            item.defer(self.bucket_object_retry_delay(retry_attempt + 1));
                            if let Err(item) = deferred.push(item) {
                                inline_retry = Some(item);
                            }
                        } else {
                            disposition = HealObjectDisposition::Failed(if age_exhausted || err.is_recoverable_heal() {
                                HealFailureClass::RetryExhausted
                            } else {
                                HealFailureClass::Permanent
                            });
                            telemetry_unknown |= !increment_counter(&mut failed);
                            if age_exhausted || err.is_recoverable_heal() {
                                retryable_failed = retryable_failed.saturating_add(1);
                            } else {
                                permanent_failed = permanent_failed.saturating_add(1);
                            }
                            first_failed_object.get_or_insert_with(|| object.to_string());
                            first_error.get_or_insert_with(|| err.to_string());
                            if take_failure_log_sample(&mut failure_samples_logged) {
                                warn!(
                                    target: "rustfs::heal::task",
                                    event = EVENT_HEAL_BUCKET_RESULT,
                                    component = LOG_COMPONENT_HEAL,
                                    subsystem = LOG_SUBSYSTEM_TASK,
                                    task_id = %self.id,
                                    bucket,
                                    object,
                                    retry_attempt,
                                    error = %err,
                                    result = "object_failed",
                                    "Heal bucket object repair failed"
                                );
                            }
                        }
                    }

                    if terminal_outcome {
                        telemetry_unknown |= !increment_counter(&mut scanned);
                    }

                    if !terminal_outcome {
                        continue;
                    }

                    self.outcome.write().await.record(HealObjectOutcome {
                        identity,
                        disposition,
                        detail,
                    });

                    let mut progress = self.progress.write().await;
                    progress.update_object_progress(
                        previous_progress.objects_scanned.saturating_add(scanned),
                        previous_progress.objects_healed.saturating_add(healed),
                        previous_progress.objects_failed.saturating_add(failed),
                        previous_progress.skipped_objects.saturating_add(skipped),
                        previous_progress.bytes_processed.saturating_add(bytes),
                    );
                    if telemetry_unknown {
                        progress.mark_unknown();
                    }
                }
                Ok(())
            }
            .await;
            if let Err(error) = scope_result {
                let disposition = match error {
                    Error::TaskCancelled => HealObjectDisposition::Cancelled,
                    Error::TaskTimeout => HealObjectDisposition::Deferred {
                        reason: HealDeferredReason::Deadline,
                        retry_not_before: None,
                    },
                    _ => HealObjectDisposition::Unknown,
                };
                // Only attempted identities have terminal outcomes. Unstarted
                // page tails remain unprocessed under the task's partial coverage.
                // No detached sleepers survive abort.
                for item in deferred.objects.into_iter().chain(inline_retry) {
                    self.outcome.write().await.record(HealObjectOutcome {
                        identity: self.outcome_identity(
                            bucket,
                            &item.name,
                            item.version_id.as_deref(),
                            heal_opts.pool,
                            heal_opts.set,
                        ),
                        disposition: disposition.clone(),
                        detail: None,
                    });
                    aborted_progress_unknown |= !increment_counter(&mut scanned);
                    aborted_progress_unknown |= !increment_counter(&mut skipped);
                }
                let mut progress = self.progress.write().await;
                progress.update_object_progress(
                    previous_progress.objects_scanned.saturating_add(scanned),
                    previous_progress.objects_healed.saturating_add(healed),
                    previous_progress.objects_failed.saturating_add(failed),
                    previous_progress.skipped_objects.saturating_add(skipped),
                    previous_progress.bytes_processed.saturating_add(bytes),
                );
                if aborted_progress_unknown {
                    progress.mark_unknown();
                }
                return Err(error);
            }
        }

        if failed > 0 {
            let failure = BatchHealFailure {
                scope: format!("bucket:{bucket}"),
                failed,
                retryable: retryable_failed,
                permanent: permanent_failed,
                first_object: first_failed_object.unwrap_or_default(),
                first_error: first_error.unwrap_or_default(),
            };
            return Err(self.record_batch_failure(failure).await);
        }

        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_BUCKET_RESULT,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            bucket,
            prefix,
            scanned,
            healed,
            failed,
            bytes_processed = bytes,
            result = "recursive_ok",
            "Heal bucket recursive pass completed"
        );
        Ok(())
    }

    pub(super) async fn apply_erasure_set_usage_baseline(&self, buckets: &[String]) -> Result<()> {
        if matches!(self.options.scan_mode, HealScanMode::Deep) || matches!(self.source, HealRequestSource::AutoHeal) {
            return Ok(());
        }
        let baseline = match self
            .await_with_control(self.storage.erasure_set_usage_baseline(buckets))
            .await
        {
            Ok(Some(baseline)) => baseline,
            Ok(None) => return Ok(()),
            Err(err @ Error::TaskCancelled) | Err(err @ Error::TaskTimeout) => return Err(err),
            Err(_) => return Ok(()),
        };

        let HealBucketUsageBaseline {
            objects_count,
            bytes,
            generation,
        } = baseline;
        let generation = generation.map(|snapshot_generation| stable_generation(&[&snapshot_generation.to_be_bytes()]));
        let mut progress = self.progress.write().await;
        if let Some(generation) = generation {
            progress.set_total_baseline_with_generation(objects_count, bytes, generation);
        } else {
            progress.set_total_baseline(objects_count, bytes);
        }
        Ok(())
    }
}
