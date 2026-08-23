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

impl HealTask {
    pub(super) async fn heal_bucket(&self, bucket: &str) -> Result<()> {
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
        let mut continuation_token: Option<String> = None;
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
            pool: self.options.pool_index,
            set: self.options.set_index,
        };

        loop {
            self.check_control_flags().await?;
            let (objects, next_token, is_truncated) = self
                .await_with_control(
                    self.storage
                        .list_objects_for_heal_page(bucket, prefix, continuation_token.as_deref(), false),
                )
                .await?;

            let mut pending = objects;
            let mut retry_attempt = 0_u32;
            while !pending.is_empty() {
                if retry_attempt > 0 {
                    self.await_with_control(async {
                        tokio::time::sleep(self.bucket_object_retry_delay(retry_attempt)).await;
                        Ok(())
                    })
                    .await?;
                }
                let mut retry = Vec::with_capacity(pending.len());
                for item in pending {
                    self.check_control_flags().await?;
                    let mut telemetry_unknown = false;
                    let object = item.name.as_str();
                    {
                        let mut progress = self.progress.write().await;
                        progress.set_current_object(Some(format!("{bucket}/{object}")));
                    }

                    let mut terminal_outcome = true;
                    let error = match self
                        .await_with_control(
                            self.storage
                                .heal_object(bucket, object, item.version_id.as_deref(), &heal_opts),
                        )
                        .await
                    {
                        Ok((result, None)) => {
                            telemetry_unknown |= !increment_counter(&mut healed);
                            telemetry_unknown |= !add_bytes(&mut bytes, u64::try_from(result.object_size).unwrap_or(u64::MAX));
                            self.record_result_item(result).await;
                            None
                        }
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
                    };

                    if let Some(err) = error {
                        if Self::should_skip_data_usage_cache_heal_error(bucket, object, &err) {
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
                        } else if err.is_recoverable_heal() && retry_attempt < MAX_BUCKET_OBJECT_HEAL_RETRIES {
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
                            retry.push(item);
                        } else {
                            telemetry_unknown |= !increment_counter(&mut failed);
                            if err.is_recoverable_heal() {
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

                    let mut progress = self.progress.write().await;
                    progress.update_object_progress(scanned, healed, failed, skipped, bytes);
                    if telemetry_unknown {
                        progress.mark_unknown();
                    }
                }
                pending = retry;
                retry_attempt = retry_attempt.saturating_add(1);
            }

            if !is_truncated {
                break;
            }

            continuation_token = next_heal_listing_token(bucket, prefix, next_token, is_truncated)?;
            if continuation_token.is_none() {
                // Truncated but no continuation token: end of listing.
                break;
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
