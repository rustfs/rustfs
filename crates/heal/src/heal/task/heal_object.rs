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
/// object-level heal: metadata dir canonicalization and missing-object recreation
use super::*;

impl HealTask {
    // specific heal implementation method
    #[tracing::instrument(skip(self), fields(bucket = %bucket, object = %object, version_id = ?version_id))]
    #[hotpath::measure]
    pub(super) async fn heal_object(&self, bucket: &str, object: &str, version_id: Option<&str>) -> Result<()> {
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_OBJECT_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_OBJECT,
            task_id = %self.id,
            bucket,
            object,
            version_id = ?version_id,
            stage = "start",
            "Heal object started"
        );

        // update progress
        {
            let mut progress = self.progress.write().await;
            progress.set_current_object(Some(format!("{bucket}/{object}")));
            progress.update_stage(0, 4);
        }

        // Step 1: Check if object exists and get metadata
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_OBJECT_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_OBJECT,
            task_id = %self.id,
            bucket,
            object,
            stage = "check_existence",
            "Heal object stage entered"
        );
        self.check_control_flags().await?;
        let mut object_exists = match self.await_with_control(self.storage.object_exists(bucket, object)).await {
            Ok(exists) => exists,
            Err(err @ Error::TransientSkip { .. }) => {
                return self.skip_due_to_transient_object_exists(bucket, object, &err).await;
            }
            Err(err) => return Err(err),
        };

        let canonicalized_object = if !object_exists {
            match self.canonicalize_scanner_missing_object_dir(bucket, object).await {
                Ok(canonicalized_object) => canonicalized_object,
                Err(err @ Error::TransientSkip { .. }) => {
                    return self.skip_due_to_transient_object_exists(bucket, object, &err).await;
                }
                Err(err) => return Err(err),
            }
        } else {
            None
        };
        let object = if let Some(canonicalized_object) = canonicalized_object.as_deref() {
            object_exists = true;
            {
                let mut progress = self.progress.write().await;
                progress.set_current_object(Some(format!("{bucket}/{canonicalized_object}")));
            }
            canonicalized_object
        } else {
            object
        };

        if !object_exists {
            // Background loops (scanner/MRF/autoheal/read-repair) routinely
            // race object deletion, so a missing target is per-object noise
            // for them; only foreground admin/internal requests keep the warn.
            let background_source = !matches!(self.source, HealRequestSource::Admin | HealRequestSource::Internal);
            demote_to_debug_when!(background_source, warn, target: "rustfs::heal::task", {
                event = EVENT_HEAL_OBJECT_MISSING,
                component = LOG_COMPONENT_HEAL,
                subsystem = LOG_SUBSYSTEM_OBJECT,
                task_id = %self.id,
                bucket,
                object,
                source = self.source.as_str(),
                recreate_missing = self.options.recreate_missing,
                "Heal target object is missing"
            });
            if self.options.recreate_missing {
                debug!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_OBJECT_STAGE,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_OBJECT,
                    task_id = %self.id,
                    bucket,
                    object,
                    stage = "recreate_missing",
                    "Heal object recreate requested"
                );
                return self.recreate_missing_object(bucket, object, version_id).await;
            } else if self.source == HealRequestSource::Scanner {
                debug!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_OBJECT_STAGE,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_OBJECT,
                    task_id = %self.id,
                    bucket,
                    object,
                    stage = "scanner_missing_probe",
                    "Heal scanner missing object will be checked by storage layer"
                );
            } else {
                return Err(Error::TaskExecutionFailed {
                    message: format!("Object not found: {bucket}/{object}"),
                });
            }
        }

        {
            let mut progress = self.progress.write().await;
            progress.update_stage(1, 3);
        }

        // Step 2: directly call ecstore to perform heal
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_OBJECT_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_OBJECT,
            task_id = %self.id,
            bucket,
            object,
            stage = "heal_with_ecstore",
            dry_run = self.options.dry_run,
            remove_corrupted = self.options.remove_corrupted,
            update_parity = self.options.update_parity,
            "Heal object stage entered"
        );
        let heal_opts = HealOpts {
            recursive: self.options.recursive,
            dry_run: self.options.dry_run,
            remove: self.options.remove_corrupted,
            recreate: self.options.recreate_missing,
            scan_mode: self.options.scan_mode,
            update_parity: self.options.update_parity,
            no_lock: self.options.no_lock,
            pool: self.options.pool_index,
            set: self.options.set_index,
        };

        let heal_result = self
            .await_with_control(self.storage.heal_object(bucket, object, version_id, &heal_opts))
            .await;

        match heal_result {
            Ok((result, error)) => {
                if let Some(e) = error {
                    if self.skip_data_usage_cache_heal_error(bucket, object, &e).await {
                        return Ok(());
                    }

                    if Self::is_object_not_found_heal_error(&e) {
                        debug!(
                            target: "rustfs::heal::task",
                            event = EVENT_HEAL_OBJECT_RESULT,
                            component = LOG_COMPONENT_HEAL,
                            subsystem = LOG_SUBSYSTEM_OBJECT,
                            task_id = %self.id,
                            bucket,
                            object,
                            result = "treated_as_deleted",
                            "Heal missing object treated as deleted"
                        );
                        {
                            let mut progress = self.progress.write().await;
                            progress.update_stage(3, 3);
                        }
                        return Ok(());
                    }

                    error!(
                        target: "rustfs::heal::task",
                        event = EVENT_HEAL_OBJECT_RESULT,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_OBJECT,
                        task_id = %self.id,
                        bucket,
                        object,
                        result = "failed",
                        error = %e,
                        "Heal object operation failed"
                    );

                    {
                        let mut progress = self.progress.write().await;
                        progress.update_stage(3, 3);
                    }

                    if Self::should_return_typed_heal_error(&e) {
                        return Err(e);
                    }

                    return Err(Error::TaskExecutionFailed {
                        message: format!("Failed to heal object {bucket}/{object}: {e}"),
                    });
                }

                // Step 3: Verify heal result
                debug!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_OBJECT_STAGE,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_OBJECT,
                    task_id = %self.id,
                    bucket,
                    object,
                    stage = "verify_result",
                    "Heal object stage entered"
                );
                let object_size = result.object_size as u64;
                debug!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_OBJECT_RESULT,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_OBJECT,
                    task_id = %self.id,
                    bucket,
                    object,
                    object_size = object_size,
                    drives_healed = result.drives_healed(),
                    drives_total = result.drives_reported(),
                    result = "ok",
                    "Heal object repaired"
                );

                {
                    let mut progress = self.progress.write().await;
                    progress.update_object_progress(1, 1, 0, 0, object_size);
                }
                self.record_result_item(result).await;
                Ok(())
            }
            Err(Error::TaskCancelled) => Err(Error::TaskCancelled),
            Err(Error::TaskTimeout) => Err(Error::TaskTimeout),
            Err(e) => {
                if self.skip_data_usage_cache_heal_error(bucket, object, &e).await {
                    return Ok(());
                }

                if Self::is_object_not_found_heal_error(&e) {
                    debug!(
                        target: "rustfs::heal::task",
                        event = EVENT_HEAL_OBJECT_RESULT,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_OBJECT,
                        task_id = %self.id,
                        bucket,
                        object,
                        result = "treated_as_deleted",
                        "Heal missing object treated as deleted"
                    );
                    {
                        let mut progress = self.progress.write().await;
                        progress.update_stage(3, 3);
                    }
                    return Ok(());
                }

                error!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_OBJECT_RESULT,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_OBJECT,
                    task_id = %self.id,
                    bucket,
                    object,
                    result = "failed",
                    error = %e,
                    "Heal object operation failed"
                );

                {
                    let mut progress = self.progress.write().await;
                    progress.update_stage(3, 3);
                }

                if Self::should_return_typed_heal_error(&e) {
                    Err(e)
                } else {
                    Err(Error::TaskExecutionFailed {
                        message: format!("Failed to heal object {bucket}/{object}: {e}"),
                    })
                }
            }
        }
    }

    async fn canonicalize_scanner_missing_object_dir(&self, bucket: &str, object: &str) -> Result<Option<String>> {
        if self.source != HealRequestSource::Scanner {
            return Ok(None);
        }

        let Some(candidate) = object.strip_suffix(SLASH_SEPARATOR) else {
            return Ok(None);
        };
        if candidate.is_empty() {
            return Ok(None);
        }

        match self.await_with_control(self.storage.object_exists(bucket, candidate)).await {
            Ok(true) => {
                debug!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_OBJECT_STAGE,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_OBJECT,
                    task_id = %self.id,
                    bucket,
                    object = %candidate,
                    canonicalized_from = %object,
                    stage = "canonicalize_scanner_object_dir",
                    result = "canonicalized",
                    "Heal scanner object-dir candidate canonicalized"
                );
                Ok(Some(candidate.to_string()))
            }
            Ok(false) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Recreate missing object (for EC decode scenarios)
    async fn recreate_missing_object(&self, bucket: &str, object: &str, version_id: Option<&str>) -> Result<()> {
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_OBJECT_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_OBJECT,
            task_id = %self.id,
            bucket,
            object,
            version_id = ?version_id,
            stage = "recreate_missing",
            "Heal object recreate started"
        );

        // Use ecstore's heal_object with recreate option
        let heal_opts = HealOpts {
            recursive: false,
            dry_run: self.options.dry_run,
            remove: false,
            recreate: true,
            scan_mode: HealScanMode::Deep,
            update_parity: true,
            no_lock: self.options.no_lock,
            pool: None,
            set: None,
        };

        match self
            .await_with_control(self.storage.heal_object(bucket, object, version_id, &heal_opts))
            .await
        {
            Ok((result, error)) => {
                if let Some(e) = error {
                    if self.skip_scanner_synthetic_object_dir_missing(bucket, object, &e).await {
                        return Ok(());
                    }

                    error!(
                        target: "rustfs::heal::task",
                        event = EVENT_HEAL_OBJECT_RESULT,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_OBJECT,
                        task_id = %self.id,
                        bucket,
                        object,
                        result = "recreate_failed",
                        error = %e,
                        "Heal object recovery failed"
                    );
                    return Err(Error::TaskExecutionFailed {
                        message: format!("Failed to recreate missing object {bucket}/{object}: {e}"),
                    });
                }

                let object_size = result.object_size as u64;
                debug!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_OBJECT_RESULT,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_OBJECT,
                    task_id = %self.id,
                    bucket,
                    object,
                    object_size,
                    result = "recreated",
                    "Heal object recreated"
                );

                {
                    let mut progress = self.progress.write().await;
                    progress.update_object_progress(1, 1, 0, 0, object_size);
                }
                self.record_result_item(result).await;
                Ok(())
            }
            Err(Error::TaskCancelled) => Err(Error::TaskCancelled),
            Err(Error::TaskTimeout) => Err(Error::TaskTimeout),
            Err(e) => {
                if self.skip_scanner_synthetic_object_dir_missing(bucket, object, &e).await {
                    return Ok(());
                }

                error!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_OBJECT_RESULT,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_OBJECT,
                    task_id = %self.id,
                    bucket,
                    object,
                    result = "recreate_failed",
                    error = %e,
                    "Heal object recovery failed"
                );
                Err(Error::TaskExecutionFailed {
                    message: format!("Failed to recreate missing object {bucket}/{object}: {e}"),
                })
            }
        }
    }
}
