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
/// erasure-set heal: drives the ErasureSetHealer across the set's buckets
use super::*;

impl HealTask {
    pub(super) async fn heal_erasure_set(&self, buckets: Vec<String>, set_disk_id: String) -> Result<()> {
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_ERASURE_SET_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            set_disk_id,
            bucket_count = buckets.len(),
            stage = "start",
            "Heal erasure set started"
        );

        // update progress
        {
            let mut progress = self.progress.write().await;
            progress.set_current_object(Some(format!("erasure_set: {} ({} buckets)", set_disk_id, buckets.len())));
            progress.update_stage(0, 4);
        }

        let is_auto_replacement = matches!(self.source, HealRequestSource::AutoHeal) && !self.heal_endpoints.is_empty();
        let replacement_resume_disk = if is_auto_replacement {
            let mut requested_targets = self.heal_endpoints.clone();
            requested_targets.sort_unstable();
            requested_targets.dedup();
            let selection = self
                .await_with_control(
                    self.storage
                        .get_replacement_resume_disk(&set_disk_id, &self.id, &self.heal_endpoints),
                )
                .await?;
            let disk = match selection {
                crate::heal::storage::ReplacementResumeDisk::Existing(disk) => {
                    if let Some(anchor) = &self.replacement_resume_endpoint
                        && disk.endpoint().to_string() != *anchor
                    {
                        return Err(Error::TaskExecutionFailed {
                            message: format!("Replacement resume anchor changed for automatic heal {set_disk_id}"),
                        });
                    }
                    Some(disk)
                }
                crate::heal::storage::ReplacementResumeDisk::Fresh => {
                    if self.replacement_resume_endpoint.is_some() {
                        return Err(Error::TaskExecutionFailed {
                            message: format!("Replacement resume anchor is unavailable for automatic heal {set_disk_id}"),
                        });
                    }
                    None
                }
            };
            if let Some(disk) = disk.as_ref()
                && ResumeManager::has_replacement_intent(disk, &self.id).await
            {
                let resume_manager = ResumeManager::load_replacement_intent(disk.clone(), &self.id).await?;
                let state = resume_manager.get_state().await;
                if state.completed
                    && matches!(state.replacement_phase, ReplacementPhase::CleanupPending)
                    && state.set_disk_id == set_disk_id
                    && state.replacement_targets == requested_targets
                    && state.replacement_generation.as_deref() == Some(self.id.as_str())
                {
                    resume_manager.ensure_replacement_completion_proof().await?;
                    if CheckpointManager::has_checkpoint(disk, &self.id).await {
                        CheckpointManager::load_from_disk(disk.clone(), &self.id)
                            .await?
                            .cleanup()
                            .await?;
                    }
                    resume_manager.cleanup().await?;
                    return Ok(());
                }
            }
            disk
        } else {
            None
        };

        if is_auto_replacement
            && !self
                .await_with_control(self.storage.replacement_targets_ready(&self.heal_endpoints))
                .await?
        {
            return Err(Error::TaskExecutionFailed {
                message: format!("Replacement target is no longer ready for automatic heal {set_disk_id}"),
            });
        }

        let replacement_resume_disk = if is_auto_replacement {
            Some(match replacement_resume_disk {
                Some(disk) => disk,
                None => {
                    self.await_with_control(self.storage.get_disk_for_resume_excluding(&set_disk_id, &self.heal_endpoints))
                        .await?
                }
            })
        } else {
            None
        };

        let mut buckets = if buckets.is_empty() {
            debug!(
                target: "rustfs::heal::task",
                event = EVENT_HEAL_ERASURE_SET_STAGE,
                component = LOG_COMPONENT_HEAL,
                subsystem = LOG_SUBSYSTEM_TASK,
                task_id = %self.id,
                set_disk_id,
                stage = "list_buckets",
                "Heal erasure set bucket list resolved"
            );
            let bucket_infos = self.await_with_control(self.storage.list_buckets()).await?;
            bucket_infos.into_iter().map(|info| info.name).collect()
        } else {
            buckets
        };

        // Persist automatic replacement intent on a surviving disk before the
        // first target format write. A task retry keeps this id; a newly
        // admitted blank replacement gets a fresh id and cannot reuse cursor
        // progress from an older disk at the same endpoint.
        let replacement_resume = if is_auto_replacement {
            let identities = self
                .await_with_control(self.storage.replacement_target_identities(&self.heal_endpoints))
                .await?;
            let disk = replacement_resume_disk.clone().ok_or_else(|| Error::TaskExecutionFailed {
                message: format!("Replacement resume disk is missing for automatic heal {set_disk_id}"),
            })?;
            let manager = ResumeManager::new_replacement_intent(
                disk.clone(),
                self.id.clone(),
                set_disk_id.clone(),
                buckets.clone(),
                self.heal_endpoints.clone(),
                identities.clone(),
            )
            .await?;
            buckets = manager.get_state().await.replacement_buckets;
            Some((disk, manager, identities))
        } else {
            None
        };

        self.apply_erasure_set_usage_baseline(&buckets).await?;

        let healing_marker = format!("{set_disk_id}:{}", self.id);
        if let Some((disk, resume_manager, _)) = replacement_resume.as_ref() {
            let state = resume_manager.get_state().await;
            if state.completed && matches!(state.replacement_phase, ReplacementPhase::Verified) {
                resume_manager.ensure_replacement_completion_proof().await?;
                super::super::clear_healing_markers_after_verified(&self.heal_endpoints, &healing_marker).await?;
                resume_manager.mark_replacement_cleanup_pending().await?;
                if CheckpointManager::has_checkpoint(disk, &self.id).await {
                    CheckpointManager::load_from_disk(disk.clone(), &self.id)
                        .await?
                        .cleanup()
                        .await?;
                }
                resume_manager.cleanup().await?;
                return Ok(());
            }
        }

        // Step 1: Perform disk format heal using ecstore
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_ERASURE_SET_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            set_disk_id,
            stage = "heal_format",
            "Heal erasure set stage entered"
        );
        if is_auto_replacement {
            let Some((_, _, expected_identities)) = replacement_resume.as_ref() else {
                return Err(Error::TaskExecutionFailed {
                    message: format!("Replacement intent is missing for automatic heal {set_disk_id}"),
                });
            };
            self.verify_replacement_identity_fence(expected_identities, &set_disk_id, "format")
                .await?;
        }
        let format_result = if is_auto_replacement {
            let pool_index = self.options.pool_index.ok_or_else(|| Error::TaskExecutionFailed {
                message: format!("Missing pool scope for automatic replacement heal {set_disk_id}"),
            })?;
            let set_index = self.options.set_index.ok_or_else(|| Error::TaskExecutionFailed {
                message: format!("Missing set scope for automatic replacement heal {set_disk_id}"),
            })?;
            self.await_with_control(self.storage.heal_replacement_format(
                self.options.dry_run,
                pool_index,
                set_index,
                &self.heal_endpoints,
            ))
            .await
        } else {
            self.await_with_control(self.storage.heal_format(self.options.dry_run)).await
        };

        match format_result {
            Ok((result, error)) => {
                if let Some(e) = error {
                    if Self::is_no_heal_required_error(&e) {
                        debug!(
                            target: "rustfs::heal::task",
                            event = EVENT_HEAL_ERASURE_SET_RESULT,
                            component = LOG_COMPONENT_HEAL,
                            subsystem = LOG_SUBSYSTEM_TASK,
                            task_id = %self.id,
                            set_disk_id,
                            result = "format_noop",
                            "Heal erasure set format repair skipped because no format heal was required"
                        );
                    } else {
                        let error = e;
                        if error.is_recoverable_heal() {
                            return Err(error);
                        }
                        error!(
                            target: "rustfs::heal::task",
                            event = EVENT_HEAL_ERASURE_SET_RESULT,
                            component = LOG_COMPONENT_HEAL,
                            subsystem = LOG_SUBSYSTEM_TASK,
                            task_id = %self.id,
                            set_disk_id,
                            result = "format_failed",
                            error = %error,
                            "Heal erasure set failed"
                        );
                        {
                            let mut progress = self.progress.write().await;
                            progress.update_stage(4, 4);
                        }
                        return Err(Error::TaskExecutionFailed {
                            message: format!("Failed to heal disk format for {set_disk_id}: {error}"),
                        });
                    }
                } else {
                    debug!(
                        target: "rustfs::heal::task",
                        event = EVENT_HEAL_ERASURE_SET_RESULT,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_TASK,
                        task_id = %self.id,
                        set_disk_id,
                        drives_healed = result.drives_healed(),
                    drives_total = result.drives_reported(),
                        result = "format_ok",
                        "Heal erasure set format repaired"
                    );
                }
                if !self.options.dry_run && !target_outcomes_complete(&result, &self.heal_endpoints) {
                    return Err(Error::TaskExecutionFailed {
                        message: format!("Failed to verify formatted replacement targets for {set_disk_id}"),
                    });
                }
                if let Some((_, replacement_resume, expected_identities)) = &replacement_resume {
                    let identities = self
                        .await_with_control(self.storage.replacement_target_identities(&self.heal_endpoints))
                        .await?;
                    if !replacement_target_identities_match(expected_identities, &identities) {
                        return Err(Error::TaskExecutionFailed {
                            message: format!("Replacement target changed after format for automatic heal {set_disk_id}"),
                        });
                    }
                    replacement_resume.mark_replacement_rebuilding(identities).await?;
                }
            }
            Err(Error::TaskCancelled) => return Err(Error::TaskCancelled),
            Err(Error::TaskTimeout) => return Err(Error::TaskTimeout),
            Err(e) => {
                if e.is_recoverable_heal() {
                    return Err(e);
                }
                error!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_ERASURE_SET_RESULT,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_TASK,
                    task_id = %self.id,
                    set_disk_id,
                    result = "format_failed",
                    error = %e,
                    "Heal erasure set failed"
                );
                {
                    let mut progress = self.progress.write().await;
                    progress.update_stage(4, 4);
                }
                return Err(Error::TaskExecutionFailed {
                    message: format!("Failed to heal disk format for {set_disk_id}: {e}"),
                });
            }
        }

        {
            let mut progress = self.progress.write().await;
            progress.update_stage(1, 4);
        }

        // The rebuilt disks are formatted now: mark them as healing so
        // DiskInfo.healing reflects the rebuild until it completes.
        super::super::set_healing_markers(&self.heal_endpoints, &healing_marker).await?;

        // Step 2: Get disk for resume functionality
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_ERASURE_SET_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            set_disk_id,
            stage = "resolve_resume_disk",
            "Heal erasure set stage entered"
        );
        let replacement_target_identities = replacement_resume.as_ref().map(|(_, _, identities)| identities.clone());
        let disk = match replacement_resume.as_ref() {
            Some((disk, _, _)) => disk.clone(),
            None => {
                self.await_with_control(self.storage.get_disk_for_resume(&set_disk_id))
                    .await?
            }
        };

        {
            let mut progress = self.progress.write().await;
            progress.update_stage(2, 4);
        }

        // Step 3: Heal bucket structure
        // Check control flags before each iteration to ensure timely cancellation.
        let bucket_heal_opts = HealOpts {
            recursive: false,
            dry_run: self.options.dry_run,
            remove: false,
            recreate: self.options.recreate_missing,
            scan_mode: self.options.scan_mode,
            update_parity: self.options.update_parity,
            no_lock: self.options.no_lock,
            pool: self.options.pool_index,
            set: self.options.set_index,
        };

        for bucket in buckets.iter() {
            // Check control flags before starting each bucket heal
            self.check_control_flags().await?;
            if let Some(expected_identities) = replacement_target_identities.as_ref() {
                self.verify_replacement_identity_fence(expected_identities, &set_disk_id, "bucket prepass")
                    .await?;
            }
            let heal_result = self
                .await_with_control(self.storage.heal_bucket(bucket, &bucket_heal_opts))
                .await;
            match heal_result {
                Ok(result) => {
                    self.record_result_item(result).await;
                }
                Err(err) => {
                    warn!(
                        target: "rustfs::heal::task",
                        event = EVENT_HEAL_ERASURE_SET_RESULT,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_TASK,
                        task_id = %self.id,
                        set_disk_id,
                        bucket,
                        result = "bucket_failed",
                        error = %err,
                        "Heal erasure set bucket prepass failed"
                    );
                    return Err(err);
                }
            }
        }

        // Create erasure set healer with resume support
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_ERASURE_SET_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            set_disk_id,
            stage = "build_resumable_healer",
            "Heal erasure set stage entered"
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
        let erasure_healer = ErasureSetHealer::new(
            self.storage.clone(),
            self.progress.clone(),
            self.cancel_token.clone(),
            disk,
            heal_opts,
            self.source,
        )
        .with_replacement_targets(self.heal_endpoints.clone(), is_auto_replacement.then(|| self.id.clone()))
        .with_replacement_identity_fence(replacement_target_identities.clone());

        {
            let mut progress = self.progress.write().await;
            progress.update_stage(3, 4);
        }

        // Step 4: Execute erasure set heal with resume
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_ERASURE_SET_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            set_disk_id,
            stage = "execute_resumable_heal",
            "Heal erasure set stage entered"
        );
        let result = self
            .await_with_control(erasure_healer.heal_erasure_set(&buckets, &set_disk_id))
            .await;

        // Keep the markers on failure: the resume state also persists, and the
        // next run of this set heal re-marks and eventually clears them.
        let result = match result {
            Ok(()) => {
                if let Some(expected_identities) = replacement_target_identities.as_ref() {
                    self.verify_replacement_identity_fence(expected_identities, &set_disk_id, "marker completion")
                        .await?;
                }
                super::super::clear_healing_markers_after_verified(&self.heal_endpoints, &healing_marker).await?;
                if let Some((disk, resume_manager, _)) = replacement_resume.as_ref() {
                    resume_manager.mark_replacement_cleanup_pending().await?;
                    if CheckpointManager::has_checkpoint(disk, &self.id).await {
                        CheckpointManager::load_from_disk(disk.clone(), &self.id)
                            .await?
                            .cleanup()
                            .await?;
                    }
                    resume_manager.cleanup().await?;
                }
                Ok(())
            }
            Err(err) => Err(err),
        };

        {
            self.progress.write().await.update_stage(4, 4);
        }

        match result {
            Ok(_) => {
                debug!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_ERASURE_SET_RESULT,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_TASK,
                    task_id = %self.id,
                    set_disk_id,
                    bucket_count = buckets.len(),
                    result = "ok",
                    "Heal erasure set repaired"
                );
                Ok(())
            }
            Err(Error::TaskCancelled) => Err(Error::TaskCancelled),
            Err(Error::TaskTimeout) => Err(Error::TaskTimeout),
            Err(e) => {
                error!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_ERASURE_SET_RESULT,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_TASK,
                    task_id = %self.id,
                    set_disk_id,
                    result = "failed",
                    error = %e,
                    "Heal erasure set failed"
                );
                Err(Error::TaskExecutionFailed {
                    message: format!("Failed to heal erasure set {set_disk_id}: {e}"),
                })
            }
        }
    }
}
