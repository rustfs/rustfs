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
/// metadata and erasure-decode heal for a single object version
use super::*;

impl HealTask {
    pub(super) async fn heal_metadata(&self, bucket: &str, object: &str) -> Result<()> {
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_METADATA_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            bucket,
            object,
            stage = "start",
            "Heal metadata started"
        );

        // update progress
        {
            let mut progress = self.progress.write().await;
            progress.set_current_object(Some(format!("metadata: {bucket}/{object}")));
            progress.update_stage(0, 3);
        }

        // Step 1: Check if object exists
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_METADATA_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            bucket,
            object,
            stage = "check_existence",
            "Heal metadata stage entered"
        );
        self.check_control_flags().await?;
        let object_exists = match self.await_with_control(self.storage.object_exists(bucket, object)).await {
            Ok(exists) => exists,
            Err(err @ Error::TransientSkip { .. }) => {
                return self.skip_due_to_transient_object_exists(bucket, object, &err).await;
            }
            Err(err) => return Err(err),
        };
        if !object_exists {
            warn!(
                target: "rustfs::heal::task",
                event = EVENT_HEAL_METADATA_RESULT,
                component = LOG_COMPONENT_HEAL,
                subsystem = LOG_SUBSYSTEM_TASK,
                task_id = %self.id,
                bucket,
                object,
                result = "missing",
                "Heal metadata failed because object is missing"
            );
            return Err(Error::TaskExecutionFailed {
                message: format!("Object not found: {bucket}/{object}"),
            });
        }

        {
            let mut progress = self.progress.write().await;
            progress.update_stage(1, 3);
        }

        // Step 2: Perform metadata heal using ecstore
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_METADATA_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            bucket,
            object,
            stage = "heal_with_ecstore",
            "Heal metadata stage entered"
        );
        let heal_opts = HealOpts {
            recursive: false,
            dry_run: self.options.dry_run,
            remove: false,
            recreate: false,
            scan_mode: HealScanMode::Deep,
            update_parity: false,
            no_lock: self.options.no_lock,
            pool: self.options.pool_index,
            set: self.options.set_index,
        };

        let heal_result = self
            .await_with_control(self.storage.heal_object(bucket, object, None, &heal_opts))
            .await;

        match heal_result {
            Ok((result, error)) => {
                if let Some(e) = error {
                    error!(
                        target: "rustfs::heal::task",
                        event = EVENT_HEAL_METADATA_RESULT,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_TASK,
                        task_id = %self.id,
                        bucket,
                        object,
                        result = "failed",
                        error = %e,
                        "Heal metadata failed"
                    );
                    {
                        let mut progress = self.progress.write().await;
                        progress.update_stage(3, 3);
                    }
                    return Err(Error::TaskExecutionFailed {
                        message: format!("Failed to heal metadata {bucket}/{object}: {e}"),
                    });
                }

                debug!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_METADATA_RESULT,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_TASK,
                    task_id = %self.id,
                    bucket,
                    object,
                    drives_healed = result.drives_healed(),
                    drives_total = result.drives_reported(),
                    result = "ok",
                    "Heal metadata repaired"
                );

                {
                    let mut progress = self.progress.write().await;
                    progress.update_stage(3, 3);
                }
                self.record_result_item(result).await;
                Ok(())
            }
            Err(Error::TaskCancelled) => Err(Error::TaskCancelled),
            Err(Error::TaskTimeout) => Err(Error::TaskTimeout),
            Err(e) => {
                error!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_METADATA_RESULT,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_TASK,
                    task_id = %self.id,
                    bucket,
                    object,
                    result = "failed",
                    error = %e,
                    "Heal metadata failed"
                );
                {
                    let mut progress = self.progress.write().await;
                    progress.update_stage(3, 3);
                }
                Err(Error::TaskExecutionFailed {
                    message: format!("Failed to heal metadata {bucket}/{object}: {e}"),
                })
            }
        }
    }

    pub(super) async fn heal_ec_decode(&self, bucket: &str, object: &str, version_id: Option<&str>) -> Result<()> {
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_EC_DECODE_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            bucket,
            object,
            version_id = ?version_id,
            stage = "start",
            "Heal EC decode started"
        );

        // update progress
        {
            let mut progress = self.progress.write().await;
            progress.set_current_object(Some(format!("ec_decode: {bucket}/{object}")));
            progress.update_stage(0, 3);
        }

        // Step 1: Check if object exists
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_EC_DECODE_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            bucket,
            object,
            stage = "check_existence",
            "Heal EC decode stage entered"
        );
        self.check_control_flags().await?;
        let object_exists = match self.await_with_control(self.storage.object_exists(bucket, object)).await {
            Ok(exists) => exists,
            Err(err @ Error::TransientSkip { .. }) => {
                return self.skip_due_to_transient_object_exists(bucket, object, &err).await;
            }
            Err(err) => return Err(err),
        };
        if !object_exists {
            warn!(
                target: "rustfs::heal::task",
                event = EVENT_HEAL_EC_DECODE_RESULT,
                component = LOG_COMPONENT_HEAL,
                subsystem = LOG_SUBSYSTEM_TASK,
                task_id = %self.id,
                bucket,
                object,
                result = "missing",
                "Heal EC decode failed because object is missing"
            );
            return Err(Error::TaskExecutionFailed {
                message: format!("Object not found: {bucket}/{object}"),
            });
        }

        {
            let mut progress = self.progress.write().await;
            progress.update_stage(1, 3);
        }

        // Step 2: Perform EC decode heal using ecstore
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_EC_DECODE_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            bucket,
            object,
            stage = "heal_with_ecstore",
            "Heal EC decode stage entered"
        );
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

        let heal_result = self
            .await_with_control(self.storage.heal_object(bucket, object, version_id, &heal_opts))
            .await;

        match heal_result {
            Ok((result, error)) => {
                if let Some(e) = error {
                    error!(
                        target: "rustfs::heal::task",
                        event = EVENT_HEAL_EC_DECODE_RESULT,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_TASK,
                        task_id = %self.id,
                        bucket,
                        object,
                        result = "failed",
                        error = %e,
                        "Heal EC decode failed"
                    );
                    {
                        let mut progress = self.progress.write().await;
                        progress.update_stage(3, 3);
                    }
                    return Err(Error::TaskExecutionFailed {
                        message: format!("Failed to heal EC decode {bucket}/{object}: {e}"),
                    });
                }

                let object_size = result.object_size as u64;
                debug!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_EC_DECODE_RESULT,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_TASK,
                    task_id = %self.id,
                    bucket,
                    object,
                    object_size,
                    drives_healed = result.drives_healed(),
                    drives_total = result.drives_reported(),
                    result = "ok",
                    "Heal EC decode repaired"
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
                error!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_EC_DECODE_RESULT,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_TASK,
                    task_id = %self.id,
                    bucket,
                    object,
                    result = "failed",
                    error = %e,
                    "Heal EC decode failed"
                );
                {
                    let mut progress = self.progress.write().await;
                    progress.update_stage(3, 3);
                }
                Err(Error::TaskExecutionFailed {
                    message: format!("Failed to heal EC decode {bucket}/{object}: {e}"),
                })
            }
        }
    }
}
