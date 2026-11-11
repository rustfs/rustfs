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
use crate::heal::storage::HealStorageAPI;
use crate::heal::storage_api::storage::DiskSetSelector;
use std::collections::BTreeMap;

async fn namespace_snapshot(storage: &dyn HealStorageAPI, state: &ResumeState) -> Result<BTreeMap<String, Uuid>> {
    let (pool, set) = crate::heal::utils::parse_set_disk_id(&state.set_disk_id)?;
    // This API rejects partial topology and propagates read-quorum failures.
    let buckets = storage
        .list_buckets_for_admin_heal(Some(DiskSetSelector::new(pool, set)))
        .await?;
    let mut identities = BTreeMap::new();
    for bucket in buckets {
        let Some(incarnation) = storage.replacement_bucket_incarnation(&bucket.name).await? else {
            // A returning disk may still list a retired volume. Only the admitted
            // old UUID plus its durable receipt can discharge that obligation.
            if let Some(expected) = state
                .replacement_bucket_incarnations
                .as_ref()
                .and_then(|map| map.get(&bucket.name))
                .or_else(|| state.replacement_retired_bucket_incarnations.get(&bucket.name))
                && storage.mrf_bucket_incarnation_retired(&bucket.name, *expected).await?
            {
                continue;
            }
            return Err(Error::ReplacementNamespaceChanged);
        };
        if incarnation.is_nil() {
            return Err(Error::InvalidBucketIncarnation);
        }
        identities.insert(bucket.name, incarnation);
    }
    Ok(identities)
}

impl ResumeManager {
    pub(crate) async fn replacement_bucket_incarnation(&self, bucket: &str) -> Result<Uuid> {
        self.state
            .read()
            .await
            .replacement_bucket_incarnations
            .as_ref()
            .and_then(|identities| identities.get(bucket).copied())
            .ok_or(Error::ReplacementNamespaceChanged)
    }

    pub(crate) async fn prepare_replacement_bucket_pass(&self, storage: &dyn HealStorageAPI) -> Result<Vec<String>> {
        let previous = self.get_state().await;
        let current = namespace_snapshot(storage, &previous).await?;
        // Recorded identities that are gone or replaced. Only a durable deletion
        // receipt discharges one; absence or a different identity alone does not.
        let mut retired = BTreeMap::new();
        if let Some(identities) = previous.replacement_bucket_incarnations.as_ref() {
            for (bucket, expected) in identities {
                if current.get(bucket) != Some(expected) {
                    if !storage.mrf_bucket_incarnation_retired(bucket, *expected).await? {
                        return Err(Error::ReplacementNamespaceChanged);
                    }
                    retired.insert(bucket.clone(), *expected);
                }
            }
        }
        match previous.replacement_bucket_incarnations.as_ref() {
            // The first pass of this generation, or a recorded bucket was deleted
            // (and possibly recreated): the pass restarts over the current namespace.
            None => self.restart_replacement_bucket_pass(current, retired).await?,
            Some(_) if !retired.is_empty() => self.restart_replacement_bucket_pass(current, retired).await?,
            // Buckets created since the snapshot join the pass so their identity
            // is fenced like every other bucket, without discarding progress.
            Some(identities) => {
                let added = current
                    .iter()
                    .filter(|(bucket, _)| !identities.contains_key(*bucket))
                    .map(|(bucket, incarnation)| (bucket.clone(), *incarnation))
                    .collect::<Vec<_>>();
                if !added.is_empty() {
                    let mut state = self.state.write().await;
                    if state.replacement_bucket_pass_order.is_empty() {
                        state.replacement_bucket_pass_order = identities.keys().cloned().collect();
                    }
                    for (bucket, incarnation) in added {
                        state
                            .replacement_bucket_incarnations
                            .get_or_insert_with(BTreeMap::new)
                            .insert(bucket.clone(), incarnation);
                        state.replacement_bucket_pass_order.push(bucket.clone());
                        state.pending_buckets.push(bucket);
                    }
                    drop(state);
                    self.save_state_strict().await?;
                }
            }
        }
        if self.get_state().await.replacement_bucket_pass_reset_pending {
            let checkpoint = if CheckpointManager::has_checkpoint(&self.disk, &previous.task_id).await {
                CheckpointManager::load_from_disk(self.disk.clone(), &previous.task_id).await?
            } else {
                CheckpointManager::new(self.disk.clone(), previous.task_id.clone()).await?
            };
            // Unlike the throttled retry writer this is durable before clearing the intent.
            checkpoint.reset_after_delete_recovery().await?;
            self.state.write().await.replacement_bucket_pass_reset_pending = false;
            self.save_state_strict().await?;
        }
        Ok(self.get_state().await.replacement_bucket_pass_buckets())
    }

    /// Bind a fresh pass to `current`, publishing the checkpoint rewind first.
    async fn restart_replacement_bucket_pass(
        &self,
        current: BTreeMap<String, Uuid>,
        retired: BTreeMap<String, Uuid>,
    ) -> Result<()> {
        let mut state = self.state.write().await;
        state.replacement_retired_bucket_incarnations.extend(retired);
        state.replacement_bucket_pass_order = current.keys().cloned().collect();
        state.pending_buckets = current.keys().cloned().collect();
        state.replacement_bucket_incarnations = Some(current);
        if state.replacement_generation.is_some() {
            // Protocol-1 readers reject this before interpreting a new pass cursor.
            state.replacement_execution_protocol = 2;
        }
        state.replacement_bucket_pass_reset_pending = true;
        state.reset_for_retry();
        state.total_objects = 0;
        state.total_bytes = 0;
        state.baseline_generation = None;
        state.baseline_known = false;
        drop(state);
        self.save_state_strict().await
    }

    pub(crate) async fn verify_replacement_bucket_pass(&self, storage: &dyn HealStorageAPI) -> Result<()> {
        let state = self.get_state().await;
        let current = namespace_snapshot(storage, &state).await?;
        let Some(identities) = state.replacement_bucket_incarnations.as_ref() else {
            return Err(Error::ReplacementNamespaceChanged);
        };
        // Buckets created during the pass have nothing the pass could have
        // missed; only a recorded identity that is gone or replaced fails it.
        if identities
            .iter()
            .any(|(bucket, expected)| current.get(bucket) != Some(expected))
        {
            return Err(Error::ReplacementNamespaceChanged);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::heal::storage::{ECStoreHealStorage, HealStorageAPI};
    use crate::heal::storage_api::{
        storage::{BucketOperations as _, DeleteBucketOptions, MakeBucketOptions, ObjectIO as _},
        test_support::{
            ObjectOptions, PutObjReader, delete_config, init_bucket_metadata_sys, isolated_store_over_temp_disks, read_config,
            save_config,
        },
    };
    use rustfs_heal_contracts::heal_channel::HealOpts;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    #[serial_test::serial]
    async fn replacement_bucket_pass_retires_deleted_identity_and_restarts_recreated_bucket() {
        // Keep the concrete storage futures off the test executor stack.
        let (directories, store) = Box::pin(isolated_store_over_temp_disks()).await;
        init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let bucket = "replacement-pass-bucket";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("create original generation");
        let old = store
            .bucket_incarnation_id_from_disk(bucket)
            .await
            .expect("original identity");
        let storage = ECStoreHealStorage::new(store.clone());
        let (_anchor_dir, disk) = super::super::tests::schema_test_disk().await;
        let task_id = ResumeUtils::generate_task_id();
        let manager = ResumeManager::new_replacement_intent(
            disk.clone(),
            task_id.clone(),
            "pool_0_set_0".to_string(),
            vec![bucket.to_owned()],
            vec!["replacement-a".to_string()],
            vec![ReplacementTargetIdentity {
                endpoint: "replacement-a".to_string(),
                canonical_path: "/mnt/replacement-a".to_string(),
                physical_device_ids: vec!["device-a".to_string()],
                filesystem_identity: "1:2:3".to_string(),
            }],
        )
        .await
        .expect("persist generation");
        // A legacy name-only cursor cannot be attributed to the currently live bucket.
        manager
            .set_resume_cursor(Some("unbound-legacy-cursor".to_string()))
            .await
            .expect("legacy cursor");
        let legacy_checkpoint = CheckpointManager::new(disk.clone(), task_id.clone())
            .await
            .expect("legacy checkpoint");
        legacy_checkpoint
            .add_processed_object("legacy-key".to_string())
            .await
            .expect("legacy dedup");
        legacy_checkpoint.complete_page(0, 99).await.expect("persist legacy cursor");
        assert_eq!(
            manager
                .prepare_replacement_bucket_pass(&storage)
                .await
                .expect("admit original pass"),
            [bucket]
        );
        assert_eq!(
            manager
                .get_state()
                .await
                .replacement_bucket_incarnations
                .as_ref()
                .and_then(|m| m.get(bucket))
                .copied(),
            Some(old)
        );
        assert!(
            manager.get_state().await.resume_cursor.is_none(),
            "legacy cursor must not enter the fresh pass"
        );
        let fresh_checkpoint = CheckpointManager::load_from_disk(disk.clone(), &task_id)
            .await
            .expect("fresh checkpoint")
            .get_checkpoint()
            .await;
        assert_eq!(fresh_checkpoint.current_object_index, 0);
        assert!(fresh_checkpoint.processed_objects.is_empty());
        manager
            .set_resume_cursor(Some("old-generation-cursor".to_string()))
            .await
            .expect("persist old cursor");
        let checkpoint = CheckpointManager::load_from_disk(disk.clone(), &task_id)
            .await
            .expect("checkpoint");
        checkpoint
            .add_processed_object("same-key".to_string())
            .await
            .expect("old dedup entry");
        checkpoint.complete_page(0, 7).await.expect("persist old position and dedup");

        store
            .delete_bucket(bucket, &DeleteBucketOptions::default())
            .await
            .expect("delete original generation");
        assert!(matches!(
            manager.verify_replacement_bucket_pass(&storage).await,
            Err(Error::ReplacementNamespaceChanged)
        ));
        manager
            .record_replacement_failure(&Error::ReplacementNamespaceChanged, 99)
            .await
            .expect("defer without exhausting replacement");
        assert_eq!(manager.get_state().await.retry_count, 0);
        let resumed = ResumeManager::load_replacement_intent(disk.clone(), &task_id)
            .await
            .expect("restart replacement manager");
        let retirement_path = format!("bucket-retirements/{bucket}/{old}.json");
        let retirement = Box::pin(read_config(store.clone(), &retirement_path))
            .await
            .expect("authoritative retirement");
        Box::pin(delete_config(store.clone(), &retirement_path))
            .await
            .expect("simulate missing retirement evidence");
        assert!(
            matches!(
                resumed.prepare_replacement_bucket_pass(&storage).await,
                Err(Error::ReplacementNamespaceChanged)
            ),
            "physical absence alone cannot retire an admitted identity"
        );
        Box::pin(save_config(store.clone(), &retirement_path, b"corrupt retirement".to_vec()))
            .await
            .expect("inject unreadable proof");
        assert!(
            matches!(resumed.prepare_replacement_bucket_pass(&storage).await, Err(Error::Storage(_))),
            "a corrupt proof cannot be treated as an absent bucket"
        );
        assert_eq!(resumed.get_state().await.resume_cursor.as_deref(), Some("old-generation-cursor"));
        Box::pin(save_config(store.clone(), &retirement_path, retirement))
            .await
            .expect("restore proof");
        assert!(
            resumed
                .prepare_replacement_bucket_pass(&storage)
                .await
                .expect("retirement proves deleted plan item")
                .is_empty()
        );

        // A disk returning after DELETE may expose the retired physical volume
        // again; the persisted receipt identity still discharges that old name.
        tokio::fs::create_dir(directories[0].path().join(bucket))
            .await
            .expect("return stale retired volume");
        let returned = ResumeManager::load_replacement_intent(disk.clone(), &task_id)
            .await
            .expect("restart after retirement");
        assert!(
            returned
                .prepare_replacement_bucket_pass(&storage)
                .await
                .expect("retired stale volume is not a new bucket")
                .is_empty()
        );
        tokio::fs::remove_dir(directories[0].path().join(bucket))
            .await
            .expect("remove stale test volume");
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("recreate same name");
        let new = store.bucket_incarnation_id_from_disk(bucket).await.expect("new identity");
        assert_ne!(new, old);
        Box::pin(store.put_object(
            bucket,
            "same-key",
            &mut PutObjReader::from_vec(b"new payload".to_vec()),
            &ObjectOptions::default(),
        ))
        .await
        .expect("new generation payload");
        assert_eq!(
            resumed
                .prepare_replacement_bucket_pass(&storage)
                .await
                .expect("new generation gets independent pass"),
            [bucket]
        );
        let state = resumed.get_state().await;
        assert_eq!(
            state.replacement_execution_protocol, 2,
            "older binaries must reject identity-bound cursors"
        );
        assert_eq!(
            state
                .replacement_bucket_incarnations
                .as_ref()
                .and_then(|m| m.get(bucket))
                .copied(),
            Some(new)
        );
        assert!(state.resume_cursor.is_none());
        let checkpoint = CheckpointManager::load_from_disk(disk.clone(), &task_id)
            .await
            .expect("restarted checkpoint")
            .get_checkpoint()
            .await;
        assert_eq!((checkpoint.current_bucket_index, checkpoint.current_object_index), (0, 0));
        assert!(checkpoint.processed_objects.is_empty());

        let opts = HealOpts {
            pool: Some(0),
            set: Some(0),
            ..Default::default()
        };
        assert!(
            storage
                .heal_replacement_object_at_incarnation(bucket, "same-key", None, old, &opts, &CancellationToken::new())
                .await
                .is_err(),
            "a stale worker cannot mutate a recreated bucket"
        );
        // Name targets as the store's endpoints render them (`C:/...` on Windows),
        // not as raw OS paths.
        let targets = directories
            .iter()
            .map(|dir| {
                crate::heal::Endpoint::try_from(dir.path().to_str().expect("UTF-8 temp disk path"))
                    .expect("temp disk endpoint")
                    .to_string()
            })
            .collect::<Vec<_>>();
        assert!(
            storage
                .heal_replacement_bucket_metadata_at_incarnation(bucket, old, &opts, &targets)
                .await
                .is_err(),
            "a stale metadata pass cannot certify the new incarnation"
        );
        storage
            .heal_replacement_bucket_metadata_at_incarnation(bucket, new, &opts, &targets)
            .await
            .expect("current incarnation can repair metadata");
        let mut reader =
            Box::pin(store.get_object_reader(bucket, "same-key", None, Default::default(), &ObjectOptions::default()))
                .await
                .expect("open new generation payload");
        assert_eq!(reader.read_all().await.expect("read new payload"), b"new payload");
        drop(reader);
        assert_eq!(
            store
                .bucket_incarnation_id_from_disk(bucket)
                .await
                .expect("new identity remains"),
            new
        );
        // Simulate interruption after publishing the reset obligation, before
        // replacing the old checkpoint.
        let stale_checkpoint = CheckpointManager::load_from_disk(disk.clone(), &task_id)
            .await
            .expect("checkpoint before interruption");
        stale_checkpoint
            .add_processed_object("stale-before-reset".to_string())
            .await
            .expect("stale dedup");
        stale_checkpoint
            .complete_page(0, 42)
            .await
            .expect("persist interrupted checkpoint");
        resumed.state.write().await.replacement_bucket_pass_reset_pending = true;
        resumed.save_state_strict().await.expect("persist reset obligation");
        let interrupted = ResumeManager::load_replacement_intent(disk.clone(), &task_id)
            .await
            .expect("restart interrupted pass");
        interrupted
            .prepare_replacement_bucket_pass(&storage)
            .await
            .expect("finish interrupted checkpoint rewind");
        let repaired_checkpoint = CheckpointManager::load_from_disk(disk.clone(), &task_id)
            .await
            .expect("repaired checkpoint")
            .get_checkpoint()
            .await;
        assert_eq!(repaired_checkpoint.current_object_index, 0);
        assert!(repaired_checkpoint.processed_objects.is_empty());
        assert!(!interrupted.get_state().await.replacement_bucket_pass_reset_pending);
        let loaded = ResumeManager::load_replacement_intent(disk, &task_id)
            .await
            .expect("new pass survives restart");
        loaded
            .verify_replacement_bucket_pass(&storage)
            .await
            .expect("new pass matches current namespace");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn replacement_bucket_pass_keeps_progress_when_a_bucket_is_created_mid_pass() {
        let (_directories, store) = Box::pin(isolated_store_over_temp_disks()).await;
        init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let recorded = "pass-recorded";
        store
            .make_bucket(recorded, &MakeBucketOptions::default())
            .await
            .expect("create the recorded bucket");
        let storage = ECStoreHealStorage::new(store.clone());
        let (_anchor_dir, disk) = super::super::tests::schema_test_disk().await;
        let task_id = ResumeUtils::generate_task_id();
        let manager = ResumeManager::new_replacement_intent(
            disk.clone(),
            task_id.clone(),
            "pool_0_set_0".to_string(),
            vec![recorded.to_owned()],
            vec!["replacement-a".to_string()],
            vec![ReplacementTargetIdentity {
                endpoint: "replacement-a".to_string(),
                canonical_path: "/mnt/replacement-a".to_string(),
                physical_device_ids: vec!["device-a".to_string()],
                filesystem_identity: "1:2:3".to_string(),
            }],
        )
        .await
        .expect("persist generation");
        assert_eq!(
            manager
                .prepare_replacement_bucket_pass(&storage)
                .await
                .expect("admit the recorded pass"),
            [recorded]
        );
        // Progress into the recorded bucket.
        manager
            .set_resume_cursor(Some("mid-pass-cursor".to_string()))
            .await
            .expect("persist mid-pass cursor");
        let checkpoint = CheckpointManager::load_from_disk(disk.clone(), &task_id)
            .await
            .expect("admitted checkpoint");
        checkpoint.complete_page(0, 7).await.expect("persist mid-pass position");

        // The new name sorts before the recorded one, so a sorted rebuild would
        // move the recorded bucket away from the checkpoint's index.
        let created = "pass-created";
        store
            .make_bucket(created, &MakeBucketOptions::default())
            .await
            .expect("create a bucket during the pass");
        let created_identity = store
            .bucket_incarnation_id_from_disk(created)
            .await
            .expect("created identity");

        manager
            .verify_replacement_bucket_pass(&storage)
            .await
            .expect("a bucket created mid-pass does not fail verification");
        assert_eq!(
            manager
                .prepare_replacement_bucket_pass(&storage)
                .await
                .expect("additions extend the pass"),
            [recorded, created],
            "additions are appended so the checkpoint's bucket index keeps its meaning"
        );
        let state = manager.get_state().await;
        assert_eq!(
            state
                .replacement_bucket_incarnations
                .as_ref()
                .and_then(|identities| identities.get(created))
                .copied(),
            Some(created_identity)
        );
        assert!(state.pending_buckets.contains(&created.to_string()));
        assert_eq!(state.resume_cursor.as_deref(), Some("mid-pass-cursor"));
        assert!(!state.replacement_bucket_pass_reset_pending);
        assert_eq!(state.replacement_execution_protocol, 2);
        let checkpoint = CheckpointManager::load_from_disk(disk.clone(), &task_id)
            .await
            .expect("checkpoint after the addition")
            .get_checkpoint()
            .await;
        assert_eq!(
            (checkpoint.current_bucket_index, checkpoint.current_object_index),
            (0, 7),
            "the checkpoint must not rewind"
        );

        // A restart reads the persisted order instead of rebuilding it sorted.
        let restarted = ResumeManager::load_replacement_intent(disk.clone(), &task_id)
            .await
            .expect("restart the pass");
        assert_eq!(
            restarted
                .prepare_replacement_bucket_pass(&storage)
                .await
                .expect("an unchanged namespace keeps the pass"),
            [recorded, created]
        );
        restarted
            .verify_replacement_bucket_pass(&storage)
            .await
            .expect("the extended pass matches the namespace");
        assert_eq!(restarted.get_state().await.resume_cursor.as_deref(), Some("mid-pass-cursor"));

        // Deleting the created bucket is still a namespace change for the pass.
        store
            .delete_bucket(created, &DeleteBucketOptions::default())
            .await
            .expect("delete the created bucket");
        assert!(matches!(
            restarted.verify_replacement_bucket_pass(&storage).await,
            Err(Error::ReplacementNamespaceChanged)
        ));
    }
}
