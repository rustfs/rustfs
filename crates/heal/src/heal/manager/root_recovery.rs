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

//! Graceful-shutdown handoff for administrator root heals. This namespace is
//! separate from erasure-set checkpoints and replacement generations, which
//! cannot represent a cluster traversal. One coordinator disk owns each
//! record; never create a fallback copy after an uncertain write or deletion.

use super::*;
use crate::heal::storage_api::owner::{EcstoreConditionalFileUpdate, EcstoreDiskAPI, EcstoreDiskBytes};
use crate::heal::{DiskStore, RUSTFS_META_BUCKET};
use serde::{Deserialize, Serialize};

// The metadata bucket already exists and its parent is durable. Creating a
// nested journal directory here would also require syncing every ancestor.
const ROOT_RECOVERY_PREFIX: &str = "root-heal-";
const ROOT_RECOVERY_SCHEMA: u32 = 1;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RootHealIntent {
    schema: u32,
    task_id: String,
    #[serde(deserialize_with = "decode_options")]
    options: HealOptions,
    priority: HealPriority,
    retry_attempts: u32,
    created_at: SystemTime,
}

impl RootHealIntent {
    fn from_request(request: &HealRequest) -> Self {
        Self {
            schema: ROOT_RECOVERY_SCHEMA,
            task_id: request.id.clone(),
            options: request.options.clone(),
            priority: request.priority,
            retry_attempts: request.retry_attempts,
            created_at: request.created_at,
        }
    }

    fn into_request(self) -> HealRequest {
        let mut request = HealRequest::new(HealType::Cluster, self.options, self.priority);
        request.id = self.task_id;
        request.source = HealRequestSource::Admin;
        request.retry_attempts = self.retry_attempts;
        request.created_at = self.created_at;
        request
    }
}

#[derive(Default)]
pub(super) struct RootHealRecovery {
    mutation: Mutex<()>,
    #[cfg(test)]
    disks: Option<Vec<DiskStore>>,
}

pub(super) fn is_root_heal(heal_type: &HealType, source: HealRequestSource) -> bool {
    source == HealRequestSource::Admin && matches!(heal_type, HealType::Cluster)
}

fn decode_options<'de, D: serde::Deserializer<'de>>(deserializer: D) -> std::result::Result<HealOptions, D::Error> {
    let value = serde_json::Value::deserialize(deserializer)?;
    let object = value
        .as_object()
        .ok_or_else(|| serde::de::Error::custom("root heal options must be an object"))?;
    const FIELDS: &[&str] = &[
        "scan_mode",
        "remove_corrupted",
        "recreate_missing",
        "update_parity",
        "recursive",
        "dry_run",
        "no_lock",
        "timeout",
        "pool_index",
        "set_index",
    ];
    if object.keys().any(|key| !FIELDS.contains(&key.as_str())) {
        return Err(serde::de::Error::custom("unknown root heal recovery option"));
    }
    let options: HealOptions = serde_json::from_value(value).map_err(serde::de::Error::custom)?;
    if options.no_lock {
        return Err(serde::de::Error::custom("administrator root heal cannot skip namespace locking"));
    }
    Ok(options)
}

fn intent_path(task_id: &str) -> Result<String> {
    let parsed = uuid::Uuid::parse_str(task_id).map_err(|_| Error::Other("Invalid root heal recovery task id".to_string()))?;
    if parsed.to_string() != task_id {
        return Err(Error::Other("Noncanonical root heal recovery task id".to_string()));
    }
    Ok(format!("{ROOT_RECOVERY_PREFIX}{task_id}.json"))
}

fn decode_intent(task_id: &str, bytes: &[u8]) -> Result<RootHealIntent> {
    let _ = intent_path(task_id)?;
    let intent: RootHealIntent = serde_json::from_slice(bytes)
        .map_err(|error| Error::Other(format!("Invalid root heal recovery record {task_id}: {error}")))?;
    if intent.schema != ROOT_RECOVERY_SCHEMA || intent.task_id != task_id {
        return Err(Error::Other(format!("Unsupported or mismatched root heal recovery record {task_id}")));
    }
    Ok(intent)
}

impl RootHealRecovery {
    #[cfg(test)]
    pub(super) fn with_disks(disks: Vec<DiskStore>) -> Self {
        Self {
            mutation: Mutex::new(()),
            disks: Some(disks),
        }
    }

    async fn disks(&self) -> Result<Vec<DiskStore>> {
        #[cfg(test)]
        if let Some(disks) = &self.disks {
            return Ok(disks.clone());
        }
        let map = local_disk_map_read().await;
        if map.values().any(Option::is_none) {
            return Err(Error::Other("Root heal recovery owner may be on an unavailable local disk".to_string()));
        }
        let mut disks = map.values().flatten().cloned().collect::<Vec<_>>();
        disks.sort_by_key(|disk| EcstoreDiskAPI::endpoint(disk.as_ref()).to_string());
        Ok(disks)
    }

    async fn find(disks: &[DiskStore], task_id: &str) -> Result<Option<(DiskStore, EcstoreDiskBytes)>> {
        let path = intent_path(task_id)?;
        let mut found = None;
        for disk in disks {
            // read_all reports FileNotFound even when the whole metadata
            // volume is absent; that is an unknown owner, not empty state.
            EcstoreDiskAPI::stat_volume(disk.as_ref(), RUSTFS_META_BUCKET).await?;
            match EcstoreDiskAPI::read_all(disk.as_ref(), RUSTFS_META_BUCKET, &path).await {
                Ok(bytes) => {
                    decode_intent(task_id, &bytes)?;
                    if found.is_some() {
                        return Err(Error::Other(format!("Multiple root heal recovery owners for {task_id}")));
                    }
                    found = Some((disk.clone(), bytes));
                }
                Err(DiskError::FileNotFound) => {}
                Err(error) => return Err(Error::Disk(error)),
            }
        }
        Ok(found)
    }

    pub(super) async fn persist(&self, request: &HealRequest) -> Result<()> {
        if !is_root_heal(&request.heal_type, request.source) {
            return Ok(());
        }
        let _guard = self.mutation.lock().await;
        let disks = self.disks().await?;
        let existing = Self::find(&disks, &request.id).await?;
        let (disk, expected) = match existing {
            Some((disk, bytes)) => (disk, Some(bytes)),
            None => {
                let disk = disks
                    .first()
                    .cloned()
                    .ok_or_else(|| Error::Other("No local disk available for root heal shutdown recovery".to_string()))?;
                (disk, None)
            }
        };
        if request.options.no_lock {
            return Err(Error::Other("Administrator root heal cannot skip namespace locking".to_string()));
        }
        let bytes = serde_json::to_vec(&RootHealIntent::from_request(request))
            .map_err(|error| Error::Other(format!("Serialize root heal recovery record: {error}")))?;
        match EcstoreDiskAPI::compare_and_update_file(
            disk.as_ref(),
            RUSTFS_META_BUCKET,
            &intent_path(&request.id)?,
            expected,
            Some(bytes.into()),
        )
        .await?
        {
            EcstoreConditionalFileUpdate::Updated => Ok(()),
            _ => Err(Error::Other(format!("Root heal recovery record changed for {}", request.id))),
        }
    }

    pub(super) async fn remove(&self, task_id: &str, heal_type: &HealType, source: HealRequestSource) -> Result<bool> {
        if !is_root_heal(heal_type, source) {
            return Ok(false);
        }
        let _guard = self.mutation.lock().await;
        let Some((disk, bytes)) = Self::find(&self.disks().await?, task_id).await? else {
            return Ok(false);
        };
        match EcstoreDiskAPI::compare_and_update_file(
            disk.as_ref(),
            RUSTFS_META_BUCKET,
            &intent_path(task_id)?,
            Some(bytes),
            None,
        )
        .await?
        {
            EcstoreConditionalFileUpdate::Updated => Ok(true),
            _ => Err(Error::Other(format!("Root heal recovery record changed while retiring {task_id}"))),
        }
    }

    pub(super) async fn checkpoint_failed_execution(&self, task: &HealTask) -> Result<()> {
        if !is_root_heal(&task.heal_type, task.source) {
            return Ok(());
        }
        let remaining = match task.retry_request_with_remaining_timeout().await {
            Ok(request) => request.options.timeout,
            Err(Error::TaskTimeout) => Some(Duration::ZERO),
            Err(error) => return Err(error),
        };
        let _guard = self.mutation.lock().await;
        let Some((disk, expected)) = Self::find(&self.disks().await?, &task.id).await? else {
            // A first execution that failed has no restart handoff to update.
            return Ok(());
        };
        let mut intent = decode_intent(&task.id, &expected)?;
        let mut expected_options = intent.options.clone();
        expected_options.timeout = task.options.timeout;
        if intent.created_at != task.created_at || intent.priority != task.priority || expected_options != task.options {
            return Err(Error::Other(format!("Root heal recovery owner changed for {}", task.id)));
        }
        // A terminal timeout leaves no runtime owner for stop() to snapshot.
        // Checkpoint its consumed budget before publishing terminal status;
        // never refund time if an earlier checkpoint is already stricter.
        intent.options.timeout = match (intent.options.timeout, remaining) {
            (Some(previous), Some(remaining)) => Some(previous.min(remaining)),
            (previous, remaining) => previous.or(remaining),
        };
        intent.retry_attempts = intent.retry_attempts.max(task.retry_attempts);
        let bytes = serde_json::to_vec(&intent)
            .map_err(|error| Error::Other(format!("Serialize root heal recovery checkpoint: {error}")))?;
        match EcstoreDiskAPI::compare_and_update_file(
            disk.as_ref(),
            RUSTFS_META_BUCKET,
            &intent_path(&task.id)?,
            Some(expected),
            Some(bytes.into()),
        )
        .await?
        {
            EcstoreConditionalFileUpdate::Updated => Ok(()),
            _ => Err(Error::Other(format!("Root heal recovery record changed while checkpointing {}", task.id))),
        }
    }

    pub(super) async fn cancel_pending(&self, task_id: &str) -> Result<bool> {
        if intent_path(task_id).is_err() {
            return Ok(false);
        }
        self.remove(task_id, &HealType::Cluster, HealRequestSource::Admin).await
    }

    pub(super) async fn pending(&self) -> Result<Vec<HealRequest>> {
        let _guard = self.mutation.lock().await;
        let disks = self.disks().await?;
        let mut ids = HashSet::new();
        for disk in &disks {
            EcstoreDiskAPI::stat_volume(disk.as_ref(), RUSTFS_META_BUCKET).await?;
            let entries = match EcstoreDiskAPI::list_dir(disk.as_ref(), "", RUSTFS_META_BUCKET, "", -1).await {
                Ok(entries) => entries,
                Err(DiskError::FileNotFound) => continue,
                Err(error) => return Err(Error::Disk(error)),
            };
            for entry in entries {
                let Some(task_id) = entry
                    .strip_prefix(ROOT_RECOVERY_PREFIX)
                    .and_then(|entry| entry.strip_suffix(".json"))
                else {
                    continue;
                };
                let _ = intent_path(task_id)?;
                ids.insert(task_id.to_string());
            }
        }
        let mut requests = Vec::new();
        for task_id in ids {
            if let Some((_, bytes)) = Self::find(&disks, &task_id).await? {
                requests.push(decode_intent(&task_id, &bytes)?.into_request());
            }
        }
        requests.sort_by(|left, right| left.created_at.cmp(&right.created_at).then_with(|| left.id.cmp(&right.id)));
        Ok(requests)
    }
}

impl HealManager {
    pub(super) async fn replay_root_heals(&self) -> Result<()> {
        // Decode every record before admitting anything. These are already
        // accepted responsibilities, so restore distinct IDs even when their
        // paths overlap or the configured admission capacity has changed.
        let requests = self.root_recovery.pending().await?;
        let active = self.active_heals.lock().await;
        let mut queue = self.heal_queue.lock().await;
        let retrying = self.retrying_heals.lock().await;
        for mut request in requests {
            request.force_start = true;
            let existing = active
                .get(&request.id)
                .map(|task| request_matches_task(&request, task))
                .or_else(|| {
                    queue
                        .requests()
                        .find(|queued| queued.id == request.id)
                        .map(|queued| request_matches_request(&request, queued))
                })
                .or_else(|| {
                    retrying
                        .get(&request.id)
                        .map(|retrying| request_matches_request(&request, &retrying.request))
                });
            match existing {
                Some(true) => continue,
                Some(false) => return Err(Error::Other(format!("Conflicting root heal recovery task {}", request.id))),
                None => {}
            }
            queue.push(request);
        }
        publish_heal_queue_length(&queue);
        Ok(())
    }
}
