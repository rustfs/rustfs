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

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::bucket::lifecycle::tier_sweeper::{Jentry, delete_object_from_remote_tier_idempotent};
use crate::config::com::{delete_config, read_config, save_config};
use crate::disk::RUSTFS_META_BUCKET;
use crate::error::{Error, Result};
use crate::store::ECStore;
use crate::store_api::{ListOperations, ObjectIO, ObjectOperations};

const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_LIFECYCLE: &str = "lifecycle";
const EVENT_LIFECYCLE_TIER_DELETE_JOURNAL: &str = "lifecycle_tier_delete_journal";

pub const DEFAULT_TIER_DELETE_JOURNAL_RECOVERY_LIMIT: usize = 1_000;
const TIER_DELETE_JOURNAL_VERSION: u8 = 1;
const TIER_DELETE_JOURNAL_PREFIX: &str = "ilm/tier-delete-journal/";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct PersistedTierDeleteJournalEntry {
    version: u8,
    obj_name: String,
    version_id: String,
    tier_name: String,
}

impl PersistedTierDeleteJournalEntry {
    fn from_jentry(je: &Jentry) -> Self {
        Self {
            version: TIER_DELETE_JOURNAL_VERSION,
            obj_name: je.obj_name.clone(),
            version_id: je.version_id.clone(),
            tier_name: je.tier_name.clone(),
        }
    }

    fn into_jentry(self) -> Result<Jentry> {
        if self.version != TIER_DELETE_JOURNAL_VERSION {
            return Err(Error::other(format!("unsupported tier delete journal version {}", self.version)));
        }
        if self.obj_name.is_empty() || self.version_id.is_empty() || self.tier_name.is_empty() {
            return Err(Error::other("tier delete journal entry is incomplete"));
        }
        Ok(Jentry {
            obj_name: self.obj_name,
            version_id: self.version_id,
            tier_name: self.tier_name,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierDeleteJournalRecoveryStats {
    pub scanned: usize,
    pub deleted: usize,
    pub failed: usize,
    pub next_marker: Option<String>,
    pub truncated: bool,
}

pub(crate) fn tier_delete_journal_object_name(je: &Jentry) -> String {
    let mut hasher = Sha256::new();
    hasher.update(je.tier_name.as_bytes());
    hasher.update([0]);
    hasher.update(je.obj_name.as_bytes());
    hasher.update([0]);
    hasher.update(je.version_id.as_bytes());
    format!(
        "{TIER_DELETE_JOURNAL_PREFIX}{}.json",
        rustfs_utils::crypto::hex(hasher.finalize().as_slice())
    )
}

fn decode_tier_delete_journal_entry(data: &[u8]) -> Result<Jentry> {
    let persisted: PersistedTierDeleteJournalEntry =
        serde_json::from_slice(data).map_err(|err| Error::other(format!("decode tier delete journal failed: {err}")))?;
    persisted.into_jentry()
}

fn encode_tier_delete_journal_entry(je: &Jentry) -> Result<Vec<u8>> {
    serde_json::to_vec(&PersistedTierDeleteJournalEntry::from_jentry(je))
        .map_err(|err| Error::other(format!("encode tier delete journal failed: {err}")))
}

pub async fn persist_tier_delete_journal_entry<S>(api: Arc<S>, je: &Jentry) -> std::io::Result<()>
where
    S: ObjectIO,
{
    let data = encode_tier_delete_journal_entry(je).map_err(std::io::Error::other)?;
    save_config(api, &tier_delete_journal_object_name(je), data)
        .await
        .map_err(std::io::Error::other)
}

pub async fn remove_tier_delete_journal_entry<S>(api: Arc<S>, je: &Jentry) -> std::io::Result<()>
where
    S: ObjectOperations,
{
    match delete_config(api, &tier_delete_journal_object_name(je)).await {
        Ok(()) | Err(Error::ConfigNotFound) => Ok(()),
        Err(err) => Err(std::io::Error::other(err)),
    }
}

pub async fn process_tier_delete_journal_entry(api: Arc<ECStore>, je: &Jentry) -> std::io::Result<()> {
    delete_object_from_remote_tier_idempotent(&je.obj_name, &je.version_id, &je.tier_name).await?;
    remove_tier_delete_journal_entry(api, je).await
}

pub async fn recover_tier_delete_journal_entries(
    api: Arc<ECStore>,
    limit: usize,
    marker: Option<String>,
) -> Result<TierDeleteJournalRecoveryStats> {
    if limit == 0 {
        return Err(Error::other("tier delete journal recovery limit must be greater than zero"));
    }

    let list = api
        .clone()
        .list_objects_v2(
            RUSTFS_META_BUCKET,
            TIER_DELETE_JOURNAL_PREFIX,
            marker.clone(),
            None,
            i32::try_from(limit).unwrap_or(i32::MAX),
            false,
            None,
            false,
        )
        .await?;

    let mut stats = TierDeleteJournalRecoveryStats {
        scanned: 0,
        deleted: 0,
        failed: 0,
        next_marker: list.next_continuation_token,
        truncated: list.is_truncated,
    };

    for object in list.objects {
        stats.scanned += 1;
        let data = match read_config(api.clone(), &object.name).await {
            Ok(data) => data,
            Err(Error::ConfigNotFound) => continue,
            Err(err) => {
                stats.failed += 1;
                warn!(
                    event = EVENT_LIFECYCLE_TIER_DELETE_JOURNAL,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    journal_object = %object.name,
                    error = ?err,
                    "Failed to read tier delete journal entry"
                );
                continue;
            }
        };

        let je = match decode_tier_delete_journal_entry(&data) {
            Ok(je) => je,
            Err(err) => {
                stats.failed += 1;
                warn!(
                    event = EVENT_LIFECYCLE_TIER_DELETE_JOURNAL,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    journal_object = %object.name,
                    error = ?err,
                    "Failed to decode tier delete journal entry"
                );
                continue;
            }
        };

        match process_tier_delete_journal_entry(api.clone(), &je).await {
            Ok(()) => stats.deleted += 1,
            Err(err) => {
                stats.failed += 1;
                debug!(
                    event = EVENT_LIFECYCLE_TIER_DELETE_JOURNAL,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    journal_object = %object.name,
                    remote_object = %je.obj_name,
                    remote_version_id = %je.version_id,
                    tier = %je.tier_name,
                    error = ?err,
                    "Tier delete journal recovery will retry later"
                );
            }
        }
    }

    Ok(stats)
}

pub async fn run_tier_delete_journal_recovery_loop(api: Arc<ECStore>, cancel_token: CancellationToken) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
    let mut marker: Option<String> = None;

    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => return,
            _ = interval.tick() => {}
        }

        match recover_tier_delete_journal_entries(api.clone(), DEFAULT_TIER_DELETE_JOURNAL_RECOVERY_LIMIT, marker.clone()).await {
            Ok(stats) => {
                marker = stats.next_marker;
                debug!(
                    event = EVENT_LIFECYCLE_TIER_DELETE_JOURNAL,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    scanned = stats.scanned,
                    deleted = stats.deleted,
                    failed = stats.failed,
                    truncated = stats.truncated,
                    next_marker = ?marker,
                    "Recovered tier delete journal tasks"
                );
            }
            Err(err) => {
                warn!(
                    event = EVENT_LIFECYCLE_TIER_DELETE_JOURNAL,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    next_marker = ?marker,
                    error = ?err,
                    "Failed to recover tier delete journal tasks"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{decode_tier_delete_journal_entry, encode_tier_delete_journal_entry, tier_delete_journal_object_name};
    use crate::bucket::lifecycle::tier_sweeper::Jentry;

    fn journal_entry() -> Jentry {
        Jentry {
            obj_name: "remote/object".to_string(),
            version_id: "remote-version".to_string(),
            tier_name: "WARM".to_string(),
        }
    }

    #[test]
    fn tier_delete_journal_roundtrips_entry() {
        let je = journal_entry();

        let encoded = encode_tier_delete_journal_entry(&je).expect("journal entry should encode");
        let decoded = decode_tier_delete_journal_entry(&encoded).expect("journal entry should decode");

        assert_eq!(decoded.obj_name, je.obj_name);
        assert_eq!(decoded.version_id, je.version_id);
        assert_eq!(decoded.tier_name, je.tier_name);
    }

    #[test]
    fn tier_delete_journal_path_is_stable_and_sanitized() {
        let je = journal_entry();

        let first = tier_delete_journal_object_name(&je);
        let second = tier_delete_journal_object_name(&je);

        assert_eq!(first, second);
        assert!(first.starts_with("ilm/tier-delete-journal/"));
        assert!(first.ends_with(".json"));
        assert!(!first.contains("remote/object"));
    }

    #[test]
    fn tier_delete_journal_rejects_incomplete_entry() {
        let payload = br#"{"version":1,"obj_name":"","version_id":"v1","tier_name":"WARM"}"#;

        let err = decode_tier_delete_journal_entry(payload).expect_err("incomplete journal entry should be rejected");

        assert!(err.to_string().contains("incomplete"));
    }
}
