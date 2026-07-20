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

use std::{future::Future, sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::bucket::lifecycle::config_boundary;
use crate::bucket::lifecycle::tier_sweeper::{Jentry, delete_object_from_remote_tier_idempotent_with_manager_and_identity};
use crate::disk::RUSTFS_META_BUCKET;
use crate::error::{Error, Result};
use crate::object_api::{GetObjectReader, ObjectInfo, ObjectOptions, PutObjReader};
use crate::services::tier::tier::tier_destination_id_from_metadata;
use crate::storage_api_contracts::{
    list::ListOperations as _,
    object::{DeletedObject, ObjectIO, ObjectOperations, ObjectToDelete},
    range::HTTPRangeSpec,
};
use crate::store::ECStore;
use rustfs_filemeta::FileInfo;

const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_LIFECYCLE: &str = "lifecycle";
const EVENT_LIFECYCLE_TIER_DELETE_JOURNAL: &str = "lifecycle_tier_delete_journal";

pub const DEFAULT_TIER_DELETE_JOURNAL_RECOVERY_LIMIT: usize = 1_000;
const TIER_DELETE_JOURNAL_RECOVERY_INTERVAL: Duration = Duration::from_secs(60);
const TIER_DELETE_JOURNAL_RECOVERY_TIMEOUT: Duration = Duration::from_secs(300);
const TIER_DELETE_JOURNAL_VERSION: u8 = 2;
const TIER_DELETE_JOURNAL_EXACT_VERSION: u8 = 3;
pub(crate) const TIER_DELETE_JOURNAL_PREFIX: &str = "ilm/tier-delete-journal/";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct PersistedTierDeleteJournalEntry {
    version: u8,
    obj_name: String,
    version_id: String,
    tier_name: String,
    #[serde(default)]
    backend_identity: Option<[u8; 32]>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    version_id_exact: Option<bool>,
}

impl PersistedTierDeleteJournalEntry {
    fn from_jentry(je: &Jentry) -> Self {
        Self {
            version: if je.version_id_exact {
                TIER_DELETE_JOURNAL_EXACT_VERSION
            } else if je.backend_identity.is_some() {
                TIER_DELETE_JOURNAL_VERSION
            } else {
                1
            },
            obj_name: je.obj_name.clone(),
            version_id: je.version_id.clone(),
            tier_name: je.tier_name.clone(),
            backend_identity: je.backend_identity,
            version_id_exact: je.version_id_exact.then_some(true),
        }
    }

    fn into_jentry(self) -> Result<Jentry> {
        // Empty `version_id` is a legal sentinel for objects transitioned to an
        // unversioned remote tier (see CLAUDE.md: a tier version of `None`/`""`
        // means the tier bucket is unversioned, so the remote delete is issued
        // without a versionId). Only reject entries missing the object or tier
        // name, which are always populated for a TRANSITION_COMPLETE object.
        if self.obj_name.is_empty() || self.tier_name.is_empty() {
            return Err(Error::other("tier delete journal entry is incomplete"));
        }
        if self.version != TIER_DELETE_JOURNAL_EXACT_VERSION && self.version_id_exact.unwrap_or(false) {
            return Err(Error::other(
                "legacy tier delete journal entry has an unsupported exact version constraint",
            ));
        }
        let (backend_identity, version_id_exact) = match self.version {
            1 => (None, false),
            TIER_DELETE_JOURNAL_VERSION => (
                Some(
                    self.backend_identity
                        .ok_or_else(|| Error::other("tier delete journal v2 entry is missing its backend identity"))?,
                ),
                false,
            ),
            TIER_DELETE_JOURNAL_EXACT_VERSION => {
                if self.version_id.is_empty() || self.version_id_exact != Some(true) {
                    return Err(Error::other("tier delete journal v3 entry is missing its exact version constraint"));
                }
                (
                    Some(
                        self.backend_identity
                            .ok_or_else(|| Error::other("tier delete journal v3 entry is missing its backend identity"))?,
                    ),
                    true,
                )
            }
            version => return Err(Error::other(format!("unsupported tier delete journal version {version}"))),
        };
        Ok(Jentry {
            obj_name: self.obj_name,
            version_id: self.version_id,
            tier_name: self.tier_name,
            backend_identity,
            version_id_exact,
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
    if let Some(backend_identity) = je.backend_identity {
        hasher.update([0]);
        hasher.update(backend_identity);
    }
    if je.version_id_exact {
        hasher.update([0]);
        hasher.update(b"exact-version-id");
    }
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

pub fn record_tier_delete_journal_backend_identity(
    je: &mut Jentry,
    metadata: &std::collections::HashMap<String, String>,
) -> std::io::Result<()> {
    if let Some(identity) = tier_destination_id_from_metadata(metadata)? {
        je.backend_identity = Some(identity);
    }
    Ok(())
}

pub async fn persist_tier_delete_journal_entry<S>(api: Arc<S>, je: &Jentry) -> std::io::Result<()>
where
    S: ObjectIO<
            Error = Error,
            RangeSpec = HTTPRangeSpec,
            HeaderMap = http::HeaderMap,
            ObjectOptions = ObjectOptions,
            ObjectInfo = ObjectInfo,
            GetObjectReader = GetObjectReader,
            PutObjectReader = PutObjReader,
        >,
{
    let data = encode_tier_delete_journal_entry(je).map_err(std::io::Error::other)?;
    config_boundary::save_config(api, &tier_delete_journal_object_name(je), data)
        .await
        .map_err(std::io::Error::other)
}

pub async fn remove_tier_delete_journal_entry<S>(api: Arc<S>, je: &Jentry) -> std::io::Result<()>
where
    S: ObjectOperations<
            Error = Error,
            ObjectInfo = ObjectInfo,
            ObjectOptions = ObjectOptions,
            FileInfo = FileInfo,
            ObjectToDelete = ObjectToDelete,
            DeletedObject = DeletedObject,
        >,
{
    match config_boundary::delete_config(api, &tier_delete_journal_object_name(je)).await {
        Ok(()) | Err(Error::ConfigNotFound) => Ok(()),
        Err(err) => Err(std::io::Error::other(err)),
    }
}

pub async fn process_tier_delete_journal_entry(api: Arc<ECStore>, je: &Jentry) -> std::io::Result<()> {
    let backend_identity = je
        .backend_identity
        .ok_or_else(|| std::io::Error::other("legacy tier delete journal has no durable backend identity"))?;
    delete_object_from_remote_tier_idempotent_with_manager_and_identity(
        &je.obj_name,
        &je.version_id,
        &je.tier_name,
        backend_identity,
        &api.tier_config_mgr(),
        je.version_id_exact,
    )
    .await?;
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
        let data = match config_boundary::read_config(api.clone(), &object.name).await {
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

        if je.backend_identity.is_none() {
            stats.failed += 1;
            warn!(
                event = EVENT_LIFECYCLE_TIER_DELETE_JOURNAL,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                journal_object = %object.name,
                remote_object = %je.obj_name,
                remote_version_id = %je.version_id,
                tier = %je.tier_name,
                "Legacy tier delete journal entry has no durable backend identity and will be retained"
            );
            continue;
        }

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
    let mut interval = tokio::time::interval(TIER_DELETE_JOURNAL_RECOVERY_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut marker: Option<String> = None;

    loop {
        #[cfg(test)]
        tokio::select! {
            _ = cancel_token.cancelled() => return,
            _ = interval.tick() => {},
            _ = api.ctx.wait_for_tier_delete_journal_recovery() => {},
        }
        #[cfg(not(test))]
        tokio::select! {
            _ = cancel_token.cancelled() => return,
            _ = interval.tick() => {},
        }

        let recovery =
            recover_tier_delete_journal_entries(api.clone(), DEFAULT_TIER_DELETE_JOURNAL_RECOVERY_LIMIT, marker.clone());
        let Some(result) =
            await_tier_delete_journal_recovery(&cancel_token, TIER_DELETE_JOURNAL_RECOVERY_TIMEOUT, recovery).await
        else {
            return;
        };
        match result {
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

async fn await_tier_delete_journal_recovery<T, F>(
    cancel_token: &CancellationToken,
    timeout: Duration,
    recovery: F,
) -> Option<Result<T>>
where
    F: Future<Output = Result<T>>,
{
    tokio::select! {
        _ = cancel_token.cancelled() => None,
        result = tokio::time::timeout(timeout, recovery) => Some(match result {
            Ok(result) => result,
            Err(_) => Err(Error::other(format!(
                "tier delete journal recovery timed out after {} seconds",
                timeout.as_secs()
            ))),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        TIER_DELETE_JOURNAL_EXACT_VERSION, await_tier_delete_journal_recovery, decode_tier_delete_journal_entry,
        encode_tier_delete_journal_entry, record_tier_delete_journal_backend_identity, tier_delete_journal_object_name,
    };
    use crate::bucket::lifecycle::tier_sweeper::Jentry;
    use crate::error::Result;
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    fn journal_entry() -> Jentry {
        Jentry {
            obj_name: "remote/object".to_string(),
            version_id: "remote-version".to_string(),
            tier_name: "WARM".to_string(),
            backend_identity: Some([7; 32]),
            version_id_exact: false,
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
        assert_eq!(decoded.backend_identity, je.backend_identity);
        assert_eq!(decoded.version_id_exact, je.version_id_exact);
    }

    #[test]
    fn tier_delete_journal_roundtrips_exact_put_response_constraint() {
        let mut exact = journal_entry();
        exact.version_id = uuid::Uuid::nil().to_string();
        exact.version_id_exact = true;
        let mut normalized = exact.clone();
        normalized.version_id_exact = false;

        let encoded = encode_tier_delete_journal_entry(&exact).expect("exact journal entry should encode");
        let persisted: serde_json::Value = serde_json::from_slice(&encoded).expect("exact journal JSON should decode");
        let decoded = decode_tier_delete_journal_entry(&encoded).expect("exact journal entry should decode");

        assert_eq!(persisted["version"], TIER_DELETE_JOURNAL_EXACT_VERSION);
        assert_eq!(persisted["version_id_exact"], true);
        assert!(decoded.version_id_exact);
        assert_ne!(tier_delete_journal_object_name(&exact), tier_delete_journal_object_name(&normalized));
    }

    #[test]
    fn tier_delete_journal_rejects_invalid_exact_version_constraints() {
        let identity = vec![7_u8; 32];
        let invalid = [
            serde_json::json!({
                "version": 1,
                "obj_name": "remote/object",
                "version_id": "exact-version",
                "tier_name": "WARM",
                "version_id_exact": true,
            }),
            serde_json::json!({
                "version": 2,
                "obj_name": "remote/object",
                "version_id": "exact-version",
                "tier_name": "WARM",
                "backend_identity": identity,
                "version_id_exact": true,
            }),
            serde_json::json!({
                "version": TIER_DELETE_JOURNAL_EXACT_VERSION,
                "obj_name": "remote/object",
                "version_id": "",
                "tier_name": "WARM",
                "backend_identity": identity,
                "version_id_exact": true,
            }),
            serde_json::json!({
                "version": TIER_DELETE_JOURNAL_EXACT_VERSION,
                "obj_name": "remote/object",
                "version_id": "exact-version",
                "tier_name": "WARM",
                "backend_identity": identity,
            }),
            serde_json::json!({
                "version": TIER_DELETE_JOURNAL_EXACT_VERSION,
                "obj_name": "remote/object",
                "version_id": "exact-version",
                "tier_name": "WARM",
                "backend_identity": identity,
                "version_id_exact": false,
            }),
            serde_json::json!({
                "version": TIER_DELETE_JOURNAL_EXACT_VERSION,
                "obj_name": "remote/object",
                "version_id": "exact-version",
                "tier_name": "WARM",
                "version_id_exact": true,
            }),
        ];

        for persisted in invalid {
            let encoded = serde_json::to_vec(&persisted).expect("invalid journal fixture should encode");
            decode_tier_delete_journal_entry(&encoded).expect_err("invalid exact journal constraint must fail closed");
        }
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
    fn tier_delete_journal_paths_separate_legacy_and_backend_identities() {
        let mut legacy = journal_entry();
        legacy.backend_identity = None;
        let mut backend_a = journal_entry();
        backend_a.backend_identity = Some([1; 32]);
        let mut backend_b = journal_entry();
        backend_b.backend_identity = Some([2; 32]);

        assert_eq!(
            tier_delete_journal_object_name(&legacy),
            "ilm/tier-delete-journal/5ba6a7eb6338412b771613a6845a42ae5b8e26b5d201323eb01b38c5b42ff300.json"
        );
        assert_ne!(tier_delete_journal_object_name(&legacy), tier_delete_journal_object_name(&backend_a));
        assert_ne!(tier_delete_journal_object_name(&backend_a), tier_delete_journal_object_name(&backend_b));
    }

    #[test]
    fn tier_delete_journal_v2_requires_backend_identity() {
        let payload = br#"{"version":2,"obj_name":"remote/object","version_id":"v1","tier_name":"WARM"}"#;

        let err = decode_tier_delete_journal_entry(payload).expect_err("v2 entry without identity must fail closed");

        assert!(err.to_string().contains("backend identity"));
    }

    #[test]
    fn tier_delete_journal_uses_persisted_transition_destination_identity() {
        let mut je = journal_entry();
        je.backend_identity = None;
        let identity = [9_u8; 32];
        let mut metadata = std::collections::HashMap::new();
        rustfs_utils::http::metadata_compat::insert_str(
            &mut metadata,
            rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
            rustfs_utils::crypto::hex(identity),
        );

        record_tier_delete_journal_backend_identity(&mut je, &metadata).expect("persisted transition identity should decode");
        let encoded = encode_tier_delete_journal_entry(&je).expect("identity-bound journal should encode");
        let decoded = decode_tier_delete_journal_entry(&encoded).expect("identity-bound journal should decode");

        assert_eq!(decoded.backend_identity, Some(identity));
    }

    #[test]
    fn tier_delete_journal_without_transition_identity_stays_legacy() {
        let mut je = journal_entry();
        je.backend_identity = None;

        let encoded = encode_tier_delete_journal_entry(&je).expect("legacy journal should remain encodable");
        let persisted: serde_json::Value = serde_json::from_slice(&encoded).expect("journal JSON should decode");

        assert_eq!(persisted["version"], 1);
        assert!(persisted["backend_identity"].is_null());
    }

    #[test]
    fn tier_delete_journal_rejects_incomplete_entry() {
        let payload = br#"{"version":1,"obj_name":"","version_id":"v1","tier_name":"WARM"}"#;

        let err = decode_tier_delete_journal_entry(payload).expect_err("incomplete journal entry should be rejected");

        assert!(err.to_string().contains("incomplete"));
    }

    #[test]
    fn tier_delete_journal_recovers_unversioned_tier_entry() {
        // A remote tier that is unversioned records an empty `version_id`. Such a
        // WAL entry must decode successfully so recovery can drive a versionless
        // remote delete, otherwise the remote object is orphaned and the journal
        // file leaks forever.
        let payload = br#"{"version":1,"obj_name":"remote/object","version_id":"","tier_name":"WARM"}"#;

        let decoded = decode_tier_delete_journal_entry(payload).expect("unversioned tier entry should decode");

        assert_eq!(decoded.obj_name, "remote/object");
        assert!(decoded.version_id.is_empty());
        assert_eq!(decoded.tier_name, "WARM");
        assert_eq!(decoded.backend_identity, None);
    }

    #[test]
    fn tier_delete_journal_rejects_missing_tier_name() {
        let payload = br#"{"version":1,"obj_name":"remote/object","version_id":"v1","tier_name":""}"#;

        let err = decode_tier_delete_journal_entry(payload).expect_err("entry missing tier name should be rejected");

        assert!(err.to_string().contains("incomplete"));
    }

    #[test]
    fn tier_delete_journal_rejects_truncated_payload() {
        // A partially written journal file fails at JSON deserialization, so
        // relaxing the empty-version_id check does not admit truncated records.
        let payload = br#"{"version":1,"obj_name":"remote/object","version_id":""#;

        let err = decode_tier_delete_journal_entry(payload).expect_err("truncated journal payload should be rejected");

        assert!(err.to_string().contains("decode tier delete journal failed"));
    }

    #[tokio::test]
    async fn tier_delete_journal_recovery_has_a_hard_outer_timeout() {
        let result = await_tier_delete_journal_recovery(
            &CancellationToken::new(),
            Duration::from_millis(10),
            std::future::pending::<Result<()>>(),
        )
        .await
        .expect("an elapsed timeout should return a recovery error")
        .expect_err("a permanently pending recovery must time out");

        assert!(result.to_string().contains("recovery timed out"), "{result}");
    }

    #[tokio::test]
    async fn tier_delete_journal_recovery_drops_in_flight_work_on_shutdown() {
        let cancel = CancellationToken::new();
        cancel.cancel();

        let result =
            await_tier_delete_journal_recovery(&cancel, Duration::from_secs(30), std::future::pending::<Result<()>>()).await;

        assert!(result.is_none(), "shutdown must cancel the in-flight recovery future");
    }
}
