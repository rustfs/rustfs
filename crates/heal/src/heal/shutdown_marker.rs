// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! A clean, read-only handoff reuses the existing shutdown marker. Receipt-aware
//! readers must consume it before legacy rollback; legacy startup alone cannot
//! guarantee that checkpoint continuity expires before a write.

use super::{DiskError, DiskStore, EcstoreDiskAPI, RUSTFS_META_BUCKET, UNCLEAN_SHUTDOWN_MARKER_PATH, local_disk_map_read};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use uuid::Uuid;

static PREVIOUS_SHUTDOWN: LazyLock<Mutex<HashMap<String, bool>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadOnlyHandoff {
    schema: u32,
    owner: String,
    disk_id: Uuid,
    inventory: [u8; 32],
    checkpoint_instance_id: String,
}

fn valid_instance(instance: &str) -> bool {
    instance.len() == 32
        && instance
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        && instance.bytes().any(|byte| byte != b'0')
}

fn decode(bytes: &[u8], owner: &str, disk_id: Option<Uuid>, inventory: Option<[u8; 32]>) -> Option<ReadOnlyHandoff> {
    if bytes.len() > 2048 || owner.is_empty() || disk_id.is_none_or(|id| id.is_nil()) {
        return None;
    }
    let record: ReadOnlyHandoff = serde_json::from_slice(bytes).ok()?;
    (record.schema == 1
        && record.owner == owner
        && Some(record.disk_id) == disk_id
        && Some(record.inventory) == inventory
        && valid_instance(&record.checkpoint_instance_id))
    .then_some(record)
}

async fn local_inventory(disks: &[Option<DiskStore>]) -> Vec<(DiskStore, Option<Uuid>)> {
    let mut inventory = Vec::with_capacity(disks.len());
    for disk in disks.iter().flatten() {
        let id = disk.get_disk_id().await.ok().flatten();
        inventory.push((disk.clone(), id));
    }
    inventory
}

fn inventory_digest(disks: &[(DiskStore, Option<Uuid>)]) -> Option<[u8; 32]> {
    if disks.is_empty() {
        return None;
    }
    let mut members = disks
        .iter()
        .map(|(disk, id)| (disk.endpoint().to_string(), *id))
        .collect::<Vec<_>>();
    members.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    if members.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return None;
    }
    let mut hash = Sha256::new();
    hash.update(b"scanner-read-only-local-disks-v1");
    for (endpoint, id) in members {
        let id = id.filter(|id| !id.is_nil())?;
        hash.update(u64::try_from(endpoint.len()).ok()?.to_be_bytes());
        hash.update(endpoint.as_bytes());
        hash.update(id.as_bytes());
    }
    Some(hash.finalize().into())
}

pub(super) fn previous_shutdown_was_unclean(endpoint: &str) -> Option<bool> {
    PREVIOUS_SHUTDOWN
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(endpoint)
        .copied()
}

/// Consume every local receipt before starting mutation producers or Scanner.
/// Continuity requires matching receipts from the complete local disk inventory.
/// The fresh unclean marker must be durable before returning a reusable identity.
///
/// # Errors
/// Returns a storage error if a valid handoff cannot be consumed. Starting a
/// writer in that case could leave an older receipt authorizing stale coverage.
pub async fn consume_read_only_scanner_handoff(owner: &str, registry: &[Option<DiskStore>]) -> crate::Result<Option<String>> {
    let disks = local_inventory(registry).await;
    let inventory = if registry.iter().any(Option::is_none) {
        None
    } else {
        inventory_digest(&disks)
    };
    let mut candidate = None;
    let mut complete = inventory.is_some() && !owner.is_empty();
    let mut previous = HashMap::new();
    for (disk, disk_id) in disks {
        let (record, unclean, absent) = match disk.read_all(RUSTFS_META_BUCKET, UNCLEAN_SHUTDOWN_MARKER_PATH).await {
            Ok(bytes) => {
                let record = decode(&bytes, owner, disk_id, inventory);
                let unclean = record.is_none();
                (record, unclean, false)
            }
            Err(DiskError::FileNotFound | DiskError::VolumeNotFound) => (None, false, true),
            Err(_) => (None, true, false),
        };
        previous.insert(disk.endpoint().to_string(), unclean);
        if let Some(record) = &record {
            if candidate
                .as_ref()
                .is_some_and(|instance| instance != &record.checkpoint_instance_id)
            {
                complete = false;
            }
            candidate = Some(record.checkpoint_instance_id.clone());
        } else {
            complete = false;
        }
        // This system-volume write uses the disk's metadata durability path.
        // A crash after consumption leaves no clean receipt to reuse.
        if let Err(error) = disk
            .write_all(RUSTFS_META_BUCKET, UNCLEAN_SHUTDOWN_MARKER_PATH, bytes::Bytes::from_static(b"running"))
            .await
        {
            if !absent {
                return Err(error.into());
            }
            complete = false;
        }
    }
    *PREVIOUS_SHUTDOWN.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = previous;
    Ok(complete.then_some(candidate).flatten())
}

/// Publish a handoff only after frontends, background producers and detached
/// mutations have drained, and only if the caller observed no namespace or
/// maintenance mutation during this process. No dirty hint is cleared here.
///
/// # Errors
/// Fails closed on a missing disk identity, invalid owner, or persistence error.
/// A partially written inventory cannot authorize continuity on the next start.
pub async fn save_read_only_scanner_handoff(owner: &str, instance: &str) -> crate::Result<()> {
    if owner.is_empty() || !valid_instance(instance) {
        return Err(crate::Error::InvalidCheckpoint("invalid scanner handoff identity".to_owned()));
    }
    let registry = local_disk_map_read().await.values().cloned().collect::<Vec<_>>();
    if registry.iter().any(Option::is_none) {
        return Err(crate::Error::InvalidCheckpoint(
            "scanner handoff has an unavailable local disk".to_owned(),
        ));
    }
    let disks = local_inventory(&registry).await;
    let inventory = inventory_digest(&disks)
        .ok_or_else(|| crate::Error::InvalidCheckpoint("scanner handoff has no complete disk inventory".to_owned()))?;
    for (disk, disk_id) in disks {
        let disk_id = disk_id
            .filter(|id| !id.is_nil())
            .ok_or_else(|| crate::Error::InvalidCheckpoint("scanner handoff has no disk identity".to_owned()))?;
        let record = ReadOnlyHandoff {
            schema: 1,
            owner: owner.to_owned(),
            disk_id,
            inventory,
            checkpoint_instance_id: instance.to_owned(),
        };
        let bytes = serde_json::to_vec(&record).map_err(|error| crate::Error::Serialization(error.to_string()))?;
        disk.write_all(RUSTFS_META_BUCKET, UNCLEAN_SHUTDOWN_MARKER_PATH, bytes.into())
            .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_only_handoff_requires_exact_owner_disk_and_supported_state() {
        let id = Uuid::new_v4();
        let record = ReadOnlyHandoff {
            schema: 1,
            owner: "node-a".to_owned(),
            disk_id: id,
            inventory: [7; 32],
            checkpoint_instance_id: "1".repeat(32),
        };
        let bytes = serde_json::to_vec(&record).expect("valid handoff");
        assert!(decode(&bytes, "node-a", Some(id), Some([7; 32])).is_some());
        assert!(decode(&bytes, "node-a", Some(id), Some([8; 32])).is_none());
        for (owner, disk) in [("node-b", Some(id)), ("node-a", Some(Uuid::new_v4())), ("node-a", None)] {
            assert!(decode(&bytes, owner, disk, Some([7; 32])).is_none());
        }
        for bytes in [b"running".as_slice(), b"1730000000", b"{}", b"{\"schema\":1}"] {
            assert!(decode(bytes, "node-a", Some(id), Some([7; 32])).is_none());
        }
        for field in ["schema", "checkpoint_instance_id", "unknown"] {
            let mut value = serde_json::to_value(&record).expect("handoff JSON");
            value[field] = if field == "schema" {
                serde_json::json!(2)
            } else {
                serde_json::json!("0".repeat(32))
            };
            assert!(decode(&serde_json::to_vec(&value).expect("invalid handoff"), "node-a", Some(id), Some([7; 32])).is_none());
        }
    }
}
