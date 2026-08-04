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

use super::super::*;

pub(crate) fn metadata_log_locations(
    current_metadata: &serde_json::Value,
    namespace: &Namespace,
    table: &IdentifierSegment,
) -> BTreeSet<String> {
    let mut locations = BTreeSet::new();
    let Some(metadata_log) = current_metadata.get("metadata-log").and_then(serde_json::Value::as_array) else {
        return locations;
    };

    for entry in metadata_log {
        let Some(metadata_location) = entry.get("metadata-file").and_then(serde_json::Value::as_str) else {
            continue;
        };
        if is_valid_table_metadata_location(namespace, table, metadata_location) {
            locations.insert(metadata_location.to_string());
        }
    }

    locations
}

pub(crate) async fn metadata_locations_for_protected_snapshot_refs<B>(
    backend: &B,
    table_bucket: &str,
    namespace: &Namespace,
    table: &IdentifierSegment,
    current_metadata: &serde_json::Value,
    metadata_locations: &[String],
) -> TableCatalogStoreResult<BTreeSet<String>>
where
    B: TableCatalogObjectBackend,
{
    let protected_snapshot_ids = protected_ref_snapshot_ids(current_metadata);
    if protected_snapshot_ids.is_empty() {
        return Ok(BTreeSet::new());
    }

    let mut retained = BTreeSet::new();
    for metadata_location in metadata_locations {
        if !is_valid_table_metadata_location(namespace, table, metadata_location) {
            continue;
        }
        let Some(metadata_object) = backend.read_object(table_bucket, metadata_location).await? else {
            continue;
        };
        let Ok(metadata) = serde_json::from_slice::<serde_json::Value>(&metadata_object.data) else {
            continue;
        };
        if metadata_contains_protected_snapshot_ref(&metadata, &protected_snapshot_ids) {
            retained.insert(metadata_location.clone());
        }
    }
    Ok(retained)
}

pub(crate) fn protected_ref_snapshot_ids(current_metadata: &serde_json::Value) -> BTreeSet<i64> {
    let mut snapshot_ids = BTreeSet::new();
    let current_snapshot_id = current_metadata
        .get("current-snapshot-id")
        .and_then(serde_json::Value::as_i64);
    let Some(refs) = current_metadata.get("refs").and_then(serde_json::Value::as_object) else {
        return snapshot_ids;
    };

    for reference in refs.values() {
        if let Some(snapshot_id) = reference.get("snapshot-id").and_then(serde_json::Value::as_i64)
            && Some(snapshot_id) != current_snapshot_id
        {
            snapshot_ids.insert(snapshot_id);
        }
    }
    snapshot_ids
}

pub(crate) fn metadata_contains_protected_snapshot_ref(
    metadata: &serde_json::Value,
    protected_snapshot_ids: &BTreeSet<i64>,
) -> bool {
    let current_snapshot_matches = metadata
        .get("current-snapshot-id")
        .and_then(serde_json::Value::as_i64)
        .is_some_and(|snapshot_id| protected_snapshot_ids.contains(&snapshot_id));
    if current_snapshot_matches {
        return true;
    }

    let Some(refs) = metadata.get("refs").and_then(serde_json::Value::as_object) else {
        return false;
    };
    refs.values().any(|reference| {
        reference
            .get("snapshot-id")
            .and_then(serde_json::Value::as_i64)
            .is_some_and(|snapshot_id| protected_snapshot_ids.contains(&snapshot_id))
    })
}

pub(crate) fn metadata_candidate_is_past_safety_window(mod_time: Option<OffsetDateTime>, now: OffsetDateTime) -> bool {
    let Some(mod_time) = mod_time else {
        return false;
    };
    mod_time <= now - Duration::seconds(TABLE_METADATA_CLEANUP_SAFETY_WINDOW_SECONDS)
}
