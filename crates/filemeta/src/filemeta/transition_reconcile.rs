// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::{FileMeta, FileMetaVersion};
use crate::{Error, Result, TRANSITION_COMPLETE, TransitionVersionState};
use rustfs_utils::http::metadata_compat::{
    SUFFIX_TRANSITION_TIER_DESTINATION_ID, SUFFIX_TRANSITIONED_VERSION_ID, SUFFIX_TRANSITIONED_VERSION_STATE, contains_key_bytes,
    get_consistent_bytes, insert_bytes, remove_bytes,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const RECONCILE_SUFFIXES: [&str; 3] = [
    SUFFIX_TRANSITIONED_VERSION_STATE,
    SUFFIX_TRANSITIONED_VERSION_ID,
    SUFFIX_TRANSITION_TIER_DESTINATION_ID,
];

/// The only fields a legacy transition repair is allowed to persist.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransitionStateReconcileTarget {
    pub state: TransitionVersionState,
    pub remote_version: Option<String>,
    pub destination_id: String,
}

impl TransitionStateReconcileTarget {
    pub fn validate(&self) -> Result<()> {
        let valid_version = match self.state {
            TransitionVersionState::KnownDisabled => self.remote_version.is_none(),
            TransitionVersionState::SuspendedNull => self.remote_version.as_deref() == Some("null"),
            TransitionVersionState::Exact => self.remote_version.as_deref().is_some_and(|value| {
                !value.is_empty()
                    && value.len() <= 1024
                    && value != "null"
                    && !value.chars().any(char::is_control)
                    && !Uuid::parse_str(value).is_ok_and(|id| id.is_nil())
            }),
            TransitionVersionState::Unknown => false,
        };
        if !valid_version
            || self.destination_id.len() != 64
            || !self
                .destination_id
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(Error::FileCorrupt);
        }
        Ok(())
    }
}

impl FileMeta {
    /// Canonical identity of every version and inline byte, excluding only the
    /// three repairable suffixes on the selected version. It survives a repair
    /// and encoding-order changes, while detecting unrelated metadata changes.
    pub fn transition_reconcile_generation(&self, version_id: Option<Uuid>) -> Result<Vec<u8>> {
        if self
            .versions
            .iter()
            .filter(|version| version.header.version_id.unwrap_or_default() == version_id.unwrap_or_default())
            .count()
            != 1
        {
            return Err(Error::FileCorrupt);
        }
        let (selected, _) = self.find_version(version_id)?;
        let mut versions = Vec::with_capacity(self.versions.len());
        for index in 0..self.versions.len() {
            let mut version = self.get_idx(index)?;
            if index == selected {
                let object = version.object.as_mut().ok_or(Error::FileCorrupt)?;
                for suffix in RECONCILE_SUFFIXES {
                    remove_bytes(&mut object.meta_sys, suffix);
                }
            }
            versions.push(version);
        }
        versions.sort_by_key(|version| version.get_version_id().unwrap_or_default());
        let mut value = serde_json::to_value((&versions, &self.data)).map_err(|_| Error::FileCorrupt)?;
        value.sort_all_objects();
        serde_json::to_vec(&value).map_err(|_| Error::FileCorrupt)
    }

    /// Returns false for an already converged record. Callers must serialize
    /// the read/check/commit and compare the observed metadata generation.
    pub fn reconcile_transition_state(
        &mut self,
        version_id: Option<Uuid>,
        target: &TransitionStateReconcileTarget,
    ) -> Result<bool> {
        target.validate()?;
        let (index, mut version) = self.find_version(version_id)?;
        let info = version.into_fileinfo("", "", true)?;
        info.validate_for_metadata_read()?;
        if info.transition_status != TRANSITION_COMPLETE
            || info.transition_tier.is_empty()
            || info.transitioned_objname.is_empty()
        {
            return Err(Error::FileCorrupt);
        }
        let object = version.object.as_mut().ok_or(Error::FileCorrupt)?;
        let destination = get_consistent_bytes(&object.meta_sys, SUFFIX_TRANSITION_TIER_DESTINATION_ID);
        if contains_key_bytes(&object.meta_sys, SUFFIX_TRANSITION_TIER_DESTINATION_ID)
            && destination != Some(target.destination_id.as_bytes())
        {
            return Err(Error::FileCorrupt);
        }
        if contains_key_bytes(&object.meta_sys, SUFFIX_TRANSITIONED_VERSION_STATE) {
            if info.transition_version_state == target.state
                && info.transition_version == target.remote_version
                && destination == Some(target.destination_id.as_bytes())
            {
                return Ok(false);
            }
            return Err(Error::FileCorrupt);
        }
        if info.transition_version_state != TransitionVersionState::Unknown
            || info
                .transition_version
                .as_deref()
                .filter(|value| !value.is_empty())
                .is_some_and(|value| Some(value) != target.remote_version.as_deref())
        {
            return Err(Error::FileCorrupt);
        }
        insert_bytes(
            &mut object.meta_sys,
            SUFFIX_TRANSITIONED_VERSION_STATE,
            target.state.as_str().as_bytes().to_vec(),
        );
        remove_bytes(&mut object.meta_sys, SUFFIX_TRANSITIONED_VERSION_ID);
        if let Some(version) = &target.remote_version {
            insert_bytes(&mut object.meta_sys, SUFFIX_TRANSITIONED_VERSION_ID, version.as_bytes().to_vec());
        }
        insert_bytes(
            &mut object.meta_sys,
            SUFFIX_TRANSITION_TIER_DESTINATION_ID,
            target.destination_id.as_bytes().to_vec(),
        );
        version.into_fileinfo("", "", true)?.validate_for_metadata_read()?;
        self.set_idx(index, version)?;
        Ok(true)
    }
}

/// A stale healer or metadata writer may carry the original absent fields.
/// Preserve a proven binding for the same immutable transition, or reject an
/// attempted change of meaning. A new payload/version or a delete is separate.
pub(super) fn preserve_reconciled_transition(previous: &FileMetaVersion, next: &mut FileMetaVersion) -> Result<()> {
    let (Some(previous_object), Some(next_object)) = (&previous.object, &mut next.object) else {
        return Ok(());
    };
    if !contains_key_bytes(&previous_object.meta_sys, SUFFIX_TRANSITION_TIER_DESTINATION_ID)
        || !contains_key_bytes(&previous_object.meta_sys, SUFFIX_TRANSITIONED_VERSION_STATE)
        || previous_object.data_dir != next_object.data_dir
    {
        return Ok(());
    }
    let previous_info = previous.into_fileinfo("", "", true)?;
    if previous_info.transition_version_state == TransitionVersionState::Unknown
        || previous_info.transition_status != TRANSITION_COMPLETE
    {
        return Ok(());
    }
    let next_info = next.into_fileinfo("", "", true)?;
    if previous_info.transition_tier != next_info.transition_tier
        || previous_info.transitioned_objname != next_info.transitioned_objname
        || previous_info.transition_status != next_info.transition_status
        || previous_info.size != next_info.size
        || previous_info.metadata.get("etag") != next_info.metadata.get("etag")
    {
        return Err(Error::FileCorrupt);
    }
    let destination =
        get_consistent_bytes(&previous_object.meta_sys, SUFFIX_TRANSITION_TIER_DESTINATION_ID).ok_or(Error::FileCorrupt)?;
    previous_info.validate_for_metadata_read()?;
    TransitionStateReconcileTarget {
        state: previous_info.transition_version_state,
        remote_version: previous_info.transition_version.clone(),
        destination_id: std::str::from_utf8(destination).map_err(|_| Error::FileCorrupt)?.to_string(),
    }
    .validate()?;
    let next_object = next.object.as_mut().ok_or(Error::FileCorrupt)?;
    if next_info
        .transition_version
        .as_ref()
        .is_some_and(|version| Some(version) != previous_info.transition_version.as_ref())
    {
        return Err(Error::FileCorrupt);
    }
    if contains_key_bytes(&next_object.meta_sys, SUFFIX_TRANSITIONED_VERSION_STATE) {
        if previous_info.transition_version_state != next_info.transition_version_state
            || previous_info.transition_version != next_info.transition_version
        {
            return Err(Error::FileCorrupt);
        }
    } else if next_info.transition_version_state != TransitionVersionState::Unknown {
        return Err(Error::FileCorrupt);
    }
    if contains_key_bytes(&next_object.meta_sys, SUFFIX_TRANSITION_TIER_DESTINATION_ID)
        && get_consistent_bytes(&next_object.meta_sys, SUFFIX_TRANSITION_TIER_DESTINATION_ID) != Some(destination)
    {
        return Err(Error::FileCorrupt);
    }
    for suffix in RECONCILE_SUFFIXES {
        remove_bytes(&mut next_object.meta_sys, suffix);
        if let Some(value) = get_consistent_bytes(&previous_object.meta_sys, suffix) {
            insert_bytes(&mut next_object.meta_sys, suffix, value.to_vec());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ErasureInfo, FileInfo, ObjectPartInfo};

    fn legacy() -> (FileMeta, FileInfo) {
        let info = FileInfo {
            version_id: Some(Uuid::from_u128(1)),
            data_dir: Some(Uuid::from_u128(2)),
            mod_time: Some(time::OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("fixture time")),
            size: 7,
            parts: vec![ObjectPartInfo {
                number: 1,
                size: 7,
                actual_size: 7,
                ..Default::default()
            }],
            erasure: ErasureInfo {
                algorithm: "ReedSolomon".to_string(),
                data_blocks: 2,
                parity_blocks: 2,
                block_size: 1024 * 1024,
                index: 1,
                distribution: vec![1, 2, 3, 4],
                ..Default::default()
            },
            transition_status: TRANSITION_COMPLETE.to_string(),
            transition_tier: "WARM".to_string(),
            transitioned_objname: "remote-object".to_string(),
            metadata: std::collections::HashMap::from([("etag".to_string(), "source-etag".to_string())]),
            data: Some(bytes::Bytes::from_static(b"payload")),
            ..Default::default()
        };
        let mut metadata = FileMeta::new();
        metadata.add_version(info.clone()).expect("legacy fixture");
        (metadata, info)
    }

    fn target(state: TransitionVersionState) -> TransitionStateReconcileTarget {
        TransitionStateReconcileTarget {
            state,
            remote_version: match state {
                TransitionVersionState::Exact => Some("opaque-version".to_string()),
                TransitionVersionState::SuspendedNull => Some("null".to_string()),
                _ => None,
            },
            destination_id: "ab".repeat(32),
        }
    }

    #[test]
    fn transition_reconcile_preserves_payload_and_generation_and_is_idempotent() {
        for state in [
            TransitionVersionState::KnownDisabled,
            TransitionVersionState::SuspendedNull,
            TransitionVersionState::Exact,
        ] {
            let (mut metadata, info) = legacy();
            let mut other = info.clone();
            other.version_id = Some(Uuid::from_u128(3));
            other.data_dir = Some(Uuid::from_u128(4));
            other.transition_status.clear();
            other.transition_tier.clear();
            other.transitioned_objname.clear();
            other.data = Some(bytes::Bytes::from_static(b"other!!"));
            metadata.add_version(other.clone()).expect("unrelated inline version");
            let other_before = metadata.find_version(other.version_id).expect("unrelated version").1;
            let original_data = metadata.data.clone();
            let generation = metadata
                .transition_reconcile_generation(info.version_id)
                .expect("initial generation");
            let target = target(state);
            assert!(metadata.reconcile_transition_state(info.version_id, &target).expect("repair"));
            let bytes = metadata.marshal_msg().expect("encode repair");
            let mut reloaded = FileMeta::load(&bytes).expect("reload repair");
            assert_eq!(reloaded.data, original_data);
            assert_eq!(
                reloaded
                    .find_version(other.version_id)
                    .expect("preserved unrelated version")
                    .1,
                other_before
            );
            assert_eq!(
                reloaded
                    .transition_reconcile_generation(info.version_id)
                    .expect("repaired generation"),
                generation
            );
            assert!(!reloaded.reconcile_transition_state(info.version_id, &target).expect("retry"));
            let (_, version) = reloaded.find_version(info.version_id).expect("selected version");
            let repaired = version.into_fileinfo("", "", true).expect("decode explicit state");
            assert_eq!(repaired.transition_version_state, state);
            assert_eq!(repaired.transition_version, target.remote_version);
            assert_eq!(repaired.parts, info.parts);
            for prefix in [
                rustfs_utils::http::RUSTFS_INTERNAL_PREFIX,
                rustfs_utils::http::MINIO_INTERNAL_PREFIX,
            ] {
                assert_eq!(
                    version
                        .object
                        .as_ref()
                        .expect("object")
                        .meta_sys
                        .get(&format!("{prefix}{SUFFIX_TRANSITIONED_VERSION_STATE}")),
                    Some(&state.as_str().as_bytes().to_vec())
                );
            }
        }
    }

    #[test]
    fn transition_reconcile_generation_detects_unrelated_metadata_and_inline_changes() {
        let (mut metadata, info) = legacy();
        let original = metadata.transition_reconcile_generation(info.version_id).expect("generation");
        let mut updated = info.clone();
        updated.metadata.insert("user-tag".to_string(), "changed".to_string());
        metadata.update_object_version(updated).expect("update unrelated field");
        assert_ne!(metadata.transition_reconcile_generation(info.version_id).expect("generation"), original);
        let (mut metadata, mut info) = legacy();
        info.data = Some(bytes::Bytes::from_static(b"changed"));
        metadata.add_version(info.clone()).expect("change inline bytes");
        assert_ne!(metadata.transition_reconcile_generation(info.version_id).expect("generation"), original);
    }

    #[test]
    fn transition_reconcile_binding_survives_stale_heal_and_metadata_writes() {
        let (mut metadata, mut stale) = legacy();
        let target = target(TransitionVersionState::Exact);
        metadata
            .reconcile_transition_state(stale.version_id, &target)
            .expect("repair");
        metadata
            .add_version(stale.clone())
            .expect("stale heal must preserve the binding");
        stale.metadata.insert("user-tag".to_string(), "updated".to_string());
        metadata
            .update_object_version(stale.clone())
            .expect("ordinary metadata update");
        assert!(
            !metadata
                .reconcile_transition_state(stale.version_id, &target)
                .expect("binding remains exact")
        );
    }

    #[test]
    fn transition_reconcile_rejects_explicit_unknown_and_conflicting_binding() {
        let (mut metadata, mut info) = legacy();
        rustfs_utils::http::insert_str(&mut info.metadata, SUFFIX_TRANSITIONED_VERSION_STATE, "unknown".to_string());
        metadata.add_version(info.clone()).expect("explicit unknown fixture");
        assert!(
            metadata
                .reconcile_transition_state(info.version_id, &target(TransitionVersionState::Exact))
                .is_err()
        );
        let (mut metadata, mut stale) = legacy();
        metadata
            .reconcile_transition_state(stale.version_id, &target(TransitionVersionState::Exact))
            .expect("repair");
        stale.transition_version_state = TransitionVersionState::KnownDisabled;
        assert!(
            metadata.add_version(stale).is_err(),
            "an explicit state cannot be replaced with a different model"
        );
    }
}
