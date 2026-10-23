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

use std::collections::HashMap;

use crate::error::{Error, Result, StorageError, is_err_object_not_found, is_err_version_not_found};
use crate::object_api::{ObjectInfo, ObjectOptions};
use rustfs_utils::http::metadata_compat::{
    SUFFIX_REPLICATION_DELETE_MARKER_VERSION_ARN_PREFIX, SUFFIX_REPLICATION_RESET_ARN_PREFIX,
    strip_internal_prefix_preserving_case,
};
use rustfs_utils::path::decode_dir_object;
use time::OffsetDateTime;

#[derive(Debug, Default)]
pub(in crate::store) struct PoolErr {
    pub(in crate::store) index: Option<usize>,
    pub(in crate::store) err: Option<Error>,
}

#[derive(Debug, Default)]
pub(in crate::store) struct PoolObjInfo {
    pub(in crate::store) index: usize,
    pub(in crate::store) object_info: ObjectInfo,
    pub(in crate::store) err: Option<Error>,
}

impl Clone for PoolObjInfo {
    fn clone(&self) -> Self {
        Self {
            index: self.index,
            object_info: self.object_info.clone(),
            err: self.err.clone(),
        }
    }
}

pub(in crate::store) struct LatestObjectInfoCandidate {
    pub(in crate::store) info: Option<ObjectInfo>,
    pub(in crate::store) idx: usize,
    pub(in crate::store) err: Option<Error>,
}

pub(super) fn pool_lookup_not_found_error(bucket: &str, object: &str, opts: &ObjectOptions) -> Error {
    let object = decode_dir_object(object);

    if let Some(version_id) = &opts.version_id {
        StorageError::VersionNotFound(bucket.to_owned(), object, version_id.clone())
    } else {
        StorageError::ObjectNotFound(bucket.to_owned(), object)
    }
}

pub(super) fn resolve_store_rebalance_pool_meta_reload_result<T>(result: Result<T>, stage: &str) -> Result<T> {
    result.map_err(|err| Error::other(format!("store rebalance pool meta reload failed during {stage}: {err}")))
}

fn is_ignorable_rebalance_delete_error(err: &Error) -> bool {
    is_err_object_not_found(err) || is_err_version_not_found(err)
}

pub(super) fn resolve_rebalance_delete_from_all_pools_results(
    results: Vec<Result<ObjectInfo>>,
    bucket: &str,
    object: &str,
) -> Result<ObjectInfo> {
    let mut deleted = None;
    let mut ignored_error = None;

    for result in results {
        match result {
            Ok(info) => {
                if deleted.is_none() {
                    deleted = Some(info);
                }
            }
            Err(err) if is_ignorable_rebalance_delete_error(&err) => {
                ignored_error = Some(err);
            }
            Err(err) => {
                return Err(err);
            }
        }
    }

    if let Some(info) = deleted {
        return Ok(info);
    }

    if let Some(err) = ignored_error {
        return Err(err);
    }

    Err(Error::other(format!(
        "failed to delete rebalance source object {bucket}/{object}: no pools were attempted"
    )))
}

pub(super) fn rebalance_disk_set_lookup_error(pool_idx: usize, set_idx: usize, pool_count: usize) -> Error {
    Error::other(format!(
        "failed to resolve rebalance disk set: pool index {pool_idx}, set index {set_idx}, pool count {pool_count}",
    ))
}

fn latest_candidate_mod_time(candidate: &LatestObjectInfoCandidate) -> Option<OffsetDateTime> {
    candidate
        .info
        .as_ref()
        .map(|info| info.mod_time.unwrap_or(OffsetDateTime::UNIX_EPOCH))
}

fn same_transition_identity(left: &ObjectInfo, right: &ObjectInfo) -> bool {
    left.transition_version_state == right.transition_version_state
        && left.transitioned_object.name == right.transitioned_object.name
        && left.transitioned_object.version_id == right.transitioned_object.version_id
        && left.transitioned_object.tier == right.transitioned_object.tier
        && left.transitioned_object.free_version == right.transitioned_object.free_version
        && left.transitioned_object.status == right.transitioned_object.status
}

#[derive(PartialEq, Eq)]
struct LatestUserDefinedIdentity {
    internal: HashMap<String, String>,
    other: HashMap<String, String>,
}

fn normalize_internal_identity_suffix(key: &str) -> Option<String> {
    let suffix = strip_internal_prefix_preserving_case(key)?;

    for dynamic_prefix in [
        SUFFIX_REPLICATION_RESET_ARN_PREFIX,
        SUFFIX_REPLICATION_DELETE_MARKER_VERSION_ARN_PREFIX,
    ] {
        let prefix_len = dynamic_prefix.len();
        if let (Some(prefix), Some(remainder)) = (suffix.get(..prefix_len), suffix.get(prefix_len..))
            && prefix.eq_ignore_ascii_case(dynamic_prefix)
        {
            return Some(format!("{dynamic_prefix}{remainder}"));
        }
    }

    Some(suffix.to_lowercase())
}

fn normalize_user_defined_identity(user_defined: &HashMap<String, String>) -> Option<LatestUserDefinedIdentity> {
    let mut identity = LatestUserDefinedIdentity {
        internal: HashMap::with_capacity(user_defined.len()),
        other: HashMap::with_capacity(user_defined.len()),
    };

    for (key, value) in user_defined {
        if let Some(suffix) = normalize_internal_identity_suffix(key) {
            if identity
                .internal
                .insert(suffix, value.clone())
                .is_some_and(|previous| previous != *value)
            {
                return None;
            }
        } else {
            identity.other.insert(key.clone(), value.clone());
        }
    }

    Some(identity)
}

fn same_user_defined_identity(left: &ObjectInfo, right: &ObjectInfo) -> bool {
    match (
        normalize_user_defined_identity(&left.user_defined),
        normalize_user_defined_identity(&right.user_defined),
    ) {
        (Some(left), Some(right)) => left == right,
        _ => false,
    }
}

/// Pool-specific erasure geometry, version history, and data-movement rewrites
/// are intentionally excluded. A copied version can be the target's latest
/// while the source still holds newer versions. The selected winner keeps its
/// pool's layout; the version's read-visible identity must still agree.
pub(super) fn same_latest_object_info_identity(left: &ObjectInfo, right: &ObjectInfo) -> bool {
    let same_read_surface = left.bucket == right.bucket
        && left.name == right.name
        && left.is_dir == right.is_dir
        && left.restore_ongoing == right.restore_ongoing
        && left.restore_expires == right.restore_expires
        && left.content_type == right.content_type
        && left.content_encoding == right.content_encoding
        && left.inlined == right.inlined
        && left.metadata_only == right.metadata_only
        && left.version_only == right.version_only
        && left.replication_decision == right.replication_decision;

    if !same_read_surface {
        return false;
    }

    // These describe the pool's version list, not the version itself. A
    // partially moved entry keeps every version on its source and only some
    // on its target, so copies of one version legitimately disagree here
    // (rustfs/backlog#2873, defect F). A version id names one version, so an
    // exact copy of an identified version needs no matching list; a null
    // version does not, and keeps the full comparison.
    let same_version_history = left.is_latest == right.is_latest
        && left.num_versions == right.num_versions
        && left.successor_mod_time == right.successor_mod_time;
    let identified_version = left.version_id.is_some_and(|id| !id.is_nil());

    let exact_identity = left.storage_class == right.storage_class
        && left.mod_time == right.mod_time
        && left.size == right.size
        && left.actual_size == right.actual_size
        && same_user_defined_identity(left, right)
        && left.user_tags == right.user_tags
        && left.version_id == right.version_id
        && left.data_dir == right.data_dir
        && left.delete_marker == right.delete_marker
        && same_transition_identity(left, right)
        && left.parts == right.parts
        && left.expires == right.expires
        && left.etag == right.etag
        && left.replication_status_internal == right.replication_status_internal
        && left.replication_status == right.replication_status
        && left.version_purge_status_internal == right.version_purge_status_internal
        && left.version_purge_status == right.version_purge_status
        && left.checksum == right.checksum;

    if exact_identity && (same_version_history || identified_version) {
        return true;
    }

    // A copied delete marker carries the target's bucket incarnation fence,
    // which the source marker does not; data movement already treats that as
    // the same marker.
    if left.delete_marker && right.delete_marker {
        return crate::store::object::is_equivalent_data_movement_delete_marker(left, right)
            || crate::store::object::is_equivalent_data_movement_delete_marker(right, left);
    }

    let left_moved =
        rustfs_utils::http::get_consistent_str(&left.user_defined, rustfs_utils::http::SUFFIX_DATA_MOVED) == Some("true");
    let right_moved =
        rustfs_utils::http::get_consistent_str(&right.user_defined, rustfs_utils::http::SUFFIX_DATA_MOVED) == Some("true");

    match (left_moved, right_moved) {
        (false, false) => false,
        (false, true) => crate::data_movement::is_equivalent_data_movement_object_identity(left, right, true, true),
        (true, false) => crate::data_movement::is_equivalent_data_movement_object_identity(right, left, true, true),
        (true, true) => {
            crate::data_movement::is_equivalent_data_movement_object_identity(left, right, true, true)
                || crate::data_movement::is_equivalent_data_movement_object_identity(right, left, true, true)
        }
    }
}

pub(super) fn validate_prepared_pool_refetch_identity(expected: &ObjectInfo, refetched: &ObjectInfo) -> Result<()> {
    // A refetch stays in the same pool, so its history must still match the
    // snapshot even though cross-pool copies may have different inventories.
    if expected.is_latest == refetched.is_latest
        && expected.num_versions == refetched.num_versions
        && expected.successor_mod_time == refetched.successor_mod_time
        && same_latest_object_info_identity(expected, refetched)
    {
        Ok(())
    } else {
        Err(Error::ErasureReadQuorum)
    }
}

#[cfg(test)]
pub(super) fn resolve_latest_object_info_candidates(
    candidates: Vec<LatestObjectInfoCandidate>,
    bucket: &str,
    object: &str,
    opts: &ObjectOptions,
) -> Result<(ObjectInfo, usize)> {
    resolve_latest_object_info_candidates_with_pool_state(candidates, &[], bucket, object, opts)
}

pub(in crate::store) fn resolve_latest_object_info_candidates_with_pool_state(
    candidates: Vec<LatestObjectInfoCandidate>,
    suspended_pools: &[bool],
    bucket: &str,
    object: &str,
    opts: &ObjectOptions,
) -> Result<(ObjectInfo, usize)> {
    let latest_mod_time = candidates.iter().filter_map(latest_candidate_mod_time).max();

    if let Some(latest_mod_time) = latest_mod_time {
        let mut latest_candidates = candidates
            .into_iter()
            .filter(|candidate| latest_candidate_mod_time(candidate) == Some(latest_mod_time))
            .collect::<Vec<_>>();

        // A decommission source remains readable until its target commits. Once
        // equivalent copies exist, prefer the active target without hiding a
        // same-time identity conflict behind pool state.
        latest_candidates.sort_by_key(|candidate| {
            (
                suspended_pools.get(candidate.idx).copied().unwrap_or(false),
                std::cmp::Reverse(candidate.idx),
            )
        });

        let Some(winner) = latest_candidates.first() else {
            return Err(Error::ErasureReadQuorum);
        };
        let Some(winner_info) = winner.info.as_ref() else {
            return Err(Error::ErasureReadQuorum);
        };

        if latest_candidates.iter().skip(1).any(|candidate| {
            candidate
                .info
                .as_ref()
                .is_none_or(|info| !same_latest_object_info_identity(winner_info, info))
        }) {
            return Err(Error::ErasureReadQuorum);
        }

        let winner = latest_candidates.swap_remove(0);
        let Some(winner_info) = winner.info else {
            return Err(Error::ErasureReadQuorum);
        };
        return Ok((winner_info, winner.idx));
    }

    for candidate in candidates {
        if let Some(err) = candidate.err
            && !is_err_object_not_found(&err)
            && !is_err_version_not_found(&err)
        {
            return Err(err);
        }
    }

    Err(pool_lookup_not_found_error(bucket, object, opts))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prepared_pool_refetch_identity_fails_closed_on_generation_change() {
        let expected = ObjectInfo {
            mod_time: Some(OffsetDateTime::from_unix_timestamp(10).expect("test timestamp should be valid")),
            version_id: Some(uuid::Uuid::from_u128(1)),
            etag: Some("etag-a".to_string()),
            ..Default::default()
        };
        let mut refetched = expected.clone();
        refetched.etag = Some("etag-b".to_string());

        let error = validate_prepared_pool_refetch_identity(&expected, &refetched)
            .expect_err("refetched metadata from a changed generation must fail closed");

        assert_eq!(error, Error::ErasureReadQuorum);
    }

    #[test]
    fn prepared_pool_refetch_identity_fails_closed_on_version_history_change() {
        let expected = ObjectInfo {
            mod_time: Some(OffsetDateTime::from_unix_timestamp(10).expect("test timestamp should be valid")),
            version_id: Some(uuid::Uuid::from_u128(1)),
            etag: Some("etag-a".to_string()),
            ..Default::default()
        };
        for refetched in [
            ObjectInfo {
                is_latest: !expected.is_latest,
                ..expected.clone()
            },
            ObjectInfo {
                num_versions: expected.num_versions + 1,
                ..expected.clone()
            },
            ObjectInfo {
                successor_mod_time: Some(OffsetDateTime::from_unix_timestamp(20).expect("successor timestamp should be valid")),
                ..expected.clone()
            },
        ] {
            let error = validate_prepared_pool_refetch_identity(&expected, &refetched)
                .expect_err("a changed same-pool history must invalidate the prepared snapshot");
            assert_eq!(error, Error::ErasureReadQuorum);
        }
    }

    #[test]
    fn rebalance_delete_aggregation_preserves_typed_failures() {
        for expected in [
            Error::UnsupportedDisk,
            Error::GuardedDeleteUnsupported,
            Error::ErasureReadQuorum,
            Error::ErasureWriteQuorum,
            Error::SlowDown,
            Error::PreconditionFailed,
            Error::PrefixAccessDenied("bucket".to_owned(), "object".to_owned()),
            Error::FileCorrupt,
        ] {
            for results in [
                vec![Err(expected.clone())],
                vec![Ok(ObjectInfo::default()), Err(expected.clone())],
                vec![Err(expected.clone()), Ok(ObjectInfo::default())],
                vec![
                    Err(Error::ObjectNotFound("bucket".to_owned(), "object".to_owned())),
                    Err(expected.clone()),
                ],
            ] {
                let actual = resolve_rebalance_delete_from_all_pools_results(results, "bucket", "object")
                    .expect_err("a pool failure must retain its S3 error classification");
                assert_eq!(actual, expected);
            }
        }
    }

    #[test]
    fn rebalance_delete_aggregation_preserves_missing_identity() {
        for expected in [
            Error::ObjectNotFound("bucket".to_owned(), "object".to_owned()),
            Error::VersionNotFound("bucket".to_owned(), "object".to_owned(), "version".to_owned()),
        ] {
            let actual = resolve_rebalance_delete_from_all_pools_results(vec![Err(expected.clone())], "bucket", "object")
                .expect_err("all-missing deletes must preserve their typed object or version identity");
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn rebalance_delete_aggregation_rejects_no_attempted_pools() {
        let error = resolve_rebalance_delete_from_all_pools_results(Vec::new(), "bucket", "object")
            .expect_err("an empty attempt set cannot report a successful delete");
        let Error::Io(error) = error else {
            panic!("an empty attempt set must remain an internal error");
        };
        assert_eq!(error.kind(), std::io::ErrorKind::Other);
        assert_eq!(
            error.to_string(),
            "failed to delete rebalance source object bucket/object: no pools were attempted"
        );
    }
}
