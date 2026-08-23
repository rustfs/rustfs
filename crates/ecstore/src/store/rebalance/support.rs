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

pub(super) struct LatestObjectInfoCandidate {
    pub(super) info: Option<ObjectInfo>,
    pub(super) idx: usize,
    pub(super) err: Option<Error>,
}

pub(super) struct RebalanceDeletePoolResult {
    pub(super) pool_idx: usize,
    pub(super) result: Result<ObjectInfo>,
}

pub(super) fn pool_lookup_not_found_error(bucket: &str, object: &str, opts: &ObjectOptions) -> Error {
    let object = decode_dir_object(object);

    if let Some(version_id) = &opts.version_id {
        StorageError::VersionNotFound(bucket.to_owned(), object, version_id.clone())
    } else {
        StorageError::ObjectNotFound(bucket.to_owned(), object)
    }
}

pub(super) fn resolve_store_rebalance_pool_meta_reload_result(result: Result<()>, stage: &str) -> Result<()> {
    result.map_err(|err| Error::other(format!("store rebalance pool meta reload failed during {stage}: {err}")))
}

pub(super) fn resolve_rebalance_delete_from_all_pools_result(
    result: Result<ObjectInfo>,
    bucket: &str,
    object: &str,
) -> Result<ObjectInfo> {
    result.map_err(|err| {
        if matches!(&err, Error::PreconditionFailed | Error::PrefixAccessDenied(_, _)) {
            err
        } else {
            Error::other(format!("failed to delete rebalance source object {bucket}/{object}: {err}"))
        }
    })
}

fn is_ignorable_rebalance_delete_error(err: &Error) -> bool {
    is_err_object_not_found(err) || is_err_version_not_found(err)
}

fn rebalance_delete_pool_error(pool_idx: usize, bucket: &str, object: &str, err: Error) -> Error {
    if matches!(&err, Error::PreconditionFailed | Error::PrefixAccessDenied(_, _)) {
        err
    } else {
        Error::other(format!("pool {pool_idx} delete failed for {bucket}/{object}: {err}"))
    }
}

pub(super) fn resolve_rebalance_delete_from_all_pools_results(
    results: Vec<RebalanceDeletePoolResult>,
    bucket: &str,
    object: &str,
) -> Result<ObjectInfo> {
    let mut deleted = None;
    let mut ignored_error = None;

    for pool_result in results {
        let pool_idx = pool_result.pool_idx;
        match pool_result.result {
            Ok(info) => {
                if deleted.is_none() {
                    deleted = Some(info);
                }
            }
            Err(err) if is_ignorable_rebalance_delete_error(&err) => {
                ignored_error = Some((pool_idx, err));
            }
            Err(err) => {
                return Err(rebalance_delete_pool_error(pool_idx, bucket, object, err));
            }
        }
    }

    if let Some(info) = deleted {
        return Ok(info);
    }

    if let Some((pool_idx, err)) = ignored_error {
        return Err(rebalance_delete_pool_error(pool_idx, bucket, object, err));
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

/// Pool-specific erasure geometry is intentionally excluded: `get_object_info`
/// returns each pool's own `data_blocks`/`parity_blocks`, so those values can
/// differ for the same object version while the selected winner still carries
/// the chosen pool's layout. `put_object_reader` is also intentionally
/// excluded because it is a transient request handle that `ObjectInfo::clone`
/// drops. Every other ObjectInfo field is part of the production-visible
/// identity and must agree before the pool index can provide a deterministic
/// tie-break.
fn same_latest_object_info_identity(left: &ObjectInfo, right: &ObjectInfo) -> bool {
    left.bucket == right.bucket
        && left.name == right.name
        && left.storage_class == right.storage_class
        && left.mod_time == right.mod_time
        && left.size == right.size
        && left.actual_size == right.actual_size
        && left.is_dir == right.is_dir
        && same_user_defined_identity(left, right)
        && left.user_tags == right.user_tags
        && left.version_id == right.version_id
        && left.data_dir == right.data_dir
        && left.delete_marker == right.delete_marker
        && same_transition_identity(left, right)
        && left.restore_ongoing == right.restore_ongoing
        && left.restore_expires == right.restore_expires
        && left.parts == right.parts
        && left.is_latest == right.is_latest
        && left.content_type == right.content_type
        && left.content_encoding == right.content_encoding
        && left.expires == right.expires
        && left.num_versions == right.num_versions
        && left.successor_mod_time == right.successor_mod_time
        && left.etag == right.etag
        && left.inlined == right.inlined
        && left.metadata_only == right.metadata_only
        && left.version_only == right.version_only
        && left.replication_status_internal == right.replication_status_internal
        && left.replication_status == right.replication_status
        && left.version_purge_status_internal == right.version_purge_status_internal
        && left.version_purge_status == right.version_purge_status
        && left.replication_decision == right.replication_decision
        && left.checksum == right.checksum
}

pub(super) fn resolve_latest_object_info_candidates(
    candidates: Vec<LatestObjectInfoCandidate>,
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

        latest_candidates.sort_by_key(|candidate| std::cmp::Reverse(candidate.idx));

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

        return Ok((winner_info.clone(), winner.idx));
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
    fn rebalance_delete_result_preserves_precondition_failed() {
        let err = resolve_rebalance_delete_from_all_pools_result(Err(Error::PreconditionFailed), "bucket", "object")
            .expect_err("precondition failure should remain structured");

        assert_eq!(err, Error::PreconditionFailed);
    }

    #[test]
    fn rebalance_delete_result_preserves_prefix_access_denied() {
        let err = resolve_rebalance_delete_from_all_pools_result(
            Err(Error::PrefixAccessDenied("bucket".to_owned(), "object".to_owned())),
            "bucket",
            "object",
        )
        .expect_err("prefix access denial should remain structured");

        assert_eq!(err, Error::PrefixAccessDenied("bucket".to_owned(), "object".to_owned()));
    }

    #[test]
    fn rebalance_delete_pool_result_preserves_precondition_failed() {
        let err = resolve_rebalance_delete_from_all_pools_results(
            vec![RebalanceDeletePoolResult {
                pool_idx: 0,
                result: Err(Error::PreconditionFailed),
            }],
            "bucket",
            "object",
        )
        .expect_err("precondition failure should remain structured");

        assert_eq!(err, Error::PreconditionFailed);
    }

    #[test]
    fn rebalance_delete_pool_result_preserves_prefix_access_denied() {
        let err = resolve_rebalance_delete_from_all_pools_results(
            vec![RebalanceDeletePoolResult {
                pool_idx: 0,
                result: Err(Error::PrefixAccessDenied("bucket".to_owned(), "object".to_owned())),
            }],
            "bucket",
            "object",
        )
        .expect_err("prefix access denial should remain structured");

        assert_eq!(err, Error::PrefixAccessDenied("bucket".to_owned(), "object".to_owned()));
    }
}
