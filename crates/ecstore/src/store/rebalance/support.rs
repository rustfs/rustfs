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

use std::cmp::Ordering;

use crate::error::{Error, Result, StorageError, is_err_object_not_found, is_err_version_not_found};
use crate::object_api::{ObjectInfo, ObjectOptions};
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
    result.map_err(|err| Error::other(format!("failed to delete rebalance source object {bucket}/{object}: {err}")))
}

fn is_ignorable_rebalance_delete_error(err: &Error) -> bool {
    is_err_object_not_found(err) || is_err_version_not_found(err)
}

fn rebalance_delete_pool_error(pool_idx: usize, bucket: &str, object: &str, err: Error) -> Error {
    Error::other(format!("pool {pool_idx} delete failed for {bucket}/{object}: {err}"))
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

pub(super) fn resolve_latest_object_info_candidates(
    mut candidates: Vec<LatestObjectInfoCandidate>,
    bucket: &str,
    object: &str,
    opts: &ObjectOptions,
) -> Result<(ObjectInfo, usize)> {
    candidates.sort_by(|a, b| {
        let a_mod = if let Some(info) = &a.info {
            info.mod_time.unwrap_or(OffsetDateTime::UNIX_EPOCH)
        } else {
            OffsetDateTime::UNIX_EPOCH
        };

        let b_mod = if let Some(info) = &b.info {
            info.mod_time.unwrap_or(OffsetDateTime::UNIX_EPOCH)
        } else {
            OffsetDateTime::UNIX_EPOCH
        };

        if a_mod == b_mod {
            return if a.idx < b.idx { Ordering::Greater } else { Ordering::Less };
        }

        b_mod.cmp(&a_mod)
    });

    for candidate in candidates {
        if let Some(info) = candidate.info {
            return Ok((info, candidate.idx));
        }

        if let Some(err) = candidate.err
            && !is_err_object_not_found(&err)
            && !is_err_version_not_found(&err)
        {
            return Err(err);
        }
    }

    Err(pool_lookup_not_found_error(bucket, object, opts))
}
