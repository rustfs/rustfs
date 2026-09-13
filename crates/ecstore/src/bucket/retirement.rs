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

//! Immutable evidence of a completed bucket-generation deletion. These objects
//! deliberately live outside the bucket metadata cleanup prefix. Offline disks
//! have no bounded return time, so retirement evidence must not expire by age.

use crate::config::com::{read_config_limited_preserve_empty, save_config_with_opts};
use crate::error::{Error, Result};
use crate::object_api::ObjectOptions;
use crate::store::ECStore;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

pub(crate) struct MarkerRetirementContext<'a> {
    pub store: Option<Arc<ECStore>>,
    pub current_incarnation: Option<Uuid>,
    pub lifecycle_guard: &'a rustfs_lock::NamespaceLockGuard,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetirementRecord {
    version: u8,
    deployment_id: Uuid,
    bucket: String,
    incarnation_id: Uuid,
}

fn expected_record(store: &ECStore, bucket: &str, incarnation_id: Uuid) -> Result<RetirementRecord> {
    let deployment_id = store
        .pools
        .first()
        .ok_or_else(|| Error::other("missing retirement deployment"))?
        .format
        .id;
    if deployment_id.is_nil() || incarnation_id.is_nil() {
        return Err(Error::other("retirement requires non-nil deployment and incarnation IDs"));
    }
    Ok(RetirementRecord {
        version: 1,
        deployment_id,
        bucket: bucket.to_owned(),
        incarnation_id,
    })
}

fn record_path(bucket: &str, incarnation_id: Uuid) -> String {
    format!("bucket-retirements/{bucket}/{incarnation_id}.json")
}

/// The caller owns the bucket lifecycle WRITE guard and has completed physical
/// deletion on every selected set, including the existing rollback decision.
/// A failed or uncertain publication is an error, never a successful DELETE ACK.
pub(crate) async fn commit_retirement(
    store: Arc<ECStore>,
    bucket: &str,
    incarnation_id: Uuid,
    opts: &ObjectOptions,
) -> Result<()> {
    let record = expected_record(&store, bucket, incarnation_id)?;
    save_config_with_opts(store, &record_path(bucket, incarnation_id), serde_json::to_vec(&record)?, opts).await
}

pub(crate) async fn is_retired(store: Arc<ECStore>, bucket: &str, incarnation_id: Uuid) -> Result<bool> {
    let expected = expected_record(&store, bucket, incarnation_id)?;
    let bytes = match read_config_limited_preserve_empty(store, &record_path(bucket, incarnation_id), 4096).await {
        Ok(bytes) => bytes,
        Err(Error::ConfigNotFound) => return Ok(false),
        Err(error) => return Err(error),
    };
    let record: RetirementRecord = serde_json::from_slice(&bytes)?;
    if record != expected {
        return Err(Error::other("bucket retirement identity mismatch"));
    }
    Ok(true)
}
