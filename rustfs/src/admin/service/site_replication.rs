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

use rustfs_ecstore::config::com::read_config;
use rustfs_ecstore::error::Error as StorageError;
use rustfs_ecstore::new_object_layer_fn;
use s3s::{S3Error, S3ErrorCode, S3Result};

const SITE_REPLICATION_STATE_PATH: &str = "config/site-replication/state.json";

/// Reload persisted site-replication state.
///
/// RustFS does not currently keep a separate in-memory cache for this state,
/// so "reload" means validating that the persisted JSON is readable.
pub async fn reload_site_replication_runtime_state() -> S3Result<()> {
    let Some(store) = new_object_layer_fn() else {
        return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
    };

    match read_config(store, SITE_REPLICATION_STATE_PATH).await {
        Ok(data) => {
            let _: serde_json::Value = serde_json::from_slice(&data)
                .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("invalid site replication state: {e}")))?;
            Ok(())
        }
        Err(StorageError::ConfigNotFound) => Ok(()),
        Err(err) => Err(S3Error::with_message(
            S3ErrorCode::InternalError,
            format!("failed to load site replication state: {err}"),
        )),
    }
}
