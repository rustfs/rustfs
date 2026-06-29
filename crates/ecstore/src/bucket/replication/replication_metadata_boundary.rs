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

use crate::bucket::metadata_sys;
use crate::error::{Error, Result};
use s3s::dto::ReplicationConfiguration;
use time::OffsetDateTime;

pub(crate) async fn replication_config(bucket: &str) -> Result<(ReplicationConfiguration, OffsetDateTime)> {
    metadata_sys::get_replication_config(bucket).await
}

pub(crate) async fn optional_replication_config(bucket: &str) -> Result<Option<ReplicationConfiguration>> {
    let config = match replication_config(bucket).await {
        Ok((config, _)) => Some(config),
        Err(err) => {
            if err != Error::ConfigNotFound {
                return Err(err);
            }
            None
        }
    };
    Ok(config)
}
