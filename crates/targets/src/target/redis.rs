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

use redis::{AsyncConnectionConfig, Client, ClientTlsConfig, ConnectionAddr, ConnectionInfo, IntoConnectionInfo, RedisError};
use std::{
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use url::Url;

use crate::{
    StoreError, Target, TargetLog,
    arn::TargetID,
    error::TargetError,
    store::{Key, QueueStore, Store},
    target::{
        ChannelTargetType, EntityTarget, QueuedPayload, QueuedPayloadMeta, TargetDeliveryCounters, TargetDeliverySnapshot,
        TargetType,
    },
};

/// Arguments for configuring an Redis target.
#[derive(Debug, Clone)]
pub struct RedisArgs {
    /// Whether the target is enabled
    pub enable: bool,
    /// The Redis server URL in format: `{redis|rediss|valkey|valkeys}://[<username>][:<password>@]<hostname>[:port][/<db>]`
    pub url: Url,
    /// The username for the Redis connection (leave it empty if you parse with url)
    pub username: Option<String>,
    /// The password for the Redis connection (leave it empty if you parse with url)
    pub password: Option<String>,
    /// Client certificate path for TLS authentication
    pub client_cert_path: Option<String>,
    /// Client key path for TLS authentication
    pub client_key_path: Option<String>,
    /// Root certificate path if the local truststore is *not* to be used
    pub root_cert_path: Option<String>,
    /// The keep alive interval
    pub keep_alive: Duration,
    /// The directory to store events in case of failure
    pub queue_dir: String,
    /// The maximum number of events to store
    pub queue_limit: u64,
    /// the target type
    pub target_type: TargetType,
}

pub fn validate_redis_url(url: &Url, args: &RedisArgs) -> Result<(), TargetError> {
    match url.scheme() {
        "redis" | "rediss" | "valkey" | "valkeys" => {}
        _ => {
            return Err(TargetError::Configuration("unknown protocol in redis address".to_string()));
        }
    }

    let connect_addr: ConnectionInfo = url
        .clone()
        .into_connection_info()
        .map_err(|e| TargetError::Configuration(e.to_string()))?;

    Ok(())
}

impl RedisArgs {
    pub fn validate(&self) -> Result<(), TargetError> {
        if !self.enable {
            return Ok(());
        }

        validate_redis_url(&self.url, &self)?;

        Ok(())
    }
}

struct RedisTarget {
    client: Client,
}
