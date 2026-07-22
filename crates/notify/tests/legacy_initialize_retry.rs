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

use rustfs_config::notify::NOTIFY_WEBHOOK_SUB_SYS;
use rustfs_config::server_config::{Config, KVS};
use rustfs_config::{ENABLE_KEY, WEBHOOK_ENDPOINT};
use rustfs_notify::{initialize, notification_system};

#[tokio::test]
async fn failed_legacy_initialize_can_retry_the_stable_singleton() {
    let mut invalid_target = KVS::new();
    invalid_target.insert(ENABLE_KEY.to_string(), "on".to_string());
    invalid_target.insert(WEBHOOK_ENDPOINT.to_string(), "not-a-url".to_string());
    let mut invalid_config = Config::new();
    invalid_config
        .0
        .entry(NOTIFY_WEBHOOK_SUB_SYS.to_string())
        .or_default()
        .insert("primary".to_string(), invalid_target);

    initialize(invalid_config)
        .await
        .expect_err("invalid first target activation should fail");
    initialize(Config::new())
        .await
        .expect("legacy initialize should retry after the configuration is fixed");

    notification_system()
        .expect("stable singleton should remain available")
        .shutdown_checked()
        .await
        .expect("test runtime should terminate");
}
