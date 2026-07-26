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

use rustfs_config::server_config::Config;
use rustfs_notify::{NotificationError, NotificationRuntimeState, ensure_live_events, notification_system, reconcile};
use std::sync::Arc;

fn assert_terminated(error: NotificationError) {
    assert!(matches!(
        error,
        NotificationError::Initialization(detail) if detail == "Notification runtime has terminated"
    ));
}

#[tokio::test]
async fn global_singleton_survives_suspend_but_not_process_termination() {
    let system = ensure_live_events();
    let same_system = ensure_live_events();
    assert!(Arc::ptr_eq(&system, &same_system));
    assert!(Arc::ptr_eq(
        &system,
        &notification_system().expect("global notification system should exist")
    ));

    system
        .set_targets_enabled(true, Some(Config::new()))
        .await
        .expect("empty target runtime should enable");
    assert!(matches!(
        system.runtime_lifecycle_state(),
        NotificationRuntimeState::TargetsEnabled { .. }
    ));

    system
        .set_targets_enabled(false, None)
        .await
        .expect("disable should suspend targets without terminating the singleton");
    assert_eq!(system.runtime_lifecycle_state(), NotificationRuntimeState::LiveOnly);

    system
        .set_targets_enabled(true, None)
        .await
        .expect("a suspended target runtime should be restartable");
    assert!(matches!(
        system.runtime_lifecycle_state(),
        NotificationRuntimeState::TargetsEnabled { .. }
    ));

    system
        .shutdown_checked()
        .await
        .expect("process shutdown should terminate the target runtime");
    assert_eq!(system.runtime_lifecycle_state(), NotificationRuntimeState::Terminated);

    let lazy_system = ensure_live_events();
    assert!(Arc::ptr_eq(&system, &lazy_system));
    assert_terminated(
        reconcile(Config::new())
            .await
            .expect_err("lazy reconciliation must not restart a terminated process runtime"),
    );
    assert_terminated(
        lazy_system
            .reload_config(Config::new())
            .await
            .expect_err("config reload must not restart a terminated process runtime"),
    );
    assert_eq!(lazy_system.runtime_lifecycle_state(), NotificationRuntimeState::Terminated);
}
