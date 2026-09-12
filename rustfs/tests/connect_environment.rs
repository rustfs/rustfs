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

use std::time::Duration;

use rustfs::connect::{
    ENVIRONMENT_CAPABILITY, ENVIRONMENT_SCHEMA_VERSION, EnvironmentCollectionRequest, EnvironmentError,
    EnvironmentFilesystemType, InventorySnapshot, MAX_ENVIRONMENT_DURATION, collect_environment,
};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

fn inventory() -> InventorySnapshot {
    InventorySnapshot::current(2, 8, 8_000_000, 2_000_000, []).expect("known small deployment inventory")
}

fn request() -> EnvironmentCollectionRequest {
    EnvironmentCollectionRequest::negotiate(ENVIRONMENT_SCHEMA_VERSION, ENVIRONMENT_CAPABILITY, Duration::from_secs(2))
        .expect("supported inventory.environment request")
}

#[test]
fn environment_negotiation_rejects_old_versions_unknown_capabilities_and_invalid_budgets() {
    assert_eq!(
        EnvironmentCollectionRequest::negotiate(0, ENVIRONMENT_CAPABILITY, Duration::from_secs(1)),
        Err(EnvironmentError::UnsupportedVersion)
    );
    assert_eq!(
        EnvironmentCollectionRequest::negotiate(ENVIRONMENT_SCHEMA_VERSION, "inventory.environment@2", Duration::from_secs(1)),
        Err(EnvironmentError::UnsupportedCapability)
    );
    for timeout in [Duration::ZERO, MAX_ENVIRONMENT_DURATION + Duration::from_millis(1)] {
        assert_eq!(
            EnvironmentCollectionRequest::negotiate(ENVIRONMENT_SCHEMA_VERSION, ENVIRONMENT_CAPABILITY, timeout),
            Err(EnvironmentError::InvalidTimeout)
        );
    }
}

#[tokio::test]
async fn environment_collection_acknowledges_cancellation_before_sampling() {
    let cancel = CancellationToken::new();
    cancel.cancel();
    let result = collect_environment(&inventory(), request(), &cancel).await;
    assert_eq!(result, Err(EnvironmentError::Cancelled));
}

#[tokio::test]
async fn environment_collection_emits_only_the_closed_identifier_free_schema() {
    let result = collect_environment(&inventory(), request(), &CancellationToken::new())
        .await
        .expect("this host exposes bounded environment inventory");
    assert_eq!(result.node_count(), 2);
    assert_eq!(result.drive_count(), 8);
    assert!(!result.filesystem_types().is_empty());
    assert!(result.filesystem_types().len() <= 5);
    assert!(result.filesystem_types().iter().all(|value| matches!(
        value,
        EnvironmentFilesystemType::Ext4
            | EnvironmentFilesystemType::Xfs
            | EnvironmentFilesystemType::Zfs
            | EnvironmentFilesystemType::Apfs
            | EnvironmentFilesystemType::Other
    )));

    let serialized = serde_json::to_value(&result).expect("environment JSON");
    let object = serialized.as_object().expect("environment object");
    assert_eq!(
        object.keys().map(String::as_str).collect::<std::collections::BTreeSet<_>>(),
        ["driveCount", "filesystemTypes", "nodeCount", "osFamily"]
            .into_iter()
            .collect()
    );
    let encoded = serde_json::to_string(&serialized).expect("environment JSON text");
    for forbidden in [
        "hostname",
        "mountPath",
        "mountOptions",
        "device",
        "serial",
        "credential",
        "AWS_SECRET_ACCESS_KEY",
        "/srv/customer-a",
    ] {
        assert!(!encoded.contains(forbidden), "environment output exposed {forbidden}");
    }

    println!("inventory.environment actual output: {encoded}");
    assert!(matches!(serialized, Value::Object(_)));
}
