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
    DiagnosticCollectionPolicy, DiagnosticScheduleError, DiagnosticScheduleStatus, InventorySnapshot, ReceiptOutcome,
    run_local_environment_once, spawn_environment_schedule,
};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

fn policy(revision: u64, desired_state: &str, reason_code: Option<&str>) -> DiagnosticCollectionPolicy {
    serde_json::from_value(serde_json::json!({
        "policyName": "organizations/0198f4b0-1a00-7c10-8d21-2e3f4a5b6c70/clusters/0198f4b0-2b00-7d20-9e31-3f4a5b6c7d81/diagnosticCollectionPreference",
        "revision": revision,
        "supportState": "SUPPORTED",
        "desiredState": desired_state,
        "reasonCode": reason_code,
        "toolIds": if desired_state == "RUNNING" { vec!["inventory.environment"] } else { vec![] },
        "intervalSeconds": if desired_state == "RUNNING" { Some(300) } else { None },
        "scope": "CLUSTER",
        "retentionDays": if desired_state == "RUNNING" { Some(7) } else { None },
        "consentReference": if desired_state == "RUNNING" { Some("organizations/0198f4b0-1a00-7c10-8d21-2e3f4a5b6c70/diagnosticCollectionConsents/0198f4b0-3c00-7e30-8f41-4a5b6c7d8e92") } else { None },
        "consentExpiresAt": if desired_state == "RUNNING" { Some("2099-01-01T00:00:00Z") } else { None }
    }))
    .expect("policy")
}

async fn receipt(status: &mut watch::Receiver<DiagnosticScheduleStatus>) -> rustfs::connect::DiagnosticReceipt {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let DiagnosticScheduleStatus::Receipt(receipt) = status.borrow().clone() {
                return receipt;
            }
            status.changed().await.expect("schedule remains active");
        }
    })
    .await
    .expect("receipt timeout")
}

#[tokio::test]
async fn local_one_shot_observes_cancellation_before_collecting() {
    let inventory = InventorySnapshot::current(1, 1, 1_024, 512, []).expect("inventory");
    let cancel = CancellationToken::new();
    cancel.cancel();

    assert!(matches!(
        run_local_environment_once(&inventory, &cancel).await,
        Err(DiagnosticScheduleError::Cancelled)
    ));
}

#[test]
fn collection_policy_contract_rejects_unknown_fields() {
    let policy = serde_json::json!({
        "policyName": null,
        "revision": 0,
        "supportState": "SUPPORTED",
        "desiredState": "STOPPED",
        "reasonCode": "DISABLED",
        "toolIds": [],
        "intervalSeconds": null,
        "scope": "CLUSTER",
        "retentionDays": null,
        "consentReference": null,
        "consentExpiresAt": null,
        "unexpected": true
    });

    assert!(serde_json::from_value::<rustfs::connect::DiagnosticCollectionPolicy>(policy).is_err());
}

#[tokio::test]
async fn missing_inventory_has_bounded_retries_and_durable_failure_receipt() {
    let temp = tempfile::tempdir().expect("tempdir");
    let (_policy_tx, policy_rx) = watch::channel(policy(3, "RUNNING", None));
    let shutdown = CancellationToken::new();
    let mut runtime = spawn_environment_schedule(temp.path(), policy_rx, shutdown.clone()).expect("scheduler");
    let mut status = runtime.status();

    let failed = receipt(&mut status).await;
    assert_eq!(failed.outcome, ReceiptOutcome::Failed);
    assert_eq!(failed.attempt_count, 3);
    let state: serde_json::Value =
        serde_json::from_slice(&std::fs::read(temp.path().join("diagnostics/schedule.json")).expect("durable state"))
            .expect("state JSON");
    assert_eq!(state["lastReceipt"]["receiptId"], failed.receipt_id);
    shutdown.cancel();
    runtime.shutdown().await;

    let (_policy_tx, policy_rx) = watch::channel(policy(3, "RUNNING", None));
    let shutdown = CancellationToken::new();
    runtime = spawn_environment_schedule(temp.path(), policy_rx, shutdown.clone()).expect("restart scheduler");
    tokio::time::sleep(Duration::from_millis(100)).await;
    let restarted: serde_json::Value =
        serde_json::from_slice(&std::fs::read(temp.path().join("diagnostics/schedule.json")).expect("durable state"))
            .expect("state JSON");
    assert_eq!(restarted["lastReceipt"]["receiptId"], failed.receipt_id);
    shutdown.cancel();
    runtime.shutdown().await;
}

#[tokio::test]
async fn consent_revoke_cancels_retry_and_persists_receipt() {
    let temp = tempfile::tempdir().expect("tempdir");
    let (policy_tx, policy_rx) = watch::channel(policy(4, "RUNNING", None));
    let shutdown = CancellationToken::new();
    let runtime = spawn_environment_schedule(temp.path(), policy_rx, shutdown.clone()).expect("scheduler");
    let mut status = runtime.status();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !matches!(&*status.borrow(), DiagnosticScheduleStatus::Running { .. }) {
            status.changed().await.expect("schedule remains active");
        }
    })
    .await
    .expect("first attempt");

    policy_tx
        .send(policy(5, "STOPPED", Some("CONSENT_INACTIVE")))
        .expect("revoke policy");
    let cancelled = receipt(&mut status).await;
    assert_eq!(cancelled.outcome, ReceiptOutcome::Cancelled);
    let state: serde_json::Value =
        serde_json::from_slice(&std::fs::read(temp.path().join("diagnostics/schedule.json")).expect("durable state"))
            .expect("state JSON");
    assert_eq!(state["lastReceipt"]["outcome"], "CANCELLED");
    shutdown.cancel();
    runtime.shutdown().await;
}
