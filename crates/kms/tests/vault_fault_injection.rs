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

//! Fault-injection matrix for the Vault backend operation policy.
//!
//! Offline cases run against locally injected HTTP and transport faults — no external
//! dependencies. Real-Vault cases are `#[ignore]`d and need a dev Vault
//! (default `http://127.0.0.1:8200`, override with `RUSTFS_KMS_VAULT_ADDR`).
//!
//! Throttling (429) and recoverable 5xx responses cannot be forced on a stock
//! dev Vault, so their retry and metric behavior is pinned deterministically
//! by the scripted-Vault wiring tests in `backends::vault` and the engine
//! tests in `policy.rs`. Pointing `RUSTFS_KMS_VAULT_ADDR` at a
//! fault-injecting proxy reuses the ignored cases here unchanged.
//!
//! Every case installs a thread-local debugging metrics recorder and drives a
//! current-thread runtime inside it, so the policy metrics double as the
//! request-count assertion even against a real server.

use std::time::Duration;

use metrics_util::MetricKind;
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use rustfs_kms::backends::KmsBackend as KmsBackendTrait;
use rustfs_kms::backends::vault::VaultKmsBackend;
use rustfs_kms::{
    BackendConfig, DescribeKeyRequest, KmsBackend as KmsBackendKind, KmsConfig, KmsError, VaultAuthMethod, VaultConfig,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

const OPERATIONS_TOTAL: &str = "rustfs_kms_backend_operations_total";
const ATTEMPT_FAILURES_TOTAL: &str = "rustfs_kms_backend_attempt_failures_total";
const LOGIN: &str = "vault_login";
const READ_KEY: &str = "vault_kv2_read_key";
const LOOKUP_REQUEST: &str = "GET /v1/auth/token/lookup-self HTTP/1.1";

/// Unlike the unit-test scripted Vault, this fixture records the credential
/// probe too and can fail it independently of the subsequent key request.
/// `None` parks a connection without responding; extra requests receive 599.
struct FaultVault {
    address: String,
    requests: mpsc::UnboundedReceiver<String>,
    task: JoinHandle<()>,
}

impl FaultVault {
    async fn serve(responses: Vec<Option<(u16, serde_json::Value)>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind fault-injection Vault");
        let address = format!("http://{}", listener.local_addr().expect("fault-injection Vault address"));
        let (recorded, requests) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let mut responses = responses.into_iter();
            let mut parked = Vec::new();
            loop {
                let (stream, _) = listener.accept().await.expect("accept Vault request");
                let mut stream = BufReader::new(stream);
                let mut line = String::new();
                assert_ne!(stream.read_line(&mut line).await.expect("read request line"), 0);
                recorded.send(line.trim_end().to_string()).expect("record Vault request");
                loop {
                    line.clear();
                    assert_ne!(stream.read_line(&mut line).await.expect("read request header"), 0);
                    if line == "\r\n" {
                        break;
                    }
                }
                let mut stream = stream.into_inner();
                let response = responses
                    .next()
                    .unwrap_or_else(|| Some((599, serde_json::json!({"errors": ["unexpected Vault request"]}))));
                if let Some((status, body)) = response {
                    let body = body.to_string();
                    let response = format!(
                        "HTTP/1.1 {status} Scripted\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    stream.write_all(response.as_bytes()).await.expect("write Vault response");
                    stream.shutdown().await.expect("close Vault response");
                } else {
                    parked.push(stream);
                }
            }
        });
        Self { address, requests, task }
    }

    async fn finish(&mut self) {
        assert!(self.requests.try_recv().is_err(), "no unexpected requests may remain");
        self.task.abort();
        let error = (&mut self.task).await.expect_err("fault server runs until aborted");
        assert!(error.is_cancelled(), "fault server must not panic: {error}");
    }
}

impl Drop for FaultVault {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn healthy_token_lookup() -> serde_json::Value {
    serde_json::json!({
        "data": {
            "accessor": "fault-injection-accessor",
            "creation_time": 1_700_000_000u64,
            "creation_ttl": 0,
            "display_name": "token",
            "entity_id": "",
            "explicit_max_ttl": 0,
            "id": "unused",
            "num_uses": 0,
            "orphan": true,
            "path": "auth/token/create",
            "policies": ["default"],
            "renewable": false,
            "ttl": 0
        }
    })
}

fn vault_config(address: &str, token: &str) -> VaultConfig {
    VaultConfig {
        address: address.to_string(),
        auth_method: VaultAuthMethod::Token {
            token: token.to_string(),
        },
        namespace: None,
        mount_path: "transit".to_string(),
        kv_mount: "secret".to_string(),
        key_path_prefix: "rustfs/kms/fault-injection".to_string(),
        tls: None,
    }
}

fn kms_config(vault_config: VaultConfig, attempt_timeout: Duration, retry_attempts: u32) -> KmsConfig {
    KmsConfig {
        backend: KmsBackendKind::VaultKv2,
        backend_config: BackendConfig::VaultKv2(Box::new(vault_config)),
        allow_insecure_dev_defaults: true,
        timeout: attempt_timeout,
        retry_attempts,
        ..KmsConfig::default()
    }
}

fn describe_key_request(key_id: &str) -> DescribeKeyRequest {
    DescribeKeyRequest {
        key_id: key_id.to_string(),
    }
}

type MetricEntry = (
    metrics_util::CompositeKey,
    Option<metrics::Unit>,
    Option<metrics::SharedString>,
    DebugValue,
);

/// Run `test` on a current-thread runtime under a debugging metrics recorder
/// and return one snapshot of everything it emitted.
///
/// A single snapshot per test on purpose: `Snapshotter::snapshot` drains the
/// recorded state, so taking it per assertion would only show the first
/// assertion any data.
fn record_metrics(test: impl FnOnce() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()>>>) -> Vec<MetricEntry> {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::with_local_recorder(&recorder, || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("current-thread runtime must build");
        runtime.block_on(test());
    });
    snapshotter.snapshot().into_vec()
}

/// Sum of counters with `name` whose labels include every `(key, value)` pair.
fn counter_value(snapshot: &[MetricEntry], name: &str, labels: &[(&str, &str)]) -> u64 {
    snapshot
        .iter()
        .filter_map(|(composite, _unit, _description, value)| {
            let key = composite.key();
            let matches = composite.kind() == MetricKind::Counter
                && key.name() == name
                && labels
                    .iter()
                    .all(|(label, expected)| key.labels().any(|l| l.key() == *label && l.value() == *expected));
            match (matches, value) {
                (true, DebugValue::Counter(count)) => Some(*count),
                _ => None,
            }
        })
        .sum()
}

/// Stalled connection: a server that accepts but never responds is cut off by
/// the per-attempt timeout (either the policy timer or the equally sized HTTP
/// client timeout, whichever fires first) instead of hanging forever.
#[test]
fn stalled_connection_is_cut_off_by_the_attempt_timeout() {
    let snapshot = record_metrics(|| {
        Box::pin(async move {
            let mut vault = FaultVault::serve(vec![Some((200, healthy_token_lookup())), None]).await;
            let attempt_timeout = Duration::from_millis(250);
            let client = VaultKmsBackend::new(kms_config(vault_config(&vault.address, "unused"), attempt_timeout, 1))
                .await
                .expect("the token lookup must succeed before injecting the stalled key read");
            assert_eq!(vault.requests.try_recv().as_deref(), Ok(LOOKUP_REQUEST));

            let read = KmsBackendTrait::describe_key(&client, describe_key_request("fault-injection-stalled"));
            tokio::pin!(read);
            tokio::select! {
                request = vault.requests.recv() => assert_eq!(
                    request.as_deref(),
                    Some("GET /v1/secret/data/rustfs/kms/fault-injection/fault-injection-stalled? HTTP/1.1")
                ),
                result = &mut read => panic!("the key request must reach the stall listener: {result:?}"),
            }
            // Pause only after real loopback I/O reaches the intended request;
            // otherwise auto-advancing time could expire the login instead.
            tokio::time::pause();
            let stalled_at = tokio::time::Instant::now();
            // Tokio rounds timer deadlines up to the next millisecond.
            let virtual_step = attempt_timeout + Duration::from_millis(1);
            tokio::time::advance(virtual_step).await;
            let error = tokio::time::timeout(Duration::from_secs(1), read)
                .await
                .expect("the attempt timer must resolve without further network activity")
                .expect_err("a stalled request must be cut off by the attempt timeout");
            assert_eq!(
                stalled_at.elapsed(),
                virtual_step,
                "the read must resolve within the attempt budget plus one timer tick"
            );
            assert!(
                matches!(error, KmsError::OperationTimedOut { .. } | KmsError::BackendError { .. }),
                "got {error:?}"
            );
            vault.finish().await;
        })
    });

    // The policy timer reports attempt_timeout; the client-level HTTP timeout
    // surfaces as a connection-class failure. Either way it is exactly one
    // attempt that was cut off.
    let cut_off = counter_value(
        &snapshot,
        ATTEMPT_FAILURES_TOTAL,
        &[("operation", READ_KEY), ("error_class", "attempt_timeout")],
    ) + counter_value(
        &snapshot,
        ATTEMPT_FAILURES_TOTAL,
        &[("operation", READ_KEY), ("error_class", "retryable_conn")],
    );
    assert_eq!(cut_off, 1, "the single budgeted attempt must be cut off by a timeout");
    assert_eq!(counter_value(&snapshot, ATTEMPT_FAILURES_TOTAL, &[("operation", LOGIN)]), 0);
    assert_eq!(
        counter_value(&snapshot, OPERATIONS_TOTAL, &[("operation", LOGIN), ("outcome", "success")]),
        1
    );
    assert_eq!(
        counter_value(&snapshot, OPERATIONS_TOTAL, &[("operation", READ_KEY), ("outcome", "budget_exhausted")]),
        1
    );
}

/// A returned lookup error degrades lease discovery, not Vault authorization:
/// the subsequent forbidden key read must still fail once, without retrying.
#[test]
fn token_lookup_errors_do_not_bypass_key_authorization() {
    for lookup_status in [403, 503] {
        let snapshot = record_metrics(|| {
            Box::pin(async move {
                let mut vault = FaultVault::serve(vec![
                    Some((lookup_status, serde_json::json!({"errors": ["token lookup unavailable"]}))),
                    Some((403, serde_json::json!({"errors": ["permission denied"]}))),
                ])
                .await;
                let client = VaultKmsBackend::new(kms_config(vault_config(&vault.address, "unused"), Duration::from_secs(5), 3))
                    .await
                    .expect("a returned token lookup error must preserve static-token fallback");
                assert_eq!(vault.requests.try_recv().as_deref(), Ok(LOOKUP_REQUEST));
                let error = KmsBackendTrait::describe_key(&client, describe_key_request("fault-injection-forbidden"))
                    .await
                    .expect_err("lease discovery fallback must not authorize a forbidden key read");
                assert!(matches!(error, KmsError::BackendError { .. }), "got {error:?}");
                assert_eq!(
                    vault.requests.try_recv().as_deref(),
                    Ok("GET /v1/secret/data/rustfs/kms/fault-injection/fault-injection-forbidden? HTTP/1.1")
                );
                vault.finish().await;
            })
        });
        assert_eq!(counter_value(&snapshot, ATTEMPT_FAILURES_TOTAL, &[("operation", LOGIN)]), 0);
        assert_eq!(
            counter_value(&snapshot, OPERATIONS_TOTAL, &[("operation", LOGIN), ("outcome", "success")]),
            1
        );
        assert_eq!(counter_value(&snapshot, ATTEMPT_FAILURES_TOTAL, &[("operation", READ_KEY)]), 1);
        assert_eq!(
            counter_value(&snapshot, ATTEMPT_FAILURES_TOTAL, &[("operation", READ_KEY), ("error_class", "fatal")]),
            1
        );
        assert_eq!(
            counter_value(&snapshot, OPERATIONS_TOTAL, &[("operation", READ_KEY), ("outcome", "fatal")]),
            1
        );
    }
}

fn real_vault_address() -> String {
    std::env::var("RUSTFS_KMS_VAULT_ADDR").unwrap_or_else(|_| "http://127.0.0.1:8200".to_string())
}

/// Invalid token against a real Vault: the 403 is fatal — exactly one
/// attempt, no retry, and the operation fails closed.
#[test]
#[ignore] // Requires a running Vault dev server
fn real_vault_invalid_token_is_fatal_and_never_retried() {
    let snapshot = record_metrics(|| {
        Box::pin(async {
            let config = vault_config(&real_vault_address(), "fault-injection-invalid-token");
            let client = VaultKmsBackend::new(kms_config(config, Duration::from_secs(5), 3))
                .await
                .expect("a returned token lookup error must preserve static-token fallback");
            let error = KmsBackendTrait::describe_key(&client, describe_key_request("fault-injection-forbidden"))
                .await
                .expect_err("an invalid token must be rejected");
            assert!(matches!(error, KmsError::BackendError { .. }), "got {error:?}");
        })
    });

    assert_eq!(
        counter_value(&snapshot, ATTEMPT_FAILURES_TOTAL, &[("operation", READ_KEY), ("error_class", "fatal")]),
        1,
        "a 403 must be observed by exactly one attempt"
    );
    assert_eq!(
        counter_value(&snapshot, OPERATIONS_TOTAL, &[("operation", READ_KEY), ("outcome", "fatal")]),
        1
    );
    assert_eq!(
        counter_value(
            &snapshot,
            ATTEMPT_FAILURES_TOTAL,
            &[("operation", READ_KEY), ("error_class", "retryable_status")]
        ),
        0,
        "an auth failure must never be classified as retryable"
    );
}

/// Healthy read against a real Vault: a missing key resolves in one attempt
/// (404 is fatal for retry purposes) and records a fatal outcome rather than
/// burning the retry budget.
#[test]
#[ignore] // Requires a running Vault dev server
fn real_vault_missing_key_is_resolved_in_one_attempt() {
    let token = std::env::var("RUSTFS_KMS_VAULT_TOKEN").unwrap_or_else(|_| "dev-only-token".to_string());
    let snapshot = record_metrics(|| {
        Box::pin(async move {
            let config = vault_config(&real_vault_address(), &token);
            let client = VaultKmsBackend::new(kms_config(config, Duration::from_secs(5), 3))
                .await
                .expect("static-token initialization must complete against the running Vault");
            let error = KmsBackendTrait::describe_key(&client, describe_key_request("fault-injection-definitely-missing"))
                .await
                .expect_err("a missing key must resolve to key-not-found");
            assert!(matches!(error, KmsError::KeyNotFound { .. }), "got {error:?}");
        })
    });

    assert_eq!(
        counter_value(&snapshot, ATTEMPT_FAILURES_TOTAL, &[("operation", READ_KEY), ("error_class", "fatal")]),
        1,
        "a 404 must be observed by exactly one attempt"
    );
    assert_eq!(
        counter_value(&snapshot, OPERATIONS_TOTAL, &[("operation", READ_KEY), ("outcome", "fatal")]),
        1
    );
}
