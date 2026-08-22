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

//! Ignored live test for a real three-node Vault Raft leader failure.
//!
//! `scripts/test/vault_ha_kms_live.sh` owns the official Vault containers and
//! kills the active node while this test continuously decrypts through a
//! surviving standby. KV2 and Transit must recover after the bounded circuit
//! interval, use a bounded number of attempts, and leave the circuit and
//! in-flight gauges at zero after a new leader is elected.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use metrics_util::MetricKind;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use rustfs_kms::backends::KmsBackend as KmsBackendTrait;
use rustfs_kms::backends::vault::VaultKmsBackend;
use rustfs_kms::backends::vault_transit::VaultTransitKmsBackend;
use rustfs_kms::{
    BackendConfig, CreateKeyRequest, DecryptRequest, GenerateDataKeyRequest, KeySpec, KeyUsage, KmsBackend, KmsConfig,
    VaultAuthMethod, VaultConfig, VaultTransitConfig,
};
use tokio_util::sync::CancellationToken;

const OPERATIONS_TOTAL: &str = "rustfs_kms_backend_operations_total";
const ATTEMPT_FAILURES_TOTAL: &str = "rustfs_kms_backend_attempt_failures_total";
const OPERATION_ATTEMPTS: &str = "rustfs_kms_backend_operation_attempts";
const IN_FLIGHT: &str = "rustfs_kms_backend_in_flight";
const CIRCUIT_OPEN: &str = "rustfs_kms_backend_circuit_open";
const MAX_ATTEMPTS: u32 = 10;
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(2);
const HEALTHY_PROGRESS_TIMEOUT: Duration = Duration::from_secs(20);
// The circuit remains open for 30s after five failed attempts.
const POST_FAILOVER_PROGRESS_TIMEOUT: Duration = Duration::from_secs(35);
const FAILOVER_ERROR_POLL_INTERVAL: Duration = Duration::from_millis(100);

type MetricEntry = (
    metrics_util::CompositeKey,
    Option<metrics::Unit>,
    Option<metrics::SharedString>,
    DebugValue,
);

fn required_env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} must be set by scripts/test/vault_ha_kms_live.sh"))
}

fn auth_method() -> VaultAuthMethod {
    VaultAuthMethod::approle(required_env("RUSTFS_TEST_VAULT_ROLE_ID"), required_env("RUSTFS_TEST_VAULT_SECRET_ID"))
}

fn config(backend: KmsBackend, backend_config: BackendConfig) -> KmsConfig {
    KmsConfig {
        backend,
        backend_config,
        allow_insecure_dev_defaults: true,
        timeout: ATTEMPT_TIMEOUT,
        retry_attempts: MAX_ATTEMPTS,
        enable_cache: false,
        ..KmsConfig::default()
    }
}

fn kv2_config(address: &str) -> KmsConfig {
    config(
        KmsBackend::VaultKv2,
        BackendConfig::VaultKv2(Box::new(VaultConfig {
            address: address.to_string(),
            auth_method: auth_method(),
            namespace: None,
            mount_path: "transit".to_string(),
            kv_mount: "secret".to_string(),
            key_path_prefix: "rustfs/kms/ha-kv2".to_string(),
            tls: None,
        })),
    )
}

fn transit_config(address: &str) -> KmsConfig {
    config(
        KmsBackend::VaultTransit,
        BackendConfig::VaultTransit(Box::new(VaultTransitConfig {
            address: address.to_string(),
            auth_method: auth_method(),
            namespace: None,
            mount_path: "transit".to_string(),
            metadata_kv_mount: "secret".to_string(),
            metadata_key_prefix: "rustfs/kms/ha-transit-metadata".to_string(),
            tls: None,
        })),
    )
}

fn labels_match(key: &metrics::Key, labels: &[(&str, &str)]) -> bool {
    labels.iter().all(|(label, expected)| {
        key.labels()
            .any(|candidate| candidate.key() == *label && candidate.value() == *expected)
    })
}

fn counter_value(snapshot: &[MetricEntry], name: &str, labels: &[(&str, &str)]) -> u64 {
    snapshot
        .iter()
        .filter_map(|(composite, _, _, value)| {
            let matches = composite.kind() == MetricKind::Counter
                && composite.key().name() == name
                && labels_match(composite.key(), labels);
            match (matches, value) {
                (true, DebugValue::Counter(count)) => Some(*count),
                _ => None,
            }
        })
        .sum()
}

fn gauge_value(snapshot: &[MetricEntry], name: &str, labels: &[(&str, &str)]) -> Option<f64> {
    snapshot.iter().find_map(|(composite, _, _, value)| {
        let matches =
            composite.kind() == MetricKind::Gauge && composite.key().name() == name && labels_match(composite.key(), labels);
        match (matches, value) {
            (true, DebugValue::Gauge(value)) => Some(value.into_inner()),
            _ => None,
        }
    })
}

fn histogram_values(snapshot: &[MetricEntry], name: &str, labels: &[(&str, &str)]) -> Vec<f64> {
    snapshot
        .iter()
        .filter_map(|(composite, _, _, value)| {
            let matches = composite.kind() == MetricKind::Histogram
                && composite.key().name() == name
                && labels_match(composite.key(), labels);
            match (matches, value) {
                (true, DebugValue::Histogram(values)) => Some(values),
                _ => None,
            }
        })
        .flatten()
        .map(|value| value.into_inner())
        .collect()
}

fn retryable_failures(snapshot: &[MetricEntry], operation: &str) -> u64 {
    ["retryable_conn", "retryable_status", "attempt_timeout"]
        .into_iter()
        .map(|error_class| {
            counter_value(
                snapshot,
                ATTEMPT_FAILURES_TOTAL,
                &[("operation", operation), ("error_class", error_class)],
            )
        })
        .sum()
}

async fn wait_for_count(
    counter: &AtomicU64,
    failure: &Mutex<Option<String>>,
    minimum: u64,
    description: &str,
    timeout: Duration,
) {
    tokio::time::timeout(timeout, async {
        while counter.load(Ordering::SeqCst) < minimum {
            if let Some(error) = failure.lock().expect("decrypt failure lock poisoned").as_ref() {
                panic!(
                    "{description} worker failed after {} successful decrypts: {error}",
                    counter.load(Ordering::SeqCst)
                );
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "timed out after {timeout:?} waiting for {description}: completed {}, expected {minimum}",
            counter.load(Ordering::SeqCst)
        )
    });
}

async fn wait_for_file(path: &Path, description: &str) {
    tokio::time::timeout(Duration::from_secs(70), async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {description}"));
}

async fn decrypt_loop<B: KmsBackendTrait + Send + Sync + 'static>(
    backend: Arc<B>,
    request: DecryptRequest,
    expected: Vec<u8>,
    completed: Arc<AtomicU64>,
    allow_failover_errors: Arc<AtomicBool>,
    failure: Arc<Mutex<Option<String>>>,
    stop: CancellationToken,
) {
    while !stop.is_cancelled() {
        match backend.decrypt(request.clone()).await {
            Ok(response) if response.plaintext == expected => {
                completed.fetch_add(1, Ordering::SeqCst);
            }
            Ok(_) => {
                *failure.lock().expect("decrypt failure lock poisoned") =
                    Some("decrypt returned unexpected plaintext".to_string());
                return;
            }
            Err(rustfs_kms::KmsError::BackendError { .. } | rustfs_kms::KmsError::OperationTimedOut { .. })
                if allow_failover_errors.load(Ordering::SeqCst) =>
            {
                tokio::time::sleep(FAILOVER_ERROR_POLL_INTERVAL).await;
            }
            Err(error) => {
                *failure.lock().expect("decrypt failure lock poisoned") = Some(error.to_string());
                return;
            }
        }
    }
}

async fn exercise_failover(snapshotter: &Snapshotter) {
    let address = required_env("RUSTFS_TEST_VAULT_ADDRESS");
    let marker = PathBuf::from(required_env("RUSTFS_TEST_VAULT_FAILOVER_MARKER"));
    let elected = marker.with_extension("elected");
    let old_leader = required_env("RUSTFS_TEST_VAULT_OLD_LEADER");

    let kv2 = Arc::new(VaultKmsBackend::new(kv2_config(&address)).await.expect("build KV2 backend"));
    let transit = Arc::new(
        VaultTransitKmsBackend::new(transit_config(&address))
            .await
            .expect("build Transit backend"),
    );
    let context = HashMap::from([("live".to_string(), "vault-ha-failover".to_string())]);

    let kv2_key = format!("rustfs-ha-kv2-{}", uuid::Uuid::new_v4());
    kv2.create_key(CreateKeyRequest {
        key_name: Some(kv2_key.clone()),
        key_usage: KeyUsage::EncryptDecrypt,
        ..Default::default()
    })
    .await
    .expect("create KV2 key");
    let kv2_data_key = kv2
        .generate_data_key(GenerateDataKeyRequest {
            key_id: kv2_key,
            key_spec: KeySpec::Aes256,
            encryption_context: context.clone(),
        })
        .await
        .expect("generate KV2 data key");

    let transit_key = format!("rustfs-ha-transit-{}", uuid::Uuid::new_v4());
    transit
        .create_key(CreateKeyRequest {
            key_name: Some(transit_key.clone()),
            key_usage: KeyUsage::EncryptDecrypt,
            ..Default::default()
        })
        .await
        .expect("create Transit key");
    let transit_data_key = transit
        .generate_data_key(GenerateDataKeyRequest {
            key_id: transit_key,
            key_spec: KeySpec::Aes256,
            encryption_context: context.clone(),
        })
        .await
        .expect("generate Transit data key");

    let kv2_request = DecryptRequest {
        ciphertext: kv2_data_key.ciphertext_blob,
        encryption_context: context.clone(),
        grant_tokens: Vec::new(),
    };
    let transit_request = DecryptRequest {
        ciphertext: transit_data_key.ciphertext_blob,
        encryption_context: context,
        grant_tokens: Vec::new(),
    };

    for _ in 0..2 {
        let kv2_response = kv2
            .decrypt(kv2_request.clone())
            .await
            .expect("healthy KV2 decrypt before failover");
        assert!(
            kv2_response.plaintext == kv2_data_key.plaintext_key,
            "healthy KV2 decrypt returned unexpected plaintext"
        );
        let transit_response = transit
            .decrypt(transit_request.clone())
            .await
            .expect("healthy Transit decrypt before failover");
        assert!(
            transit_response.plaintext == transit_data_key.plaintext_key,
            "healthy Transit decrypt returned unexpected plaintext"
        );
    }
    let baseline = snapshotter.snapshot().into_vec();
    assert_eq!(
        retryable_failures(&baseline, "vault_kv2_read_key"),
        0,
        "healthy KV2 baseline must not retry"
    );
    assert_eq!(
        retryable_failures(&baseline, "vault_transit_decrypt"),
        0,
        "healthy Transit baseline must not retry"
    );

    let stop = CancellationToken::new();
    let allow_failover_errors = Arc::new(AtomicBool::new(false));
    let kv2_failure = Arc::new(Mutex::new(None));
    let transit_failure = Arc::new(Mutex::new(None));
    let kv2_completed = Arc::new(AtomicU64::new(0));
    let transit_completed = Arc::new(AtomicU64::new(0));
    let kv2_worker = tokio::spawn(decrypt_loop(
        Arc::clone(&kv2),
        kv2_request,
        kv2_data_key.plaintext_key,
        Arc::clone(&kv2_completed),
        Arc::clone(&allow_failover_errors),
        Arc::clone(&kv2_failure),
        stop.clone(),
    ));
    let transit_worker = tokio::spawn(decrypt_loop(
        Arc::clone(&transit),
        transit_request,
        transit_data_key.plaintext_key,
        Arc::clone(&transit_completed),
        Arc::clone(&allow_failover_errors),
        Arc::clone(&transit_failure),
        stop.clone(),
    ));

    wait_for_count(&kv2_completed, &kv2_failure, 2, "two healthy KV2 decrypts", HEALTHY_PROGRESS_TIMEOUT).await;
    wait_for_count(
        &transit_completed,
        &transit_failure,
        2,
        "two healthy Transit decrypts",
        HEALTHY_PROGRESS_TIMEOUT,
    )
    .await;
    allow_failover_errors.store(true, Ordering::SeqCst);
    std::fs::write(&marker, b"ready").expect("publish failover readiness marker");

    wait_for_file(&elected, "the replacement Vault leader").await;
    let new_leader = std::fs::read_to_string(&elected).expect("read replacement Vault leader marker");
    assert_ne!(new_leader.trim(), old_leader, "the killed active node cannot remain leader");

    let kv2_after_election = kv2_completed.load(Ordering::SeqCst) + 2;
    let transit_after_election = transit_completed.load(Ordering::SeqCst) + 2;
    wait_for_count(
        &kv2_completed,
        &kv2_failure,
        kv2_after_election,
        "post-failover KV2 decrypts",
        POST_FAILOVER_PROGRESS_TIMEOUT,
    )
    .await;
    wait_for_count(
        &transit_completed,
        &transit_failure,
        transit_after_election,
        "post-failover Transit decrypts",
        POST_FAILOVER_PROGRESS_TIMEOUT,
    )
    .await;

    stop.cancel();
    kv2_worker.await.expect("KV2 decrypt worker must join");
    transit_worker.await.expect("Transit decrypt worker must join");
    assert!(
        kv2_failure.lock().expect("KV2 failure lock poisoned").is_none(),
        "no KV2 decrypt may fail or return different plaintext"
    );
    assert!(
        transit_failure.lock().expect("Transit failure lock poisoned").is_none(),
        "no Transit decrypt may fail or return different plaintext"
    );
}

#[test]
#[ignore = "requires a real three-node Vault Raft cluster; run scripts/test/vault_ha_kms_live.sh"]
fn vault_raft_leader_failure_recovers_kv2_and_transit_decrypts() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::with_local_recorder(&recorder, || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("current-thread runtime must build")
            .block_on(exercise_failover(&snapshotter));
    });
    let snapshot = snapshotter.snapshot().into_vec();

    assert_eq!(
        counter_value(&snapshot, OPERATIONS_TOTAL, &[("outcome", "budget_exhausted")]),
        0,
        "leader failover must recover within the retry budget"
    );

    for (backend, operation) in [
        ("vault-kv2", "vault_kv2_read_key"),
        ("vault-transit", "vault_transit_decrypt"),
    ] {
        assert!(
            retryable_failures(&snapshot, operation) > 0,
            "{operation} must observe the killed leader as a retryable attempt failure"
        );
        let attempts = histogram_values(&snapshot, OPERATION_ATTEMPTS, &[("operation", operation), ("outcome", "success")]);
        assert!(!attempts.is_empty(), "{operation} must record successful attempts");
        assert!(
            attempts
                .iter()
                .all(|attempts| (1.0..=f64::from(MAX_ATTEMPTS)).contains(attempts)),
            "{operation} attempts must stay within the configured budget: {attempts:?}"
        );
        assert_eq!(
            gauge_value(&snapshot, IN_FLIGHT, &[("backend", backend), ("scope", "operations")]),
            Some(0.0),
            "{backend} must release every in-flight permit"
        );
        assert_eq!(
            gauge_value(&snapshot, CIRCUIT_OPEN, &[("backend", backend), ("scope", "operations")]),
            Some(0.0),
            "{backend} circuit must remain closed after recovery"
        );
    }
}
