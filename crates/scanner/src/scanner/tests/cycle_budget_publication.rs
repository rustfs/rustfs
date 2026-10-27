// Copyright 2026 RustFS Team
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

use super::*;
use crate::storage_api::scan::ScannerPublicationLeaseFailureInfo;
use rustfs_utils::path::path_join_buf;

#[derive(Clone)]
struct LeaseLogWriter(Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for LeaseLogWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("lease log mutex").extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn publication_lease_observations_keep_cycle_epoch_purpose_and_outcome() {
    use tracing::instrument::WithSubscriber as _;

    let output = Arc::new(std::sync::Mutex::new(Vec::new()));
    let writer = LeaseLogWriter(Arc::clone(&output));
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_max_level(tracing::Level::DEBUG)
        .with_writer(move || writer.clone())
        .with_ansi(false)
        .without_time()
        .finish();
    let _interest_guard = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());

    async {
        let ctx = CancellationToken::new();
        let budget = ScannerCycleBudget::new(&ctx, ScannerCycleBudgetConfig::default());
        let ready = ScannerCyclePublicationPlan::ProgressOnly
            .await_leases(&ctx, &budget, 501, 17, |_| async { Ok::<_, EcstoreError>(()) })
            .await;
        assert!(matches!(ready, ScannerPublicationProofWait::Ready(())));
        let next_leader = ScannerCyclePublicationPlan::Observational
            .await_leases(&ctx, &budget, 501, 22, |_| async { Ok::<_, EcstoreError>(()) })
            .await;
        assert!(matches!(next_leader, ScannerPublicationProofWait::Ready(())));
        let rejected = ScannerCyclePublicationPlan::Observational
            .await_leases(&ctx, &budget, 502, 18, |_| async {
                Err::<(), _>(EcstoreError::other("protocol mismatch"))
            })
            .await;
        assert!(matches!(rejected, ScannerPublicationProofWait::Rejected(_)));
        ctx.cancel();
        let cancelled = ScannerCyclePublicationPlan::Authoritative
            .await_leases(&ctx, &budget, 503, 19, |_| async { Ok::<_, EcstoreError>(()) })
            .await;
        assert!(matches!(cancelled, ScannerPublicationProofWait::Cancelled));
        let timeout_ctx = CancellationToken::new();
        let timeout_budget = ScannerCycleBudget::new(&timeout_ctx, ScannerCycleBudgetConfig::default());
        let timed_out = ScannerCyclePublicationPlan::ProgressOnly
            .await_leases(&timeout_ctx, &timeout_budget, 504, 20, |_| async {
                std::future::pending::<Result<(), EcstoreError>>().await
            })
            .await;
        assert!(matches!(timed_out, ScannerPublicationProofWait::Cancelled));
        let budget_ctx = CancellationToken::new();
        let stopped_budget = ScannerCycleBudget::new(&budget_ctx, ScannerCycleBudgetConfig::default());
        stopped_budget.cancel_for_runtime();
        let budget_cancelled = ScannerCyclePublicationPlan::Observational
            .await_leases(&budget_ctx, &stopped_budget, 505, 21, |_| async { Ok::<_, EcstoreError>(()) })
            .await;
        assert!(matches!(budget_cancelled, ScannerPublicationProofWait::Cancelled));
    }
    .with_subscriber(subscriber)
    .await;

    let bytes = output.lock().expect("lease log mutex").clone();
    let logs = String::from_utf8(bytes).expect("lease logs are UTF-8");
    let entries: Vec<serde_json::Value> = logs
        .lines()
        .map(|line| serde_json::from_str(line).expect("lease logs are JSON"))
        .collect();
    for (cycle, epoch, kind, purpose, outcome, parent_cancelled, proof_cancelled) in [
        (501, 17, "progress_only", "observational", "acquired", false, false),
        (502, 18, "observational", "observational", "rejected", false, false),
        (503, 19, "authoritative", "authoritative", "cancelled", true, true),
        (504, 20, "progress_only", "observational", "timed_out", false, false),
        (505, 21, "observational", "observational", "cancelled", false, true),
        (501, 22, "observational", "observational", "acquired", false, false),
    ] {
        let started: Vec<_> = entries
            .iter()
            .filter_map(|entry| entry.get("fields"))
            .filter(|fields| {
                fields["state"] == "publication_proof_started" && fields["cycle"] == cycle && fields["leader_epoch"] == epoch
            })
            .collect();
        assert_eq!(started.len(), 1, "missing or duplicated started event for cycle {cycle}: {logs}");
        assert_eq!(started[0]["leader_epoch"], epoch);
        assert_eq!(started[0]["publication_kind"], kind);
        assert_eq!(started[0]["lease_purpose"], purpose);
        let finished: Vec<_> = entries
            .iter()
            .filter_map(|entry| entry.get("fields"))
            .filter(|fields| {
                fields["state"] == "publication_lease_wait_finished"
                    && fields["cycle"] == cycle
                    && fields["leader_epoch"] == epoch
            })
            .collect();
        assert_eq!(finished.len(), 1, "missing or duplicated finished event for cycle {cycle}: {logs}");
        let fields = finished[0];
        assert_eq!(fields["leader_epoch"], epoch);
        assert_eq!(fields["publication_kind"], kind);
        assert_eq!(fields["lease_purpose"], purpose);
        assert_eq!(fields["outcome"], outcome);
        assert_eq!(fields["parent_cancelled_at_return"], parent_cancelled);
        assert_eq!(fields["proof_token_cancelled_at_return"], proof_cancelled);
    }
}

#[tokio::test(start_paused = true)]
async fn publication_lease_retry_logs_identify_interleaved_epochs() {
    use tracing::instrument::WithSubscriber as _;

    for level in [tracing::Level::WARN, tracing::Level::INFO, tracing::Level::DEBUG] {
        let output = Arc::new(std::sync::Mutex::new(Vec::new()));
        let writer = LeaseLogWriter(Arc::clone(&output));
        let subscriber = tracing_subscriber::fmt()
            .json()
            .flatten_event(true)
            .with_env_filter(format!("off,rustfs::scanner={level}"))
            .with_current_span(true)
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .without_time()
            .finish();
        let _interest_guard = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());

        exercise_interleaved_lease_waits().with_subscriber(subscriber).await;

        let bytes = output.lock().expect("lease log mutex").clone();
        let logs = String::from_utf8(bytes).expect("lease logs are UTF-8");
        assert!(!logs.contains("remote-secret"), "untrusted error text must not enter logs");
        assert!(!logs.contains("forged:9000"), "unstructured error text must not establish a peer");
        let entries: Vec<serde_json::Value> = logs
            .lines()
            .map(|line| serde_json::from_str(line).expect("lease logs are JSON"))
            .collect();
        let find_lease_event = |state: &str| {
            entries
                .iter()
                .position(|entry| entry["state"] == state && entry["stage"] == "lease_acquire")
                .unwrap_or_else(|| panic!("missing {state} at {level}: {logs}"))
        };
        let pending = find_lease_event("publication_proof_pending");
        let rejected = find_lease_event("publication_proof_rejected");
        assert!(pending < rejected, "waits must interleave: {logs}");
        for (index, epoch, purpose) in [(pending, 41, "observational"), (rejected, 42, "authoritative")] {
            assert_eq!(entries[index]["cycle"], 601);
            assert_eq!(entries[index]["leader_epoch"], epoch, "{logs}");
            assert_eq!(entries[index]["lease_purpose"], purpose, "{logs}");
        }
        assert_eq!(entries[rejected]["peer"], "peer-a:9000", "{logs}");
        assert_eq!(entries[rejected]["failure_kind"], "peer_unavailable", "{logs}");
        let ipv6 = entries
            .iter()
            .find(|entry| entry["state"] == "publication_proof_rejected" && entry["cycle"] == 604)
            .expect("IPv6 target failure should be logged");
        assert_eq!(ipv6["peer"], "[2001:db8::1]:9443", "{logs}");
        let secret_target = entries
            .iter()
            .find(|entry| entry["state"] == "publication_proof_rejected" && entry["cycle"] == 605)
            .expect("credential-bearing target failure should be logged");
        assert!(secret_target["peer"].is_null(), "credential-bearing target must not be logged");
        assert!(!logs.contains("target-secret"), "target credentials must not enter logs");
        let local = entries
            .iter()
            .find(|entry| entry["state"] == "publication_proof_rejected" && entry["cycle"] == 606)
            .expect("local diagnostic should be logged");
        assert_eq!(local["reason"], "invalid_proof");
        assert!(local["reported_reason"].is_null());
        let reported = entries
            .iter()
            .find(|entry| entry["state"] == "publication_proof_rejected" && entry["cycle"] == 607)
            .expect("remote report should be logged");
        assert!(reported["reason"].is_null());
        assert_eq!(reported["reported_reason"], "generation_stale");
        let timeout = entries
            .iter()
            .find(|entry| entry["state"] == "publication_proof_rejected" && entry["cycle"] == 608)
            .expect("channel deadline should be logged");
        assert_eq!(timeout["reason"], "channel_acquisition_timed_out");
        assert!(timeout["reported_reason"].is_null());
        assert!(!logs.contains("diagnostic-secret"), "error text must not enter logs");
        if level != tracing::Level::WARN {
            let recovered = find_lease_event("publication_proof_recovered");
            assert!(rejected < recovered, "waits must interleave: {logs}");
            assert_eq!(entries[recovered]["leader_epoch"], 41, "{logs}");
            assert_eq!(entries[recovered]["lease_purpose"], "observational", "{logs}");
        }
        if level == tracing::Level::DEBUG {
            let retry = find_lease_event("publication_proof_retry");
            let recovered = find_lease_event("publication_proof_recovered");
            assert!(rejected < retry && retry < recovered, "retry must remain interleaved: {logs}");
            assert_eq!(entries[retry]["leader_epoch"], 41, "{logs}");
            assert_eq!(entries[retry]["lease_purpose"], "observational", "{logs}");
        }
        let unknown = entries
            .iter()
            .find(|entry| entry["state"] == "publication_proof_rejected" && entry["stage"] == "postscan")
            .expect("activity rejection should be logged");
        assert!(unknown["leader_epoch"].is_null(), "unknown epoch must not be invented");
        assert!(unknown["lease_purpose"].is_null(), "unknown purpose must not be invented");
    }
}

async fn exercise_interleaved_lease_waits() {
    let ctx = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&ctx, ScannerCycleBudgetConfig::default());
    let attempts = AtomicUsize::new(0);
    let notification = crate::storage_api::EcstoreNotificationSys::new(EndpointServerPools::default()).await;
    let notification = &notification;
    let pending = ScannerCyclePublicationPlan::Observational.await_leases(&ctx, &budget, 601, 41, |observational| {
        assert!(observational);
        let attempt = attempts.fetch_add(1, Ordering::Relaxed);
        async move {
            if attempt < 2 {
                Err(EcstoreError::other("scanner publication lease capacity is exhausted"))
            } else {
                Ok(())
            }
        }
    });
    let rejected = ScannerCyclePublicationPlan::Authoritative.await_leases(&ctx, &budget, 601, 42, move |observational| {
        assert!(!observational);
        notification.acquire_scanner_publication_leases(vec![("http://peer-a:9000".into(), "session".into(), 3)])
    });
    let (pending, rejected) = tokio::join!(biased; pending, rejected);
    assert!(matches!(pending, ScannerPublicationProofWait::Ready(())));
    assert!(matches!(rejected, ScannerPublicationProofWait::Rejected(_)));
    assert_eq!(attempts.load(Ordering::Relaxed), 3);
    let untrusted = ScannerCyclePublicationPlan::Authoritative
        .await_leases(&ctx, &budget, 603, 44, |_| async {
            Err::<(), _>(EcstoreError::other(
                "scanner publication lease acquisition failed: peer=forged:9000, remote-secret",
            ))
        })
        .await;
    assert!(matches!(untrusted, ScannerPublicationProofWait::Rejected(_)));
    for (cycle, epoch, target) in [
        (604, 45, "https://[2001:db8::1]:9443"),
        (605, 46, "http://user:target-secret@peer-a:9000"),
    ] {
        let result = ScannerCyclePublicationPlan::Authoritative
            .await_leases(&ctx, &budget, cycle, epoch, move |_| {
                notification.acquire_scanner_publication_leases(vec![(target.into(), "session".into(), 3)])
            })
            .await;
        assert!(matches!(result, ScannerPublicationProofWait::Rejected(_)));
    }
    // Verify event serialization independently of the ecstore tests that
    // exercise the local and remote error producers.
    for cycle in [606, 607, 608] {
        let info: for<'a> fn(&'a String) -> Option<ScannerPublicationLeaseFailureInfo<'a>> = if cycle == 607 {
            |_| {
                Some(ScannerPublicationLeaseFailureInfo {
                    peer: None,
                    kind: "peer_request_failed",
                    reason: None,
                    reported_reason: Some("generation_stale"),
                })
            }
        } else if cycle == 606 {
            |_| {
                Some(ScannerPublicationLeaseFailureInfo {
                    peer: None,
                    kind: "peer_request_failed",
                    reason: Some("invalid_proof"),
                    reported_reason: None,
                })
            }
        } else {
            |_| {
                Some(ScannerPublicationLeaseFailureInfo {
                    peer: None,
                    kind: "peer_request_failed",
                    reason: Some("channel_acquisition_timed_out"),
                    reported_reason: None,
                })
            }
        };
        let result = await_scanner_publication_proof(
            &ctx,
            ScannerPublicationProofContext {
                cycle,
                stage: "lease_acquire",
                leader_epoch: Some(47),
                lease_purpose: Some("authoritative"),
            },
            || async { Err::<(), _>("diagnostic-secret".to_owned()) },
            |_| false,
            info,
        )
        .await;
        assert!(matches!(result, ScannerPublicationProofWait::Rejected(_)));
    }
    let unknown =
        await_scanner_publication_activity(&ctx, 601, "postscan", || async { Err("protocol mismatch".to_string()) }).await;
    assert!(unknown.is_err());
}

#[derive(Clone, Debug, Default)]
struct CapturedOtelLogs(Arc<std::sync::Mutex<Vec<opentelemetry_sdk::logs::SdkLogRecord>>>);

impl opentelemetry_sdk::logs::LogProcessor for CapturedOtelLogs {
    fn emit(&self, data: &mut opentelemetry_sdk::logs::SdkLogRecord, _: &opentelemetry::InstrumentationScope) {
        self.0.lock().expect("OTLP log mutex").push(data.clone());
    }

    fn force_flush(&self) -> opentelemetry_sdk::error::OTelSdkResult {
        Ok(())
    }

    fn shutdown_with_timeout(&self, _: std::time::Duration) -> opentelemetry_sdk::error::OTelSdkResult {
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn publication_lease_events_include_context_in_otlp_without_span_export() {
    use opentelemetry::logs::AnyValue;
    use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
    use tracing::{Instrument as _, instrument::WithSubscriber as _};
    use tracing_subscriber::prelude::*;

    for level in [tracing::Level::WARN, tracing::Level::INFO, tracing::Level::DEBUG] {
        let captured = CapturedOtelLogs::default();
        let provider = opentelemetry_sdk::logs::SdkLoggerProvider::builder()
            .with_log_processor(captured.clone())
            .build();
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new(format!("off,rustfs::scanner={level}")))
            .with(OpenTelemetryTracingBridge::new(&provider));
        let _interest_guard = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
        async {
            // The bridge is configured like production: no tracing exporter or
            // span attribute copying. A parent span must not supply these fields.
            let unrelated = tracing::warn_span!(
                target: "rustfs::scanner",
                "unrelated_parent",
                leader_epoch = 999,
                lease_purpose = "unrelated"
            );
            exercise_interleaved_lease_waits().instrument(unrelated).await;
        }
        .with_subscriber(subscriber)
        .await;

        let records = captured.0.lock().expect("OTLP log mutex").clone();
        let attribute = |record: &opentelemetry_sdk::logs::SdkLogRecord, name: &str| {
            record
                .attributes_iter()
                .find(|(key, _)| key.as_str() == name)
                .map(|(_, value)| value.clone())
        };
        let find = |state: &'static str, cycle: i64, stage: &'static str| {
            records
                .iter()
                .find(|record| {
                    attribute(record, "state") == Some(AnyValue::from(state))
                        && attribute(record, "cycle") == Some(AnyValue::Int(cycle))
                        && attribute(record, "stage") == Some(AnyValue::from(stage))
                })
                .unwrap_or_else(|| panic!("missing {state} at {level}"))
        };
        let pending = find("publication_proof_pending", 601, "lease_acquire");
        let rejected = find("publication_proof_rejected", 601, "lease_acquire");
        for (record, epoch, purpose) in [(pending, 41, "observational"), (rejected, 42, "authoritative")] {
            assert_eq!(attribute(record, "leader_epoch"), Some(AnyValue::Int(epoch)));
            assert_eq!(attribute(record, "lease_purpose"), Some(AnyValue::from(purpose)));
        }
        assert_eq!(attribute(rejected, "peer"), Some(AnyValue::from("peer-a:9000")));
        assert_eq!(attribute(rejected, "failure_kind"), Some(AnyValue::from("peer_unavailable")));
        let ipv6 = find("publication_proof_rejected", 604, "lease_acquire");
        assert_eq!(attribute(ipv6, "peer"), Some(AnyValue::from("[2001:db8::1]:9443")));
        let secret_target = find("publication_proof_rejected", 605, "lease_acquire");
        assert!(attribute(secret_target, "peer").is_none());
        let local = find("publication_proof_rejected", 606, "lease_acquire");
        assert_eq!(attribute(local, "reason"), Some(AnyValue::from("invalid_proof")));
        assert!(attribute(local, "reported_reason").is_none());
        let reported = find("publication_proof_rejected", 607, "lease_acquire");
        assert!(attribute(reported, "reason").is_none());
        assert_eq!(attribute(reported, "reported_reason"), Some(AnyValue::from("generation_stale")));
        let timeout = find("publication_proof_rejected", 608, "lease_acquire");
        assert_eq!(attribute(timeout, "reason"), Some(AnyValue::from("channel_acquisition_timed_out")));
        assert!(attribute(timeout, "reported_reason").is_none());
        if level != tracing::Level::WARN {
            let recovered = find("publication_proof_recovered", 601, "lease_acquire");
            assert_eq!(attribute(recovered, "leader_epoch"), Some(AnyValue::Int(41)));
            assert_eq!(attribute(recovered, "lease_purpose"), Some(AnyValue::from("observational")));
        }
        if level == tracing::Level::DEBUG {
            let retry = find("publication_proof_retry", 601, "lease_acquire");
            assert_eq!(attribute(retry, "leader_epoch"), Some(AnyValue::Int(41)));
            assert_eq!(attribute(retry, "lease_purpose"), Some(AnyValue::from("observational")));
        }
        let unknown = find("publication_proof_rejected", 601, "postscan");
        assert!(attribute(unknown, "leader_epoch").is_none());
        assert!(attribute(unknown, "lease_purpose").is_none());
        let untrusted = find("publication_proof_rejected", 603, "lease_acquire");
        assert!(attribute(untrusted, "peer").is_none());
        assert!(!format!("{records:?}").contains("remote-secret"));
        assert!(!format!("{records:?}").contains("forged:9000"));
        assert!(!format!("{records:?}").contains("target-secret"));
        assert!(!format!("{records:?}").contains("diagnostic-secret"));
    }
}

#[tokio::test]
async fn dropped_lease_wait_does_not_report_a_finished_outcome() {
    use tracing::instrument::WithSubscriber as _;

    let output = Arc::new(std::sync::Mutex::new(Vec::new()));
    let writer = LeaseLogWriter(Arc::clone(&output));
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_max_level(tracing::Level::DEBUG)
        .with_writer(move || writer.clone())
        .with_ansi(false)
        .without_time()
        .finish();
    let _interest_guard = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());

    async {
        let ctx = CancellationToken::new();
        let budget = ScannerCycleBudget::new(&ctx, ScannerCycleBudgetConfig::default());
        let entered = AtomicBool::new(false);
        {
            let proof = ScannerCyclePublicationPlan::Observational.await_leases(&ctx, &budget, 602, 43, |_| {
                entered.store(true, Ordering::Relaxed);
                std::future::pending::<Result<(), EcstoreError>>()
            });
            tokio::pin!(proof);
            assert!(futures::poll!(&mut proof).is_pending());
            assert!(entered.load(Ordering::Relaxed));
        }
        assert!(!ctx.is_cancelled());
        assert!(!budget.token().is_cancelled());
    }
    .with_subscriber(subscriber)
    .await;

    let bytes = output.lock().expect("lease log mutex").clone();
    let logs = String::from_utf8(bytes).expect("lease logs are UTF-8");
    let entries: Vec<serde_json::Value> = logs
        .lines()
        .map(|line| serde_json::from_str(line).expect("lease logs are JSON"))
        .collect();
    let started = entries
        .iter()
        .filter(|entry| entry["fields"]["state"] == "publication_proof_started" && entry["fields"]["cycle"] == 602)
        .count();
    let finished = entries
        .iter()
        .filter(|entry| entry["fields"]["state"] == "publication_lease_wait_finished" && entry["fields"]["cycle"] == 602)
        .count();
    assert_eq!(started, 1, "the wait was polled: {logs}");
    assert_eq!(finished, 0, "a dropped future has no observed outcome: {logs}");
}

#[test]
fn publication_lease_retries_local_channel_deadline_and_stops_on_cancel() {
    const CHILD_MARKER: &str = "RUSTFS_TEST_SCANNER_LEASE_DIAL_RETRY_CHILD";
    if std::env::var_os(CHILD_MARKER).is_none() {
        let module = module_path!()
            .strip_prefix(concat!(env!("CARGO_CRATE_NAME"), "::"))
            .unwrap_or(module_path!());
        let name = format!("{module}::publication_lease_retries_local_channel_deadline_and_stops_on_cancel");
        let output = std::process::Command::new(std::env::current_exe().expect("test executable"))
            .args(["--exact", &name, "--nocapture"])
            .env(CHILD_MARKER, "1")
            .env(rustfs_config::ENV_INTERNODE_CONNECT_TIMEOUT_SECS, "0")
            .env("NO_PROXY", "127.0.0.1,localhost")
            .env("no_proxy", "127.0.0.1,localhost")
            .output()
            .expect("channel deadline test subprocess");
        assert!(
            output.status.success() && String::from_utf8_lossy(&output.stdout).contains("1 passed"),
            "channel deadline subprocess failed: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .expect("test runtime");
    runtime.block_on(async {
        use crate::storage_api::scan::EcstorePeerRestClient;
        use rustfs_utils::net::XHost;

        async fn channel_deadline_error(
            notification: &crate::storage_api::EcstoreNotificationSys,
            target: &str,
        ) -> EcstoreError {
            let error = match notification
                .acquire_scanner_publication_leases(vec![(target.to_owned(), "session".into(), 3)])
                .await
            {
                Ok(_) => panic!("the stalled channel cannot grant a lease"),
                Err(error) => error,
            };
            let info = crate::storage_api::scan::scanner_publication_lease_failure_info(&error)
                .expect("local channel deadline must carry structured diagnostics");
            assert_eq!(info.kind, "peer_request_failed");
            assert_eq!(info.reason, Some("channel_acquisition_timed_out"));
            assert_eq!(info.reported_reason, None);
            assert!(!scanner_publication_lease_error_is_retryable(&error.to_string()));
            error
        }

        // The local endpoint never completes TLS or receives an RPC.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("local listener");
        let address = listener.local_addr().expect("listener address");
        let target = format!("https://{address}");
        let client = EcstorePeerRestClient::new(
            XHost {
                name: "127.0.0.1".to_owned(),
                port: address.port(),
                is_port_set: true,
            },
            target.clone(),
        );
        let mut notification = crate::storage_api::EcstoreNotificationSys::new(EndpointServerPools::default()).await;
        notification.peer_clients.push(Some(client));

        let ctx = CancellationToken::new();
        let budget = ScannerCycleBudget::new(&ctx, ScannerCycleBudgetConfig::default());
        let attempts = AtomicUsize::new(0);
        let started_at = Instant::now();
        let result = ScannerCyclePublicationPlan::Authoritative
            .await_leases(&ctx, &budget, 701, 51, |_| {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                let target = target.clone();
                let notification = &notification;
                async move {
                    if attempt > 0 {
                        return Ok(());
                    }
                    Err(channel_deadline_error(notification, &target).await)
                }
            })
            .await;
        assert!(matches!(result, ScannerPublicationProofWait::Ready(())));
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert!(started_at.elapsed() >= scanner_publication_proof_retry_delay(1));

        for detail in [
            "channel_acquisition_timed_out",
            "scanner publication lease acquisition failed: peer=peer-a:9000, purpose=authoritative: Io error: internode channel acquisition deadline elapsed",
        ] {
            let attempts = AtomicUsize::new(0);
            let result = ScannerCyclePublicationPlan::Authoritative
                .await_leases(&ctx, &budget, 702, 52, |_| {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    async move { Err::<(), _>(EcstoreError::other(detail)) }
                })
                .await;
            assert!(matches!(result, ScannerPublicationProofWait::Rejected(_)));
            assert_eq!(attempts.load(Ordering::SeqCst), 1, "remote text must not create a local diagnosis");
        }

        let cancel_ctx = CancellationToken::new();
        let cancel_budget = ScannerCycleBudget::new(&cancel_ctx, ScannerCycleBudgetConfig::default());
        let attempts = AtomicUsize::new(0);
        let started_at = Instant::now();
        let result = ScannerCyclePublicationPlan::Authoritative
            .await_leases(&cancel_ctx, &cancel_budget, 703, 53, |_| {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                let target = target.clone();
                let notification = &notification;
                let cancel_ctx = cancel_ctx.clone();
                async move {
                    assert_eq!(attempt, 0, "cancellation must stop the scheduled retry");
                    let error = channel_deadline_error(notification, &target).await;
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        cancel_ctx.cancel();
                    });
                    Err::<(), _>(error)
                }
            })
            .await;
        assert!(matches!(result, ScannerPublicationProofWait::Cancelled));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert!(started_at.elapsed() < scanner_publication_proof_retry_delay(1));

        let progress_ctx = CancellationToken::new();
        let progress_budget = ScannerCycleBudget::new(&progress_ctx, ScannerCycleBudgetConfig::default());
        let attempts = AtomicUsize::new(0);
        let started_at = Instant::now();
        let result = ScannerCyclePublicationPlan::ProgressOnly
            .await_leases(&progress_ctx, &progress_budget, 704, 54, |observational| {
                assert!(observational);
                attempts.fetch_add(1, Ordering::SeqCst);
                let target = target.clone();
                let notification = &notification;
                async move { Err::<(), _>(channel_deadline_error(notification, &target).await) }
            })
            .await;
        assert!(matches!(result, ScannerPublicationProofWait::Cancelled));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert!(started_at.elapsed() < scanner_publication_proof_retry_delay(1));
        drop(listener);
    });
}

#[tokio::test]
#[serial]
async fn exhausted_budget_still_proves_peer_movement_without_namespace_authority() {
    let (_temp_dir, store) = setup_scanner_cycle_store_with_pool_count(true, 2).await;
    let pending = ecstore_hold_namespace_commit(store.as_ref());
    let ctx = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&ctx, ScannerCycleBudgetConfig::default());
    budget.cancel_for_runtime();
    let generation = store.scanner_data_movement_generation();
    let result = ScannerCycleResult::new(ScannerCycleStatus::Incomplete, None).with_remote_publication_lease_targets(vec![(
        "peer".into(),
        "session".into(),
        generation,
    )]);
    let plan = ScannerCyclePublicationPlan::for_result(&result);
    let calls = AtomicUsize::new(0);

    let proof = plan
        .await_leases(&ctx, &budget, 352, 19, |observational| {
            calls.fetch_add(1, Ordering::Relaxed);
            store.acquire_scanner_publication_lease_with_purpose(generation, Duration::from_secs(60), observational)
        })
        .await;
    let ScannerPublicationProofWait::Ready((token, admitted_generation)) = proof else {
        panic!("a stopped walk must still prove peer movement while the leader context is alive");
    };
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert!(!ctx.is_cancelled());
    store
        .validate_scanner_publication_lease_with_purpose(token, admitted_generation, true)
        .await
        .expect("progress must retain a movement-only lease");
    assert!(
        store
            .validate_scanner_publication_lease_with_purpose(token, admitted_generation, false)
            .await
            .is_err(),
        "the lease must not authorize an authoritative snapshot"
    );
    assert!(store.release_scanner_publication_lease(token).await);

    let stale = plan
        .await_leases(&ctx, &budget, 352, 19, |observational| {
            store.acquire_scanner_publication_lease_with_purpose(generation + 1, Duration::from_secs(60), observational)
        })
        .await;
    assert!(matches!(stale, ScannerPublicationProofWait::Rejected(_)));
    drop(pending);

    store.pool_meta.write().await.pools[0].decommission = Some(crate::storage_api::owner::EcstorePoolDecommissionInfo {
        failed: true,
        ..Default::default()
    });
    assert!(store.scanner_data_movement_pause_status().await.paused);
    let current_generation = store.scanner_data_movement_generation();
    let moving = plan
        .await_leases(&ctx, &budget, 352, 19, |observational| {
            store.acquire_scanner_publication_lease_with_purpose(current_generation, Duration::from_secs(60), observational)
        })
        .await;
    assert!(
        matches!(moving, ScannerPublicationProofWait::Rejected(error) if error.to_string().contains("blocked by data movement"))
    );
}

#[tokio::test]
async fn emitted_updates_keep_budget_cancellation_and_publication_purpose() {
    for (status, expected_plan) in [
        (ScannerCycleStatus::Complete, ScannerCyclePublicationPlan::Authoritative),
        (ScannerCycleStatus::Incomplete, ScannerCyclePublicationPlan::Observational),
        (ScannerCycleStatus::Superseded, ScannerCyclePublicationPlan::Observational),
        (
            ScannerCycleStatus::Deferred(ScannerCycleDeferReason::ActivityBaselineUnavailable),
            ScannerCyclePublicationPlan::Observational,
        ),
        (
            ScannerCycleStatus::Deferred(ScannerCycleDeferReason::DataMovement),
            ScannerCyclePublicationPlan::Authoritative,
        ),
    ] {
        let result = ScannerCycleResult::new(status, None).with_usage_update_emitted(true);
        let plan = ScannerCyclePublicationPlan::for_result(&result);
        assert_eq!(plan, expected_plan);
        let ctx = CancellationToken::new();
        let budget = ScannerCycleBudget::new(&ctx, ScannerCycleBudgetConfig::default());
        let ready = plan
            .await_leases(
                &ctx,
                &budget,
                352,
                19,
                |observational| async move { Ok::<_, EcstoreError>(observational) },
            )
            .await;
        assert!(matches!(ready, ScannerPublicationProofWait::Ready(observational) if observational == plan.is_observational()));
        budget.cancel_for_runtime();
        let calls = AtomicUsize::new(0);
        let cancelled = plan
            .await_leases(&ctx, &budget, 352, 19, |_| {
                calls.fetch_add(1, Ordering::Relaxed);
                async { Ok::<(), EcstoreError>(()) }
            })
            .await;
        assert!(matches!(cancelled, ScannerPublicationProofWait::Cancelled));
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }
}

#[tokio::test]
async fn progress_proof_honors_parent_cancellation() {
    let ctx = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&ctx, ScannerCycleBudgetConfig::default());
    ctx.cancel();
    let calls = AtomicUsize::new(0);
    let proof = ScannerCyclePublicationPlan::ProgressOnly
        .await_leases(&ctx, &budget, 352, 19, |_| {
            calls.fetch_add(1, Ordering::Relaxed);
            async { Ok::<_, EcstoreError>(()) }
        })
        .await;
    assert!(matches!(proof, ScannerPublicationProofWait::Cancelled));
    assert_eq!(calls.load(Ordering::Relaxed), 0);
}

#[tokio::test(start_paused = true)]
async fn progress_proof_is_bounded_without_a_runtime_budget() {
    for stalled in [false, true] {
        let ctx = CancellationToken::new();
        let budget = ScannerCycleBudget::new(
            &ctx,
            ScannerCycleBudgetConfig {
                max_objects: Some(1),
                ..Default::default()
            },
        );
        budget.record_object_scanned();
        assert!(budget.budget_elapsed());
        let calls = AtomicUsize::new(0);
        let started = Instant::now();
        let proof = ScannerCyclePublicationPlan::ProgressOnly
            .await_leases(&ctx, &budget, 352, 19, |_| {
                calls.fetch_add(1, Ordering::Relaxed);
                async move {
                    if stalled {
                        std::future::pending::<()>().await;
                    }
                    Err::<(), _>(EcstoreError::other("scanner publication lease capacity is exhausted"))
                }
            })
            .await;
        assert!(matches!(proof, ScannerPublicationProofWait::Cancelled));
        assert!(calls.load(Ordering::Relaxed) > 0);
        assert_eq!(started.elapsed(), SCANNER_LOCK_LOSS_SHUTDOWN_TIMEOUT);
        assert!(!ctx.is_cancelled());
    }
}

#[tokio::test]
#[serial]
async fn coordinator_persists_budget_progress_without_usage_or_dirty_acknowledgement() {
    assert_coordinator_partial_progress(false, None, false).await;
}

#[tokio::test]
#[serial]
async fn coordinator_preserves_checkpoint_when_bucket_identity_is_unavailable() {
    assert_coordinator_partial_progress(false, None, true).await;
}

#[tokio::test]
#[serial]
async fn coordinator_persists_mixed_plan_tail_without_usage_or_dirty_acknowledgement() {
    assert_coordinator_partial_progress(true, None, false).await;
}

#[tokio::test]
#[serial]
async fn coordinator_partial_progress_requires_successful_cycle_state_save() {
    for after_commit in [false, true] {
        assert_coordinator_partial_progress(true, Some(after_commit), false).await;
    }
}

async fn assert_coordinator_partial_progress(resume_mixed_plan: bool, fail_cycle_save: Option<bool>, block_identity_read: bool) {
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    let (_temp_dir, store) = setup_scanner_cycle_store().await;
    let bucket = "scanner-budget-progress";
    store
        .make_bucket(bucket, &crate::storage_api::scan::MakeBucketOptions::default())
        .await
        .expect("fixture bucket");
    for object in ["first", "second", "third"] {
        store.pools[0].disk_set[0]
            .put_object(
                bucket,
                object,
                &mut PutObjReader::from_vec(b"value".to_vec()),
                &ObjectOptions {
                    write_completion: WriteCompletion::TailDrained,
                    ..Default::default()
                },
            )
            .await
            .expect("fixture object");
    }
    crate::scanner_io::record_dirty_usage_bucket(bucket);
    let dirty_before = crate::scanner_io::dirty_usage_buckets_for_tests();
    let baseline = read_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
        .await
        .expect("usage baseline");
    let ctx = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(
        &ctx,
        ScannerCycleBudgetConfig {
            max_objects: Some(1),
            ..Default::default()
        },
    );
    let mut cycle = CurrentCycle {
        next: 352,
        ..Default::default()
    };
    let mut revision = DataUsageCacheRevision::Missing;
    let outcome = run_data_scanner_cycle_with_budget(
        &ctx,
        &store,
        &mut cycle,
        &mut revision,
        1,
        Arc::clone(&budget),
        ScannerCycleScheduling {
            requires_full_scan: true,
            service_cohort: None,
            scope_fallback: None,
        },
    )
    .await;
    assert!(matches!(outcome, ScannerCycleOutcome::Partial));
    assert_eq!(budget.reason(), Some(ScannerCycleBudgetReason::Objects));
    assert!(budget.progress().0 > 0);
    assert!(budget.cycle_state_persisted());
    assert_eq!(cycle.next, 353);
    assert!(cycle.cycle_completed.is_empty());
    assert!(matches!(revision, DataUsageCacheRevision::Etag(_)));
    let state = read_config(store.clone(), &DATA_USAGE_BLOOM_NAME_PATH)
        .await
        .expect("durable cycle progress");
    let (persisted, epoch) = decode_scanner_cycle_state(&state).expect("valid cycle state");
    assert_eq!(persisted.next, 353);
    assert_eq!(epoch, 1);
    assert_eq!(
        read_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
            .await
            .expect("unchanged root"),
        baseline
    );
    assert!(
        read_config(store.clone(), DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str())
            .await
            .is_err()
    );
    assert_eq!(crate::scanner_io::dirty_usage_buckets_for_tests(), dirty_before);

    if block_identity_read {
        let set = store.pools[0].disk_set[0].clone();
        let cache_name = path_join_buf(&[bucket, DATA_USAGE_CACHE_NAME]);
        let mut checkpoint = DataUsageCache::default();
        let revisions = checkpoint
            .load_with_revisions(set.clone(), &cache_name)
            .await
            .expect("load the saved checkpoint before blocking its identity read");
        assert!(checkpoint.info.scan_identity.is_some());
        assert!(checkpoint.has_scan_position());
        let before = serde_json::to_value(&checkpoint).expect("serialize the existing checkpoint");
        let identity_key = path_join_buf(&[crate::BUCKET_META_PREFIX, bucket, ".bucket-incarnation"]);
        let lock = store
            .new_ns_lock(RUSTFS_META_BUCKET, &identity_key)
            .await
            .expect("bucket identity object lock");
        let guard = lock
            .get_write_lock(Duration::from_secs(30))
            .await
            .expect("hold the identity sidecar unavailable to scanner reads");
        let blocked_budget = ScannerCycleBudget::new_with_progress_tracking(
            &ctx,
            ScannerCycleBudgetConfig {
                max_objects: Some(1),
                ..Default::default()
            },
        );
        let blocked_outcome = tokio::time::timeout(
            Duration::from_secs(60),
            run_data_scanner_cycle_with_budget(
                &ctx,
                &store,
                &mut cycle,
                &mut revision,
                1,
                Arc::clone(&blocked_budget),
                ScannerCycleScheduling {
                    requires_full_scan: true,
                    service_cohort: None,
                    scope_fallback: None,
                },
            ),
        )
        .await
        .expect("blocked identity reads must finish within the existing acquisition bounds");
        assert_eq!(
            blocked_outcome,
            ScannerCycleOutcome::Failed,
            "no verified bucket work must not publish a cycle"
        );
        assert_eq!(blocked_budget.progress().0, 0, "unverified identity must not start a legacy rebuild");
        let after_revisions = checkpoint
            .load_with_revisions(set, &cache_name)
            .await
            .expect("reload the retained checkpoint while only the identity object is locked");
        assert_eq!(after_revisions, revisions, "unavailable identity must not overwrite saved progress");
        assert_eq!(serde_json::to_value(&checkpoint).expect("serialize retained checkpoint"), before);
        assert_eq!(crate::scanner_io::dirty_usage_buckets_for_tests(), dirty_before);
        drop(guard);

        let recovery_budget = ScannerCycleBudget::new_with_progress_tracking(
            &ctx,
            ScannerCycleBudgetConfig {
                max_objects: Some(1),
                ..Default::default()
            },
        );
        let recovered = run_data_scanner_cycle_with_budget(
            &ctx,
            &store,
            &mut cycle,
            &mut revision,
            1,
            Arc::clone(&recovery_budget),
            ScannerCycleScheduling {
                requires_full_scan: true,
                service_cohort: None,
                scope_fallback: None,
            },
        )
        .await;
        assert_eq!(
            recovered,
            ScannerCycleOutcome::Partial,
            "the same checkpoint must resume after identity reads recover"
        );
        assert_eq!(recovery_budget.progress().0, 1);
        checkpoint
            .load(store.pools[0].disk_set[0].clone(), &cache_name)
            .await
            .expect("reload resumed progress");
        assert!(checkpoint.info.scan_identity.is_some());
        assert!(checkpoint.has_scan_position());
        assert_eq!(crate::scanner_io::dirty_usage_buckets_for_tests(), dirty_before);
    }

    if resume_mixed_plan {
        let set = store.pools[0].disk_set[0].clone();
        let cache_name = path_join_buf(&[bucket, DATA_USAGE_CACHE_NAME]);
        let mut checkpoint = DataUsageCache::default();
        let revisions = checkpoint
            .load_with_revisions(set.clone(), &cache_name)
            .await
            .expect("load the bounded sweep's durable checkpoint");
        assert!(
            checkpoint.info.scan_checkpoint.is_some()
                || checkpoint.info.scan_resume_after.is_some()
                || checkpoint.info.scan_raw_enumeration_cursor.is_some()
                || checkpoint.info.scan_raw_enumeration_page_index.is_some()
                || checkpoint.info.scan_coverage_receipt.is_some(),
            "bounded sweep must retain a durable position: {:?}",
            checkpoint.info
        );
        let progress = checkpoint.info.scan_progress.as_mut().expect("bounded sweep has a plan");
        // A restarted process requests a new plan while retaining the old prefix.
        progress.started_plan.0[0] ^= 1;
        let epoch = checkpoint.info.scan_identity.expect("checkpoint identity").publication_epoch;
        checkpoint
            .save_with_revisions_for_epoch(set.clone(), &cache_name, &revisions, epoch)
            .await
            .expect("persist a prefix from a different process plan");

        let tail_budget = ScannerCycleBudget::new_with_progress_tracking(&ctx, ScannerCycleBudgetConfig::default());
        // A committed-then-failed write is confirmed from durable state by the
        // save itself, so only an uncommitted fault that outlives every bounded
        // retry makes the cycle-state save fail.
        let failure_guard = fail_cycle_save.map(|after_commit| {
            if after_commit {
                super::cycle_persist_failure::fail_next_save(1, true)
            } else {
                super::cycle_persist_failure::fail_saves(1, false, SCANNER_PERSIST_CAS_RETRIES + 1)
            }
        });
        let tail_outcome = run_data_scanner_cycle_with_budget(
            &ctx,
            &store,
            &mut cycle,
            &mut revision,
            1,
            Arc::clone(&tail_budget),
            ScannerCycleScheduling {
                requires_full_scan: true,
                service_cohort: None,
                scope_fallback: None,
            },
        )
        .await;
        assert!(!tail_budget.budget_elapsed(), "this is not the budget-exhaustion fast path");
        checkpoint.load(set, &cache_name).await.expect("reload the saved mixed tail");
        assert!(!checkpoint.info.snapshot_complete);
        assert!(checkpoint.info.scan_checkpoint.is_none());
        assert!(checkpoint.info.scan_resume_after.is_none());
        let progress = checkpoint
            .info
            .scan_progress
            .expect("a fresh verification sweep is still required");
        assert_eq!(progress.started_plan, progress.requested_plan);
        assert_eq!(
            read_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
                .await
                .expect("root retained"),
            baseline
        );
        assert!(
            read_config(store.clone(), DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str())
                .await
                .is_err()
        );
        assert_eq!(crate::scanner_io::dirty_usage_buckets_for_tests(), dirty_before);
        if let Some(after_commit) = fail_cycle_save {
            assert!(super::cycle_persist_failure::take_failure(1).is_none(), "injected fault must have fired");
            let state = read_config(store, &DATA_USAGE_BLOOM_NAME_PATH)
                .await
                .expect("last durable cycle");
            let durable_next = decode_scanner_cycle_state(&state)
                .expect("cycle after failed acknowledgement")
                .0
                .next;
            if after_commit {
                // The write committed before the error surfaced; the save
                // confirms it from durable state under the same epoch.
                assert_eq!(tail_outcome, ScannerCycleOutcome::Partial);
                assert!(tail_budget.cycle_state_persisted());
                assert_eq!(durable_next, 354);
            } else {
                // Every bounded retry failed: progress stays unacknowledged
                // and the durable cycle does not move.
                assert_eq!(tail_outcome, ScannerCycleOutcome::StatePersistenceFailed);
                assert!(!tail_budget.cycle_state_persisted());
                assert_eq!(durable_next, 353);
            }
            drop(failure_guard);
            global_metrics().set_cycle(None).await;
            crate::scanner_io::clear_dirty_usage_buckets_for_tests();
            return;
        }
        assert_eq!(
            tail_outcome,
            ScannerCycleOutcome::Partial,
            "saved recovery progress is not a usage-save failure"
        );
        assert_eq!(cycle.next, 354);
        assert!(cycle.cycle_completed.is_empty());
        assert!(tail_budget.cycle_state_persisted());
        let state = read_config(store.clone(), &DATA_USAGE_BLOOM_NAME_PATH)
            .await
            .expect("durable tail cycle");
        assert_eq!(decode_scanner_cycle_state(&state).expect("tail cycle state").0.next, 354);

        let verification = run_data_scanner_cycle_with_budget(
            &ctx,
            &store,
            &mut cycle,
            &mut revision,
            1,
            ScannerCycleBudget::new_with_progress_tracking(&ctx, ScannerCycleBudgetConfig::default()),
            ScannerCycleScheduling {
                requires_full_scan: true,
                service_cohort: None,
                scope_fallback: None,
            },
        )
        .await;
        assert_eq!(verification, ScannerCycleOutcome::Completed, "only the fresh sweep certifies usage");
        let usage: DataUsageInfo = serde_json::from_slice(
            &read_config(store, DATA_USAGE_OBJ_NAME_PATH.as_str())
                .await
                .expect("verified root"),
        )
        .expect("verified usage JSON");
        assert_eq!((usage.objects_total_count, usage.objects_total_size), (3, 15));
        assert!(!crate::scanner_io::dirty_usage_buckets_pending());
    }
    global_metrics().set_cycle(None).await;
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
}

#[test]
fn partial_progress_does_not_mask_missing_updates_or_persistence_failure() {
    for (persisted, expected) in [
        (DataUsagePersistOutcome::NoUpdate, ScannerCycleOutcome::Partial),
        (DataUsagePersistOutcome::Failed, ScannerCycleOutcome::Failed),
        (DataUsagePersistOutcome::Current, ScannerCycleOutcome::Failed),
        (
            DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::DataMovement),
            ScannerCycleOutcome::Deferred(ScannerCycleDeferReason::DataMovement),
        ),
    ] {
        let result = ScannerCycleResult::new(ScannerCycleStatus::Incomplete, None).with_partial_bucket_progress(true);
        assert_eq!(scanner_cycle_completion_outcome_for_result(&result, persisted), expected);
    }
    for result in [
        ScannerCycleResult::new(ScannerCycleStatus::Incomplete, None),
        ScannerCycleResult::new(ScannerCycleStatus::Incomplete, None)
            .with_partial_bucket_progress(true)
            .with_usage_update_emitted(true),
        ScannerCycleResult::new(ScannerCycleStatus::Complete, None).with_partial_bucket_progress(true),
    ] {
        assert_eq!(
            scanner_cycle_completion_outcome_for_result(&result, DataUsagePersistOutcome::NoUpdate),
            ScannerCycleOutcome::Failed,
            "partial progress cannot stand in for an expected root update: {result:?}"
        );
    }
}
