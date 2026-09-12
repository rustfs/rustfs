use serial_test::serial;
use std::io::{Cursor, Read as _};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt as _;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64_simd::URL_SAFE_NO_PAD;
use p256::ecdsa::signature::Verifier as _;
use p256::ecdsa::{Signature, VerifyingKey};
use p256::pkcs8::DecodePublicKey as _;
use rustfs::connect::{
    DeviceIdentity, LocalTelemetryConsent, ObservedTelemetrySpan, TelemetryArtifactConsent, TelemetryArtifactError,
    TelemetryArtifactRequest, TelemetryDiagnosticResult, TelemetryOperation, TelemetryProducerError, TelemetryProvenance,
    TelemetrySpanStatus, TelemetryTool, TraceRecordCompletion, TraceRecordLimits, encode_signed_telemetry_export,
    record_diagnostic_result, record_trace, record_trace_bus, save_signed_telemetry_export,
};
use rustfs_common::trace_bus::{TraceEvent, TraceFunc, TraceKind, trace_emit};
use sha2::{Digest as _, Sha256};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use zip::ZipArchive;

fn consent() -> LocalTelemetryConsent {
    LocalTelemetryConsent::new(Instant::now() + Duration::from_secs(5)).expect("future consent")
}

fn artifact_request() -> TelemetryArtifactRequest {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).expect("current time").as_secs() as i64;
    let organization = "organizations/019e3ae0-0000-7000-8000-000000000001";
    let cluster = format!("{organization}/clusters/019e3ae0-0000-7000-8000-000000000002");
    TelemetryArtifactRequest {
        organization_name: organization.to_owned(),
        cluster_name: cluster.clone(),
        device_name: format!("{cluster}/clusterDevices/019e3ae0-0000-7000-8000-000000000003"),
        run_uid: "019e3ae0-0000-7000-8000-000000000004".to_owned(),
        artifact_uid: "019e3ae0-0000-7000-8000-000000000005".to_owned(),
        schema_version: 1,
        consent: TelemetryArtifactConsent {
            consent_uid: "019e3ae0-0000-7000-8000-000000000006".to_owned(),
            policy_revision: 1,
            expires_at_unix: now + 120,
            confirmed: true,
        },
        produced_at_unix: now,
        expires_at_unix: now + 60,
        nonce: [0x5a; 32],
        provenance: TelemetryProvenance::new("a".repeat(40), "b".repeat(64), "1.0.0-rc.6", vec![]),
    }
}

fn archive_entry(archive: &mut ZipArchive<Cursor<Vec<u8>>>, name: &str) -> Vec<u8> {
    let mut entry = archive.by_name(name).expect("archive entry");
    let mut bytes = Vec::new();
    entry.read_to_end(&mut bytes).expect("read archive entry");
    bytes
}

#[tokio::test]
#[serial]
async fn connect_trace_record_emits_only_the_frozen_redacted_shape() {
    let (sender, receiver) = mpsc::channel(4);
    sender
        .send(ObservedTelemetrySpan::new(
            TelemetryOperation::GetObject,
            Duration::from_micros(500),
            TelemetrySpanStatus::Ok,
        ))
        .await
        .expect("open source");
    let task = tokio::spawn(async move {
        record_trace(
            receiver,
            consent(),
            TraceRecordLimits {
                duration: Duration::from_millis(15),
                max_spans: 4,
            },
            &CancellationToken::new(),
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    let record = task.await.expect("capture task").expect("capture succeeds");
    assert_eq!(record.completion, TraceRecordCompletion::Complete);
    let json = serde_json::to_value(&record.data).expect("serialize record");

    assert_eq!(json["spans"][0]["operation"], "GET_OBJECT");
    assert_eq!(json["spans"][0]["durationMicros"], 500);
    assert_eq!(json["spans"][0]["status"], "OK");
    assert_eq!(json["droppedSpanCount"], 0);
    let mut root_keys = json
        .as_object()
        .expect("record object")
        .keys()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let mut span_keys = json["spans"][0]
        .as_object()
        .expect("span object")
        .keys()
        .map(String::as_str)
        .collect::<Vec<_>>();
    root_keys.sort_unstable();
    span_keys.sort_unstable();
    assert_eq!(root_keys, ["droppedSpanCount", "spans"]);
    assert_eq!(span_keys, ["durationMicros", "operation", "status"]);
}

#[tokio::test]
#[serial]
async fn connect_trace_record_stops_at_span_limit_and_counts_queued_drops() {
    let (sender, receiver) = mpsc::channel(8);
    for _ in 0..3 {
        sender
            .send(ObservedTelemetrySpan::new(
                TelemetryOperation::InternalRpc,
                Duration::from_micros(1),
                TelemetrySpanStatus::Error,
            ))
            .await
            .expect("open source");
    }
    let record = record_trace(
        receiver,
        consent(),
        TraceRecordLimits {
            duration: Duration::from_secs(1),
            max_spans: 1,
        },
        &CancellationToken::new(),
    )
    .await
    .expect("bounded capture succeeds");

    assert_eq!(record.data.spans.len(), 1);
    assert_eq!(record.data.dropped_span_count, 2);
    assert_eq!(record.completion, TraceRecordCompletion::LimitExceeded);
}

#[tokio::test]
#[serial]
async fn connect_trace_record_marks_a_closed_source_incomplete_after_real_observations() {
    let (sender, receiver) = mpsc::channel(1);
    sender
        .send(ObservedTelemetrySpan::new(
            TelemetryOperation::HeadObject,
            Duration::from_micros(2),
            TelemetrySpanStatus::Ok,
        ))
        .await
        .expect("open source");
    drop(sender);
    let record = record_trace(
        receiver,
        consent(),
        TraceRecordLimits {
            duration: Duration::from_secs(1),
            max_spans: 2,
        },
        &CancellationToken::new(),
    )
    .await
    .expect("observed data remains usable");

    assert_eq!(record.data.spans.len(), 1);
    assert_eq!(record.completion, TraceRecordCompletion::SourceUnavailable);
}

#[tokio::test]
#[serial]
async fn connect_trace_record_rejects_limits_and_honors_stop() {
    let (_sender, receiver) = mpsc::channel(1);
    let error = record_trace(
        receiver,
        consent(),
        TraceRecordLimits {
            duration: Duration::from_secs(31),
            max_spans: 1,
        },
        &CancellationToken::new(),
    )
    .await
    .expect_err("overlong capture must fail");
    assert_eq!(error, TelemetryProducerError::InvalidDuration);

    let (_sender, receiver) = mpsc::channel(1);
    let cancel = CancellationToken::new();
    cancel.cancel();
    let error = record_trace(
        receiver,
        consent(),
        TraceRecordLimits {
            duration: Duration::from_secs(1),
            max_spans: 1,
        },
        &cancel,
    )
    .await
    .expect_err("cancelled capture must fail");
    assert_eq!(error, TelemetryProducerError::Cancelled);
}

#[test]
#[serial]
fn connect_trace_record_requires_live_local_consent() {
    assert_eq!(
        LocalTelemetryConsent::new(Instant::now()).expect_err("expired consent must fail"),
        TelemetryProducerError::ConsentExpired
    );
}

#[test]
#[serial]
fn connect_trace_record_enforces_exact_duration_and_sample_boundaries() {
    assert!(
        TraceRecordLimits {
            duration: Duration::from_secs(30),
            max_spans: 1024,
        }
        .validate()
        .is_ok()
    );
    assert_eq!(
        TraceRecordLimits {
            duration: Duration::from_secs(30) + Duration::from_nanos(1),
            max_spans: 1024,
        }
        .validate()
        .expect_err("duration N+1 must fail"),
        TelemetryProducerError::InvalidDuration
    );
    assert_eq!(
        TraceRecordLimits {
            duration: Duration::from_secs(30),
            max_spans: 1025,
        }
        .validate()
        .expect_err("sample N+1 must fail"),
        TelemetryProducerError::InvalidSpanLimit
    );
}

#[tokio::test]
#[serial]
async fn connect_trace_record_refuses_to_relabel_the_real_heal_bus_as_frozen_telemetry() {
    let task = tokio::spawn(async {
        record_trace_bus(
            consent(),
            TraceRecordLimits {
                duration: Duration::from_millis(80),
                max_spans: 8,
            },
            &CancellationToken::new(),
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(trace_emit(|| {
        TraceEvent::new(TraceKind::Scanner, TraceFunc::ScannerHealCandidate)
            .with_bucket("SYNTHETIC_SECRET_BUCKET")
            .with_object("private/object")
            .with_duration(Duration::from_micros(41))
            .with_attr("error", "SYNTHETIC_SECRET_ERROR")
    }));
    let error = task
        .await
        .expect("capture task")
        .expect_err("heal/scanner events have no frozen telemetry semantics");
    assert_eq!(error, TelemetryProducerError::SourceUnavailable);
}

#[test]
#[serial]
fn connect_trace_record_writes_a_verified_no_clobber_signed_archive() {
    let request = artifact_request();
    let key = DeviceIdentity::generate();
    let result = record_diagnostic_result(
        &request,
        rustfs::connect::TraceRecordCapture {
            data: rustfs::connect::RecordedTrace {
                spans: vec![rustfs::connect::TelemetrySpan {
                    operation: TelemetryOperation::GetObject,
                    duration_micros: 500,
                    status: TelemetrySpanStatus::Ok,
                }],
                dropped_span_count: 0,
            },
            completion: TraceRecordCompletion::Complete,
        },
        Duration::from_millis(1),
    );
    let export =
        encode_signed_telemetry_export(&request, &result, &key, &CancellationToken::new()).expect("signed telemetry export");
    let mut archive = ZipArchive::new(Cursor::new(export.archive_bytes.clone())).expect("telemetry archive");
    assert_eq!(archive.len(), 3);
    let envelope_bytes = archive_entry(&mut archive, "envelope.json");
    let signature_bytes = archive_entry(&mut archive, "envelope.sig");
    let result_bytes = archive_entry(&mut archive, "result.json");
    let envelope: serde_json::Value = serde_json::from_slice(&envelope_bytes).expect("envelope JSON");
    let signature: serde_json::Value = serde_json::from_slice(&signature_bytes).expect("signature JSON");
    let result_json: serde_json::Value = serde_json::from_slice(&result_bytes).expect("result JSON");
    assert_eq!(envelope["formatVersion"], "rustfs.connect.diagnosticEnvelope/1");
    assert_eq!(envelope["toolId"], "telemetry.record");
    assert_eq!(envelope["classification"], "L3");
    assert_eq!(
        envelope["payload"]["sha256"],
        hex_simd::encode_to_string(Sha256::digest(&result_bytes), hex_simd::AsciiCase::Lower)
    );
    assert_eq!(result_json["toolId"], "telemetry.record");
    assert_eq!(result_json["capability"], "telemetry.record@1");
    assert_eq!(result_json["outcome"], "SUCCEEDED");
    assert_eq!(result_json["provenance"]["sourceCommit"], "a".repeat(40));

    let raw_signature = URL_SAFE_NO_PAD
        .decode_to_vec(signature["value"].as_str().expect("signature value"))
        .expect("base64url signature");
    let signature = Signature::from_slice(&raw_signature).expect("P-256 signature");
    let public = VerifyingKey::from_public_key_der(&key.public_key_der()).expect("public key");
    let mut input = b"rustfs-diagnostic-envelope-v1\0".to_vec();
    input.extend_from_slice(&envelope_bytes);
    public.verify(&input, &signature).expect("valid envelope signature");

    let directory = tempfile::tempdir().expect("temporary output");
    let output = directory.path().join("telemetry.zip");
    save_signed_telemetry_export(&output, &export, &CancellationToken::new()).expect("saved telemetry export");
    #[cfg(unix)]
    assert_eq!(std::fs::metadata(&output).expect("output metadata").permissions().mode() & 0o777, 0o600);
    assert!(matches!(
        save_signed_telemetry_export(&output, &export, &CancellationToken::new()),
        Err(TelemetryArtifactError::AlreadyExists)
    ));
}

#[test]
#[serial]
fn connect_trace_record_rejects_tampered_or_cancelled_archives_without_a_file() {
    let request = artifact_request();
    let key = DeviceIdentity::generate();
    let result = TelemetryDiagnosticResult::succeeded(
        &request,
        TelemetryTool::Record,
        Duration::ZERO,
        rustfs::connect::RecordedTrace {
            spans: vec![],
            dropped_span_count: 0,
        },
    );
    let export =
        encode_signed_telemetry_export(&request, &result, &key, &CancellationToken::new()).expect("signed telemetry export");
    let directory = tempfile::tempdir().expect("temporary output");

    let tampered_output = directory.path().join("tampered.zip");
    let mut tampered = export.clone();
    tampered.archive_bytes.push(0);
    assert!(matches!(
        save_signed_telemetry_export(&tampered_output, &tampered, &CancellationToken::new()),
        Err(TelemetryArtifactError::InvalidRequest)
    ));
    assert!(!tampered_output.exists());

    let cancelled_output = directory.path().join("cancelled.zip");
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(matches!(
        save_signed_telemetry_export(&cancelled_output, &export, &cancel),
        Err(TelemetryArtifactError::Cancelled)
    ));
    assert!(!cancelled_output.exists());
}

#[test]
#[serial]
fn connect_trace_record_never_publishes_an_unsupported_artifact() {
    let request = artifact_request();
    let key = DeviceIdentity::generate();
    let result =
        TelemetryDiagnosticResult::<rustfs::connect::RecordedTrace>::unsupported(&request, TelemetryTool::Record, Duration::ZERO);
    assert!(matches!(
        encode_signed_telemetry_export(&request, &result, &key, &CancellationToken::new()),
        Err(TelemetryArtifactError::InvalidRequest)
    ));
}

#[test]
#[serial]
fn connect_trace_record_never_publishes_an_incomplete_single_window() {
    let request = artifact_request();
    let key = DeviceIdentity::generate();
    for (completion, expected_reason) in [
        (TraceRecordCompletion::LimitExceeded, "COLLECTION_FAILED"),
        (TraceRecordCompletion::SourceUnavailable, "SOURCE_UNAVAILABLE"),
    ] {
        let result = record_diagnostic_result(
            &request,
            rustfs::connect::TraceRecordCapture {
                data: rustfs::connect::RecordedTrace {
                    spans: vec![rustfs::connect::TelemetrySpan {
                        operation: TelemetryOperation::GetObject,
                        duration_micros: 500,
                        status: TelemetrySpanStatus::Ok,
                    }],
                    dropped_span_count: 1,
                },
                completion,
            },
            Duration::from_millis(1),
        );
        let serialized = serde_json::to_value(&result).expect("failed diagnostic result");
        assert_eq!(serialized["outcome"], "FAILED");
        assert_eq!(serialized["coverage"]["requestedUnits"], 1);
        assert_eq!(serialized["coverage"]["completedUnits"], 0);
        assert_eq!(serialized["reasonCode"], expected_reason);
        assert!(serialized["data"].is_null());
        assert!(matches!(
            encode_signed_telemetry_export(&request, &result, &key, &CancellationToken::new()),
            Err(TelemetryArtifactError::InvalidRequest)
        ));
    }
}

#[test]
#[serial]
fn connect_trace_record_rejects_a_forged_partial_complete_result() {
    let request = artifact_request();
    let key = DeviceIdentity::generate();
    let result = TelemetryDiagnosticResult::partial(
        &request,
        TelemetryTool::Record,
        Duration::ZERO,
        rustfs::connect::TelemetryReasonCode::Complete,
        rustfs::connect::RecordedTrace {
            spans: vec![],
            dropped_span_count: 0,
        },
    );
    assert!(matches!(
        encode_signed_telemetry_export(&request, &result, &key, &CancellationToken::new()),
        Err(TelemetryArtifactError::InvalidRequest)
    ));
}
