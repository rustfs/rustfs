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

mod connect {
    pub use rustfs::connect::DeviceIdentity;
}

#[allow(dead_code)]
#[path = "../src/connect/diagnostics/logs.rs"]
mod logs;

use std::fs::{self, OpenOptions};
use std::io::{Cursor, Read as _, Write as _};
#[cfg(unix)]
use std::os::unix::fs::{PermissionsExt as _, symlink};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64_simd::URL_SAFE_NO_PAD;
use logs::{
    CaptureMode, ConfiguredLogSource, LOGS_CAPABILITY, LocalLogConsent, LogCaptureError, LogCaptureRequest, LogProvenance,
    export_logs_from, save_signed_log_export,
};
use p256::ecdsa::signature::Verifier as _;
use p256::ecdsa::{Signature, VerifyingKey};
use p256::pkcs8::DecodePublicKey as _;
use sha2::{Digest as _, Sha256};
use time::{Duration as TimeDuration, OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use zip::ZipArchive;

static TEST_LOCK: Mutex<()> = Mutex::const_new(());

fn now() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).expect("current time").as_secs() as i64
}

fn timestamp(unix: i64, offset_millis: i64) -> String {
    (OffsetDateTime::from_unix_timestamp(unix).expect("valid timestamp") + TimeDuration::milliseconds(offset_millis))
        .format(&Rfc3339)
        .expect("RFC3339 timestamp")
}

fn request(mode: CaptureMode) -> LogCaptureRequest {
    let now = now();
    let organization = "organizations/019e3ae0-0000-7000-8000-000000000011";
    let cluster = format!("{organization}/clusters/019e3ae0-0000-7000-8000-000000000012");
    LogCaptureRequest {
        organization_name: organization.to_string(),
        cluster_name: cluster.clone(),
        device_name: format!("{cluster}/clusterDevices/019e3ae0-0000-7000-8000-000000000013"),
        run_uid: "019e3ae0-0000-7000-8000-000000000014".to_string(),
        artifact_uid: "019e3ae0-0000-7000-8000-000000000015".to_string(),
        schema_version: 1,
        capability: LOGS_CAPABILITY.to_string(),
        consent: LocalLogConsent {
            consent_uid: "019e3ae0-0000-7000-8000-000000000016".to_string(),
            policy_revision: 7,
            expires_at_unix: now + 120,
            confirmed: true,
        },
        produced_at_unix: now,
        expires_at_unix: now + 60,
        nonce: [0x5a; 32],
        mode,
        duration: Duration::from_millis(100),
        max_events: 1_024,
        provenance: LogProvenance::new("c".repeat(40), "d".repeat(64), "1.0.0-rc.6", vec!["gcs".to_string()]),
    }
}

fn line(timestamp: &str, level: &str, event: &str, extra: &str) -> String {
    format!(r#"{{"timestamp":"{timestamp}","level":"{level}","event":"{event}","message":"{extra}"}}"#)
}

fn source(contents: &str) -> (tempfile::TempDir, ConfiguredLogSource) {
    let directory = tempfile::tempdir().expect("temporary log directory");
    fs::write(directory.path().join("rustfs.log"), contents).expect("write active log");
    let source = ConfiguredLogSource::new(directory.path(), "rustfs.log").expect("configured log source");
    (directory, source)
}

fn fixture() -> serde_json::Value {
    serde_json::from_str(include_str!("fixtures/connect-logs-v1.json")).expect("logs fixture JSON")
}

fn archive_entry(archive: &mut ZipArchive<Cursor<Vec<u8>>>, name: &str) -> Vec<u8> {
    let mut entry = archive.by_name(name).expect("archive entry");
    let mut bytes = Vec::new();
    entry.read_to_end(&mut bytes).expect("read archive entry");
    bytes
}

#[tokio::test]
async fn batch_capture_exports_only_allow_listed_fields_in_a_signed_artifact() {
    let _guard = TEST_LOCK.lock().await;
    let mut request = request(CaptureMode::Batch);
    request.duration = Duration::from_secs(2);
    let first_timestamp = timestamp(request.produced_at_unix - 1, 0);
    let second_timestamp = timestamp(request.produced_at_unix - 1, 125);
    let logs = [
        line(
            &first_timestamp,
            "INFO",
            "http_startup_endpoints",
            "endpoint=/srv/customer-a Authorization: Bearer SYNTHETIC_TOKEN_123",
        ),
        line(
            &second_timestamp,
            "ERROR",
            "rpc_request_failed",
            "Cookie: session=SYNTHETIC_SESSION_123\\nprivate/path",
        ),
        String::new(),
    ]
    .join("\n");
    let (_directory, source) = source(&logs);
    let key = connect::DeviceIdentity::generate();
    let export = export_logs_from(&request, &key, &CancellationToken::new(), &source)
        .await
        .expect("signed logs export");
    assert_eq!(export.event_count, 2);
    assert_eq!(export.dropped_event_count, 0);
    assert_eq!(export.archive_sha256, hex(&Sha256::digest(&export.archive_bytes)));

    let mut archive = ZipArchive::new(Cursor::new(export.archive_bytes)).expect("logs archive");
    assert_eq!(archive.len(), 3);
    let envelope_bytes = archive_entry(&mut archive, "envelope.json");
    let signature_bytes = archive_entry(&mut archive, "envelope.sig");
    let result_bytes = archive_entry(&mut archive, "result.json");
    let envelope: serde_json::Value = serde_json::from_slice(&envelope_bytes).expect("envelope JSON");
    let signature: serde_json::Value = serde_json::from_slice(&signature_bytes).expect("signature JSON");
    let result: serde_json::Value = serde_json::from_slice(&result_bytes).expect("result JSON");

    assert_eq!(envelope["classification"], "L3");
    assert_eq!(envelope["toolId"], "logs.capture");
    assert_eq!(envelope["payload"]["sha256"], hex(&Sha256::digest(&result_bytes)));
    assert_eq!(result["outcome"], "SUCCEEDED");
    assert_eq!(result["provenance"]["buildFeatures"], serde_json::json!(["gcs"]));
    assert_eq!(result["data"]["events"][0]["eventId"], "SERVICE_STARTED");
    assert_eq!(result["data"]["events"][0]["offsetMillis"], 0);
    assert_eq!(result["data"]["events"][1]["eventId"], "REQUEST_FAILED");
    assert_eq!(result["data"]["events"][1]["offsetMillis"], 125);
    let encoded = serde_json::to_string(&result).expect("encoded result");
    for forbidden in [
        "message",
        "endpoint",
        "/srv/",
        "Authorization",
        "SYNTHETIC_TOKEN",
        "Cookie",
        "private/path",
    ] {
        assert!(!encoded.contains(forbidden), "result leaked forbidden material: {forbidden}");
    }

    let raw_signature = URL_SAFE_NO_PAD
        .decode_to_vec(signature["value"].as_str().expect("signature value"))
        .expect("base64url signature");
    let signature_value = Signature::from_slice(&raw_signature).expect("P-256 signature");
    assert_eq!(signature_value.normalize_s(), signature_value);
    let public = VerifyingKey::from_public_key_der(&key.public_key_der()).expect("public key");
    let mut input = b"rustfs-diagnostic-envelope-v1\0".to_vec();
    input.extend_from_slice(&envelope_bytes);
    public
        .verify(&input, &signature_value)
        .expect("signature over exact envelope bytes");
}

#[tokio::test]
async fn protocol_fixture_projects_sensitive_source_fields_to_the_closed_event_shape() {
    let _guard = TEST_LOCK.lock().await;
    let fixture = fixture();
    let mut request = request(CaptureMode::Batch);
    request.duration = Duration::from_secs(2);
    let timestamp = timestamp(request.produced_at_unix - 1, 0);
    let source_lines = fixture["sourceEvents"]
        .as_array()
        .expect("sourceEvents array")
        .iter()
        .map(|event| {
            let mut event = event.clone();
            event["timestamp"] = timestamp.clone().into();
            serde_json::to_string(&event).expect("source event JSON")
        })
        .collect::<Vec<_>>()
        .join("\n");
    let (_directory, source) = source(&source_lines);
    let key = connect::DeviceIdentity::generate();
    let export = export_logs_from(&request, &key, &CancellationToken::new(), &source)
        .await
        .expect("signed logs export");
    assert_eq!(
        export.event_count,
        fixture["expectedEventCount"].as_u64().expect("expectedEventCount") as usize
    );
    assert_eq!(
        export.dropped_event_count,
        fixture["expectedDroppedEventCount"]
            .as_u64()
            .expect("expectedDroppedEventCount")
    );

    let mut archive = ZipArchive::new(Cursor::new(export.archive_bytes)).expect("logs archive");
    let result_bytes = archive_entry(&mut archive, "result.json");
    let result: serde_json::Value = serde_json::from_slice(&result_bytes).expect("result JSON");
    assert_eq!(result["data"]["events"][0]["eventId"], "DRIVE_UNAVAILABLE");
    assert_eq!(result["data"]["events"][1]["eventId"], "REQUEST_FAILED");
    assert_eq!(result["data"]["events"][2]["eventId"], "SERVICE_STARTED");
    let encoded = String::from_utf8(result_bytes).expect("UTF-8 result");
    for forbidden in fixture["forbiddenExportFragments"]
        .as_array()
        .expect("forbiddenExportFragments array")
    {
        let forbidden = forbidden.as_str().expect("forbidden fragment");
        assert!(!encoded.contains(forbidden), "result leaked forbidden material: {forbidden}");
    }
}

#[tokio::test]
async fn batch_capture_drops_unknown_malformed_oversized_and_excess_events() {
    let _guard = TEST_LOCK.lock().await;
    let mut request = request(CaptureMode::Batch);
    request.duration = Duration::from_secs(2);
    request.max_events = 1;
    let first_timestamp = timestamp(request.produced_at_unix - 1, 0);
    let second_timestamp = timestamp(request.produced_at_unix, 0);
    let logs = [
        "not-json".to_string(),
        line(&first_timestamp, "DEBUG", "rpc_request_failed", "debug"),
        line(&first_timestamp, "ERROR", "future_event", "unknown"),
        format!(
            "{{\"timestamp\":\"{first_timestamp}\",\"level\":\"ERROR\",\"event\":\"rpc_request_failed\",\"message\":\"{}\"}}",
            "x".repeat(4_097),
        ),
        line(&first_timestamp, "ERROR", "rpc_request_failed", "first"),
        line(&second_timestamp, "ERROR", "rpc_request_failed", "second"),
        String::new(),
    ]
    .join("\n");
    let (_directory, source) = source(&logs);
    let key = connect::DeviceIdentity::generate();
    let export = export_logs_from(&request, &key, &CancellationToken::new(), &source)
        .await
        .expect("bounded export");
    assert_eq!(export.event_count, 1);
    assert_eq!(export.dropped_event_count, 5);
}

#[tokio::test]
async fn live_capture_tails_new_events_and_honors_cancellation() {
    let _guard = TEST_LOCK.lock().await;
    let (directory, source) = source("");
    let key = connect::DeviceIdentity::generate();
    let mut request = request(CaptureMode::Live);
    request.duration = Duration::from_millis(120);
    let writer = async {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let mut file = OpenOptions::new()
            .append(true)
            .open(directory.path().join("rustfs.log"))
            .expect("open active log");
        writeln!(
            file,
            "{}",
            line("2026-09-12T12:00:00Z", "WARN", "drive_unavailable", "secret=/tmp/customer")
        )
        .expect("append live log");
        file.flush().expect("flush live log");
    };
    let capture_cancel = CancellationToken::new();
    let capture = export_logs_from(&request, &key, &capture_cancel, &source);
    let ((), export) = tokio::join!(writer, capture);
    assert_eq!(export.expect("live export").event_count, 1);

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert!(matches!(
        export_logs_from(&request, &key, &cancelled, &source).await,
        Err(LogCaptureError::Cancelled)
    ));
}

#[tokio::test]
async fn consent_limits_and_source_boundary_fail_closed() {
    let _guard = TEST_LOCK.lock().await;
    let (directory, source) = source(&line("2026-09-12T12:00:00Z", "INFO", "http_startup_endpoints", "safe"));
    let key = connect::DeviceIdentity::generate();
    let mut denied = request(CaptureMode::Batch);
    denied.consent.confirmed = false;
    assert!(matches!(
        export_logs_from(&denied, &key, &CancellationToken::new(), &source).await,
        Err(LogCaptureError::ConsentRequired)
    ));

    let mut over_limit = request(CaptureMode::Batch);
    over_limit.max_events = logs::MAX_EVENTS + 1;
    assert!(matches!(
        export_logs_from(&over_limit, &key, &CancellationToken::new(), &source).await,
        Err(LogCaptureError::LimitExceeded)
    ));

    assert!(matches!(
        ConfiguredLogSource::new(directory.path(), "../rustfs.log"),
        Err(LogCaptureError::SourceUnavailable)
    ));
    #[cfg(unix)]
    {
        let outside = tempfile::NamedTempFile::new().expect("outside log");
        symlink(outside.path(), directory.path().join("linked.log")).expect("symlink log");
        assert!(matches!(
            ConfiguredLogSource::new(directory.path(), "linked.log"),
            Err(LogCaptureError::SourceUnavailable)
        ));
    }
}

#[tokio::test]
async fn local_export_is_private_no_clobber_and_cancel_safe() {
    let _guard = TEST_LOCK.lock().await;
    let (_directory, source) = source(&line("2026-09-12T12:00:00Z", "INFO", "http_startup_endpoints", "safe"));
    let key = connect::DeviceIdentity::generate();
    let export = export_logs_from(&request(CaptureMode::Batch), &key, &CancellationToken::new(), &source)
        .await
        .expect("signed export");
    let output_directory = tempfile::tempdir().expect("output directory");
    let output = output_directory.path().join("logs.zip");
    let receipt = save_signed_log_export(&output, &export, &CancellationToken::new()).expect("save export");
    assert_eq!(receipt.archive_sha256, export.archive_sha256);
    assert_eq!(receipt.archive_size_bytes, export.archive_bytes.len() as u64);
    assert_eq!(fs::read(&output).expect("saved artifact"), export.archive_bytes);
    #[cfg(unix)]
    assert_eq!(fs::metadata(&output).expect("metadata").permissions().mode() & 0o777, 0o600);
    assert!(matches!(
        save_signed_log_export(&output, &export, &CancellationToken::new()),
        Err(LogCaptureError::AlreadyExists)
    ));

    let mut forged = export.clone();
    forged.artifact_uid = "../escape".to_string();
    assert!(matches!(
        save_signed_log_export(&output_directory.path().join("forged.zip"), &forged, &CancellationToken::new()),
        Err(LogCaptureError::InvalidRequest)
    ));

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert!(matches!(
        save_signed_log_export(&output_directory.path().join("cancelled.zip"), &export, &cancelled),
        Err(LogCaptureError::Cancelled)
    ));
}

fn assert_archive_contains_no_sentinels(export: &logs::SignedLogExport, sentinels: &[&str]) {
    let raw_lossy = String::from_utf8_lossy(&export.archive_bytes);
    for sentinel in sentinels {
        assert!(!raw_lossy.contains(sentinel), "raw zip archive leaked sentinel: {sentinel}");
    }
    let mut archive = ZipArchive::new(Cursor::new(export.archive_bytes.clone())).expect("signed logs archive");
    for entry_name in ["envelope.json", "envelope.sig", "result.json"] {
        let content = String::from_utf8(archive_entry(&mut archive, entry_name)).expect("UTF-8 archive entry");
        for sentinel in sentinels {
            assert!(!content.contains(sentinel), "{entry_name} leaked sentinel: {sentinel}");
        }
    }
}

#[tokio::test]
async fn live_capture_handles_mid_read_truncate_and_drops_partial_prefix() {
    let _guard = TEST_LOCK.lock().await;
    let initial = format!("{}\n", "x".repeat(512));
    let (directory, source) = source(&initial);
    let log_path = directory.path().join("rustfs.log");
    let key = connect::DeviceIdentity::generate();
    let mut request = request(CaptureMode::Live);
    request.duration = Duration::from_millis(260);

    let sentinels = [
        "SENTINEL_CONNECT_BEARER_TOKEN",
        "SENTINEL_CONNECT_SESSION_TOKEN",
        "SENTINEL_CONNECT_SSEC_KEY",
        "SENTINEL_CONNECT_URL_PASS",
        "SENTINEL_CONNECT_URL_QUERY",
        "tenant-a/secret-object-key.bin",
    ];
    let sensitive_msg = format!(
        "Authorization=Bearer {} session={} x-amz-server-side-encryption-customer-key={} \
         url=https://otel:{}@collector.invalid/v1/logs?api_key={} key={}",
        sentinels[0], sentinels[1], sentinels[2], sentinels[3], sentinels[4], sentinels[5]
    );
    let post_truncate_event = line("2026-09-12T12:00:01Z", "WARN", "drive_unavailable", &sensitive_msg);

    let writer = async {
        tokio::time::sleep(Duration::from_millis(25)).await;
        // Truncate the active log file below the collector's current offset, then
        // write a partial first line followed by a valid reviewed event.
        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&log_path)
            .expect("truncate active log file");
        writeln!(file, "truncated-prefix-fragment-not-json").expect("write partial prefix");
        writeln!(file, "{post_truncate_event}").expect("write post-truncate event");
        file.flush().expect("flush truncated active log");
    };

    let cancel = CancellationToken::new();
    let ((), export) = tokio::join!(writer, export_logs_from(&request, &key, &cancel, &source));
    let export = export.expect("live capture should survive mid-read truncation");
    assert_eq!(export.event_count, 1, "valid event after truncation must be captured");
    assert!(
        export.dropped_event_count >= 1,
        "mid-read truncation must record dropped prefix accounting"
    );
    assert_archive_contains_no_sentinels(&export, &sentinels);
}

#[tokio::test]
async fn batch_capture_truncates_oversized_source_without_leaking_secrets() {
    let _guard = TEST_LOCK.lock().await;
    let mut req = request(CaptureMode::Batch);
    req.duration = Duration::from_secs(2);
    let ts = timestamp(req.produced_at_unix - 1, 0);
    let sentinels = [
        "SENTINEL_OVERSIZED_BEARER",
        "SENTINEL_OVERSIZED_SSEC",
        "https://user:SENTINEL_OVERSIZED_PASS@otlp.invalid?token=SENTINEL_OVERSIZED_QUERY",
    ];
    let filler_line = format!("{}\n", "p".repeat(1_024));
    let mut content = String::with_capacity(logs::MAX_SOURCE_BYTES + 4_096);
    while content.len() <= logs::MAX_SOURCE_BYTES + 2_048 {
        content.push_str(&filler_line);
    }
    content.push_str(&line(
        &ts,
        "ERROR",
        "rpc_request_failed",
        &format!("{} {} {}", sentinels[0], sentinels[1], sentinels[2]),
    ));
    content.push('\n');

    let (_directory, source) = source(&content);
    let key = connect::DeviceIdentity::generate();
    let export = export_logs_from(&req, &key, &CancellationToken::new(), &source)
        .await
        .expect("oversized batch source should export bounded tail");
    assert_eq!(export.event_count, 1);
    assert!(
        export.dropped_event_count >= 1,
        "source larger than MAX_SOURCE_BYTES must account for truncated prefix and dropped lines"
    );
    assert_archive_contains_no_sentinels(&export, &sentinels);
}

#[tokio::test]
async fn mid_flight_cancellation_and_concurrent_busy_release_collector_lease() {
    let _guard = TEST_LOCK.lock().await;
    let mut live_req = request(CaptureMode::Live);
    live_req.duration = Duration::from_secs(5);
    let mut batch_req = request(CaptureMode::Batch);
    batch_req.duration = Duration::from_secs(2);
    let ts = timestamp(batch_req.produced_at_unix - 1, 0);
    let (_directory, source) = source(&format!("{}\n", line(&ts, "INFO", "http_startup_endpoints", "safe-recovery")));
    let key = connect::DeviceIdentity::generate();

    let live_cancel = CancellationToken::new();
    let live_source = source.clone();
    let live_cancel_clone = live_cancel.clone();
    let live_key = connect::DeviceIdentity::generate();
    let started = std::time::Instant::now();
    let live_handle = tokio::spawn(async move { export_logs_from(&live_req, &live_key, &live_cancel_clone, &live_source).await });

    // Yield once so the spawned live capture enters `export_logs_from`, acquires
    // `CollectorLease`, and parks on `capture_live`'s `tokio::select!`.
    tokio::task::yield_now().await;

    // A concurrent capture must now fail closed with `LogCaptureError::Busy`.
    let concurrent_err = export_logs_from(&batch_req, &key, &CancellationToken::new(), &source)
        .await
        .expect_err("concurrent capture must fail with LogCaptureError::Busy while live capture holds lease");
    assert!(
        matches!(concurrent_err, LogCaptureError::Busy),
        "expected LogCaptureError::Busy while live capture holds lease, got {concurrent_err:?}"
    );

    live_cancel.cancel();
    let live_result = live_handle.await.expect("live capture task should not panic");
    assert!(matches!(live_result, Err(LogCaptureError::Cancelled)));
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "mid-flight cancellation must terminate promptly instead of waiting full 5s duration"
    );

    // After cancellation, the collector lease must be released so the next
    // valid capture succeeds immediately.
    let recovery = export_logs_from(&batch_req, &key, &CancellationToken::new(), &source)
        .await
        .expect("subsequent capture must succeed once cancelled capture releases collector lease");
    assert_eq!(recovery.event_count, 1);
}

#[tokio::test]
async fn source_io_failure_releases_lease_and_hides_raw_source_path() {
    let _guard = TEST_LOCK.lock().await;
    let dir = tempfile::Builder::new()
        .prefix("SENTINEL_LOG_DIR_PATH_")
        .tempdir()
        .expect("temporary log directory with sentinel name");
    let log_path = dir.path().join("rustfs.log");
    let mut req = request(CaptureMode::Batch);
    req.duration = Duration::from_secs(2);
    let ts = timestamp(req.produced_at_unix - 1, 0);
    let valid_line = format!("{}\n", line(&ts, "INFO", "http_startup_endpoints", "recovered"));
    fs::write(&log_path, &valid_line).expect("write initial active log");
    let source = ConfiguredLogSource::new(dir.path(), "rustfs.log").expect("configured log source");
    let key = connect::DeviceIdentity::generate();

    // Remove the file after source discovery to inject a real read I/O failure.
    fs::remove_file(&log_path).expect("remove log file to inject I/O failure");
    let io_err = export_logs_from(&req, &key, &CancellationToken::new(), &source)
        .await
        .expect_err("removed active log file must fail with LogCaptureError::Io");
    assert!(matches!(io_err, LogCaptureError::Io(_)));
    let err_display = io_err.to_string();
    assert_eq!(err_display, "logs_io_failed");
    assert!(
        !err_display.contains("SENTINEL_LOG_DIR_PATH_"),
        "error display must not leak raw filesystem path: {err_display}"
    );

    // On Unix, also prove replacing the file with a symlink fails closed via O_NOFOLLOW.
    #[cfg(unix)]
    {
        let outside = tempfile::NamedTempFile::new().expect("outside target");
        symlink(outside.path(), &log_path).expect("replace active log with symlink");
        let symlink_err = export_logs_from(&req, &key, &CancellationToken::new(), &source)
            .await
            .expect_err("symlink replacement must fail closed via O_NOFOLLOW");
        assert!(matches!(symlink_err, LogCaptureError::Io(_)));
        fs::remove_file(&log_path).expect("remove injected symlink");
    }

    // Restore a regular log file and verify the collector lease was released
    // across both I/O failures so the next valid capture succeeds.
    fs::write(&log_path, &valid_line).expect("restore active log file");
    let recovered = export_logs_from(&req, &key, &CancellationToken::new(), &source)
        .await
        .expect("capture after I/O failures must succeed once collector lease is released");
    assert_eq!(recovered.event_count, 1);
}

#[tokio::test]
async fn save_failure_preserves_existing_file_cleans_partial_and_recovers_after_readonly_dir() {
    let _guard = TEST_LOCK.lock().await;
    let mut req = request(CaptureMode::Batch);
    req.duration = Duration::from_secs(2);
    let ts = timestamp(req.produced_at_unix - 1, 0);
    let (_directory, source) = source(&format!("{}\n", line(&ts, "INFO", "http_startup_endpoints", "save-recovery")));
    let key = connect::DeviceIdentity::generate();

    // Verify additional typed validation rejections leave the collector lease free.
    let mut expired_consent = req.clone();
    expired_consent.consent.expires_at_unix = req.produced_at_unix - 1;
    assert!(matches!(
        export_logs_from(&expired_consent, &key, &CancellationToken::new(), &source).await,
        Err(LogCaptureError::ConsentExpired)
    ));
    let mut expired_request = req.clone();
    expired_request.expires_at_unix = req.produced_at_unix - 1;
    assert!(matches!(
        export_logs_from(&expired_request, &key, &CancellationToken::new(), &source).await,
        Err(LogCaptureError::Expired)
    ));
    let mut bad_version = req.clone();
    bad_version.schema_version = 99;
    assert!(matches!(
        export_logs_from(&bad_version, &key, &CancellationToken::new(), &source).await,
        Err(LogCaptureError::UnsupportedVersion)
    ));
    let mut bad_capability = req.clone();
    bad_capability.capability = "logs.capture@99".to_string();
    assert!(matches!(
        export_logs_from(&bad_capability, &key, &CancellationToken::new(), &source).await,
        Err(LogCaptureError::UnsupportedCapability)
    ));

    let export = export_logs_from(&req, &key, &CancellationToken::new(), &source)
        .await
        .expect("valid capture should succeed after rejected requests");

    // 1. Existing target file must never be overwritten, and no `.partial` temp file may remain.
    let output_dir = tempfile::tempdir().expect("output directory");
    let existing = output_dir.path().join("existing.zip");
    fs::write(&existing, b"pre-existing-operator-file").expect("seed existing destination");
    assert!(matches!(
        save_signed_log_export(&existing, &export, &CancellationToken::new()),
        Err(LogCaptureError::AlreadyExists)
    ));
    assert_eq!(
        fs::read(&existing).expect("read existing file"),
        b"pre-existing-operator-file",
        "no-clobber failure must preserve existing file bytes"
    );
    let leftovers: Vec<_> = fs::read_dir(output_dir.path())
        .expect("list output directory")
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(".partial"))
        .collect();
    assert!(
        leftovers.is_empty(),
        "failed no-clobber save must clean up partial temporary files: {leftovers:?}"
    );

    // 2. Missing parent directory fails with Io and leaves no partial artifact.
    let missing_target = output_dir.path().join("missing-subdir").join("logs.zip");
    assert!(matches!(
        save_signed_log_export(&missing_target, &export, &CancellationToken::new()),
        Err(LogCaptureError::Io(_))
    ));
    assert!(!missing_target.exists());

    // 3. Read-only directory fails with Io, leaves no target/partial file, and
    //    succeeds after restoring write permission (non-root Unix).
    #[cfg(unix)]
    {
        let is_root = std::process::Command::new("id")
            .arg("-u")
            .output()
            .ok()
            .and_then(|output| String::from_utf8(output.stdout).ok())
            .is_some_and(|uid| uid.trim() == "0");
        if !is_root {
            let ro_dir = output_dir.path().join("readonly");
            fs::create_dir(&ro_dir).expect("create readonly test dir");
            fs::set_permissions(&ro_dir, fs::Permissions::from_mode(0o500)).expect("make directory read-only");
            let ro_target = ro_dir.join("logs.zip");
            let ro_err = save_signed_log_export(&ro_target, &export, &CancellationToken::new());
            // Restore permissions before assertions so tempdir cleanup always succeeds.
            fs::set_permissions(&ro_dir, fs::Permissions::from_mode(0o700)).expect("restore writable directory");
            assert!(
                matches!(ro_err, Err(LogCaptureError::Io(_))),
                "read-only destination directory must fail with LogCaptureError::Io, got {ro_err:?}"
            );
            assert!(!ro_target.exists(), "failed save must not leave an incomplete receipt file");

            let recovered_receipt = save_signed_log_export(&ro_target, &export, &CancellationToken::new())
                .expect("save must succeed after directory permissions are restored");
            assert_eq!(recovered_receipt.archive_sha256, export.archive_sha256);
            assert_eq!(fs::metadata(&ro_target).expect("metadata").permissions().mode() & 0o777, 0o600);
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("hex encoding");
    }
    encoded
}
