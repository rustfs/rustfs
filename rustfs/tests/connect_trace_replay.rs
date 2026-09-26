use serial_test::serial;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rustfs::connect::{
    LocalTelemetryConsent, LocallyReviewedTraceArtifact, TelemetryArtifactConsent, TelemetryArtifactRequest, TelemetryProvenance,
    TraceReplayError, analyze_trace, replay_trace, replay_trace_result,
};
use tokio_util::sync::CancellationToken;

fn consent() -> LocalTelemetryConsent {
    LocalTelemetryConsent::new(Instant::now() + Duration::from_secs(1)).expect("future consent")
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

#[test]
#[serial]
fn connect_trace_replay_validates_and_digests_reviewed_bytes_without_file_io() {
    let bytes = br#"{"spans":[{"operation":"GET_OBJECT","durationMicros":500,"status":"OK"}],"droppedSpanCount":0}"#;
    let replay = replay_trace(
        LocallyReviewedTraceArtifact::new(bytes).expect("bounded artifact"),
        consent(),
        &CancellationToken::new(),
    )
    .expect("valid record replays");

    assert_eq!(replay.spans.len(), 1);
    assert_eq!(
        replay.input_artifact_sha256,
        "1daadd3e226e7dd67a35dd56092ec985f464d832756e75277bc11d4a25c0291b"
    );
}

#[test]
#[serial]
fn connect_trace_replay_wraps_reviewed_bytes_in_the_frozen_result_contract() {
    let bytes = br#"{"spans":[{"operation":"GET_OBJECT","durationMicros":500,"status":"OK"}],"droppedSpanCount":0}"#;
    let result = replay_trace_result(
        &artifact_request(),
        LocallyReviewedTraceArtifact::new(bytes).expect("bounded artifact"),
        consent(),
        &CancellationToken::new(),
    )
    .expect("typed replay result");
    let analysis = analyze_trace(result.data().expect("replayed data"), consent(), &CancellationToken::new())
        .expect("replayed trace is locally analyzable");
    assert_eq!(analysis.span_count, 1);
    assert_eq!(analysis.total_duration_micros, 500);
    let json = serde_json::to_value(result).expect("result JSON");
    assert_eq!(json["toolId"], "telemetry.replay");
    assert_eq!(json["capability"], "telemetry.replay@1");
    assert_eq!(json["outcome"], "SUCCEEDED");
    assert_eq!(json["reasonCode"], "COMPLETE");
    assert_eq!(json["coverage"]["requestedUnits"], 1);
    assert_eq!(json["coverage"]["completedUnits"], 1);
    assert_eq!(json["data"]["spans"][0]["operation"], "GET_OBJECT");
}

#[test]
#[serial]
fn connect_trace_replay_rejects_unknown_or_secret_bearing_fields() {
    let bytes = br#"{"spans":[],"droppedSpanCount":0,"authorization":"Bearer secret"}"#;
    let error = replay_trace(
        LocallyReviewedTraceArtifact::new(bytes).expect("bounded artifact"),
        consent(),
        &CancellationToken::new(),
    )
    .expect_err("unknown field must fail");
    assert_eq!(error, TraceReplayError::InvalidArtifact);
}

#[test]
#[serial]
fn connect_trace_replay_honors_stop_before_parsing() {
    let cancel = CancellationToken::new();
    cancel.cancel();
    let error = replay_trace(
        LocallyReviewedTraceArtifact::new(br#"{"spans":[],"droppedSpanCount":0}"#).expect("bounded artifact"),
        consent(),
        &cancel,
    )
    .expect_err("cancelled replay must fail");
    assert_eq!(error, TraceReplayError::Cancelled);
}

#[test]
#[serial]
fn connect_trace_replay_rejects_duplicate_fields_and_safe_integer_overflow() {
    for bytes in [
        br#"{"spans":[],"spans":[],"droppedSpanCount":0}"#.as_slice(),
        br#"{"spans":[{"operation":"GET_OBJECT","durationMicros":9007199254740992,"status":"OK"}],"droppedSpanCount":0}"#
            .as_slice(),
        br#"{"spans":[],"droppedSpanCount":9007199254740992}"#.as_slice(),
    ] {
        let error = replay_trace(
            LocallyReviewedTraceArtifact::new(bytes).expect("bounded artifact"),
            consent(),
            &CancellationToken::new(),
        )
        .expect_err("invalid record must fail");
        assert_eq!(error, TraceReplayError::InvalidArtifact);
    }
}

#[test]
#[serial]
fn connect_trace_replay_enforces_exact_artifact_size_boundary() {
    assert!(LocallyReviewedTraceArtifact::new(&vec![b' '; 262_144]).is_ok());
    assert_eq!(
        LocallyReviewedTraceArtifact::new(&vec![b' '; 262_145]).err(),
        Some(TraceReplayError::InvalidArtifact)
    );
}
