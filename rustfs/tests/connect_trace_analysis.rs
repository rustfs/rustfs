use serial_test::serial;
use std::time::{Duration, Instant};

use rustfs::connect::{LocalTelemetryConsent, LocallyReviewedTraceArtifact, TraceAnalysisError, analyze_trace, replay_trace};
use tokio_util::sync::CancellationToken;

fn consent() -> LocalTelemetryConsent {
    LocalTelemetryConsent::new(Instant::now() + Duration::from_secs(1)).expect("future consent")
}

#[test]
#[serial]
fn connect_trace_analysis_is_deterministic_and_uses_only_observed_values() {
    let bytes = br#"{"spans":[{"operation":"GET_OBJECT","durationMicros":500,"status":"OK"},{"operation":"GET_OBJECT","durationMicros":250,"status":"ERROR"},{"operation":"INTERNAL_RPC","durationMicros":25,"status":"OK"}],"droppedSpanCount":0}"#;
    let replay = replay_trace(
        LocallyReviewedTraceArtifact::new(bytes).expect("bounded artifact"),
        consent(),
        &CancellationToken::new(),
    )
    .expect("valid replay");
    let first = analyze_trace(&replay, consent(), &CancellationToken::new()).expect("analysis succeeds");
    let second = analyze_trace(&replay, consent(), &CancellationToken::new()).expect("analysis is repeatable");

    assert_eq!(first, second);
    assert_eq!(first.span_count, 3);
    assert_eq!(first.error_count, 1);
    assert_eq!(first.total_duration_micros, 775);
    assert_eq!(first.operations.len(), 2);
    assert_eq!(first.operations[0].span_count, 2);
    assert_eq!(first.operations[0].error_count, 1);
}

#[test]
#[serial]
fn connect_trace_analysis_honors_stop() {
    let replay = replay_trace(
        LocallyReviewedTraceArtifact::new(br#"{"spans":[],"droppedSpanCount":0}"#).expect("bounded artifact"),
        consent(),
        &CancellationToken::new(),
    )
    .expect("valid replay");
    let cancel = CancellationToken::new();
    cancel.cancel();
    let error = analyze_trace(&replay, consent(), &cancel).expect_err("cancelled analysis must fail");
    assert_eq!(error, TraceAnalysisError::Cancelled);
}
