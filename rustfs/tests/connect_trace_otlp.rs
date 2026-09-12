use serial_test::serial;
use std::time::{Duration, Instant};

use opentelemetry_proto::tonic::{
    collector::trace::v1::ExportTraceServiceRequest,
    trace::v1::{ResourceSpans, ScopeSpans, Span},
};
use prost::Message as _;
use reqwest::{Url, header};
use rustfs::connect::{
    LocalOtlpHeaders, LocalTelemetryConsent, LocallyReviewedTraceArtifact, OtlpBatch, OtlpForwardError, TraceReplayError,
    export_trace_otlp, replay_trace,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tokio_util::sync::CancellationToken;

fn consent() -> LocalTelemetryConsent {
    LocalTelemetryConsent::new(Instant::now() + Duration::from_secs(2)).expect("future consent")
}

fn encoded_batch(span_count: usize) -> Vec<u8> {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            scope_spans: vec![ScopeSpans {
                spans: vec![Span::default(); span_count],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
    .encode_to_vec()
}

fn encoded_named_batch(name_length: usize) -> Vec<u8> {
    let mut span = Span::default();
    span.name = "x".repeat(name_length);
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            scope_spans: vec![ScopeSpans {
                spans: vec![span],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
    .encode_to_vec()
}

async fn collector(status: &str) -> (Url, tokio::task::JoinHandle<Vec<u8>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind collector");
    let address = listener.local_addr().expect("collector address");
    let status = status.to_owned();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept OTLP request");
        let mut received = vec![0u8; 4096];
        let size = stream.read(&mut received).await.expect("read OTLP request");
        received.truncate(size);
        let response = format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        stream.write_all(response.as_bytes()).await.expect("write response");
        received
    });
    (Url::parse(&format!("http://{address}/v1/traces")).expect("collector URL"), task)
}

#[tokio::test]
#[serial]
async fn connect_trace_otlp_forwards_bounded_protobuf_and_keeps_auth_out_of_receipt() {
    let (endpoint, server) = collector("200 OK").await;
    let mut headers = header::HeaderMap::new();
    headers.insert(header::AUTHORIZATION, header::HeaderValue::from_static("Bearer local-secret"));
    let receipt = export_trace_otlp(
        endpoint,
        LocalOtlpHeaders::new(headers),
        OtlpBatch::new(encoded_batch(2)).expect("valid batch"),
        consent(),
        Duration::from_secs(1),
        &CancellationToken::new(),
    )
    .await
    .expect("collector accepts batch");
    let request = String::from_utf8(server.await.expect("collector task")).expect("HTTP is UTF-8");
    assert!(request.to_ascii_lowercase().contains("authorization: bearer local-secret"));
    assert!(request.to_ascii_lowercase().contains("content-type: application/x-protobuf"));
    assert_eq!(receipt.accepted_span_count, 2);
    assert_eq!(receipt.exported_bytes, encoded_batch(2).len() as u64);
    assert!(
        !serde_json::to_string(&receipt)
            .expect("receipt JSON")
            .contains("local-secret")
    );
}

#[tokio::test]
#[serial]
async fn connect_trace_otlp_reports_fixed_failure_without_response_or_secret_body() {
    let (endpoint, server) = collector("401 Unauthorized").await;
    let error = export_trace_otlp(
        endpoint,
        LocalOtlpHeaders::new(header::HeaderMap::new()),
        OtlpBatch::new(encoded_batch(1)).expect("valid batch"),
        consent(),
        Duration::from_secs(1),
        &CancellationToken::new(),
    )
    .await
    .expect_err("rejection must fail");
    server.await.expect("collector task");
    assert_eq!(error, OtlpForwardError::Rejected);
}

#[test]
#[serial]
fn connect_trace_otlp_rejects_remote_plaintext_and_oversized_batches() {
    assert!(OtlpBatch::new(encoded_batch(1)).is_ok());
    assert!(OtlpBatch::new(encoded_batch(1024)).is_ok());
    assert!(OtlpBatch::new(encoded_batch(0)).is_err());
    assert!(OtlpBatch::new(encoded_batch(1025)).is_err());
    assert!(OtlpBatch::new(vec![0; 1_048_577]).is_err());
    assert!(OtlpBatch::new(vec![1]).is_err());
    let endpoint = Url::parse("http://collector.example/v1/traces").expect("URL");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let error = runtime
        .block_on(export_trace_otlp(
            endpoint,
            LocalOtlpHeaders::new(header::HeaderMap::new()),
            OtlpBatch::new(encoded_batch(1)).expect("valid batch"),
            consent(),
            Duration::from_secs(1),
            &CancellationToken::new(),
        ))
        .expect_err("plaintext remote collector must fail");
    assert_eq!(error, OtlpForwardError::Endpoint);
}

#[test]
#[serial]
fn connect_trace_otlp_lease_blocks_replay_until_the_batch_is_dropped() {
    let held = OtlpBatch::new(encoded_batch(1)).expect("held parsed batch");
    let reviewed = br#"{"spans":[],"droppedSpanCount":0}"#;
    let error = replay_trace(
        LocallyReviewedTraceArtifact::new(reviewed).expect("reviewed artifact"),
        consent(),
        &CancellationToken::new(),
    )
    .expect_err("cross-tool concurrency must fail");
    assert_eq!(error, TraceReplayError::Busy);

    drop(held);
    replay_trace(
        LocallyReviewedTraceArtifact::new(reviewed).expect("reviewed artifact"),
        consent(),
        &CancellationToken::new(),
    )
    .expect("replay succeeds after lease release");
}

#[tokio::test]
#[serial]
async fn connect_trace_otlp_honors_stop_without_contacting_the_collector() {
    let cancel = CancellationToken::new();
    cancel.cancel();
    let error = export_trace_otlp(
        Url::parse("http://127.0.0.1:9/v1/traces").expect("URL"),
        LocalOtlpHeaders::new(header::HeaderMap::new()),
        OtlpBatch::new(encoded_batch(1)).expect("valid batch"),
        consent(),
        Duration::from_secs(1),
        &cancel,
    )
    .await
    .expect_err("cancelled forward must fail");
    assert_eq!(error, OtlpForwardError::Cancelled);
}

#[tokio::test]
#[serial]
async fn connect_trace_otlp_enforces_the_timeout_byte_budget_before_network_io() {
    let endpoint = Url::parse("http://127.0.0.1:9/v1/traces").expect("URL");
    let error = export_trace_otlp(
        endpoint.clone(),
        LocalOtlpHeaders::new(header::HeaderMap::new()),
        OtlpBatch::new(encoded_batch(1024)).expect("bounded actual batch"),
        consent(),
        Duration::from_millis(1),
        &CancellationToken::new(),
    )
    .await
    .expect_err("one millisecond cannot send the full batch budget");
    assert_eq!(error, OtlpForwardError::InvalidBatch);

    let error = export_trace_otlp(
        endpoint,
        LocalOtlpHeaders::new(header::HeaderMap::new()),
        OtlpBatch::new(encoded_batch(1)).expect("bounded actual batch"),
        consent(),
        Duration::from_micros(999),
        &CancellationToken::new(),
    )
    .await
    .expect_err("sub-millisecond timeout has no byte budget");
    assert_eq!(error, OtlpForwardError::InvalidTimeout);

    let error = export_trace_otlp(
        Url::parse("http://127.0.0.1:9/v1/traces").expect("URL"),
        LocalOtlpHeaders::new(header::HeaderMap::new()),
        OtlpBatch::new(encoded_named_batch(200_000)).expect("bounded actual batch"),
        LocalTelemetryConsent::new(Instant::now() + Duration::from_millis(100)).expect("short consent"),
        Duration::from_secs(30),
        &CancellationToken::new(),
    )
    .await
    .expect_err("effective consent timeout limits the byte budget");
    assert_eq!(error, OtlpForwardError::InvalidBatch);
}
