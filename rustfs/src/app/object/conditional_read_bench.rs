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

//! Manually invoked, paired application-read benchmark on real local EC disks.
//! Run the identical harness on both revisions in separate processes. Environment
//! configuration must be set before either process initializes the store.

use super::*;
use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};
use futures::stream;
use http::{HeaderValue, Method};
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use std::time::Instant;

fn percentile(samples: &mut [f64], percent: usize) -> f64 {
    assert!(!samples.is_empty(), "benchmark must collect samples");
    samples.sort_by(f64::total_cmp);
    samples[(samples.len() * percent).div_ceil(100).saturating_sub(1)]
}

async fn read_once(usecase: &DefaultObjectUsecase, object: &str, etag: &str, kind: &str, expected: &[u8]) -> f64 {
    let start = Instant::now();
    let mut req = build_request(
        GetObjectInput::builder()
            .bucket("conditional-bench".to_owned())
            .key(object.to_owned())
            .build()
            .expect("benchmark input"),
        Method::GET,
    );
    if kind != "unconditional" {
        let tag = if kind == "hit" { etag } else { "unmatched-benchmark-etag" };
        req.headers.insert(
            http::header::IF_NONE_MATCH,
            HeaderValue::from_str(&format!("\"{tag}\"")).expect("benchmark condition"),
        );
    }
    match usecase.execute_get_object(req).await {
        Err(error) => {
            assert_eq!(kind, "hit");
            assert_eq!(error.code(), &S3ErrorCode::NotModified);
        }
        Ok(mut response) => {
            assert_ne!(kind, "hit");
            let mut body = response.output.body.take().expect("benchmark body");
            let mut actual = Vec::with_capacity(expected.len());
            while let Some(chunk) = body.next().await {
                actual.extend_from_slice(&chunk.expect("benchmark body chunk"));
            }
            assert_eq!(actual, expected, "benchmark must consume and validate the whole body");
        }
    }
    start.elapsed().as_secs_f64() * 1000.0
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual benchmark: use the paired runner with --ignored --nocapture"]
async fn conditional_read_paired_benchmark() {
    let iterations = std::env::var("RUSTFS_CONDITIONAL_BENCH_ITERATIONS")
        .unwrap_or_else(|_| "1000".to_owned())
        .parse::<usize>()
        .expect("benchmark iteration count");
    let concurrency = std::env::var("RUSTFS_CONDITIONAL_BENCH_CONCURRENCY")
        .unwrap_or_else(|_| "8".to_owned())
        .parse::<usize>()
        .expect("benchmark concurrency");
    assert!(iterations >= 100 && concurrency > 0);
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::set_global_recorder(recorder).expect("run the benchmark alone in a fresh process");
    let _metadata_profile = crate::app::storage_api::test::set_disk::ConditionalReadBenchmarkMetadataGuard::acquire();
    let (store, context) = real_store_test_context().await;
    rustfs_io_metrics::set_get_stage_metrics_enabled(true);
    let cache_enabled = !context.object_data_cache().is_disabled();
    let usecase = DefaultObjectUsecase::with_context(Some(context));
    store
        .make_bucket("conditional-bench", &MakeBucketOptions::default())
        .await
        .expect("benchmark bucket");
    for size in [4096, 1_300_000] {
        let object = format!("bench-{size}");
        let expected = (0..size).map(|index| (index % 251) as u8).collect::<Vec<_>>();
        let info = put_real_cold_fill_object(&store, "conditional-bench", &object, &expected).await;
        let etag = info.etag.as_deref().expect("benchmark ETag");
        for kind in ["unconditional", "hit", "miss"] {
            // Prime the body cache and filesystem for every case, including 304.
            for _ in 0..20 {
                read_once(&usecase, &object, etag, "unconditional", &expected).await;
            }
            snapshotter.snapshot();
            let start = Instant::now();
            let mut latencies = stream::iter(0..iterations)
                .map(|_| read_once(&usecase, &object, etag, kind, &expected))
                .buffer_unordered(concurrency)
                .collect::<Vec<_>>()
                .await;
            let elapsed = start.elapsed().as_secs_f64();
            let mut lock_ms = Vec::new();
            let mut metadata_cache_hits = 0;
            for (key, _, _, value) in snapshotter.snapshot().into_vec() {
                if key.key().name() == "rustfs_io_get_object_metadata_cache_total"
                    && key
                        .key()
                        .labels()
                        .any(|label| label.key() == "decision" && label.value() == "hit")
                    && let DebugValue::Counter(count) = &value
                {
                    metadata_cache_hits += count;
                }
                if key.key().name() == "rustfs_object_lock_diag_hold_duration_seconds"
                    && key
                        .key()
                        .labels()
                        .any(|label| label.key() == "mode" && label.value() == "read")
                    && let DebugValue::Histogram(values) = value
                {
                    lock_ms.extend(values.iter().map(|sample| sample.0 * 1000.0));
                }
            }
            assert!(!lock_ms.is_empty(), "enable RUSTFS_OBJECT_LOCK_DIAG_ENABLE before startup");
            assert_eq!(metadata_cache_hits, 0, "slow-disk measurements require a metadata-cache bypass");
            println!(
                "CONDITIONAL_BENCH {}",
                serde_json::json!({
                    "size": size, "kind": kind, "cache": cache_enabled,
                    "iterations": iterations, "concurrency": concurrency,
                    "metadata_cache_hits": metadata_cache_hits, "metadata_cache_bypassed": true,
                    "slowtail_ms": std::env::var("RUSTFS_GET_METADATA_SLOWTAIL_FAULT_DELAY_MS").unwrap_or_default(),
                    "ops_per_sec": iterations as f64 / elapsed,
                    "p95_ms": percentile(&mut latencies, 95), "p99_ms": percentile(&mut latencies, 99),
                    "lock_samples": lock_ms.len(), "lock_p95_ms": percentile(&mut lock_ms, 95),
                    "lock_p99_ms": percentile(&mut lock_ms, 99),
                })
            );
        }
    }
}
