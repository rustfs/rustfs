use criterion::{Criterion, criterion_group, criterion_main};
use metrics_util::debugging::DebuggingRecorder;
use rustfs_io_metrics::{
    LocalBatchReadDecision, LocalBatchReadMetrics, LocalBatchReadStage, record_local_batch_read_decision,
    set_get_stage_metrics_enabled,
};
use rustfs_io_metrics::{MetricsCollector, PerformanceMetrics, record_get_object_timeout_operation_start};
use rustfs_io_metrics::{record_s3_op, s3_http_metrics::S3HttpRequestGuard};
use rustfs_s3_ops::S3Operation;
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn local_batch_attribution_sample() {
    let prepare = rustfs_io_metrics::get_stage_metrics_enabled().then(Instant::now);
    record_local_batch_read_decision(LocalBatchReadDecision::Selected, prepare.map(|started| started.elapsed()));
    if let Some(metrics) = LocalBatchReadMetrics::capture() {
        metrics.record_batch(4);
        let execution = Instant::now();
        metrics.record_stage(LocalBatchReadStage::QueueWait, true, execution.elapsed());
        for _ in 0..4 {
            for stage in [
                LocalBatchReadStage::Metadata,
                LocalBatchReadStage::Open,
                LocalBatchReadStage::Read,
            ] {
                let started = Instant::now();
                metrics.record_stage(stage, true, started.elapsed());
            }
            metrics.record_shard(true, 32 * 1024, 32 * 1024);
        }
        metrics.record_stage(LocalBatchReadStage::Execution, true, execution.elapsed());
    }
}

fn bench_local_batch_attribution(c: &mut Criterion) {
    let mut group = c.benchmark_group("local_batch_attribution_4_shards_debug_recorder");
    for (name, enabled) in [("disabled", false), ("enabled", true)] {
        set_get_stage_metrics_enabled(enabled);
        group.bench_function(name, |b| {
            b.iter_custom(|iterations| {
                let mut remaining = iterations;
                let mut elapsed = Duration::ZERO;
                while remaining > 0 {
                    // Bound captured histogram storage independently of Criterion's
                    // iteration count. Recorder setup/teardown is not measured.
                    let chunk = remaining.min(128);
                    let recorder = DebuggingRecorder::new();
                    metrics::with_local_recorder(&recorder, || {
                        let started = Instant::now();
                        for _ in 0..chunk {
                            local_batch_attribution_sample();
                        }
                        elapsed += started.elapsed();
                    });
                    remaining -= chunk;
                }
                elapsed
            });
        });
    }
    group.finish();
    set_get_stage_metrics_enabled(false);
}

fn bench_record_get_object_timeout_operation_start(c: &mut Criterion) {
    c.bench_function("record_get_object_timeout_operation_start", |b| {
        b.iter(record_get_object_timeout_operation_start)
    });
}

fn bench_s3_http_outcomes(c: &mut Criterion) {
    c.bench_function("s3_http_handler_counter", |b| b.iter(|| record_s3_op(black_box(S3Operation::PutObject))));
    c.bench_function("s3_http_handler_counter_with_outcome", |b| {
        b.iter(|| {
            let mut request = S3HttpRequestGuard::new(black_box("PUT"));
            request.in_scope(|| record_s3_op(black_box(S3Operation::PutObject)));
            request.response(black_box(200));
        })
    });
}

fn bench_update_concurrent_requests(c: &mut Criterion) {
    let metrics = PerformanceMetrics::new();

    c.bench_function("performance_metrics_update_concurrent_requests", |b| {
        b.iter(|| metrics.update_concurrent_requests(black_box(64)))
    });
}

fn bench_metrics_collector_record_io_operation(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime for io-metrics benchmark");
    let collector = MetricsCollector::new(Arc::new(PerformanceMetrics::new()), 256);

    c.bench_function("metrics_collector_record_io_operation", |b| {
        b.iter(|| {
            runtime.block_on(collector.record_io_operation(
                black_box(64 * 1024),
                Duration::from_micros(black_box(250)),
                black_box(true),
            ))
        })
    });
}

criterion_group!(
    benches,
    bench_record_get_object_timeout_operation_start,
    bench_s3_http_outcomes,
    bench_update_concurrent_requests,
    bench_metrics_collector_record_io_operation,
    bench_local_batch_attribution
);
criterion_main!(benches);
