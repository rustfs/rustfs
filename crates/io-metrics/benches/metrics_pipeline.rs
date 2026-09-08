use criterion::{Criterion, criterion_group, criterion_main};
use rustfs_io_metrics::{MetricsCollector, PerformanceMetrics, record_get_object_request_started};
use rustfs_io_metrics::{record_s3_op, s3_http_metrics::S3HttpRequestGuard};
use rustfs_s3_ops::S3Operation;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

fn bench_record_get_object_request_started(c: &mut Criterion) {
    c.bench_function("record_get_object_request_started", |b| b.iter(record_get_object_request_started));
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
    bench_record_get_object_request_started,
    bench_s3_http_outcomes,
    bench_update_concurrent_requests,
    bench_metrics_collector_record_io_operation
);
criterion_main!(benches);
