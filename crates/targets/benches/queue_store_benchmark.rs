use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rustfs_targets::store::{QueueStore, Store};
use serde::{Deserialize, Serialize};
use std::hint::black_box;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Clone, Serialize, Deserialize)]
struct BenchEvent {
    bucket: String,
    key: String,
    metadata: Vec<(String, String)>,
    payload: String,
}

fn bench_dir(prefix: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("rustfs-targets-bench-{prefix}-{}", Uuid::new_v4()))
}

fn build_payload(payload_len: usize) -> Vec<u8> {
    let event = BenchEvent {
        bucket: "bench-bucket".to_string(),
        key: format!("objects/{payload_len}/file.json"),
        metadata: (0..8)
            .map(|idx| (format!("x-amz-meta-{idx}"), "benchmark-value".repeat(4)))
            .collect(),
        payload: "abcdefghijklmnopqrstuvwxyz0123456789".repeat(payload_len / 36 + 1)[..payload_len].to_string(),
    };
    serde_json::to_vec(&event).unwrap()
}

fn queue_store_write_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("queue_store_put_raw");

    for payload_size in [512usize, 8 * 1024, 64 * 1024] {
        let payload = build_payload(payload_size);
        group.throughput(Throughput::Bytes(payload.len() as u64));

        for compress in [false, true] {
            let dir = bench_dir(if compress { "put-compress" } else { "put-plain" });
            let store = QueueStore::<BenchEvent>::new_with_compression(&dir, 100_000, ".bench", compress);
            store.open().unwrap();

            group.bench_with_input(
                BenchmarkId::new(if compress { "snap_on" } else { "snap_off" }, payload_size),
                &payload,
                |b, payload| {
                    b.iter(|| {
                        let key = store.put_raw(payload).unwrap();
                        store.del(&key).unwrap();
                    });
                },
            );

            let _ = store.delete();
        }
    }

    group.finish();
}

fn queue_store_read_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("queue_store_get_raw");

    for payload_size in [512usize, 8 * 1024, 64 * 1024] {
        let payload = build_payload(payload_size);
        group.throughput(Throughput::Bytes(payload.len() as u64));

        for compress in [false, true] {
            let dir = bench_dir(if compress { "get-compress" } else { "get-plain" });
            let store = Arc::new(QueueStore::<BenchEvent>::new_with_compression(&dir, 100_000, ".bench", compress));
            store.open().unwrap();
            let key = store.put_raw(&payload).unwrap();

            group.bench_with_input(
                BenchmarkId::new(if compress { "snap_on" } else { "snap_off" }, payload_size),
                &(Arc::clone(&store), key),
                |b, (store, key)| {
                    b.iter(|| {
                        let raw = store.get_raw(key).unwrap();
                        black_box(raw);
                    });
                },
            );

            let _ = store.delete();
        }
    }

    group.finish();
}

criterion_group!(benches, queue_store_write_benchmark, queue_store_read_benchmark);
criterion_main!(benches);
