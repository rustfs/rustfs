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

//! Throughput / latency benchmarks for the object data cache
//! (backlog#1107 production-readiness follow-up).
//!
//! The concurrency correctness was covered by deterministic and stress tests;
//! these measure the GET-path cost the audit only argued about statically. Run
//! with `cargo bench -p rustfs-object-data-cache`.

use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use rustfs_object_data_cache::{ObjectDataCache, ObjectDataCacheConfig, ObjectDataCacheGetRequest, ObjectDataCacheMode};
use std::hint::black_box;

fn fill_cache() -> ObjectDataCache {
    let config = ObjectDataCacheConfig {
        mode: ObjectDataCacheMode::FillBufferedOnly,
        max_bytes: 256 * 1024 * 1024,
        max_entry_bytes: 1024 * 1024,
        // Opt out of the memory gate so fills are deterministic regardless of
        // the host's live memory (matches the test suite's convention).
        min_free_memory_percent: 0,
        ..ObjectDataCacheConfig::default()
    };
    ObjectDataCache::new(config).expect("cache should build")
}

fn request(object: &str) -> ObjectDataCacheGetRequest<'_> {
    ObjectDataCacheGetRequest {
        bucket: "bucket",
        object,
        etag: "etag",
        size: 4096,
        ..Default::default()
    }
}

/// The per-GET planning cost on the hot path: key construction plus the metric
/// label-set hash and recorder dispatch. `plan_get` is synchronous, so this is
/// measured directly with no runtime overhead.
fn bench_plan_get(c: &mut Criterion) {
    let cache = fill_cache();
    c.bench_function("plan_get", |b| {
        b.iter(|| {
            black_box(cache.plan_get(black_box(request("object"))));
        });
    });
}

/// Hit-path latency: lookup of a resident body. Batches 100 lookups per sample
/// so the single `block_on` is amortised out of the measurement.
fn bench_lookup_hit(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread().build().expect("runtime");
    let cache = fill_cache();
    let plan = cache.plan_get(request("hot-object"));
    runtime.block_on(async {
        let _ = cache.fill_body(&plan, Bytes::from(vec![0u8; 4096])).await;
    });

    c.bench_function("lookup_hit_x100", |b| {
        b.iter(|| {
            runtime.block_on(async {
                for _ in 0..100 {
                    black_box(cache.lookup_body(black_box(&plan)).await);
                }
            });
        });
    });
}

/// Fill throughput for distinct cold keys. Batches 100 fills per sample.
fn bench_fill(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread().build().expect("runtime");
    let cache = fill_cache();
    let body = Bytes::from(vec![0u8; 4096]);

    c.bench_function("fill_distinct_x100", |b| {
        let mut n: u64 = 0;
        b.iter(|| {
            runtime.block_on(async {
                for _ in 0..100 {
                    n += 1;
                    let object = format!("fill-{n}");
                    let plan = cache.plan_get(request(&object));
                    black_box(cache.fill_body(black_box(&plan), body.clone()).await);
                }
            });
        });
    });
}

criterion_group!(benches, bench_plan_get, bench_lookup_hit, bench_fill);
criterion_main!(benches);
