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

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rustfs_utils::HashAlgorithm;
use std::hint::black_box;

fn generate_payload(size: usize) -> Vec<u8> {
    (0..size)
        .map(|i| u8::try_from(i % 251).expect("modulo output fits in u8"))
        .collect()
}

fn bench_hash_hotpaths(c: &mut Criterion) {
    let payloads = [
        ("64KiB", generate_payload(64 * 1024)),
        ("1MiB", generate_payload(1024 * 1024)),
    ];
    let algorithms = [
        ("md5", HashAlgorithm::Md5),
        ("sha256", HashAlgorithm::SHA256),
        ("highwayhash256s", HashAlgorithm::HighwayHash256S),
        ("highwayhash256s_legacy", HashAlgorithm::HighwayHash256SLegacy),
    ];

    let mut group = c.benchmark_group("hash_hotpath");
    for (payload_name, payload) in &payloads {
        let payload_len = u64::try_from(payload.len()).expect("benchmark payload length fits in u64");
        group.throughput(Throughput::Bytes(payload_len));
        for (algo_name, algorithm) in &algorithms {
            let algorithm = algorithm.clone();
            group.bench_with_input(BenchmarkId::new(*algo_name, payload_name), payload.as_slice(), move |b, payload| {
                b.iter(|| {
                    let hash = algorithm.hash_encode(black_box(payload));
                    black_box(hash.as_ref()[0]);
                });
            });
        }
    }
    group.finish();
}

criterion_group!(benches, bench_hash_hotpaths);
criterion_main!(benches);
