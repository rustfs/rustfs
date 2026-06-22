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
use rustfs_ecstore::api::erasure::{BitrotWriterWrapper, CustomWriter, Erasure};
use rustfs_utils::HashAlgorithm;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Debug)]
struct BenchConfig {
    name: String,
    payload_size: usize,
    data_shards: usize,
    parity_shards: usize,
    block_size: usize,
}

impl BenchConfig {
    fn new(payload_size: usize, data_shards: usize, parity_shards: usize, block_size: usize) -> Self {
        Self {
            name: format!(
                "payload={}KB_{}+{}_block={}KB",
                payload_size / 1024,
                data_shards,
                parity_shards,
                block_size / 1024
            ),
            payload_size,
            data_shards,
            parity_shards,
            block_size,
        }
    }
}

fn generate_payload(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 251) as u8).collect()
}

fn build_non_inline_writers(config: &BenchConfig) -> Vec<Option<BitrotWriterWrapper>> {
    let erasure = Erasure::new(config.data_shards, config.parity_shards, config.block_size);
    let total_shards = config.data_shards + config.parity_shards;
    let shard_size = erasure.shard_size();
    (0..total_shards)
        .map(|_| {
            Some(BitrotWriterWrapper::new(
                CustomWriter::new_tokio_writer(tokio::io::sink()),
                shard_size,
                HashAlgorithm::HighwayHash256S,
            ))
        })
        .collect()
}

fn bench_single_block_non_inline_fast_path(c: &mut Criterion) {
    let configs = vec![
        BenchConfig::new(4 * 1024, 4, 2, 128 * 1024),
        BenchConfig::new(64 * 1024, 4, 2, 128 * 1024),
        BenchConfig::new(128 * 1024, 4, 2, 128 * 1024),
    ];

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("benchmark runtime");

    let mut group = c.benchmark_group("single_block_non_inline_write_path");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(6));

    for config in configs {
        let payload = generate_payload(config.payload_size);
        let erasure = Arc::new(Erasure::new(config.data_shards, config.parity_shards, config.block_size));
        group.throughput(Throughput::Bytes(config.payload_size as u64));

        group.bench_with_input(BenchmarkId::new("encode_pipeline", &config.name), &config, |b, config| {
            let payload = payload.clone();
            let erasure = Arc::clone(&erasure);
            b.iter(|| {
                let mut writers = build_non_inline_writers(config);
                let reader = tokio::io::BufReader::new(Cursor::new(payload.clone()));
                rt.block_on(async {
                    erasure
                        .clone()
                        .encode(reader, &mut writers, config.data_shards)
                        .await
                        .expect("encode pipeline benchmark");
                });
            });
        });

        group.bench_with_input(BenchmarkId::new("single_block_candidate", &config.name), &config, |b, config| {
            let payload = payload.clone();
            let erasure = Arc::clone(&erasure);
            b.iter(|| {
                let mut writers = build_non_inline_writers(config);
                let reader = tokio::io::BufReader::new(Cursor::new(payload.clone()));
                rt.block_on(async {
                    erasure
                        .clone()
                        .encode_single_block_non_inline(reader, &mut writers, config.data_shards)
                        .await
                        .expect("single block candidate benchmark");
                });
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_single_block_non_inline_fast_path);
criterion_main!(benches);
