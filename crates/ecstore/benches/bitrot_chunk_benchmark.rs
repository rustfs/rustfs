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

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rustfs_ecstore::bitrot::decode_bitrot_chunk_source_for_bench;
use rustfs_io_core::IoChunk;
use rustfs_utils::HashAlgorithm;
use std::hint::black_box;

struct BitrotChunkBenchCase {
    name: &'static str,
    source_chunks: Vec<IoChunk>,
    shard_size: usize,
    expected_decoded_len: usize,
    expected_copied: bool,
}

fn encode_shard(checksum_algo: HashAlgorithm, shard: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(checksum_algo.size() + shard.len());
    encoded.extend_from_slice(checksum_algo.hash_encode(shard).as_ref());
    encoded.extend_from_slice(shard);
    encoded
}

fn bitrot_chunk_bench_cases() -> [BitrotChunkBenchCase; 2] {
    let checksum_algo = HashAlgorithm::Md5;
    let shard_one = b"abcd";
    let shard_two = b"efgh";
    let encoded_one = encode_shard(checksum_algo.clone(), shard_one);
    let encoded_two = encode_shard(checksum_algo.clone(), shard_two);

    let mut cross_chunk = Vec::with_capacity(encoded_one.len() + encoded_two.len());
    cross_chunk.extend_from_slice(&encoded_one);
    cross_chunk.extend_from_slice(&encoded_two);
    let split = checksum_algo.size() + 2;

    [
        BitrotChunkBenchCase {
            name: "aligned_multi_chunk_no_copy",
            source_chunks: vec![
                IoChunk::Shared(Bytes::from(encoded_one)),
                IoChunk::Shared(Bytes::from(encoded_two)),
            ],
            shard_size: shard_one.len(),
            expected_decoded_len: shard_one.len() + shard_two.len(),
            expected_copied: false,
        },
        BitrotChunkBenchCase {
            name: "cross_chunk_frame_copy",
            source_chunks: vec![
                IoChunk::Shared(Bytes::copy_from_slice(&cross_chunk[..split])),
                IoChunk::Shared(Bytes::copy_from_slice(&cross_chunk[split..])),
            ],
            shard_size: shard_one.len(),
            expected_decoded_len: shard_one.len() + shard_two.len(),
            expected_copied: true,
        },
    ]
}

fn bench_bitrot_chunk_decode(c: &mut Criterion) {
    let checksum_algo = HashAlgorithm::Md5;
    let mut group = c.benchmark_group("bitrot_chunk_decode");
    group.sample_size(20);

    for case in bitrot_chunk_bench_cases() {
        let (decoded, copied) =
            decode_bitrot_chunk_source_for_bench(&case.source_chunks, case.shard_size, checksum_algo.clone(), false)
                .expect("decode bitrot source");
        let decoded_len: usize = decoded.iter().map(IoChunk::len).sum();

        assert_eq!(decoded_len, case.expected_decoded_len);
        assert_eq!(copied, case.expected_copied);

        group.throughput(Throughput::Bytes(case.expected_decoded_len as u64));
        group.bench_with_input(BenchmarkId::new("decode", case.name), &case, |b, case| {
            b.iter(|| {
                let result = decode_bitrot_chunk_source_for_bench(
                    black_box(&case.source_chunks),
                    black_box(case.shard_size),
                    checksum_algo.clone(),
                    false,
                )
                .expect("decode bitrot source");
                black_box(result);
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_bitrot_chunk_decode);
criterion_main!(benches);
