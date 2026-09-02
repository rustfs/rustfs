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

//! Throughput of `tee_reader` versus reading the same source directly:
//! 64 MiB of data served in 1 MiB chunks, consumed with 1 MiB reads.

use bytes::Bytes;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use rustfs_rio::tee_reader;
use std::hint::black_box;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};

const CHUNK_BYTES: usize = 1024 * 1024;
const TOTAL_BYTES: usize = 64 * 1024 * 1024;
const TEE_BUFFER_BYTES: usize = 4 * CHUNK_BYTES;

/// In-memory source that serves at most `CHUNK_BYTES` per poll.
struct ChunkedSource {
    data: Bytes,
    pos: usize,
}

impl AsyncRead for ChunkedSource {
    fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let remaining = self.data.len() - self.pos;
        let n = CHUNK_BYTES.min(remaining).min(buf.remaining());
        buf.put_slice(&self.data[self.pos..self.pos + n]);
        self.pos += n;
        Poll::Ready(Ok(()))
    }
}

async fn consume<R: AsyncRead + Unpin>(mut reader: R) -> usize {
    let mut buf = vec![0u8; CHUNK_BYTES];
    let mut total = 0;
    loop {
        let n = reader.read(&mut buf).await.expect("read");
        if n == 0 {
            return total;
        }
        total += n;
    }
}

fn bench_tee_reader(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build tokio runtime for tee_reader benchmark");
    let data = Bytes::from(vec![0xA5u8; TOTAL_BYTES]);

    let mut group = c.benchmark_group("tee_reader_64mib_1mib_chunks");
    group.throughput(Throughput::Bytes(TOTAL_BYTES as u64));
    group.sample_size(10);

    group.bench_function("direct_read", |b| {
        b.iter(|| {
            let source = ChunkedSource {
                data: data.clone(),
                pos: 0,
            };
            let total = runtime.block_on(consume(source));
            black_box(total)
        })
    });

    group.bench_function("tee_primary_plus_secondary", |b| {
        b.iter(|| {
            let source = ChunkedSource {
                data: data.clone(),
                pos: 0,
            };
            let (primary, secondary) = tee_reader(source, TEE_BUFFER_BYTES);
            let totals = runtime.block_on(async {
                let secondary_task = tokio::spawn(consume(secondary));
                let primary_total = consume(primary).await;
                let secondary_total = secondary_task.await.expect("secondary task");
                (primary_total, secondary_total)
            });
            black_box(totals)
        })
    });

    group.finish();
}

criterion_group!(benches, bench_tee_reader);
criterion_main!(benches);
