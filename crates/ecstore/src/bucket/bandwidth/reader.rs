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

use crate::bucket::bandwidth::monitor::Monitor;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};
use tokio::time::Sleep;
use tracing::{debug, warn};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BucketOptions {
    pub name: String,
    pub replication_arn: String,
}

pub struct MonitorReaderOptions {
    pub bucket_options: BucketOptions,
    pub header_size: usize,
}

struct WaitState {
    sleep: Pin<Box<Sleep>>,
}

pub struct MonitoredReader<R> {
    r: R,
    m: Arc<Monitor>,
    opts: MonitorReaderOptions,
    wait_state: std::sync::Mutex<Option<WaitState>>,
    temp_buf: Vec<u8>,
}

impl<R> MonitoredReader<R> {
    pub fn new(m: Arc<Monitor>, r: R, opts: MonitorReaderOptions) -> Self {
        let throttle = m.throttle(&opts.bucket_options);
        debug!(
            bucket = opts.bucket_options.name,
            arn = opts.bucket_options.replication_arn,
            throttle_active = throttle.is_some(),
            limit_bps = throttle.as_ref().map(|t| t.node_bandwidth_per_sec).unwrap_or(0),
            header_size = opts.header_size,
            "MonitoredReader created"
        );
        MonitoredReader {
            r,
            m,
            opts,
            wait_state: std::sync::Mutex::new(None),
            temp_buf: Vec::new(),
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for MonitoredReader<R> {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();

        {
            let mut guard = this.wait_state.lock().unwrap_or_else(|e| {
                warn!("MonitoredReader wait_state mutex poisoned, recovering");
                e.into_inner()
            });
            if let Some(ref mut ws) = *guard {
                match ws.sleep.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(()) => {
                        *guard = None;
                        drop(guard);
                    }
                }
            }
        }

        let throttle = match this.m.throttle(&this.opts.bucket_options) {
            Some(t) => t,
            None => return Pin::new(&mut this.r).poll_read(cx, buf),
        };

        let b = throttle.burst();
        debug_assert!(b >= 1, "burst must be at least 1");
        let (need, tokens) = calc_need_and_tokens(b, buf.remaining(), &mut this.opts.header_size);
        let (deficit, rate, consumed) = throttle.consume(tokens);
        let need = need.min(consumed as usize);

        if deficit > 0 && rate > 0.0 {
            let duration = std::time::Duration::from_secs_f64(deficit as f64 / rate);
            debug!(
                tokens = tokens,
                deficit = deficit,
                rate = rate,
                sleep_ms = duration.as_millis() as u64,
                "bandwidth throttle sleep"
            );
            let mut sleep = Box::pin(tokio::time::sleep(duration));
            match sleep.as_mut().poll(cx) {
                Poll::Pending => {
                    *this.wait_state.lock().unwrap_or_else(|e| {
                        warn!("MonitoredReader wait_state mutex poisoned, recovering");
                        e.into_inner()
                    }) = Some(WaitState { sleep });
                    return Poll::Pending;
                }
                Poll::Ready(()) => {}
            }
        }

        poll_limited_read(&mut this.r, cx, buf, need, &mut this.temp_buf)
    }
}

fn calc_need_and_tokens(burst: u64, need_upper: usize, header_size: &mut usize) -> (usize, u64) {
    let hdr = *header_size;
    let mut need = need_upper;
    let tokens: u64 = if hdr > 0 {
        if (hdr as u64) < burst {
            *header_size = 0;
            need = ((burst - hdr as u64) as usize).min(need);
            need as u64 + hdr as u64
        } else {
            *header_size -= burst as usize;
            need = 0;
            burst
        }
    } else {
        need = need.min(burst as usize);
        need as u64
    };
    (need, tokens)
}

fn poll_limited_read<R: AsyncRead + Unpin>(
    r: &mut R,
    cx: &mut Context<'_>,
    buf: &mut ReadBuf<'_>,
    limit: usize,
    reusable_buf: &mut Vec<u8>,
) -> Poll<std::io::Result<()>> {
    let remaining = buf.remaining();
    if limit == 0 || remaining == 0 {
        return Poll::Ready(Ok(()));
    }

    if remaining <= limit {
        return Pin::new(r).poll_read(cx, buf);
    }

    reusable_buf.resize(limit, 0);
    let mut temp_buf = ReadBuf::new(&mut reusable_buf[..limit]);
    match Pin::new(r).poll_read(cx, &mut temp_buf) {
        Poll::Ready(Ok(())) => {
            buf.put_slice(temp_buf.filled());
            Poll::Ready(Ok(()))
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::task::noop_waker_ref;
    use std::io;
    use std::pin::Pin;
    use std::task::Context;
    use tokio::io::AsyncReadExt;

    #[derive(Default)]
    struct TestAsyncReader {
        data: Vec<u8>,
        pos: usize,
    }

    impl TestAsyncReader {
        fn new(data: &[u8]) -> Self {
            Self {
                data: data.to_vec(),
                pos: 0,
            }
        }
    }

    impl AsyncRead for TestAsyncReader {
        fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            if self.pos >= self.data.len() {
                return Poll::Ready(Ok(()));
            }

            let remaining = self.data.len() - self.pos;
            let n = remaining.min(buf.remaining());
            buf.put_slice(&self.data[self.pos..self.pos + n]);
            self.pos += n;
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn test_monitored_reader_passthrough_when_throttle_absent() {
        let monitor = Monitor::new(1);
        let opts = MonitorReaderOptions {
            bucket_options: BucketOptions {
                name: "b1".to_string(),
                replication_arn: "arn1".to_string(),
            },
            header_size: 0,
        };
        let inner = TestAsyncReader::new(b"hello-world");
        let mut reader = MonitoredReader::new(monitor, inner, opts);

        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, b"hello-world");
    }

    #[test]
    fn test_poll_limited_read_honors_limit() {
        let mut inner = TestAsyncReader::new(b"abcdef");
        let mut out = [0u8; 8];
        let mut reusable = Vec::new();
        let waker = noop_waker_ref();
        let mut cx = Context::from_waker(waker);

        let mut read_buf = ReadBuf::new(&mut out);
        let first = poll_limited_read(&mut inner, &mut cx, &mut read_buf, 3, &mut reusable);
        assert!(matches!(first, Poll::Ready(Ok(()))));
        assert_eq!(read_buf.filled(), b"abc");

        let second = poll_limited_read(&mut inner, &mut cx, &mut read_buf, 3, &mut reusable);
        assert!(matches!(second, Poll::Ready(Ok(()))));
        assert_eq!(read_buf.filled(), b"abcdef");
    }

    #[tokio::test]
    async fn test_header_only_consumption_skips_io() {
        let monitor = Monitor::new(1);
        monitor.set_bandwidth_limit("b1", "arn1", 10);

        let data = vec![0xAAu8; 20];
        let inner = TestAsyncReader::new(&data);
        let opts = MonitorReaderOptions {
            bucket_options: BucketOptions {
                name: "b1".to_string(),
                replication_arn: "arn1".to_string(),
            },
            header_size: 25,
        };
        let mut reader = MonitoredReader::new(monitor, inner, opts);

        let waker = noop_waker_ref();
        let mut cx = Context::from_waker(waker);
        let mut out = [0u8; 16];
        let mut read_buf = ReadBuf::new(&mut out);

        let poll = Pin::new(&mut reader).poll_read(&mut cx, &mut read_buf);
        assert!(matches!(poll, Poll::Ready(Ok(())) | Poll::Pending));
        assert!(read_buf.filled().is_empty());
        assert_eq!(reader.opts.header_size, 15);
    }

    #[tokio::test]
    async fn test_monitored_reader_with_throttle_reads_all_data() {
        let monitor = Monitor::new(1);
        monitor.set_bandwidth_limit("b1", "arn1", 1024);

        let data = vec![0xABu8; 4096];
        let inner = TestAsyncReader::new(&data);
        let opts = MonitorReaderOptions {
            bucket_options: BucketOptions {
                name: "b1".to_string(),
                replication_arn: "arn1".to_string(),
            },
            header_size: 0,
        };
        let mut reader = MonitoredReader::new(monitor, inner, opts);

        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.unwrap();
        assert_eq!(out.len(), 4096);
        assert!(out.iter().all(|&b| b == 0xAB));
    }

    #[tokio::test]
    async fn test_monitored_reader_header_size_accounting() {
        let monitor = Monitor::new(1);
        monitor.set_bandwidth_limit("b1", "arn1", 100);

        let data = vec![0u8; 200];
        let inner = TestAsyncReader::new(&data);
        let opts = MonitorReaderOptions {
            bucket_options: BucketOptions {
                name: "b1".to_string(),
                replication_arn: "arn1".to_string(),
            },
            header_size: 50,
        };
        let mut reader = MonitoredReader::new(monitor, inner, opts);

        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.unwrap();
        assert_eq!(out.len(), 200);
        assert_eq!(reader.opts.header_size, 0);
    }

    #[tokio::test]
    async fn test_monitored_reader_very_small_limit() {
        let monitor = Monitor::new(1);
        monitor.set_bandwidth_limit("b1", "arn1", 1);

        let data = vec![0xFFu8; 10];
        let inner = TestAsyncReader::new(&data);
        let opts = MonitorReaderOptions {
            bucket_options: BucketOptions {
                name: "b1".to_string(),
                replication_arn: "arn1".to_string(),
            },
            header_size: 0,
        };
        let mut reader = MonitoredReader::new(monitor, inner, opts);

        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.unwrap();
        assert_eq!(out.len(), 10);
    }
}
