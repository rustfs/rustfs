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

use crate::bucket::bandwidth::monitor::{BucketThrottle, Monitor};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};
use tokio::time::Sleep;
use tracing::debug;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BucketOptions {
    pub name: String,
    pub replication_arn: String,
}

pub struct MonitorReaderOptions {
    pub bucket_options: BucketOptions,
    pub header_size: i32,
}

struct WaitState {
    sleep: Pin<Box<Sleep>>,
    need: usize,
}

pub struct MonitoredReader<R> {
    r: R,
    throttle: Option<BucketThrottle>,
    last_err: Option<std::io::Error>,
    #[allow(dead_code)]
    m: Arc<Monitor>,
    opts: MonitorReaderOptions,
    wait_state: std::sync::Mutex<Option<WaitState>>,
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
            throttle,
            last_err: None,
            m,
            opts,
            wait_state: std::sync::Mutex::new(None),
        }
    }
}

impl<R: std::io::Read> std::io::Read for MonitoredReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let throttle = match &self.throttle {
            Some(t) => t.clone(),
            None => return self.r.read(buf),
        };
        if let Some(ref e) = self.last_err {
            return Err(std::io::Error::new(e.kind(), e.to_string()));
        }

        let b = throttle.burst();
        let mut need = buf.len();
        let hdr = self.opts.header_size;
        let tokens: u64 = if hdr > 0 {
            if (hdr as u64) < b {
                self.opts.header_size = 0;
                need = ((b - hdr as u64) as usize).min(need);
                need as u64 + hdr as u64
            } else {
                self.opts.header_size -= b as i32 - 1;
                need = 1;
                b
            }
        } else {
            need = need.min(b as usize);
            need as u64
        };

        let av = throttle.tokens();
        let tokens = if av < tokens && av > 0 {
            need = need.min(av as usize);
            av
        } else {
            tokens
        };

        throttle.wait_n(tokens)?;
        match self.r.read(&mut buf[..need]) {
            Ok(n) => Ok(n),
            Err(e) => {
                self.last_err = Some(std::io::Error::new(e.kind(), e.to_string()));
                Err(e)
            }
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for MonitoredReader<R> {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();

        {
            let mut guard = this.wait_state.lock().unwrap();
            if let Some(ref mut ws) = *guard {
                match ws.sleep.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(()) => {
                        let need = ws.need;
                        *guard = None;
                        drop(guard);
                        return poll_limited_read(&mut this.r, cx, buf, need, &mut this.last_err);
                    }
                }
            }
        }

        let throttle = match &this.throttle {
            Some(t) => t,
            None => return Pin::new(&mut this.r).poll_read(cx, buf),
        };

        if let Some(ref e) = this.last_err {
            return Poll::Ready(Err(std::io::Error::new(e.kind(), e.to_string())));
        }

        let b = throttle.burst();
        let mut need = buf.remaining();
        let hdr = this.opts.header_size;
        let tokens: u64 = if hdr > 0 {
            if (hdr as u64) < b {
                this.opts.header_size = 0;
                need = ((b - hdr as u64) as usize).min(need);
                need as u64 + hdr as u64
            } else {
                this.opts.header_size -= b as i32 - 1;
                need = 1;
                b
            }
        } else {
            need = need.min(b as usize);
            need as u64
        };

        let av = throttle.tokens();
        let tokens = if av < tokens && av > 0 {
            need = need.min(av as usize);
            av
        } else {
            tokens
        };

        let (deficit, rate) = throttle.consume(tokens);

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
                    *this.wait_state.lock().unwrap() = Some(WaitState { sleep, need });
                    return Poll::Pending;
                }
                Poll::Ready(()) => {}
            }
        }

        poll_limited_read(&mut this.r, cx, buf, need, &mut this.last_err)
    }
}

fn poll_limited_read<R: AsyncRead + Unpin>(
    r: &mut R,
    cx: &mut Context<'_>,
    buf: &mut ReadBuf<'_>,
    limit: usize,
    last_err: &mut Option<std::io::Error>,
) -> Poll<std::io::Result<()>> {
    let remaining = buf.remaining();
    if limit == 0 || remaining == 0 {
        return Poll::Ready(Ok(()));
    }

    if remaining <= limit {
        return match Pin::new(r).poll_read(cx, buf) {
            Poll::Ready(Err(e)) => {
                *last_err = Some(std::io::Error::new(e.kind(), e.to_string()));
                Poll::Ready(Err(e))
            }
            other => other,
        };
    }

    let mut temp = vec![0u8; limit];
    let mut temp_buf = ReadBuf::new(&mut temp);
    match Pin::new(r).poll_read(cx, &mut temp_buf) {
        Poll::Ready(Ok(())) => {
            buf.put_slice(temp_buf.filled());
            Poll::Ready(Ok(()))
        }
        Poll::Ready(Err(e)) => {
            *last_err = Some(std::io::Error::new(e.kind(), e.to_string()));
            Poll::Ready(Err(e))
        }
        Poll::Pending => Poll::Pending,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::task::noop_waker_ref;
    use std::io;
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
        let mut last_err = None;
        let waker = noop_waker_ref();
        let mut cx = Context::from_waker(waker);

        let mut read_buf = ReadBuf::new(&mut out);
        let first = poll_limited_read(&mut inner, &mut cx, &mut read_buf, 3, &mut last_err);
        assert!(matches!(first, Poll::Ready(Ok(()))));
        assert_eq!(read_buf.filled(), b"abc");
        assert!(last_err.is_none());

        let second = poll_limited_read(&mut inner, &mut cx, &mut read_buf, 3, &mut last_err);
        assert!(matches!(second, Poll::Ready(Ok(()))));
        assert_eq!(read_buf.filled(), b"abcdef");
        assert!(last_err.is_none());
    }
}
