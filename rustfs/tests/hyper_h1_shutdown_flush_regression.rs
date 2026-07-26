// Regression guard for rustfs/backlog#1232 — sporadic large-object GET
// `unexpected EOF` under load.
//
// Root cause was in hyper, not RustFS: hyper <= 1.10.1 could call
// `poll_shutdown()` on the HTTP/1 socket while response bytes were still
// buffered (a prior `poll_flush()` returned `Poll::Pending` and the result was
// discarded). A backpressured / slow-reading peer then received a graceful FIN
// before the full Content-Length body was flushed, which S3 clients
// (minio-go / warp) report as `unexpected EOF`. Fixed upstream by
// hyperium/hyper#4018 (commit 72046cc7, "fix(http1): flush buffered data
// before shutdown"), released in hyper 1.11.0.
//
// This test reproduces the exact trigger deterministically and fails if hyper
// is downgraded below the fix. It mirrors
// hyper's own upstream regression test `tests/h1_shutdown_while_buffered.rs`.
//
// The load-bearing assertion is on `shutdown_called_with_buffered`, a flag set
// synchronously inside `poll_shutdown`. It is timing-independent: a slow CI
// runner can only fail to reach the shutdown at all (a false pass), never flip
// the flag spuriously (a false red).

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use http::{Request, Response};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{sleep, timeout};

const RESP_BODY_LEN: usize = 500_000;
// Accept one partial write, then pend forever — the boundary that fills hyper's
// write buffer and used to trip the premature shutdown. Same value hyper uses.
const WRITE_CHUNK: usize = 212_992;

#[derive(Debug, Default)]
struct Stats {
    bytes_written: usize,
    total_attempted: usize,
    shutdown_called_with_buffered: bool,
    buffered_at_shutdown: usize,
}

/// A socket wrapper that accepts a single partial write and then pends on every
/// further write, simulating a backpressured peer whose receive window is full
/// exactly at the write-buffer boundary.
struct PendingStream {
    inner: TcpStream,
    write_count: usize,
    stats: Arc<Mutex<Stats>>,
}

impl AsyncRead for PendingStream {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for PendingStream {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        self.write_count += 1;
        self.stats.lock().unwrap().total_attempted += buf.len();

        if self.write_count == 1 {
            let partial = std::cmp::min(buf.len(), WRITE_CHUNK);
            let result = Pin::new(&mut self.inner).poll_write(cx, &buf[..partial]);
            if let Poll::Ready(Ok(n)) = result {
                self.stats.lock().unwrap().bytes_written += n;
            }
            return result;
        }
        Poll::Pending
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let buffered = {
            let s = self.stats.lock().unwrap();
            s.total_attempted - s.bytes_written
        };
        // Cannot flush while the peer refuses more data.
        if buffered > 0 {
            return Poll::Pending;
        }
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        {
            let mut s = self.stats.lock().unwrap();
            let buffered = s.total_attempted - s.bytes_written;
            if buffered > 0 {
                s.shutdown_called_with_buffered = true;
                s.buffered_at_shutdown = buffered;
            }
        }
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hyper_h1_does_not_shutdown_with_buffered_response() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let stats = Arc::new(Mutex::new(Stats::default()));

    let stats_srv = stats.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let ps = PendingStream {
            inner: stream,
            write_count: 0,
            stats: stats_srv,
        };
        let service = service_fn(|_req: Request<Incoming>| async move {
            Ok::<_, hyper::Error>(Response::new(Full::new(Bytes::from(vec![b'X'; RESP_BODY_LEN]))))
        });
        hyper::server::conn::http1::Builder::new()
            .serve_connection(TokioIo::new(ps), service)
            .await
    });

    sleep(Duration::from_millis(50)).await;

    // Client: send one complete request, then hold the connection open. It does
    // not need to read — the backpressure is modelled by PendingStream.
    let client = tokio::spawn(async move {
        use tokio::io::AsyncWriteExt;
        let mut s = TcpStream::connect(addr).await.unwrap();
        s.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n").await.unwrap();
        s.flush().await.unwrap();
        sleep(Duration::from_secs(2)).await;
    });

    // On buggy hyper the connection future resolves after the wrongful shutdown;
    // on fixed hyper it parks (flush pends), so a timeout here is expected and
    // harmless — the verdict is the flag below, not the future's completion.
    let _ = timeout(Duration::from_millis(1500), server).await;
    client.abort();

    let s = stats.lock().unwrap();
    assert!(
        !s.shutdown_called_with_buffered,
        "hyper shut the HTTP/1 socket down with {} response bytes still buffered — the backlog#1232 \
         premature-FIN bug is back; hyperium/hyper#4018 may have regressed",
        s.buffered_at_shutdown
    );
}
