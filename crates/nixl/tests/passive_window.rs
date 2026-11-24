#![cfg(all(target_os = "linux", feature = "client-http"))]

use aws_credential_types::Credentials;
use reqwest::{Client, Method};
use rustfs_nixl::{
    ClientWindowHttp, NIXL_AGENT_HEADER, NIXL_BUFFER_HEADER, NIXL_METADATA_HEADER, NixlConfig, NixlServer,
    RDMA_BYTES_TRANSFERRED_HEADER, RDMA_REPLY_HEADER, RdmaToken, RegisteredClientWindow,
};
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;

fn config(name: &str, size: usize, windows: usize) -> NixlConfig {
    NixlConfig {
        agent_name: format!("{name}-{}", std::process::id()),
        chunk_bytes: size,
        pool_bytes: size * windows,
        ..NixlConfig::default()
    }
}

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("HTTP and NIXL runtime")
}

fn clients() -> (Client, ClientWindowHttp) {
    let transport = ClientWindowHttp::new(Client::builder().no_proxy()).expect("loopback HTTP transport");
    (Client::new(), transport)
}

fn serve(handler: impl FnOnce(Method, RdmaToken, &mut TcpStream) + Send + 'static) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).expect("bind HTTP peer");
    let url = format!("http://{}/object", listener.local_addr().expect("peer address"));
    let peer = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("HTTP request");
        let mut lines = BufReader::new(&stream).lines().map(|line| line.expect("HTTP header line"));
        let first = lines.next().expect("request line");
        let method = first.split_once(' ').expect("request method").0.parse().expect("HTTP method");
        let headers: Vec<_> = lines.take_while(|line| !line.is_empty()).collect();
        let header = |name: &str| {
            headers
                .iter()
                .filter_map(|line| line.split_once(':'))
                .find(|(k, _)| k.eq_ignore_ascii_case(name))
                .expect(name)
                .1
                .trim()
        };
        let authorization = header("authorization");
        assert!(authorization.starts_with("AWS4-HMAC-SHA256 "));
        let (_, fields) = authorization.split_once("SignedHeaders=").expect("SigV4 covered headers");
        let (fields, _) = fields.split_once(',').expect("SigV4 signature");
        for field in [NIXL_AGENT_HEADER, NIXL_METADATA_HEADER, NIXL_BUFFER_HEADER] {
            assert!(fields.split(';').any(|signed| signed == field), "unsigned {field}");
        }
        let token =
            RdmaToken::from_headers(header(NIXL_AGENT_HEADER), Some(header(NIXL_METADATA_HEADER)), header(NIXL_BUFFER_HEADER))
                .expect("parse the descriptor received over HTTP");
        handler(method, token, &mut stream);
    });
    (url, peer)
}

fn credentials() -> Credentials {
    Credentials::new("fixture-key", "fixture-secret", None, None, "loopback-test")
}

fn receipt(stream: &mut TcpStream, status: &str, bytes: usize) {
    let code = status.split_once(' ').expect("status code").0;
    write!(stream, "HTTP/1.1 {status}\r\n{RDMA_REPLY_HEADER}: {code}\r\n{RDMA_BYTES_TRANSFERRED_HEADER}: {bytes}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").expect("send RDMA receipt");
}

#[test]
#[ignore = "requires native NIXL and its UCX backend"]
fn peer_reads_and_writes_the_remaining_registered_window() {
    const SIZE: usize = 4 * 1024 * 1024;
    let runtime = runtime();
    let (client, http) = clients();
    let windows = RegisteredClientWindow::register(&config("passive", SIZE, 2)).expect("register two windows");
    let [sibling, mut window]: [_; 2] = windows.try_into().ok().expect("two windows");
    drop(sibling);
    let peer = NixlServer::new(&config("active", SIZE, 1)).expect("create peer agent");
    let upload: Vec<u8> = (0..=250).cycle().take(SIZE).collect();
    let download: Vec<u8> = upload.iter().map(|byte| !byte).collect();
    window.write_at(0, &upload).expect("write upload source");

    let handle = runtime.handle().clone();
    let reader = Arc::clone(&peer);
    let (url, server) = serve(move |method, token, stream| {
        assert_eq!(method, Method::PUT);
        let chunk = reader.try_reserve().expect("reserve peer staging");
        let chunk = handle
            .block_on(reader.read_from_client_owned("object", &token, 0, SIZE, chunk))
            .expect("peer READ");
        assert!(chunk.staged().expect("read payload") == upload, "peer READ returned different bytes");
        receipt(stream, "200 OK", SIZE);
    });
    let guard = window.begin(SIZE).expect("begin PUT");
    let builder = client.put(&url);
    let mut window = runtime
        .block_on(guard.exchange(&http, builder, &credentials(), "us-east-1"))
        .expect("PUT exchange")
        .window
        .expect("confirmed PUT");
    server.join().expect("PUT peer");

    for ranged in [false, true] {
        let handle = runtime.handle().clone();
        let writer = Arc::clone(&peer);
        let payload = download.clone();
        let (url, server) = serve(move |method, token, stream| {
            assert_eq!(method, Method::GET);
            let mut chunk = writer.try_reserve().expect("reserve peer staging");
            chunk.append(&payload).expect("stage a distinct reply");
            let (written, _chunk) = handle
                .block_on(writer.write_to_client_owned("object", &token, 0, chunk))
                .expect("peer WRITE");
            assert_eq!(written, SIZE as u64);
            receipt(stream, if ranged { "206 Partial Content" } else { "200 OK" }, SIZE);
        });
        let mut bytes = vec![0; SIZE];
        window.write_at(0, &bytes).expect("clear the destination before each GET");
        let guard = window.begin(SIZE).expect("begin GET");
        let mut builder = client.get(&url);
        if ranged {
            builder = builder.header("range", format!("bytes=0-{}", SIZE - 1));
        }
        window = runtime
            .block_on(guard.exchange(&http, builder, &credentials(), "us-east-1"))
            .expect("GET exchange")
            .window
            .expect("confirmed GET");
        window.read_at(0, &mut bytes).expect("read completed GET");
        assert!(bytes == download, "peer WRITE returned different bytes");
        server.join().expect("GET peer");
    }
}

#[test]
#[ignore = "requires native NIXL and its UCX backend"]
fn exchange_rejects_each_reserved_header_before_sending() {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).expect("bind HTTP peer");
    listener.set_nonblocking(true).expect("observe whether any request was sent");
    let url = format!("http://{}/object", listener.local_addr().expect("peer address"));
    let runtime = runtime();
    let (client, http) = clients();
    let fields = [NIXL_AGENT_HEADER, NIXL_METADATA_HEADER, NIXL_BUFFER_HEADER];
    let windows = RegisteredClientWindow::register(&config("bad-request", 4096, fields.len())).expect("register windows");
    assert_eq!(windows.len(), fields.len());
    for (field, window) in fields.into_iter().zip(windows) {
        let guard = window.begin(4096).expect("begin PUT");
        let builder = client.put(&url).header(field, "other-window").timeout(Duration::from_secs(1));
        assert!(
            matches!(
                runtime.block_on(guard.exchange(&http, builder, &credentials(), "us-east-1")),
                Err(rustfs_nixl::NixlError::Token(_))
            ),
            "mismatched {field}"
        );
    }
    assert_eq!(
        listener.accept().expect_err("no connection was opened").kind(),
        std::io::ErrorKind::WouldBlock
    );
}

#[test]
#[ignore = "requires native NIXL and its UCX backend"]
fn redirect_keeps_the_window_quarantined_and_never_reaches_the_other_endpoint() {
    let other = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).expect("bind redirect destination");
    other.set_nonblocking(true).expect("observe whether a connection was made");
    let location = format!("http://{}/elsewhere", other.local_addr().expect("redirect address"));
    let (url, server) = serve(move |method, _token, stream| {
        assert_eq!(method, Method::PUT);
        write!(
            stream,
            "HTTP/1.1 307 Temporary Redirect\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        )
        .expect("redirect the signed request");
    });
    let (client, http) = clients();
    let mut windows = RegisteredClientWindow::register(&config("redirect", 4096, 1)).expect("register one window");
    let guard = windows.pop().expect("registered window").begin(4096).expect("prepare PUT");
    let response = runtime()
        .block_on(guard.exchange(&http, client.put(url).timeout(Duration::from_secs(2)), &credentials(), "us-east-1"))
        .expect("read original peer's redirect");
    assert_eq!(response.status, reqwest::StatusCode::TEMPORARY_REDIRECT);
    assert!(response.window.is_none());
    server.join().expect("redirecting peer");
    assert_eq!(
        other
            .accept()
            .expect_err("destination must not receive the signed request")
            .kind(),
        std::io::ErrorKind::WouldBlock
    );
}

#[derive(Clone, Copy, Debug)]
enum Incomplete {
    WrongByteCount,
    Timeout,
    Cancelled,
}

#[test]
#[ignore = "requires native NIXL and its UCX backend"]
fn peer_can_finish_after_an_unconfirmed_window_is_quarantined() {
    const SIZE: usize = 4096;
    let runtime = runtime();
    let (client, http) = clients();
    let peer = NixlServer::new(&config("delayed-peer", SIZE, 1)).expect("create peer agent");
    for outcome in [Incomplete::WrongByteCount, Incomplete::Timeout, Incomplete::Cancelled] {
        let windows = RegisteredClientWindow::register(&config(&format!("delayed-{outcome:?}"), SIZE, 1))
            .expect("register a single window");
        let [mut window]: [_; 1] = windows
            .try_into()
            .ok()
            .expect("one window with no sibling retaining the mapping");
        window.write_at(0, &[0x5a; SIZE]).expect("write payload before publication");
        let guard = window.begin(SIZE).expect("begin delayed PUT");
        let (published, observed) = oneshot::channel();
        let (resume, released) = std::sync::mpsc::channel();
        let handle = runtime.handle().clone();
        let reader = Arc::clone(&peer);
        let (url, server) = serve(move |method, token, stream| {
            assert_eq!(method, Method::PUT);
            if matches!(outcome, Incomplete::WrongByteCount) {
                receipt(stream, "200 OK", SIZE - 1);
            }
            published.send(()).expect("request observed");
            released
                .recv_timeout(Duration::from_secs(20))
                .expect("wait for client to discard the exchange");
            let chunk = reader.try_reserve().expect("reserve staging after cancellation");
            let chunk = handle
                .block_on(reader.read_from_client_owned("object", &token, 0, SIZE, chunk))
                .expect("finish the pending peer READ");
            assert_eq!(chunk.staged().expect("quarantined payload"), &[0x5a; SIZE]);
        });
        let mut builder = client.put(&url);
        if matches!(outcome, Incomplete::Timeout) {
            builder = builder.timeout(Duration::from_secs(2));
        }
        runtime.block_on(async {
            let http = http.clone();
            let exchange = tokio::spawn(async move { guard.exchange(&http, builder, &credentials(), "us-east-1").await });
            tokio::time::timeout(Duration::from_secs(10), observed)
                .await
                .expect("peer received request")
                .expect("live peer");
            match outcome {
                Incomplete::WrongByteCount => {
                    let response = exchange.await.expect("exchange task").expect("read HTTP response");
                    assert!(response.window.is_none(), "a short receipt cannot release the window");
                }
                Incomplete::Timeout => assert!(
                    exchange.await.expect("exchange task").is_err(),
                    "a timed-out request cannot release the window"
                ),
                Incomplete::Cancelled => {
                    exchange.abort();
                    assert!(matches!(exchange.await, Err(error) if error.is_cancelled()));
                }
            }
        });
        resume.send(()).expect("allow the peer to finish its original transfer");
        server.join().expect("delayed peer");
    }
}

#[test]
#[ignore = "requires native NIXL and its UCX backend"]
fn get_with_an_http_payload_cannot_return_a_window() {
    let runtime = runtime();
    let (client, http) = clients();
    let responses = [
        "Content-Length: 1\r\n\r\nx",
        "Transfer-Encoding: chunked\r\n\r\n1\r\nx\r\n0\r\n\r\n",
    ];
    let windows = RegisteredClientWindow::register(&config("invalid-get", 4096, responses.len())).expect("register GET windows");
    for (window, entity) in windows.into_iter().zip(responses) {
        let (url, peer) = serve(move |method, _, connection| {
            assert_eq!(method, Method::GET);
            write!(
                connection,
                "HTTP/1.1 200 OK\r\n{RDMA_REPLY_HEADER}: 200\r\n{RDMA_BYTES_TRANSFERRED_HEADER}: 8\r\n{entity}"
            )
            .expect("send contradictory control response");
        });
        let response = runtime
            .block_on(
                window
                    .begin(8)
                    .expect("begin GET")
                    .exchange(&http, client.get(url), &credentials(), "us-east-1"),
            )
            .expect("receive control response");
        assert!(response.window.is_none());
        peer.join().expect("HTTP peer");
    }
}
