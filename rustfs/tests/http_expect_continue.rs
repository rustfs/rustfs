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

//! Transport regressions for SDK clients that withhold DATA until 100 Continue.

use std::{convert::Infallible, future::Future, sync::Arc, time::Duration};

use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::{BodyExt, Empty};
use hyper::body::Incoming;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder,
    service::TowerToHyperService,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::Notify,
    task::JoinHandle,
};
use tower::{Service, service_fn};

async fn bounded<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(3), future)
        .await
        .expect("transport must progress while the client waits for Continue")
}

struct H2Client {
    sender: h2::client::SendRequest<Bytes>,
    tasks: [JoinHandle<()>; 2],
}

impl Drop for H2Client {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

async fn connect<S>(service: S) -> H2Client
where
    S: Service<Request<Incoming>, Response = Response<Empty<Bytes>>, Error = Infallible> + Clone + Send + 'static,
    S::Future: Send,
{
    let (client, server) = tokio::io::duplex(64 * 1024);
    let server_task = tokio::spawn(async move {
        let _ = Builder::new(TokioExecutor::new())
            .serve_connection(TokioIo::new(server), TowerToHyperService::new(service))
            .await;
    });
    let (sender, connection) = h2::client::handshake(client).await.expect("H2 handshake");
    let client_task = tokio::spawn(async move {
        let _ = connection.await;
    });
    H2Client {
        sender,
        tasks: [server_task, client_task],
    }
}

fn request(expect: Option<&str>, end_stream: bool) -> Request<()> {
    let mut request = Request::put("https://localhost/bucket/key?partNumber=1&uploadId=test");
    if let Some(expect) = expect {
        request = request.header("expect", expect);
    }
    request
        .header("content-length", if end_stream { "0" } else { "5" })
        .body(())
        .expect("request")
}

async fn consume(request: Request<Incoming>) -> Result<Response<Empty<Bytes>>, Infallible> {
    assert_eq!(
        request.into_body().collect().await.expect("request body").to_bytes(),
        Bytes::from_static(b"hello")
    );
    Ok(Response::new(Empty::new()))
}

async fn continued(response: &mut h2::client::ResponseFuture) {
    let info = std::future::poll_fn(|cx| response.poll_informational(cx))
        .await
        .expect("100 Continue before DATA")
        .expect("valid informational response");
    assert_eq!(info.status(), StatusCode::CONTINUE);
}

async fn final_response(mut response: h2::client::ResponseFuture, status: StatusCode) {
    assert!(
        std::future::poll_fn(|cx| response.poll_informational(cx)).await.is_none(),
        "unexpected extra informational response"
    );
    assert_eq!(response.await.expect("final response").status(), status);
}

#[tokio::test]
async fn h2_expect_continue_unblocks_sequential_upload_parts_once_each() {
    bounded(async {
        let mut client = connect(service_fn(consume)).await;
        for expect in ["100-continue", "100-CONTINUE"] {
            let (mut response, mut body) = client
                .sender
                .send_request(request(Some(expect), false), false)
                .expect("request headers");
            continued(&mut response).await;
            body.send_data(Bytes::from_static(b"hello"), true)
                .expect("DATA after Continue");
            final_response(response, StatusCode::OK).await;
        }
    })
    .await;
}

#[tokio::test]
async fn h2_without_expect_does_not_send_continue() {
    bounded(async {
        let mut client = connect(service_fn(consume)).await;
        let (response, mut body) = client.sender.send_request(request(None, false), false).expect("request");
        body.send_data(Bytes::from_static(b"hello"), true).expect("DATA");
        final_response(response, StatusCode::OK).await;
    })
    .await;
}

#[tokio::test]
async fn h2_unknown_expectation_does_not_send_continue() {
    bounded(async {
        let mut client = connect(service_fn(consume)).await;
        let (response, mut body) = client
            .sender
            .send_request(request(Some("unsupported-expectation"), false), false)
            .expect("request headers");
        body.send_data(Bytes::from_static(b"hello"), true).expect("eager DATA");
        final_response(response, StatusCode::OK).await;
    })
    .await;
}

#[tokio::test]
async fn h2_expect_continue_waits_until_service_polls_body() {
    bounded(async {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let service_entered = Arc::clone(&entered);
        let service_release = Arc::clone(&release);
        let mut client = connect(service_fn(move |request: Request<Incoming>| {
            let entered = Arc::clone(&service_entered);
            let release = Arc::clone(&service_release);
            async move {
                entered.notify_one();
                release.notified().await;
                consume(request).await
            }
        }))
        .await;
        let (mut response, mut body) = client
            .sender
            .send_request(request(Some("100-continue"), false), false)
            .expect("request headers");
        entered.notified().await;
        std::future::poll_fn(|cx| {
            assert!(
                response.poll_informational(cx).is_pending(),
                "Continue must wait for the application's body poll"
            );
            std::task::Poll::Ready(())
        })
        .await;
        release.notify_one();
        continued(&mut response).await;
        body.send_data(Bytes::from_static(b"hello"), true)
            .expect("DATA after Continue");
        final_response(response, StatusCode::OK).await;
    })
    .await;
}

#[tokio::test]
async fn h2_early_rejection_does_not_invite_upload() {
    bounded(async {
        let mut client = connect(service_fn(|request: Request<Incoming>| async move {
            drop(request);
            Ok::<_, Infallible>(
                Response::builder()
                    .status(StatusCode::FORBIDDEN)
                    .body(Empty::new())
                    .expect("response"),
            )
        }))
        .await;
        let (response, _body) = client
            .sender
            .send_request(request(Some("100-continue"), false), false)
            .expect("request headers");
        final_response(response, StatusCode::FORBIDDEN).await;
    })
    .await;
}

#[tokio::test]
async fn h2_end_stream_does_not_send_continue() {
    bounded(async {
        let mut client = connect(service_fn(|request: Request<Incoming>| async move {
            assert!(request.into_body().collect().await.expect("empty body").to_bytes().is_empty());
            Ok::<_, Infallible>(Response::new(Empty::new()))
        }))
        .await;
        let (response, _body) = client
            .sender
            .send_request(request(Some("100-continue"), true), true)
            .expect("request headers");
        final_response(response, StatusCode::OK).await;
    })
    .await;
}

#[tokio::test]
async fn h2_client_that_sends_body_without_waiting_completes() {
    bounded(async {
        let ready = Arc::new(Notify::new());
        let gate = Arc::clone(&ready);
        let mut client = connect(service_fn(move |request: Request<Incoming>| {
            let gate = Arc::clone(&gate);
            async move {
                gate.notified().await;
                consume(request).await
            }
        }))
        .await;
        let (mut response, mut body) = client
            .sender
            .send_request(request(Some("100-continue"), false), false)
            .expect("request headers");
        body.send_data(Bytes::from_static(b"hello"), true).expect("eager DATA");
        ready.notify_one();
        // Depending on transport scheduling, the first body poll can precede DATA arrival.
        // In either case, an eager client must complete and receive at most one Continue.
        if let Some(info) = std::future::poll_fn(|cx| response.poll_informational(cx)).await {
            assert_eq!(info.expect("informational response").status(), StatusCode::CONTINUE);
        }
        final_response(response, StatusCode::OK).await;
    })
    .await;
}

#[tokio::test]
async fn h2_cancelled_waiting_upload_does_not_block_another_stream() {
    bounded(async {
        let mut client = connect(service_fn(|request: Request<Incoming>| async move {
            if let Ok(body) = request.into_body().collect().await {
                assert_eq!(body.to_bytes(), Bytes::from_static(b"hello"));
            }
            Ok::<_, Infallible>(Response::new(Empty::new()))
        }))
        .await;
        let (mut cancelled, mut abandoned_body) = client
            .sender
            .send_request(request(Some("100-continue"), false), false)
            .expect("first stream");
        let (mut response, mut body) = client
            .sender
            .send_request(request(Some("100-continue"), false), false)
            .expect("second stream");
        continued(&mut cancelled).await;
        abandoned_body.send_reset(h2::Reason::CANCEL);
        drop(cancelled);
        continued(&mut response).await;
        body.send_data(Bytes::from_static(b"hello"), true)
            .expect("second stream DATA");
        final_response(response, StatusCode::OK).await;
    })
    .await;
}

#[tokio::test]
async fn http1_expect_continue_control() {
    bounded(async {
        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let server_task = tokio::spawn(async move {
            let _ = Builder::new(TokioExecutor::new())
                .serve_connection(TokioIo::new(server), TowerToHyperService::new(service_fn(consume)))
                .await;
        });
        client.write_all(b"PUT /bucket/key HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n").await.expect("headers");
        let mut informational = Vec::new();
        while !informational.ends_with(b"\r\n\r\n") {
            informational.push(client.read_u8().await.expect("Continue bytes"));
        }
        assert_eq!(informational, b"HTTP/1.1 100 Continue\r\n\r\n");
        client.write_all(b"hello").await.expect("body after Continue");
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.expect("final response");
        assert!(response.starts_with(b"HTTP/1.1 200 OK\r\n"));
        server_task.await.expect("server task");
    }).await;
}
