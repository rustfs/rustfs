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

use super::*;
use crate::server::gateway_stack::{GatewayAssembly, GatewayStack};
use http_body_util::BodyExt;
use rustfs_gateway::{Handler, HandlerResult, Req, Resp, dto};
use rustfs_rio::{SharedTrailerSource, TrailerValue};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone, Default)]
struct Consumer {
    received: Arc<AtomicUsize>,
    committed: Arc<Mutex<Option<Vec<u8>>>>,
    trailer: Arc<Mutex<Option<SharedTrailerSource>>>,
}

impl Handler<dto::UploadPart> for Consumer {
    async fn call(&self, req: Req<dto::UploadPart>) -> HandlerResult<dto::UploadPart> {
        use crate::app::object::BodyChunks;
        let control = req
            .context()
            .transport_extensions()
            .get::<BodyReadControl>()
            .expect("HTTP control extension")
            .clone();
        let mut headers = http::HeaderMap::new();
        for (name, value) in req.context().headers().iter_raw() {
            headers.append(name.clone(), value.clone());
        }
        let input = req.into_input();
        let (chunks, trailers) = BodyChunks::new(input.body.expect("body").into_body());
        let inner = rustfs_rio::wrap_reader(StreamReader::new(chunks.map(|item| item.map_err(io::Error::other))));
        let declared_trailer = headers.contains_key("x-amz-trailer").then_some(trailers);
        let expected = input.content_length;
        control.activate_upload(Duration::from_secs(300), "test-bucket", "test-key", "request", expected as u64);
        let mut reader =
            rustfs_rio::HashReader::from_stream(DemandReader::new(inner, control), expected, expected, None, None, false)
                .expect("logical body reader");
        *self.trailer.lock() = declared_trailer.clone();
        reader
            .add_checksum(&headers, declared_trailer, false)
            .expect("request checksum context");
        let mut output = Vec::new();
        let mut buffer = [0; 8192];
        loop {
            let count = reader
                .read(&mut buffer)
                .await
                .map_err(|error| crate::server::handler_error(ApiError::from(error).into()))?;
            if count == 0 {
                break;
            }
            self.received.fetch_add(count, Ordering::Relaxed);
            output.extend_from_slice(&buffer[..count]);
        }
        assert_eq!(output.len() as i64, expected, "wire length must not reach the business DTO");
        *self.committed.lock() = Some(output);
        let mut extra_headers = http::HeaderMap::new();
        if let Some(checksum) = reader.content_crc().get("CRC32") {
            extra_headers.insert("x-amz-checksum-crc32", checksum.parse().expect("base64 checksum"));
        }
        Ok(Resp::new(dto::UploadPartOutput {
            e_tag: crate::app::dto_values::stored_etag("test-part"),
            ..Default::default()
        })
        .with_extra_headers(extra_headers))
    }
}

fn gateway_service(consumer: Consumer) -> GatewayStack {
    use rustfs_gateway::{Credentials, RegionSet, SigV4Authenticator, StaticCredentials};
    let credentials = StaticCredentials::new().with(Credentials::new("test-access", b"test-secret").unwrap());
    GatewayStack::assemble_serving(
        GatewayAssembly {
            server_ctx: crate::runtime_sources::ServerContextSlot::new(),
            root_access_key: "test-access".into(),
            root_secret_key: "test-secret".into(),
            region: Some("us-east-1".into()),
            server_domains: Vec::new(),
            body_read_timeout: Duration::from_secs(300),
            keystone_auth: None,
        },
        |builder| {
            builder
                .authenticator(
                    SigV4Authenticator::new(Arc::new(credentials), RegionSet::new(["us-east-1"]).unwrap()).rustfs_profile(),
                )
                .register::<dto::UploadPart, _>(Arc::new(consumer))
        },
    )
    .expect("the gateway stack assembles")
}

#[tokio::test]
async fn signed_chunk_size_limit_accepts_exact_boundary_and_rejects_one_more() {
    for exceeds_limit in [false, true] {
        // The RustFS gateway profile accepts one signed chunk of 16 MiB.
        let payload = Bytes::from(vec![7; 16 * 1024 * 1024 + usize::from(exceeds_limit)]);
        let SignedRequest {
            request,
            sender,
            prefix,
            suffix,
        } = signed_request(&payload, false);
        let consumer = Consumer::default();
        let service = gateway_service(consumer.clone());
        sender.send(Ok(Frame::data(prefix))).expect("chunk metadata");
        for offset in (0..payload.len()).step_by(64 * 1024) {
            sender
                .send(Ok(Frame::data(payload.slice(offset..(offset + 64 * 1024).min(payload.len())))))
                .expect("chunk data");
        }
        sender.send(Ok(Frame::data(suffix))).expect("terminating chunk");
        drop(sender);
        let response = service.call(request).await;
        if exceeds_limit {
            assert_eq!(response.status(), http::StatusCode::BAD_REQUEST);
            let body = BodyExt::collect(response.into_body()).await.expect("error XML").to_bytes();
            let xml = std::str::from_utf8(&body).expect("UTF-8 XML");
            assert!(xml.contains("<Code>EntityTooLarge</Code>"));
            assert!(!xml.contains("ChunkDataTooLarge"));
            assert_eq!(consumer.received.load(Ordering::Relaxed), 0);
            assert!(consumer.committed.lock().is_none());
        } else {
            assert_eq!(response.status(), http::StatusCode::OK);
            assert_eq!(consumer.committed.lock().as_deref(), Some(payload.as_ref()));
        }
    }
}

type WireBody = ObservedBody<StreamBody<UnboundedReceiverStream<io::Result<Frame<Bytes>>>>>;

struct SignedRequest {
    request: http::Request<WireBody>,
    sender: FrameSender,
    prefix: Bytes,
    suffix: Bytes,
}

/// Independently sign the wire fixture; the gateway verifies the header and each signed chunk.
fn signed_request(payload: &[u8], unsigned_trailer: bool) -> SignedRequest {
    use rustfs_signer::request_signature_v4::{get_scope, get_signature, get_signing_key};
    use rustfs_utils::hash::EMPTY_STRING_SHA256_HASH;
    const REGION: &str = "us-east-1";
    const SERVICE: &str = "s3";
    const SECRET_KEY: &str = "test-secret";
    let mode = if unsigned_trailer {
        "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
    } else {
        "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
    };
    let mut builder = http::Request::put("https://s3.amazonaws.com/test-bucket/test-key?partNumber=1&uploadId=test-upload")
        .header("host", "s3.amazonaws.com")
        .header("content-encoding", "aws-chunked")
        .header("x-amz-content-sha256", mode)
        .header("x-amz-decoded-content-length", payload.len());
    if unsigned_trailer {
        builder = builder.header("x-amz-trailer", "x-amz-checksum-crc32");
    }
    let request =
        rustfs_signer::try_sign_v4(builder.body(()).unwrap(), payload.len() as i64, "test-access", SECRET_KEY, "", REGION)
            .expect("the client signs the upload head");
    let (prefix, suffix) = if unsigned_trailer {
        (
            format!("{:x}\r\n", payload.len()),
            "\r\n0\r\nx-amz-checksum-crc32:y/Q5Jg==\r\n\r\n".to_owned(),
        )
    } else {
        let headers = request.headers();
        let seed = headers["authorization"]
            .to_str()
            .unwrap()
            .rsplit_once("Signature=")
            .expect("seed signature")
            .1;
        let stamp = headers["x-amz-date"].to_str().unwrap();
        let day = time::Date::parse(&stamp[..8], time::macros::format_description!("[year][month][day]"))
            .unwrap()
            .midnight()
            .assume_utc();
        let scope = get_scope(REGION, day, SERVICE);
        let key = get_signing_key(SECRET_KEY, REGION, day, SERVICE);
        let chunk_signature = |previous: &str, chunk: &[u8]| {
            let digest = rustfs_utils::crypto::hex_sha256(chunk, str::to_owned);
            get_signature(
                key,
                &format!("AWS4-HMAC-SHA256-PAYLOAD\n{stamp}\n{scope}\n{previous}\n{EMPTY_STRING_SHA256_HASH}\n{digest}"),
            )
        };
        let signature = chunk_signature(seed, payload);
        (
            format!("{:x};chunk-signature={signature}\r\n", payload.len()),
            format!("\r\n0;chunk-signature={}\r\n\r\n", chunk_signature(&signature, b"")),
        )
    };
    let (sender, receiver) = mpsc::unbounded_channel();
    let control = BodyReadControl::default();
    let body = ObservedBody::new(StreamBody::new(UnboundedReceiverStream::new(receiver)), control.clone());
    let mut request = request.map(|()| body);
    request
        .headers_mut()
        .insert(http::header::CONTENT_LENGTH, (prefix.len() + payload.len() + suffix.len()).into());
    request.extensions_mut().insert(control);
    SignedRequest {
        request,
        sender,
        prefix: Bytes::from(prefix),
        suffix: Bytes::from(suffix),
    }
}

#[tokio::test(start_paused = true)]
async fn signed_chunk_with_raw_progress_survives_eight_minutes_without_decoded_output() {
    let payload = vec![7; 1024 * 1024];
    let SignedRequest {
        request,
        sender,
        prefix,
        suffix,
    } = signed_request(&payload, false);
    let consumer = Consumer::default();
    let service = gateway_service(consumer.clone());
    let mut call = Box::pin(service.call(request));
    sender.send(Ok(Frame::data(prefix))).expect("body receiver");
    assert!(poll!(call.as_mut()).is_pending());
    let start = Instant::now();
    for chunk in payload.chunks(65536) {
        assert_eq!(
            consumer.received.load(Ordering::Relaxed),
            0,
            "incomplete chunks must not escape signature validation"
        );
        tokio::time::advance(Duration::from_secs(30)).await;
        sender
            .send(Ok(Frame::data(Bytes::copy_from_slice(chunk))))
            .expect("body receiver");
        assert!(poll!(call.as_mut()).is_pending());
    }
    sender.send(Ok(Frame::data(suffix))).expect("body receiver");
    drop(sender);
    let response = call.await;
    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(start.elapsed(), Duration::from_secs(480));
    assert_eq!(*consumer.committed.lock(), Some(payload));
}

#[tokio::test(start_paused = true)]
async fn decoded_length_does_not_end_waiting_for_terminator_trailer_or_raw_eof() {
    for (unsigned_trailer, send_suffix) in [(false, false), (false, true), (true, false), (true, true)] {
        let payload = b"123456789";
        let SignedRequest {
            request,
            sender,
            prefix,
            suffix,
        } = signed_request(payload, unsigned_trailer);
        let consumer = Consumer::default();
        let service = gateway_service(consumer.clone());
        sender.send(Ok(Frame::data(prefix))).expect("prefix");
        sender.send(Ok(Frame::data(Bytes::from_static(payload)))).expect("payload");
        sender
            .send(Ok(Frame::data(if send_suffix { suffix } else { Bytes::from_static(b"\r\n") })))
            .expect("suffix");
        let mut call = Box::pin(service.call(request));
        assert!(poll!(call.as_mut()).is_pending());
        assert_eq!(consumer.received.load(Ordering::Relaxed), payload.len());
        tokio::time::advance(Duration::from_secs(300)).await;
        let response = call.await;
        assert_eq!(response.status(), http::StatusCode::BAD_REQUEST);
        let xml = BodyExt::collect(response.into_body()).await.expect("error XML").to_bytes();
        assert!(String::from_utf8_lossy(&xml).contains("<Code>RequestTimeout</Code>"));
        assert!(consumer.committed.lock().is_none());
    }
}

#[tokio::test]
async fn unsigned_trailer_normalizes_length_and_signed_corruption_cannot_commit() {
    for corrupt in [false, true] {
        let payload = b"123456789";
        let SignedRequest {
            request,
            sender,
            prefix,
            suffix,
        } = signed_request(payload, !corrupt);
        let consumer = Consumer::default();
        let service = gateway_service(consumer.clone());
        sender.send(Ok(Frame::data(prefix))).expect("prefix");
        sender
            .send(Ok(Frame::data(Bytes::from_static(if corrupt { b"923456789" } else { payload }))))
            .expect("payload");
        sender.send(Ok(Frame::data(suffix))).expect("suffix");
        drop(sender);
        let response = service.call(request).await;
        if corrupt {
            assert_ne!(response.status(), http::StatusCode::OK);
            assert_eq!(consumer.received.load(Ordering::Relaxed), 0);
            assert!(consumer.committed.lock().is_none());
        } else {
            assert_eq!(response.status(), http::StatusCode::OK);
            assert_eq!(
                response
                    .headers()
                    .get("x-amz-checksum-crc32")
                    .expect("validated response checksum"),
                "y/Q5Jg=="
            );
            assert_eq!(*consumer.committed.lock(), Some(payload.to_vec()));
        }
    }
}

#[tokio::test]
async fn unsigned_trailer_with_wrong_checksum_cannot_commit() {
    let SignedRequest {
        request, sender, prefix, ..
    } = signed_request(b"123456789", true);
    let consumer = Consumer::default();
    let service = gateway_service(consumer.clone());
    sender.send(Ok(Frame::data(prefix))).expect("prefix");
    sender
        .send(Ok(Frame::data(Bytes::from_static(
            b"123456789\r\n0\r\nx-amz-checksum-crc32:AAAAAA==\r\n\r\n",
        ))))
        .expect("invalid trailer");
    drop(sender);
    let response = service.call(request).await;
    assert_eq!(response.status(), http::StatusCode::BAD_REQUEST);
    let xml = BodyExt::collect(response.into_body()).await.expect("error XML").to_bytes();
    assert!(String::from_utf8_lossy(&xml).contains("<Code>BadDigest</Code>"));
    assert!(consumer.committed.lock().is_none());
}

#[tokio::test]
async fn trailer_adapter_is_pending_until_the_decoded_body_ends() {
    let payload = b"123456789";
    let SignedRequest {
        request,
        sender,
        prefix,
        suffix,
    } = signed_request(payload, true);
    let consumer = Consumer::default();
    let service = gateway_service(consumer.clone());
    sender.send(Ok(Frame::data(prefix))).expect("prefix");
    sender.send(Ok(Frame::data(Bytes::from_static(payload)))).expect("payload");
    sender.send(Ok(Frame::data(suffix.slice(..2)))).expect("chunk terminator");
    let mut call = Box::pin(service.call(request));
    assert!(poll!(call.as_mut()).is_pending());
    assert_eq!(consumer.received.load(Ordering::Relaxed), payload.len());
    let trailer = consumer.trailer.lock().clone().expect("declared trailer is adapted");
    assert_eq!(trailer.lookup("x-amz-checksum-crc32"), TrailerValue::Pending);
    assert!(consumer.committed.lock().is_none());

    sender.send(Ok(Frame::data(suffix.slice(2..)))).expect("trailer section");
    drop(sender);
    let response = call.await;
    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(trailer.lookup("x-amz-checksum-crc32"), TrailerValue::Present("y/Q5Jg==".to_owned()));
    assert_eq!(trailer.lookup("x-amz-checksum-sha256"), TrailerValue::Missing);
    assert_eq!(*consumer.committed.lock(), Some(payload.to_vec()));
}

#[tokio::test(start_paused = true)]
async fn signed_chunk_trickle_cannot_renew_upload_progress_window() {
    let SignedRequest {
        request, sender, prefix, ..
    } = signed_request(&[7; 65536], false);
    let consumer = Consumer::default();
    let service = gateway_service(consumer.clone());
    sender.send(Ok(Frame::data(prefix))).unwrap();
    let mut call = Box::pin(service.call(request));
    assert!(poll!(call.as_mut()).is_pending());
    for _ in 0..5 {
        tokio::time::advance(Duration::from_secs(10)).await;
        sender.send(Ok(Frame::data(Bytes::from_static(&[7])))).unwrap();
        assert!(poll!(call.as_mut()).is_pending());
        assert_eq!(consumer.received.load(Ordering::Relaxed), 0);
    }
    tokio::time::advance(Duration::from_secs(10)).await;
    let response = call.await;
    assert_eq!(response.status(), http::StatusCode::BAD_REQUEST);
    let body = BodyExt::collect(response.into_body()).await.unwrap().to_bytes();
    assert!(String::from_utf8_lossy(&body).contains("<Code>RequestTimeout</Code>"));
    assert!(consumer.committed.lock().is_none());
}
