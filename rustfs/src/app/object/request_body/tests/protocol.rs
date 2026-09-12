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
use crate::app::storage_api::s3::{
    Body as S3Body, S3, S3Config, S3Error, S3Request, S3Response, S3Result, S3Service, S3ServiceBuilder, SimpleAuth,
    StaticConfigProvider, UploadPartInput, UploadPartOutput,
};
use http_body_util::BodyExt;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone, Default)]
struct Consumer {
    received: Arc<AtomicUsize>,
    committed: Arc<Mutex<Option<Vec<u8>>>>,
}

#[async_trait::async_trait]
impl S3 for Consumer {
    async fn upload_part(&self, req: S3Request<UploadPartInput>) -> S3Result<S3Response<UploadPartOutput>> {
        let expected = req.input.content_length.expect("S3S must normalize the decoded length");
        let control = req
            .extensions
            .get::<BodyReadControl>()
            .expect("HTTP control extension")
            .clone();
        control.activate(Duration::from_secs(300), "test-bucket", "test-key", "request", expected as u64);
        let stream = req.input.body.expect("body");
        let inner = rustfs_rio::wrap_reader(StreamReader::new(stream.map(|item| item.map_err(io::Error::other))));
        let mut reader =
            rustfs_rio::HashReader::from_stream(DemandReader::new(inner, control), expected, expected, None, None, false)
                .expect("logical body reader");
        reader
            .add_checksum_from_s3s(&req.headers, req.trailing_headers, false)
            .expect("request checksum context");
        let mut output = Vec::new();
        let mut buffer = [0; 8192];
        loop {
            let count = reader
                .read(&mut buffer)
                .await
                .map_err(|error| S3Error::from(ApiError::from(error)))?;
            if count == 0 {
                break;
            }
            self.received.fetch_add(count, Ordering::Relaxed);
            output.extend_from_slice(&buffer[..count]);
        }
        assert_eq!(output.len() as i64, expected, "wire length must not reach the business DTO");
        *self.committed.lock() = Some(output);
        Ok(S3Response::new(UploadPartOutput {
            checksum_crc32: reader.content_crc().get("CRC32").cloned(),
            ..UploadPartOutput::default()
        }))
    }
}

fn service(consumer: Consumer) -> S3Service {
    let mut builder = S3ServiceBuilder::new(consumer);
    builder.set_auth(SimpleAuth::from_single("test-access", "test-secret"));
    let mut config = S3Config::default();
    config.presigned_url_max_skew_time_secs = u32::MAX;
    builder.set_config(Arc::new(StaticConfigProvider::new(Arc::new(config))));
    builder.build()
}

struct SignedRequest {
    request: http::Request<S3Body>,
    sender: FrameSender,
    prefix: Bytes,
    suffix: Bytes,
}

/// Constructs a real SigV4 fixture using the production crypto primitives.
/// S3S verifies both the request authorization and every signed chunk.
fn signed_request(payload: &[u8], unsigned_trailer: bool) -> SignedRequest {
    use rustfs_utils::{hex_sha256, hmac_sha256};

    const DATE: &str = "20130524T000000Z";
    const SCOPE: &str = "20130524/us-east-1/s3/aws4_request";
    let mode = if unsigned_trailer {
        "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
    } else {
        "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
    };
    let signed_headers = "host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length";
    let headers = format!(
        "host:s3.amazonaws.com\nx-amz-content-sha256:{mode}\nx-amz-date:{DATE}\nx-amz-decoded-content-length:{}\n",
        payload.len()
    );
    let canonical = format!("PUT\n/test-bucket/test-key\npartNumber=1&uploadId=test-upload\n{headers}\n{signed_headers}\n{mode}");
    let key = hmac_sha256("AWS4test-secret", "20130524");
    let key = hmac_sha256(key, "us-east-1");
    let key = hmac_sha256(key, "s3");
    let key = hmac_sha256(key, "aws4_request");
    let digest = |data: &[u8]| hex_sha256(data, str::to_owned);
    let encode = |data: [u8; 32]| hex_simd::encode_to_string(data, hex_simd::AsciiCase::Lower);
    let seed = encode(hmac_sha256(
        key,
        format!("AWS4-HMAC-SHA256\n{DATE}\n{SCOPE}\n{}", digest(canonical.as_bytes())),
    ));
    let chunk_signature = |previous: &str, data: &[u8]| {
        encode(hmac_sha256(
            key,
            format!("AWS4-HMAC-SHA256-PAYLOAD\n{DATE}\n{SCOPE}\n{previous}\n{}\n{}", digest(b""), digest(data)),
        ))
    };
    let (prefix, suffix) = if unsigned_trailer {
        (
            format!("{:x}\r\n", payload.len()),
            "\r\n0\r\nx-amz-checksum-crc32:y/Q5Jg==\r\n\r\n".to_owned(),
        )
    } else {
        let signature = chunk_signature(&seed, payload);
        (
            format!("{:x};chunk-signature={signature}\r\n", payload.len()),
            format!("\r\n0;chunk-signature={}\r\n\r\n", chunk_signature(&signature, b"")),
        )
    };
    let (sender, receiver) = mpsc::unbounded_channel();
    let control = BodyReadControl::default();
    let body = ObservedBody::new(StreamBody::new(UnboundedReceiverStream::new(receiver)), control.clone());
    let mut builder = http::Request::builder()
        .method("PUT")
        .uri("https://s3.amazonaws.com/test-bucket/test-key?partNumber=1&uploadId=test-upload")
        .header("host", "s3.amazonaws.com")
        .header("content-encoding", "aws-chunked")
        .header("content-length", prefix.len() + payload.len() + suffix.len())
        .header("x-amz-content-sha256", mode)
        .header("x-amz-date", DATE)
        .header("x-amz-decoded-content-length", payload.len())
        .header(
            "authorization",
            format!("AWS4-HMAC-SHA256 Credential=test-access/{SCOPE}, SignedHeaders={signed_headers}, Signature={seed}"),
        );
    if unsigned_trailer {
        builder = builder.header("x-amz-trailer", "x-amz-checksum-crc32");
    }
    let mut request = builder.body(S3Body::http_body_unsync(body)).expect("signed request");
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
    let payload = vec![7; 65536];
    let SignedRequest {
        request,
        sender,
        prefix,
        suffix,
    } = signed_request(&payload, false);
    let consumer = Consumer::default();
    let service = service(consumer.clone());
    let mut call = Box::pin(service.call(request));
    sender.send(Ok(Frame::data(prefix))).expect("body receiver");
    assert!(poll!(call.as_mut()).is_pending());
    let start = Instant::now();
    for chunk in payload.chunks(8192) {
        assert_eq!(
            consumer.received.load(Ordering::Relaxed),
            0,
            "incomplete chunks must not escape signature validation"
        );
        tokio::time::advance(Duration::from_secs(60)).await;
        sender
            .send(Ok(Frame::data(Bytes::copy_from_slice(chunk))))
            .expect("body receiver");
        assert!(poll!(call.as_mut()).is_pending());
    }
    sender.send(Ok(Frame::data(suffix))).expect("body receiver");
    drop(sender);
    let response = call.await.expect("S3 response");
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
        let service = service(consumer.clone());
        sender.send(Ok(Frame::data(prefix))).expect("prefix");
        sender.send(Ok(Frame::data(Bytes::from_static(payload)))).expect("payload");
        sender
            .send(Ok(Frame::data(if send_suffix { suffix } else { Bytes::from_static(b"\r\n") })))
            .expect("suffix");
        let mut call = Box::pin(service.call(request));
        assert!(poll!(call.as_mut()).is_pending());
        assert_eq!(consumer.received.load(Ordering::Relaxed), payload.len());
        tokio::time::advance(Duration::from_secs(300)).await;
        let response = call.await.expect("S3 error response");
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
        let service = service(consumer.clone());
        sender.send(Ok(Frame::data(prefix))).expect("prefix");
        sender
            .send(Ok(Frame::data(Bytes::from_static(if corrupt { b"923456789" } else { payload }))))
            .expect("payload");
        sender.send(Ok(Frame::data(suffix))).expect("suffix");
        drop(sender);
        let response = service.call(request).await.expect("S3 response");
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
    let service = service(consumer.clone());
    sender.send(Ok(Frame::data(prefix))).expect("prefix");
    sender
        .send(Ok(Frame::data(Bytes::from_static(
            b"123456789\r\n0\r\nx-amz-checksum-crc32:AAAAAA==\r\n\r\n",
        ))))
        .expect("invalid trailer");
    drop(sender);
    let response = service.call(request).await.expect("S3 response");
    assert_eq!(response.status(), http::StatusCode::BAD_REQUEST);
    let xml = BodyExt::collect(response.into_body()).await.expect("error XML").to_bytes();
    assert!(String::from_utf8_lossy(&xml).contains("<Code>BadDigest</Code>"));
    assert!(consumer.committed.lock().is_none());
}
