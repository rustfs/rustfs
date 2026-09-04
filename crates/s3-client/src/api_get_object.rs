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
#![allow(clippy::map_entry)]
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_assignments)]
#![allow(unused_must_use)]
#![allow(clippy::all)]

use futures_util::ready;
use http::HeaderMap;
use std::io::{Cursor, Error as IoError, ErrorKind as IoErrorKind, Read};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::BufReader;
use tokio_util::io::StreamReader;

use crate::{
    api_error_response::err_invalid_argument,
    api_get_options::GetObjectOptions,
    transition_api::{
        ObjectInfo, ReadCloser, ReaderImpl, RequestMetadata, TransitionClient, collect_response_body, to_object_info_for_provider,
    },
};
use futures_util::StreamExt;
use http_body_util::BodyExt;
use hyper::body::Body;
use hyper::body::Bytes;
use rustfs_utils::hash::EMPTY_STRING_SHA256_HASH;
use tokio_util::io::ReaderStream;

fn response_limit_from_range(opts: &GetObjectOptions) -> Result<Option<usize>, std::io::Error> {
    let Some(range) = opts
        .headers
        .iter()
        .find_map(|(name, value)| name.eq_ignore_ascii_case("range").then_some(value.as_str()))
    else {
        return Ok(None);
    };
    let Some((unit, bounds)) = range.split_once('=') else {
        return Ok(None);
    };
    if !unit.eq_ignore_ascii_case("bytes") {
        return Ok(None);
    }
    let Some((start, end)) = bounds.split_once('-') else {
        return Ok(None);
    };
    if start.is_empty() || end.is_empty() || end.contains(',') {
        return Ok(None);
    }
    let start = start
        .parse::<u64>()
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "closed response range start is invalid"))?;
    let end = end
        .parse::<u64>()
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "closed response range end is invalid"))?;
    let length = end
        .checked_sub(start)
        .and_then(|length| length.checked_add(1))
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "closed response range length overflows"))?;
    let limit = usize::try_from(length).map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "closed response range length does not fit in memory")
    })?;
    Ok(Some(limit))
}

impl TransitionClient {
    pub fn get_object(&self, bucket_name: &str, object_name: &str, opts: &GetObjectOptions) -> Result<Object, std::io::Error> {
        let _ = opts;
        Err(std::io::Error::new(
            IoErrorKind::Unsupported,
            format!("get_object is not implemented for {bucket_name}/{object_name}"),
        ))
    }

    pub async fn get_object_inner(
        &self,
        bucket_name: &str,
        object_name: &str,
        opts: &GetObjectOptions,
    ) -> Result<(ObjectInfo, HeaderMap, ReadCloser), std::io::Error> {
        let max_response_bytes = response_limit_from_range(opts)?;
        let resp = self
            .execute_method(
                http::Method::GET,
                &mut RequestMetadata {
                    bucket_name: bucket_name.to_string(),
                    object_name: object_name.to_string(),
                    query_values: opts.to_query_values(),
                    custom_header: opts.header(),
                    content_sha256_hex: EMPTY_STRING_SHA256_HASH.to_string(),
                    content_body: ReaderImpl::Body(Bytes::new()),
                    content_length: 0,
                    content_md5_base64: "".to_string(),
                    stream_sha256: false,
                    trailer: HeaderMap::new(),
                    pre_sign_url: Default::default(),
                    extra_pre_sign_header: Default::default(),
                    bucket_location: Default::default(),
                    expires: Default::default(),
                },
            )
            .await?;

        let object_stat =
            to_object_info_for_provider(bucket_name, object_name, resp.headers(), self.provider_version_capabilities())?;

        let h = resp.headers().clone();

        let mut body = resp.into_body();
        let body_vec = if let Some(limit) = max_response_bytes {
            collect_response_body(body, limit).await?
        } else {
            let mut body_vec = Vec::new();
            while let Some(frame) = body.frame().await {
                let frame = frame.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
                if let Some(data) = frame.data_ref() {
                    body_vec.extend_from_slice(data);
                }
            }
            body_vec
        };
        Ok((object_stat, h, BufReader::new(Cursor::new(body_vec))))
    }
}

#[cfg(test)]
mod bounded_response_tests {
    use super::response_limit_from_range;
    use crate::{
        api_get_options::GetObjectOptions,
        credentials::{Credentials, SignatureType, Static, Value},
        transition_api::{BucketLookupType, Options, TransitionClient, collect_response_body},
    };
    use http_body_util::Full;
    use hyper::body::Bytes;
    use std::time::Duration;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    #[test]
    fn closed_range_derives_a_collection_limit_without_new_public_options() {
        let mut opts = GetObjectOptions::default();
        opts.set_range(5, 11).expect("the closed range should be valid");

        assert_eq!(response_limit_from_range(&opts).expect("the range should parse"), Some(7));
    }

    #[tokio::test]
    async fn response_collection_rejects_the_body_that_exceeds_its_range_limit() {
        let mut opts = GetObjectOptions::default();
        opts.set_range(0, 6).expect("the probe range should be valid");
        let max_response_bytes = response_limit_from_range(&opts)
            .expect("the range should parse")
            .expect("the closed range should have a limit");
        let err = collect_response_body(Full::new(Bytes::from_static(b"RustFSxx")), max_response_bytes)
            .await
            .expect_err("the collection layer must reject a response larger than its limit");

        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    async fn bounded_get_fixture(body: &'static [u8]) -> Option<(TransitionClient, tokio::task::JoinHandle<String>)> {
        let listener = match TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return None,
            Err(err) => panic!("test listener should bind: {err}"),
        };
        let endpoint = listener
            .local_addr()
            .expect("listener local address should be available")
            .to_string();
        let request = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("fixture should accept one GET");
            let mut request = Vec::new();
            let mut buffer = [0; 1024];
            loop {
                let read = stream.read(&mut buffer).await.expect("fixture should read request headers");
                assert_ne!(read, 0, "connection closed before request headers were received");
                request.extend_from_slice(&buffer[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            let request = String::from_utf8_lossy(&request).into_owned();
            let response = format!(
                "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("fixture should write response headers");
            stream.write_all(body).await.expect("fixture should write response body");
            request
        });
        let client = TransitionClient::new(
            &endpoint,
            Options {
                creds: Credentials::new(Static(Value {
                    access_key_id: "access-key".to_string(),
                    secret_access_key: "secret-key".to_string(),
                    signer_type: SignatureType::SignatureV4,
                    ..Default::default()
                })),
                region: "us-east-1".to_string(),
                bucket_lookup: BucketLookupType::BucketLookupPath,
                max_retries: 1,
                ..Default::default()
            },
            "",
        )
        .await
        .expect("fixture client should build");
        Some((client, request))
    }

    #[tokio::test]
    async fn real_transport_accepts_the_exact_closed_range_length() {
        let Some((client, request)) = bounded_get_fixture(b"RustFS!").await else {
            return;
        };
        let mut opts = GetObjectOptions::default();
        opts.set_range(0, 6).expect("the probe range should be valid");

        let (_, _, mut reader) = client
            .get_object_inner("bucket", "probe", &opts)
            .await
            .expect("a seven-byte response should fit the requested range");
        let mut body = Vec::new();
        reader
            .read_to_end(&mut body)
            .await
            .expect("bounded response should be readable");

        assert_eq!(body, b"RustFS!");
        assert!(
            request
                .await
                .expect("fixture should join")
                .to_ascii_lowercase()
                .contains("\r\nrange: bytes=0-6\r\n")
        );
    }

    #[tokio::test]
    async fn real_transport_rejects_a_body_larger_than_the_closed_range() {
        let Some((client, request)) = bounded_get_fixture(b"RustFS!!").await else {
            return;
        };
        let mut opts = GetObjectOptions::default();
        opts.set_range(0, 6).expect("the probe range should be valid");

        let err = client
            .get_object_inner("bucket", "probe", &opts)
            .await
            .expect_err("an eight-byte response must exceed the seven-byte range limit");

        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(
            request
                .await
                .expect("fixture should join")
                .to_ascii_lowercase()
                .contains("\r\nrange: bytes=0-6\r\n")
        );
    }

    #[tokio::test]
    async fn overflowing_closed_range_is_rejected_before_network_io() {
        let listener = match TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return,
            Err(err) => panic!("test listener should bind: {err}"),
        };
        let endpoint = listener
            .local_addr()
            .expect("listener local address should be available")
            .to_string();
        let client = TransitionClient::new(
            &endpoint,
            Options {
                creds: Credentials::new(Static(Value {
                    access_key_id: "access-key".to_string(),
                    secret_access_key: "secret-key".to_string(),
                    signer_type: SignatureType::SignatureV4,
                    ..Default::default()
                })),
                region: "us-east-1".to_string(),
                bucket_lookup: BucketLookupType::BucketLookupPath,
                max_retries: 1,
                ..Default::default()
            },
            "",
        )
        .await
        .expect("fixture client should build");
        let mut opts = GetObjectOptions::default();
        opts.headers
            .insert("range".to_string(), "bytes=0-18446744073709551615".to_string());

        let err = client
            .get_object_inner("bucket", "probe", &opts)
            .await
            .expect_err("an overflowing closed range must be rejected locally");

        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
    }
}

#[derive(Default)]
pub struct GetRequest {
    pub buffer: Vec<u8>,
    pub offset: i64,
    pub did_offset_change: bool,
    pub been_read: bool,
    pub is_read_at: bool,
    pub is_read_op: bool,
    pub is_first_req: bool,
    pub setting_object_info: bool,
}

pub struct GetResponse {
    pub size: i64,
    //pub error:       error,
    #[allow(dead_code, reason = "written but never read back (backlog#1823)")]
    pub did_read: bool,
    #[allow(dead_code, reason = "written but never read back (backlog#1823)")]
    pub object_info: ObjectInfo,
}

#[derive(Default)]
pub struct Object {
    //pub reqch:      chan<- getRequest,
    //pub resch:      <-chan getResponse,
    //pub cancel:     context.CancelFunc,
    pub curr_offset: i64,
    pub object_info: ObjectInfo,
    pub seek_data: bool,
    pub is_closed: bool,
    pub is_started: bool,
    //pub prev_err: error,
    pub been_read: bool,
    pub object_info_set: bool,
}

impl Object {
    pub fn new() -> Object {
        Self { ..Default::default() }
    }

    #[allow(
        dead_code,
        reason = "MinIO-parity reader surface with no caller in this port (backlog#1823)"
    )]
    fn do_get_request(&self, request: &GetRequest) -> Result<GetResponse, std::io::Error> {
        let _ = request.did_offset_change;
        let _ = request.offset;
        let _ = request.is_first_req;
        let _ = request.is_read_at;
        let _ = request.setting_object_info;
        let _ = request.is_read_op;
        let _ = request.been_read;
        let _ = request.buffer.len();
        Err(std::io::Error::new(
            IoErrorKind::Unsupported,
            "read-path for Object in api_get_object is not implemented",
        ))
    }

    #[allow(
        dead_code,
        reason = "MinIO-parity Object reader method with no caller in this port (backlog#1823)"
    )]
    fn set_offset(&mut self, bytes_read: i64) -> Result<(), std::io::Error> {
        self.curr_offset += bytes_read;

        Ok(())
    }

    #[allow(
        dead_code,
        reason = "MinIO-parity Object reader method with no caller in this port (backlog#1823)"
    )]
    fn read(&mut self, b: &[u8]) -> Result<i64, std::io::Error> {
        let mut read_req = GetRequest {
            is_read_op: true,
            been_read: self.been_read,
            buffer: b.to_vec(),
            ..Default::default()
        };

        if !self.is_started {
            read_req.is_first_req = true;
        }

        read_req.did_offset_change = self.seek_data;
        read_req.offset = self.curr_offset;

        let response = self.do_get_request(&read_req)?;

        let bytes_read = response.size;

        let oerr = self.set_offset(bytes_read);

        Ok(response.size)
    }

    #[allow(
        dead_code,
        reason = "MinIO-parity Object reader method with no caller in this port (backlog#1823)"
    )]
    fn stat(&self) -> Result<ObjectInfo, std::io::Error> {
        if !self.is_started || !self.object_info_set {
            let _ = self.do_get_request(&GetRequest {
                is_first_req: !self.is_started,
                setting_object_info: !self.object_info_set,
                ..Default::default()
            })?;
        }

        Ok(self.object_info.clone())
    }

    #[allow(
        dead_code,
        reason = "MinIO-parity Object reader method with no caller in this port (backlog#1823)"
    )]
    fn read_at(&mut self, b: &[u8], offset: i64) -> Result<i64, std::io::Error> {
        self.curr_offset = offset;

        let mut read_at_req = GetRequest {
            is_read_op: true,
            is_read_at: true,
            did_offset_change: true,
            been_read: self.been_read,
            offset,
            buffer: b.to_vec(),
            ..Default::default()
        };

        if !self.is_started {
            read_at_req.is_first_req = true;
        }

        let response = self.do_get_request(&read_at_req)?;
        let bytes_read = response.size;
        if !self.object_info_set {
            self.curr_offset += bytes_read;
        } else {
            let oerr = self.set_offset(bytes_read);
        }
        Ok(response.size)
    }

    #[allow(
        dead_code,
        reason = "MinIO-parity Object reader method with no caller in this port (backlog#1823)"
    )]
    fn seek(&mut self, offset: i64, whence: i64) -> Result<i64, std::io::Error> {
        if !self.is_started || !self.object_info_set {
            let seek_req = GetRequest {
                is_read_op: false,
                offset,
                is_first_req: true,
                ..Default::default()
            };
            let _ = self.do_get_request(&seek_req);
        }

        let mut new_offset = self.curr_offset;

        match whence {
            0 => {
                new_offset = offset;
            }
            1 => {
                new_offset += offset;
            }
            2 => {
                new_offset = self.object_info.size as i64 + offset as i64;
            }
            _ => {
                return Err(std::io::Error::other(err_invalid_argument(&format!("Invalid whence {}", whence))));
            }
        }

        self.seek_data = (new_offset != self.curr_offset) || self.seek_data;
        self.curr_offset = new_offset;

        Ok(self.curr_offset)
    }

    #[allow(
        dead_code,
        reason = "MinIO-parity Object reader method with no caller in this port (backlog#1823)"
    )]
    fn close(&mut self) -> Result<(), std::io::Error> {
        self.is_closed = true;
        Ok(())
    }
}
