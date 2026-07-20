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

use std::{
    collections::HashMap,
    io,
    sync::atomic::{AtomicBool, Ordering},
};

use s3s::header::{X_AMZ_DELETE_MARKER, X_AMZ_VERSION_ID};
use uuid::Uuid;

use crate::{
    client::transition_api::{BucketLookupType, ReadCloser, ReaderImpl},
    services::tier::{
        tier_config::{TierS3, TierWasabi},
        warm_backend::{WarmBackend, WarmBackendGetOpts},
        warm_backend_s3::WarmBackendS3,
    },
};

const WASABI_VERSIONING_DRIFT_ERROR: &str = "Wasabi tier bucket versioning changed after configuration";

pub struct WarmBackendWasabi {
    s3: WarmBackendS3,
    versioning_checked: AtomicBool,
    versioning_drifted: AtomicBool,
}

impl WarmBackendWasabi {
    pub async fn new(conf: &TierWasabi, _tier: &str) -> io::Result<Self> {
        let endpoint = conf.canonical_endpoint()?;
        let s3 = TierS3 {
            name: conf.name.clone(),
            endpoint,
            access_key: conf.access_key.clone(),
            secret_key: conf.secret_key.clone(),
            bucket: conf.bucket.clone(),
            prefix: conf.prefix.clone(),
            region: conf.region.clone(),
            ..Default::default()
        };
        let backend = WarmBackendS3::new_with_bucket_lookup(&s3, BucketLookupType::BucketLookupPath, "wasabi").await?;
        Ok(Self {
            s3: backend,
            versioning_checked: AtomicBool::new(false),
            versioning_drifted: AtomicBool::new(false),
        })
    }

    fn drift_error() -> io::Error {
        io::Error::other(WASABI_VERSIONING_DRIFT_ERROR)
    }

    fn normalized_remote_version(remote_version: &str) -> &str {
        if remote_version.is_empty() || Uuid::parse_str(remote_version).is_ok_and(|version_id| version_id.is_nil()) {
            ""
        } else {
            remote_version
        }
    }

    async fn check_remote_bucket_unversioned(&self) -> io::Result<()> {
        if self.versioning_drifted.load(Ordering::Acquire) {
            return Err(Self::drift_error());
        }
        let config = self.s3.client.get_bucket_versioning(&self.s3.bucket).await?;
        if config.status.is_some() || config.mfa_delete.is_some() {
            self.versioning_drifted.store(true, Ordering::Release);
            return Err(io::Error::other("Wasabi tier bucket must be unversioned"));
        }
        self.versioning_checked.store(true, Ordering::Release);
        if self.versioning_drifted.load(Ordering::Acquire) {
            return Err(Self::drift_error());
        }
        Ok(())
    }

    async fn ensure_unversioned(&self) -> io::Result<()> {
        if self.versioning_drifted.load(Ordering::Acquire) {
            return Err(Self::drift_error());
        }
        if self.versioning_checked.load(Ordering::Acquire) {
            return Ok(());
        }
        self.check_remote_bucket_unversioned().await
    }
}

#[async_trait::async_trait]
impl WarmBackend for WarmBackendWasabi {
    async fn validate(&self) -> io::Result<()> {
        self.check_remote_bucket_unversioned().await
    }

    fn validate_remote_version_id(&self, remote_version_id: &str) -> io::Result<()> {
        if remote_version_id.is_empty() {
            return Ok(());
        }
        self.versioning_drifted.store(true, Ordering::Release);
        Err(io::Error::other(
            "Wasabi returned an object version ID for a tier that requires an unversioned bucket",
        ))
    }

    async fn put(&self, object: &str, r: ReaderImpl, length: i64) -> io::Result<String> {
        self.ensure_unversioned().await?;
        self.s3.put(object, r, length).await
    }

    async fn put_with_meta(&self, object: &str, r: ReaderImpl, length: i64, meta: HashMap<String, String>) -> io::Result<String> {
        self.ensure_unversioned().await?;
        self.s3.put_with_meta(object, r, length, meta).await
    }

    async fn get(&self, object: &str, rv: &str, opts: WarmBackendGetOpts) -> io::Result<ReadCloser> {
        let rv = Self::normalized_remote_version(rv);
        if rv.is_empty() {
            self.ensure_unversioned().await?;
        }
        let (headers, reader) = self.s3.get_with_headers(object, rv, opts).await?;
        if rv.is_empty() && (headers.contains_key(X_AMZ_VERSION_ID) || headers.contains_key(X_AMZ_DELETE_MARKER)) {
            self.versioning_drifted.store(true, Ordering::Release);
            return Err(io::Error::other(
                "Wasabi bucket versioning changed while reading an unversioned tier object",
            ));
        }
        Ok(reader)
    }

    async fn remove(&self, object: &str, rv: &str) -> io::Result<()> {
        let rv = Self::normalized_remote_version(rv);
        if !rv.is_empty() {
            return self.s3.remove(object, rv).await;
        }
        self.ensure_unversioned().await?;

        let result = self.s3.remove_with_result(object, rv).await?;
        if result.delete_marker || !result.delete_marker_version_id.is_empty() {
            self.versioning_drifted.store(true, Ordering::Release);
            if !result.delete_marker_version_id.is_empty()
                && let Err(rollback_error) = self.s3.remove(object, &result.delete_marker_version_id).await
            {
                return Err(io::Error::other(format!(
                    "{WASABI_VERSIONING_DRIFT_ERROR}; failed to remove the unexpected delete marker: {rollback_error}"
                )));
            }
            return Err(io::Error::other(WASABI_VERSIONING_DRIFT_ERROR));
        }
        Ok(())
    }

    async fn remove_exact(&self, object: &str, rv: &str) -> io::Result<()> {
        if rv.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "an exact Wasabi tier delete requires a remote version ID",
            ));
        }
        self.s3.remove(object, rv).await
    }

    async fn in_use(&self) -> io::Result<bool> {
        self.check_remote_bucket_unversioned().await?;
        let in_use = self.s3.in_use().await?;
        if self.versioning_drifted.load(Ordering::Acquire) {
            return Err(Self::drift_error());
        }
        Ok(in_use)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{
        credentials::{Credentials, SignatureType, Static, Value},
        transition_api::{Options, TransitionCore},
    };
    use std::sync::{Arc, atomic::AtomicUsize};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    #[derive(Clone, Copy)]
    struct TestResponse {
        status: &'static str,
        headers: &'static str,
        body: &'static str,
    }

    #[derive(Debug)]
    struct CapturedRequest {
        head: String,
        body: Vec<u8>,
    }

    fn test_config() -> TierWasabi {
        TierWasabi {
            name: "WASABI".to_string(),
            endpoint: String::new(),
            access_key: "access-key".to_string(),
            secret_key: "secret-key".to_string(),
            bucket: "tier-bucket".to_string(),
            prefix: "/archive/".to_string(),
            region: "ap-northeast-1".to_string(),
        }
    }

    async fn backend_for_endpoint(endpoint: &str, max_retries: i64) -> WarmBackendWasabi {
        let client = Arc::new(
            crate::client::transition_api::TransitionClient::new(
                endpoint,
                Options {
                    creds: Credentials::new(Static(Value {
                        access_key_id: "access-key".to_string(),
                        secret_access_key: "secret-key".to_string(),
                        signer_type: SignatureType::SignatureV4,
                        ..Default::default()
                    })),
                    region: "us-east-1".to_string(),
                    bucket_lookup: BucketLookupType::BucketLookupPath,
                    max_retries,
                    ..Default::default()
                },
                "wasabi",
            )
            .await
            .expect("test transition client should initialize"),
        );
        WarmBackendWasabi {
            s3: WarmBackendS3 {
                core: TransitionCore(client.clone()),
                client,
                bucket: "tier-bucket".to_string(),
                prefix: "archive".to_string(),
                storage_class: String::new(),
            },
            versioning_checked: AtomicBool::new(true),
            versioning_drifted: AtomicBool::new(false),
        }
    }

    async fn backend_with_responses(
        responses: Vec<TestResponse>,
    ) -> Option<(WarmBackendWasabi, tokio::task::JoinHandle<Vec<CapturedRequest>>)> {
        let listener = match TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(err) if err.kind() == io::ErrorKind::PermissionDenied => return None,
            Err(err) => panic!("test listener should bind: {err}"),
        };
        let endpoint = listener
            .local_addr()
            .expect("test listener address should resolve")
            .to_string();
        let request_task = tokio::spawn(async move {
            let mut requests = Vec::with_capacity(responses.len());
            for response in responses {
                let (mut stream, _) = listener.accept().await.expect("S3 request should connect");
                let mut request = Vec::new();
                let mut buffer = [0_u8; 1024];
                loop {
                    let read = stream.read(&mut buffer).await.expect("S3 request should be readable");
                    assert_ne!(read, 0, "connection closed before request headers were received");
                    request.extend_from_slice(&buffer[..read]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                let header_end = request
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .expect("request should contain a complete header")
                    + 4;
                let head = String::from_utf8(request[..header_end].to_vec()).expect("S3 request header should be UTF-8");
                let content_length = head
                    .lines()
                    .filter_map(|line| line.split_once(':'))
                    .find_map(|(name, value)| {
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().expect("Content-Length should be numeric"))
                    })
                    .unwrap_or_default();
                let request_end = header_end
                    .checked_add(content_length)
                    .expect("request length should not overflow");
                while request.len() < request_end {
                    let remaining = request_end - request.len();
                    let read_len = remaining.min(buffer.len());
                    let read = stream
                        .read(&mut buffer[..read_len])
                        .await
                        .expect("S3 request body should be readable");
                    assert_ne!(read, 0, "connection closed before the complete request body was received");
                    request.extend_from_slice(&buffer[..read]);
                }
                assert_eq!(request.len(), request_end, "fixture must capture exactly one request body");
                let wire_response = format!(
                    "HTTP/1.1 {}\r\nContent-Length: {}\r\n{}Connection: close\r\n\r\n{}",
                    response.status,
                    response.body.len(),
                    response.headers,
                    response.body
                );
                stream
                    .write_all(wire_response.as_bytes())
                    .await
                    .expect("S3 response should be writable");
                requests.push(CapturedRequest {
                    head,
                    body: request[header_end..].to_vec(),
                });
            }
            requests
        });
        let backend = backend_for_endpoint(&endpoint, 1).await;
        Some((backend, request_task))
    }

    fn request_target(request: &CapturedRequest) -> &str {
        request
            .head
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .expect("request should contain a target")
    }

    fn request_header<'a>(request: &'a CapturedRequest, name: &str) -> Option<&'a str> {
        request
            .head
            .lines()
            .filter_map(|line| line.split_once(':'))
            .find_map(|(header, value)| header.eq_ignore_ascii_case(name).then(|| value.trim()))
    }

    #[tokio::test]
    async fn new_uses_canonical_path_style_sigv4_client() {
        let backend = WarmBackendWasabi::new(&test_config(), "WASABI")
            .await
            .expect("Wasabi backend should initialize without network I/O");
        let mut credentials = backend
            .s3
            .client
            .creds_provider
            .lock()
            .expect("test credentials lock should not be poisoned");
        let credentials = credentials.get().expect("static Wasabi credentials should resolve");

        assert_eq!(backend.s3.client.lookup, BucketLookupType::BucketLookupPath);
        assert_eq!(backend.s3.client.endpoint_url.scheme(), "https");
        assert_eq!(backend.s3.client.endpoint_url.host_str(), Some("s3.ap-northeast-1.wasabisys.com"));
        assert_eq!(backend.s3.client.tier_type, "wasabi");
        assert_eq!(credentials.signer_type, SignatureType::SignatureV4);
        assert_eq!(backend.s3.prefix, "archive");
        assert!(!backend.versioning_checked.load(Ordering::Acquire));
        assert!(!backend.versioning_drifted.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn validate_reads_remote_bucket_versioning_and_fails_closed() {
        for (body, should_succeed) in [
            (r#"<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>"#, true),
            (
                r#"<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>"#,
                false,
            ),
            (
                r#"<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Suspended</Status></VersioningConfiguration>"#,
                false,
            ),
            (
                r#"<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>FutureStatus</Status></VersioningConfiguration>"#,
                false,
            ),
            (
                r#"<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><MfaDelete>Enabled</MfaDelete></VersioningConfiguration>"#,
                false,
            ),
        ] {
            let Some((backend, request_task)) = backend_with_responses(vec![TestResponse {
                status: "200 OK",
                headers: "",
                body,
            }])
            .await
            else {
                return;
            };

            let result = backend.validate().await;
            let requests = request_task.await.expect("versioning request task should finish");

            assert_eq!(result.is_ok(), should_succeed, "unexpected validation result for {body}");
            assert_eq!(requests.len(), 1);
            let target = request_target(&requests[0]);
            assert!(target.starts_with("/tier-bucket"), "{target}");
            assert!(target.contains("versioning"), "{target}");
        }
    }

    #[tokio::test]
    async fn empty_remote_version_get_and_delete_omit_version_id() {
        for remote_version in [String::new(), Uuid::nil().to_string()] {
            let Some((backend, request_task)) = backend_with_responses(vec![
                TestResponse {
                    status: "200 OK",
                    headers: "ETag: \"etag\"\r\n",
                    body: "remote body",
                },
                TestResponse {
                    status: "204 No Content",
                    headers: "",
                    body: "",
                },
            ])
            .await
            else {
                return;
            };

            let mut reader = backend
                .get("remote-object", &remote_version, WarmBackendGetOpts::default())
                .await
                .expect("unversioned Wasabi object should be readable");
            let mut body = String::new();
            reader
                .read_to_string(&mut body)
                .await
                .expect("remote object body should be readable");
            backend
                .remove("remote-object", &remote_version)
                .await
                .expect("unversioned Wasabi object should be removable");
            let requests = request_task.await.expect("GET and DELETE request task should finish");

            assert_eq!(body, "remote body");
            assert_eq!(requests.len(), 2);
            assert_eq!(request_target(&requests[0]), "/tier-bucket/archive/remote-object");
            assert_eq!(request_target(&requests[1]), "/tier-bucket/archive/remote-object");
            assert!(requests[0].head.starts_with("GET "));
            assert!(requests[1].head.starts_with("DELETE "));
        }
    }

    #[tokio::test]
    async fn exact_nil_put_response_delete_preserves_version_id() {
        let Some((backend, request_task)) = backend_with_responses(vec![TestResponse {
            status: "204 No Content",
            headers: "",
            body: "",
        }])
        .await
        else {
            return;
        };
        let remote_version = Uuid::nil().to_string();

        backend
            .remove_exact("remote-object", &remote_version)
            .await
            .expect("an exact rejected PUT response should be removable");
        let requests = request_task.await.expect("DELETE request task should finish");

        assert_eq!(requests.len(), 1);
        assert!(requests[0].head.starts_with("DELETE "));
        assert_eq!(
            request_target(&requests[0]),
            format!("/tier-bucket/archive/remote-object?versionId={remote_version}")
        );
    }

    #[tokio::test]
    async fn empty_remote_version_get_rejects_versioned_response() {
        // An empty `x-amz-version-id` is rejected one layer down by the shared remote-version
        // validator, so it fails closed with that validator's message instead of the drift message.
        for (response_headers, expected_error) in [
            ("ETag: \"etag\"\r\nx-amz-version-id: unexpected-version\r\n", "bucket versioning changed"),
            ("ETag: \"etag\"\r\nx-amz-version-id:\r\n", "empty object version id header"),
            ("ETag: \"etag\"\r\nx-amz-delete-marker: true\r\n", "bucket versioning changed"),
        ] {
            let Some((backend, request_task)) = backend_with_responses(vec![TestResponse {
                status: "200 OK",
                headers: response_headers,
                body: "newer body",
            }])
            .await
            else {
                return;
            };

            let err = backend
                .get("remote-object", "", WarmBackendGetOpts::default())
                .await
                .expect_err("a versioned GET response must not return a possibly newer object");
            let requests = request_task.await.expect("GET request task should finish");

            assert!(err.to_string().contains(expected_error), "{err}");
            assert_eq!(requests.len(), 1);
            assert!(requests[0].head.starts_with("GET "));
            assert_eq!(request_target(&requests[0]), "/tier-bucket/archive/remote-object");
        }
    }

    #[tokio::test]
    async fn nonempty_remote_version_get_and_delete_send_exact_version_id() {
        let Some((backend, request_task)) = backend_with_responses(vec![
            TestResponse {
                status: "200 OK",
                headers: "ETag: \"etag\"\r\n",
                body: "legacy version body",
            },
            TestResponse {
                status: "204 No Content",
                headers: "",
                body: "",
            },
        ])
        .await
        else {
            return;
        };

        let mut reader = backend
            .get("remote-object", "remote-version-1", WarmBackendGetOpts::default())
            .await
            .expect("legacy exact-version GET should succeed");
        let mut body = String::new();
        reader
            .read_to_string(&mut body)
            .await
            .expect("legacy exact-version body should be readable");
        backend
            .remove("remote-object", "remote-version-1")
            .await
            .expect("versioned Wasabi object should use exact-version deletion");
        let requests = request_task.await.expect("GET and DELETE request task should finish");

        assert_eq!(body, "legacy version body");
        assert_eq!(requests.len(), 2);
        assert_eq!(
            request_target(&requests[0]),
            "/tier-bucket/archive/remote-object?versionId=remote-version-1"
        );
        assert_eq!(request_target(&requests[1]), request_target(&requests[0]));
        assert!(requests[0].head.starts_with("GET "));
        assert!(requests[1].head.starts_with("DELETE "));
    }

    #[tokio::test]
    async fn exact_delete_preserves_remote_no_such_version_code() {
        let Some((backend, request_task)) = backend_with_responses(vec![TestResponse {
            status: "404 Not Found",
            headers: "x-amz-request-id: request-id\r\n",
            body: "<Error><Code>NoSuchVersion</Code><Message>The specified version does not exist.</Message></Error>",
        }])
        .await
        else {
            return;
        };

        let err = backend
            .remove("remote-object", "missing-version")
            .await
            .expect_err("a missing exact version should retain its S3 error code");
        let requests = request_task.await.expect("DELETE request task should finish");

        assert!(err.to_string().contains("NoSuchVersion"), "{err}");
        assert_eq!(requests.len(), 1);
        assert_eq!(
            request_target(&requests[0]),
            "/tier-bucket/archive/remote-object?versionId=missing-version"
        );
    }

    #[tokio::test]
    async fn non_success_response_is_not_retried() {
        let listener = match TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(err) if err.kind() == io::ErrorKind::PermissionDenied => return,
            Err(err) => panic!("test listener should bind: {err}"),
        };
        let endpoint = listener
            .local_addr()
            .expect("test listener address should resolve")
            .to_string();
        let request_count = Arc::new(AtomicUsize::new(0));
        let server_count = request_count.clone();
        let server = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                server_count.fetch_add(1, Ordering::AcqRel);
                let mut request = Vec::new();
                let mut buffer = [0_u8; 1024];
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    let read = stream.read(&mut buffer).await.expect("request should be readable");
                    if read == 0 {
                        return;
                    }
                    request.extend_from_slice(&buffer[..read]);
                }
                stream
                    .write_all(
                        b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 41\r\nConnection: close\r\n\r\n<Error><Code>InternalError</Code></Error>",
                    )
                    .await
                    .expect("error response should be writable");
            }
        });
        let backend = backend_for_endpoint(&endpoint, 4).await;

        backend
            .remove("remote-object", "exact-version")
            .await
            .expect_err("the first non-success response must be returned");
        tokio::task::yield_now().await;
        assert_eq!(request_count.load(Ordering::Acquire), 1);
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn production_error_response_enforces_the_small_body_limit() {
        let listener = match TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(err) if err.kind() == io::ErrorKind::PermissionDenied => return,
            Err(err) => panic!("test listener should bind: {err}"),
        };
        let endpoint = listener
            .local_addr()
            .expect("test listener address should resolve")
            .to_string();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("S3 request should connect");
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = stream.read(&mut buffer).await.expect("request should be readable");
                assert_ne!(read, 0, "connection closed before request headers were received");
                request.extend_from_slice(&buffer[..read]);
            }
            let body = vec![b'x'; crate::client::transition_api::MAX_S3_ERROR_RESPONSE_SIZE + 1];
            let head = format!(
                "HTTP/1.1 500 Internal Server Error\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream
                .write_all(head.as_bytes())
                .await
                .expect("response headers should be writable");
            stream.write_all(&body).await.expect("response body should be writable");
        });
        let backend = backend_for_endpoint(&endpoint, 1).await;

        let err = backend
            .remove("remote-object", "exact-version")
            .await
            .expect_err("an oversized error response must fail closed");
        server.await.expect("error response task should finish");

        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn remote_error_fields_are_sanitized() {
        let Some((backend, request_task)) = backend_with_responses(vec![TestResponse {
            status: "404 Not Found",
            headers: "x-amz-request-id: request-id\r\nx-minio-error-code: secret-header-value\r\n",
            body: "<Error><Code>NoSuchVersion</Code><Message>secret-body-message</Message><RequestId>secret-request</RequestId><HostId>secret-host</HostId></Error>",
        }])
        .await
        else {
            return;
        };

        let err = backend
            .remove("remote-object", "missing-version")
            .await
            .expect_err("the malicious remote error should remain an error");
        let requests = request_task.await.expect("DELETE request task should finish");
        let rendered = format!("{err} {err:?}");

        assert!(rendered.contains("ResponseInterrupted"), "{rendered}");
        for secret in ["secret-header-value", "secret-body-message", "secret-request", "secret-host"] {
            assert!(!rendered.contains(secret), "remote error field leaked: {rendered}");
        }
        assert_eq!(requests.len(), 1);
    }

    #[tokio::test]
    async fn empty_version_delete_checks_the_real_s3_response_for_markers() {
        for (response_headers, marker_version) in [
            ("x-amz-delete-marker: true\r\n", None),
            ("x-amz-version-id: marker-version\r\n", Some("marker-version")),
        ] {
            let mut responses = vec![TestResponse {
                status: "204 No Content",
                headers: response_headers,
                body: "",
            }];
            if marker_version.is_some() {
                responses.push(TestResponse {
                    status: "204 No Content",
                    headers: "",
                    body: "",
                });
            }
            responses.push(TestResponse {
                status: "204 No Content",
                headers: "",
                body: "",
            });
            let Some((backend, request_task)) = backend_with_responses(responses).await else {
                return;
            };
            let err = backend
                .remove("remote-object", "")
                .await
                .expect_err("a delete-marker response must retain the local cleanup record");
            backend
                .remove("remote-object", "")
                .await
                .expect_err("a detected versioning drift must stop subsequent versionless deletes");
            backend
                .put("new-object", ReaderImpl::Body(hyper::body::Bytes::new()), 0)
                .await
                .expect_err("a detected versioning drift must stop subsequent uploads");
            backend
                .in_use()
                .await
                .expect_err("a detected versioning drift must make usage checks fail closed");
            backend
                .remove("remote-object", "legacy-version")
                .await
                .expect("exact cleanup must remain available after versioning drift");
            let requests = request_task.await.expect("delete request task should finish");

            assert!(err.to_string().contains("bucket versioning changed"), "{err}");
            assert_eq!(requests.len(), 2 + usize::from(marker_version.is_some()));
            assert!(requests[0].head.starts_with("DELETE "));
            assert_eq!(request_target(&requests[0]), "/tier-bucket/archive/remote-object");
            if let Some(marker_version) = marker_version {
                assert_eq!(
                    request_target(&requests[1]),
                    format!("/tier-bucket/archive/remote-object?versionId={marker_version}")
                );
            }
            assert_eq!(
                request_target(requests.last().expect("exact cleanup request should exist")),
                "/tier-bucket/archive/remote-object?versionId=legacy-version"
            );
        }
    }

    #[tokio::test]
    async fn in_use_delegates_to_the_path_style_s3_listing() {
        let Some((backend, request_task)) = backend_with_responses(vec![
            TestResponse {
                status: "200 OK",
                headers: "",
                body: r#"<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>"#,
            },
            TestResponse {
                status: "200 OK",
                headers: "",
                body: r#"<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>"#,
            },
        ])
        .await
        else {
            return;
        };

        assert!(!backend.in_use().await.expect("an empty Wasabi prefix should not be in use"));
        let requests = request_task.await.expect("LIST request task should finish");

        assert_eq!(requests.len(), 2);
        assert!(request_target(&requests[0]).contains("versioning"));
        assert!(requests[1].head.starts_with("GET "));
        let target = request_target(&requests[1]);
        // Path-style bucket-level listing addresses the bucket root, so the target keeps the
        // trailing slash before the query string.
        assert!(target.starts_with("/tier-bucket/?"), "{target}");
        assert!(target.contains("list-type=2"), "{target}");
        assert!(target.contains("prefix=archive"), "{target}");
        assert!(target.contains("max-keys=1"), "{target}");
    }

    #[tokio::test]
    async fn remote_version_validation_rejects_every_nonempty_put_version() {
        let backend = WarmBackendWasabi::new(&test_config(), "WASABI")
            .await
            .expect("Wasabi backend should initialize without network I/O");

        assert!(backend.validate_remote_version_id("").is_ok());
        assert!(backend.validate_remote_version_id(&Uuid::nil().to_string()).is_err());
        assert!(backend.validate_remote_version_id("opaque-version").is_err());
        assert!(
            backend
                .validate_remote_version_id("550e8400-e29b-41d4-a716-446655440000")
                .is_err()
        );
    }

    #[tokio::test]
    async fn rejected_put_version_latches_nonexact_operations() {
        let Some((backend, request_task)) = backend_with_responses(vec![TestResponse {
            status: "204 No Content",
            headers: "",
            body: "",
        }])
        .await
        else {
            return;
        };

        backend
            .validate_remote_version_id("unexpected-version")
            .expect_err("a real version ID must detect versioning drift");
        backend
            .put("new-object", ReaderImpl::Body(hyper::body::Bytes::new()), 0)
            .await
            .expect_err("drift must stop subsequent uploads");
        backend
            .get("remote-object", "", WarmBackendGetOpts::default())
            .await
            .expect_err("drift must stop versionless reads");
        backend
            .remove("remote-object", "")
            .await
            .expect_err("drift must stop versionless deletes");
        backend.in_use().await.expect_err("drift must make usage checks fail closed");
        backend
            .remove("remote-object", "unexpected-version")
            .await
            .expect("exact cleanup must remain available after rejecting a PUT version");
        let requests = request_task.await.expect("exact cleanup request should finish");

        assert_eq!(requests.len(), 1);
        assert_eq!(
            request_target(&requests[0]),
            "/tier-bucket/archive/remote-object?versionId=unexpected-version"
        );
    }

    #[tokio::test]
    async fn a_fresh_backend_rechecks_versioning_before_an_empty_delete() {
        let Some((backend, request_task)) = backend_with_responses(vec![TestResponse {
            status: "200 OK",
            headers: "",
            body: r#"<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>"#,
        }])
        .await
        else {
            return;
        };
        backend.versioning_checked.store(false, Ordering::Release);

        let err = backend
            .remove("remote-object", "")
            .await
            .expect_err("a restarted backend must reject versionless deletion after versioning drift");
        assert!(err.to_string().contains("must be unversioned"), "{err}");
        let requests = request_task.await.expect("versioning request task should finish");

        assert_eq!(requests.len(), 1);
        assert!(requests[0].head.starts_with("GET "));
        assert!(request_target(&requests[0]).contains("versioning"));
    }

    #[tokio::test]
    async fn a_fresh_backend_rechecks_versioning_before_put_with_meta() {
        let Some((backend, request_task)) = backend_with_responses(vec![TestResponse {
            status: "200 OK",
            headers: "",
            body: r#"<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>"#,
        }])
        .await
        else {
            return;
        };
        backend.versioning_checked.store(false, Ordering::Release);

        let err = backend
            .put_with_meta("remote-object", ReaderImpl::Body(hyper::body::Bytes::new()), 0, HashMap::new())
            .await
            .expect_err("lifecycle PUT must reject a remotely versioned Wasabi bucket before upload");
        assert!(err.to_string().contains("must be unversioned"), "{err}");
        let requests = request_task.await.expect("versioning request task should finish");

        assert_eq!(requests.len(), 1);
        assert!(requests[0].head.starts_with("GET "));
        assert!(request_target(&requests[0]).contains("versioning"));
    }

    #[tokio::test]
    async fn delete_marker_rollback_failure_latches_versioning_drift() {
        let Some((backend, request_task)) = backend_with_responses(vec![
            TestResponse {
                status: "204 No Content",
                headers: "x-amz-delete-marker: true\r\nx-amz-version-id: marker-version\r\n",
                body: "",
            },
            TestResponse {
                status: "500 Internal Server Error",
                headers: "",
                body: "<Error><Code>InternalError</Code></Error>",
            },
        ])
        .await
        else {
            return;
        };

        let err = backend
            .remove("remote-object", "")
            .await
            .expect_err("marker rollback failure must retain the cleanup record");
        backend
            .remove("remote-object", "")
            .await
            .expect_err("the drift latch must stop another versionless delete");
        let requests = request_task.await.expect("delete requests should finish");

        assert!(err.to_string().contains("failed to remove the unexpected delete marker"), "{err}");
        assert_eq!(requests.len(), 2);
        assert_eq!(request_target(&requests[0]), "/tier-bucket/archive/remote-object");
        assert_eq!(
            request_target(&requests[1]),
            "/tier-bucket/archive/remote-object?versionId=marker-version"
        );
    }

    #[tokio::test]
    async fn put_paths_send_complete_bodies_metadata_and_preserve_version_header() {
        let Some((backend, request_task)) = backend_with_responses(vec![
            TestResponse {
                status: "200 OK",
                headers: "ETag: \"etag-1\"\r\nx-amz-version-id: opaque.wasabi_01\r\n",
                body: "",
            },
            TestResponse {
                status: "200 OK",
                headers: "ETag: \"etag-2\"\r\n",
                body: "",
            },
        ])
        .await
        else {
            return;
        };
        let with_meta = vec![0x5a; 4096];
        let plain = vec![0x3c; 3072];
        let mut metadata = HashMap::new();
        metadata.insert("source-etag".to_string(), "etag-value".to_string());

        let version = backend
            .put_with_meta(
                "with-meta",
                ReaderImpl::Body(hyper::body::Bytes::from(with_meta.clone())),
                i64::try_from(with_meta.len()).expect("test body length should fit i64"),
                metadata,
            )
            .await
            .expect("Wasabi PUT with metadata should succeed");
        let unversioned = backend
            .put(
                "plain",
                ReaderImpl::Body(hyper::body::Bytes::from(plain.clone())),
                i64::try_from(plain.len()).expect("test body length should fit i64"),
            )
            .await
            .expect("unversioned Wasabi PUT should succeed");
        let requests = request_task.await.expect("PUT request task should finish");

        assert_eq!(version, "opaque.wasabi_01");
        assert!(unversioned.is_empty());
        assert_eq!(requests.len(), 2);
        assert!(requests[0].head.starts_with("PUT "));
        assert_eq!(request_target(&requests[0]), "/tier-bucket/archive/with-meta");
        assert_eq!(requests[0].body, with_meta);
        assert_eq!(request_header(&requests[0], "content-length"), Some("4096"));
        assert_eq!(request_header(&requests[0], "x-amz-meta-source-etag"), Some("etag-value"));
        assert!(requests[1].head.starts_with("PUT "));
        assert_eq!(request_target(&requests[1]), "/tier-bucket/archive/plain");
        assert_eq!(requests[1].body, plain);
        assert_eq!(request_header(&requests[1], "content-length"), Some("3072"));
        assert_eq!(request_header(&requests[1], "x-amz-meta-source-etag"), None);
    }
}
