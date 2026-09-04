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
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_assignments)]
#![allow(unused_must_use)]
#![allow(clippy::all)]

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::io::{Error, ErrorKind};
use std::sync::Arc;

use bytes::Bytes;
use google_cloud_auth::credentials::service_account::Builder;
use google_cloud_storage as gcs;
use google_cloud_storage::client::Storage;
use google_cloud_storage::client::StorageControl;
use std::convert::TryFrom;

use crate::services::tier::{
    tier_config::TierGCS,
    warm_backend::{TransitionCandidateProbe, WarmBackend, WarmBackendGetOpts},
};
use rustfs_s3_client::{
    admin_handler_utils::AdminError,
    api_put_object::PutObjectOptions,
    transition_api::{Options, ReadCloser, ReaderImpl},
};
use rustfs_utils::egress::validate_outbound_url;
use tracing::warn;

const _MAX_PART_SIZE: i64 = 1024 * 1024 * 1024 * 5;
const MAX_GCS_CANDIDATE_PAGES: usize = 64;

fn parse_generation(remote_version: &str) -> Result<Option<i64>, Error> {
    if remote_version.is_empty() {
        return Ok(None);
    }
    let generation = remote_version
        .parse::<i64>()
        .map_err(|_| Error::new(ErrorKind::InvalidData, "GCS remote version is not a valid generation"))?;
    if generation <= 0 {
        return Err(Error::new(ErrorKind::InvalidData, "GCS remote version generation must be positive"));
    }
    Ok(Some(generation))
}

fn append_gcs_chunk<E: std::fmt::Display>(
    contents: &mut Vec<u8>,
    chunk: Result<Bytes, E>,
    max_response_bytes: Option<usize>,
) -> std::io::Result<()> {
    let chunk = chunk.map_err(|err| std::io::Error::other(err.to_string()))?;
    if max_response_bytes.is_some_and(|limit| contents.len().saturating_add(chunk.len()) > limit) {
        return Err(std::io::Error::new(
            ErrorKind::InvalidData,
            "GCS object response exceeded the configured byte limit",
        ));
    }
    contents.extend_from_slice(&chunk);
    Ok(())
}

fn gcs_bucket_resource_name(bucket: &str) -> String {
    format!("projects/_/buckets/{bucket}")
}

struct GcsCandidateObject {
    name: String,
    generation: i64,
}

struct GcsCandidatePage {
    objects: Vec<GcsCandidateObject>,
    next_page_token: String,
}

async fn probe_exact_gcs_candidate<F, Fut>(
    remote_object: &str,
    mut fetch_page: F,
) -> Result<TransitionCandidateProbe, std::io::Error>
where
    F: FnMut(String) -> Fut,
    Fut: Future<Output = Result<GcsCandidatePage, std::io::Error>>,
{
    let mut page_token = String::new();
    let mut seen_page_tokens = HashSet::new();
    let mut generation = None;
    let mut pages_seen = 0_usize;

    loop {
        pages_seen += 1;
        if pages_seen > MAX_GCS_CANDIDATE_PAGES {
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                "GCS candidate listing exceeded the page limit",
            ));
        }
        let response = fetch_page(page_token.clone()).await?;
        for candidate in response.objects.iter().filter(|candidate| candidate.name == remote_object) {
            if candidate.generation <= 0 {
                return Err(std::io::Error::new(
                    ErrorKind::InvalidData,
                    "GCS candidate listing returned a non-positive generation",
                ));
            }
            if generation.replace(candidate.generation).is_some() {
                return Ok(TransitionCandidateProbe::Ambiguous);
            }
        }

        if response.next_page_token.is_empty() {
            break;
        }
        if !seen_page_tokens.insert(response.next_page_token.clone()) {
            return Err(std::io::Error::new(ErrorKind::InvalidData, "GCS candidate listing repeated a page token"));
        }
        page_token = response.next_page_token;
    }

    Ok(match generation {
        Some(generation) => TransitionCandidateProbe::VersionedPresent(generation.to_string()),
        None => TransitionCandidateProbe::Missing,
    })
}

pub struct WarmBackendGCS {
    pub client: Arc<Storage>,
    pub control: Arc<StorageControl>,
    pub bucket: String,
    pub prefix: String,
}

impl WarmBackendGCS {
    pub async fn new(conf: &TierGCS, tier: &str) -> Result<Self, std::io::Error> {
        if conf.creds == "" {
            return Err(std::io::Error::other("both access and secret keys are required"));
        }

        if conf.bucket == "" {
            return Err(std::io::Error::other("no bucket name was provided"));
        }

        if !conf.endpoint.is_empty() {
            let endpoint_url = url::Url::parse(&conf.endpoint).map_err(|e| std::io::Error::other(e.to_string()))?;
            validate_outbound_url(&endpoint_url)
                .map_err(|err| std::io::Error::other(format!("tier endpoint is not allowed: {err}")))?;
        }

        let service_account = serde_json::from_str(&conf.creds)?;
        let credentials = Builder::new(service_account)
            //.with_retry_policy(AlwaysRetry.with_attempt_limit(3))
            //.with_backoff_policy(backoff)
            .build()
            .map_err(|e| std::io::Error::other(format!("Invalid credentials JSON: {}", e)))?;

        let Ok(client) = Storage::builder()
            .with_endpoint(conf.endpoint.clone())
            .with_credentials(credentials.clone())
            .build()
            .await
        else {
            return Err(std::io::Error::other("Storage::builder error"));
        };
        let client = Arc::new(client);
        // Control-plane client: the data-plane `Storage` client cannot delete or list objects;
        // delete_object/list_objects live on StorageControl.
        let mut control_builder = StorageControl::builder().with_credentials(credentials);
        if !conf.endpoint.is_empty() {
            control_builder = control_builder.with_endpoint(conf.endpoint.clone());
        }
        let Ok(control) = control_builder.build().await else {
            return Err(std::io::Error::other("StorageControl::builder error"));
        };
        let control = Arc::new(control);
        Ok(Self {
            client,
            control,
            bucket: conf.bucket.clone(),
            prefix: conf.prefix.strip_suffix("/").unwrap_or(&conf.prefix).to_owned(),
        })
    }

    pub fn get_dest(&self, object: &str) -> String {
        let mut dest_obj = object.to_string();
        if self.prefix != "" {
            dest_obj = format!("{}/{}", &self.prefix, object);
        }
        return dest_obj;
    }
}

#[async_trait::async_trait]
impl WarmBackend for WarmBackendGCS {
    fn validate_remote_version_id(&self, remote_version_id: &str) -> Result<(), std::io::Error> {
        parse_generation(remote_version_id).map(|_| ())
    }

    async fn put_with_meta(
        &self,
        object: &str,
        r: ReaderImpl,
        length: i64,
        meta: HashMap<String, String>,
    ) -> Result<String, std::io::Error> {
        let d = match r {
            ReaderImpl::Body(content_body) => content_body.to_vec(),
            ReaderImpl::ObjectBody(mut content_body) => content_body.read_all().await?,
        };
        let bucket = gcs_bucket_resource_name(&self.bucket);
        let Ok(res) = Box::pin(
            self.client
                .write_object(&bucket, &self.get_dest(object), Bytes::from(d))
                .send_buffered(),
        )
        .await
        else {
            return Err(std::io::Error::other("write_object error"));
        };
        //self.ToObjectError(err, object)
        Ok(res.generation.to_string())
    }

    async fn put(&self, object: &str, r: ReaderImpl, length: i64) -> Result<String, std::io::Error> {
        self.put_with_meta(object, r, length, HashMap::new()).await
    }

    async fn get(&self, object: &str, rv: &str, opts: WarmBackendGetOpts) -> Result<ReadCloser, std::io::Error> {
        let bucket = gcs_bucket_resource_name(&self.bucket);
        let mut req = self.client.read_object(&bucket, &self.get_dest(object));
        let mut max_response_bytes = None;
        if let Some(generation) = parse_generation(rv)? {
            req = req.set_generation(generation);
        }

        // Honor the requested byte range so Range GETs on tiered objects return the exact
        // interval instead of the whole object (matches the s3/s3sdk/rustfs warm backends).
        if opts.start_offset >= 0 && opts.length > 0 {
            let offset: u64 = opts
                .start_offset
                .try_into()
                .map_err(|_| std::io::Error::other("invalid range: negative start_offset"))?;
            let count: u64 = opts
                .length
                .try_into()
                .map_err(|_| std::io::Error::other("invalid range: negative length"))?;
            max_response_bytes = Some(
                opts.length
                    .try_into()
                    .map_err(|_| std::io::Error::other("invalid range: length does not fit in memory"))?,
            );
            req = req.set_read_range(google_cloud_storage::model_ext::ReadRange::segment(offset, count));
        }

        let Ok(mut reader) = req.send().await else {
            return Err(std::io::Error::other("read_object error"));
        };
        let mut contents = Vec::new();
        while let Some(chunk) = reader.next().await {
            append_gcs_chunk(&mut contents, chunk, max_response_bytes)?;
        }
        Ok(ReadCloser::new(std::io::Cursor::new(contents)))
    }

    async fn remove(&self, object: &str, rv: &str) -> Result<(), std::io::Error> {
        // gRPC v2 DeleteObject requires the bucket in resource-name form. Without this the
        // deleted tiered object was never removed from GCS (empty impl returned Ok), leaking
        // remote data forever.
        let mut req = self
            .control
            .delete_object()
            .set_bucket(gcs_bucket_resource_name(&self.bucket))
            .set_object(self.get_dest(object));
        if let Some(generation) = parse_generation(rv)? {
            req = req.set_generation(generation);
        }
        req.send().await.map_err(|e| std::io::Error::other(e.to_string()))?;
        Ok(())
    }

    async fn probe_transition_candidate(&self, object: &str) -> Result<TransitionCandidateProbe, std::io::Error> {
        let remote_object = self.get_dest(object);
        let parent = gcs_bucket_resource_name(&self.bucket);
        probe_exact_gcs_candidate(&remote_object, |page_token| {
            let control = self.control.clone();
            let parent = parent.clone();
            let prefix = remote_object.clone();
            async move {
                let response = control
                    .list_objects()
                    .set_parent(parent)
                    .set_prefix(prefix)
                    .set_versions(true)
                    .set_page_size(2)
                    .set_page_token(page_token)
                    .send()
                    .await
                    .map_err(|err| std::io::Error::other(err.to_string()))?;
                Ok(GcsCandidatePage {
                    objects: response
                        .objects
                        .into_iter()
                        .map(|candidate| GcsCandidateObject {
                            name: candidate.name,
                            generation: candidate.generation,
                        })
                        .collect(),
                    next_page_token: response.next_page_token,
                })
            }
        })
        .await
    }

    async fn in_use(&self) -> Result<bool, std::io::Error> {
        // Scope the listing to this tier's prefix (matching the other warm backends) and only
        // need to know whether a single object exists.
        let resp = self
            .control
            .list_objects()
            .set_parent(gcs_bucket_resource_name(&self.bucket))
            .set_prefix(self.prefix.clone())
            .set_page_size(1)
            .send()
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        Ok(!resp.objects.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::GcsCandidateObject;
    use super::GcsCandidatePage;
    use super::MAX_GCS_CANDIDATE_PAGES;
    use super::WarmBackendGCS;
    use super::append_gcs_chunk;
    use super::gcs_bucket_resource_name;
    use super::parse_generation;
    use super::probe_exact_gcs_candidate;
    use crate::services::tier::tier_config::TierGCS;
    use crate::services::tier::warm_backend::{TransitionCandidateProbe, WarmBackend, WarmBackendGetOpts};
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use google_cloud_storage::client::{Storage, StorageControl};
    use std::io::ErrorKind;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    async fn serve_data_plane_fixture(listener: TcpListener) -> Vec<String> {
        let upload_body = r#"{"name":"probe","bucket":"tier-bucket","generation":"123"}"#;
        let responses = [
            format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{upload_body}",
                upload_body.len()
            ),
            "HTTP/1.1 206 Partial Content\r\ncontent-type: application/octet-stream\r\ncontent-range: bytes 0-6/7\r\nx-goog-generation: 123\r\ncontent-length: 7\r\nconnection: close\r\n\r\nRustFS!"
                .to_string(),
            "HTTP/1.1 206 Partial Content\r\ncontent-type: application/octet-stream\r\ncontent-range: bytes 0-7/8\r\nx-goog-generation: 123\r\ncontent-length: 8\r\nconnection: close\r\n\r\nRustFS!!"
                .to_string(),
        ];
        let mut requests = Vec::new();

        for response in responses {
            let (mut stream, _) = listener.accept().await.expect("the GCS fixture should accept a request");
            let mut request = Vec::new();
            loop {
                let mut chunk = [0_u8; 1024];
                let count = stream
                    .read(&mut chunk)
                    .await
                    .expect("the GCS fixture should read request headers");
                if count == 0 {
                    break;
                }
                request.extend_from_slice(&chunk[..count]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            let header_end = request
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .map(|position| position + 4)
                .expect("the GCS fixture should receive complete request headers");
            let headers = String::from_utf8_lossy(&request[..header_end]);
            if headers.lines().any(|line| line.eq_ignore_ascii_case("expect: 100-continue")) {
                stream
                    .write_all(b"HTTP/1.1 100 Continue\r\n\r\n")
                    .await
                    .expect("the GCS fixture should acknowledge 100-continue");
            }
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().expect("content-length should be numeric"))
                })
                .unwrap_or_default();
            while request.len() < header_end.saturating_add(content_length) {
                let mut chunk = [0_u8; 1024];
                let count = stream
                    .read(&mut chunk)
                    .await
                    .expect("the GCS fixture should read the request body");
                if count == 0 {
                    break;
                }
                request.extend_from_slice(&chunk[..count]);
            }
            requests.push(String::from_utf8_lossy(&request).into_owned());
            stream
                .write_all(response.as_bytes())
                .await
                .expect("the GCS fixture should write its response");
        }

        requests
    }

    fn candidate_page(objects: &[(&str, i64)], next_page_token: &str) -> GcsCandidatePage {
        GcsCandidatePage {
            objects: objects
                .iter()
                .map(|(name, generation)| GcsCandidateObject {
                    name: (*name).to_string(),
                    generation: *generation,
                })
                .collect(),
            next_page_token: next_page_token.to_string(),
        }
    }

    async fn probe_candidate_pages(
        remote_object: &str,
        pages: Vec<GcsCandidatePage>,
    ) -> (Result<TransitionCandidateProbe, std::io::Error>, Vec<String>) {
        let mut pages = pages.into_iter();
        let mut requested_tokens = Vec::new();
        let result = probe_exact_gcs_candidate(remote_object, |page_token| {
            requested_tokens.push(page_token);
            std::future::ready(
                pages
                    .next()
                    .ok_or_else(|| std::io::Error::new(ErrorKind::UnexpectedEof, "test fixture ran out of GCS pages")),
            )
        })
        .await;
        (result, requested_tokens)
    }

    #[test]
    fn generation_parser_preserves_exact_numeric_versions() {
        assert_eq!(parse_generation("").expect("empty generation means no version condition"), None);
        assert_eq!(parse_generation("1").expect("minimum generation should parse"), Some(1));
        assert_eq!(
            parse_generation(&i64::MAX.to_string()).expect("maximum generation should parse"),
            Some(i64::MAX)
        );
    }

    #[test]
    fn generation_parser_rejects_unknown_or_non_positive_versions() {
        for value in ["unknown", "1.0", "-1", "0", "9223372036854775808"] {
            let err = parse_generation(value).expect_err("unknown generation must fail closed");
            assert_eq!(err.kind(), ErrorKind::InvalidData, "{value}");
        }
    }

    #[test]
    fn body_collection_propagates_an_error_after_a_complete_prefix() {
        let mut contents = Vec::new();
        append_gcs_chunk::<std::io::Error>(&mut contents, Ok(bytes::Bytes::from_static(b"RustFS")), Some(7))
            .expect("the prefix chunk should be collected");
        let err = append_gcs_chunk(&mut contents, Err(std::io::Error::other("trailing stream failure")), Some(7))
            .expect_err("a trailing stream error must not be mistaken for EOF");

        assert_eq!(contents, b"RustFS");
        assert!(err.to_string().contains("trailing stream failure"));
    }

    #[test]
    fn body_collection_rejects_a_chunk_that_exceeds_the_probe_limit() {
        let mut contents = Vec::new();
        let err = append_gcs_chunk::<std::io::Error>(&mut contents, Ok(bytes::Bytes::from_static(b"RustFSxx")), Some(7))
            .expect_err("the GCS collection layer must reject an oversized probe response");

        assert!(contents.is_empty());
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn candidate_probe_finds_exact_name_on_first_or_later_page() {
        let (first, first_tokens) =
            probe_candidate_pages("prefix/object", vec![candidate_page(&[("prefix/object", 7)], "")]).await;
        assert_eq!(
            first.expect("an exact first-page object should be discovered"),
            TransitionCandidateProbe::VersionedPresent("7".to_string())
        );
        assert_eq!(first_tokens, [""]);

        let (later, later_tokens) = probe_candidate_pages(
            "prefix/object",
            vec![
                candidate_page(&[("prefix/object-shadow", 8)], "next"),
                candidate_page(&[("prefix/object", 9)], ""),
            ],
        )
        .await;
        assert_eq!(
            later.expect("an exact later-page object should be discovered"),
            TransitionCandidateProbe::VersionedPresent("9".to_string())
        );
        assert_eq!(later_tokens, ["", "next"]);
    }

    #[tokio::test]
    async fn candidate_probe_ignores_non_exact_prefix_matches() {
        let (probe, _) = probe_candidate_pages(
            "prefix/object",
            vec![candidate_page(
                &[("prefix/object-shadow", 8), ("prefix/object/child", 9), ("prefix/object", 7)],
                "",
            )],
        )
        .await;

        assert_eq!(
            probe.expect("prefix-only matches should not hide the exact object"),
            TransitionCandidateProbe::VersionedPresent("7".to_string())
        );
    }

    #[tokio::test]
    async fn candidate_probe_reports_duplicate_exact_names_as_ambiguous() {
        let (probe, _) = probe_candidate_pages(
            "prefix/object",
            vec![
                candidate_page(&[("prefix/object", 7)], "next"),
                candidate_page(&[("prefix/object", 8)], ""),
            ],
        )
        .await;

        assert_eq!(
            probe.expect("multiple exact generations should produce a conservative result"),
            TransitionCandidateProbe::Ambiguous
        );
    }

    #[tokio::test]
    async fn candidate_probe_reports_missing_without_an_exact_name() {
        let (probe, _) = probe_candidate_pages("prefix/object", vec![candidate_page(&[("prefix/object-shadow", 8)], "")]).await;

        assert_eq!(
            probe.expect("a complete listing without an exact name should be definitive"),
            TransitionCandidateProbe::Missing
        );
    }

    #[tokio::test]
    async fn candidate_probe_rejects_non_positive_generations() {
        for generation in [0, -1] {
            let (probe, _) =
                probe_candidate_pages("prefix/object", vec![candidate_page(&[("prefix/object", generation)], "")]).await;
            let err = probe.expect_err("a non-positive GCS generation must fail closed");
            assert_eq!(err.kind(), ErrorKind::InvalidData, "generation {generation}");
        }
    }

    #[tokio::test]
    async fn candidate_probe_rejects_a_page_token_that_does_not_advance() {
        let (probe, requested_tokens) =
            probe_candidate_pages("prefix/object", vec![candidate_page(&[], "next"), candidate_page(&[], "next")]).await;

        let err = probe.expect_err("a repeated GCS page token must fail closed");
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert_eq!(requested_tokens, ["", "next"]);
    }

    #[tokio::test]
    async fn candidate_probe_rejects_a_non_adjacent_page_token_cycle() {
        let (probe, requested_tokens) = probe_candidate_pages(
            "prefix/object",
            vec![candidate_page(&[], "a"), candidate_page(&[], "b"), candidate_page(&[], "a")],
        )
        .await;

        let err = probe.expect_err("a non-adjacent GCS page token cycle must fail closed");
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert_eq!(requested_tokens, ["", "a", "b"]);
    }

    #[tokio::test]
    async fn candidate_probe_rejects_an_unbounded_unique_token_chain() {
        let pages = (0..MAX_GCS_CANDIDATE_PAGES)
            .map(|index| candidate_page(&[], &format!("token-{index}")))
            .collect();
        let (probe, requested_tokens) = probe_candidate_pages("prefix/object", pages).await;

        let err = probe.expect_err("an unbounded unique page-token chain must fail closed");
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert_eq!(requested_tokens.len(), MAX_GCS_CANDIDATE_PAGES);
    }

    #[tokio::test]
    async fn plain_bucket_reaches_gcs_put_and_get_transport_with_resource_name() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the GCS fixture should bind a loopback port");
        let endpoint = format!("http://{}", listener.local_addr().expect("the GCS fixture should have a local address"));
        let fixture = tokio::spawn(serve_data_plane_fixture(listener));
        let credentials = Anonymous::new().build();
        let client = Storage::builder()
            .with_endpoint(endpoint.clone())
            .with_credentials(credentials.clone())
            .build()
            .await
            .expect("the GCS data client should build");
        let control = StorageControl::builder()
            .with_endpoint(endpoint)
            .with_credentials(credentials)
            .build()
            .await
            .expect("the GCS control client should build");
        let backend = WarmBackendGCS {
            client: Arc::new(client),
            control: Arc::new(control),
            bucket: "tier-bucket".to_string(),
            prefix: String::new(),
        };

        let (version, body, oversized_error_kind, requests) = tokio::time::timeout(Duration::from_secs(5), async {
            let version = backend
                .put(
                    "probe",
                    rustfs_s3_client::transition_api::ReaderImpl::Body(bytes::Bytes::from_static(b"RustFS")),
                    6,
                )
                .await
                .expect("a plain configured bucket should reach the GCS upload transport");
            let mut reader = backend
                .get(
                    "probe",
                    &version,
                    WarmBackendGetOpts {
                        start_offset: 0,
                        length: 7,
                    },
                )
                .await
                .expect("a plain configured bucket should reach the GCS read transport");
            let mut body = Vec::new();
            reader
                .read_to_end(&mut body)
                .await
                .expect("the fixture body should be readable");
            let oversized_error = match backend
                .get(
                    "probe",
                    &version,
                    WarmBackendGetOpts {
                        start_offset: 0,
                        length: 7,
                    },
                )
                .await
            {
                Ok(_) => panic!("an eight-byte response must not pass a seven-byte collection limit"),
                Err(err) => err,
            };
            let requests = fixture.await.expect("the GCS fixture task should finish");
            (version, body, oversized_error.kind(), requests)
        })
        .await
        .expect("the GCS data-plane requests should not be rejected before transport");

        assert_eq!(gcs_bucket_resource_name("tier-bucket"), "projects/_/buckets/tier-bucket");
        assert_eq!(version, "123");
        assert_eq!(body, b"RustFS!");
        assert_eq!(oversized_error_kind, ErrorKind::InvalidData);
        assert!(
            requests[0].starts_with("POST /upload/storage/v1/b/tier-bucket/o?"),
            "unexpected upload request line: {}",
            requests[0].lines().next().unwrap_or_default()
        );
        assert!(
            requests[1].starts_with("GET /storage/v1/b/tier-bucket/o/probe?"),
            "unexpected read request line: {}",
            requests[1].lines().next().unwrap_or_default()
        );
        assert!(
            requests[1].to_ascii_lowercase().contains("\r\nrange: bytes=0-6\r\n"),
            "the GCS probe read must preserve its seven-byte range"
        );
        assert!(
            requests[2].starts_with("GET /storage/v1/b/tier-bucket/o/probe?"),
            "unexpected oversized read request line: {}",
            requests[2].lines().next().unwrap_or_default()
        );
        assert!(
            requests[2].to_ascii_lowercase().contains("\r\nrange: bytes=0-6\r\n"),
            "the oversized response must be fetched under the same seven-byte request boundary"
        );
    }

    #[tokio::test]
    async fn new_rejects_loopback_endpoint_before_credential_setup() {
        let conf = TierGCS {
            endpoint: "https://127.0.0.1:9000".to_string(),
            creds: "not-json".to_string(),
            bucket: "tier-bucket".to_string(),
            ..Default::default()
        };

        match WarmBackendGCS::new(&conf, "tier").await {
            Ok(_) => panic!("loopback endpoint should be rejected"),
            Err(err) => assert!(err.to_string().contains("not allowed"), "unexpected error: {err}"),
        }
    }
}

/*fn gcs_to_object_error(err: Error, params: Vec<String>) -> Option<Error> {
  if err == nil {
    return nil
  }

  bucket := ""
  object := ""
  uploadID := ""
  if len(params) >= 1 {
    bucket = params[0]
  }
  if len(params) == 2 {
    object = params[1]
  }
  if len(params) == 3 {
    uploadID = params[2]
  }

  // in some cases just a plain error is being returned
  switch err.Error() {
  case "storage: bucket doesn't exist":
    err = BucketNotFound{
      Bucket: bucket,
    }
    return err
  case "storage: object doesn't exist":
    if uploadID != "" {
      err = InvalidUploadID{
        UploadID: uploadID,
      }
    } else {
      err = ObjectNotFound{
        Bucket: bucket,
        Object: object,
      }
    }
    return err
  }

  googleAPIErr, ok := err.(*googleapi.Error)
  if !ok {
    // We don't interpret non MinIO errors. As minio errors will
    // have StatusCode to help to convert to object errors.
    return err
  }

  if len(googleAPIErr.Errors) == 0 {
    return err
  }

  reason := googleAPIErr.Errors[0].Reason
  message := googleAPIErr.Errors[0].Message

  switch reason {
  case "required":
    // Anonymous users does not have storage.xyz access to project 123.
    fallthrough
  case "keyInvalid":
    fallthrough
  case "forbidden":
    err = PrefixAccessDenied{
      Bucket: bucket,
      Object: object,
    }
  case "invalid":
    err = BucketNameInvalid{
      Bucket: bucket,
    }
  case "notFound":
    if object != "" {
      err = ObjectNotFound{
        Bucket: bucket,
        Object: object,
      }
      break
    }
    err = BucketNotFound{Bucket: bucket}
  case "conflict":
    if message == "You already own this bucket. Please select another name." {
      err = BucketAlreadyOwnedByYou{Bucket: bucket}
      break
    }
    if message == "Sorry, that name is not available. Please try a different one." {
      err = BucketAlreadyExists{Bucket: bucket}
      break
    }
    err = BucketNotEmpty{Bucket: bucket}
  }

  return err
}*/
