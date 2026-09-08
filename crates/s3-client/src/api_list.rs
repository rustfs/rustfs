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

use crate::{
    api_error_response::http_resp_to_error_response,
    api_s3_datatypes::{
        ListBucketResult, ListBucketV2Result, ListMultipartUploadsResult, ListObjectPartsResult, ListVersionsResult, ObjectPart,
    },
    credentials,
    transition_api::{ReaderImpl, RequestMetadata, TransitionClient, collect_response_body},
};
use http::{HeaderMap, StatusCode};
use hyper::body::Body;
use hyper::body::Bytes;
use rustfs_config::MAX_S3_CLIENT_RESPONSE_SIZE;
use rustfs_storage_api::BucketInfo;
use rustfs_utils::hash::EMPTY_STRING_SHA256_HASH;
use std::collections::HashMap;
use std::io::ErrorKind;

impl TransitionClient {
    pub fn list_buckets(&self) -> Result<Vec<BucketInfo>, std::io::Error> {
        Err(std::io::Error::new(
            ErrorKind::Unsupported,
            credentials::ErrorResponse {
                sts_error: credentials::STSError {
                    r#type: "".to_string(),
                    code: "NotImplemented".to_string(),
                    message: "The list_buckets API is not implemented in this build.".to_string(),
                },
                request_id: "".to_string(),
            },
        ))
    }

    pub async fn list_objects_v2_query(
        &self,
        bucket_name: &str,
        object_prefix: &str,
        continuation_token: &str,
        fetch_owner: bool,
        metadata: bool,
        delimiter: &str,
        start_after: &str,
        max_keys: i64,
        headers: HeaderMap,
    ) -> Result<ListBucketV2Result, std::io::Error> {
        let mut url_values = HashMap::new();

        url_values.insert("list-type".to_string(), "2".to_string());
        if metadata {
            url_values.insert("metadata".to_string(), "true".to_string());
        }
        if start_after != "" {
            url_values.insert("start-after".to_string(), start_after.to_string());
        }
        url_values.insert("encoding-type".to_string(), "url".to_string());
        url_values.insert("prefix".to_string(), object_prefix.to_string());
        url_values.insert("delimiter".to_string(), delimiter.to_string());

        if continuation_token != "" {
            url_values.insert("continuation-token".to_string(), continuation_token.to_string());
        }

        if fetch_owner {
            url_values.insert("fetch-owner".to_string(), "true".to_string());
        }

        if max_keys > 0 {
            url_values.insert("max-keys".to_string(), max_keys.to_string());
        }

        let resp = self
            .execute_method(
                http::Method::GET,
                &mut RequestMetadata {
                    bucket_name: bucket_name.to_string(),
                    object_name: "".to_string(),
                    query_values: url_values,
                    content_sha256_hex: EMPTY_STRING_SHA256_HASH.to_string(),
                    custom_header: headers,
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

        let resp_status = resp.status();
        let h = resp.headers().clone();

        if resp.status() != StatusCode::OK {
            return Err(std::io::Error::other(http_resp_to_error_response(
                resp_status,
                &h,
                vec![],
                bucket_name,
                "",
            )));
        }

        //let mut list_bucket_result = ListBucketV2Result::default();
        let body_vec = self
            .collect_response_body(resp.into_body(), MAX_S3_CLIENT_RESPONSE_SIZE)
            .await?;
        let mut list_bucket_result = match quick_xml::de::from_str::<ListBucketV2Result>(&String::from_utf8_lossy(&body_vec)) {
            Ok(result) => result,
            Err(err) => {
                return Err(std::io::Error::other(err.to_string()));
            }
        };
        //println!("list_bucket_result: {:?}", list_bucket_result);

        if list_bucket_result.is_truncated && list_bucket_result.next_continuation_token == "" {
            return Err(std::io::Error::other(credentials::ErrorResponse {
                sts_error: credentials::STSError {
                    r#type: "".to_string(),
                    code: "NotImplemented".to_string(),
                    message: "Truncated response should have continuation token set".to_string(),
                },
                request_id: "".to_string(),
            }));
        }

        for (i, obj) in list_bucket_result.contents.iter_mut().enumerate() {
            obj.name = decode_s3_name(&obj.name, &list_bucket_result.encoding_type)?;
            //list_bucket_result.contents[i].mod_time = list_bucket_result.contents[i].mod_time.Truncate(time.Millisecond);
        }

        for (i, obj) in list_bucket_result.common_prefixes.iter_mut().enumerate() {
            obj.prefix = decode_s3_name(&obj.prefix, &list_bucket_result.encoding_type)?;
        }

        Ok(list_bucket_result)
    }

    pub async fn list_object_versions_query(
        &self,
        bucket_name: &str,
        opts: &ListObjectsOptions,
        key_marker: &str,
        version_id_marker: &str,
        delimiter: &str,
    ) -> Result<ListVersionsResult, std::io::Error> {
        let mut url_values = HashMap::new();
        url_values.insert("versions".to_string(), "".to_string());
        url_values.insert("prefix".to_string(), opts.prefix.clone());
        url_values.insert("delimiter".to_string(), delimiter.to_string());
        url_values.insert("encoding-type".to_string(), "url".to_string());

        if !key_marker.is_empty() {
            url_values.insert("key-marker".to_string(), key_marker.to_string());
        }
        if opts.max_keys > 0 {
            url_values.insert("max-keys".to_string(), opts.max_keys.to_string());
        }
        if !version_id_marker.is_empty() {
            url_values.insert("version-id-marker".to_string(), version_id_marker.to_string());
        }
        if opts.with_metadata {
            url_values.insert("metadata".to_string(), "true".to_string());
        }

        let mut resp = self
            .execute_method(
                http::Method::GET,
                &mut RequestMetadata {
                    bucket_name: bucket_name.to_string(),
                    object_name: "".to_string(),
                    query_values: url_values,
                    content_sha256_hex: EMPTY_STRING_SHA256_HASH.to_string(),
                    custom_header: opts.headers.clone(),
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

        let resp_status = resp.status();
        let headers = resp.headers().clone();
        let body = self
            .collect_response_body(resp.into_body(), MAX_S3_CLIENT_RESPONSE_SIZE)
            .await?;
        if resp_status != StatusCode::OK {
            return Err(std::io::Error::other(http_resp_to_error_response(
                resp_status,
                &headers,
                body,
                bucket_name,
                "",
            )));
        }

        let mut versions = quick_xml::de::from_reader::<_, ListVersionsResult>(body.as_slice())
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
        for version in &mut versions.versions {
            version.key = decode_s3_name(&version.key, &versions.encoding_type)?;
        }
        for marker in &mut versions.delete_markers {
            marker.key = decode_s3_name(&marker.key, &versions.encoding_type)?;
        }
        for prefix in &mut versions.common_prefixes {
            prefix.prefix = decode_s3_name(&prefix.prefix, &versions.encoding_type)?;
        }
        if !versions.next_key_marker.is_empty() {
            versions.next_key_marker = decode_s3_name(&versions.next_key_marker, &versions.encoding_type)?;
        }

        if versions.is_truncated && versions.next_key_marker.is_empty() {
            return Err(std::io::Error::other(credentials::ErrorResponse {
                sts_error: credentials::STSError {
                    r#type: "".to_string(),
                    code: "NotImplemented".to_string(),
                    message: "Truncated ListObjectVersions response should have next key marker set".to_string(),
                },
                request_id: "".to_string(),
            }));
        }

        Ok(versions)
    }

    pub fn list_objects_query(
        &self,
        bucket_name: &str,
        object_prefix: &str,
        object_marker: &str,
        delimiter: &str,
        max_keys: i64,
        headers: HeaderMap,
    ) -> Result<ListBucketResult, std::io::Error> {
        Err(std::io::Error::new(
            ErrorKind::Unsupported,
            credentials::ErrorResponse {
                sts_error: credentials::STSError {
                    r#type: "".to_string(),
                    code: "NotImplemented".to_string(),
                    message: format!("list_objects_query is not implemented for bucket {bucket_name}"),
                },
                request_id: "".to_string(),
            },
        ))
    }

    pub fn list_multipart_uploads_query(
        &self,
        bucket_name: &str,
        key_marker: &str,
        upload_id_marker: &str,
        prefix: &str,
        delimiter: &str,
        max_uploads: i64,
    ) -> Result<ListMultipartUploadsResult, std::io::Error> {
        Err(std::io::Error::new(
            ErrorKind::Unsupported,
            credentials::ErrorResponse {
                sts_error: credentials::STSError {
                    r#type: "".to_string(),
                    code: "NotImplemented".to_string(),
                    message: format!("list_multipart_uploads_query is not implemented for bucket {bucket_name}"),
                },
                request_id: "".to_string(),
            },
        ))
    }

    pub fn list_object_parts(
        &self,
        bucket_name: &str,
        object_name: &str,
        upload_id: &str,
    ) -> Result<HashMap<i64, ObjectPart>, std::io::Error> {
        Err(std::io::Error::new(
            ErrorKind::Unsupported,
            credentials::ErrorResponse {
                sts_error: credentials::STSError {
                    r#type: "".to_string(),
                    code: "NotImplemented".to_string(),
                    message: format!(
                        "list_object_parts is not implemented for bucket {bucket_name}, object {object_name}, upload_id {upload_id}"
                    ),
                },
                request_id: "".to_string(),
            },
        ))
    }

    pub fn find_upload_ids(&self, bucket_name: &str, object_name: &str) -> Result<Vec<String>, std::io::Error> {
        Err(std::io::Error::new(
            ErrorKind::Unsupported,
            credentials::ErrorResponse {
                sts_error: credentials::STSError {
                    r#type: "".to_string(),
                    code: "NotImplemented".to_string(),
                    message: format!("find_upload_ids is not implemented for bucket {bucket_name}, object {object_name}"),
                },
                request_id: "".to_string(),
            },
        ))
    }

    pub async fn list_object_parts_query(
        &self,
        bucket_name: &str,
        object_name: &str,
        upload_id: &str,
        part_number_marker: i64,
        max_parts: i64,
    ) -> Result<ListObjectPartsResult, std::io::Error> {
        Err(std::io::Error::new(
            ErrorKind::Unsupported,
            credentials::ErrorResponse {
                sts_error: credentials::STSError {
                    r#type: "".to_string(),
                    code: "NotImplemented".to_string(),
                    message: format!(
                        "list_object_parts_query is not implemented for bucket {bucket_name}, object {object_name}, upload_id {upload_id}"
                    ),
                },
                request_id: "".to_string(),
            },
        ))
    }
}

#[derive(Default)]
pub struct ListObjectsOptions {
    reverse_versions: bool,
    with_versions: bool,
    with_metadata: bool,
    prefix: String,
    recursive: bool,
    max_keys: i64,
    start_after: String,
    use_v1: bool,
    headers: HeaderMap,
}

impl ListObjectsOptions {
    pub fn set(&mut self, key: &str, value: &str) {
        match key {
            "prefix" => {
                self.prefix = value.to_string();
            }
            "start-after" => {
                self.start_after = value.to_string();
            }
            "max-keys" => {
                if let Ok(v) = value.parse::<i64>() {
                    self.max_keys = v;
                }
            }
            "delimiter" => {
                // delimiter is currently kept in request only; this option structure does not persist it yet.
            }
            "reverse" | "versions" | "metadata" | "recursive" | "use-v1" => {
                if let Some(v) = value.strip_prefix("v").or_else(|| value.strip_prefix("V")) {
                    let v = v.eq_ignore_ascii_case("true");
                    match key {
                        "reverse" => self.reverse_versions = v,
                        "versions" => self.with_versions = v,
                        "metadata" => self.with_metadata = v,
                        "recursive" => self.recursive = v,
                        _ => self.use_v1 = v,
                    }
                } else {
                    let v = value.eq_ignore_ascii_case("true");
                    match key {
                        "reverse" => self.reverse_versions = v,
                        "versions" => self.with_versions = v,
                        "metadata" => self.with_metadata = v,
                        "recursive" => self.recursive = v,
                        _ => self.use_v1 = v,
                    }
                }
            }
            _ => {}
        }
    }
}

fn decode_s3_name(name: &str, encoding_type: &str) -> Result<String, std::io::Error> {
    match encoding_type {
        "url" => {
            //return url::QueryUnescape(name);
            return Ok(name.to_string());
        }
        _ => {
            return Ok(name.to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        credentials::{Credentials, SignatureType, Static, Value},
        transition_api::{BucketLookupType, Options, TransitionClientTimeouts},
    };
    use std::time::Duration;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    fn timeout_test_options() -> Options {
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
        }
    }

    #[test]
    fn list_versions_xml_preserves_versions_and_delete_markers() {
        let xml = br#"
            <ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Name>tier-bucket</Name>
                <Prefix>archive/object</Prefix>
                <KeyMarker></KeyMarker>
                <VersionIdMarker></VersionIdMarker>
                <MaxKeys>2</MaxKeys>
                <IsTruncated>true</IsTruncated>
                <NextKeyMarker>archive/object</NextKeyMarker>
                <NextVersionIdMarker>version-a</NextVersionIdMarker>
                <Version>
                    <Key>archive/object</Key>
                    <VersionId>version-a</VersionId>
                    <IsLatest>true</IsLatest>
                    <LastModified>2026-07-22T00:00:00Z</LastModified>
                    <ETag>&quot;etag-a&quot;</ETag>
                    <Size>5</Size>
                    <StorageClass>STANDARD</StorageClass>
                </Version>
                <DeleteMarker>
                    <Key>archive/object</Key>
                    <VersionId>marker-a</VersionId>
                    <IsLatest>false</IsLatest>
                    <LastModified>2026-07-22T00:00:01Z</LastModified>
                </DeleteMarker>
            </ListVersionsResult>
        "#;

        let parsed =
            quick_xml::de::from_reader::<_, ListVersionsResult>(xml.as_slice()).expect("ListObjectVersions XML should parse");

        assert!(parsed.is_truncated);
        assert_eq!(parsed.next_key_marker, "archive/object");
        assert_eq!(parsed.next_version_id_marker, "version-a");
        assert_eq!(parsed.versions.len(), 1);
        assert_eq!(parsed.versions[0].key, "archive/object");
        assert_eq!(parsed.versions[0].version_id, "version-a");
        assert_eq!(parsed.delete_markers.len(), 1);
        assert_eq!(parsed.delete_markers[0].version_id, "marker-a");
    }

    // Regression test for backlog#2076: a ListObjectsV2 response for a delimited
    // listing over a bucket that holds nested-key objects (e.g. any warm-tier
    // target that already stores more than one flat object) includes a
    // <CommonPrefixes><Prefix>...</Prefix></CommonPrefixes> element. `CommonPrefix`
    // previously had no `rename_all = "PascalCase"`, so quick_xml looked for a
    // lowercase `<prefix>` child, never found one, and (with no `#[serde(default)]`
    // either) failed the whole response with "missing field `prefix`" — surfacing to
    // callers of `WarmBackendS3::in_use()` (tier add/remove) as `TierPermErr`.
    #[test]
    fn list_objects_v2_xml_parses_common_prefixes() {
        let xml = br#"
            <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Name>warm-bucket</Name>
                <Prefix></Prefix>
                <Delimiter>/</Delimiter>
                <MaxKeys>1</MaxKeys>
                <IsTruncated>false</IsTruncated>
                <CommonPrefixes>
                    <Prefix>subdir/</Prefix>
                </CommonPrefixes>
            </ListBucketResult>
        "#;

        let parsed = quick_xml::de::from_reader::<_, ListBucketV2Result>(xml.as_slice()).expect("ListObjectsV2 XML should parse");

        assert_eq!(parsed.common_prefixes.len(), 1);
        assert_eq!(parsed.common_prefixes[0].prefix, "subdir/");
    }

    // Same fixture shape as list_object_versions_query hits (ListVersionsResult
    // reuses the same CommonPrefix type).
    #[test]
    fn list_object_versions_xml_parses_common_prefixes() {
        let xml = br#"
            <ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Name>warm-bucket</Name>
                <Prefix></Prefix>
                <KeyMarker></KeyMarker>
                <VersionIdMarker></VersionIdMarker>
                <MaxKeys>1</MaxKeys>
                <IsTruncated>false</IsTruncated>
                <CommonPrefixes>
                    <Prefix>subdir/</Prefix>
                </CommonPrefixes>
            </ListVersionsResult>
        "#;

        let parsed =
            quick_xml::de::from_reader::<_, ListVersionsResult>(xml.as_slice()).expect("ListObjectVersions XML should parse");

        assert_eq!(parsed.common_prefixes.len(), 1);
        assert_eq!(parsed.common_prefixes[0].prefix, "subdir/");
    }

    #[tokio::test]
    async fn list_objects_v2_body_stall_returns_timed_out() {
        let listener = match TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return,
            Err(err) => panic!("test listener should bind: {err}"),
        };
        let endpoint = listener
            .local_addr()
            .expect("listener local address should be available")
            .to_string();
        let fixture = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("fixture should accept one list request");
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
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 512\r\nConnection: close\r\n\r\n<ListBucketResult><Name>warm")
                .await
                .expect("fixture should write a partial list response");
            tokio::time::sleep(Duration::from_millis(200)).await;
        });
        let client = TransitionClient::new_with_timeouts(
            &endpoint,
            timeout_test_options(),
            "",
            TransitionClientTimeouts::new(Duration::from_secs(1), Duration::from_secs(1), Duration::from_millis(50)),
        )
        .await
        .expect("fixture client should build");
        client
            .bucket_loc_cache
            .lock()
            .expect("location cache should lock")
            .set("bucket", "us-east-1");

        let err = client
            .list_objects_v2_query("bucket", "", "", false, false, "", "", 1, HeaderMap::new())
            .await
            .expect_err("a stalled ListObjectsV2 body must be bounded");

        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
        fixture.await.expect("fixture should join");
    }
}
