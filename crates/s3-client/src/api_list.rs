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
        "url" => urlencoding::decode(name)
            .map(|decoded| decoded.into_owned())
            .map_err(|err| std::io::Error::new(ErrorKind::InvalidData, err)),
        _ => Ok(name.to_string()),
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
    fn url_encoded_s3_names_are_decoded() {
        for (encoded, expected) in [
            ("test_file%283%29.png", "test_file(3).png"),
            ("dir%20a/file.txt", "dir a/file.txt"),
            ("%E4%B8%AD%E6%96%87.txt", "中文.txt"),
            ("%F0%9F%98%80.txt", "😀.txt"),
            ("file%2bname+%25.txt", "file+name+%.txt"),
            ("literal%252F.txt", "literal%2F.txt"),
            ("dir%2Ffile%00%01.txt", "dir/file\0\u{1}.txt"),
            ("", ""),
        ] {
            assert_eq!(decode_s3_name(encoded, "url").expect("valid URL-encoded S3 key"), expected);
        }
    }

    #[test]
    fn s3_names_without_url_encoding_are_preserved() {
        for encoding in ["", "unknown"] {
            let name = "中文+%20%FF.txt";
            assert_eq!(decode_s3_name(name, encoding).expect("unencoded key should pass through"), name);
        }
    }

    #[test]
    fn url_encoded_s3_names_reject_invalid_utf8() {
        for name in ["%FF", "%C3%28", "%E4%B8", "%ED%A0%80", "%C0%AF"] {
            let err = decode_s3_name(name, "url").expect_err("invalid UTF-8 must not become a different object key");
            assert_eq!(err.kind(), ErrorKind::InvalidData);
        }
    }

    #[test]
    fn url_encoded_s3_names_preserve_malformed_percent_escapes() {
        // Match urlencoding's permissive handling without treating '+' as a space.
        for name in ["file%", "file%2", "file%GG+name"] {
            assert_eq!(decode_s3_name(name, "url").expect("literal malformed escape"), name);
        }
    }

    async fn list_response_fixture(responses: Vec<&'static str>) -> (TransitionClient, tokio::task::JoinHandle<Vec<url::Url>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("list fixture should bind");
        let endpoint = listener.local_addr().expect("list fixture address").to_string();
        let fixture = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_secs(5), async move {
                let mut requests = Vec::new();
                for body in responses {
                    let (mut stream, _) = listener.accept().await.expect("list fixture should accept request");
                    let mut request = Vec::new();
                    let mut buffer = [0; 1024];
                    while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                        let read = stream.read(&mut buffer).await.expect("list fixture should read request");
                        assert_ne!(read, 0, "request headers must be complete");
                        request.extend_from_slice(&buffer[..read]);
                        assert!(request.len() <= 16 * 1024, "fixture request headers should be bounded");
                    }
                    let request = std::str::from_utf8(&request).expect("request headers should be UTF-8");
                    let target = request.split_whitespace().nth(1).expect("request should have a target");
                    requests.push(url::Url::parse(&format!("http://localhost{target}")).expect("request URL should parse"));
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    stream.write_all(response.as_bytes()).await.expect("list fixture should send response");
                }
                requests
            })
            .await
            .expect("list fixture should finish within its deadline")
        });
        let client = TransitionClient::new_with_timeouts(
            &endpoint,
            timeout_test_options(),
            "",
            TransitionClientTimeouts::new(Duration::from_secs(1), Duration::from_secs(1), Duration::from_secs(1)),
        )
        .await
        .expect("list fixture client should build");
        client
            .bucket_loc_cache
            .lock()
            .expect("location cache should lock")
            .set("bucket", "us-east-1");
        (client, fixture)
    }

    #[tokio::test]
    async fn list_objects_v2_decodes_prefixes_and_preserves_continuation_tokens() {
        let (client, fixture) = list_response_fixture(vec![
            r#"<ListBucketResult><EncodingType>url</EncodingType><IsTruncated>true</IsTruncated>
                <CommonPrefixes><Prefix>dir%20a/</Prefix></CommonPrefixes>
                <NextContinuationToken>opaque%2F+token</NextContinuationToken></ListBucketResult>"#,
            "<ListBucketResult><EncodingType>url</EncodingType><IsTruncated>false</IsTruncated></ListBucketResult>",
        ])
        .await;
        let first = client
            .list_objects_v2_query("bucket", "", "", false, false, "/", "", 1, HeaderMap::new())
            .await
            .expect("first object page should decode");
        assert_eq!(first.common_prefixes[0].prefix, "dir a/");
        assert_eq!(first.next_continuation_token, "opaque%2F+token");
        let second = client
            .list_objects_v2_query("bucket", "", &first.next_continuation_token, false, false, "/", "", 1, HeaderMap::new())
            .await
            .expect("second object page should succeed");
        assert!(!second.is_truncated);
        let requests = fixture.await.expect("object list fixture should join");
        let query: HashMap<_, _> = requests[1].query_pairs().into_owned().collect();
        assert_eq!(query.get("continuation-token").map(String::as_str), Some("opaque%2F+token"));
        assert_eq!(query.get("encoding-type").map(String::as_str), Some("url"));
    }

    #[tokio::test]
    async fn list_versions_decodes_names_and_round_trips_pagination_markers() {
        let (client, fixture) = list_response_fixture(vec![
            r#"<ListVersionsResult><EncodingType>url</EncodingType><IsTruncated>true</IsTruncated>
                <Version><Key>dir%20a/%E4%B8%AD%E6%96%87%2B%252F.txt</Key><VersionId>version%2F+id</VersionId></Version>
                <DeleteMarker><Key>deleted%20file%2B.txt</Key><VersionId>delete%2F+id</VersionId></DeleteMarker>
                <CommonPrefixes><Prefix>dir%20a/</Prefix></CommonPrefixes>
                <NextKeyMarker>dir%20a/%E4%B8%AD%E6%96%87%2B%252F.txt</NextKeyMarker>
                <NextVersionIdMarker>version%2F+id</NextVersionIdMarker></ListVersionsResult>"#,
            "<ListVersionsResult><EncodingType>url</EncodingType><IsTruncated>false</IsTruncated></ListVersionsResult>",
        ])
        .await;
        let mut opts = ListObjectsOptions::default();
        opts.set("prefix", "dir a/");
        let first = client
            .list_object_versions_query("bucket", &opts, "", "", "/")
            .await
            .expect("first version page");
        assert_eq!(first.versions[0].key, "dir a/中文+%2F.txt");
        assert_eq!(first.versions[0].version_id, "version%2F+id");
        assert_eq!(first.delete_markers[0].key, "deleted file+.txt");
        assert_eq!(first.delete_markers[0].version_id, "delete%2F+id");
        assert_eq!(first.common_prefixes[0].prefix, "dir a/");
        assert_eq!(first.next_key_marker, "dir a/中文+%2F.txt");
        assert_eq!(first.next_version_id_marker, "version%2F+id");
        let second = client
            .list_object_versions_query("bucket", &opts, &first.next_key_marker, &first.next_version_id_marker, "/")
            .await
            .expect("second version page");
        assert!(!second.is_truncated);
        let requests = fixture.await.expect("version list fixture should join");
        let query: HashMap<_, _> = requests[1].query_pairs().into_owned().collect();
        assert_eq!(query.get("key-marker").map(String::as_str), Some("dir a/中文+%2F.txt"));
        assert_eq!(query.get("version-id-marker").map(String::as_str), Some("version%2F+id"));
        assert_eq!(query.get("prefix").map(String::as_str), Some("dir a/"));
        assert_eq!(query.get("encoding-type").map(String::as_str), Some("url"));
    }

    #[tokio::test]
    async fn list_versions_rejects_invalid_utf8_keys() {
        let (client, fixture) = list_response_fixture(vec![
            "<ListVersionsResult><EncodingType>url</EncodingType><Version><Key>%FF</Key></Version></ListVersionsResult>",
        ])
        .await;
        let err = client
            .list_object_versions_query("bucket", &ListObjectsOptions::default(), "", "", "")
            .await
            .expect_err("invalid UTF-8 in a remote key should fail the listing");
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert_eq!(fixture.await.expect("invalid key fixture should join").len(), 1);
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
