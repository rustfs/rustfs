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

use crate::client::{
    api_error_response::http_resp_to_error_response,
    api_s3_datatypes::{
        ListBucketResult, ListBucketV2Result, ListMultipartUploadsResult, ListObjectPartsResult, ListVersionsResult, ObjectPart,
    },
    credentials,
    transition_api::{ReaderImpl, RequestMetadata, TransitionClient, collect_response_body},
};
use crate::storage_api_contracts::bucket::BucketInfo;
use http::{HeaderMap, StatusCode};
use http_body_util::BodyExt;
use hyper::body::Body;
use hyper::body::Bytes;
use rustfs_config::MAX_S3_CLIENT_RESPONSE_SIZE;
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
                    add_crc: Default::default(),
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
        let mut body_vec = Vec::new();
        let mut body = resp.into_body();
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
            if let Some(data) = frame.data_ref() {
                body_vec.extend_from_slice(data);
            }
        }
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
                    add_crc: Default::default(),
                    extra_pre_sign_header: Default::default(),
                    bucket_location: Default::default(),
                    expires: Default::default(),
                },
            )
            .await?;

        let resp_status = resp.status();
        let headers = resp.headers().clone();
        let body = collect_response_body(resp.into_body(), MAX_S3_CLIENT_RESPONSE_SIZE).await?;
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
#[allow(dead_code)]
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
}
