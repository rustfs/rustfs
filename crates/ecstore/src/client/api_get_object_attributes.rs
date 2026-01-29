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

use http::{HeaderMap, HeaderValue};
use std::collections::HashMap;
use time::OffsetDateTime;

use crate::client::constants::{GET_OBJECT_ATTRIBUTES_MAX_PARTS, GET_OBJECT_ATTRIBUTES_TAGS, ISO8601_DATEFORMAT};
use crate::client::{
    api_get_object_acl::AccessControlPolicy,
    transition_api::{ReaderImpl, RequestMetadata, TransitionClient},
};
use http_body_util::BodyExt;
use hyper::body::Body;
use hyper::body::Bytes;
use hyper::body::Incoming;
use rustfs_config::MAX_S3_CLIENT_RESPONSE_SIZE;
use rustfs_utils::EMPTY_STRING_SHA256_HASH;
use s3s::header::{X_AMZ_MAX_PARTS, X_AMZ_OBJECT_ATTRIBUTES, X_AMZ_PART_NUMBER_MARKER, X_AMZ_VERSION_ID};

pub struct ObjectAttributesOptions {
    pub max_parts: i64,
    pub version_id: String,
    pub part_number_marker: i64,
    //server_side_encryption: encrypt::ServerSide,
}

pub struct ObjectAttributes {
    pub version_id: String,
    pub last_modified: OffsetDateTime,
    pub object_attributes_response: ObjectAttributesResponse,
}

impl ObjectAttributes {
    fn new() -> Self {
        Self {
            version_id: "".to_string(),
            last_modified: OffsetDateTime::now_utc(),
            object_attributes_response: ObjectAttributesResponse::new(),
        }
    }
}

#[derive(Debug, Default, serde::Deserialize)]
pub struct Checksum {
    checksum_crc32: String,
    checksum_crc32c: String,
    checksum_sha1: String,
    checksum_sha256: String,
}

impl Checksum {
    fn new() -> Self {
        Self {
            checksum_crc32: "".to_string(),
            checksum_crc32c: "".to_string(),
            checksum_sha1: "".to_string(),
            checksum_sha256: "".to_string(),
        }
    }
}

#[derive(Debug, Default, serde::Deserialize)]
pub struct ObjectParts {
    pub parts_count: i64,
    pub part_number_marker: i64,
    pub next_part_number_marker: i64,
    pub max_parts: i64,
    is_truncated: bool,
    parts: Vec<ObjectAttributePart>,
}

impl ObjectParts {
    fn new() -> Self {
        Self {
            parts_count: 0,
            part_number_marker: 0,
            next_part_number_marker: 0,
            max_parts: 0,
            is_truncated: false,
            parts: Vec::new(),
        }
    }
}

#[derive(Debug, Default, serde::Deserialize)]
pub struct ObjectAttributesResponse {
    pub etag: String,
    pub storage_class: String,
    pub object_size: i64,
    pub checksum: Checksum,
    pub object_parts: ObjectParts,
}

impl ObjectAttributesResponse {
    fn new() -> Self {
        Self {
            etag: "".to_string(),
            storage_class: "".to_string(),
            object_size: 0,
            checksum: Checksum::new(),
            object_parts: ObjectParts::new(),
        }
    }
}

#[derive(Debug, Default, serde::Deserialize)]
struct ObjectAttributePart {
    checksum_crc32: String,
    checksum_crc32c: String,
    checksum_sha1: String,
    checksum_sha256: String,
    part_number: i64,
    size: i64,
}

impl ObjectAttributes {
    pub async fn parse_response(&mut self, h: &HeaderMap, body_vec: Vec<u8>) -> Result<(), std::io::Error> {
        let mod_time = OffsetDateTime::parse(h.get("Last-Modified").unwrap().to_str().unwrap(), ISO8601_DATEFORMAT).unwrap(); //RFC7231Time
        self.last_modified = mod_time;
        self.version_id = h.get(X_AMZ_VERSION_ID).unwrap().to_str().unwrap().to_string();

        let mut response = match quick_xml::de::from_str::<ObjectAttributesResponse>(&String::from_utf8(body_vec).unwrap()) {
            Ok(result) => result,
            Err(err) => {
                return Err(std::io::Error::other(err.to_string()));
            }
        };
        self.object_attributes_response = response;

        Ok(())
    }
}

impl TransitionClient {
    pub async fn get_object_attributes(
        &self,
        bucket_name: &str,
        object_name: &str,
        opts: ObjectAttributesOptions,
    ) -> Result<ObjectAttributes, std::io::Error> {
        let mut url_values = HashMap::new();
        url_values.insert("attributes".to_string(), "".to_string());
        if opts.version_id != "" {
            url_values.insert("versionId".to_string(), opts.version_id);
        }

        let mut headers = HeaderMap::new();
        headers.insert(X_AMZ_OBJECT_ATTRIBUTES, HeaderValue::from_str(GET_OBJECT_ATTRIBUTES_TAGS).unwrap());

        if opts.part_number_marker > 0 {
            headers.insert(
                X_AMZ_PART_NUMBER_MARKER,
                HeaderValue::from_str(&opts.part_number_marker.to_string()).unwrap(),
            );
        }

        if opts.max_parts > 0 {
            headers.insert(X_AMZ_MAX_PARTS, HeaderValue::from_str(&opts.max_parts.to_string()).unwrap());
        } else {
            headers.insert(
                X_AMZ_MAX_PARTS,
                HeaderValue::from_str(&GET_OBJECT_ATTRIBUTES_MAX_PARTS.to_string()).unwrap(),
            );
        }

        /*if opts.server_side_encryption.is_some() {
            opts.server_side_encryption.Marshal(headers);
        }*/

        let mut resp = self
            .execute_method(
                http::Method::HEAD,
                &mut RequestMetadata {
                    bucket_name: bucket_name.to_string(),
                    object_name: object_name.to_string(),
                    query_values: url_values,
                    custom_header: headers,
                    content_sha256_hex: EMPTY_STRING_SHA256_HASH.to_string(),
                    content_md5_base64: "".to_string(),
                    content_body: ReaderImpl::Body(Bytes::new()),
                    content_length: 0,
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
        let has_etag = h.get("ETag").unwrap().to_str().unwrap();
        if !has_etag.is_empty() {
            return Err(std::io::Error::other(
                "get_object_attributes is not supported by the current endpoint version",
            ));
        }

        let mut body_vec = Vec::new();
        let mut body = resp.into_body();
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
            if let Some(data) = frame.data_ref() {
                body_vec.extend_from_slice(data);
            }
        }

        if resp_status != http::StatusCode::OK {
            let err_body = String::from_utf8(body_vec).unwrap();
            let mut er = match quick_xml::de::from_str::<AccessControlPolicy>(&err_body) {
                Ok(result) => result,
                Err(err) => {
                    return Err(std::io::Error::other(err.to_string()));
                }
            };

            return Err(std::io::Error::other(er.access_control_list.permission));
        }

        let mut oa = ObjectAttributes::new();
        oa.parse_response(&h, body_vec).await?;

        Ok(oa)
    }
}
