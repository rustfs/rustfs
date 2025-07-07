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

use bytes::Bytes;
use http::{HeaderMap, HeaderValue};
use rustfs_utils::EMPTY_STRING_SHA256_HASH;
use std::{collections::HashMap, str::FromStr};
use tokio::io::BufReader;
use uuid::Uuid;

use crate::client::{
    api_error_response::{ErrorResponse, err_invalid_argument, http_resp_to_error_response},
    api_get_options::GetObjectOptions,
    transition_api::{ObjectInfo, ReadCloser, ReaderImpl, RequestMetadata, TransitionClient, to_object_info},
};
use s3s::header::{X_AMZ_DELETE_MARKER, X_AMZ_VERSION_ID};

impl TransitionClient {
    pub async fn bucket_exists(&self, bucket_name: &str) -> Result<bool, std::io::Error> {
        let resp = self
            .execute_method(
                http::Method::HEAD,
                &mut RequestMetadata {
                    bucket_name: bucket_name.to_string(),
                    object_name: "".to_string(),
                    query_values: HashMap::new(),
                    custom_header: HeaderMap::new(),
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
            .await;

        if let Ok(resp) = resp {
            let b = resp.body().bytes().expect("err").to_vec();
            let resperr = http_resp_to_error_response(resp, b, bucket_name, "");
            /*if to_error_response(resperr).code == "NoSuchBucket" {
                return Ok(false);
            }
            if resp.status_code() != http::StatusCode::OK {
                return Ok(false);
            }*/
        }
        Ok(true)
    }

    pub async fn stat_object(
        &self,
        bucket_name: &str,
        object_name: &str,
        opts: &GetObjectOptions,
    ) -> Result<ObjectInfo, std::io::Error> {
        let mut headers = opts.header();
        if opts.internal.replication_delete_marker {
            headers.insert("X-Source-DeleteMarker", HeaderValue::from_str("true").unwrap());
        }
        if opts.internal.is_replication_ready_for_delete_marker {
            headers.insert("X-Check-Replication-Ready", HeaderValue::from_str("true").unwrap());
        }

        let resp = self
            .execute_method(
                http::Method::HEAD,
                &mut RequestMetadata {
                    bucket_name: bucket_name.to_string(),
                    object_name: object_name.to_string(),
                    query_values: opts.to_query_values(),
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
            .await;

        match resp {
            Ok(resp) => {
                let h = resp.headers();
                let delete_marker = if let Some(x_amz_delete_marker) = h.get(X_AMZ_DELETE_MARKER.as_str()) {
                    x_amz_delete_marker.to_str().unwrap() == "true"
                } else {
                    false
                };
                let replication_ready = if let Some(x_amz_delete_marker) = h.get("X-Replication-Ready") {
                    x_amz_delete_marker.to_str().unwrap() == "true"
                } else {
                    false
                };
                if resp.status() != http::StatusCode::OK && resp.status() != http::StatusCode::PARTIAL_CONTENT {
                    if resp.status() == http::StatusCode::METHOD_NOT_ALLOWED && opts.version_id != "" && delete_marker {
                        let err_resp = ErrorResponse {
                            status_code: resp.status(),
                            code: s3s::S3ErrorCode::MethodNotAllowed,
                            message: "the specified method is not allowed against this resource.".to_string(),
                            bucket_name: bucket_name.to_string(),
                            key: object_name.to_string(),
                            ..Default::default()
                        };
                        return Ok(ObjectInfo {
                            version_id: match Uuid::from_str(h.get(X_AMZ_VERSION_ID).unwrap().to_str().unwrap()) {
                                Ok(v) => v,
                                Err(e) => {
                                    return Err(std::io::Error::other(e));
                                }
                            },
                            is_delete_marker: delete_marker,
                            ..Default::default()
                        });
                        //err_resp
                    }
                    return Ok(ObjectInfo {
                        version_id: match Uuid::from_str(h.get(X_AMZ_VERSION_ID).unwrap().to_str().unwrap()) {
                            Ok(v) => v,
                            Err(e) => {
                                return Err(std::io::Error::other(e));
                            }
                        },
                        is_delete_marker: delete_marker,
                        replication_ready: replication_ready,
                        ..Default::default()
                    });
                    //http_resp_to_error_response(resp, bucket_name, object_name)
                }

                Ok(to_object_info(bucket_name, object_name, h).unwrap())
            }
            Err(err) => {
                return Err(std::io::Error::other(err));
            }
        }
    }
}
