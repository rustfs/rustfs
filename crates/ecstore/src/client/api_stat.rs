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

use http::{HeaderMap, HeaderValue, StatusCode};
use http_body_util::BodyExt;
use hyper::body::Body;
use hyper::body::Bytes;
use rustfs_utils::EMPTY_STRING_SHA256_HASH;
use std::{collections::HashMap, str::FromStr};
use tokio::io::BufReader;
use tracing::warn;
use uuid::Uuid;

use crate::client::{
    api_error_response::{ErrorResponse, err_invalid_argument, http_resp_to_error_response},
    api_get_options::GetObjectOptions,
    transition_api::{
        ObjectInfo, ReadCloser, ReaderImpl, RequestMetadata, TransitionClient, collect_response_body, to_object_info_for_provider,
    },
};
use s3s::{
    dto::{BucketVersioningStatus, MFADelete, VersioningConfiguration},
    header::{X_AMZ_DELETE_MARKER, X_AMZ_VERSION_ID},
};

const S3_XML_NAMESPACE: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictVersioningConfiguration {
    #[serde(rename = "@xmlns")]
    namespace: Option<String>,
    #[serde(rename = "MfaDelete")]
    mfa_delete: Option<String>,
    #[serde(rename = "Status")]
    status: Option<String>,
}

#[derive(serde::Deserialize)]
enum StrictVersioningResponse {
    VersioningConfiguration(StrictVersioningConfiguration),
}

fn parse_bucket_versioning_response(
    status: StatusCode,
    headers: &HeaderMap,
    body: Vec<u8>,
    bucket_name: &str,
) -> Result<VersioningConfiguration, std::io::Error> {
    if status != StatusCode::OK {
        return Err(std::io::Error::other(http_resp_to_error_response(status, headers, body, bucket_name, "")));
    }

    let StrictVersioningResponse::VersioningConfiguration(parsed) =
        quick_xml::de::from_reader(body.as_slice()).map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    if parsed
        .namespace
        .as_deref()
        .is_some_and(|namespace| namespace != S3_XML_NAMESPACE)
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "unexpected GetBucketVersioning XML namespace",
        ));
    }
    Ok(VersioningConfiguration {
        mfa_delete: parsed.mfa_delete.map(MFADelete::from),
        status: parsed.status.map(BucketVersioningStatus::from),
        ..Default::default()
    })
}

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
            if resp.status() != http::StatusCode::OK {
                return Ok(false);
            }

            let resp_status = resp.status();
            let h = resp.headers().clone();

            let mut body_vec = Vec::new();
            let mut body = resp.into_body();
            while let Some(frame) = body.frame().await {
                let frame = frame.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
                if let Some(data) = frame.data_ref() {
                    body_vec.extend_from_slice(data);
                }
            }
            let resperr = http_resp_to_error_response(resp_status, &h, body_vec, bucket_name, "");

            warn!("bucket exists, resperr: {:?}", resperr);
            /*if to_error_response(resperr).code == "NoSuchBucket" {
                return Ok(false);
            }
            if resp.status_code() != http::StatusCode::OK {
                return Ok(false);
            }*/
        }
        Ok(true)
    }

    pub async fn get_bucket_versioning(&self, bucket_name: &str) -> Result<VersioningConfiguration, std::io::Error> {
        let mut query_values = HashMap::new();
        query_values.insert("versioning".to_string(), "".to_string());
        let resp = self
            .execute_method(
                http::Method::GET,
                &mut RequestMetadata {
                    bucket_name: bucket_name.to_string(),
                    object_name: "".to_string(),
                    query_values,
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

        match resp {
            Ok(resp) => {
                let resp_status = resp.status();
                let h = resp.headers().clone();

                let body_vec = collect_response_body(resp.into_body(), rustfs_config::MAX_S3_CLIENT_RESPONSE_SIZE).await?;
                parse_bucket_versioning_response(resp_status, &h, body_vec, bucket_name)
            }

            Err(err) => Err(std::io::Error::other(err)),
        }
    }

    pub async fn stat_object(
        &self,
        bucket_name: &str,
        object_name: &str,
        opts: &GetObjectOptions,
    ) -> Result<ObjectInfo, std::io::Error> {
        let mut headers = opts.header();
        if opts.internal.replication_delete_marker {
            headers.insert("X-Source-DeleteMarker", HeaderValue::from_str("true").expect("operation should succeed"));
        }
        if opts.internal.is_replication_ready_for_delete_marker {
            headers.insert(
                "X-Check-Replication-Ready",
                HeaderValue::from_str("true").expect("operation should succeed"),
            );
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
                    x_amz_delete_marker.to_str().expect("operation should succeed") == "true"
                } else {
                    false
                };
                let replication_ready = if let Some(x_amz_delete_marker) = h.get("X-Replication-Ready") {
                    x_amz_delete_marker.to_str().expect("operation should succeed") == "true"
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
                            version_id: self
                                .raw_version_id(h)?
                                .and_then(|s| Uuid::from_str(s).ok())
                                .filter(|v| !v.is_nil()),
                            is_delete_marker: delete_marker,
                            ..Default::default()
                        });
                        //err_resp
                    }
                    return Ok(ObjectInfo {
                        version_id: self
                            .raw_version_id(h)?
                            .and_then(|s| Uuid::from_str(s).ok())
                            .filter(|v| !v.is_nil()),
                        is_delete_marker: delete_marker,
                        replication_ready: replication_ready,
                        ..Default::default()
                    });
                    //http_resp_to_error_response(resp, bucket_name, object_name)
                }

                to_object_info_for_provider(bucket_name, object_name, h, self.provider_version_capabilities())
            }
            Err(err) => {
                return Err(std::io::Error::other(err));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::parse_bucket_versioning_response;
    use http::{HeaderMap, StatusCode};
    use s3s::dto::BucketVersioningStatus;

    #[test]
    fn parses_bucket_versioning_statuses_mfa_delete_and_unversioned_state() {
        for (xml, expected) in [
            (
                br#"<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>"#
                    .as_slice(),
                Some(BucketVersioningStatus::ENABLED),
            ),
            (
                br#"<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Suspended</Status></VersioningConfiguration>"#
                    .as_slice(),
                Some(BucketVersioningStatus::SUSPENDED),
            ),
            (
                br#"<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>"#.as_slice(),
                None,
            ),
        ] {
            let config = parse_bucket_versioning_response(StatusCode::OK, &HeaderMap::new(), xml.to_vec(), "tier-bucket")
                .expect("valid GetBucketVersioning response should parse");
            assert_eq!(config.status.as_ref().map(|status| status.as_str()), expected);
        }

        let config = parse_bucket_versioning_response(
            StatusCode::OK,
            &HeaderMap::new(),
            br#"<VersioningConfiguration><MfaDelete>Enabled</MfaDelete></VersioningConfiguration>"#.to_vec(),
            "tier-bucket",
        )
        .expect("valid MfaDelete should parse");
        assert_eq!(config.mfa_delete.as_ref().map(|status| status.as_str()), Some("Enabled"));
    }

    #[test]
    fn rejects_non_ok_and_malformed_bucket_versioning_responses() {
        let valid = br#"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"#.to_vec();
        for status in [StatusCode::NO_CONTENT, StatusCode::PARTIAL_CONTENT] {
            let status_err = parse_bucket_versioning_response(status, &HeaderMap::new(), valid.clone(), "tier-bucket")
                .expect_err("GetBucketVersioning must return exactly HTTP 200");
            assert_eq!(status_err.kind(), std::io::ErrorKind::Other);
        }

        let parse_err = parse_bucket_versioning_response(
            StatusCode::OK,
            &HeaderMap::new(),
            b"<VersioningConfiguration>".to_vec(),
            "tier-bucket",
        )
        .expect_err("malformed GetBucketVersioning XML must fail closed");
        assert_eq!(parse_err.kind(), std::io::ErrorKind::InvalidData);

        for xml in [
            b"<VersioningConfiguration><Statuz>Enabled</Statuz></VersioningConfiguration>".as_slice(),
            b"<WrongRoot><Status>Enabled</Status></WrongRoot>".as_slice(),
            b"<VersioningConfiguration xmlns=\"https://example.invalid\"/>".as_slice(),
        ] {
            let strict_err = parse_bucket_versioning_response(StatusCode::OK, &HeaderMap::new(), xml.to_vec(), "tier-bucket")
                .expect_err("unknown GetBucketVersioning XML must fail closed");
            assert_eq!(strict_err.kind(), std::io::ErrorKind::InvalidData);
        }
    }
}
