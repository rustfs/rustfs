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

use http::{HeaderMap, StatusCode};
use serde::{Deserialize, Serialize};
use serde::{de::Deserializer, ser::Serializer};
use std::fmt::Display;

use s3s::S3ErrorCode;

const _REPORT_ISSUE: &str = "Please report this issue at https://github.com/rustfs/rustfs/issues.";

#[derive(Serialize, Deserialize, Debug, Clone, thiserror::Error, PartialEq, Eq)]
#[serde(default, rename_all = "PascalCase")]
pub struct ErrorResponse {
    #[serde(serialize_with = "serialize_code", deserialize_with = "deserialize_code")]
    pub code: S3ErrorCode,
    pub message: String,
    pub bucket_name: String,
    pub key: String,
    pub resource: String,
    pub request_id: String,
    pub host_id: String,
    pub region: String,
    pub server: String,
    #[serde(skip)]
    pub status_code: StatusCode,
}

fn serialize_code<S>(_data: &S3ErrorCode, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_str("")
}

fn deserialize_code<'de, D>(d: D) -> Result<S3ErrorCode, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(S3ErrorCode::from_bytes(String::deserialize(d)?.as_bytes()).unwrap_or(S3ErrorCode::Custom("".into())))
}

impl Default for ErrorResponse {
    fn default() -> Self {
        ErrorResponse {
            code: S3ErrorCode::Custom("".into()),
            message: Default::default(),
            bucket_name: Default::default(),
            key: Default::default(),
            resource: Default::default(),
            request_id: Default::default(),
            host_id: Default::default(),
            region: Default::default(),
            server: Default::default(),
            status_code: Default::default(),
        }
    }
}

impl Display for ErrorResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

pub fn to_error_response(err: &std::io::Error) -> ErrorResponse {
    if let Some(err) = err.get_ref() {
        if err.is::<ErrorResponse>() {
            err.downcast_ref::<ErrorResponse>().expect("err!").clone()
        } else {
            ErrorResponse::default()
        }
    } else {
        ErrorResponse::default()
    }
}

pub fn http_resp_to_error_response(
    resp_status: StatusCode,
    h: &HeaderMap,
    b: Vec<u8>,
    bucket_name: &str,
    object_name: &str,
) -> ErrorResponse {
    let err_body = String::from_utf8(b).unwrap();
    if h.is_empty() || resp_status.is_client_error() || resp_status.is_server_error() {
        return ErrorResponse {
            status_code: resp_status,
            code: S3ErrorCode::ResponseInterrupted,
            message: "Invalid HTTP response.".to_string(),
            bucket_name: bucket_name.to_string(),
            key: object_name.to_string(),
            ..Default::default()
        };
    }
    let err_resp_ = quick_xml::de::from_str::<ErrorResponse>(&err_body);
    let mut err_resp = ErrorResponse::default();
    if err_resp_.is_err() {
        match resp_status {
            StatusCode::NOT_FOUND => {
                if object_name == "" {
                    err_resp = ErrorResponse {
                        status_code: resp_status,
                        code: S3ErrorCode::NoSuchBucket,
                        message: "The specified bucket does not exist.".to_string(),
                        bucket_name: bucket_name.to_string(),
                        ..Default::default()
                    };
                } else {
                    err_resp = ErrorResponse {
                        status_code: resp_status,
                        code: S3ErrorCode::NoSuchKey,
                        message: "The specified key does not exist.".to_string(),
                        bucket_name: bucket_name.to_string(),
                        key: object_name.to_string(),
                        ..Default::default()
                    };
                }
            }
            StatusCode::FORBIDDEN => {
                err_resp = ErrorResponse {
                    status_code: resp_status,
                    code: S3ErrorCode::AccessDenied,
                    message: "Access Denied.".to_string(),
                    bucket_name: bucket_name.to_string(),
                    key: object_name.to_string(),
                    ..Default::default()
                };
            }
            StatusCode::CONFLICT => {
                err_resp = ErrorResponse {
                    status_code: resp_status,
                    code: S3ErrorCode::BucketNotEmpty,
                    message: "Bucket not empty.".to_string(),
                    bucket_name: bucket_name.to_string(),
                    ..Default::default()
                };
            }
            StatusCode::PRECONDITION_FAILED => {
                err_resp = ErrorResponse {
                    status_code: resp_status,
                    code: S3ErrorCode::PreconditionFailed,
                    message: "Pre condition failed.".to_string(),
                    bucket_name: bucket_name.to_string(),
                    key: object_name.to_string(),
                    ..Default::default()
                };
            }
            _ => {
                let mut msg = resp_status.to_string();
                if err_body.len() > 0 {
                    msg = err_body;
                }
                err_resp = ErrorResponse {
                    status_code: resp_status,
                    code: S3ErrorCode::Custom(resp_status.to_string().into()),
                    message: msg,
                    bucket_name: bucket_name.to_string(),
                    ..Default::default()
                };
            }
        }
    } else {
        err_resp = err_resp_.unwrap();
    }
    err_resp.status_code = resp_status;
    if let Some(server_name) = h.get("Server") {
        err_resp.server = server_name.to_str().expect("err").to_string();
    }

    let code = h.get("x-minio-error-code");
    if code.is_some() {
        err_resp.code = S3ErrorCode::Custom(code.expect("err").to_str().expect("err").into());
    }
    let desc = h.get("x-minio-error-desc");
    if desc.is_some() {
        err_resp.message = desc.expect("err").to_str().expect("err").trim_matches('"').to_string();
    }

    if err_resp.request_id == "" {
        if let Some(x_amz_request_id) = h.get("x-amz-request-id") {
            err_resp.request_id = x_amz_request_id.to_str().expect("err").to_string();
        }
    }
    if err_resp.host_id == "" {
        if let Some(x_amz_id_2) = h.get("x-amz-id-2") {
            err_resp.host_id = x_amz_id_2.to_str().expect("err").to_string();
        }
    }
    if err_resp.region == "" {
        if let Some(x_amz_bucket_region) = h.get("x-amz-bucket-region") {
            err_resp.region = x_amz_bucket_region.to_str().expect("err").to_string();
        }
    }
    if err_resp.code == S3ErrorCode::InvalidLocationConstraint/*InvalidRegion*/ && err_resp.region != "" {
        err_resp.message = format!("Region does not match, expecting region ‘{}’.", err_resp.region);
    }

    err_resp
}

pub fn err_transfer_acceleration_bucket(bucket_name: &str) -> ErrorResponse {
    ErrorResponse {
        status_code: StatusCode::BAD_REQUEST,
        code: S3ErrorCode::InvalidArgument,
        message: "The name of the bucket used for Transfer Acceleration must be DNS-compliant and must not contain periods ‘.’."
            .to_string(),
        bucket_name: bucket_name.to_string(),
        ..Default::default()
    }
}

pub fn err_entity_too_large(total_size: i64, max_object_size: i64, bucket_name: &str, object_name: &str) -> ErrorResponse {
    let msg = format!(
        "Your proposed upload size ‘{}’ exceeds the maximum allowed object size ‘{}’ for single PUT operation.",
        total_size, max_object_size
    );
    ErrorResponse {
        status_code: StatusCode::BAD_REQUEST,
        code: S3ErrorCode::EntityTooLarge,
        message: msg,
        bucket_name: bucket_name.to_string(),
        key: object_name.to_string(),
        ..Default::default()
    }
}

pub fn err_entity_too_small(total_size: i64, bucket_name: &str, object_name: &str) -> ErrorResponse {
    let msg = format!(
        "Your proposed upload size ‘{}’ is below the minimum allowed object size ‘0B’ for single PUT operation.",
        total_size
    );
    ErrorResponse {
        status_code: StatusCode::BAD_REQUEST,
        code: S3ErrorCode::EntityTooSmall,
        message: msg,
        bucket_name: bucket_name.to_string(),
        key: object_name.to_string(),
        ..Default::default()
    }
}

pub fn err_unexpected_eof(total_read: i64, total_size: i64, bucket_name: &str, object_name: &str) -> ErrorResponse {
    let msg = format!(
        "Data read ‘{}’ is not equal to the size ‘{}’ of the input Reader.",
        total_read, total_size
    );
    ErrorResponse {
        status_code: StatusCode::BAD_REQUEST,
        code: S3ErrorCode::Custom("UnexpectedEOF".into()),
        message: msg,
        bucket_name: bucket_name.to_string(),
        key: object_name.to_string(),
        ..Default::default()
    }
}

pub fn err_invalid_argument(message: &str) -> ErrorResponse {
    ErrorResponse {
        status_code: StatusCode::BAD_REQUEST,
        code: S3ErrorCode::InvalidArgument,
        message: message.to_string(),
        request_id: "rustfs".to_string(),
        ..Default::default()
    }
}

pub fn err_api_not_supported(message: &str) -> ErrorResponse {
    ErrorResponse {
        status_code: StatusCode::NOT_IMPLEMENTED,
        code: S3ErrorCode::Custom("APINotSupported".into()),
        message: message.to_string(),
        request_id: "rustfs".to_string(),
        ..Default::default()
    }
}
