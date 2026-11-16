// Copyright 2026 RustFS Team
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

use super::dto::ValidationExceptionField;
use http::{HeaderValue, Response, StatusCode};
use serde::Serialize;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ErrorCode {
    AccessDenied,
    Conflict,
    InternalServer,
    KmsDisabled,
    KmsInvalidKeyUsage,
    KmsInvalidState,
    KmsNotFound,
    NotFound,
    RequestTimeout,
    ServiceQuotaExceeded,
    ServiceUnavailable,
    TooManyRequests,
    Validation,
}

impl ErrorCode {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::AccessDenied => "AccessDeniedException",
            Self::Conflict => "ConflictException",
            Self::InternalServer => "InternalServerException",
            Self::KmsDisabled => "KmsDisabledException",
            Self::KmsInvalidKeyUsage => "KmsInvalidKeyUsageException",
            Self::KmsInvalidState => "KmsInvalidStateException",
            Self::KmsNotFound => "KmsNotFoundException",
            Self::NotFound => "NotFoundException",
            Self::RequestTimeout => "RequestTimeoutException",
            Self::ServiceQuotaExceeded => "ServiceQuotaExceededException",
            Self::ServiceUnavailable => "ServiceUnavailableException",
            Self::TooManyRequests => "TooManyRequestsException",
            Self::Validation => "ValidationException",
        }
    }

    pub(crate) fn status_code(self) -> StatusCode {
        match self {
            Self::AccessDenied => StatusCode::FORBIDDEN,
            Self::Conflict => StatusCode::CONFLICT,
            Self::InternalServer => StatusCode::INTERNAL_SERVER_ERROR,
            Self::KmsDisabled | Self::KmsInvalidKeyUsage | Self::KmsInvalidState | Self::KmsNotFound | Self::Validation => {
                StatusCode::BAD_REQUEST
            }
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::RequestTimeout => StatusCode::REQUEST_TIMEOUT,
            Self::ServiceQuotaExceeded => StatusCode::PAYMENT_REQUIRED,
            Self::ServiceUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::TooManyRequests => StatusCode::TOO_MANY_REQUESTS,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ProtocolError {
    code: ErrorCode,
    message: String,
    field_list: Vec<ValidationExceptionField>,
}

impl ProtocolError {
    pub(crate) fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            field_list: Vec::new(),
        }
    }

    pub(crate) fn validation(path: impl Into<String>, message: impl Into<String>) -> Self {
        let message = message.into();
        Self {
            code: ErrorCode::Validation,
            field_list: vec![ValidationExceptionField {
                path: path.into(),
                message: message.clone(),
            }],
            message,
        }
    }

    pub(crate) fn code(&self) -> ErrorCode {
        self.code
    }

    pub(crate) fn status_code(&self) -> StatusCode {
        self.code.status_code()
    }

    pub(crate) fn body_bytes(&self) -> Vec<u8> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct ErrorBody<'a> {
            #[serde(rename = "__type")]
            code: &'static str,
            message: &'a str,
            #[serde(skip_serializing_if = "<[ValidationExceptionField]>::is_empty")]
            field_list: &'a [ValidationExceptionField],
        }
        // This envelope contains only strings and lists, with no fallible custom serializers.
        serde_json::to_vec(&ErrorBody {
            code: self.code.name(),
            message: &self.message,
            field_list: &self.field_list,
        })
        .expect("the error envelope contains JSON-compatible strings")
    }

    pub(crate) fn into_response(self) -> Response<Vec<u8>> {
        let mut response = Response::new(self.body_bytes());
        *response.status_mut() = self.status_code();
        response
            .headers_mut()
            .insert(http::header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
        response
            .headers_mut()
            .insert("x-amzn-errortype", HeaderValue::from_static(self.code.name()));
        response
    }
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code.name(), self.message)
    }
}

impl std::error::Error for ProtocolError {}
