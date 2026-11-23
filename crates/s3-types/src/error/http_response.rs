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

//! The legacy HTTP response contract for an owned S3 error.

use super::{S3Error, S3ErrorCode};
use crate::Body;
use http::header::{CONTENT_LENGTH, CONTENT_TYPE, TRANSFER_ENCODING};
use http::{HeaderValue, Response, StatusCode};
use quick_xml::escape::escape;

impl S3Error {
    /// Renders this error as an HTTP response using the legacy S3 wire format.
    ///
    /// # Errors
    ///
    /// Returns `InternalError` if an XML field contains a disallowed control character.
    pub fn to_http_response(self) -> Result<Response<Body>, Self> {
        let parts = self.into_http_response_parts();
        let status = parts
            .status
            .or_else(|| parts.code.status_code())
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let bodyless = status.is_informational()
            || matches!(status, StatusCode::NO_CONTENT | StatusCode::RESET_CONTENT | StatusCode::NOT_MODIFIED);
        let mut response = if bodyless {
            Response::new(Body::empty())
        } else {
            let body = xml_body(parts.code.as_str(), parts.message.as_deref(), parts.request_id.as_deref())?;
            let mut response = Response::new(body);
            response
                .headers_mut()
                .insert(CONTENT_TYPE, HeaderValue::from_static("application/xml"));
            response
        };
        *response.status_mut() = status;
        if let Some(headers) = parts.headers {
            *response.headers_mut() = headers;
        }
        if bodyless {
            let headers = response.headers_mut();
            headers.remove(CONTENT_LENGTH);
            headers.remove(CONTENT_TYPE);
            headers.remove(TRANSFER_ENCODING);
        }
        Ok(response)
    }
}

fn xml_body(code: &str, message: Option<&str>, request_id: Option<&str>) -> Result<Body, S3Error> {
    let mut xml = String::from(r#"<?xml version="1.0" encoding="UTF-8"?><Error><Code>"#);
    xml.push_str(&escape(code));
    xml.push_str("</Code>");
    if let Some(message) = message {
        xml.push_str("<Message>");
        xml.push_str(&escape(message));
        xml.push_str("</Message>");
    }
    if let Some(request_id) = request_id {
        xml.push_str("<RequestId>");
        xml.push_str(&escape(request_id));
        xml.push_str("</RequestId>");
    }
    xml.push_str("</Error>");
    if xml.bytes().any(|byte| byte < b' ' && !matches!(byte, b'\t' | b'\n' | b'\r')) {
        return Err(S3Error::with_message(
            S3ErrorCode::InternalError,
            "XML response contains a control character",
        ));
    }
    Ok(Body::from(xml))
}
