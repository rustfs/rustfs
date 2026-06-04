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

use http::{HeaderValue, request};
use time::OffsetDateTime;

use super::request_signature_v4::SignV4Error;
use s3s::Body;

pub(crate) fn streaming_unsigned_v4(
    mut req: request::Request<Body>,
    session_token: &str,
    _data_len: i64,
    _req_time: OffsetDateTime,
) -> Result<request::Request<Body>, Box<(request::Request<Body>, SignV4Error)>> {
    let token_value = if session_token.is_empty() {
        None
    } else {
        match HeaderValue::from_str(session_token) {
            Ok(value) => Some(value),
            Err(err) => {
                return Err(Box::new((
                    req,
                    SignV4Error::HeaderValueParse {
                        name: "X-Amz-Security-Token".to_string(),
                        reason: err.to_string(),
                    },
                )));
            }
        }
    };

    let headers = req.headers_mut();
    headers.insert(http::header::TRANSFER_ENCODING, HeaderValue::from_static("aws-chunked"));
    if let Some(token_value) = token_value {
        headers.insert("X-Amz-Security-Token", token_value);
    }
    //req.content_length = 100；

    Ok(req)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn streaming_unsigned_v4_rejects_invalid_session_token_header() {
        let req = request::Request::builder()
            .uri("https://bucket.example.com/object")
            .body(Body::empty())
            .expect("request should build");

        let failure = streaming_unsigned_v4(req, "bad\nsession", 0, OffsetDateTime::UNIX_EPOCH)
            .expect_err("invalid session token should fail");
        let (_, err) = *failure;

        assert!(matches!(
            err,
            SignV4Error::HeaderValueParse { name, .. } if name == "X-Amz-Security-Token"
        ));
    }
}
