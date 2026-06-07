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
use time::{OffsetDateTime, macros::format_description};

use s3s::Body;

pub fn streaming_unsigned_v4(
    mut req: request::Request<Body>,
    session_token: &str,
    _data_len: i64,
    req_time: OffsetDateTime,
) -> request::Request<Body> {
    let headers = req.headers_mut();

    let chunked_value = HeaderValue::from_static("aws-chunked");
    headers.insert(http::header::TRANSFER_ENCODING, chunked_value);
    if !session_token.is_empty()
        && let Ok(token_value) = HeaderValue::from_str(session_token)
    {
        headers.insert("X-Amz-Security-Token", token_value);
    }

    let format = format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond]Z");
    if let Ok(date) = req_time.format(&format)
        && let Ok(date_value) = HeaderValue::from_str(&date)
    {
        headers.insert("X-Amz-Date", date_value);
    }
    //req.content_length = 100；

    req
}

#[cfg(test)]
mod tests {
    use super::streaming_unsigned_v4;
    use http::request;
    use s3s::Body;
    use time::OffsetDateTime;

    #[test]
    fn streaming_unsigned_v4_skips_invalid_session_token_header() {
        let req = request::Request::builder()
            .method(http::Method::GET)
            .uri("https://bucket.example.com/object")
            .body(Body::empty())
            .expect("request should build");

        let req = streaming_unsigned_v4(req, "invalid\ntoken", 0, OffsetDateTime::UNIX_EPOCH);

        assert!(req.headers().get("X-Amz-Security-Token").is_none());
        assert_eq!(
            req.headers().get(http::header::TRANSFER_ENCODING),
            Some(&http::HeaderValue::from_static("aws-chunked"))
        );
        assert!(req.headers().get("X-Amz-Date").is_some());
    }
}
