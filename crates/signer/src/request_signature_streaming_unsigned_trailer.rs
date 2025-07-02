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

pub fn streaming_unsigned_v4(
    mut req: request::Builder,
    session_token: &str,
    _data_len: i64,
    req_time: OffsetDateTime,
) -> request::Builder {
    let headers = req.headers_mut().expect("err");

    let chunked_value = HeaderValue::from_str(&["aws-chunked"].join(",")).expect("err");
    headers.insert(http::header::TRANSFER_ENCODING, chunked_value);
    if !session_token.is_empty() {
        headers.insert("X-Amz-Security-Token", HeaderValue::from_str(session_token).expect("err"));
    }

    let format = format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond]Z");
    headers.insert("X-Amz-Date", HeaderValue::from_str(&req_time.format(&format).unwrap()).expect("err"));
    //req.content_length = 100；

    req
}
