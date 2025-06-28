use http::{HeaderValue, request};
use time::{OffsetDateTime, macros::format_description};

pub fn streaming_unsigned_v4(
    mut req: request::Builder,
    session_token: &str,
    _data_len: i64,
    req_time: OffsetDateTime,
) -> request::Builder {
    let headers = req.headers_mut().expect("err");

    let chunked_value = HeaderValue::from_str(&vec!["aws-chunked"].join(",")).expect("err");
    headers.insert(http::header::TRANSFER_ENCODING, chunked_value);
    if !session_token.is_empty() {
        headers.insert("X-Amz-Security-Token", HeaderValue::from_str(&session_token).expect("err"));
    }

    let format = format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond]Z");
    headers.insert("X-Amz-Date", HeaderValue::from_str(&req_time.format(&format).unwrap()).expect("err"));
    //req.content_length = 100；

    req
}
