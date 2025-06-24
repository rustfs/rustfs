#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_assignments)]
#![allow(unused_must_use)]

use http::request;
use time::OffsetDateTime;

pub fn streaming_unsigned_v4(
    mut req: request::Builder,
    session_token: &str,
    data_len: i64,
    req_time: OffsetDateTime,
) -> request::Builder {
    todo!();
}
