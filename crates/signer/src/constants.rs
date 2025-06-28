use time::{format_description::FormatItem, macros::format_description};

pub const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";
pub const UNSIGNED_PAYLOAD_TRAILER: &str = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";

pub const TOTAL_WORKERS: i64 = 4;

pub const SIGN_V4_ALGORITHM: &str = "AWS4-HMAC-SHA256";
pub const ISO8601_DATEFORMAT: &[FormatItem<'_>] =
    format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond]Z");
