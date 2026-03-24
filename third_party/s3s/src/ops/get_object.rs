use crate::S3Request;
use crate::S3Result;
use crate::dto::GetObjectInput;
use crate::dto::Timestamp;
use crate::dto::TimestampFormat;
use crate::header;
use crate::http::Response;
use crate::utils::format::fmt_timestamp;

use hyper::HeaderMap;
use hyper::header::CONTENT_LENGTH;
use hyper::header::TRANSFER_ENCODING;
use hyper::http::HeaderName;
use hyper::http::HeaderValue;

use stdx::default::default;

pub fn extract_overridden_response_headers(req: &S3Request<GetObjectInput>) -> S3Result<HeaderMap> {
    let mut map: HeaderMap = default();

    add(&mut map, header::CONTENT_TYPE, req.input.response_content_type.as_deref())?;
    add(&mut map, header::CONTENT_LANGUAGE, req.input.response_content_language.as_deref())?;
    add_ts(&mut map, header::EXPIRES, req.input.response_expires.as_ref())?;
    add(&mut map, header::CACHE_CONTROL, req.input.response_cache_control.as_deref())?;
    add(&mut map, header::CONTENT_DISPOSITION, req.input.response_content_disposition.as_deref())?;
    add(&mut map, header::CONTENT_ENCODING, req.input.response_content_encoding.as_deref())?;

    Ok(map)
}

fn add(map: &mut HeaderMap, name: HeaderName, value: Option<&str>) -> S3Result<()> {
    let error = |e| invalid_request!(e, "invalid overridden header: {name}: {value:?}");
    if let Some(value) = value {
        let value = value.parse().map_err(error)?;
        map.insert(name, value);
    }
    Ok(())
}

fn add_ts(map: &mut HeaderMap, name: HeaderName, value: Option<&Timestamp>) -> S3Result<()> {
    let error = |e| invalid_request!(e, "invalid overridden header: {name}: {value:?}");
    if let Some(value) = value {
        let value = fmt_timestamp(value, TimestampFormat::HttpDate, HeaderValue::from_bytes).map_err(error)?;
        map.insert(name, value);
    }
    Ok(())
}

pub fn merge_custom_headers(resp: &mut Response, headers: HeaderMap) {
    resp.headers.extend(headers);

    // special case for https://github.com/Nugine/s3s/issues/80
    if let Some(val) = resp.headers.get(TRANSFER_ENCODING)
        && val.as_bytes() == b"chunked"
    {
        resp.headers.remove(CONTENT_LENGTH);
    }
}
