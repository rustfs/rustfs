use super::Body;
use super::Response;

use crate::StdError;
use crate::dto::SelectObjectContentEventStream;
use crate::dto::{Metadata, StreamingBlob, Timestamp, TimestampFormat};
use crate::error::{S3Error, S3Result};
use crate::http::KeepAliveBody;
use crate::http::{HeaderName, HeaderValue};
use crate::utils::format::fmt_timestamp;
use crate::utils::rfc2047;
use crate::xml;

use std::convert::Infallible;
use std::fmt::Write as _;

use hyper::header::{IntoHeaderName, InvalidHeaderValue};

// pub fn add_header<N, V>(res: &mut Response, name: N, value: V) -> S3Result
// where
//     N: IntoHeaderName,
//     V: TryIntoHeaderValue,
//     V::Error: std::error::Error + Send + Sync + 'static,
// {
//     let val = value.try_into_header_value().map_err(S3Error::internal_error)?;
//     res.headers.insert(name, val);
//     Ok(())
// }

pub fn add_opt_header<N, V>(res: &mut Response, name: N, value: Option<V>) -> S3Result
where
    N: IntoHeaderName,
    V: TryIntoHeaderValue,
    V::Error: std::error::Error + Send + Sync + 'static,
{
    if let Some(value) = value {
        let val = value.try_into_header_value().map_err(S3Error::internal_error)?;
        res.headers.insert(name, val);
    }
    Ok(())
}

pub fn add_opt_header_timestamp<N>(res: &mut Response, name: N, value: Option<Timestamp>, fmt: TimestampFormat) -> S3Result
where
    N: IntoHeaderName,
{
    if let Some(value) = value {
        let val = fmt_timestamp(&value, fmt, HeaderValue::from_bytes).map_err(S3Error::internal_error)?;
        res.headers.insert(name, val);
    }
    Ok(())
}

pub trait TryIntoHeaderValue {
    type Error;
    fn try_into_header_value(self) -> Result<HeaderValue, Self::Error>;
}

impl TryIntoHeaderValue for bool {
    type Error = Infallible;

    #[allow(clippy::declare_interior_mutable_const)]
    fn try_into_header_value(self) -> Result<HeaderValue, Self::Error> {
        const TRUE: HeaderValue = HeaderValue::from_static("true");
        const FALSE: HeaderValue = HeaderValue::from_static("false");
        Ok(if self { TRUE } else { FALSE })
    }
}

impl TryIntoHeaderValue for i32 {
    type Error = Infallible;

    fn try_into_header_value(self) -> Result<HeaderValue, Self::Error> {
        Ok(HeaderValue::from(self))
    }
}

impl TryIntoHeaderValue for i64 {
    type Error = Infallible;

    fn try_into_header_value(self) -> Result<HeaderValue, Self::Error> {
        Ok(HeaderValue::from(self))
    }
}

impl TryIntoHeaderValue for String {
    type Error = InvalidHeaderValue;

    fn try_into_header_value(self) -> Result<HeaderValue, Self::Error> {
        HeaderValue::try_from(self)
    }
}

/// See <https://github.com/hyperium/mime/issues/144>
#[allow(clippy::declare_interior_mutable_const)]
const APPLICATION_XML: HeaderValue = HeaderValue::from_static("application/xml");

pub fn set_xml_body<T: xml::Serialize>(res: &mut Response, val: &T) -> S3Result {
    let mut buf = Vec::with_capacity(256);
    {
        let mut ser = xml::Serializer::new(&mut buf);
        ser.decl()
            .and_then(|()| val.serialize(&mut ser))
            .map_err(S3Error::internal_error)?;
    }
    res.body = Body::from(buf);
    res.headers.insert(hyper::header::CONTENT_TYPE, APPLICATION_XML);
    Ok(())
}

#[allow(clippy::declare_interior_mutable_const)]
const TRANSFER_ENCODING_CHUNKED: HeaderValue = HeaderValue::from_static("chunked");

pub fn set_keep_alive_xml_body(
    res: &mut Response,
    fut: impl std::future::Future<Output = Result<Response, StdError>> + Send + Sync + 'static,
    duration: std::time::Duration,
) -> S3Result {
    let mut buf = Vec::with_capacity(40);
    let mut ser = xml::Serializer::new(&mut buf);
    ser.decl().map_err(S3Error::internal_error)?;

    res.body = Body::http_body(KeepAliveBody::new(fut, duration, Some(buf.into()), false));
    res.headers.insert(hyper::header::CONTENT_TYPE, APPLICATION_XML);
    res.headers
        .insert(hyper::header::TRANSFER_ENCODING, TRANSFER_ENCODING_CHUNKED);
    Ok(())
}

pub fn set_xml_body_no_decl<T: xml::Serialize>(res: &mut Response, val: &T) -> S3Result {
    let mut buf = Vec::with_capacity(256);
    let mut ser = xml::Serializer::new(&mut buf);
    val.serialize(&mut ser).map_err(S3Error::internal_error)?;
    res.body = Body::from(buf);
    res.headers.insert(hyper::header::CONTENT_TYPE, APPLICATION_XML);
    Ok(())
}

pub fn set_stream_body(res: &mut Response, stream: StreamingBlob) {
    res.body = Body::from(stream);
}

pub fn set_event_stream_body(res: &mut Response, stream: SelectObjectContentEventStream) {
    res.body = Body::from(stream.into_byte_stream());
    res.headers
        .insert(hyper::header::TRANSFER_ENCODING, HeaderValue::from_static("chunked"));
}

pub fn add_opt_metadata(res: &mut Response, metadata: Option<Metadata>) -> S3Result {
    if let Some(map) = metadata {
        let mut buf = String::new();
        for (key, val) in map {
            write!(&mut buf, "x-amz-meta-{key}").unwrap();
            let name = HeaderName::from_bytes(buf.as_bytes()).map_err(S3Error::internal_error)?;
            let value = rfc2047::encode(&val)
                .map_err(S3Error::internal_error)
                .and_then(|s| HeaderValue::try_from(s.as_ref()).map_err(S3Error::internal_error))?;
            res.headers.insert(name, value);
            buf.clear();
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn new_response() -> Response {
        Response::default()
    }

    #[test]
    fn try_into_header_value_bool() {
        let hv = true.try_into_header_value().unwrap();
        assert_eq!(hv.as_bytes(), b"true");
        let hv = false.try_into_header_value().unwrap();
        assert_eq!(hv.as_bytes(), b"false");
    }

    #[test]
    fn try_into_header_value_i32() {
        let hv = 42i32.try_into_header_value().unwrap();
        assert_eq!(hv.as_bytes(), b"42");
        let hv = (-1i32).try_into_header_value().unwrap();
        assert_eq!(hv.as_bytes(), b"-1");
        let hv = 0i32.try_into_header_value().unwrap();
        assert_eq!(hv.as_bytes(), b"0");
    }

    #[test]
    fn try_into_header_value_i64() {
        let hv = 123_456_789_i64.try_into_header_value().unwrap();
        assert_eq!(hv.as_bytes(), b"123456789");
        let hv = (-99i64).try_into_header_value().unwrap();
        assert_eq!(hv.as_bytes(), b"-99");
    }

    #[test]
    fn try_into_header_value_string() {
        let hv = "hello".to_string().try_into_header_value().unwrap();
        assert_eq!(hv.as_bytes(), b"hello");
    }

    #[test]
    fn add_opt_header_some() {
        let mut res = new_response();
        add_opt_header(&mut res, hyper::header::CONTENT_LENGTH, Some(42i64)).unwrap();
        assert_eq!(res.headers.get(hyper::header::CONTENT_LENGTH).unwrap().as_bytes(), b"42");
    }

    #[test]
    fn add_opt_header_none() {
        let mut res = new_response();
        add_opt_header::<_, String>(&mut res, hyper::header::CONTENT_TYPE, None).unwrap();
        assert!(res.headers.get(hyper::header::CONTENT_TYPE).is_none());
    }

    #[test]
    fn add_opt_header_bool() {
        let mut res = new_response();
        add_opt_header(&mut res, "x-amz-delete-marker", Some(true)).unwrap();
        assert_eq!(res.headers.get("x-amz-delete-marker").unwrap().as_bytes(), b"true");
    }

    #[test]
    fn add_opt_header_timestamp_some() {
        let mut res = new_response();
        let ts = Timestamp::parse(TimestampFormat::HttpDate, "Wed, 21 Oct 2015 07:28:00 GMT").unwrap();
        super::add_opt_header_timestamp(&mut res, "x-amz-date", Some(ts), TimestampFormat::HttpDate).unwrap();
        assert_eq!(res.headers.get("x-amz-date").unwrap().as_bytes(), b"Wed, 21 Oct 2015 07:28:00 GMT");
    }

    #[test]
    fn add_opt_header_timestamp_is_none() {
        let mut res = new_response();
        super::add_opt_header_timestamp(&mut res, "x-amz-date", None, TimestampFormat::HttpDate).unwrap();
        assert!(res.headers.get("x-amz-date").is_none());
    }

    #[test]
    fn set_stream_body_test() {
        let mut res = new_response();
        let blob = StreamingBlob::new(Body::from(Bytes::from_static(b"stream data")));
        set_stream_body(&mut res, blob);
        assert!(!http_body::Body::is_end_stream(&res.body));
    }

    #[test]
    fn add_opt_metadata_some() {
        let mut res = new_response();
        let mut metadata = Metadata::default();
        metadata.insert("key1".into(), "value1".into());
        metadata.insert("key2".into(), "value2".into());
        add_opt_metadata(&mut res, Some(metadata)).unwrap();
        assert!(res.headers.get("x-amz-meta-key1").is_some());
        assert!(res.headers.get("x-amz-meta-key2").is_some());
    }

    #[test]
    fn add_opt_metadata_none() {
        let mut res = new_response();
        add_opt_metadata(&mut res, None).unwrap();
        assert!(res.headers.is_empty());
    }

    #[test]
    fn add_opt_metadata_empty_map() {
        let mut res = new_response();
        let metadata = Metadata::default();
        add_opt_metadata(&mut res, Some(metadata)).unwrap();
        assert!(res.headers.is_empty());
    }
}
