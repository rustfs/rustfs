use super::{Multipart, Request};

use crate::dto::{List, Metadata, StreamingBlob, Timestamp, TimestampFormat};
use crate::error::*;
use crate::http::{HeaderName, HeaderValue};
use crate::path::S3Path;
use crate::utils::rfc2047;
use crate::xml;

use std::fmt;
use std::str::FromStr;

use stdx::string::StringExt;
use tracing::{debug, error};

fn missing_header(name: &HeaderName) -> S3Error {
    invalid_request!("missing header: {}", name.as_str())
}

fn duplicate_header(name: &HeaderName) -> S3Error {
    invalid_request!("duplicate header: {}", name.as_str())
}

fn invalid_header<E>(source: E, name: &HeaderName, val: impl fmt::Debug) -> S3Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    s3_error!(source, InvalidArgument, "invalid header: {}: {:?}", name.as_str(), val)
}

fn get_required_header<'r>(req: &'r Request, name: &HeaderName) -> S3Result<&'r HeaderValue> {
    let mut iter = req.headers.get_all(name).into_iter();
    let Some(val) = iter.next() else { return Err(missing_header(name)) };
    let None = iter.next() else { return Err(duplicate_header(name)) };

    if val.is_empty() {
        return Err(missing_header(name));
    }

    Ok(val)
}

fn get_optional_header<'r>(req: &'r Request, name: &HeaderName) -> S3Result<Option<&'r HeaderValue>> {
    let mut iter = req.headers.get_all(name).into_iter();
    let Some(val) = iter.next() else { return Ok(None) };
    let None = iter.next() else { return Err(duplicate_header(name)) };

    if val.is_empty() {
        return Ok(None);
    }

    Ok(Some(val))
}

pub fn parse_header<T>(req: &Request, name: &HeaderName) -> S3Result<T>
where
    T: TryFromHeaderValue,
    T::Error: std::error::Error + Send + Sync + 'static,
{
    let val = get_required_header(req, name)?;

    T::try_from_header_value(val).map_err(|err| invalid_header(err, name, val))
}

pub fn parse_opt_header<T>(req: &Request, name: &HeaderName) -> S3Result<Option<T>>
where
    T: TryFromHeaderValue,
    T::Error: std::error::Error + Send + Sync + 'static,
{
    let Some(val) = get_optional_header(req, name)? else { return Ok(None) };

    match T::try_from_header_value(val) {
        Ok(ans) => Ok(Some(ans)),
        Err(err) => Err(invalid_header(err, name, val)),
    }
}

pub fn parse_checksum_algorithm_header(req: &Request) -> S3Result<Option<crate::dto::ChecksumAlgorithm>> {
    let ans: Option<crate::dto::ChecksumAlgorithm> = parse_opt_header(req, &crate::header::X_AMZ_CHECKSUM_ALGORITHM)?;

    if ans.is_some() {
        return Ok(ans);
    }

    let Some(trailer) = req.headers.get("x-amz-trailer") else {
        return Ok(None);
    };

    let mapping = &const {
        [
            (crate::header::X_AMZ_CHECKSUM_CRC32, crate::dto::ChecksumAlgorithm::CRC32),
            (crate::header::X_AMZ_CHECKSUM_CRC32C, crate::dto::ChecksumAlgorithm::CRC32C),
            (crate::header::X_AMZ_CHECKSUM_SHA1, crate::dto::ChecksumAlgorithm::SHA1),
            (crate::header::X_AMZ_CHECKSUM_SHA256, crate::dto::ChecksumAlgorithm::SHA256),
            (crate::header::X_AMZ_CHECKSUM_CRC64NVME, crate::dto::ChecksumAlgorithm::CRC64NVME),
        ]
    };

    for (h, v) in mapping {
        if trailer.as_bytes() == h.as_str().as_bytes() {
            return Ok(Some(crate::dto::ChecksumAlgorithm::from_static(v)));
        }
    }

    Ok(None)
}

pub fn parse_opt_header_timestamp(req: &Request, name: &HeaderName, fmt: TimestampFormat) -> S3Result<Option<Timestamp>> {
    let Some(val) = get_optional_header(req, name)? else { return Ok(None) };

    let s = val.to_str().map_err(|err| invalid_header(err, name, val))?;
    match Timestamp::parse(fmt, s) {
        Ok(ans) => Ok(Some(ans)),
        Err(err) => Err(invalid_header(err, name, val)),
    }
}

pub fn parse_list_header<T>(req: &Request, name: &HeaderName) -> S3Result<List<T>>
where
    T: TryFromHeaderValue,
    T::Error: std::error::Error + Send + Sync + 'static,
{
    let mut list = List::new();
    for val in req.headers.get_all(name) {
        let ans = T::try_from_header_value(val).map_err(|err| invalid_header(err, name, val))?;
        list.push(ans);
    }
    if list.is_empty() {
        return Err(missing_header(name));
    }
    Ok(list)
}

pub fn parse_opt_list_header<T>(req: &Request, name: &HeaderName) -> S3Result<Option<List<T>>>
where
    T: TryFromHeaderValue,
    T::Error: std::error::Error + Send + Sync + 'static,
{
    let mut list = List::new();
    for val in req.headers.get_all(name) {
        let ans = T::try_from_header_value(val).map_err(|err| invalid_header(err, name, val))?;
        list.push(ans);
    }
    if list.is_empty() {
        return Ok(None);
    }
    Ok(Some(list))
}

fn missing_query(name: &str) -> S3Error {
    invalid_request!("missing query: {}", name)
}

fn duplicate_query(name: &str) -> S3Error {
    invalid_request!("duplicate query: {}", name)
}

fn invalid_query<E>(source: E, name: &str, val: &str) -> S3Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    s3_error!(source, InvalidArgument, "invalid query: {}: {}", name, val)
}

pub fn parse_query<T: FromStr>(req: &Request, name: &str) -> S3Result<T>
where
    T::Err: std::error::Error + Send + Sync + 'static,
{
    let Some(qs) = req.s3ext.qs.as_ref() else { return Err(missing_query(name)) };

    let mut iter = qs.get_all(name);
    let Some(val) = iter.next() else { return Err(missing_query(name)) };
    let None = iter.next() else { return Err(duplicate_query(name)) };

    val.parse::<T>().map_err(|err| invalid_query(err, name, val))
}

pub fn parse_opt_query<T: FromStr>(req: &Request, name: &str) -> S3Result<Option<T>>
where
    T::Err: std::error::Error + Send + Sync + 'static,
{
    let Some(qs) = req.s3ext.qs.as_ref() else { return Ok(None) };

    let mut iter = qs.get_all(name);
    let Some(val) = iter.next() else { return Ok(None) };
    let None = iter.next() else { return Err(duplicate_query(name)) };

    Ok(Some(val.parse::<T>().map_err(|err| invalid_query(err, name, val))?))
}

pub fn parse_opt_query_timestamp(req: &Request, name: &str, fmt: TimestampFormat) -> S3Result<Option<Timestamp>> {
    let Some(qs) = req.s3ext.qs.as_ref() else { return Ok(None) };

    let mut iter = qs.get_all(name);
    let Some(val) = iter.next() else { return Ok(None) };
    let None = iter.next() else { return Err(duplicate_query(name)) };

    Ok(Some(Timestamp::parse(fmt, val).map_err(|err| invalid_query(err, name, val))?))
}

#[track_caller]
pub fn unwrap_bucket(req: &mut Request) -> String {
    match req.s3ext.s3_path.take() {
        Some(S3Path::Bucket { bucket }) => bucket.into(),
        _ => panic!("s3 path not found, expected bucket"),
    }
}

#[track_caller]
pub fn unwrap_object(req: &mut Request) -> (String, String) {
    match req.s3ext.s3_path.take() {
        Some(S3Path::Object { bucket, key }) => (bucket.into(), key.into()),
        _ => panic!("s3 path not found, expected object"),
    }
}

fn malformed_xml(source: xml::DeError) -> S3Error {
    S3Error::with_source(S3ErrorCode::MalformedXML, Box::new(source))
}

fn deserialize_xml<T>(bytes: &[u8]) -> S3Result<T>
where
    T: for<'xml> xml::Deserialize<'xml>,
{
    let mut d = xml::Deserializer::new(bytes);
    let ans = T::deserialize(&mut d).map_err(malformed_xml)?;
    d.expect_eof().map_err(malformed_xml)?;
    Ok(ans)
}

pub fn take_xml_body<T>(req: &mut Request) -> S3Result<T>
where
    T: for<'xml> xml::Deserialize<'xml>,
{
    let bytes = req.body.take_bytes().expect("full body not found");
    if bytes.is_empty() {
        return Err(S3ErrorCode::MissingRequestBodyError.into());
    }
    let result = deserialize_xml(&bytes);
    if result.is_err() {
        error!(?bytes, "malformed xml body");
    }
    result
}

pub fn take_opt_xml_body<T>(req: &mut Request) -> S3Result<Option<T>>
where
    T: for<'xml> xml::Deserialize<'xml>,
{
    let bytes = req.body.take_bytes().expect("full body not found");
    if bytes.is_empty() {
        return Ok(None);
    }
    let result = deserialize_xml(&bytes).map(Some);
    if result.is_err() {
        error!(?bytes, "malformed xml body");
    }
    result
}

/// `MinIO` compatibility: literal `" Enabled "` (with spaces) for legacy object lock/versioning config.
#[cfg(feature = "minio")]
fn is_minio_enabled_literal(bytes: &[u8]) -> bool {
    bytes.trim_ascii() == b"Enabled"
}

/// `MinIO` compatibility: take `ObjectLockConfiguration`, accepting literal `" Enabled "` as enabled.
#[cfg(feature = "minio")]
pub fn take_opt_object_lock_configuration(req: &mut Request) -> S3Result<Option<crate::dto::ObjectLockConfiguration>> {
    use crate::dto::{ObjectLockConfiguration, ObjectLockEnabled};

    let bytes = req.body.take_bytes().expect("full body not found");
    if bytes.is_empty() {
        return Ok(None);
    }
    if is_minio_enabled_literal(&bytes) {
        return Ok(Some(ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from("Enabled".to_owned())),
            rule: None,
        }));
    }
    let result = deserialize_xml::<ObjectLockConfiguration>(&bytes).map(Some);
    if result.is_err() {
        error!(?bytes, "malformed xml body");
    }
    result
}

/// `MinIO` compatibility: take `VersioningConfiguration`, accepting literal `" Enabled "` as enabled.
#[cfg(feature = "minio")]
pub fn take_versioning_configuration(req: &mut Request) -> S3Result<crate::dto::VersioningConfiguration> {
    use crate::dto::{BucketVersioningStatus, VersioningConfiguration};
    use stdx::default::default;

    let bytes = req.body.take_bytes().expect("full body not found");
    if bytes.is_empty() {
        return Err(S3ErrorCode::MissingRequestBodyError.into());
    }
    if is_minio_enabled_literal(&bytes) {
        return Ok(VersioningConfiguration {
            status: Some(BucketVersioningStatus::from("Enabled".to_owned())),
            ..default()
        });
    }
    let result = deserialize_xml::<VersioningConfiguration>(&bytes);
    if result.is_err() {
        error!(?bytes, "malformed xml body");
    }
    result
}

pub fn take_string_body(req: &mut Request) -> S3Result<String> {
    let bytes = req.body.take_bytes().expect("full body not found");
    match String::from_utf8_simd(bytes.into()) {
        Ok(s) => Ok(s),
        Err(_) => Err(invalid_request!("expected UTF-8 body")),
    }
}

pub fn take_stream_body(req: &mut Request) -> StreamingBlob {
    let body = std::mem::take(&mut req.body);
    let size_hint = http_body::Body::size_hint(&body);
    debug!(?size_hint, "taking streaming blob");
    StreamingBlob::from(body)
}

pub fn parse_opt_metadata(req: &Request) -> S3Result<Option<Metadata>> {
    let mut metadata = Metadata::default();
    let map = &req.headers;
    for name in map.keys() {
        let Some(key) = name.as_str().strip_prefix("x-amz-meta-") else { continue };
        if key.is_empty() {
            continue;
        }
        let mut iter = map.get_all(name).into_iter();
        let val = iter.next().unwrap();
        let None = iter.next() else { return Err(duplicate_header(name)) };

        let raw = std::str::from_utf8(val.as_bytes()).map_err(|err| invalid_header(err, name, val))?;
        let val = rfc2047::decode(raw).map_err(|err| invalid_header(err, name, val))?;
        metadata.insert(key.into(), val.into_owned());
    }
    if metadata.is_empty() {
        return Ok(None);
    }
    Ok(Some(metadata))
}

pub trait TryFromHeaderValue: Sized {
    type Error;
    fn try_from_header_value(val: &HeaderValue) -> Result<Self, Self::Error>;
}

#[derive(Debug, thiserror::Error)]
pub enum ParseHeaderError {
    #[error("Invalid boolean value")]
    Boolean,

    #[error("Invalid integer value")]
    Integer,

    #[error("Invalid long value")]
    Long,

    #[error("Invalid enum value")]
    Enum,

    #[error("Invalid string value")]
    String,
}

impl TryFromHeaderValue for bool {
    type Error = ParseHeaderError;

    fn try_from_header_value(val: &HeaderValue) -> Result<Self, Self::Error> {
        match val.as_bytes() {
            b"true" | b"True" => Ok(true),
            b"false" | b"False" => Ok(false),
            _ => Err(ParseHeaderError::Boolean),
        }
    }
}

impl TryFromHeaderValue for i32 {
    type Error = ParseHeaderError;

    fn try_from_header_value(val: &HeaderValue) -> Result<Self, Self::Error> {
        atoi::atoi(val.as_bytes()).ok_or(ParseHeaderError::Integer)
    }
}

impl TryFromHeaderValue for i64 {
    type Error = ParseHeaderError;

    fn try_from_header_value(val: &HeaderValue) -> Result<Self, Self::Error> {
        atoi::atoi(val.as_bytes()).ok_or(ParseHeaderError::Long)
    }
}

impl TryFromHeaderValue for String {
    type Error = ParseHeaderError;

    fn try_from_header_value(val: &HeaderValue) -> Result<Self, Self::Error> {
        match val.to_str() {
            Ok(s) => Ok(s.to_owned()),
            Err(_) => Err(ParseHeaderError::String),
        }
    }
}

pub fn parse_field_value<T>(m: &Multipart, name: &str) -> S3Result<Option<T>>
where
    T: FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    let Some(val) = m.find_field_value(name) else { return Ok(None) };
    match val.parse() {
        Ok(ans) => Ok(Some(ans)),
        Err(source) => Err(s3_error!(source, InvalidArgument, "invalid field value: {}: {:?}", name, val)),
    }
}

pub fn parse_field_value_timestamp(m: &Multipart, name: &str, fmt: TimestampFormat) -> S3Result<Option<Timestamp>> {
    let Some(val) = m.find_field_value(name) else { return Ok(None) };
    match Timestamp::parse(fmt, val) {
        Ok(ans) => Ok(Some(ans)),
        Err(source) => Err(s3_error!(source, InvalidArgument, "invalid field value: {}: {:?}", name, val)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dto::TimestampFormat;
    use crate::http::multipart::File;
    use crate::http::{Body, OrderedQs};
    use crate::path::S3Path;
    use crate::stream::ByteStream;
    use bytes::Bytes;

    fn make_request() -> Request {
        Request {
            version: http::Version::HTTP_11,
            method: hyper::Method::GET,
            uri: hyper::Uri::from_static("http://example.com"),
            headers: hyper::HeaderMap::new(),
            extensions: hyper::http::Extensions::default(),
            body: Body::empty(),
            s3ext: super::super::request::S3Extensions::default(),
        }
    }

    // --- TryFromHeaderValue tests ---

    #[test]
    fn try_from_header_value_bool() {
        use crate::http::HeaderValue;
        assert!(bool::try_from_header_value(&HeaderValue::from_static("true")).unwrap());
        assert!(bool::try_from_header_value(&HeaderValue::from_static("True")).unwrap());
        assert!(!bool::try_from_header_value(&HeaderValue::from_static("false")).unwrap());
        assert!(!bool::try_from_header_value(&HeaderValue::from_static("False")).unwrap());
        assert!(bool::try_from_header_value(&HeaderValue::from_static("invalid")).is_err());
    }

    #[test]
    fn try_from_header_value_i32() {
        use crate::http::HeaderValue;
        assert_eq!(i32::try_from_header_value(&HeaderValue::from_static("42")).unwrap(), 42);
        assert_eq!(i32::try_from_header_value(&HeaderValue::from_static("-1")).unwrap(), -1);
        assert!(i32::try_from_header_value(&HeaderValue::from_static("abc")).is_err());
    }

    #[test]
    fn try_from_header_value_i64() {
        use crate::http::HeaderValue;
        assert_eq!(
            i64::try_from_header_value(&HeaderValue::from_static("9876543210")).unwrap(),
            9_876_543_210_i64
        );
        assert!(i64::try_from_header_value(&HeaderValue::from_static("abc")).is_err());
    }

    #[test]
    fn try_from_header_value_string() {
        use crate::http::HeaderValue;
        assert_eq!(String::try_from_header_value(&HeaderValue::from_static("hello")).unwrap(), "hello");
    }

    // --- parse_header / parse_opt_header tests ---

    #[test]
    fn parse_header_present() {
        let mut req = make_request();
        req.headers.insert("x-custom", "42".parse().unwrap());
        let name = HeaderName::from_static("x-custom");
        let val: i32 = parse_header(&req, &name).unwrap();
        assert_eq!(val, 42);
    }

    #[test]
    fn parse_header_missing() {
        let req = make_request();
        let name = HeaderName::from_static("x-custom");
        let result: S3Result<i32> = parse_header(&req, &name);
        assert!(result.is_err());
    }

    #[test]
    fn parse_header_empty_value() {
        let mut req = make_request();
        req.headers.insert("x-custom", "".parse().unwrap());
        let name = HeaderName::from_static("x-custom");
        let result: S3Result<i32> = parse_header(&req, &name);
        assert!(result.is_err());
    }

    #[test]
    fn parse_header_duplicate() {
        let mut req = make_request();
        req.headers.append("x-custom", "1".parse().unwrap());
        req.headers.append("x-custom", "2".parse().unwrap());
        let name = HeaderName::from_static("x-custom");
        let result: S3Result<i32> = parse_header(&req, &name);
        assert!(result.is_err());
    }

    #[test]
    fn parse_opt_header_present() {
        let mut req = make_request();
        req.headers.insert("x-custom", "hello".parse().unwrap());
        let name = HeaderName::from_static("x-custom");
        let val: Option<String> = parse_opt_header(&req, &name).unwrap();
        assert_eq!(val.as_deref(), Some("hello"));
    }

    #[test]
    fn parse_opt_header_missing() {
        let req = make_request();
        let name = HeaderName::from_static("x-custom");
        let val: Option<String> = parse_opt_header(&req, &name).unwrap();
        assert!(val.is_none());
    }

    #[test]
    fn parse_opt_header_empty() {
        let mut req = make_request();
        req.headers.insert("x-custom", "".parse().unwrap());
        let name = HeaderName::from_static("x-custom");
        let val: Option<String> = parse_opt_header(&req, &name).unwrap();
        assert!(val.is_none());
    }

    #[test]
    fn parse_opt_header_duplicate() {
        let mut req = make_request();
        req.headers.append("x-custom", "a".parse().unwrap());
        req.headers.append("x-custom", "b".parse().unwrap());
        let name = HeaderName::from_static("x-custom");
        let result: S3Result<Option<String>> = parse_opt_header(&req, &name);
        assert!(result.is_err());
    }

    #[test]
    fn parse_opt_header_timestamp_present() {
        let mut req = make_request();
        req.headers
            .insert("x-amz-date", "Wed, 21 Oct 2015 07:28:00 GMT".parse().unwrap());
        let name = HeaderName::from_static("x-amz-date");
        let ts = parse_opt_header_timestamp(&req, &name, TimestampFormat::HttpDate).unwrap();
        assert!(ts.is_some());
    }

    #[test]
    fn parse_opt_header_timestamp_missing() {
        let req = make_request();
        let name = HeaderName::from_static("x-amz-date");
        let ts = parse_opt_header_timestamp(&req, &name, TimestampFormat::HttpDate).unwrap();
        assert!(ts.is_none());
    }

    #[test]
    fn parse_opt_header_timestamp_invalid() {
        let mut req = make_request();
        req.headers.insert("x-amz-date", "not-a-date".parse().unwrap());
        let name = HeaderName::from_static("x-amz-date");
        let result = parse_opt_header_timestamp(&req, &name, TimestampFormat::HttpDate);
        assert!(result.is_err());
    }

    // --- parse_query / parse_opt_query tests ---

    #[test]
    fn parse_query_present() {
        let mut req = make_request();
        req.s3ext.qs = Some(OrderedQs::from_vec_unchecked(vec![("max-keys".into(), "100".into())]));
        let val: i32 = parse_query(&req, "max-keys").unwrap();
        assert_eq!(val, 100);
    }

    #[test]
    fn parse_query_missing_qs() {
        let req = make_request();
        let result: S3Result<i32> = parse_query(&req, "max-keys");
        assert!(result.is_err());
    }

    #[test]
    fn parse_query_missing_key() {
        let mut req = make_request();
        req.s3ext.qs = Some(OrderedQs::from_vec_unchecked(vec![("other".into(), "1".into())]));
        let result: S3Result<i32> = parse_query(&req, "max-keys");
        assert!(result.is_err());
    }

    #[test]
    fn parse_query_duplicate() {
        let mut req = make_request();
        req.s3ext.qs = Some(OrderedQs::from_vec_unchecked(vec![
            ("key".into(), "1".into()),
            ("key".into(), "2".into()),
        ]));
        let result: S3Result<i32> = parse_query(&req, "key");
        assert!(result.is_err());
    }

    #[test]
    fn parse_query_invalid_value() {
        let mut req = make_request();
        req.s3ext.qs = Some(OrderedQs::from_vec_unchecked(vec![("max-keys".into(), "abc".into())]));
        let result: S3Result<i32> = parse_query(&req, "max-keys");
        assert!(result.is_err());
    }

    #[test]
    fn parse_opt_query_present() {
        let mut req = make_request();
        req.s3ext.qs = Some(OrderedQs::from_vec_unchecked(vec![("prefix".into(), "foo".into())]));
        let val: Option<String> = parse_opt_query(&req, "prefix").unwrap();
        assert_eq!(val.as_deref(), Some("foo"));
    }

    #[test]
    fn parse_opt_query_missing() {
        let mut req = make_request();
        req.s3ext.qs = Some(OrderedQs::from_vec_unchecked(vec![]));
        let val: Option<String> = parse_opt_query(&req, "prefix").unwrap();
        assert!(val.is_none());
    }

    #[test]
    fn parse_opt_query_no_qs() {
        let req = make_request();
        let val: Option<String> = parse_opt_query(&req, "prefix").unwrap();
        assert!(val.is_none());
    }

    #[test]
    fn parse_opt_query_duplicate() {
        let mut req = make_request();
        req.s3ext.qs = Some(OrderedQs::from_vec_unchecked(vec![
            ("key".into(), "a".into()),
            ("key".into(), "b".into()),
        ]));
        let result: S3Result<Option<String>> = parse_opt_query(&req, "key");
        assert!(result.is_err());
    }

    #[test]
    fn parse_opt_query_timestamp_present() {
        let mut req = make_request();
        req.s3ext.qs = Some(OrderedQs::from_vec_unchecked(vec![(
            "date".into(),
            "Wed, 21 Oct 2015 07:28:00 GMT".into(),
        )]));
        let ts = parse_opt_query_timestamp(&req, "date", TimestampFormat::HttpDate).unwrap();
        assert!(ts.is_some());
    }

    #[test]
    fn parse_opt_query_timestamp_missing() {
        let req = make_request();
        let ts = parse_opt_query_timestamp(&req, "date", TimestampFormat::HttpDate).unwrap();
        assert!(ts.is_none());
    }

    // --- unwrap_bucket / unwrap_object tests ---

    #[test]
    fn unwrap_bucket_test() {
        let mut req = make_request();
        req.s3ext.s3_path = Some(S3Path::Bucket {
            bucket: "my-bucket".into(),
        });
        let bucket = unwrap_bucket(&mut req);
        assert_eq!(bucket, "my-bucket");
    }

    #[test]
    #[should_panic(expected = "s3 path not found")]
    fn unwrap_bucket_missing() {
        let mut req = make_request();
        let _ = unwrap_bucket(&mut req);
    }

    #[test]
    fn unwrap_object_test() {
        let mut req = make_request();
        req.s3ext.s3_path = Some(S3Path::Object {
            bucket: "my-bucket".into(),
            key: "my-key".into(),
        });
        let (bucket, key) = unwrap_object(&mut req);
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "my-key");
    }

    #[test]
    #[should_panic(expected = "s3 path not found")]
    fn unwrap_object_missing() {
        let mut req = make_request();
        let _ = unwrap_object(&mut req);
    }

    // --- take_string_body / take_xml_body / take_opt_xml_body tests ---

    #[test]
    fn take_string_body_ok() {
        let mut req = make_request();
        req.body = Body::from(Bytes::from_static(b"hello world"));
        let s = take_string_body(&mut req).unwrap();
        assert_eq!(s, "hello world");
    }

    #[test]
    fn take_string_body_empty() {
        let mut req = make_request();
        req.body = Body::from(Bytes::new());
        let s = take_string_body(&mut req).unwrap();
        assert_eq!(s, "");
    }

    #[test]
    fn take_string_body_invalid_utf8() {
        let mut req = make_request();
        req.body = Body::from(vec![0xff, 0xfe]);
        let result = take_string_body(&mut req);
        assert!(result.is_err());
    }

    #[test]
    fn take_stream_body_test() {
        let mut req = make_request();
        req.body = Body::from(Bytes::from_static(b"stream data"));
        let blob = take_stream_body(&mut req);
        let rl = blob.remaining_length();
        // After taking, body should be default (empty)
        assert!(http_body::Body::is_end_stream(&req.body));
        let _ = rl;
    }

    #[test]
    fn take_xml_body_empty_returns_error() {
        let mut req = make_request();
        req.body = Body::from(Bytes::new());
        let result = take_xml_body::<crate::dto::Tagging>(&mut req);
        assert!(result.is_err());
    }

    #[test]
    fn take_opt_xml_body_empty_returns_none() {
        let mut req = make_request();
        req.body = Body::from(Bytes::new());
        let result = take_opt_xml_body::<crate::dto::Tagging>(&mut req).unwrap();
        assert!(result.is_none());
    }

    // --- parse_opt_metadata ---

    #[test]
    fn parse_opt_metadata_none() {
        let req = make_request();
        let result = parse_opt_metadata(&req).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn parse_opt_metadata_some() {
        let mut req = make_request();
        req.headers.insert("x-amz-meta-mykey", "myvalue".parse().unwrap());
        let result = parse_opt_metadata(&req).unwrap();
        let metadata = result.unwrap();
        assert_eq!(metadata.get("mykey").map(String::as_str), Some("myvalue"));
    }

    #[test]
    fn parse_opt_metadata_empty_key_ignored() {
        let mut req = make_request();
        // "x-amz-meta-" with empty suffix should be ignored
        req.headers.insert("x-amz-meta-", "value".parse().unwrap());
        let result = parse_opt_metadata(&req).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn parse_opt_metadata_duplicate_header() {
        let mut req = make_request();
        req.headers.append("x-amz-meta-key", "val1".parse().unwrap());
        req.headers.append("x-amz-meta-key", "val2".parse().unwrap());
        let result = parse_opt_metadata(&req);
        assert!(result.is_err());
    }

    // --- parse_list_header / parse_opt_list_header ---

    #[test]
    fn parse_list_header_single() {
        let mut req = make_request();
        req.headers.insert("x-list", "hello".parse().unwrap());
        let name = HeaderName::from_static("x-list");
        let list: List<String> = parse_list_header(&req, &name).unwrap();
        assert_eq!(list, vec!["hello".to_string()]);
    }

    #[test]
    fn parse_list_header_multiple() {
        let mut req = make_request();
        req.headers.append("x-list", "a".parse().unwrap());
        req.headers.append("x-list", "b".parse().unwrap());
        let name = HeaderName::from_static("x-list");
        let list: List<String> = parse_list_header(&req, &name).unwrap();
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn parse_list_header_missing() {
        let req = make_request();
        let name = HeaderName::from_static("x-list");
        let result: S3Result<List<String>> = parse_list_header(&req, &name);
        assert!(result.is_err());
    }

    #[test]
    fn parse_opt_list_header_present() {
        let mut req = make_request();
        req.headers.insert("x-list", "item".parse().unwrap());
        let name = HeaderName::from_static("x-list");
        let list: Option<List<String>> = parse_opt_list_header(&req, &name).unwrap();
        assert!(list.is_some());
    }

    #[test]
    fn parse_opt_list_header_missing() {
        let req = make_request();
        let name = HeaderName::from_static("x-list");
        let list: Option<List<String>> = parse_opt_list_header(&req, &name).unwrap();
        assert!(list.is_none());
    }

    // --- parse_field_value / parse_field_value_timestamp ---

    #[test]
    fn parse_field_value_found() {
        let m = Multipart::new_for_test(
            vec![("key".into(), "42".into())],
            File {
                name: "file".into(),
                content_type: None,
                stream: None,
            },
        );
        let val: Option<i32> = parse_field_value(&m, "key").unwrap();
        assert_eq!(val, Some(42));
    }

    #[test]
    fn parse_field_value_not_found() {
        let m = Multipart::new_for_test(
            vec![],
            File {
                name: "file".into(),
                content_type: None,
                stream: None,
            },
        );
        let val: Option<i32> = parse_field_value(&m, "key").unwrap();
        assert!(val.is_none());
    }

    #[test]
    fn parse_field_value_invalid() {
        let m = Multipart::new_for_test(
            vec![("key".into(), "abc".into())],
            File {
                name: "file".into(),
                content_type: None,
                stream: None,
            },
        );
        let result: S3Result<Option<i32>> = parse_field_value(&m, "key");
        assert!(result.is_err());
    }

    #[test]
    fn parse_field_value_timestamp_found() {
        let m = Multipart::new_for_test(
            vec![("date".into(), "Wed, 21 Oct 2015 07:28:00 GMT".into())],
            File {
                name: "file".into(),
                content_type: None,
                stream: None,
            },
        );
        let ts = parse_field_value_timestamp(&m, "date", TimestampFormat::HttpDate).unwrap();
        assert!(ts.is_some());
    }

    #[test]
    fn parse_field_value_timestamp_not_found() {
        let m = Multipart::new_for_test(
            vec![],
            File {
                name: "file".into(),
                content_type: None,
                stream: None,
            },
        );
        let ts = parse_field_value_timestamp(&m, "date", TimestampFormat::HttpDate).unwrap();
        assert!(ts.is_none());
    }

    #[test]
    fn parse_field_value_timestamp_invalid() {
        let m = Multipart::new_for_test(
            vec![("date".into(), "not-a-date".into())],
            File {
                name: "file".into(),
                content_type: None,
                stream: None,
            },
        );
        let result = parse_field_value_timestamp(&m, "date", TimestampFormat::HttpDate);
        assert!(result.is_err());
    }

    // --- ParseHeaderError Display ---

    #[test]
    fn parse_header_error_display() {
        assert!(format!("{}", ParseHeaderError::Boolean).contains("boolean"));
        assert!(format!("{}", ParseHeaderError::Integer).contains("integer"));
        assert!(format!("{}", ParseHeaderError::Long).contains("long"));
        assert!(format!("{}", ParseHeaderError::Enum).contains("enum"));
        assert!(format!("{}", ParseHeaderError::String).contains("string"));
    }
}
