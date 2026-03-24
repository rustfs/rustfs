//! XML serialization and deserialization for S3 request and response bodies.
//!
//! This module provides the [`Serialize`] / [`SerializeContent`] traits and
//! the corresponding [`Deserialize`] / [`DeserializeContent`] traits, together
//! with [`Serializer`] and [`Deserializer`] implementations used to convert
//! between Rust DTOs and the XML wire format required by the S3 REST API.

#![allow(clippy::missing_errors_doc)] // TODO

mod de;
pub use self::de::*;

mod ser;
pub use self::ser::*;

#[cfg(feature = "minio")]
mod generated_minio;

#[cfg(not(feature = "minio"))]
mod generated;

mod manually {
    use super::*;

    use crate::dto::BucketLocationConstraint;
    use crate::dto::GetBucketLocationOutput;

    impl Serialize for GetBucketLocationOutput {
        fn serialize<W: std::io::Write>(&self, s: &mut Serializer<W>) -> SerResult {
            let xmlns = "http://s3.amazonaws.com/doc/2006-03-01/";
            if let Some(location_constraint) = &self.location_constraint {
                s.content_with_ns("LocationConstraint", xmlns, location_constraint)?;
            } else {
                s.content_with_ns("LocationConstraint", xmlns, "")?;
            }
            Ok(())
        }
    }

    impl<'xml> Deserialize<'xml> for GetBucketLocationOutput {
        fn deserialize(d: &mut Deserializer<'xml>) -> DeResult<Self> {
            let mut location_constraint: Option<BucketLocationConstraint> = None;
            d.for_each_element(|d, x| match x {
                b"LocationConstraint" => {
                    if location_constraint.is_some() {
                        return Err(DeError::DuplicateField);
                    }
                    let val: BucketLocationConstraint = d.content()?;
                    if !val.as_str().is_empty() {
                        location_constraint = Some(val);
                    }
                    Ok(())
                }
                _ => Err(DeError::UnexpectedTagName),
            })?;
            Ok(Self { location_constraint })
        }
    }

    use crate::dto::AssumeRoleOutput;

    impl Serialize for AssumeRoleOutput {
        fn serialize<W: std::io::Write>(&self, s: &mut Serializer<W>) -> SerResult {
            let xmlns = "https://sts.amazonaws.com/doc/2011-06-15/";
            s.element_with_ns("AssumeRoleResponse", xmlns, |s| {
                s.content("AssumeRoleResult", self) //
            })?;
            Ok(())
        }
    }

    impl<'xml> Deserialize<'xml> for AssumeRoleOutput {
        fn deserialize(d: &mut Deserializer<'xml>) -> DeResult<Self> {
            d.named_element("AssumeRoleResponse", |d| {
                d.named_element("AssumeRoleResult", Self::deserialize_content) //
            })
        }
    }

    use crate::dto::ETag;
    use crate::dto::ParseETagError;

    use stdx::default::default;

    impl SerializeContent for ETag {
        fn serialize_content<W: std::io::Write>(&self, s: &mut Serializer<W>) -> SerResult {
            let val = self.value();
            if val.len() <= 64 {
                let mut buf: arrayvec::ArrayString<72> = default();
                buf.push('"');
                buf.push_str(val);
                buf.push('"');
                s.write_raw_text(buf.as_str())
            } else {
                s.write_raw_text(&format!("\"{val}\""))
            }
        }
    }

    impl<'xml> DeserializeContent<'xml> for ETag {
        fn deserialize_content(d: &mut Deserializer<'xml>) -> DeResult<Self> {
            let val: String = d.content()?;

            // try to parse as quoted ETag first
            // fallback if the ETag is not quoted
            match ETag::parse_http_header(val.as_bytes()) {
                Ok(v) => Ok(v),
                Err(ParseETagError::InvalidFormat) => Ok(ETag::Strong(val)),
                Err(ParseETagError::InvalidChar) => Err(DeError::InvalidContent),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dto::ETag;
    use std::io::Cursor;

    #[test]
    fn etag_xml_serialization_uses_literal_quotes_not_entities() {
        let etag = ETag::Strong("b264846671938cd88cd6121b3171589b".to_string());
        let mut buf = Vec::new();
        let mut ser = Serializer::new(Cursor::new(&mut buf));
        ser.element("ETag", |s| etag.serialize_content(s)).unwrap();
        let xml = String::from_utf8(buf).unwrap();
        assert!(
            xml.contains("\"b264846671938cd88cd6121b3171589b\""),
            "ETag must be serialized with literal quotes for S3; got: {xml}"
        );
        assert!(!xml.contains("&quot;"), "ETag must not use HTML entity encoding; got: {xml}");
    }

    #[test]
    fn etag_xml_serialization_long_value_uses_literal_quotes() {
        let long_hash = "a".repeat(65);
        let etag = ETag::Strong(long_hash.clone());
        let mut buf = Vec::new();
        let mut ser = Serializer::new(Cursor::new(&mut buf));
        ser.element("ETag", |s| etag.serialize_content(s)).unwrap();
        let xml = String::from_utf8(buf).unwrap();
        let expected = format!("\"{long_hash}\"");
        assert!(xml.contains(&expected), "Long ETag must use literal quotes; got: {xml}");
        assert!(!xml.contains("&quot;"), "Long ETag must not use &quot;; got: {xml}");
    }

    #[test]
    fn create_session_output_xml_serialization() {
        use crate::dto::{CreateSessionOutput, SessionCredentials, Timestamp, TimestampFormat};

        let creds = SessionCredentials {
            access_key_id: "AKIAIOSFODNN7EXAMPLE".to_owned(),
            expiration: Timestamp::parse(TimestampFormat::DateTime, "2024-01-01T00:05:00.000Z").unwrap(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_owned(),
            session_token: "FwoGZXIvYXdzEBYaDHqa0A".to_owned(),
        };

        let output = CreateSessionOutput {
            credentials: creds,
            ..Default::default()
        };

        let mut buf = Vec::new();
        let mut ser = Serializer::new(Cursor::new(&mut buf));
        output.serialize(&mut ser).unwrap();
        let xml = String::from_utf8(buf).unwrap();

        assert!(xml.contains("CreateSessionResult"), "root element must be CreateSessionResult: {xml}");
        assert!(xml.contains("<Credentials>"), "must contain Credentials element: {xml}");
        assert!(
            xml.contains("<AccessKeyId>AKIAIOSFODNN7EXAMPLE</AccessKeyId>"),
            "must contain AccessKeyId: {xml}"
        );
        assert!(xml.contains("<SecretAccessKey>"), "must contain SecretAccessKey: {xml}");
        assert!(xml.contains("<SessionToken>"), "must contain SessionToken: {xml}");
        assert!(xml.contains("<Expiration>"), "must contain Expiration: {xml}");
    }
}
