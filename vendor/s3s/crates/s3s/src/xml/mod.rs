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

    #[cfg(feature = "minio")]
    use crate::dto::{
        DeleteMarkerM, ListObjectVersionMEntry, ListObjectVersionsMOutput, ListObjectsV2MOutput, ObjectInternalInfo, ObjectM,
        ObjectVersionM, TimestampFormat, UserMetadataCollection,
    };

    #[cfg(feature = "minio")]
    impl Serialize for ListObjectVersionsMOutput {
        fn serialize<W: std::io::Write>(&self, s: &mut Serializer<W>) -> SerResult {
            s.element_with_ns("ListVersionsResult", "http://s3.amazonaws.com/doc/2006-03-01/", |s| {
                self.serialize_content(s)
            })
        }
    }

    #[cfg(feature = "minio")]
    impl SerializeContent for ListObjectVersionsMOutput {
        fn serialize_content<W: std::io::Write>(&self, s: &mut Serializer<W>) -> SerResult {
            if let Some(ref val) = self.name {
                s.content("Name", val)?;
            }
            if let Some(ref val) = self.prefix {
                s.content("Prefix", val)?;
            }
            if let Some(ref val) = self.key_marker {
                s.content("KeyMarker", val)?;
            }
            if let Some(ref val) = self.next_key_marker {
                s.content("NextKeyMarker", val)?;
            }
            if let Some(ref val) = self.next_version_id_marker {
                s.content("NextVersionIdMarker", val)?;
            }
            if let Some(ref val) = self.version_id_marker {
                s.content("VersionIdMarker", val)?;
            }
            if let Some(ref val) = self.max_keys {
                s.content("MaxKeys", val)?;
            }
            if let Some(ref val) = self.delimiter {
                s.content("Delimiter", val)?;
            }
            if let Some(ref val) = self.is_truncated {
                s.content("IsTruncated", val)?;
            }
            if let Some(ref prefixes) = self.common_prefixes {
                for prefix in prefixes {
                    s.content("CommonPrefixes", prefix)?;
                }
            }
            for entry in &self.entries {
                match entry {
                    ListObjectVersionMEntry::Version(version) => s.content("Version", version)?,
                    ListObjectVersionMEntry::DeleteMarker(marker) => s.content("DeleteMarker", marker)?,
                }
            }
            if let Some(ref val) = self.encoding_type {
                s.content("EncodingType", val)?;
            }
            Ok(())
        }
    }

    #[cfg(feature = "minio")]
    impl SerializeContent for ObjectVersionM {
        fn serialize_content<W: std::io::Write>(&self, s: &mut Serializer<W>) -> SerResult {
            if let Some(ref val) = self.key {
                s.content("Key", val)?;
            }
            if let Some(ref val) = self.last_modified {
                s.timestamp("LastModified", val, TimestampFormat::DateTime)?;
            }
            if let Some(ref val) = self.e_tag {
                s.content("ETag", val)?;
            }
            if let Some(ref val) = self.size {
                s.content("Size", val)?;
            }
            if let Some(ref val) = self.owner {
                s.content("Owner", val)?;
            }
            if let Some(ref val) = self.storage_class {
                s.content("StorageClass", val)?;
            }
            if let Some(ref val) = self.user_metadata {
                s.content("UserMetadata", val)?;
            }
            if let Some(ref val) = self.user_tags {
                s.content("UserTags", val)?;
            }
            if let Some(ref val) = self.internal {
                s.content("Internal", val)?;
            }
            if let Some(ref val) = self.version_id {
                s.content("VersionId", val)?;
            }
            if let Some(ref val) = self.is_latest {
                s.content("IsLatest", val)?;
            }
            Ok(())
        }
    }

    #[cfg(feature = "minio")]
    impl Serialize for ListObjectsV2MOutput {
        fn serialize<W: std::io::Write>(&self, s: &mut Serializer<W>) -> SerResult {
            s.element_with_ns("ListBucketResult", "http://s3.amazonaws.com/doc/2006-03-01/", |s| {
                self.serialize_content(s)
            })
        }
    }

    #[cfg(feature = "minio")]
    impl SerializeContent for ListObjectsV2MOutput {
        fn serialize_content<W: std::io::Write>(&self, s: &mut Serializer<W>) -> SerResult {
            if let Some(ref val) = self.name {
                s.content("Name", val)?;
            }
            if let Some(ref val) = self.prefix {
                s.content("Prefix", val)?;
            }
            if let Some(ref val) = self.max_keys {
                s.content("MaxKeys", val)?;
            }
            if let Some(ref val) = self.key_count {
                s.content("KeyCount", val)?;
            }
            if let Some(ref val) = self.continuation_token {
                s.content("ContinuationToken", val)?;
            }
            if let Some(ref val) = self.is_truncated {
                s.content("IsTruncated", val)?;
            }
            if let Some(ref val) = self.next_continuation_token {
                s.content("NextContinuationToken", val)?;
            }
            if let Some(ref contents) = self.contents {
                for content in contents {
                    s.content("Contents", content)?;
                }
            }
            if let Some(ref prefixes) = self.common_prefixes {
                for prefix in prefixes {
                    s.content("CommonPrefixes", prefix)?;
                }
            }
            if let Some(ref val) = self.delimiter {
                s.content("Delimiter", val)?;
            }
            if let Some(ref val) = self.encoding_type {
                s.content("EncodingType", val)?;
            }
            if let Some(ref val) = self.start_after {
                s.content("StartAfter", val)?;
            }
            Ok(())
        }
    }

    #[cfg(feature = "minio")]
    impl SerializeContent for ObjectM {
        fn serialize_content<W: std::io::Write>(&self, s: &mut Serializer<W>) -> SerResult {
            if let Some(ref val) = self.key {
                s.content("Key", val)?;
            }
            if let Some(ref val) = self.last_modified {
                s.timestamp("LastModified", val, TimestampFormat::DateTime)?;
            }
            if let Some(ref val) = self.e_tag {
                s.content("ETag", val)?;
            }
            if let Some(ref val) = self.size {
                s.content("Size", val)?;
            }
            if let Some(ref val) = self.owner {
                s.content("Owner", val)?;
            }
            if let Some(ref val) = self.storage_class {
                s.content("StorageClass", val)?;
            }
            if let Some(ref val) = self.user_metadata {
                s.content("UserMetadata", val)?;
            }
            if let Some(ref val) = self.user_tags {
                s.content("UserTags", val)?;
            }
            if let Some(ref val) = self.internal {
                s.content("Internal", val)?;
            }
            Ok(())
        }
    }

    #[cfg(feature = "minio")]
    impl SerializeContent for DeleteMarkerM {
        fn serialize_content<W: std::io::Write>(&self, s: &mut Serializer<W>) -> SerResult {
            if let Some(ref val) = self.key {
                s.content("Key", val)?;
            }
            if let Some(ref val) = self.last_modified {
                s.timestamp("LastModified", val, TimestampFormat::DateTime)?;
            }
            if let Some(ref val) = self.owner {
                s.content("Owner", val)?;
            }
            if let Some(ref val) = self.user_metadata {
                s.content("UserMetadata", val)?;
            }
            if let Some(ref val) = self.user_tags {
                s.content("UserTags", val)?;
            }
            if let Some(ref val) = self.internal {
                s.content("Internal", val)?;
            }
            if let Some(ref val) = self.version_id {
                s.content("VersionId", val)?;
            }
            if let Some(ref val) = self.is_latest {
                s.content("IsLatest", val)?;
            }
            Ok(())
        }
    }

    #[cfg(feature = "minio")]
    impl SerializeContent for UserMetadataCollection {
        fn serialize_content<W: std::io::Write>(&self, s: &mut Serializer<W>) -> SerResult {
            for item in &self.items {
                if item.key.is_empty() {
                    continue;
                }
                s.content(&item.key, &item.value)?;
            }
            Ok(())
        }
    }

    #[cfg(feature = "minio")]
    impl SerializeContent for ObjectInternalInfo {
        fn serialize_content<W: std::io::Write>(&self, s: &mut Serializer<W>) -> SerResult {
            s.content("K", &self.k)?;
            s.content("M", &self.m)?;
            Ok(())
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
