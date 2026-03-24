//! POST Object response DTO for `success_action_status=201`
//!
//! See: <https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPOST.html>

use crate::xml;

use std::io::Write;

/// The XML response body returned when `success_action_status=201` is specified
/// in a POST Object request.
#[derive(Debug, Clone)]
pub struct PostResponse<'a> {
    /// The URL of the uploaded object (format: `/{bucket}/{key}`)
    pub location: &'a str,
    /// The name of the bucket
    pub bucket: &'a str,
    /// The object key
    pub key: &'a str,
    /// The entity tag (`ETag`) of the uploaded object
    pub etag: &'a str,
}

impl xml::Serialize for PostResponse<'_> {
    fn serialize<W: Write>(&self, s: &mut xml::Serializer<W>) -> xml::SerResult {
        s.content("PostResponse", self)
    }
}

impl xml::SerializeContent for PostResponse<'_> {
    fn serialize_content<W: Write>(&self, s: &mut xml::Serializer<W>) -> xml::SerResult {
        s.content("Location", self.location)?;
        s.content("Bucket", self.bucket)?;
        s.content("Key", self.key)?;
        s.content("ETag", self.etag)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::xml::Serialize;

    #[test]
    fn test_post_response_xml() {
        let response = PostResponse {
            location: "/my-bucket/my-key",
            bucket: "my-bucket",
            key: "my-key",
            etag: "\"abc123\"",
        };

        let mut buf = Vec::new();
        let mut ser = xml::Serializer::new(&mut buf);
        ser.decl().unwrap();
        response.serialize(&mut ser).unwrap();

        let xml_str = String::from_utf8(buf).unwrap();
        assert!(xml_str.contains("<PostResponse>"));
        assert!(xml_str.contains("<Location>/my-bucket/my-key</Location>"));
        assert!(xml_str.contains("<Bucket>my-bucket</Bucket>"));
        assert!(xml_str.contains("<Key>my-key</Key>"));
        // XML serializer escapes quotes: " -> &quot;
        assert!(xml_str.contains("<ETag>&quot;abc123&quot;</ETag>"));
        assert!(xml_str.contains("</PostResponse>"));
    }

    #[test]
    fn test_post_response_xml_escaping() {
        let response = PostResponse {
            location: "/bucket/<test>",
            bucket: "bucket",
            key: "<test>&value",
            etag: "\"etag\"",
        };

        let mut buf = Vec::new();
        let mut ser = xml::Serializer::new(&mut buf);
        response.serialize(&mut ser).unwrap();

        let xml_str = String::from_utf8(buf).unwrap();
        // XML entities should be escaped
        assert!(xml_str.contains("&lt;test&gt;"));
        assert!(xml_str.contains("&amp;value"));
    }
}
