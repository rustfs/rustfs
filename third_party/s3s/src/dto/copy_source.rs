//! x-amz-copy-source
//!
//! The `x-amz-copy-source` header specifies the source object for copy operations.
//! It supports three formats:
//!
//! 1. **Bucket**: `<bucket>/<key>[?versionId=<id>]`
//! 2. **Access Point ARN**: `arn:<partition>:s3:<region>:<account-id>:accesspoint/<name>/object/<key>[?versionId=<id>]`
//! 3. **Outpost ARN**: `arn:<partition>:s3-outposts:<region>:<account-id>:outpost/<outpost-id>/object/<key>[?versionId=<id>]`
//!
//! See <https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html>

use crate::http;
use crate::path;

use std::borrow::Cow;
use std::fmt::Write;

/// x-amz-copy-source
#[derive(Debug, Clone, PartialEq)]
pub enum CopySource {
    /// Bucket format: `<bucket>/<key>`
    Bucket {
        /// bucket
        bucket: Box<str>,
        /// key
        key: Box<str>,
        /// version id
        version_id: Option<Box<str>>,
    },
    /// Access Point ARN format: `arn:<partition>:s3:<region>:<account-id>:accesspoint/<name>/object/<key>`
    AccessPoint {
        /// partition (e.g. `aws`, `aws-cn`, `aws-us-gov`)
        partition: Box<str>,
        /// region
        region: Box<str>,
        /// account id (12-digit)
        account_id: Box<str>,
        /// access point name
        access_point_name: Box<str>,
        /// key
        key: Box<str>,
        /// version id
        version_id: Option<Box<str>>,
    },
    /// S3 on Outposts ARN format: `arn:<partition>:s3-outposts:<region>:<account-id>:outpost/<outpost-id>/object/<key>`
    Outpost {
        /// partition (e.g. `aws`, `aws-cn`, `aws-us-gov`)
        partition: Box<str>,
        /// region
        region: Box<str>,
        /// account id (12-digit)
        account_id: Box<str>,
        /// outpost id
        outpost_id: Box<str>,
        /// key
        key: Box<str>,
        /// version id
        version_id: Option<Box<str>>,
    },
}

/// [`CopySource`]
#[derive(Debug, thiserror::Error)]
pub enum ParseCopySourceError {
    /// pattern mismatch
    #[error("ParseAmzCopySourceError: PatternMismatch")]
    PatternMismatch,

    /// invalid bucket name
    #[error("ParseAmzCopySourceError: InvalidBucketName")]
    InvalidBucketName,

    /// invalid key
    #[error("ParseAmzCopySourceError: InvalidKey")]
    InvalidKey,

    /// invalid encoding
    #[error("ParseAmzCopySourceError: InvalidEncoding")]
    InvalidEncoding,

    /// invalid ARN
    #[error("ParseAmzCopySourceError: InvalidArn")]
    InvalidArn,

    /// invalid access point name
    #[error("ParseAmzCopySourceError: InvalidAccessPointName")]
    InvalidAccessPointName,

    /// invalid account id
    #[error("ParseAmzCopySourceError: InvalidAccountId")]
    InvalidAccountId,
}

/// Extracts `?versionId=<value>` from the raw header, returning `(path_part, Option<version_id>)`.
fn extract_version_id(header: &str) -> Result<(&str, Option<Cow<'_, str>>), ParseCopySourceError> {
    if let Some(idx) = header.find("?versionId=") {
        let (path, version_part) = header.split_at(idx);
        let version_id_raw = version_part.strip_prefix("?versionId=");
        let version_id = version_id_raw
            .map(urlencoding::decode)
            .transpose()
            .map_err(|_| ParseCopySourceError::InvalidEncoding)?;
        Ok((path, version_id))
    } else {
        Ok((header, None))
    }
}

/// Checks if an access point name is valid.
///
/// Rules (from AWS docs):
/// - 3 to 63 characters long
/// - Must start and end with a lowercase letter or digit
/// - Can contain lowercase letters, digits, and hyphens
/// - No consecutive hyphens
fn check_access_point_name(name: &str) -> bool {
    if !(3..=63).contains(&name.len()) {
        return false;
    }
    if !name
        .as_bytes()
        .iter()
        .all(|&b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        return false;
    }
    let first = name.as_bytes()[0];
    let last = name.as_bytes()[name.len() - 1];
    if !(first.is_ascii_lowercase() || first.is_ascii_digit()) {
        return false;
    }
    if !(last.is_ascii_lowercase() || last.is_ascii_digit()) {
        return false;
    }
    if name.contains("--") {
        return false;
    }
    true
}

/// Checks if an AWS account ID is valid (exactly 12 ASCII digits).
fn check_account_id(id: &str) -> bool {
    id.len() == 12 && id.bytes().all(|b| b.is_ascii_digit())
}

/// URL-encodes a path, preserving `/` separators.
fn encode_path(s: &str) -> String {
    let mut buf = String::new();
    for (i, seg) in s.split('/').enumerate() {
        if i > 0 {
            buf.push('/');
        }
        buf.push_str(&urlencoding::encode(seg));
    }
    buf
}

/// Parses an access point ARN resource: `accesspoint/<name>/object/<key>`
fn parse_access_point_resource(
    resource: &str,
    partition: &str,
    region: &str,
    account_id: &str,
    version_id: Option<Cow<'_, str>>,
) -> Result<CopySource, ParseCopySourceError> {
    let rest = resource
        .strip_prefix("accesspoint/")
        .ok_or(ParseCopySourceError::InvalidArn)?;
    let (name, key) = rest.split_once("/object/").ok_or(ParseCopySourceError::InvalidArn)?;

    if !check_access_point_name(name) {
        return Err(ParseCopySourceError::InvalidAccessPointName);
    }
    if key.is_empty() || !path::check_key(key) {
        return Err(ParseCopySourceError::InvalidKey);
    }

    Ok(CopySource::AccessPoint {
        partition: partition.into(),
        region: region.into(),
        account_id: account_id.into(),
        access_point_name: name.into(),
        key: key.into(),
        version_id: version_id.map(Into::into),
    })
}

/// Parses an outpost ARN resource: `outpost/<outpost-id>/object/<key>`
fn parse_outpost_resource(
    resource: &str,
    partition: &str,
    region: &str,
    account_id: &str,
    version_id: Option<Cow<'_, str>>,
) -> Result<CopySource, ParseCopySourceError> {
    let rest = resource.strip_prefix("outpost/").ok_or(ParseCopySourceError::InvalidArn)?;
    let (outpost_id, key) = rest.split_once("/object/").ok_or(ParseCopySourceError::InvalidArn)?;

    if outpost_id.is_empty() {
        return Err(ParseCopySourceError::InvalidArn);
    }
    if key.is_empty() || !path::check_key(key) {
        return Err(ParseCopySourceError::InvalidKey);
    }

    Ok(CopySource::Outpost {
        partition: partition.into(),
        region: region.into(),
        account_id: account_id.into(),
        outpost_id: outpost_id.into(),
        key: key.into(),
        version_id: version_id.map(Into::into),
    })
}

/// Parses a full ARN string: `arn:<partition>:<service>:<region>:<account-id>:<resource>`
fn parse_arn(arn: &str, version_id: Option<Cow<'_, str>>) -> Result<CopySource, ParseCopySourceError> {
    // ARN format: arn:partition:service:region:account-id:resource
    // Use splitn(6, ':') so colons in the resource part are preserved.
    let parts: Vec<&str> = arn.splitn(6, ':').collect();
    if parts.len() != 6 || parts[0] != "arn" {
        return Err(ParseCopySourceError::InvalidArn);
    }

    let partition = parts[1];
    let service = parts[2];
    let region = parts[3];
    let account_id = parts[4];
    let resource = parts[5];

    // Validate partition
    if !matches!(partition, "aws" | "aws-cn" | "aws-us-gov") {
        return Err(ParseCopySourceError::InvalidArn);
    }

    // Validate region is non-empty
    if region.is_empty() {
        return Err(ParseCopySourceError::InvalidArn);
    }

    // Validate account ID (12 ASCII digits)
    if !check_account_id(account_id) {
        return Err(ParseCopySourceError::InvalidAccountId);
    }

    match service {
        "s3" => parse_access_point_resource(resource, partition, region, account_id, version_id),
        "s3-outposts" => parse_outpost_resource(resource, partition, region, account_id, version_id),
        _ => Err(ParseCopySourceError::InvalidArn),
    }
}

impl CopySource {
    /// Parses [`CopySource`] from header
    ///
    /// Supports three formats:
    /// - Bucket: `<bucket>/<key>[?versionId=<id>]`
    /// - Access Point ARN: `arn:<partition>:s3:<region>:<account-id>:accesspoint/<name>/object/<key>[?versionId=<id>]`
    /// - Outpost ARN: `arn:<partition>:s3-outposts:<region>:<account-id>:outpost/<outpost-id>/object/<key>[?versionId=<id>]`
    ///
    /// # Errors
    /// Returns an error if the header is invalid
    pub fn parse(header: &str) -> Result<Self, ParseCopySourceError> {
        let (path_part, version_id) = extract_version_id(header)?;

        let decoded = urlencoding::decode(path_part).map_err(|_| ParseCopySourceError::InvalidEncoding)?;
        let decoded = decoded.strip_prefix('/').unwrap_or(&decoded);

        // Check if this is an ARN-based format
        if decoded.starts_with("arn:") {
            return parse_arn(decoded, version_id);
        }

        // Otherwise, parse as bucket/key
        match decoded.split_once('/') {
            None => Err(ParseCopySourceError::PatternMismatch),
            Some((bucket, key)) => {
                if !path::check_bucket_name(bucket) {
                    return Err(ParseCopySourceError::InvalidBucketName);
                }
                if !path::check_key(key) {
                    return Err(ParseCopySourceError::InvalidKey);
                }
                Ok(Self::Bucket {
                    bucket: bucket.into(),
                    key: key.into(),
                    version_id: version_id.map(Into::into),
                })
            }
        }
    }

    /// Formats the [`CopySource`] to a string suitable for the `x-amz-copy-source` header.
    #[must_use]
    pub fn format_to_string(&self) -> String {
        let mut buf = String::new();
        match self {
            CopySource::Bucket { bucket, key, version_id } => {
                let encoded_key = encode_path(key);
                write!(&mut buf, "{bucket}/{encoded_key}").unwrap();
                if let Some(version_id) = version_id {
                    let encoded_vid = urlencoding::encode(version_id);
                    write!(&mut buf, "?versionId={encoded_vid}").unwrap();
                }
            }
            CopySource::AccessPoint {
                partition,
                region,
                account_id,
                access_point_name,
                key,
                version_id,
            } => {
                let encoded_key = encode_path(key);
                write!(
                    &mut buf,
                    "arn:{partition}:s3:{region}:{account_id}:accesspoint/{access_point_name}/object/{encoded_key}"
                )
                .unwrap();
                if let Some(version_id) = version_id {
                    let encoded_vid = urlencoding::encode(version_id);
                    write!(&mut buf, "?versionId={encoded_vid}").unwrap();
                }
            }
            CopySource::Outpost {
                partition,
                region,
                account_id,
                outpost_id,
                key,
                version_id,
            } => {
                let encoded_key = encode_path(key);
                write!(
                    &mut buf,
                    "arn:{partition}:s3-outposts:{region}:{account_id}:outpost/{outpost_id}/object/{encoded_key}"
                )
                .unwrap();
                if let Some(version_id) = version_id {
                    let encoded_vid = urlencoding::encode(version_id);
                    write!(&mut buf, "?versionId={encoded_vid}").unwrap();
                }
            }
        }
        buf
    }
}

impl http::TryFromHeaderValue for CopySource {
    type Error = ParseCopySourceError;

    fn try_from_header_value(val: &http::HeaderValue) -> Result<Self, Self::Error> {
        let header = val.to_str().map_err(|_| ParseCopySourceError::InvalidEncoding)?;
        Self::parse(header)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Bucket format tests ──

    #[test]
    fn leading_slash_and_percent_decoding() {
        let header = "/awsexamplebucket/reports/file%3Fversion.txt?versionId=abc";
        let val = CopySource::parse(header).unwrap();
        match val {
            CopySource::Bucket { bucket, key, version_id } => {
                assert_eq!(&*bucket, "awsexamplebucket");
                assert_eq!(&*key, "reports/file?version.txt");
                assert_eq!(version_id.as_deref().unwrap(), "abc");
            }
            _ => panic!("expected Bucket variant"),
        }
    }

    #[test]
    fn path_style() {
        {
            let header = "awsexamplebucket/reports/january.pdf";
            let val = CopySource::parse(header).unwrap();
            match val {
                CopySource::Bucket { bucket, key, version_id } => {
                    assert_eq!(&*bucket, "awsexamplebucket");
                    assert_eq!(&*key, "reports/january.pdf");
                    assert!(version_id.is_none());
                }
                _ => panic!("expected Bucket variant"),
            }
        }

        {
            let header = "awsexamplebucket/reports/january.pdf?versionId=QUpfdndhfd8438MNFDN93jdnJFkdmqnh893";
            let val = CopySource::parse(header).unwrap();
            match val {
                CopySource::Bucket { bucket, key, version_id } => {
                    assert_eq!(&*bucket, "awsexamplebucket");
                    assert_eq!(&*key, "reports/january.pdf");
                    assert_eq!(version_id.as_deref().unwrap(), "QUpfdndhfd8438MNFDN93jdnJFkdmqnh893");
                }
                _ => panic!("expected Bucket variant"),
            }
        }
    }

    #[test]
    fn bucket_format_to_string() {
        let cs = CopySource::Bucket {
            bucket: "mybucket".into(),
            key: "mykey".into(),
            version_id: None,
        };
        assert_eq!(cs.format_to_string(), "mybucket/mykey");

        let cs = CopySource::Bucket {
            bucket: "mybucket".into(),
            key: "path/to/obj".into(),
            version_id: Some("v1".into()),
        };
        assert_eq!(cs.format_to_string(), "mybucket/path/to/obj?versionId=v1");
    }

    #[test]
    fn bucket_no_key() {
        let header = "awsexamplebucket";
        let err = CopySource::parse(header).unwrap_err();
        assert!(matches!(err, ParseCopySourceError::PatternMismatch));
    }

    // ── Access Point ARN tests ──

    #[test]
    fn access_point_basic() {
        let header = "arn:aws:s3:us-west-2:123456789012:accesspoint/my-access-point/object/reports/january.pdf";
        let val = CopySource::parse(header).unwrap();
        match val {
            CopySource::AccessPoint {
                partition,
                region,
                account_id,
                access_point_name,
                key,
                version_id,
            } => {
                assert_eq!(&*partition, "aws");
                assert_eq!(&*region, "us-west-2");
                assert_eq!(&*account_id, "123456789012");
                assert_eq!(&*access_point_name, "my-access-point");
                assert_eq!(&*key, "reports/january.pdf");
                assert!(version_id.is_none());
            }
            _ => panic!("expected AccessPoint variant"),
        }
    }

    #[test]
    fn access_point_with_version_id() {
        let header = "arn:aws:s3:us-east-1:111122223333:accesspoint/prod-ap/object/data/file.csv?versionId=ver123";
        let val = CopySource::parse(header).unwrap();
        match val {
            CopySource::AccessPoint {
                partition,
                region,
                account_id,
                access_point_name,
                key,
                version_id,
            } => {
                assert_eq!(&*partition, "aws");
                assert_eq!(&*region, "us-east-1");
                assert_eq!(&*account_id, "111122223333");
                assert_eq!(&*access_point_name, "prod-ap");
                assert_eq!(&*key, "data/file.csv");
                assert_eq!(version_id.as_deref().unwrap(), "ver123");
            }
            _ => panic!("expected AccessPoint variant"),
        }
    }

    #[test]
    fn access_point_url_encoded() {
        // URL-encoded ARN (colons as %3A, slashes in resource preserved)
        let header = "arn%3Aaws%3As3%3Aus-west-2%3A123456789012%3Aaccesspoint/my-ap/object/key.txt";
        let val = CopySource::parse(header).unwrap();
        match val {
            CopySource::AccessPoint {
                partition,
                region,
                account_id,
                access_point_name,
                key,
                version_id,
            } => {
                assert_eq!(&*partition, "aws");
                assert_eq!(&*region, "us-west-2");
                assert_eq!(&*account_id, "123456789012");
                assert_eq!(&*access_point_name, "my-ap");
                assert_eq!(&*key, "key.txt");
                assert!(version_id.is_none());
            }
            _ => panic!("expected AccessPoint variant"),
        }
    }

    #[test]
    fn access_point_with_leading_slash() {
        let header = "/arn:aws:s3:eu-west-1:999888777666:accesspoint/test-ap/object/dir/obj";
        let val = CopySource::parse(header).unwrap();
        match val {
            CopySource::AccessPoint {
                partition,
                region,
                account_id,
                access_point_name,
                key,
                ..
            } => {
                assert_eq!(&*partition, "aws");
                assert_eq!(&*region, "eu-west-1");
                assert_eq!(&*account_id, "999888777666");
                assert_eq!(&*access_point_name, "test-ap");
                assert_eq!(&*key, "dir/obj");
            }
            _ => panic!("expected AccessPoint variant"),
        }
    }

    #[test]
    fn access_point_aws_cn_partition() {
        let header = "arn:aws-cn:s3:cn-north-1:123456789012:accesspoint/cn-ap/object/file.bin";
        let val = CopySource::parse(header).unwrap();
        match val {
            CopySource::AccessPoint {
                partition,
                region,
                account_id,
                access_point_name,
                key,
                version_id,
            } => {
                assert_eq!(&*partition, "aws-cn");
                assert_eq!(&*region, "cn-north-1");
                assert_eq!(&*account_id, "123456789012");
                assert_eq!(&*access_point_name, "cn-ap");
                assert_eq!(&*key, "file.bin");
                assert!(version_id.is_none());
            }
            _ => panic!("expected AccessPoint variant"),
        }
    }

    #[test]
    fn access_point_aws_us_gov_partition() {
        let header = "arn:aws-us-gov:s3:us-gov-west-1:123456789012:accesspoint/gov-ap/object/doc.pdf";
        let val = CopySource::parse(header).unwrap();
        match val {
            CopySource::AccessPoint { partition, region, .. } => {
                assert_eq!(&*partition, "aws-us-gov");
                assert_eq!(&*region, "us-gov-west-1");
            }
            _ => panic!("expected AccessPoint variant"),
        }
    }

    #[test]
    fn access_point_format_to_string() {
        let cs = CopySource::AccessPoint {
            partition: "aws".into(),
            region: "us-west-2".into(),
            account_id: "123456789012".into(),
            access_point_name: "my-ap".into(),
            key: "reports/january.pdf".into(),
            version_id: None,
        };
        assert_eq!(
            cs.format_to_string(),
            "arn:aws:s3:us-west-2:123456789012:accesspoint/my-ap/object/reports/january.pdf"
        );

        let cs = CopySource::AccessPoint {
            partition: "aws".into(),
            region: "us-east-1".into(),
            account_id: "111122223333".into(),
            access_point_name: "prod-ap".into(),
            key: "data/file.csv".into(),
            version_id: Some("v42".into()),
        };
        assert_eq!(
            cs.format_to_string(),
            "arn:aws:s3:us-east-1:111122223333:accesspoint/prod-ap/object/data/file.csv?versionId=v42"
        );
    }

    #[test]
    fn access_point_format_to_string_cn_partition() {
        let cs = CopySource::AccessPoint {
            partition: "aws-cn".into(),
            region: "cn-north-1".into(),
            account_id: "123456789012".into(),
            access_point_name: "cn-ap".into(),
            key: "file.bin".into(),
            version_id: None,
        };
        assert_eq!(
            cs.format_to_string(),
            "arn:aws-cn:s3:cn-north-1:123456789012:accesspoint/cn-ap/object/file.bin"
        );
    }

    #[test]
    fn access_point_roundtrip() {
        let original = "arn:aws:s3:us-west-2:123456789012:accesspoint/my-access-point/object/reports/january.pdf";
        let parsed = CopySource::parse(original).unwrap();
        assert_eq!(parsed.format_to_string(), original);
    }

    #[test]
    fn access_point_roundtrip_with_version() {
        let original = "arn:aws:s3:us-east-1:111122223333:accesspoint/prod-ap/object/data/file.csv?versionId=ver123";
        let parsed = CopySource::parse(original).unwrap();
        assert_eq!(parsed.format_to_string(), original);
    }

    // ── Outpost ARN tests ──

    #[test]
    fn outpost_basic() {
        let header = "arn:aws:s3-outposts:us-west-2:123456789012:outpost/my-outpost/object/reports/january.pdf";
        let val = CopySource::parse(header).unwrap();
        match val {
            CopySource::Outpost {
                partition,
                region,
                account_id,
                outpost_id,
                key,
                version_id,
            } => {
                assert_eq!(&*partition, "aws");
                assert_eq!(&*region, "us-west-2");
                assert_eq!(&*account_id, "123456789012");
                assert_eq!(&*outpost_id, "my-outpost");
                assert_eq!(&*key, "reports/january.pdf");
                assert!(version_id.is_none());
            }
            _ => panic!("expected Outpost variant"),
        }
    }

    #[test]
    fn outpost_with_version_id() {
        let header = "arn:aws:s3-outposts:ap-southeast-1:999888777666:outpost/op-123/object/backup.tar.gz?versionId=abc";
        let val = CopySource::parse(header).unwrap();
        match val {
            CopySource::Outpost {
                region,
                outpost_id,
                key,
                version_id,
                ..
            } => {
                assert_eq!(&*region, "ap-southeast-1");
                assert_eq!(&*outpost_id, "op-123");
                assert_eq!(&*key, "backup.tar.gz");
                assert_eq!(version_id.as_deref().unwrap(), "abc");
            }
            _ => panic!("expected Outpost variant"),
        }
    }

    #[test]
    fn outpost_format_to_string() {
        let cs = CopySource::Outpost {
            partition: "aws".into(),
            region: "us-west-2".into(),
            account_id: "123456789012".into(),
            outpost_id: "my-outpost".into(),
            key: "reports/january.pdf".into(),
            version_id: None,
        };
        assert_eq!(
            cs.format_to_string(),
            "arn:aws:s3-outposts:us-west-2:123456789012:outpost/my-outpost/object/reports/january.pdf"
        );
    }

    #[test]
    fn outpost_roundtrip() {
        let original = "arn:aws:s3-outposts:us-west-2:123456789012:outpost/my-outpost/object/reports/january.pdf";
        let parsed = CopySource::parse(original).unwrap();
        assert_eq!(parsed.format_to_string(), original);
    }

    // ── Error case tests ──

    #[test]
    fn invalid_arn_missing_parts() {
        let header = "arn:aws:s3";
        let err = CopySource::parse(header).unwrap_err();
        assert!(matches!(err, ParseCopySourceError::InvalidArn));
    }

    #[test]
    fn invalid_arn_bad_partition() {
        let header = "arn:invalid:s3:us-west-2:123456789012:accesspoint/ap/object/key";
        let err = CopySource::parse(header).unwrap_err();
        assert!(matches!(err, ParseCopySourceError::InvalidArn));
    }

    #[test]
    fn invalid_arn_bad_service() {
        let header = "arn:aws:ec2:us-west-2:123456789012:accesspoint/ap/object/key";
        let err = CopySource::parse(header).unwrap_err();
        assert!(matches!(err, ParseCopySourceError::InvalidArn));
    }

    #[test]
    fn invalid_arn_empty_region() {
        let header = "arn:aws:s3::123456789012:accesspoint/ap/object/key";
        let err = CopySource::parse(header).unwrap_err();
        assert!(matches!(err, ParseCopySourceError::InvalidArn));
    }

    #[test]
    fn invalid_arn_empty_account_id() {
        let header = "arn:aws:s3:us-west-2::accesspoint/ap/object/key";
        let err = CopySource::parse(header).unwrap_err();
        assert!(matches!(err, ParseCopySourceError::InvalidAccountId));
    }

    #[test]
    fn invalid_arn_missing_object_delimiter() {
        let header = "arn:aws:s3:us-west-2:123456789012:accesspoint/my-ap/key";
        let err = CopySource::parse(header).unwrap_err();
        assert!(matches!(err, ParseCopySourceError::InvalidArn));
    }

    #[test]
    fn invalid_arn_empty_access_point_name() {
        let header = "arn:aws:s3:us-west-2:123456789012:accesspoint//object/key";
        let err = CopySource::parse(header).unwrap_err();
        assert!(matches!(err, ParseCopySourceError::InvalidAccessPointName));
    }

    #[test]
    fn invalid_arn_empty_key() {
        let header = "arn:aws:s3:us-west-2:123456789012:accesspoint/my-ap/object/";
        let err = CopySource::parse(header).unwrap_err();
        assert!(matches!(err, ParseCopySourceError::InvalidKey));
    }

    #[test]
    fn invalid_arn_empty_outpost_id() {
        let header = "arn:aws:s3-outposts:us-west-2:123456789012:outpost//object/key";
        let err = CopySource::parse(header).unwrap_err();
        assert!(matches!(err, ParseCopySourceError::InvalidArn));
    }

    #[test]
    fn invalid_outpost_missing_object_delimiter() {
        let header = "arn:aws:s3-outposts:us-west-2:123456789012:outpost/my-outpost/key";
        let err = CopySource::parse(header).unwrap_err();
        assert!(matches!(err, ParseCopySourceError::InvalidArn));
    }

    #[test]
    fn invalid_arn_bad_resource_prefix() {
        // s3 service but resource doesn't start with "accesspoint/"
        let header = "arn:aws:s3:us-west-2:123456789012:bucket/mybucket/object/key";
        let err = CopySource::parse(header).unwrap_err();
        assert!(matches!(err, ParseCopySourceError::InvalidArn));
    }

    #[test]
    fn invalid_encoding() {
        // %80 is an invalid UTF-8 start byte, which causes urlencoding::decode to fail
        let header = "awsexamplebucket/reports/%80";
        let err = CopySource::parse(header).unwrap_err();
        assert!(matches!(err, ParseCopySourceError::InvalidEncoding));
    }

    // ── Key with special characters in ARN ──

    #[test]
    fn access_point_key_with_special_chars() {
        // Key containing characters that look like URL params but were encoded
        let header = "arn:aws:s3:us-west-2:123456789012:accesspoint/my-ap/object/path/to/file%3Fname.txt";
        let val = CopySource::parse(header).unwrap();
        match val {
            CopySource::AccessPoint { key, .. } => {
                assert_eq!(&*key, "path/to/file?name.txt");
            }
            _ => panic!("expected AccessPoint variant"),
        }
    }

    #[test]
    fn access_point_key_with_spaces() {
        let header = "arn:aws:s3:us-west-2:123456789012:accesspoint/my-ap/object/my%20file.txt";
        let val = CopySource::parse(header).unwrap();
        match val {
            CopySource::AccessPoint { key, .. } => {
                assert_eq!(&*key, "my file.txt");
            }
            _ => panic!("expected AccessPoint variant"),
        }
    }

    // ── Access point name validation tests ──

    #[test]
    fn check_access_point_name_valid() {
        assert!(check_access_point_name("my-access-point"));
        assert!(check_access_point_name("abc")); // minimum length
        assert!(check_access_point_name("a1b2c3"));
        assert!(check_access_point_name("prod-ap-01"));
    }

    #[test]
    fn check_access_point_name_too_short() {
        assert!(!check_access_point_name("ab"));
        assert!(!check_access_point_name(""));
    }

    #[test]
    fn check_access_point_name_too_long() {
        let name = "a".repeat(64);
        assert!(!check_access_point_name(&name));
    }

    #[test]
    fn check_access_point_name_uppercase() {
        assert!(!check_access_point_name("MyAccessPoint"));
    }

    #[test]
    fn check_access_point_name_starts_with_hyphen() {
        assert!(!check_access_point_name("-my-ap"));
    }

    #[test]
    fn check_access_point_name_ends_with_hyphen() {
        assert!(!check_access_point_name("my-ap-"));
    }

    #[test]
    fn check_access_point_name_consecutive_hyphens() {
        assert!(!check_access_point_name("my--ap"));
    }

    #[test]
    fn check_access_point_name_special_chars() {
        assert!(!check_access_point_name("my.ap"));
        assert!(!check_access_point_name("my_ap"));
    }

    #[test]
    fn invalid_access_point_name_in_arn() {
        // Name with uppercase
        let header = "arn:aws:s3:us-west-2:123456789012:accesspoint/MyAP/object/key";
        let err = CopySource::parse(header).unwrap_err();
        assert!(matches!(err, ParseCopySourceError::InvalidAccessPointName));

        // Name too short (2 chars)
        let header = "arn:aws:s3:us-west-2:123456789012:accesspoint/ab/object/key";
        let err = CopySource::parse(header).unwrap_err();
        assert!(matches!(err, ParseCopySourceError::InvalidAccessPointName));

        // Name with consecutive hyphens
        let header = "arn:aws:s3:us-west-2:123456789012:accesspoint/my--ap/object/key";
        let err = CopySource::parse(header).unwrap_err();
        assert!(matches!(err, ParseCopySourceError::InvalidAccessPointName));
    }

    // ── Account ID validation tests ──

    #[test]
    fn check_account_id_valid() {
        assert!(check_account_id("123456789012"));
        assert!(check_account_id("000000000000"));
        assert!(check_account_id("999999999999"));
    }

    #[test]
    fn check_account_id_wrong_length() {
        assert!(!check_account_id("12345678901")); // 11 digits
        assert!(!check_account_id("1234567890123")); // 13 digits
        assert!(!check_account_id(""));
    }

    #[test]
    fn check_account_id_non_digits() {
        assert!(!check_account_id("12345678901a"));
        assert!(!check_account_id("abcdefghijkl"));
    }

    #[test]
    fn invalid_account_id_in_arn() {
        // Too short
        let header = "arn:aws:s3:us-west-2:12345:accesspoint/my-ap/object/key";
        let err = CopySource::parse(header).unwrap_err();
        assert!(matches!(err, ParseCopySourceError::InvalidAccountId));

        // Contains letters
        let header = "arn:aws:s3:us-west-2:12345678901a:accesspoint/my-ap/object/key";
        let err = CopySource::parse(header).unwrap_err();
        assert!(matches!(err, ParseCopySourceError::InvalidAccountId));

        // Too long
        let header = "arn:aws:s3:us-west-2:1234567890123:accesspoint/my-ap/object/key";
        let err = CopySource::parse(header).unwrap_err();
        assert!(matches!(err, ParseCopySourceError::InvalidAccountId));
    }

    // ── Partition roundtrip tests ──

    #[test]
    fn access_point_roundtrip_cn_partition() {
        let original = "arn:aws-cn:s3:cn-north-1:123456789012:accesspoint/cn-ap/object/file.bin";
        let parsed = CopySource::parse(original).unwrap();
        assert_eq!(parsed.format_to_string(), original);
    }

    #[test]
    fn access_point_roundtrip_gov_partition() {
        let original = "arn:aws-us-gov:s3:us-gov-west-1:123456789012:accesspoint/gov-ap/object/doc.pdf";
        let parsed = CopySource::parse(original).unwrap();
        assert_eq!(parsed.format_to_string(), original);
    }

    #[test]
    fn outpost_roundtrip_cn_partition() {
        let original = "arn:aws-cn:s3-outposts:cn-north-1:123456789012:outpost/my-outpost/object/key.txt";
        let parsed = CopySource::parse(original).unwrap();
        assert_eq!(parsed.format_to_string(), original);
    }

    // ── Missing error branch coverage ──

    #[test]
    fn invalid_bucket_name() {
        // Bucket name too short (1 char)
        let header = "a/some-key";
        let err = CopySource::parse(header).unwrap_err();
        assert!(matches!(err, ParseCopySourceError::InvalidBucketName));
    }

    #[test]
    fn invalid_key_too_long_bucket() {
        let long_key = "a".repeat(1025);
        let header = format!("my-bucket/{long_key}");
        let err = CopySource::parse(&header).unwrap_err();
        assert!(matches!(err, ParseCopySourceError::InvalidKey));
    }

    #[test]
    fn invalid_outpost_empty_key() {
        let header = "arn:aws:s3-outposts:us-west-2:123456789012:outpost/my-outpost/object/";
        let err = CopySource::parse(header).unwrap_err();
        assert!(matches!(err, ParseCopySourceError::InvalidKey));
    }

    #[test]
    fn invalid_encoding_version_id() {
        // %80 in versionId triggers InvalidEncoding from extract_version_id
        let header = "my-bucket/key?versionId=%80";
        let err = CopySource::parse(header).unwrap_err();
        assert!(matches!(err, ParseCopySourceError::InvalidEncoding));
    }

    #[test]
    fn invalid_outpost_bad_resource_prefix() {
        // s3-outposts service but resource doesn't start with "outpost/"
        let header = "arn:aws:s3-outposts:us-west-2:123456789012:accesspoint/my-ap/object/key";
        let err = CopySource::parse(header).unwrap_err();
        assert!(matches!(err, ParseCopySourceError::InvalidArn));
    }

    // ── Encoded roundtrip tests ──

    #[test]
    fn bucket_roundtrip_encoded_key() {
        let header = "awsexamplebucket/reports/file%3Fversion.txt?versionId=abc";
        let parsed = CopySource::parse(header).unwrap();
        assert_eq!(parsed.format_to_string(), header);
    }

    #[test]
    fn access_point_roundtrip_encoded_key_spaces() {
        let original = "arn:aws:s3:us-west-2:123456789012:accesspoint/my-ap/object/my%20file.txt";
        let parsed = CopySource::parse(original).unwrap();
        assert_eq!(parsed.format_to_string(), original);
    }

    #[test]
    fn access_point_roundtrip_encoded_key_question_mark() {
        let original = "arn:aws:s3:us-west-2:123456789012:accesspoint/my-ap/object/path/to/file%3Fname.txt";
        let parsed = CopySource::parse(original).unwrap();
        assert_eq!(parsed.format_to_string(), original);
    }

    #[test]
    fn outpost_roundtrip_encoded_key() {
        let original = "arn:aws:s3-outposts:us-west-2:123456789012:outpost/my-outpost/object/my%20doc.pdf";
        let parsed = CopySource::parse(original).unwrap();
        assert_eq!(parsed.format_to_string(), original);
    }
}
