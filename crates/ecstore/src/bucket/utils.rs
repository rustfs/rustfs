// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::disk::{MIGRATING_META_BUCKET, RUSTFS_META_BUCKET};
use crate::error::{Error, Result, StorageError};
use regex::Regex;
use rustfs_gateway_types::cors_tagging::{
    CorsTaggingCodecError, PersistedCorsConfiguration, PersistedTagging, parse_cors, parse_tagging, serialize_cors,
    serialize_tagging,
};
use rustfs_gateway_types::persistence::{
    PersistedAccelerateConfiguration, PersistedBucketEncryptionConfiguration, PersistedBucketLoggingStatus,
    PersistedLifecycleConfiguration, PersistedNotificationConfiguration, PersistedObjectLockConfiguration,
    PersistedPublicAccessBlockConfiguration, PersistedReplicationConfiguration, PersistedRequestPaymentConfiguration,
    PersistedVersioningConfiguration, PersistedWebsiteConfiguration, PersistenceCodecError, parse_accelerate,
    parse_bucket_encryption, parse_bucket_logging, parse_lifecycle_legacy_dates, parse_notification, parse_object_lock,
    parse_public_access_block, parse_replication, parse_request_payment, parse_versioning, parse_website, serialize_accelerate,
    serialize_bucket_encryption, serialize_bucket_logging, serialize_lifecycle_legacy_dates, serialize_notification,
    serialize_object_lock, serialize_public_access_block, serialize_replication, serialize_request_payment, serialize_versioning,
    serialize_website,
};
use rustfs_utils::WindowsFileNameError;
use rustfs_utils::path::SLASH_SEPARATOR;
use tracing::instrument;

pub fn is_meta_bucketname(name: &str) -> bool {
    name.starts_with(RUSTFS_META_BUCKET) || name.starts_with(MIGRATING_META_BUCKET)
}

lazy_static::lazy_static! {
    static ref VALID_BUCKET_NAME: Regex = Regex::new(r"^[A-Za-z0-9][A-Za-z0-9\.\-\_\:]{1,61}[A-Za-z0-9]$").unwrap();
    static ref VALID_BUCKET_NAME_STRICT: Regex = Regex::new(r"^[a-z0-9][a-z0-9\.\-]{1,61}[a-z0-9]$").unwrap();
    static ref IP_ADDRESS: Regex = Regex::new(r"^(\d+\.){3}\d+$").unwrap();
}

pub fn check_bucket_name_common(bucket_name: &str, strict: bool) -> Result<()> {
    let bucket_name_trimmed = bucket_name.trim();

    if bucket_name_trimmed != bucket_name {
        return Err(Error::other("Bucket name cannot contain leading or trailing whitespace"));
    }
    if bucket_name_trimmed.is_empty() {
        return Err(Error::other("Bucket name cannot be empty"));
    }
    if bucket_name_trimmed.len() < 3 {
        return Err(Error::other("Bucket name cannot be shorter than 3 characters"));
    }
    if bucket_name_trimmed.len() > 63 {
        return Err(Error::other("Bucket name cannot be longer than 63 characters"));
    }

    if bucket_name_trimmed == "rustfs" {
        return Err(Error::other("Bucket name cannot be rustfs"));
    }

    if IP_ADDRESS.is_match(bucket_name_trimmed) {
        return Err(Error::other("Bucket name cannot be an IP address"));
    }
    if bucket_name_trimmed.contains("..") || bucket_name_trimmed.contains(".-") || bucket_name_trimmed.contains("-.") {
        return Err(Error::other("Bucket name contains invalid characters"));
    }
    if strict {
        if !VALID_BUCKET_NAME_STRICT.is_match(bucket_name_trimmed) {
            return Err(Error::other("Bucket name contains invalid characters"));
        }
    } else if !VALID_BUCKET_NAME.is_match(bucket_name_trimmed) {
        return Err(Error::other("Bucket name contains invalid characters"));
    }
    Ok(())
}

pub fn check_valid_bucket_name(bucket_name: &str) -> Result<()> {
    check_bucket_name_common(bucket_name, false)
}

pub fn check_valid_bucket_name_strict(bucket_name: &str) -> Result<()> {
    check_bucket_name_common(bucket_name, true)
}

/// A bucket configuration persisted as XML in the bucket metadata, together
/// with the codec that reads and writes its stored bytes.
///
/// Every family reads and writes its stored bytes through the gateway
/// persistence codecs (`rustfs_gateway_types::persistence` and
/// `rustfs_gateway_types::cors_tagging`, rustfs/backlog#2745). The codecs keep
/// the persisted bytes the `bucket_config_goldens` pin, so a document the old
/// s3s codec wrote reads back and re-encodes unchanged.
pub trait BucketConfigXml: Sized {
    /// Decodes the stored bytes of one configuration document.
    fn decode_config_xml(input: &[u8]) -> std::result::Result<Self, BucketConfigXmlError>;

    /// Encodes the value as RustFS stores it.
    fn encode_config_xml(&self) -> std::result::Result<Vec<u8>, BucketConfigXmlError>;
}

/// Why a persisted bucket configuration could not be decoded or encoded.
#[derive(Debug, thiserror::Error)]
pub enum BucketConfigXmlError {
    /// A gateway persistence codec refused the bytes or the value.
    #[error(transparent)]
    Persisted(#[from] PersistenceCodecError),
    /// The gateway CORS or Tagging codec refused the bytes or the value.
    #[error(transparent)]
    CorsTagging(#[from] CorsTaggingCodecError),
}

/// Decodes one persisted bucket configuration document.
pub fn deserialize<T: BucketConfigXml>(input: &[u8]) -> std::result::Result<T, BucketConfigXmlError> {
    T::decode_config_xml(input)
}

/// Encodes one bucket configuration as RustFS persists it.
pub fn serialize<T: BucketConfigXml>(val: &T) -> std::result::Result<Vec<u8>, BucketConfigXmlError> {
    val.encode_config_xml()
}

impl BucketConfigXml for PersistedReplicationConfiguration {
    fn decode_config_xml(input: &[u8]) -> std::result::Result<Self, BucketConfigXmlError> {
        Ok(parse_replication(input)?)
    }

    fn encode_config_xml(&self) -> std::result::Result<Vec<u8>, BucketConfigXmlError> {
        Ok(serialize_replication(self)?)
    }
}

impl BucketConfigXml for PersistedLifecycleConfiguration {
    fn decode_config_xml(input: &[u8]) -> std::result::Result<Self, BucketConfigXmlError> {
        Ok(parse_lifecycle_legacy_dates(input)?)
    }

    fn encode_config_xml(&self) -> std::result::Result<Vec<u8>, BucketConfigXmlError> {
        Ok(serialize_lifecycle_legacy_dates(self)?)
    }
}

// The gateway writers of these families are infallible: a value that exists is
// always encodable, so only the readers can refuse bytes.
impl BucketConfigXml for PersistedAccelerateConfiguration {
    fn decode_config_xml(input: &[u8]) -> std::result::Result<Self, BucketConfigXmlError> {
        Ok(parse_accelerate(input)?)
    }

    fn encode_config_xml(&self) -> std::result::Result<Vec<u8>, BucketConfigXmlError> {
        Ok(serialize_accelerate(self))
    }
}

impl BucketConfigXml for PersistedBucketLoggingStatus {
    fn decode_config_xml(input: &[u8]) -> std::result::Result<Self, BucketConfigXmlError> {
        Ok(parse_bucket_logging(input)?)
    }

    fn encode_config_xml(&self) -> std::result::Result<Vec<u8>, BucketConfigXmlError> {
        Ok(serialize_bucket_logging(self))
    }
}

impl BucketConfigXml for PersistedCorsConfiguration {
    fn decode_config_xml(input: &[u8]) -> std::result::Result<Self, BucketConfigXmlError> {
        Ok(parse_cors(input)?)
    }

    fn encode_config_xml(&self) -> std::result::Result<Vec<u8>, BucketConfigXmlError> {
        Ok(serialize_cors(self))
    }
}

impl BucketConfigXml for PersistedNotificationConfiguration {
    fn decode_config_xml(input: &[u8]) -> std::result::Result<Self, BucketConfigXmlError> {
        Ok(parse_notification(input)?)
    }

    fn encode_config_xml(&self) -> std::result::Result<Vec<u8>, BucketConfigXmlError> {
        Ok(serialize_notification(self))
    }
}

impl BucketConfigXml for PersistedObjectLockConfiguration {
    fn decode_config_xml(input: &[u8]) -> std::result::Result<Self, BucketConfigXmlError> {
        Ok(parse_object_lock(input)?)
    }

    fn encode_config_xml(&self) -> std::result::Result<Vec<u8>, BucketConfigXmlError> {
        Ok(serialize_object_lock(self))
    }
}

impl BucketConfigXml for PersistedPublicAccessBlockConfiguration {
    fn decode_config_xml(input: &[u8]) -> std::result::Result<Self, BucketConfigXmlError> {
        Ok(parse_public_access_block(input)?)
    }

    fn encode_config_xml(&self) -> std::result::Result<Vec<u8>, BucketConfigXmlError> {
        Ok(serialize_public_access_block(self))
    }
}

impl BucketConfigXml for PersistedRequestPaymentConfiguration {
    fn decode_config_xml(input: &[u8]) -> std::result::Result<Self, BucketConfigXmlError> {
        Ok(parse_request_payment(input)?)
    }

    fn encode_config_xml(&self) -> std::result::Result<Vec<u8>, BucketConfigXmlError> {
        Ok(serialize_request_payment(self))
    }
}

impl BucketConfigXml for PersistedBucketEncryptionConfiguration {
    fn decode_config_xml(input: &[u8]) -> std::result::Result<Self, BucketConfigXmlError> {
        Ok(parse_bucket_encryption(input)?)
    }

    fn encode_config_xml(&self) -> std::result::Result<Vec<u8>, BucketConfigXmlError> {
        Ok(serialize_bucket_encryption(self))
    }
}

impl BucketConfigXml for PersistedTagging {
    fn decode_config_xml(input: &[u8]) -> std::result::Result<Self, BucketConfigXmlError> {
        Ok(parse_tagging(input)?)
    }

    fn encode_config_xml(&self) -> std::result::Result<Vec<u8>, BucketConfigXmlError> {
        Ok(serialize_tagging(self))
    }
}

impl BucketConfigXml for PersistedVersioningConfiguration {
    fn decode_config_xml(input: &[u8]) -> std::result::Result<Self, BucketConfigXmlError> {
        Ok(parse_versioning(input)?)
    }

    fn encode_config_xml(&self) -> std::result::Result<Vec<u8>, BucketConfigXmlError> {
        Ok(serialize_versioning(self))
    }
}

impl BucketConfigXml for PersistedWebsiteConfiguration {
    fn decode_config_xml(input: &[u8]) -> std::result::Result<Self, BucketConfigXmlError> {
        Ok(parse_website(input)?)
    }

    fn encode_config_xml(&self) -> std::result::Result<Vec<u8>, BucketConfigXmlError> {
        Ok(serialize_website(self))
    }
}

pub fn has_bad_path_component(path: &str) -> bool {
    let n = path.len();
    if n > 32 << 10 {
        // At 32K we are beyond reasonable.
        return true;
    }

    let bytes = path.as_bytes();
    let mut i = 0;

    // Skip leading slashes (for sake of Windows \ is included as well)
    while i < n && (bytes[i] == b'/' || bytes[i] == b'\\') {
        i += 1;
    }

    while i < n {
        // Find the next segment
        let start = i;
        while i < n && bytes[i] != b'/' && bytes[i] != b'\\' {
            i += 1;
        }

        // Trim whitespace of segment and check for ".." or "."
        match path[start..i].trim() {
            "." | ".." => return true,
            _ => {}
        }

        if i < n {
            i += 1;
        }
    }

    false
}

pub fn is_valid_object_prefix(object: &str) -> bool {
    if has_bad_path_component(object) {
        return false;
    }

    if !object.is_char_boundary(0) || std::str::from_utf8(object.as_bytes()).is_err() {
        return false;
    }

    if object.contains("//") {
        return false;
    }

    // This is valid for AWS S3 but it will never
    // work with file systems, we will reject here
    // to return object name invalid rather than
    // a cryptic error from the file system.
    !object.contains('\0')
}

pub fn is_valid_object_name(object: &str) -> bool {
    // Implement object name validation
    if object.is_empty() {
        return false;
    }

    is_valid_object_prefix(object)
}

/// Client-facing reason attached to rejections of object keys that Win32/NTFS
/// cannot represent as file paths (issue #3299). Deployments on Linux/macOS
/// accept the full S3 key character set.
#[allow(
    dead_code,
    reason = "live on Windows: callers sit inside the #[cfg(target_os = \"windows\")] block in check_object_name_for_length_and_slash (backlog#1823)"
)]
pub const WINDOWS_RESERVED_CHARACTERS_REASON: &str = "object key contains characters unsupported on Windows hosts (one of ':', '*', '?', '\"', '|', '<', '>', or a control character)";

/// Client-facing reason for path segments Windows can store but not address
/// afterwards (issue #3449): trailing dot/space or reserved DOS device names.
#[allow(
    dead_code,
    reason = "live on Windows: callers sit inside the #[cfg(target_os = \"windows\")] block in check_object_name_for_length_and_slash (backlog#1823)"
)]
pub const WINDOWS_RESERVED_SEGMENT_REASON: &str = "object key contains a path segment unsupported on Windows hosts (trailing dot or space, or a reserved device name such as NUL/CON/COM1/CONIN$)";

/// Client-facing reason for keys containing `\`: Windows hosts treat it as a
/// path separator, so `a\b` would land on the same file as `a/b`.
pub const WINDOWS_BACKSLASH_REASON: &str =
    "object key contains '\\', which Windows hosts treat as a path separator (it would alias the same key spelled with '/')";

/// Returns the client-facing reason when `object` cannot round-trip through
/// the Win32 path layer, or `None` when it can.
///
/// Each segment is checked with [`rustfs_utils::check_windows_file_name`].
/// `\` splits segments here, like on a Windows disk, so this check never
/// rejects it: an existing `a\b` key must stay readable and deletable. New
/// writes reject `\` separately in [`check_new_object_args`].
#[allow(
    dead_code,
    reason = "live on Windows: callers sit inside the #[cfg(target_os = \"windows\")] block in check_object_name_for_length_and_slash (backlog#1823)"
)]
pub fn windows_object_key_violation(object: &str) -> Option<&'static str> {
    // A NUL byte is rejected on every platform as ObjectNameInvalid by
    // is_valid_object_prefix; keep that answer on Windows too.
    if object.contains('\0') {
        return None;
    }

    let mut segment_violation = false;
    for segment in object.split(['/', '\\']).filter(|segment| !segment.is_empty()) {
        match rustfs_utils::check_windows_file_name(segment) {
            Err(WindowsFileNameError::ReservedCharacter | WindowsFileNameError::ControlCharacter) => {
                return Some(WINDOWS_RESERVED_CHARACTERS_REASON);
            }
            Err(WindowsFileNameError::TrailingDotOrSpace | WindowsFileNameError::ReservedDeviceName) => {
                segment_violation = true;
            }
            // Segment length is enforced in bytes by object_key_segments_fit_on_disk,
            // which is stricter than the UTF-16 limit and answers ObjectNameInvalid.
            Ok(()) | Err(WindowsFileNameError::TooLong | WindowsFileNameError::Empty) => {}
        }
    }
    segment_violation.then_some(WINDOWS_RESERVED_SEGMENT_REASON)
}

pub fn check_object_name_for_length_and_slash(bucket: &str, object: &str) -> Result<()> {
    if object.len() > 1024 {
        return Err(StorageError::ObjectNameTooLong(bucket.to_owned(), object.to_owned()));
    }

    if object.starts_with(SLASH_SEPARATOR) {
        return Err(StorageError::ObjectNamePrefixAsSlash(bucket.to_owned(), object.to_owned()));
    }

    // Reject names that NTFS would refuse, or create but the Win32 path layer
    // cannot read back (os error 3), e.g. `a:b`, `baddir.` or `NUL.txt`.
    #[cfg(target_os = "windows")]
    if let Some(reason) = windows_object_key_violation(object) {
        return Err(StorageError::InvalidArgument(bucket.to_owned(), object.to_owned(), reason.to_owned()));
    }

    Ok(())
}

/// Admission check for every request that introduces `object` into the
/// namespace: PutObject, a CopyObject destination, and CreateMultipartUpload.
/// Sharing one entry keeps their rejections (and S3 error codes) identical.
///
/// `data_movement` writes (decommission, rebalance) relocate keys that already
/// exist, so they skip the new-key-only rules and stay able to move old data.
/// Internal meta buckets are exempt too: their keys are produced by RustFS
/// itself and resolve to the same path either way.
pub fn check_new_object_args(bucket: &str, object: &str, data_movement: bool) -> Result<()> {
    check_put_object_args(bucket, object)?;

    if cfg!(target_os = "windows") && !data_movement && !is_meta_bucketname(bucket) && object.contains('\\') {
        return Err(StorageError::InvalidArgument(
            bucket.to_owned(),
            object.to_owned(),
            WINDOWS_BACKSLASH_REASON.to_owned(),
        ));
    }

    Ok(())
}

pub fn check_copy_obj_args(bucket: &str, object: &str) -> Result<()> {
    check_bucket_and_object_names(bucket, object)
}

pub fn check_get_obj_args(bucket: &str, object: &str) -> Result<()> {
    check_bucket_and_object_names(bucket, object)
}

pub fn check_del_obj_args(bucket: &str, object: &str) -> Result<()> {
    check_bucket_and_object_names(bucket, object)
}

/// Filesystem `NAME_MAX`: every object-key path segment becomes one on-disk
/// directory entry, so a longer segment can never be stored and previously
/// escaped as an `ENAMETOOLONG` io error → `InternalError` 500 (rustfs#5785).
const MAX_OBJECT_KEY_SEGMENT_BYTES: usize = 255;

/// Reject object keys whose on-disk directory names would exceed `NAME_MAX`.
///
/// Middle segments map to their raw bytes; the final segment of a
/// directory-object key (trailing `/`) is stored with the `__XLDIR__` suffix
/// appended, shrinking its budget accordingly.
fn object_key_segments_fit_on_disk(object: &str) -> bool {
    let trailing_dir = object.ends_with('/');
    let segments: Vec<&str> = object.split('/').collect();
    let last_nonempty = segments.iter().rposition(|s| !s.is_empty());
    for (index, segment) in segments.iter().enumerate() {
        let mut budget = MAX_OBJECT_KEY_SEGMENT_BYTES;
        if trailing_dir && Some(index) == last_nonempty {
            budget = budget.saturating_sub(rustfs_utils::path::GLOBAL_DIR_SUFFIX.len());
        }
        if segment.len() > budget {
            return false;
        }
    }
    true
}

pub fn check_bucket_and_object_names(bucket: &str, object: &str) -> Result<()> {
    if !is_meta_bucketname(bucket) && check_valid_bucket_name_strict(bucket).is_err() {
        return Err(StorageError::BucketNameInvalid(bucket.to_string()));
    }

    if object.is_empty() {
        return Err(StorageError::ObjectNameInvalid(bucket.to_string(), object.to_string()));
    }

    if !is_valid_object_prefix(object) {
        return Err(StorageError::ObjectNameInvalid(bucket.to_string(), object.to_string()));
    }

    if !object_key_segments_fit_on_disk(object) {
        return Err(StorageError::ObjectNameInvalid(bucket.to_string(), object.to_string()));
    }

    Ok(())
}

pub fn check_list_objs_args(bucket: &str, prefix: &str, _marker: &Option<String>) -> Result<()> {
    if !is_meta_bucketname(bucket) && check_valid_bucket_name_strict(bucket).is_err() {
        return Err(StorageError::BucketNameInvalid(bucket.to_string()));
    }

    if !is_valid_object_prefix(prefix) {
        return Err(StorageError::ObjectNameInvalid(bucket.to_string(), prefix.to_string()));
    }

    Ok(())
}

pub fn check_list_multipart_args(
    bucket: &str,
    prefix: &str,
    key_marker: &Option<String>,
    upload_id_marker: &Option<String>,
    _delimiter: &Option<String>,
) -> Result<()> {
    check_list_objs_args(bucket, prefix, key_marker)?;

    if let (Some(key_marker), Some(upload_id_marker)) = (key_marker, upload_id_marker) {
        if key_marker.ends_with('/') {
            return Err(StorageError::InvalidUploadIDKeyCombination(
                upload_id_marker.to_string(),
                key_marker.to_string(),
            ));
        }

        if let Err(_e) = base64_simd::URL_SAFE_NO_PAD.decode_to_vec(upload_id_marker.as_bytes()) {
            return Err(StorageError::MalformedUploadID(upload_id_marker.to_owned()));
        }
    }

    Ok(())
}

pub fn check_object_args(bucket: &str, object: &str) -> Result<()> {
    if !is_meta_bucketname(bucket) && check_valid_bucket_name_strict(bucket).is_err() {
        return Err(StorageError::BucketNameInvalid(bucket.to_string()));
    }

    check_object_name_for_length_and_slash(bucket, object)?;

    if !is_valid_object_name(object) {
        return Err(StorageError::ObjectNameInvalid(bucket.to_string(), object.to_string()));
    }

    Ok(())
}

pub fn check_new_multipart_args(bucket: &str, object: &str) -> Result<()> {
    check_object_args(bucket, object)
}

pub fn check_multipart_object_args(bucket: &str, object: &str, upload_id: &str) -> Result<()> {
    if let Err(e) = base64_simd::URL_SAFE_NO_PAD.decode_to_vec(upload_id.as_bytes()) {
        return Err(StorageError::MalformedUploadID(format!("{bucket}/{object}-{upload_id},err:{e}")));
    };
    check_object_args(bucket, object)
}

pub fn check_put_object_part_args(bucket: &str, object: &str, upload_id: &str) -> Result<()> {
    check_multipart_object_args(bucket, object, upload_id)
}

pub fn check_list_parts_args(bucket: &str, object: &str, upload_id: &str) -> Result<()> {
    check_multipart_object_args(bucket, object, upload_id)
}

pub fn check_complete_multipart_args(bucket: &str, object: &str, upload_id: &str) -> Result<()> {
    check_multipart_object_args(bucket, object, upload_id)
}

pub fn check_abort_multipart_args(bucket: &str, object: &str, upload_id: &str) -> Result<()> {
    check_multipart_object_args(bucket, object, upload_id)
}

#[instrument(level = "debug")]
pub fn check_put_object_args(bucket: &str, object: &str) -> Result<()> {
    if !is_meta_bucketname(bucket) && check_valid_bucket_name_strict(bucket).is_err() {
        return Err(StorageError::BucketNameInvalid(bucket.to_string()));
    }

    check_object_name_for_length_and_slash(bucket, object)?;

    if object.is_empty() || !is_valid_object_prefix(object) {
        return Err(StorageError::ObjectNameInvalid(bucket.to_string(), object.to_string()));
    }

    // The write path validates arguments here rather than through
    // check_bucket_and_object_names, so the on-disk segment budget has to be
    // enforced in both places or an over-NAME_MAX key still reaches the disk
    // layer and escapes as ENAMETOOLONG → InternalError 500 (rustfs#5785).
    if !object_key_segments_fit_on_disk(object) {
        return Err(StorageError::ObjectNameInvalid(bucket.to_string(), object.to_string()));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// Every bucket-configuration family reads and writes through its own
    /// gateway codec (rustfs/backlog#2745): the family's golden document
    /// decodes and re-encodes to the same bytes, and a document of another
    /// family is refused instead of decoding into an empty configuration.
    #[test]
    fn every_bucket_config_family_uses_its_own_gateway_codec() {
        fn assert_family<T: BucketConfigXml>(family: &str, own: &[u8], foreign: &[u8]) {
            let value = deserialize::<T>(own).unwrap_or_else(|err| panic!("{family}: own document must decode: {err}"));
            let encoded = serialize(&value).unwrap_or_else(|err| panic!("{family}: value must encode: {err}"));
            assert_eq!(encoded, own, "{family}: re-encoding must keep the persisted bytes");
            assert!(deserialize::<T>(foreign).is_err(), "{family}: a foreign root element must be refused");
        }

        macro_rules! golden {
            ($family:literal) => {
                include_bytes!(concat!("../../tests/fixtures/bucket-config-goldens/", $family, "/minio.out.xml"))
            };
        }
        let tagging = golden!("tagging");
        let versioning = golden!("versioning");

        assert_family::<PersistedAccelerateConfiguration>("accelerate", golden!("accelerate"), tagging);
        assert_family::<PersistedBucketLoggingStatus>("logging", golden!("logging"), tagging);
        assert_family::<PersistedCorsConfiguration>("cors", golden!("cors"), tagging);
        assert_family::<PersistedNotificationConfiguration>("notification", golden!("notification"), tagging);
        assert_family::<PersistedObjectLockConfiguration>("object_lock", golden!("object_lock"), tagging);
        assert_family::<PersistedPublicAccessBlockConfiguration>("public_access_block", golden!("public_access_block"), tagging);
        assert_family::<PersistedRequestPaymentConfiguration>("request_payment", golden!("request_payment"), tagging);
        assert_family::<PersistedBucketEncryptionConfiguration>("encryption", golden!("encryption"), tagging);
        assert_family::<PersistedTagging>("tagging", tagging, versioning);
        assert_family::<PersistedVersioningConfiguration>("versioning", versioning, tagging);
        assert_family::<PersistedWebsiteConfiguration>("website", golden!("website"), tagging);
    }

    /// rustfs#5785: keys whose path segments exceed the on-disk NAME_MAX
    /// budget must be rejected up front as ObjectNameInvalid (4xx), not leak
    /// ENAMETOOLONG as InternalError 500 from the disk layer.
    #[test]
    fn object_key_segment_name_max_budget() {
        // 255-byte single segment: exactly at the on-disk limit.
        assert!(check_bucket_and_object_names("bucket", &"a".repeat(255)).is_ok());
        // 256 bytes: one over.
        assert!(matches!(
            check_bucket_and_object_names("bucket", &"a".repeat(256)),
            Err(StorageError::ObjectNameInvalid(_, _))
        ));
        // Long keys are fine as long as every segment fits.
        let segmented = ["b".repeat(200), "c".repeat(200), "d".repeat(200)].join("/");
        assert!(check_bucket_and_object_names("bucket", &segmented).is_ok());
        // The budget counts bytes, not characters (100 CJK chars = 300 bytes).
        assert!(matches!(
            check_bucket_and_object_names("bucket", &"中".repeat(100)),
            Err(StorageError::ObjectNameInvalid(_, _))
        ));
        assert!(check_bucket_and_object_names("bucket", &"中".repeat(85)).is_ok());
        // Directory-object keys spend GLOBAL_DIR_SUFFIX bytes of the final
        // segment's budget on the on-disk __XLDIR__ encoding.
        let dir_budget = 255 - rustfs_utils::path::GLOBAL_DIR_SUFFIX.len();
        assert!(check_bucket_and_object_names("bucket", &format!("{}/", "e".repeat(dir_budget))).is_ok());
        assert!(matches!(
            check_bucket_and_object_names("bucket", &format!("{}/", "e".repeat(dir_budget + 1))),
            Err(StorageError::ObjectNameInvalid(_, _))
        ));
    }

    /// rustfs#5785 follow-up: the write path validates through
    /// check_put_object_args, not check_bucket_and_object_names, so the same
    /// budget has to hold there — otherwise an over-NAME_MAX PUT still reached
    /// the disk layer and came back as InternalError 500.
    #[test]
    fn put_object_args_enforce_the_same_segment_budget() {
        assert!(check_put_object_args("bucket", &"a".repeat(255)).is_ok());
        assert!(matches!(
            check_put_object_args("bucket", &"a".repeat(256)),
            Err(StorageError::ObjectNameInvalid(_, _))
        ));
        assert!(matches!(
            check_put_object_args("bucket", &"\u{4e2d}".repeat(100)),
            Err(StorageError::ObjectNameInvalid(_, _))
        ));
        let segmented = ["b".repeat(200), "c".repeat(200), "d".repeat(200)].join("/");
        assert!(check_put_object_args("bucket", &segmented).is_ok());
        let dir_budget = 255 - rustfs_utils::path::GLOBAL_DIR_SUFFIX.len();
        assert!(check_put_object_args("bucket", &format!("{}/", "e".repeat(dir_budget))).is_ok());
        assert!(matches!(
            check_put_object_args("bucket", &format!("{}/", "e".repeat(dir_budget + 1))),
            Err(StorageError::ObjectNameInvalid(_, _))
        ));
    }

    // Test validation functions
    #[test]
    fn test_is_valid_object_name() {
        // Valid cases
        assert!(is_valid_object_name("valid-object-name"));
        assert!(is_valid_object_name("object/with/slashes"));
        assert!(is_valid_object_name("object with spaces"));
        assert!(is_valid_object_name("object_with_underscores"));
        assert!(is_valid_object_name("object.with.dots"));
        assert!(is_valid_object_name("single"));
        assert!(is_valid_object_name("file.txt"));
        assert!(is_valid_object_name("path/to/file.txt"));
        assert!(is_valid_object_name("a/b/c/d/e/f"));
        assert!(is_valid_object_name("object-123"));
        assert!(is_valid_object_name("object(1)"));
        assert!(is_valid_object_name("object[1]"));
        assert!(is_valid_object_name("object@domain.com"));

        // Invalid cases - empty string
        assert!(!is_valid_object_name(""));

        // Valid cases - trailing slash is allowed (empty object with trailing slash)
        assert!(is_valid_object_name("object/"));
        assert!(is_valid_object_name("path/to/file/"));
        assert!(is_valid_object_name("ends/with/slash/"));

        // Invalid cases - bad path components (inherited from is_valid_object_prefix)
        assert!(!is_valid_object_name("."));
        assert!(!is_valid_object_name(".."));
        assert!(!is_valid_object_name("object/.."));
        assert!(!is_valid_object_name("object/."));
        assert!(!is_valid_object_name("../object"));
        assert!(!is_valid_object_name("./object"));
        assert!(!is_valid_object_name("path/../other"));
        assert!(!is_valid_object_name("path/./other"));
        assert!(!is_valid_object_name("a/../b/../c"));
        assert!(!is_valid_object_name("a/./b/./c"));

        // Invalid cases - double slashes
        assert!(!is_valid_object_name("object//with//double//slashes"));
        assert!(!is_valid_object_name("//leading/double/slash"));
        assert!(!is_valid_object_name("trailing/double/slash//"));

        // Invalid cases - null characters
        assert!(!is_valid_object_name("object\x00with\x00null"));
        assert!(!is_valid_object_name("object\x00"));
        assert!(!is_valid_object_name("\x00object"));

        // Invalid cases - overly long path (>32KB)
        let long_path = "a/".repeat(16385); // 16385 * 2 = 32770 bytes, over 32KB (32768)
        assert!(!is_valid_object_name(&long_path));

        // Valid cases - prefixes that are valid for object names too
        assert!(is_valid_object_name("prefix"));
        assert!(is_valid_object_name("deep/nested/object"));
        assert!(is_valid_object_name("normal_object"));
    }

    #[test]
    fn test_is_valid_object_prefix() {
        // Valid cases
        assert!(is_valid_object_prefix("valid-prefix"));
        assert!(is_valid_object_prefix(""));
        assert!(is_valid_object_prefix("prefix/with/slashes"));
        assert!(is_valid_object_prefix("prefix/"));
        assert!(is_valid_object_prefix("deep/nested/prefix/"));
        assert!(is_valid_object_prefix("normal-prefix"));
        assert!(is_valid_object_prefix("prefix_with_underscores"));
        assert!(is_valid_object_prefix("prefix.with.dots"));

        // Invalid cases - bad path components
        assert!(!is_valid_object_prefix("."));
        assert!(!is_valid_object_prefix(".."));
        assert!(!is_valid_object_prefix("prefix/.."));
        assert!(!is_valid_object_prefix("prefix/."));
        assert!(!is_valid_object_prefix("../prefix"));
        assert!(!is_valid_object_prefix("./prefix"));
        assert!(!is_valid_object_prefix("prefix/../other"));
        assert!(!is_valid_object_prefix("prefix/./other"));
        assert!(!is_valid_object_prefix("a/../b/../c"));
        assert!(!is_valid_object_prefix("a/./b/./c"));
        assert!(!is_valid_object_prefix("\x0b./object"));
        assert!(!is_valid_object_prefix("prefix/\x0b../object"));
        assert!(!is_valid_object_prefix("\x0b.\\\\object"));

        // Invalid cases - double slashes
        assert!(!is_valid_object_prefix("prefix//with//double//slashes"));
        assert!(!is_valid_object_prefix("//leading/double/slash"));
        assert!(!is_valid_object_prefix("trailing/double/slash//"));

        // Invalid cases - null characters
        assert!(!is_valid_object_prefix("prefix\x00with\x00null"));
        assert!(!is_valid_object_prefix("prefix\x00"));
        assert!(!is_valid_object_prefix("\x00prefix"));

        // Invalid cases - overly long path (>32KB)
        let long_path = "a/".repeat(16385); // 16385 * 2 = 32770 bytes, over 32KB (32768)
        assert!(!is_valid_object_prefix(&long_path));
    }

    #[test]
    fn test_windows_object_key_violation_segments() {
        let segment = Some(WINDOWS_RESERVED_SEGMENT_REASON);

        // Segments ending in a dot are writable on NTFS but unreadable via Win32 paths.
        assert_eq!(windows_object_key_violation("baddir."), segment);
        assert_eq!(windows_object_key_violation("dir1/dir2/baddir."), segment);
        assert_eq!(windows_object_key_violation("dir1/baddir./file.txt"), segment);
        assert_eq!(windows_object_key_violation("file.txt."), segment);

        // Segments ending in a space.
        assert_eq!(windows_object_key_violation("file.txt "), segment);
        assert_eq!(windows_object_key_violation("dir /file.txt"), segment);

        // Reserved DOS device names, bare and case-insensitive.
        for object in [
            "NUL",
            "nul",
            "CON",
            "prn",
            "Aux",
            "COM1",
            "com9",
            "LPT1",
            "lpt9",
            "dir/NUL/file.txt",
            // Superscript port numbers and console aliases are devices too.
            "COM\u{b9}",
            "dir/lpt\u{b3}",
            "CONIN$",
            "conout$",
        ] {
            assert_eq!(windows_object_key_violation(object), segment, "{object:?}");
        }

        // Reserved device names with an extension still resolve to the device.
        for object in [
            "NUL.txt",
            "dir/aux.log",
            "con.tar.gz",
            "com1.dat",
            "NUL .txt",
            "COM\u{b2}.log",
        ] {
            assert_eq!(windows_object_key_violation(object), segment, "{object:?}");
        }

        // Backslash-separated segments are checked as well.
        assert_eq!(windows_object_key_violation("dir\\baddir.\\file.txt"), segment);
        assert_eq!(windows_object_key_violation("dir\\nul"), segment);

        // Valid names must not be flagged.
        for object in [
            "file.txt",
            "dir.name/file",
            "path/to/file.tar.gz",
            "nullable",
            "CONSOLE",
            "com10",
            "com0",
            "lpt",
            "CONIN",
            "aux-data/file",
            "object with spaces inside",
            "dir/",
            "",
        ] {
            assert_eq!(windows_object_key_violation(object), None, "{object:?}");
        }
    }

    #[test]
    fn test_windows_object_key_violation_characters() {
        let characters = Some(WINDOWS_RESERVED_CHARACTERS_REASON);
        for object in ["path/*sUt*mykey", "a:b", "what?", "pipe|name", "quote\"d", "lt<gt>"] {
            assert_eq!(windows_object_key_violation(object), characters, "{object:?}");
        }

        // Control characters 0x01-0x1F cannot appear in a Win32 file name.
        for object in ["a\u{1}b", "tab\tname", "dir/line\nbreak", "esc\u{1b}", "unit\u{1f}"] {
            assert_eq!(windows_object_key_violation(object), characters, "{object:?}");
        }

        // NUL keeps its platform-independent ObjectNameInvalid answer.
        assert_eq!(windows_object_key_violation("a\0b"), None);

        // A character violation outranks a segment violation anywhere in the key.
        assert_eq!(windows_object_key_violation("baddir./a:b"), characters);
    }

    /// Existing `a\b` keys must stay readable, deletable and abortable, so the
    /// shared name check never rejects `\` (only new writes do, on Windows).
    #[test]
    fn test_backslash_passes_the_shared_name_check() {
        assert_eq!(windows_object_key_violation("a\\b"), None);
        assert_eq!(windows_object_key_violation("dir\\sub\\file.txt"), None);
        assert!(check_object_name_for_length_and_slash("test-bucket", "a\\b").is_ok());
        assert!(check_object_args("test-bucket", "a\\b").is_ok());
        assert!(check_bucket_and_object_names("test-bucket", "a\\b").is_ok());
        // Data movement relocates keys that already exist.
        assert!(check_new_object_args("test-bucket", "a\\b", true).is_ok());
        assert!(check_new_object_args(RUSTFS_META_BUCKET, "config\\a", false).is_ok());
    }

    #[test]
    fn test_check_new_object_args_matches_put_object_args() {
        for object in [
            "file.txt",
            "",
            "/leading",
            "a//b",
            "x/../y",
            &"k".repeat(1025),
            &"s".repeat(256),
            "baddir.",
            "a:b",
            "NUL.txt",
        ] {
            assert_eq!(
                format!("{:?}", check_new_object_args("test-bucket", object, false)),
                format!("{:?}", check_put_object_args("test-bucket", object)),
                "{object:?}"
            );
        }
        assert!(matches!(
            check_new_object_args("Bad_Bucket", "file.txt", false),
            Err(StorageError::BucketNameInvalid(_))
        ));
    }

    #[test]
    #[cfg(not(target_os = "windows"))]
    fn test_check_new_object_args_keeps_full_key_set_off_windows() {
        for object in ["a\\b", "a:b", "baddir.", "NUL", "CONIN$", "tab\tname"] {
            assert!(check_new_object_args("test-bucket", object, false).is_ok(), "{object:?}");
        }
    }

    /// `a\b` and `a/b` resolve to the same file on a Windows disk, so a new
    /// `a\b` would overwrite (or be overwritten by) `a/b`.
    #[test]
    #[cfg(target_os = "windows")]
    fn windows_new_object_key_rejects_backslash_alias() {
        assert!(check_new_object_args("test-bucket", "a/b", false).is_ok());
        for object in ["a\\b", "dir\\", "dir/sub\\file"] {
            assert!(
                matches!(
                    &check_new_object_args("test-bucket", object, false),
                    Err(StorageError::InvalidArgument(_, _, reason)) if reason == WINDOWS_BACKSLASH_REASON
                ),
                "{object:?}"
            );
        }
        // Reads, deletes and in-flight multipart uploads of existing keys keep working.
        assert!(check_object_args("test-bucket", "a\\b").is_ok());
        assert!(check_del_obj_args("test-bucket", "a\\b").is_ok());
        assert!(check_new_object_args("test-bucket", "a\\b", true).is_ok());
    }

    #[test]
    #[cfg(target_os = "windows")]
    fn windows_new_object_key_rejects_unaddressable_names() {
        for (object, expected) in [
            ("baddir.", WINDOWS_RESERVED_SEGMENT_REASON),
            ("dir/LPT9.log", WINDOWS_RESERVED_SEGMENT_REASON),
            ("COM\u{b9}", WINDOWS_RESERVED_SEGMENT_REASON),
            ("CONOUT$", WINDOWS_RESERVED_SEGMENT_REASON),
            ("a:b", WINDOWS_RESERVED_CHARACTERS_REASON),
            ("tab\tname", WINDOWS_RESERVED_CHARACTERS_REASON),
            ("esc\u{1b}", WINDOWS_RESERVED_CHARACTERS_REASON),
        ] {
            for result in [
                check_object_name_for_length_and_slash("test-bucket", object),
                check_put_object_args("test-bucket", object),
                check_new_object_args("test-bucket", object, false),
                check_object_args("test-bucket", object),
            ] {
                assert!(
                    matches!(&result, Err(StorageError::InvalidArgument(_, _, reason)) if reason == expected),
                    "{object:?} must be rejected with {expected:?}, got {result:?}"
                );
            }
        }
        // NUL keeps the platform-independent answer.
        assert!(matches!(
            check_put_object_args("test-bucket", "a\0b"),
            Err(StorageError::ObjectNameInvalid(_, _))
        ));
    }

    #[test]
    fn test_check_bucket_and_object_names() {
        // Valid names
        assert!(check_bucket_and_object_names("valid-bucket", "valid-object").is_ok());

        // Invalid bucket names
        assert!(check_bucket_and_object_names("", "valid-object").is_err());
        assert!(check_bucket_and_object_names("INVALID", "valid-object").is_err());

        // Invalid object names
        assert!(check_bucket_and_object_names("valid-bucket", "").is_err());
    }

    #[test]
    fn test_check_bucket_name_rejects_leading_and_trailing_whitespace() {
        for bucket in [
            " valid-bucket",
            "valid-bucket ",
            "valid-bucket\n",
            "valid-bucket\u{b}",
            "\u{c}valid-bucket\u{c}",
        ] {
            assert!(
                check_valid_bucket_name_strict(bucket).is_err(),
                "bucket name with leading or trailing whitespace must be rejected: {bucket:?}"
            );
            assert!(
                check_valid_bucket_name(bucket).is_err(),
                "legacy bucket validation must reject leading or trailing whitespace: {bucket:?}"
            );
        }
    }

    #[test]
    fn test_check_list_objs_args() {
        assert!(check_list_objs_args("valid-bucket", "", &None).is_ok());
        assert!(check_list_objs_args("", "", &None).is_err());
        assert!(check_list_objs_args("INVALID", "", &None).is_err());
    }

    #[test]
    fn test_list_multipart_upload_marker_is_ignored_without_key_marker() {
        assert!(check_list_multipart_args("valid-bucket", "", &None, &Some("not-base64!".to_string()), &None,).is_ok());
    }

    #[test]
    fn test_check_multipart_args() {
        assert!(check_new_multipart_args("valid-bucket", "valid-object").is_ok());
        assert!(check_new_multipart_args("", "valid-object").is_err());
        assert!(check_new_multipart_args("valid-bucket", "").is_err());

        // Use valid base64 encoded upload_id
        let valid_upload_id = "dXBsb2FkLWlk"; // base64 encoded "upload-id"
        assert!(check_multipart_object_args("valid-bucket", "valid-object", valid_upload_id).is_ok());
        assert!(check_multipart_object_args("", "valid-object", valid_upload_id).is_err());
        assert!(check_multipart_object_args("valid-bucket", "", valid_upload_id).is_err());
        // Empty string is valid base64 (decodes to empty vec), so this should pass bucket/object validation
        // but fail on empty upload_id check in the function logic
        assert!(check_multipart_object_args("valid-bucket", "valid-object", "").is_ok());
        assert!(check_multipart_object_args("valid-bucket", "valid-object", "invalid-base64!").is_err());
    }

    #[test]
    fn test_validation_functions_comprehensive() {
        // Test object name validation edge cases
        assert!(!is_valid_object_name(""));
        assert!(is_valid_object_name("a"));
        assert!(is_valid_object_name("test.txt"));
        assert!(is_valid_object_name("folder/file.txt"));
        assert!(is_valid_object_name("very-long-object-name-with-many-characters"));

        // Test prefix validation
        assert!(is_valid_object_prefix(""));
        assert!(is_valid_object_prefix("prefix"));
        assert!(is_valid_object_prefix("prefix/"));
        assert!(is_valid_object_prefix("deep/nested/prefix/"));
    }

    #[test]
    fn test_argument_validation_comprehensive() {
        // Test bucket and object name validation
        assert!(check_bucket_and_object_names("test-bucket", "test-object").is_ok());
        assert!(check_bucket_and_object_names("test-bucket", "folder/test-object").is_ok());

        // Test list objects arguments
        assert!(check_list_objs_args("test-bucket", "prefix", &Some("marker".to_string())).is_ok());
        assert!(check_list_objs_args("test-bucket", "", &None).is_ok());

        // Test multipart upload arguments with valid base64 upload_id
        let valid_upload_id = "dXBsb2FkLWlk"; // base64 encoded "upload-id"
        assert!(check_put_object_part_args("test-bucket", "test-object", valid_upload_id).is_ok());
        assert!(check_list_parts_args("test-bucket", "test-object", valid_upload_id).is_ok());
        assert!(check_complete_multipart_args("test-bucket", "test-object", valid_upload_id).is_ok());
        assert!(check_abort_multipart_args("test-bucket", "test-object", valid_upload_id).is_ok());

        // Test put object arguments
        assert!(check_put_object_args("test-bucket", "test-object").is_ok());
        assert!(check_put_object_args("", "test-object").is_err());
        assert!(check_put_object_args("test-bucket", "").is_err());
    }

    proptest! {
        #[test]
        fn valid_object_prefixes_preserve_prefix_invariants(input in any::<String>()) {
            if is_valid_object_prefix(&input) {
                prop_assert!(!has_bad_path_component(&input));
                prop_assert!(!input.contains("//"));
                prop_assert!(!input.contains('\0'));
            }
        }

        #[test]
        fn valid_object_names_preserve_name_invariants(input in any::<String>()) {
            if is_valid_object_name(&input) {
                prop_assert!(!input.is_empty());
                prop_assert!(is_valid_object_prefix(&input));
            }
        }

        #[test]
        fn object_name_validity_matches_prefix_validity_plus_non_empty(input in any::<String>()) {
            prop_assert_eq!(is_valid_object_name(&input), !input.is_empty() && is_valid_object_prefix(&input));
        }
    }
}
