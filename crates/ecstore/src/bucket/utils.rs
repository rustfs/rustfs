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
use rustfs_utils::WindowsFileNameError;
use rustfs_utils::path::SLASH_SEPARATOR;
use s3s::xml;
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

// RUSTFS_COMPAT_TODO(s3gate-metadata-xml): the s3s codec reads persisted XML during migration. Remove after every supported writer uses the gateway codec and every retained metadata object and backup archive is verified or rewritten.
pub fn deserialize<T>(input: &[u8]) -> xml::DeResult<T>
where
    T: for<'xml> xml::Deserialize<'xml>,
{
    let mut d = xml::Deserializer::new(input);
    let ans = T::deserialize(&mut d)?;
    d.expect_eof()?;
    Ok(ans)
}

#[allow(
    dead_code,
    reason = "xml serialize helper with no caller in this port; the live sibling is deserialize (backlog#1823)"
)]
pub fn serialize_content<T: xml::SerializeContent>(val: &T) -> xml::SerResult<String> {
    let mut buf = Vec::with_capacity(256);
    {
        let mut ser = xml::Serializer::new(&mut buf);
        val.serialize_content(&mut ser)?;
    }
    Ok(String::from_utf8(buf).unwrap())
}

pub fn serialize<T: xml::Serialize>(val: &T) -> xml::SerResult<Vec<u8>> {
    let mut buf = Vec::with_capacity(256);
    {
        let mut ser = xml::Serializer::new(&mut buf);
        val.serialize(&mut ser)?;
    }
    Ok(buf)
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

/// Returns the client-facing reason when `object` cannot round-trip through
/// the Win32 path layer, or `None` when it can.
///
/// Each segment is checked with [`rustfs_utils::check_windows_file_name`].
/// `\` splits segments here, like on a Windows disk.
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
pub fn check_new_object_args(bucket: &str, object: &str) -> Result<()> {
    check_put_object_args(bucket, object)
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
                format!("{:?}", check_new_object_args("test-bucket", object)),
                format!("{:?}", check_put_object_args("test-bucket", object)),
                "{object:?}"
            );
        }
        assert!(matches!(
            check_new_object_args("Bad_Bucket", "file.txt"),
            Err(StorageError::BucketNameInvalid(_))
        ));
    }

    #[test]
    #[cfg(not(target_os = "windows"))]
    fn test_check_new_object_args_keeps_full_key_set_off_windows() {
        for object in ["a:b", "baddir.", "NUL", "CONIN$", "tab\tname"] {
            assert!(check_new_object_args("test-bucket", object).is_ok(), "{object:?}");
        }
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
                check_new_object_args("test-bucket", object),
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
