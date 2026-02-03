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

use crate::disk::RUSTFS_META_BUCKET;
use crate::error::{Error, Result, StorageError};
use regex::Regex;
use rustfs_utils::path::SLASH_SEPARATOR;
use s3s::xml;
use tracing::instrument;

pub fn is_meta_bucketname(name: &str) -> bool {
    name.starts_with(RUSTFS_META_BUCKET)
}

lazy_static::lazy_static! {
    static ref VALID_BUCKET_NAME: Regex = Regex::new(r"^[A-Za-z0-9][A-Za-z0-9\.\-\_\:]{1,61}[A-Za-z0-9]$").unwrap();
    static ref VALID_BUCKET_NAME_STRICT: Regex = Regex::new(r"^[a-z0-9][a-z0-9\.\-]{1,61}[a-z0-9]$").unwrap();
    static ref IP_ADDRESS: Regex = Regex::new(r"^(\d+\.){3}\d+$").unwrap();
}

pub fn check_bucket_name_common(bucket_name: &str, strict: bool) -> Result<()> {
    let bucket_name_trimmed = bucket_name.trim();

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

pub fn check_valid_object_name_prefix(object_name: &str) -> Result<()> {
    if object_name.len() > 1024 {
        return Err(Error::other("Object name cannot be longer than 1024 characters"));
    }
    if !object_name.is_ascii() {
        return Err(Error::other("Object name with non-UTF-8 strings are not supported"));
    }
    Ok(())
}

pub fn check_valid_object_name(object_name: &str) -> Result<()> {
    if object_name.trim().is_empty() {
        return Err(Error::other("Object name cannot be empty"));
    }
    check_valid_object_name_prefix(object_name)
}

pub fn deserialize<T>(input: &[u8]) -> xml::DeResult<T>
where
    T: for<'xml> xml::Deserialize<'xml>,
{
    let mut d = xml::Deserializer::new(input);
    let ans = T::deserialize(&mut d)?;
    d.expect_eof()?;
    Ok(ans)
}

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

        // Trim whitespace of segment
        let mut segment_start = start;
        let mut segment_end = i;

        while segment_start < segment_end && bytes[segment_start].is_ascii_whitespace() {
            segment_start += 1;
        }
        while segment_end > segment_start && bytes[segment_end - 1].is_ascii_whitespace() {
            segment_end -= 1;
        }

        // Check for ".." or "."
        match segment_end - segment_start {
            2 if segment_start + 1 < n && bytes[segment_start] == b'.' && bytes[segment_start + 1] == b'.' => {
                return true;
            }
            1 if bytes[segment_start] == b'.' => {
                return true;
            }
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

    if object.ends_with(SLASH_SEPARATOR) {
        return false;
    }

    is_valid_object_prefix(object)
}

pub fn check_object_name_for_length_and_slash(bucket: &str, object: &str) -> Result<()> {
    if object.len() > 1024 {
        return Err(StorageError::ObjectNameTooLong(bucket.to_owned(), object.to_owned()));
    }

    if object.starts_with(SLASH_SEPARATOR) {
        return Err(StorageError::ObjectNamePrefixAsSlash(bucket.to_owned(), object.to_owned()));
    }

    #[cfg(target_os = "windows")]
    {
        if object.contains(':')
            || object.contains('*')
            || object.contains('?')
            || object.contains('"')
            || object.contains('|')
            || object.contains('<')
            || object.contains('>')
        // || object.contains('\\')
        {
            return Err(StorageError::ObjectNameInvalid(bucket.to_owned(), object.to_owned()));
        }
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

    // if cfg!(target_os = "windows") && object.contains('\\') {
    //     return Err(StorageError::ObjectNameInvalid(bucket.to_string(), object.to_string()));
    // }

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

    if let Some(upload_id_marker) = upload_id_marker {
        if let Some(key_marker) = key_marker
            && key_marker.ends_with('/')
        {
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

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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

        // Invalid cases - ends with slash (object names cannot end with slash)
        assert!(!is_valid_object_name("object/"));
        assert!(!is_valid_object_name("path/to/file/"));
        assert!(!is_valid_object_name("ends/with/slash/"));

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
    fn test_check_list_objs_args() {
        assert!(check_list_objs_args("valid-bucket", "", &None).is_ok());
        assert!(check_list_objs_args("", "", &None).is_err());
        assert!(check_list_objs_args("INVALID", "", &None).is_err());
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
}
