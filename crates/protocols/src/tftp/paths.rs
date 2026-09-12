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

//! TFTP filename to S3 bucket/key resolution with unified validation.

use async_tftp::packet::Error as TftpPacketError;
use rustfs_utils::path;
use std::path::Path;

/// Resolve a TFTP filename into `(bucket, object_key)`.
///
/// When `default_bucket` is set, the filename becomes the object key under
/// that bucket. Otherwise the path is parsed as `/<bucket>/<key>`.
/// Both modes run the same key validation before returning.
pub fn resolve_object_path(filename: &Path, default_bucket: Option<&str>) -> Result<(String, String), TftpPacketError> {
    let raw = filename.to_string_lossy();
    let trimmed = raw.trim_start_matches('/');

    if trimmed.is_empty() {
        return Err(TftpPacketError::FileNotFound);
    }

    let (bucket, key) = match default_bucket {
        Some(bucket) => (bucket.to_string(), trimmed.to_string()),
        None => {
            let cleaned = path::clean(&format!("/{trimmed}"));
            if cleaned == "." || cleaned == ".." || cleaned.starts_with("../") {
                return Err(TftpPacketError::FileNotFound);
            }
            let (bucket, object) = path::path_to_bucket_object(&cleaned);
            if bucket.is_empty() || object.is_empty() {
                return Err(TftpPacketError::FileNotFound);
            }
            (bucket, object)
        }
    };

    validate_object_key(&key)?;
    Ok((bucket, key))
}

/// Shared validation for object keys in both bucket-resolution modes.
fn validate_object_key(key: &str) -> Result<(), TftpPacketError> {
    if key.contains(['\0', '\r', '\n']) {
        return Err(TftpPacketError::IllegalOperation);
    }
    if key.contains(path::GLOBAL_DIR_SUFFIX) {
        return Err(TftpPacketError::IllegalOperation);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn default_bucket_uses_filename_as_key() {
        let (bucket, key) = resolve_object_path(Path::new("pxelinux.0"), Some("pxe")).expect("resolve");
        assert_eq!(bucket, "pxe");
        assert_eq!(key, "pxelinux.0");
    }

    #[test]
    fn default_bucket_rejects_control_chars() {
        let err = resolve_object_path(Path::new("bad\0name"), Some("pxe")).expect_err("NUL rejected");
        assert!(matches!(err, TftpPacketError::IllegalOperation));
    }

    #[test]
    fn default_bucket_rejects_xldir_marker() {
        let err = resolve_object_path(Path::new("foo__XLDIR__"), Some("pxe")).expect_err("marker rejected");
        assert!(matches!(err, TftpPacketError::IllegalOperation));
    }

    #[test]
    fn bucket_key_mode_parses_path() {
        let (bucket, key) = resolve_object_path(Path::new("/mybucket/boot/vmlinuz"), None).expect("resolve");
        assert_eq!(bucket, "mybucket");
        assert_eq!(key, "boot/vmlinuz");
    }

    #[test]
    fn bucket_key_mode_rejects_traversal() {
        let err = resolve_object_path(Path::new("/../secret"), None).expect_err("traversal rejected");
        assert!(matches!(err, TftpPacketError::FileNotFound));
    }

    #[test]
    fn empty_filename_rejected() {
        let err = resolve_object_path(Path::new(""), Some("pxe")).expect_err("empty rejected");
        assert!(matches!(err, TftpPacketError::FileNotFound));
    }
}
