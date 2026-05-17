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

//! Attribute helpers and the do_stat dispatcher behind STAT, LSTAT, and
//! FSTAT. The free functions are pure conversions; the do_stat method
//! sits on SftpDriver and runs the bucket/object branching.

use super::constants::posix::{POSIX_DIR_MODE, POSIX_FILE_MODE};
use super::driver::SftpDriver;
use super::errors::{SftpError, is_not_found_error, s3_error_to_sftp};
use super::paths::parse_s3_path;
use crate::common::client::s3::StorageBackend;
use crate::common::gateway::S3Action;
use russh_sftp::protocol::{File, FileAttributes, StatusCode};
use s3s::dto::ListObjectsV2Input;
use std::collections::HashMap;

const SFTP_META_MTIME: &str = "mtime";
const SFTP_META_MODE: &str = "mode";
const SFTP_META_UID: &str = "uid";
const SFTP_META_GID: &str = "gid";

/// Build the SFTP FileAttributes struct returned by STAT, LSTAT, and
/// FSTAT. Callers are responsible for any clamping or conversion of the
/// mtime field. See timestamp_to_mtime for the conversion used when the
/// source is an s3s Timestamp.
pub(super) fn s3_attrs_to_sftp(size: u64, mtime: Option<u32>, is_dir: bool) -> FileAttributes {
    let permissions = if is_dir { POSIX_DIR_MODE } else { POSIX_FILE_MODE };
    FileAttributes {
        size: Some(if is_dir { 0 } else { size }),
        uid: Some(0),
        gid: Some(0),
        user: None,
        group: None,
        permissions: Some(permissions),
        atime: mtime,
        mtime,
    }
}

fn parse_u32_metadata(metadata: &HashMap<String, String>, key: &str) -> Option<u32> {
    metadata.get(key).and_then(|value| value.parse::<u32>().ok())
}

pub(super) fn sftp_attrs_to_user_metadata(attrs: &FileAttributes) -> Option<HashMap<String, String>> {
    let mut metadata = HashMap::new();
    if let Some(mtime) = attrs.mtime {
        metadata.insert(SFTP_META_MTIME.to_string(), mtime.to_string());
    }
    if let Some(mode) = attrs.permissions {
        metadata.insert(SFTP_META_MODE.to_string(), mode.to_string());
    }
    if let Some(uid) = attrs.uid {
        metadata.insert(SFTP_META_UID.to_string(), uid.to_string());
    }
    if let Some(gid) = attrs.gid {
        metadata.insert(SFTP_META_GID.to_string(), gid.to_string());
    }

    if metadata.is_empty() { None } else { Some(metadata) }
}

pub(super) fn apply_user_metadata_to_sftp_attrs(attrs: &mut FileAttributes, metadata: &HashMap<String, String>) {
    if let Some(mtime) = parse_u32_metadata(metadata, SFTP_META_MTIME) {
        attrs.mtime = Some(mtime);
        attrs.atime = Some(mtime);
    }
    if let Some(mode) = parse_u32_metadata(metadata, SFTP_META_MODE) {
        attrs.permissions = Some(mode);
    }
    if let Some(uid) = parse_u32_metadata(metadata, SFTP_META_UID) {
        attrs.uid = Some(uid);
    }
    if let Some(gid) = parse_u32_metadata(metadata, SFTP_META_GID) {
        attrs.gid = Some(gid);
    }
}

/// Convert an s3s Timestamp into the u32 seconds field SFTPv3 expects.
/// Pre-1970 values clamp to 0. Post-2106 values clamp to u32::MAX. The
/// clamps prevent the i64-to-u32 cast from wrapping.
pub(super) fn timestamp_to_mtime(ts: Option<s3s::dto::Timestamp>) -> Option<u32> {
    ts.map(|t| {
        let odt: time::OffsetDateTime = t.into();
        let secs = odt.unix_timestamp().clamp(0, u32::MAX as i64);
        secs as u32
    })
}

/// Build the ls -l style longname string for a directory entry. Delegates
/// to File::new in russh_sftp, which formats the line from the attributes
/// (type prefix "d" or "-", permission triple, size, timestamp). The
/// filename is sanitised before composition so a key containing CR or LF
/// cannot inject a forged second entry in clients that split longname
/// output on newline.
pub(super) fn generate_longname(filename: &str, attrs: &FileAttributes) -> String {
    let safe = super::paths::sanitise_control_bytes(filename);
    File::new(safe.as_ref(), attrs.clone()).longname
}

impl<S: StorageBackend + Send + Sync + 'static> SftpDriver<S> {
    /// Resolve the attributes for raw_path. STAT and LSTAT both call do_stat
    /// because the SFTP server has no symlink concept (S3 has no symlinks).
    /// Root yields default directory attrs without a network call.
    ///
    /// Bucket paths run authorize_operation(HeadBucket) followed by a
    /// HeadBucket call. Success yields default directory attributes
    /// (HeadBucket exposes neither size nor mtime).
    ///
    /// Object paths run authorize_operation(HeadObject) followed by a
    /// HeadObject call. Success yields file attributes built from
    /// content_length (clamped non-negative) and last_modified (clamped to
    /// the u32 range).
    pub(super) async fn do_stat(&self, raw_path: &str) -> Result<FileAttributes, SftpError> {
        let (bucket, key) = parse_s3_path(raw_path)?;

        if bucket.is_empty() {
            // Root. Every authenticated principal sees root as a directory.
            return Ok(s3_attrs_to_sftp(0, None, true));
        }

        match key {
            // Bucket-level path: input resolved to a bucket with no object
            // component. HeadBucket returns 200 on existence or a backend
            // error mapped by s3_error_to_sftp. Default directory attrs
            // on success. Size and mtime are not returned by HeadBucket.
            None => {
                self.authorize(&S3Action::HeadBucket, &bucket, None).await?;
                self.run_backend("head_bucket", self.storage.head_bucket(&bucket, self.access_key(), self.secret_key()))
                    .await?;
                Ok(s3_attrs_to_sftp(0, None, true))
            }
            // Object path: try HeadObject first (the path may be a file).
            // If HeadObject returns not-found, fall back to a directory
            // check: list with prefix "{key}/" and max_keys=1. If any
            // content or sub-prefix exists, this path is a directory and
            // gets default directory attrs. S3 has no first-class
            // directories, so both explicit markers (__XLDIR__) and
            // implicit prefixes (objects exist under the prefix) must be
            // detected. Without this fallback, sftp clients that STAT
            // before OPENDIR (OpenSSH, FileZilla) fail to list
            // sub-directories.
            Some(object_key) => {
                self.authorize(&S3Action::HeadObject, &bucket, Some(&object_key)).await?;
                match self
                    .run_backend_with_err(
                        "head_object",
                        self.storage
                            .head_object(&bucket, &object_key, self.access_key(), self.secret_key()),
                    )
                    .await?
                {
                    Ok(out) => {
                        let size = out.content_length.unwrap_or(0).max(0) as u64;
                        let mtime = timestamp_to_mtime(out.last_modified);
                        let mut attrs = s3_attrs_to_sftp(size, mtime, false);
                        if let Some(metadata) = out.metadata {
                            apply_user_metadata_to_sftp_attrs(&mut attrs, &metadata);
                        }
                        Ok(attrs)
                    }
                    Err(e) if is_not_found_error(&e) => {
                        // No object at this key. Check whether it is a
                        // directory by listing with the key as a prefix.
                        let prefix = format!("{object_key}/");
                        self.authorize(&S3Action::ListBucket, &bucket, Some(prefix.as_str())).await?;
                        let input = ListObjectsV2Input::builder()
                            .bucket(bucket.clone())
                            .prefix(Some(prefix))
                            .delimiter(Some("/".to_string()))
                            .max_keys(Some(1))
                            .build()
                            .map_err(|e| s3_error_to_sftp("build_list_objects", e))?;
                        let out = self
                            .run_backend(
                                "list_objects_v2",
                                self.storage.list_objects_v2(input, self.access_key(), self.secret_key()),
                            )
                            .await?;

                        let has_contents = out.contents.map(|c| !c.is_empty()).unwrap_or(false);
                        let has_prefixes = out.common_prefixes.map(|c| !c.is_empty()).unwrap_or(false);
                        if has_contents || has_prefixes {
                            Ok(s3_attrs_to_sftp(0, None, true))
                        } else {
                            tracing::debug!(
                                bucket = %bucket,
                                key = %object_key,
                                "STAT fallback: HeadObject not-found and list returned no contents or prefixes. Returning NoSuchFile",
                            );
                            Err(SftpError::code(StatusCode::NoSuchFile))
                        }
                    }
                    Err(e) => Err(s3_error_to_sftp("head_object", e)),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sftp::constants::posix::POSIX_TYPE_MASK;

    #[test]
    fn s3_attrs_to_sftp_directory_has_dir_type_bit() {
        use crate::constants::paths::{DIR_MODE, DIR_PERMISSIONS};
        let attrs = s3_attrs_to_sftp(0, None, true);
        let mode = attrs.permissions.unwrap();
        assert_eq!(mode & POSIX_TYPE_MASK, DIR_MODE, "S_IFDIR bit must be set");
        assert_eq!(mode & 0o777, DIR_PERMISSIONS);
        assert!(attrs.is_dir());
    }

    #[test]
    fn s3_attrs_to_sftp_file_has_regular_type_bit() {
        use crate::constants::paths::{FILE_MODE, FILE_PERMISSIONS};
        let attrs = s3_attrs_to_sftp(42, Some(1_700_000_000), false);
        let mode = attrs.permissions.unwrap();
        assert_eq!(mode & POSIX_TYPE_MASK, FILE_MODE, "S_IFREG bit must be set");
        assert_eq!(mode & 0o777, FILE_PERMISSIONS);
        assert_eq!(attrs.size, Some(42));
        assert_eq!(attrs.mtime, Some(1_700_000_000));
        assert!(attrs.is_regular());
    }

    #[test]
    fn sftp_attrs_to_user_metadata_maps_only_present_open_attrs() {
        let attrs = FileAttributes {
            size: None,
            uid: Some(1000),
            gid: Some(1001),
            user: None,
            group: None,
            permissions: Some(0o100640),
            atime: None,
            mtime: Some(1_777_992_333),
        };

        let metadata = sftp_attrs_to_user_metadata(&attrs).expect("present attrs produce metadata");
        assert_eq!(metadata.get("mtime").map(String::as_str), Some("1777992333"));
        assert_eq!(metadata.get("mode").map(String::as_str), Some("33184"));
        assert_eq!(metadata.get("uid").map(String::as_str), Some("1000"));
        assert_eq!(metadata.get("gid").map(String::as_str), Some("1001"));
        assert!(!metadata.contains_key("size"), "object size is data-path state, not OPEN metadata");
    }

    #[test]
    fn apply_user_metadata_to_sftp_attrs_overrides_defaults() {
        let mut attrs = s3_attrs_to_sftp(42, Some(10), false);
        let metadata = HashMap::from([
            ("mtime".to_string(), "1777992348".to_string()),
            ("mode".to_string(), "33152".to_string()),
            ("uid".to_string(), "501".to_string()),
            ("gid".to_string(), "20".to_string()),
        ]);

        apply_user_metadata_to_sftp_attrs(&mut attrs, &metadata);
        assert_eq!(attrs.mtime, Some(1_777_992_348));
        assert_eq!(attrs.atime, Some(1_777_992_348));
        assert_eq!(attrs.permissions, Some(0o100600));
        assert_eq!(attrs.uid, Some(501));
        assert_eq!(attrs.gid, Some(20));
    }

    #[test]
    fn generate_longname_prefixes_d_for_directory() {
        let attrs = s3_attrs_to_sftp(0, Some(0), true);
        let line = generate_longname("mybucket", &attrs);
        assert!(line.starts_with('d'), "dir longname must start with d, got {line}");
    }

    #[test]
    fn generate_longname_prefixes_dash_for_file() {
        let attrs = s3_attrs_to_sftp(100, Some(0), false);
        let line = generate_longname("file.txt", &attrs);
        assert!(line.starts_with('-'), "file longname must start with -, got {line}");
    }

    #[test]
    fn generate_longname_strips_lf_in_filename() {
        let attrs = s3_attrs_to_sftp(100, Some(0), false);
        let line = generate_longname("evil\nfile.txt", &attrs);
        assert!(!line.contains('\n'), "longname must not contain raw LF, got {line:?}");
        assert!(
            line.contains("evil?file.txt"),
            "longname must include the sanitised filename, got {line:?}"
        );
    }

    #[test]
    fn timestamp_conversion_handles_none() {
        assert_eq!(timestamp_to_mtime(None), None);
    }

    #[test]
    fn timestamp_to_mtime_clamps_negative_to_zero() {
        let pre_epoch = s3s::dto::Timestamp::from(time::OffsetDateTime::from_unix_timestamp(-86400).expect("valid timestamp"));
        assert_eq!(timestamp_to_mtime(Some(pre_epoch)), Some(0));
    }

    #[test]
    fn timestamp_to_mtime_clamps_overflow_to_u32_max() {
        let far_future = s3s::dto::Timestamp::from(
            time::OffsetDateTime::from_unix_timestamp(u32::MAX as i64 + 86400).expect("valid timestamp"),
        );
        assert_eq!(timestamp_to_mtime(Some(far_future)), Some(u32::MAX));
    }

    #[test]
    fn posix_mode_constants_match_documented_values() {
        assert_eq!(POSIX_DIR_MODE, 0o040755);
        assert_eq!(POSIX_FILE_MODE, 0o100644);
        assert_eq!(POSIX_TYPE_MASK, 0o170000);
    }
}
