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

//! Directory iteration and the bucket/sub-directory mkdir/rmdir
//! helpers. Drives the cursor walks and emptiness checks that the
//! Handler trait's opendir/readdir/mkdir/rmdir methods consume.

use super::attrs::{generate_longname, s3_attrs_to_sftp, timestamp_to_mtime};
use super::constants::limits::{READDIR_PAGE_MAX_KEYS, ROOT_LISTING_MAX_ENTRIES};
use super::driver::SftpDriver;
use super::errors::{SftpError, s3_error_to_sftp};
use super::paths::{last_path_component, parse_s3_path, relative_filename};
use super::state::{DirCursor, HandleState, ListingContinuation};
use crate::common::client::s3::StorageBackend;
use crate::common::gateway::S3Action;
use bytes::Bytes;
use russh_sftp::protocol::{File, Handle, Name, StatusCode};
use rustfs_gateway_stream::ByteStream;
use rustfs_gateway_types::{
    BucketName, ObjectKey, OpaqueString,
    dto::{ListObjectsV2Input, PutObjectInput},
};
use rustfs_utils::path;

const LOG_COMPONENT_PROTOCOLS: &str = "protocols";
const LOG_SUBSYSTEM_SFTP_DIR: &str = "sftp_dir";
const EVENT_SFTP_DIR_STATE: &str = "sftp_dir_state";

/// Build the conventional "." and ".." directory entries that prefix
/// the first READDIR response on every directory handle. SFTPv3 does
/// not mandate these, but POSIX clients require them. Emitting both
/// keeps the directory listing compatible with OpenSSH sftp,
/// FileZilla, and WinSCP. Both are returned as directories so clients
/// render ".." as the up-navigation shortcut.
pub(super) fn dot_entries() -> Vec<File> {
    let attrs = s3_attrs_to_sftp(0, None, true);
    vec![
        File {
            filename: ".".to_string(),
            longname: generate_longname(".", &attrs),
            attrs: attrs.clone(),
        },
        File {
            filename: "..".to_string(),
            longname: generate_longname("..", &attrs),
            attrs,
        },
    ]
}

fn directory_listing_input(
    bucket: &str,
    prefix: &str,
    continuation: &ListingContinuation,
) -> Result<ListObjectsV2Input, SftpError> {
    let mut builder = ListObjectsV2Input::builder()
        .bucket(BucketName::new(bucket.to_owned()).map_err(|e| s3_error_to_sftp("build_list_objects", e))?)
        .prefix(prefix.to_owned())
        .delimiter("/".to_owned())
        .max_keys(READDIR_PAGE_MAX_KEYS);
    if let ListingContinuation::Next(token) = continuation {
        builder = builder.continuation_token(OpaqueString::from(token.clone()));
    }
    Ok(builder.build())
}

impl<S: StorageBackend + Send + Sync + 'static> SftpDriver<S> {
    /// Fetch one S3 ListObjectsV2 page for a Listing cursor, convert it to
    /// File entries (subdirectories from common_prefixes, objects from
    /// contents), and advance the cursor's continuation state for the next
    /// call. Caller passes the cursor by mutable reference. The helper
    /// updates the embedded continuation token in place.
    ///
    /// Returns an empty Vec when the cursor is already Done. Returns
    /// StatusCode::Failure if called with a Root cursor. Callers must
    /// route Root to fetch_bucket_list instead.
    pub(super) async fn next_listing_page(&self, cursor: &mut DirCursor) -> Result<Vec<File>, SftpError> {
        let DirCursor::Listing {
            bucket,
            prefix,
            continuation,
            ..
        } = cursor
        else {
            return Err(SftpError::code(StatusCode::Failure));
        };

        // Cursor already exhausted by a prior page. No network round trip.
        // The empty return signals the caller's EOF translation on the next
        // READDIR.
        if matches!(continuation, ListingContinuation::Done) {
            return Ok(Vec::new());
        }

        // Re-authorise ListBucket on every page rather than relying on
        // the OPENDIR-time check. S3 evaluates policy once per
        // list_objects_v2 wire call. Matching that means a policy
        // revoked mid-iteration takes effect on the next page rather
        // than at session end.
        let input = directory_listing_input(bucket, prefix, continuation)?;
        self.authorize_list_objects(&input).await?;

        let out = self
            .run_backend("list_objects_v2", self.storage.list_objects_v2(input, self.credentials()))
            .await?;

        let mut entries = Vec::new();

        // common_prefixes contains subdirectory entries produced by the
        // delimiter="/" split. Each prefix ends with "/".
        // last_path_component returns the final component, or None if
        // the prefix has no component (e.g. "/" on its own).
        for cp in out.common_prefixes {
            let p = cp.prefix;
            let Some(name) = last_path_component(&p) else { continue };
            let attrs = s3_attrs_to_sftp(0, None, true);
            entries.push(File {
                filename: name.to_string(),
                longname: generate_longname(name, &attrs),
                attrs,
            });
        }

        // contents holds object entries at the current level. __XLDIR__
        // marker objects are excluded. relative_filename returns None
        // for entries whose key contains a "/" after the prefix (those
        // belong under a sub-prefix and would have appeared via
        // common_prefixes).
        for obj in out.contents {
            let full_key = obj.key;
            if full_key.as_str().ends_with(path::GLOBAL_DIR_SUFFIX) {
                continue;
            }
            let Some(name) = relative_filename(full_key.as_str(), prefix.as_str()) else { continue };
            let size = obj.size.max(0) as u64;
            let mtime = timestamp_to_mtime(Some(obj.last_modified));
            let attrs = s3_attrs_to_sftp(size, mtime, false);
            entries.push(File {
                filename: name.to_string(),
                longname: generate_longname(name, &attrs),
                attrs,
            });
        }

        // Advance the continuation cursor. is_truncated without a token is
        // a backend inconsistency. Handle as Done rather than risk looping
        // forever on an absent token.
        *continuation = match (out.is_truncated, out.next_continuation_token) {
            (true, Some(token)) => ListingContinuation::Next(token.into_string()),
            _ => ListingContinuation::Done,
        };

        Ok(entries)
    }

    /// Return Err when the RMDIR target still has objects or
    /// sub-prefixes. The check authorises ListBucket, then issues a
    /// single list_objects_v2 capped at one entry: presence of any
    /// contents or common_prefixes blocks the deletion. The empty
    /// input prefix addresses a whole bucket. A non-empty prefix
    /// addresses a sub-directory.
    ///
    /// A list_objects_v2 failure aborts the operation. The caller
    /// must not fall through to a destructive call when this returns
    /// Err.
    pub(super) async fn validate_directory_empty(&self, bucket: &str, prefix: &str) -> Result<(), SftpError> {
        // For sub-directory prefixes, max_keys=2 because the backend
        // may return the directory's own __XLDIR__ marker (decoded to
        // the prefix itself, e.g. "subdir/") as a content entry.
        // max_keys=2 ensures the listing returns one entry past the
        // marker so real content is visible. For bucket-level checks
        // (prefix is empty) max_keys=1 is sufficient since there is no
        // marker to filter.
        let max_keys = if prefix.is_empty() { 1 } else { 2 };
        let mut builder = ListObjectsV2Input::builder()
            .bucket(BucketName::new(bucket.to_string()).map_err(|e| s3_error_to_sftp("build_list_objects", e))?)
            .delimiter("/".to_string())
            .max_keys(max_keys);
        if !prefix.is_empty() {
            builder = builder.prefix(prefix.to_string());
        }
        let input = builder.build();

        self.authorize_list_objects(&input).await?;

        // Issue list_objects_v2. On Err the destructive caller never
        // runs because validate_directory_empty returns the Err.
        let out = self
            .run_backend("list_objects_v2", self.storage.list_objects_v2(input, self.credentials()))
            .await?;

        // Count content entries that are not the directory's own marker.
        // The RustFS ecfs backend decodes __XLDIR__ markers back to
        // trailing-slash keys in list responses, so the marker for
        // "subdir/" appears as a content entry with key "subdir/". That
        // entry must not count as content when checking emptiness.
        let real_content_count = out.contents.iter().filter(|obj| obj.key.as_str() != prefix).count();
        let has_prefixes = !out.common_prefixes.is_empty();
        if real_content_count > 0 || has_prefixes {
            return Err(SftpError::code(StatusCode::Failure));
        }
        Ok(())
    }

    /// Authorise and issue ListBuckets, then convert the response into
    /// File entries (one per bucket the principal can see). Called lazily
    /// by readdir_cursor on the first READDIR of a Root cursor. The
    /// S3Action::ListBuckets authorisation runs here rather than at
    /// OPENDIR so a client without ListAllMyBuckets can still open the
    /// root directory handle.
    ///
    /// A principal without ListAllMyBuckets (a bucket-scoped user) gets
    /// the session-aware ListBuckets instead, which IAM filters to the
    /// buckets the principal may ListBucket or GetBucketLocation, the
    /// same as S3 ListBuckets and the WebDAV root. It carries the
    /// session's request headers and transport security, like every
    /// other SFTP authorization.
    ///
    /// ListBuckets is not batched in the S3 API. A single response
    /// carries the full set. Truncate at ROOT_LISTING_MAX_ENTRIES so a
    /// principal with many visible buckets produces a bounded Vec and
    /// does not exceed the SSH channel window with a single response.
    pub(super) async fn fetch_bucket_list(&self) -> Result<Vec<File>, SftpError> {
        let out = if self.is_authorized(&S3Action::ListBuckets, "", None).await? {
            self.run_backend("list_buckets", self.storage.list_buckets(self.credentials()))
                .await?
        } else {
            self.run_backend(
                "list_buckets",
                self.storage.list_buckets_for_session(
                    &self.session_context,
                    &self.session_context.request_headers,
                    self.session_context.secure_transport,
                ),
            )
            .await?
        };

        let mut entries = Vec::new();
        let mut truncated_at: Option<usize> = None;
        let total = out.buckets.len();
        for bucket in out.buckets {
            if entries.len() >= ROOT_LISTING_MAX_ENTRIES {
                truncated_at = Some(total);
                break;
            }
            let name = bucket.name;
            let mtime = timestamp_to_mtime(Some(bucket.creation_date));
            let attrs = s3_attrs_to_sftp(0, mtime, true);
            entries.push(File {
                filename: name.as_str().to_owned(),
                longname: generate_longname(name.as_str(), &attrs),
                attrs,
            });
        }
        if let Some(total) = truncated_at {
            tracing::warn!(
                event = EVENT_SFTP_DIR_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_SFTP_DIR,
                returned = entries.len(),
                total = total,
                cap = ROOT_LISTING_MAX_ENTRIES,
                result = "root_readdir_truncated",
                "sftp dir state changed",
            );
        }
        Ok(entries)
    }

    /// MKDIR for a bucket-level path: authorise and issue CreateBucket.
    pub(super) async fn mkdir_bucket(&self, bucket: &str) -> Result<(), SftpError> {
        self.authorize(&S3Action::CreateBucket, bucket, None).await?;
        self.run_backend("create_bucket", self.storage.create_bucket(bucket, self.credentials()))
            .await?;
        Ok(())
    }

    /// MKDIR for a sub-directory path: write a zero-byte object at
    /// encode_dir_object(prefix + "/"). The encoding maps "foo/" to
    /// "foo__XLDIR__", which matches the RustFS marker convention used
    /// by the S3, Swift, and WebDAV backends.
    pub(super) async fn mkdir_subdir_marker(&self, bucket: &str, object_key: &str) -> Result<(), SftpError> {
        let marker_key = path::encode_dir_object(&format!("{object_key}/"));
        self.authorize(&S3Action::PutObject, bucket, Some(&marker_key)).await?;

        let streaming = ByteStream::from_bytes(Bytes::new());
        let input = PutObjectInput::builder()
            .bucket(BucketName::new(bucket.to_owned()).map_err(|e| s3_error_to_sftp("build_put_object", e))?)
            .key(ObjectKey::new(marker_key.clone()).map_err(|e| s3_error_to_sftp("build_put_object", e))?)
            .content_length(0)
            .body(streaming)
            .build();
        self.run_backend("put_object", self.storage.put_object(input, self.credentials()))
            .await?;
        Ok(())
    }

    /// RMDIR for a bucket-level path: validate empty, then authorise
    /// and issue DeleteBucket.
    pub(super) async fn rmdir_bucket(&self, bucket: &str) -> Result<(), SftpError> {
        self.validate_directory_empty(bucket, "").await?;
        self.authorize(&S3Action::DeleteBucket, bucket, None).await?;
        self.run_backend("delete_bucket", self.storage.delete_bucket(bucket, self.credentials()))
            .await?;
        Ok(())
    }

    /// RMDIR for a sub-directory path: validate no objects under the
    /// prefix, then authorise and delete the __XLDIR__ marker that
    /// represents the directory.
    pub(super) async fn rmdir_subdir_marker(&self, bucket: &str, object_key: &str) -> Result<(), SftpError> {
        let prefix = format!("{object_key}/");
        self.validate_directory_empty(bucket, &prefix).await?;

        let marker_key = path::encode_dir_object(&prefix);
        self.authorize(&S3Action::DeleteObject, bucket, Some(&marker_key)).await?;
        self.run_backend("delete_object", self.storage.delete_object(bucket, &marker_key, self.credentials()))
            .await?;
        Ok(())
    }

    /// Create one READDIR response for a directory handle.
    ///
    /// Updates the cursor in place: emits dots on the first call (tracked
    /// by dots_emitted), fetches the next page of content (lazily on
    /// first call for Root, per-page for Listing), and advances the
    /// continuation state for Listing cursors via next_listing_page.
    ///
    /// Returns the assembled Vec of File entries. An empty Vec means the
    /// cursor is exhausted. The caller (readdir handler) translates that
    /// into Err(StatusCode::Eof) before sending it on the wire.
    pub(super) async fn readdir_cursor(&self, cursor: &mut DirCursor) -> Result<Vec<File>, SftpError> {
        let mut out = Vec::new();

        match cursor {
            DirCursor::Root {
                buckets_delivered,
                dots_emitted,
            } => {
                if !*dots_emitted {
                    out.extend(dot_entries());
                    *dots_emitted = true;
                }
                if !*buckets_delivered {
                    out.extend(self.fetch_bucket_list().await?);
                    *buckets_delivered = true;
                }
            }
            DirCursor::Listing { dots_emitted, .. } => {
                if !*dots_emitted {
                    out.extend(dot_entries());
                    *dots_emitted = true;
                }
                out.extend(self.next_listing_page(cursor).await?);
            }
        }

        Ok(out)
    }

    /// OPENDIR body shared with the Handler trait wrapper. Resolves the
    /// path, builds the DirCursor, and allocates a directory handle.
    /// Root paths build a Root cursor without any backend call so the
    /// listing IAM gate runs at the first READDIR. Non-root paths verify
    /// ListBucket and HeadBucket synchronously here.
    pub(super) async fn opendir_inner(&mut self, id: u32, path: &str) -> Result<Handle, SftpError> {
        let (bucket, key) = parse_s3_path(path)?;
        let cursor = if bucket.is_empty() {
            DirCursor::Root {
                buckets_delivered: false,
                dots_emitted: false,
            }
        } else {
            let prefix = match &key {
                None => String::new(),
                Some(k) if k.ends_with('/') => k.clone(),
                Some(k) => format!("{k}/"),
            };
            let input = directory_listing_input(&bucket, &prefix, &ListingContinuation::Initial)?;
            self.authorize_list_objects(&input).await?;
            self.run_backend("head_bucket", self.storage.head_bucket(&bucket, self.credentials()))
                .await?;
            DirCursor::Listing {
                bucket,
                prefix,
                continuation: ListingContinuation::Initial,
                dots_emitted: false,
            }
        };
        let handle = self.allocate_handle(HandleState::Dir(cursor))?;
        Ok(Handle { id, handle })
    }

    /// READDIR body shared with the Handler trait wrapper. Removes the
    /// handle from the table to obtain exclusive ownership of the
    /// DirCursor, dispatches by handle type, re-inserts the handle,
    /// and translates an empty page into Eof. The wrapper logs non-Eof
    /// failures explicitly so Eof stays silent in the operator log.
    pub(super) async fn readdir_inner(&mut self, id: u32, handle: String) -> Result<Name, SftpError> {
        let mut state = self
            .handles
            .remove(&handle)
            .ok_or_else(|| SftpError::code(StatusCode::Failure))?;

        let result = match &mut state {
            // READDIR on a file or write handle is a protocol error.
            HandleState::File { .. } | HandleState::Write { .. } => Err(SftpError::code(StatusCode::Failure)),
            HandleState::Dir(cursor) => {
                // Insert a pre-advance copy of the cursor into the table
                // before the await. If the listing future is cancelled,
                // the next READDIR finds the un-advanced cursor and
                // reissues the same page. No entries are duplicated
                // because no batch was sent on the wire before
                // cancellation.
                self.handles.insert(handle.clone(), HandleState::Dir(cursor.clone()));
                self.readdir_cursor(cursor).await
            }
        };
        // Overwrite the tombstone (or replace the File/Write state we
        // removed above) with the updated local state.
        self.handles.insert(handle, state);

        // An empty file list means the cursor has no more entries.
        // Return Eof so the wire response carries the spec sentinel.
        match result {
            Ok(files) if files.is_empty() => Err(SftpError::code(StatusCode::Eof)),
            Ok(files) => Ok(Name { id, files }),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::state::{DirCursor, HandleState, ListingContinuation};
    use super::super::test_support::{TEST_PART_SIZE, build_driver, build_driver_with_timeout, capture_tracing_at};
    use crate::common::dummy_storage::{DummyBackend, DummyError};
    use crate::common::gateway::{with_test_auth_override, with_test_iam_unavailable};
    use russh_sftp::protocol::StatusCode;
    use russh_sftp::server::Handler;
    use std::sync::Arc;
    use tokio::sync::Notify;
    use tracing::Level;

    fn buckets(names: &[&str]) -> rustfs_gateway_types::dto::ListBucketsOutput {
        rustfs_gateway_types::dto::ListBucketsOutput {
            buckets: names
                .iter()
                .map(|n| rustfs_gateway_types::dto::Bucket {
                    name: rustfs_gateway_types::BucketName::new((*n).to_owned()).expect("bucket name"),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    fn names(files: &[russh_sftp::protocol::File]) -> Vec<&str> {
        files.iter().map(|f| f.filename.as_str()).collect()
    }

    fn deny_list_all_my_buckets(action: &crate::common::gateway::S3Action, _: &str, _: Option<&str>) -> bool {
        !matches!(action, crate::common::gateway::S3Action::ListBuckets)
    }

    #[tokio::test]
    async fn root_listing_with_list_all_my_buckets_lists_every_bucket() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_list_buckets_ok(buckets(&["alpha", "beta"]));
        backend.queue_session_list_buckets_ok(buckets(&["beta"]));
        let driver = build_driver(backend.clone(), TEST_PART_SIZE);

        let files = with_test_auth_override(|_, _, _| true, driver.fetch_bucket_list())
            .await
            .expect("global listing");

        assert_eq!(names(&files), ["alpha", "beta"]);
        assert!(backend.last_session_list_context().is_none(), "session listing must not run");
    }

    #[tokio::test]
    async fn root_listing_without_list_all_my_buckets_lists_only_visible_buckets() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_list_buckets_ok(buckets(&["alpha", "beta"]));
        backend.queue_session_list_buckets_ok(buckets(&["beta"]));
        let mut driver = build_driver(backend.clone(), TEST_PART_SIZE);
        driver
            .session_context
            .request_headers
            .insert("user-agent", http::HeaderValue::from_static("sftp-listing-test"));
        driver.session_context.secure_transport = false;

        let files = with_test_auth_override(deny_list_all_my_buckets, driver.fetch_bucket_list())
            .await
            .expect("bucket-scoped listing");

        assert_eq!(names(&files), ["beta"]);
        let (headers, secure) = backend.last_session_list_context().expect("session listing ran");
        assert_eq!(headers, driver.session_context.request_headers, "same headers as every other SFTP check");
        assert_eq!(
            secure, driver.session_context.secure_transport,
            "same transport as every other SFTP check"
        );
    }

    #[tokio::test]
    async fn root_listing_without_any_visible_bucket_is_permission_denied() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_session_list_buckets_err(rustfs_s3_types::S3Error::new(rustfs_s3_types::S3ErrorCode::AccessDenied));
        let driver = build_driver(backend.clone(), TEST_PART_SIZE);

        let err = with_test_auth_override(deny_list_all_my_buckets, driver.fetch_bucket_list())
            .await
            .expect_err("no visible bucket");

        assert_eq!(err.0, StatusCode::PermissionDenied);
        assert!(backend.last_session_list_context().is_some(), "the session listing ran");
    }

    #[tokio::test]
    async fn root_listing_fails_without_fallback_when_iam_is_unavailable() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_session_list_buckets_ok(buckets(&["beta"]));
        let driver = build_driver(backend.clone(), TEST_PART_SIZE);

        let err = with_test_iam_unavailable(driver.fetch_bucket_list())
            .await
            .expect_err("IAM outage");

        assert_eq!(err.0, StatusCode::Failure);
        assert!(backend.last_session_list_context().is_none(), "an IAM outage is not a policy deny");
    }

    #[tokio::test(start_paused = true)]
    async fn root_listing_fails_without_fallback_when_the_global_check_times_out() {
        let backend = Arc::new(DummyBackend::new().stall_authorization());
        backend.queue_session_list_buckets_ok(buckets(&["beta"]));
        let driver = build_driver_with_timeout(backend.clone(), TEST_PART_SIZE, 1);

        let err = driver.fetch_bucket_list().await.expect_err("authorization deadline");

        assert_eq!(err.0, StatusCode::Failure);
        assert!(backend.last_session_list_context().is_none(), "a timeout is not a policy deny");
    }

    #[tokio::test(start_paused = true)]
    async fn root_listing_fallback_respects_the_backend_deadline() {
        let backend = Arc::new(DummyBackend::new().stall_session_listing());
        let driver = build_driver_with_timeout(backend, TEST_PART_SIZE, 1);

        let err = with_test_auth_override(deny_list_all_my_buckets, driver.fetch_bucket_list())
            .await
            .expect_err("session listing deadline");

        assert_eq!(err.0, StatusCode::Failure);
    }

    #[tokio::test]
    async fn root_listing_session_error_other_than_access_denied_is_failure() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_session_list_buckets_err(rustfs_s3_types::S3Error::with_message(
            rustfs_s3_types::S3ErrorCode::InternalError,
            "AccessDenied in the message is not an access denial",
        ));
        let driver = build_driver(backend.clone(), TEST_PART_SIZE);

        let err = with_test_auth_override(deny_list_all_my_buckets, driver.fetch_bucket_list())
            .await
            .expect_err("backend failure");

        assert_eq!(err.0, StatusCode::Failure);
    }

    #[tokio::test]
    async fn root_listing_fallback_is_truncated_at_the_root_listing_limit() {
        for count in [super::ROOT_LISTING_MAX_ENTRIES, super::ROOT_LISTING_MAX_ENTRIES + 1] {
            let backend = Arc::new(DummyBackend::new());
            let many: Vec<String> = (0..count).map(|i| format!("bucket-{i:05}")).collect();
            backend.queue_session_list_buckets_ok(buckets(&many.iter().map(String::as_str).collect::<Vec<_>>()));
            let driver = build_driver(backend, TEST_PART_SIZE);

            let files = with_test_auth_override(deny_list_all_my_buckets, driver.fetch_bucket_list())
                .await
                .expect("bucket-scoped listing");

            assert_eq!(files.len(), super::ROOT_LISTING_MAX_ENTRIES, "bucket count {count}");
        }
    }

    #[tokio::test]
    async fn root_listing_fails_closed_when_the_backend_has_no_session_listing() {
        let backend = Arc::new(DummyBackend::new());
        let driver = build_driver(backend.clone(), TEST_PART_SIZE);

        let err = with_test_auth_override(deny_list_all_my_buckets, driver.fetch_bucket_list())
            .await
            .expect_err("no session-aware listing");

        assert_eq!(err.0, StatusCode::PermissionDenied);
    }

    #[tokio::test]
    async fn opendir_and_readdir_authorize_the_same_listing_parameters() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_head_bucket_ok();
        let mut driver = build_driver(backend.clone(), TEST_PART_SIZE);
        let handle = with_test_auth_override(
            |_, bucket, prefix| bucket == "bucket" && prefix == Some("private/"),
            driver.opendir_inner(1, "/bucket/private"),
        )
        .await
        .expect("OPENDIR conditional allow");
        let mut cursor = DirCursor::Listing {
            bucket: "bucket".into(),
            prefix: "private/".into(),
            continuation: ListingContinuation::Initial,
            dots_emitted: true,
        };
        with_test_auth_override(|_, _, prefix| prefix == Some("private/"), driver.next_listing_page(&mut cursor))
            .await
            .expect("READDIR conditional allow");
        cursor = DirCursor::Listing {
            bucket: "bucket".into(),
            prefix: "private/".into(),
            continuation: ListingContinuation::Next("next-page".into()),
            dots_emitted: true,
        };
        with_test_auth_override(|_, _, prefix| prefix == Some("private/"), driver.next_listing_page(&mut cursor))
            .await
            .expect("continued READDIR conditional allow");
        let authorized = backend.list_authorizations();
        let executed = backend.list_objects_calls();
        assert_eq!(authorized.len(), 3);
        assert_eq!(executed.len(), 2);
        for input in &authorized {
            assert_eq!(input.prefix.as_deref(), Some("private/"));
            assert_eq!(input.delimiter.as_deref(), Some("/"));
            assert_eq!(input.max_keys, Some(super::READDIR_PAGE_MAX_KEYS));
        }
        for (auth, call) in authorized[1..].iter().zip(&executed) {
            assert_eq!(auth.prefix, call.prefix);
            assert_eq!(auth.delimiter, call.delimiter);
            assert_eq!(auth.max_keys, call.max_keys);
            assert_eq!(auth.continuation_token, call.continuation_token);
        }
        assert_eq!(authorized[2].continuation_token.as_ref().map(|value| value.as_str()), Some("next-page"));
        let denied =
            with_test_auth_override(|_, _, prefix| prefix != Some("private/"), driver.readdir_inner(2, handle.handle)).await;
        assert!(denied.is_err(), "policy change must deny the next READDIR");
        assert_eq!(backend.list_objects_calls().len(), 2, "denied READDIR cannot reach storage");
    }

    #[tokio::test]
    async fn validate_directory_empty_propagates_list_error() {
        // Safety contract: when the empty-check list_objects_v2 itself
        // fails, validate_directory_empty must return Err. A
        // fall-through to the destructive caller would convert a
        // transient backend failure into silent data loss.
        let backend = Arc::new(DummyBackend::new());
        backend.queue_list_objects_v2_err(DummyError::Injected("list_objects_v2 transient failure".into()));
        let driver = build_driver(backend.clone(), TEST_PART_SIZE);
        let result = with_test_auth_override(|_, _, _| true, driver.validate_directory_empty("bucket", "")).await;
        assert!(result.is_err(), "list_objects_v2 error must propagate as Err");
    }

    #[tokio::test]
    async fn validate_directory_empty_returns_ok_when_listing_is_empty() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_list_objects_v2_ok_empty();
        let driver = build_driver(backend.clone(), TEST_PART_SIZE);
        let result = with_test_auth_override(|_, _, _| true, driver.validate_directory_empty("bucket", "")).await;
        assert!(result.is_ok(), "empty listing must return Ok");
    }

    /// A READDIR cancelled mid-await of list_objects_v2 must leave the
    /// pre-advance cursor copy in the handle table so the next READDIR
    /// reissues the same first page. Without this, a cancellation
    /// could either lose the cursor (next READDIR fails) or skip past
    /// the entries that were never sent on the wire (silent data
    /// hiding). The first page never went out, so re-issue cannot
    /// produce a duplicate.
    #[tokio::test]
    async fn cancelled_readdir_leaves_cursor_unadvanced_for_re_issue() {
        let backend = Arc::new(DummyBackend::new());
        let entered = Arc::new(Notify::new());
        backend.stall_list_objects_v2(entered.clone());

        let mut driver = build_driver(backend.clone(), TEST_PART_SIZE);
        let cursor = DirCursor::Listing {
            bucket: "bucket".to_string(),
            prefix: String::new(),
            continuation: ListingContinuation::Initial,
            dots_emitted: true,
        };
        let handle_id = driver.allocate_handle(HandleState::Dir(cursor)).expect("allocate");

        let readdir_fut = driver.readdir(1, handle_id.clone());

        with_test_auth_override(|_, _, _| true, async {
            tokio::select! {
                biased;
                _ = entered.notified() => {
                    // list_objects_v2 has been entered. Drop readdir_fut on
                    // exit from this block; the surviving handle entry
                    // must be the pre-advance tombstone.
                }
                _ = readdir_fut => {
                    panic!("readdir must stall inside list_objects_v2, not complete");
                }
            }
        })
        .await;

        // The handle table must still hold the cursor in Initial state.
        // readdir's pre-advance insert ran before the await; the post-
        // await re-insert never ran because the future was dropped.
        let surviving = driver.handles.get(&handle_id).expect("handle must survive cancellation");
        let HandleState::Dir(DirCursor::Listing {
            continuation,
            dots_emitted,
            ..
        }) = surviving
        else {
            panic!("surviving handle must be a Listing cursor");
        };
        assert!(
            matches!(continuation, ListingContinuation::Initial),
            "cancelled READDIR must leave the cursor in Initial state",
        );
        assert!(*dots_emitted, "dots_emitted must survive cancellation unchanged");

        // Re-issue READDIR. Turn the stall off and queue a single Ok
        // page so the second call completes without exercising the
        // stall path. The cursor's Initial state means the second
        // request is identical to the cancelled one (no continuation
        // token, no skipped entries).
        backend.clear_stall_list_objects_v2();
        backend.queue_list_objects_v2_ok_empty();
        let result = with_test_auth_override(|_, _, _| true, driver.readdir(2, handle_id)).await;
        // Empty page returns Eof per readdir's empty-Name-to-Eof translation.
        let err = result.expect_err("re-issued READDIR against an empty listing must return Eof, not Ok");
        assert!(
            matches!(StatusCode::from(err), StatusCode::Eof),
            "re-issued READDIR against an empty listing must return Eof, not Failure",
        );
    }

    /// READDIR on an exhausted cursor returns the spec-mandated Eof
    /// sentinel. The handler must surface Eof on the wire and stay
    /// silent in the operator log so a normal directory listing burst
    /// does not generate one error-level event per page.
    #[tokio::test]
    async fn readdir_past_eof_emits_no_error_level_event() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_list_objects_v2_ok_empty();
        let mut driver = build_driver(Arc::clone(&backend), TEST_PART_SIZE);
        let cursor = DirCursor::Listing {
            bucket: "bucket".to_string(),
            prefix: String::new(),
            continuation: ListingContinuation::Initial,
            dots_emitted: true,
        };
        let handle_id = driver.allocate_handle(HandleState::Dir(cursor)).expect("allocate");

        let (result, captured) =
            capture_tracing_at(Level::ERROR, with_test_auth_override(|_, _, _| true, driver.readdir(7, handle_id))).await;
        let err = result.expect_err("exhausted cursor must return Eof");
        assert!(matches!(StatusCode::from(err), StatusCode::Eof));
        assert!(
            !captured.contains("ERROR"),
            "Eof return must not produce an error-level event, captured: {captured}"
        );
        assert!(
            !captured.contains("SFTP READDIR failed"),
            "Eof return must not log SFTP READDIR failed, captured: {captured}"
        );
    }

    /// A non-Eof failure on the readdir path is a real operator-visible
    /// problem. Dropping err(Debug) from the instrument attribute
    /// removed the auto-logging seam, so the handler logs explicitly.
    /// This pins the substitute path so a future refactor cannot
    /// silently let real backend failures pass without an error-level
    /// event.
    #[tokio::test]
    async fn readdir_backend_failure_emits_error_level_event() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_list_objects_v2_err(DummyError::Injected("backend exploded".into()));
        let mut driver = build_driver(Arc::clone(&backend), TEST_PART_SIZE);
        let cursor = DirCursor::Listing {
            bucket: "bucket".to_string(),
            prefix: String::new(),
            continuation: ListingContinuation::Initial,
            dots_emitted: true,
        };
        let handle_id = driver.allocate_handle(HandleState::Dir(cursor)).expect("allocate");

        let (result, captured) =
            capture_tracing_at(Level::ERROR, with_test_auth_override(|_, _, _| true, driver.readdir(8, handle_id))).await;
        let err = result.expect_err("backend error must propagate as Err");
        assert!(!matches!(StatusCode::from(err), StatusCode::Eof), "backend error must not be Eof");
        assert!(
            captured.contains("ERROR"),
            "non-Eof backend failure must produce an error-level event, captured: {captured}"
        );
        assert!(
            captured.contains("SFTP READDIR failed"),
            "error-level event must carry the SFTP READDIR failed message, captured: {captured}"
        );
    }
}
