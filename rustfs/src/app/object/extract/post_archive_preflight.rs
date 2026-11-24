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

//! Full archive input and derived-key preflight before POST member writes.
//!
//! This does not authenticate a policy or authorize any member write. The
//! caller must supply key constraints from the same verified form, and the
//! extraction path still performs each member's IAM authorization.

use super::*;
use tokio::io::AsyncReadExt as _;

pub(in crate::app::object) async fn preflight_post_archive<R>(
    body: R,
    expected_length: u64,
    archive_key: &str,
    headers: &HeaderMap,
    permits: &(dyn Fn(&str) -> bool + Send + Sync),
) -> S3Result<()>
where
    R: AsyncRead + Send + Unpin + 'static,
{
    let options = resolve_put_object_extract_options(headers)?;
    let limits = put_object_extract_limits();
    let upload = Arc::new(Mutex::new(ExtractArchiveUploadState::default()));
    let tracked = ExtractArchiveEtagReader::new(body, expected_length, Arc::clone(&upload));
    let decoder = build_admitted_extract_archive_decoder(get_concurrency_manager(), archive_key, tracked).await?;
    let decoder = ExtractDecodedLimitReader::new(decoder, limits.max_decoded_size);
    let mut archive = build_put_object_extract_archive(decoder, limits);
    let mut entries = archive.entries().map_err(map_extract_archive_error)?;
    let mut count = 0usize;
    let mut total = 0u64;
    let mut pax_bytes = 0u64;
    let mut pax_records = 0usize;
    while let Some(entry) = entries.next().await {
        let mut entry = entry.map_err(map_extract_archive_error)?;
        count = count
            .checked_add(1)
            .ok_or_else(|| s3_error!(InvalidArgument, "Archive entry count overflow"))?;
        validate_put_object_extract_entry_count(count, limits)?;
        let size = entry.effective_size();
        validate_put_object_extract_entry_size("archive member", size, limits)?;
        total = total
            .checked_add(size)
            .ok_or_else(|| s3_error!(InvalidArgument, "Archive total unpacked size overflow"))?;
        validate_put_object_extract_total_size(total, limits)?;
        count_extract_entry_pax_metadata(&mut entry, &mut pax_bytes, &mut pax_records, limits)
            .await
            .map_err(ExtractEntryError::into_s3_error)?;

        let entry_type = entry.header().entry_type();
        let kind = classify_extract_entry_type(entry_type);
        if kind == ExtractEntryKind::Skip {
            continue;
        }
        validate_extract_special_entry_size(entry_type, size)?;
        let path = entry.path_bytes().map_err(map_extract_archive_error)?;
        let path = match strict_extract_entry_path(path.as_ref()) {
            Ok(path) => path,
            Err(error) => {
                error.ignore_or_return(options.ignore_errors)?;
                continue;
            }
        };
        if is_empty_extract_entry_path(path) {
            continue;
        }
        let is_dir = kind == ExtractEntryKind::Directory || is_legacy_null_directory(entry.header(), path);
        if is_dir && options.ignore_dirs {
            continue;
        }
        let key = normalize_extract_entry_key(path, options.prefix.as_deref(), is_dir)?;
        if let Err(error) = validate_extract_member_key(&key, limits) {
            error.ignore_or_return(options.ignore_errors)?;
            continue;
        }
        if !permits(&key) {
            return Err(s3_error!(AccessDenied, "Archive member is outside the signed POST key policy"));
        }
    }
    drop(entries);
    let mut decoder = archive
        .into_inner()
        .map_err(|_| s3_error!(InvalidArgument, "Failed to finalize archive reader"))?;
    let mut buffer = zeroize::Zeroizing::new(vec![0u8; 64 * 1024]);
    while decoder.read(&mut buffer[..]).await.map_err(map_extract_archive_error)? != 0 {}
    let complete = upload
        .lock()
        .map_err(|_| s3_error!(InternalError, "Archive upload state lock was poisoned"))?
        .body_complete;
    if !complete {
        return Err(s3_error!(UnexpectedContent, "Archive decoder did not consume the complete request body"));
    }
    Ok(())
}
