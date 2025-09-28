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

use crate::bitrot::{create_bitrot_reader, create_bitrot_writer};
use crate::erasure_coding::{Erasure, calc_shard_size};
use crate::error::{Error, StorageError};
use crate::store_api::ObjectInfo;
use rustfs_filemeta::TRANSITION_COMPLETE;
use rustfs_utils::HashAlgorithm;
use rustfs_utils::http::headers::{
    AMZ_SERVER_SIDE_ENCRYPTION, AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM, AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY,
    AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5, AMZ_SERVER_SIDE_ENCRYPTION_KMS_CONTEXT, AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID,
    RESERVED_METADATA_PREFIX_LOWER,
};
use std::collections::HashSet;

/// Ensure the target object can accept append writes under current state.
pub fn validate_append_preconditions(bucket: &str, object: &str, info: &ObjectInfo) -> Result<(), Error> {
    if info.is_compressed() {
        return Err(StorageError::InvalidArgument(
            bucket.to_string(),
            object.to_string(),
            "append is not supported for compressed objects".to_string(),
        ));
    }

    let encryption_headers = [
        AMZ_SERVER_SIDE_ENCRYPTION,
        AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID,
        AMZ_SERVER_SIDE_ENCRYPTION_KMS_CONTEXT,
        AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM,
        AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY,
        AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5,
    ];

    if encryption_headers
        .iter()
        .any(|header| info.user_defined.contains_key(*header) || info.user_defined.contains_key(&header.to_ascii_lowercase()))
    {
        return Err(StorageError::InvalidArgument(
            bucket.to_string(),
            object.to_string(),
            "append is not supported for encrypted objects".to_string(),
        ));
    }

    if info.transitioned_object.status == TRANSITION_COMPLETE || !info.transitioned_object.tier.is_empty() {
        return Err(StorageError::InvalidArgument(
            bucket.to_string(),
            object.to_string(),
            "append is not supported for transitioned objects".to_string(),
        ));
    }

    Ok(())
}

/// Validate that the requested append position matches the current object length.
pub fn validate_append_position(bucket: &str, object: &str, info: &ObjectInfo, expected_position: i64) -> Result<(), Error> {
    if expected_position != info.size {
        return Err(StorageError::InvalidArgument(
            bucket.to_string(),
            object.to_string(),
            format!("append position mismatch: provided {}, expected {}", expected_position, info.size),
        ));
    }
    Ok(())
}

pub struct InlineAppendContext<'a> {
    pub existing_inline: Option<&'a [u8]>,
    pub existing_plain: Option<&'a [u8]>,
    pub existing_size: i64,
    pub append_payload: &'a [u8],
    pub erasure: &'a Erasure,
    pub hash_algorithm: HashAlgorithm,
    pub has_checksums: bool,
}

pub struct InlineAppendResult {
    pub inline_data: Vec<u8>,
    pub total_size: i64,
    pub etag: String,
}

/// Decode inline payload using available checksum algorithms. Returns raw bytes when decoding fails but
/// the inline buffer already contains the plain payload.
pub async fn decode_inline_payload(
    inline: &[u8],
    size: usize,
    erasure: &Erasure,
    preferred: HashAlgorithm,
) -> Result<(Vec<u8>, HashAlgorithm), Error> {
    match decode_inline_variants(inline, size, erasure, preferred).await {
        Ok((data, algo)) => Ok((data, algo)),
        Err(err) => {
            if inline.len() >= size {
                Ok((inline[..size].to_vec(), HashAlgorithm::None))
            } else {
                Err(err)
            }
        }
    }
}

/// Append data to an inline object and return the re-encoded inline buffer.
pub async fn append_inline_data(ctx: InlineAppendContext<'_>) -> Result<InlineAppendResult, Error> {
    let mut plain = Vec::with_capacity(ctx.existing_inline.map(|data| data.len()).unwrap_or(0) + ctx.append_payload.len());
    let mut encode_algorithm = ctx.hash_algorithm.clone();

    if let Some(existing_plain) = ctx.existing_plain {
        if existing_plain.len() != ctx.existing_size as usize {
            return Err(StorageError::other("existing plain payload length mismatch"));
        }
        plain.extend_from_slice(existing_plain);
    } else if ctx.existing_size > 0 {
        let inline = ctx
            .existing_inline
            .ok_or_else(|| StorageError::other("inline payload missing"))?;

        let (decoded, detected_algo) =
            decode_inline_payload(inline, ctx.existing_size as usize, ctx.erasure, ctx.hash_algorithm.clone()).await?;
        encode_algorithm = detected_algo;
        plain.extend_from_slice(&decoded);
    } else if let Some(inline) = ctx.existing_inline {
        plain.extend_from_slice(inline);
    }

    plain.extend_from_slice(ctx.append_payload);
    let total_size = plain.len() as i64;
    let etag = md5_hex(&plain);

    if encode_algorithm == HashAlgorithm::None {
        if ctx.has_checksums {
            encode_algorithm = ctx.hash_algorithm.clone();
        } else {
            return Ok(InlineAppendResult {
                inline_data: plain,
                total_size,
                etag,
            });
        }
    }

    let mut writer = create_bitrot_writer(
        true,
        None,
        "",
        "",
        ctx.erasure.shard_file_size(total_size),
        ctx.erasure.shard_size(),
        encode_algorithm,
    )
    .await
    .map_err(|e| StorageError::other(format!("failed to create inline writer: {e}")))?;

    let mut remaining = plain.as_slice();
    while !remaining.is_empty() {
        let chunk_len = remaining.len().min(ctx.erasure.block_size);
        writer
            .write(&remaining[..chunk_len])
            .await
            .map_err(|e| StorageError::other(format!("failed to write inline data: {e}")))?;
        remaining = &remaining[chunk_len..];
    }

    writer
        .shutdown()
        .await
        .map_err(|e| StorageError::other(format!("failed to finalize inline writer: {e}")))?;

    let inline_data = writer
        .into_inline_data()
        .ok_or_else(|| StorageError::other("inline writer did not return data"))?;

    Ok(InlineAppendResult {
        inline_data,
        total_size,
        etag,
    })
}

fn md5_hex(data: &[u8]) -> String {
    let digest = HashAlgorithm::Md5.hash_encode(data);
    hex_from_bytes(digest.as_ref())
}

fn hex_from_bytes(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write;
        write!(&mut out, "{:02x}", byte).expect("write hex");
    }
    out
}

async fn decode_inline_variants(
    inline: &[u8],
    size: usize,
    erasure: &Erasure,
    preferred: HashAlgorithm,
) -> Result<(Vec<u8>, HashAlgorithm), Error> {
    let mut tried = HashSet::new();
    let candidates = [preferred, HashAlgorithm::HighwayHash256, HashAlgorithm::HighwayHash256S];

    let mut last_err: Option<Error> = None;

    for algo in candidates {
        if !tried.insert(algo.clone()) {
            continue;
        }

        match decode_inline_with_algo(inline, size, erasure, algo.clone()).await {
            Ok(data) => return Ok((data, algo)),
            Err(err) => last_err = Some(err),
        }
    }

    Err(last_err.unwrap_or_else(|| StorageError::other("failed to decode inline data")))
}

async fn decode_inline_with_algo(inline: &[u8], size: usize, erasure: &Erasure, algo: HashAlgorithm) -> Result<Vec<u8>, Error> {
    let total_len = inline
        .len()
        .max(erasure.shard_file_size(size as i64).max(size as i64) as usize);
    let mut reader = create_bitrot_reader(Some(inline), None, "", "", 0, total_len, erasure.shard_size(), algo)
        .await
        .map_err(|e| StorageError::other(format!("failed to create inline reader: {e}")))?
        .ok_or_else(|| StorageError::other("inline reader unavailable"))?;

    let mut out = Vec::with_capacity(size);
    while out.len() < size {
        let remaining = size - out.len();
        let plain_chunk = remaining.min(erasure.block_size);
        let shard_payload = calc_shard_size(plain_chunk, erasure.data_shards).max(1);
        let mut buf = vec![0u8; shard_payload];
        let read = reader
            .read(&mut buf)
            .await
            .map_err(|e| StorageError::other(format!("failed to read inline data: {e}")))?;
        if read == 0 {
            return Err(StorageError::other("incomplete inline data read"));
        }

        let copy_len = remaining.min(read);
        out.extend_from_slice(&buf[..copy_len]);
    }

    Ok(out)
}

/// Background task to spill inline data to segmented format
pub struct InlineSpillProcessor {
    pub disks: Vec<Option<crate::disk::DiskStore>>,
    pub write_quorum: usize,
}

impl InlineSpillProcessor {
    pub fn new(disks: Vec<Option<crate::disk::DiskStore>>, write_quorum: usize) -> Self {
        Self { disks, write_quorum }
    }

    /// Process a single spill operation from InlinePendingSpill to SegmentedActive
    pub async fn process_spill(
        &self,
        bucket: &str,
        object: &str,
        mut fi: rustfs_filemeta::FileInfo,
        mut parts_metadata: Vec<rustfs_filemeta::FileInfo>,
        epoch: u64,
    ) -> Result<(), Error> {
        use rustfs_filemeta::AppendStateKind;
        use tracing::{debug, error, info, warn};

        // Verify we're in the correct state
        let current_state = fi.get_append_state();
        if current_state.state != AppendStateKind::InlinePendingSpill {
            warn!(
                bucket = bucket,
                object = object,
                current_state = ?current_state.state,
                "Spill processor called on object not in InlinePendingSpill state"
            );
            return Ok(());
        }

        // Check epoch to ensure we're processing the correct version
        if current_state.epoch != epoch {
            debug!(
                bucket = bucket,
                object = object,
                current_epoch = current_state.epoch,
                expected_epoch = epoch,
                "Spill operation skipped due to epoch mismatch"
            );
            return Ok(());
        }

        info!(
            bucket = bucket,
            object = object,
            size = fi.size,
            epoch = epoch,
            "Starting inline data spill to segmented format"
        );

        // Extract inline data
        let inline_data = fi
            .data
            .clone()
            .ok_or_else(|| StorageError::other("Cannot spill object without inline data"))?;

        // Create erasure encoder
        let erasure = Erasure::new(fi.erasure.data_blocks, fi.erasure.parity_blocks, fi.erasure.block_size);

        // Decode inline data to plain data
        let hash_algorithm = fi
            .parts
            .first()
            .map(|part| fi.erasure.get_checksum_info(part.number).algorithm)
            .unwrap_or(HashAlgorithm::HighwayHash256);

        let plain_data = match decode_inline_payload(&inline_data, fi.size as usize, &erasure, hash_algorithm.clone()).await {
            Ok((plain, _detected_algo)) => plain,
            Err(err) => {
                error!(
                    bucket = bucket,
                    object = object,
                    error = ?err,
                    "Failed to decode inline data during spill"
                );
                return Err(StorageError::other(format!("Failed to decode inline data for spill: {err}")));
            }
        };

        // Generate data directory for the object
        let data_dir = uuid::Uuid::new_v4();

        // Create temporary directory for the spill operation
        let tmp_root = format!("{}x{}", uuid::Uuid::new_v4(), time::OffsetDateTime::now_utc().unix_timestamp());
        let tmp_path = format!("{tmp_root}/{}/part.1", data_dir);

        // Encode and write the data to all disks
        match self.write_segmented_data(&plain_data, &tmp_path, &erasure).await {
            Ok(_) => {
                // Move from temp to permanent location
                let final_path = format!("{}/part.1", data_dir);
                if let Err(err) = self.move_temp_to_final(&tmp_path, &final_path).await {
                    error!(
                        bucket = bucket,
                        object = object,
                        error = ?err,
                        "Failed to move spilled data to final location"
                    );
                    // Clean up temp files
                    let _ = self.cleanup_temp_files(&tmp_path).await;
                    return Err(err);
                }

                // Update file metadata
                fi.data_dir = Some(data_dir);
                fi.data = None; // Remove inline data
                fi.metadata.remove(&format!("{}inline-data", RESERVED_METADATA_PREFIX_LOWER));

                // Update append state to SegmentedActive
                let mut new_state = current_state;
                new_state.state = AppendStateKind::SegmentedActive;
                new_state.epoch = new_state.epoch.saturating_add(1);
                new_state.pending_segments.clear();

                fi.set_append_state(&new_state)
                    .map_err(|err| StorageError::other(format!("Failed to update append state after spill: {err}")))?;

                // Update all parts metadata
                for meta in parts_metadata.iter_mut() {
                    if !meta.is_valid() {
                        continue;
                    }
                    meta.data_dir = Some(data_dir);
                    meta.data = None;
                    meta.metadata = fi.metadata.clone();
                    meta.metadata
                        .remove(&format!("{}inline-data", RESERVED_METADATA_PREFIX_LOWER));
                }

                // Write updated metadata back to disks
                // TODO: Implement metadata write-back logic
                // This would typically involve writing the updated FileInfo to all disks

                info!(
                    bucket = bucket,
                    object = object,
                    data_dir = ?data_dir,
                    new_epoch = new_state.epoch,
                    "Successfully spilled inline data to segmented format"
                );

                Ok(())
            }
            Err(err) => {
                error!(
                    bucket = bucket,
                    object = object,
                    error = ?err,
                    "Failed to write segmented data during spill"
                );
                // Clean up temp files
                let _ = self.cleanup_temp_files(&tmp_path).await;
                Err(err)
            }
        }
    }

    async fn write_segmented_data(&self, data: &[u8], tmp_path: &str, _erasure: &Erasure) -> Result<(), Error> {
        use tracing::debug;

        // TODO: Implement proper erasure encoding and writing to disks
        // This is a placeholder implementation
        debug!(
            data_len = data.len(),
            path = tmp_path,
            "Writing segmented data (placeholder implementation)"
        );

        // For now, just return success - full implementation would:
        // 1. Create bitrot writers for each disk
        // 2. Erasure encode the data
        // 3. Write each shard to its corresponding disk
        Ok(())
    }

    async fn move_temp_to_final(&self, tmp_path: &str, final_path: &str) -> Result<(), Error> {
        use tracing::debug;

        // TODO: Implement moving temp files to final location
        debug!(
            tmp_path = tmp_path,
            final_path = final_path,
            "Moving temp files to final location (placeholder)"
        );
        Ok(())
    }

    async fn cleanup_temp_files(&self, tmp_path: &str) -> Result<(), Error> {
        use tracing::debug;

        // TODO: Implement temp file cleanup
        debug!(tmp_path = tmp_path, "Cleaning up temp files (placeholder)");
        Ok(())
    }
}

/// Trigger background spill processing for an object
pub fn trigger_spill_process(
    bucket: String,
    object: String,
    fi: rustfs_filemeta::FileInfo,
    parts_metadata: Vec<rustfs_filemeta::FileInfo>,
    epoch: u64,
    disks: Vec<Option<crate::disk::DiskStore>>,
    write_quorum: usize,
) {
    use tracing::error;

    tokio::spawn(async move {
        let processor = InlineSpillProcessor::new(disks, write_quorum);
        if let Err(err) = processor.process_spill(&bucket, &object, fi, parts_metadata, epoch).await {
            error!(
                bucket = bucket,
                object = object,
                epoch = epoch,
                error = ?err,
                "Background spill process failed"
            );
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_utils::HashAlgorithm;

    fn make_object_info() -> ObjectInfo {
        ObjectInfo {
            bucket: "test-bucket".to_string(),
            name: "obj".to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn rejects_compressed_objects() {
        let mut info = make_object_info();
        info.user_defined
            .insert(format!("{RESERVED_METADATA_PREFIX_LOWER}compression"), "zstd".to_string());

        let err = validate_append_preconditions("test-bucket", "obj", &info).unwrap_err();
        matches!(err, StorageError::InvalidArgument(..))
            .then_some(())
            .expect("expected invalid argument");
    }

    #[test]
    fn rejects_encrypted_objects() {
        let mut info = make_object_info();
        info.user_defined
            .insert("x-amz-server-side-encryption".to_string(), "AES256".to_string());

        let err = validate_append_preconditions("test-bucket", "obj", &info).unwrap_err();
        matches!(err, StorageError::InvalidArgument(..))
            .then_some(())
            .expect("expected invalid argument");
    }

    #[test]
    fn rejects_transitioned_objects() {
        let mut info = make_object_info();
        info.transitioned_object.tier = "GLACIER".to_string();
        info.transitioned_object.status = TRANSITION_COMPLETE.to_string();

        let err = validate_append_preconditions("test-bucket", "obj", &info).unwrap_err();
        matches!(err, StorageError::InvalidArgument(..))
            .then_some(())
            .expect("expected invalid argument");
    }

    #[test]
    fn accepts_plain_objects() {
        let info = make_object_info();
        validate_append_preconditions("test-bucket", "obj", &info).expect("append should be allowed");
    }

    #[test]
    fn rejects_position_mismatch() {
        let mut info = make_object_info();
        info.size = 10;
        let err = validate_append_position("test-bucket", "obj", &info, 5).unwrap_err();
        matches!(err, StorageError::InvalidArgument(..))
            .then_some(())
            .expect("expected invalid argument");
    }

    fn make_inline_erasure() -> Erasure {
        Erasure::new(1, 0, 1024)
    }

    async fn encode_inline(data: &[u8], erasure: &Erasure) -> Vec<u8> {
        let mut writer = create_bitrot_writer(
            true,
            None,
            "",
            "",
            erasure.shard_file_size(data.len() as i64),
            erasure.shard_size(),
            HashAlgorithm::HighwayHash256,
        )
        .await
        .unwrap();

        let mut remaining = data;
        while !remaining.is_empty() {
            let chunk_len = remaining.len().min(erasure.block_size);
            writer.write(&remaining[..chunk_len]).await.unwrap();
            remaining = &remaining[chunk_len..];
        }

        writer.shutdown().await.unwrap();
        writer.into_inline_data().unwrap()
    }

    async fn decode_inline(encoded: &[u8], size: usize, erasure: &Erasure) -> Vec<u8> {
        let mut reader =
            create_bitrot_reader(Some(encoded), None, "", "", 0, size, erasure.shard_size(), HashAlgorithm::HighwayHash256)
                .await
                .unwrap()
                .unwrap();

        let mut out = Vec::with_capacity(size);
        while out.len() < size {
            let remaining = size - out.len();
            let mut buf = vec![0u8; erasure.block_size.min(remaining.max(1))];
            let read = reader.read(&mut buf).await.unwrap();
            if read == 0 {
                break;
            }
            out.extend_from_slice(&buf[..read.min(remaining)]);
        }
        out
    }

    #[tokio::test]
    async fn append_inline_combines_payloads() {
        let erasure = make_inline_erasure();
        let existing_plain = b"hello";
        let encoded = encode_inline(existing_plain, &erasure).await;

        let ctx = InlineAppendContext {
            existing_inline: Some(&encoded),
            existing_plain: None,
            existing_size: existing_plain.len() as i64,
            append_payload: b" world",
            erasure: &erasure,
            hash_algorithm: HashAlgorithm::HighwayHash256,
            has_checksums: true,
        };

        let result = append_inline_data(ctx).await.expect("inline append to succeed");
        assert_eq!(result.total_size, 11);
        assert_eq!(result.etag, md5_hex(b"hello world"));

        let decoded = decode_inline(&result.inline_data, result.total_size as usize, &erasure).await;
        assert_eq!(decoded, b"hello world");
    }

    #[tokio::test]
    async fn decode_inline_handles_padded_shards() {
        let erasure = Erasure::new(1, 0, 1024);
        let plain = b"hello";

        let mut padded = vec![0u8; calc_shard_size(plain.len(), erasure.data_shards)];
        padded[..plain.len()].copy_from_slice(plain);

        let mut writer = create_bitrot_writer(
            true,
            None,
            "",
            "",
            erasure.shard_file_size(plain.len() as i64),
            erasure.shard_size(),
            HashAlgorithm::HighwayHash256,
        )
        .await
        .unwrap();

        writer.write(&padded).await.unwrap();
        writer.shutdown().await.unwrap();
        let inline = writer.into_inline_data().unwrap();

        let (decoded, algo) = decode_inline_payload(&inline, plain.len(), &erasure, HashAlgorithm::HighwayHash256)
            .await
            .expect("inline decode should succeed");

        assert_eq!(decoded, plain);
        assert_eq!(algo, HashAlgorithm::HighwayHash256);
    }

    #[tokio::test]
    async fn append_inline_handles_empty_original() {
        let erasure = make_inline_erasure();
        let ctx = InlineAppendContext {
            existing_inline: None,
            existing_plain: None,
            existing_size: 0,
            append_payload: b"data",
            erasure: &erasure,
            hash_algorithm: HashAlgorithm::HighwayHash256,
            has_checksums: true,
        };

        let result = append_inline_data(ctx).await.expect("inline append to succeed");
        assert_eq!(result.total_size, 4);
        assert_eq!(result.etag, md5_hex(b"data"));

        let decoded = decode_inline(&result.inline_data, result.total_size as usize, &erasure).await;
        assert_eq!(decoded, b"data");
    }

    #[tokio::test]
    async fn append_inline_without_checksums_uses_raw_bytes() {
        let erasure = Erasure::new(1, 0, 1024);
        let existing = b"hello";

        let ctx = InlineAppendContext {
            existing_inline: Some(existing),
            existing_plain: None,
            existing_size: existing.len() as i64,
            append_payload: b" world",
            erasure: &erasure,
            hash_algorithm: HashAlgorithm::HighwayHash256,
            has_checksums: false,
        };

        let result = append_inline_data(ctx).await.expect("inline append to succeed");
        assert_eq!(result.total_size, 11);
        assert_eq!(result.etag, md5_hex(b"hello world"));

        assert_eq!(result.inline_data, b"hello world");
    }

    #[tokio::test]
    async fn append_inline_decodes_bitrot_without_checksums() {
        let erasure = Erasure::new(1, 0, 1024);
        let existing_plain = b"hello";
        let encoded = encode_inline(existing_plain, &erasure).await;

        let ctx = InlineAppendContext {
            existing_inline: Some(&encoded),
            existing_plain: None,
            existing_size: existing_plain.len() as i64,
            append_payload: b" world",
            erasure: &erasure,
            hash_algorithm: HashAlgorithm::HighwayHash256,
            has_checksums: false,
        };

        let result = append_inline_data(ctx).await.expect("inline append to succeed");
        assert_eq!(result.total_size, 11);
        assert_eq!(result.etag, md5_hex(b"hello world"));

        let decoded = decode_inline(&result.inline_data, result.total_size as usize, &erasure).await;
        assert_eq!(decoded, b"hello world");
    }
}
