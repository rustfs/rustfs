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

//! `rustfs inspect bucket-meta` — offline, **read-only** export of a bucket's
//! persisted configuration bytes (backlog#1733, P9-01 §4.5).
//!
//! Reads the `.rustfs.sys/buckets/<bucket>/.metadata.bin` object directly from
//! drive roots: xl.meta → inline shard(s) → bitrot verify → erasure
//! reconstruction → the raw `.metadata.bin` blob. It deliberately does **not**
//! go through `parse_all_configs`, so it works on buckets whose config XML no
//! longer parses — that is its forensic purpose: "what bytes are actually on
//! disk". It never writes to any drive; `--raw` output goes to `--out` only.
//! Normal filesystem reads may update access times, so strict offline forensic
//! use requires mounting source media read-only.
//!
use crate::config::{InspectBucketMetaOpts, InspectCommands, InspectOpts};
use crate::storage_api::inspect::bucket_metadata::BucketMetadata;
use crate::storage_api::inspect::{BitrotReader, Erasure, check_valid_bucket_name_strict, file_info_quorum_hash};
use rustfs_filemeta::{FileInfo, FileMeta};
use rustfs_utils::HashAlgorithm;
use std::ffi::OsString;
use std::io::{Error, ErrorKind, Result};
use std::path::{Path, PathBuf};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::io::AsyncWriteExt;

const META_BUCKET: &str = ".rustfs.sys";
const BUCKET_META_PREFIX: &str = "buckets";
const BUCKET_METADATA_FILE: &str = ".metadata.bin";
const MAX_XL_META_BYTES: u64 = 4 * 1024 * 1024;
const MAX_BUCKET_METADATA_BYTES: usize = 16 * 1024 * 1024;

/// Execute an offline inspection command without initializing server state.
pub async fn execute_inspect(opts: &InspectOpts) -> Result<()> {
    match &opts.command {
        InspectCommands::BucketMeta(opts) => execute_bucket_meta(opts).await,
    }
}

/// One drive's contribution: its bitrot-verified inline erasure shard.
struct DriveShard {
    drive: String,
    /// 1-based erasure shard index (`ErasureInfo::index`).
    index: usize,
    data: Vec<u8>,
    file_info: FileInfo,
}

async fn execute_bucket_meta(opts: &InspectBucketMetaOpts) -> Result<()> {
    let output = opts.raw.then(|| {
        opts.out
            .clone()
            .unwrap_or_else(|| PathBuf::from(format!("bucket-meta-{}", opts.bucket)))
    });
    if let Some(out) = &output {
        ensure_output_outside_source_drives(out, &opts.paths)?;
    }

    let blob = reconstruct_metadata_blob(opts).await?;

    BucketMetadata::check_header(&blob).map_err(|e| Error::other(format!("reconstructed blob failed header check: {e}")))?;
    let format = u16::from_le_bytes([blob[0], blob[1]]);
    let version = u16::from_le_bytes([blob[2], blob[3]]);
    // unmarshal is the msgpack decode only — parse_all_configs is intentionally
    // NOT called, so unparsable config XML still exports byte-exactly.
    let bm = BucketMetadata::unmarshal(&blob[4..]).map_err(|e| Error::other(format!("unmarshal bucket metadata: {e}")))?;

    println!("bucket   : {}", bm.name);
    println!("format   : {format}");
    println!("version  : {version}");
    println!("created  : {}", format_ts(bm.created));
    println!("blob-size: {} bytes", blob.len());
    println!();

    let configs = stored_configs(&bm);
    println!("{:<28} {:>10}  updated-at", "config", "bytes");
    for (name, bytes, updated_at) in &configs {
        println!("{:<28} {:>10}  {}", name, bytes.len(), format_ts(*updated_at));
    }

    if let Some(out) = output {
        tokio::fs::create_dir(&out).await?;
        let blob_path = out.join(BUCKET_METADATA_FILE);
        write_new_file(&blob_path, &blob).await?;
        let mut written = 1usize;
        for (name, bytes, _) in &configs {
            write_new_file(&out.join(name), bytes).await?;
            written += 1;
        }
        println!();
        println!("wrote {written} files (raw bytes, incl. {BUCKET_METADATA_FILE}) to {}", out.display());
    }

    Ok(())
}

/// The stored raw XML config fields, in the on-disk constant-table order.
/// Empty means the config was never set (or was deleted).
fn stored_configs(bm: &BucketMetadata) -> Vec<(&'static str, &[u8], OffsetDateTime)> {
    use crate::storage_api::inspect::bucket_metadata::*;
    vec![
        (
            BUCKET_NOTIFICATION_CONFIG,
            bm.notification_config_xml.as_slice(),
            bm.notification_config_updated_at,
        ),
        (
            BUCKET_LIFECYCLE_CONFIG,
            bm.lifecycle_config_xml.as_slice(),
            bm.lifecycle_config_updated_at,
        ),
        (OBJECT_LOCK_CONFIG, bm.object_lock_config_xml.as_slice(), bm.object_lock_config_updated_at),
        (
            BUCKET_VERSIONING_CONFIG,
            bm.versioning_config_xml.as_slice(),
            bm.versioning_config_updated_at,
        ),
        (BUCKET_SSECONFIG, bm.encryption_config_xml.as_slice(), bm.encryption_config_updated_at),
        (BUCKET_TAGGING_CONFIG, bm.tagging_config_xml.as_slice(), bm.tagging_config_updated_at),
        (
            BUCKET_REPLICATION_CONFIG,
            bm.replication_config_xml.as_slice(),
            bm.replication_config_updated_at,
        ),
        (BUCKET_CORS_CONFIG, bm.cors_config_xml.as_slice(), bm.cors_config_updated_at),
        (BUCKET_LOGGING_CONFIG, bm.logging_config_xml.as_slice(), bm.logging_config_updated_at),
        (BUCKET_WEBSITE_CONFIG, bm.website_config_xml.as_slice(), bm.website_config_updated_at),
        (
            BUCKET_ACCELERATE_CONFIG,
            bm.accelerate_config_xml.as_slice(),
            bm.accelerate_config_updated_at,
        ),
        (
            BUCKET_REQUEST_PAYMENT_CONFIG,
            bm.request_payment_config_xml.as_slice(),
            bm.request_payment_config_updated_at,
        ),
        (
            BUCKET_PUBLIC_ACCESS_BLOCK_CONFIG,
            bm.public_access_block_config_xml.as_slice(),
            bm.public_access_block_config_updated_at,
        ),
    ]
}

fn ensure_output_outside_source_drives(out: &Path, drives: &[String]) -> Result<()> {
    match std::fs::symlink_metadata(out) {
        Ok(_) => {
            return Err(Error::new(
                ErrorKind::AlreadyExists,
                format!(
                    "output path {} already exists or is a symbolic link; choose a new directory",
                    out.display()
                ),
            ));
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => {
            return Err(Error::new(error.kind(), format!("inspect output path {}: {error}", out.display())));
        }
    }

    let out = resolve_path(out)?;
    for drive in drives {
        let drive = resolve_path(Path::new(drive))?;
        if out.starts_with(&drive) {
            return Err(Error::other(format!(
                "output path {} is inside source drive {}; choose an external --out directory",
                out.display(),
                drive.display()
            )));
        }
    }
    Ok(())
}

async fn write_new_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = tokio::fs::OpenOptions::new().write(true).create_new(true).open(path).await?;
    file.write_all(bytes).await?;
    file.flush().await
}

/// Resolve symlinks in the existing prefix while preserving a not-yet-created tail.
fn resolve_path(path: &Path) -> Result<PathBuf> {
    let absolute = std::path::absolute(path)?;
    let mut cursor = absolute.as_path();
    let mut missing: Vec<OsString> = Vec::new();
    while !cursor.exists() {
        let Some(name) = cursor.file_name() else {
            return Err(Error::other(format!("cannot resolve path {}", path.display())));
        };
        missing.push(name.to_os_string());
        let Some(parent) = cursor.parent() else {
            return Err(Error::other(format!("cannot resolve path {}", path.display())));
        };
        cursor = parent;
    }

    let mut resolved = cursor.canonicalize()?;
    for component in missing.into_iter().rev() {
        resolved.push(component);
    }
    Ok(resolved)
}

fn format_ts(ts: OffsetDateTime) -> String {
    if ts == OffsetDateTime::UNIX_EPOCH {
        return "-".to_string();
    }
    ts.format(&Rfc3339).unwrap_or_else(|_| ts.to_string())
}

/// Reconstruct the `.metadata.bin` object body from the drives' inline shards.
async fn reconstruct_metadata_blob(opts: &InspectBucketMetaOpts) -> Result<Vec<u8>> {
    check_valid_bucket_name_strict(&opts.bucket).map_err(|e| Error::other(format!("invalid bucket {:?}: {e}", opts.bucket)))?;

    let mut shards: Vec<DriveShard> = Vec::new();
    let mut failures: Vec<String> = Vec::new();

    for drive in &opts.paths {
        let xl_path = Path::new(drive)
            .join(META_BUCKET)
            .join(BUCKET_META_PREFIX)
            .join(&opts.bucket)
            .join(BUCKET_METADATA_FILE)
            .join("xl.meta");
        match read_drive_shard(drive, &xl_path).await {
            Ok((fi, shard)) => {
                let index = fi.erasure.index;
                shards.push(DriveShard {
                    drive: drive.clone(),
                    index,
                    data: shard,
                    file_info: fi,
                });
            }
            Err(e) => failures.push(format!("{drive}: {e}")),
        }
    }

    if shards.is_empty() {
        return Err(Error::other(format!(
            "no drive yielded a readable bucket-metadata shard for bucket {:?}:\n  {}",
            opts.bucket,
            failures.join("\n  ")
        )));
    }
    for failure in &failures {
        eprintln!("warning: {failure}");
    }

    let (fi, shards) = select_quorum_shards(shards)?;

    let k = fi.erasure.data_blocks;
    let m = fi.erasure.parity_blocks;
    let size = usize::try_from(fi.size).map_err(|_| Error::other(format!("negative object size {}", fi.size)))?;
    let erasure = Erasure::try_new_with_options(k, m, fi.erasure.block_size, fi.uses_legacy_checksum)
        .map_err(|e| Error::other(format!("invalid erasure geometry in xl.meta (k={k}, m={m}): {e}")))?;

    reconstruct_shards(&erasure, size, shards)
}

struct ShardGroup {
    hash: [u8; 32],
    file_info: FileInfo,
    shards: Vec<DriveShard>,
}

/// Select the newest object identity that has enough distinct shards for a read quorum.
fn select_quorum_shards(shards: Vec<DriveShard>) -> Result<(FileInfo, Vec<DriveShard>)> {
    let mut groups: Vec<ShardGroup> = Vec::new();
    for shard in shards {
        let hash = file_info_quorum_hash(&shard.file_info);
        if let Some(group) = groups.iter_mut().find(|group| group.hash == hash) {
            group.shards.push(shard);
        } else {
            groups.push(ShardGroup {
                hash,
                file_info: shard.file_info.clone(),
                shards: vec![shard],
            });
        }
    }

    let mut eligible = groups
        .into_iter()
        .filter(|group| {
            let total = group.file_info.erasure.data_blocks + group.file_info.erasure.parity_blocks;
            let mut indices = group
                .shards
                .iter()
                .map(|shard| shard.index)
                .filter(|index| *index > 0 && *index <= total)
                .collect::<Vec<_>>();
            indices.sort_unstable();
            indices.dedup();
            group.file_info.erasure.data_blocks > 0 && indices.len() >= group.file_info.erasure.data_blocks
        })
        .collect::<Vec<_>>();

    let Some(latest) = eligible.iter().map(|group| group.file_info.mod_time).max() else {
        return Err(Error::other("no consistent object-metadata identity reached read quorum"));
    };
    let latest_count = eligible.iter().filter(|group| group.file_info.mod_time == latest).count();
    if latest_count != 1 {
        return Err(Error::other(format!(
            "{latest_count} different object-metadata identities reached read quorum at the latest modification time"
        )));
    }

    let selected = eligible
        .iter()
        .position(|group| group.file_info.mod_time == latest)
        .ok_or_else(|| Error::other("latest read-quorum identity disappeared during selection"))?;
    let mut group = eligible.swap_remove(selected);
    let total = group.file_info.erasure.data_blocks + group.file_info.erasure.parity_blocks;
    group.shards.retain(|shard| shard.index > 0 && shard.index <= total);
    group
        .shards
        .sort_by(|left, right| left.index.cmp(&right.index).then_with(|| left.drive.cmp(&right.drive)));

    let mut distinct: Vec<DriveShard> = Vec::with_capacity(group.shards.len());
    for shard in group.shards {
        if let Some(previous) = distinct.last()
            && previous.index == shard.index
        {
            if previous.data != shard.data {
                return Err(Error::other(format!(
                    "object-metadata identity has conflicting verified bytes for erasure shard {}",
                    shard.index
                )));
            }
            continue;
        }
        distinct.push(shard);
    }

    Ok((group.file_info, distinct))
}

/// Reconstruct verified data-or-parity shards block by block.
fn reconstruct_shards(erasure: &Erasure, size: usize, mut shards: Vec<DriveShard>) -> Result<Vec<u8>> {
    let k = erasure.data_shards;
    let m = erasure.parity_shards;
    let total_shards = k.checked_add(m).ok_or_else(|| Error::other("erasure shard count overflow"))?;

    shards.sort_by_key(|s| s.index);
    shards.dedup_by_key(|s| s.index);
    if shards.len() < k {
        return Err(Error::other(format!(
            "need at least {k} distinct shards to reconstruct (data_blocks={k}, parity_blocks={m}), got {} from drives [{}]; pass more --path drive roots",
            shards.len(),
            shards.iter().map(|s| s.drive.as_str()).collect::<Vec<_>>().join(", ")
        )));
    }

    let shard_total = erasure.shard_file_offset(0, size, size);
    let mut body = Vec::with_capacity(size);
    let mut object_off = 0usize;
    let mut shard_off = 0usize;
    while object_off < size {
        let block_len = (size - object_off).min(erasure.block_size);
        let block_shard_len = if object_off + block_len >= size {
            shard_total
                .checked_sub(shard_off)
                .ok_or_else(|| Error::other("erasure shard offset exceeds the expected shard length"))?
        } else {
            erasure.shard_size()
        };

        let mut slots: Vec<Option<Vec<u8>>> = vec![None; total_shards];
        for shard in &shards {
            if shard.index == 0 || shard.index > total_shards {
                continue;
            }
            let end = shard_off
                .checked_add(block_shard_len)
                .ok_or_else(|| Error::other("erasure shard range overflow"))?;
            if end <= shard.data.len() {
                slots[shard.index - 1] = Some(shard.data[shard_off..end].to_vec());
            }
        }
        if slots.iter().flatten().count() < k {
            return Err(Error::other(format!(
                "block at offset {object_off}: only {} of the required {k} shards are present",
                slots.iter().flatten().count()
            )));
        }
        erasure
            .decode_data(&mut slots)
            .map_err(|e| Error::other(format!("erasure reconstruction failed at object offset {object_off}: {e}")))?;

        let block_capacity = block_shard_len
            .checked_mul(k)
            .ok_or_else(|| Error::other("reconstructed block size overflow"))?;
        let mut block = Vec::with_capacity(block_capacity);
        for slot in slots.iter().take(k) {
            let Some(data) = slot else {
                return Err(Error::other(format!("data shard missing after reconstruction at offset {object_off}")));
            };
            block.extend_from_slice(data);
        }
        let data = block
            .get(..block_len)
            .ok_or_else(|| Error::other(format!("reconstructed block at offset {object_off} is too short")))?;
        body.extend_from_slice(data);
        object_off += block_len;
        shard_off += block_shard_len;
    }

    Ok(body)
}

/// Read one drive's xl.meta and return the bitrot-verified inline shard bytes.
async fn read_drive_shard(drive: &str, xl_path: &Path) -> Result<(FileInfo, Vec<u8>)> {
    let metadata = tokio::fs::metadata(xl_path)
        .await
        .map_err(|e| Error::other(format!("inspect {}: {e}", xl_path.display())))?;
    if metadata.len() > MAX_XL_META_BYTES {
        return Err(Error::other(format!(
            "{}: xl.meta is {} bytes, above the {}-byte inspection limit",
            xl_path.display(),
            metadata.len(),
            MAX_XL_META_BYTES
        )));
    }
    let bytes = tokio::fs::read(xl_path)
        .await
        .map_err(|e| Error::other(format!("read {}: {e}", xl_path.display())))?;
    let fm = FileMeta::load(&bytes).map_err(|e| Error::other(format!("parse {}: {e}", xl_path.display())))?;
    let fi = fm
        .into_fileinfo(META_BUCKET, BUCKET_METADATA_FILE, "", true, false, false)
        .map_err(|e| Error::other(format!("resolve version in {}: {e}", xl_path.display())))?;

    let Some(inline) = fi.data.clone() else {
        return Err(Error::other(format!(
            "{}: bucket metadata is not inlined in xl.meta on drive {drive}; this tool only reads inline shards",
            xl_path.display()
        )));
    };

    let checksum_info = fi.erasure.get_checksum_info(1);
    let algo = if fi.uses_legacy_checksum && checksum_info.algorithm == HashAlgorithm::HighwayHash256S {
        HashAlgorithm::HighwayHash256SLegacy
    } else {
        checksum_info.algorithm
    };

    let k = fi.erasure.data_blocks;
    let erasure = Erasure::try_new_with_options(k, fi.erasure.parity_blocks, fi.erasure.block_size, fi.uses_legacy_checksum)
        .map_err(|e| Error::other(format!("{}: invalid erasure geometry in xl.meta: {e}", xl_path.display())))?;
    let size = usize::try_from(fi.size).map_err(|_| Error::other(format!("negative object size {}", fi.size)))?;
    if size > MAX_BUCKET_METADATA_BYTES {
        return Err(Error::other(format!(
            "{}: bucket metadata body is {size} bytes, above the {MAX_BUCKET_METADATA_BYTES}-byte inspection limit",
            xl_path.display()
        )));
    }

    // Expected shard layout: per erasure block one `[hash][data]` bitrot frame.
    let shard_total = erasure.shard_file_offset(0, size, size);
    let n_blocks = if shard_total == 0 {
        0
    } else {
        shard_total.div_ceil(erasure.shard_size().max(1))
    };
    let hash_bytes = n_blocks
        .checked_mul(algo.size())
        .ok_or_else(|| Error::other(format!("{}: bitrot framing size overflow", xl_path.display())))?;
    let framed = shard_total
        .checked_add(hash_bytes)
        .ok_or_else(|| Error::other(format!("{}: inline shard size overflow", xl_path.display())))?;

    // Writers differ in shard padding (RustFS legacy even padding vs MinIO's
    // exact sizes), so trust the bytes actually present when the computed
    // framing disagrees but a single-frame interpretation is consistent.
    let (shard_total, n_blocks) = if framed == inline.len() {
        (shard_total, n_blocks)
    } else if inline.len() >= algo.size() && n_blocks <= 1 {
        (inline.len() - algo.size(), 1)
    } else {
        return Err(Error::other(format!(
            "{}: inline shard is {} bytes but the erasure layout expects {framed} ({} data + {n_blocks} bitrot hashes of {}); refusing to guess",
            xl_path.display(),
            inline.len(),
            shard_total,
            algo.size()
        )));
    };

    // Per-frame data capacity: the erasure shard size, except in the
    // single-frame fallback where the whole shard is one bitrot frame.
    let frame_cap = if n_blocks <= 1 {
        shard_total.max(1)
    } else {
        erasure.shard_size().max(1)
    };
    let mut reader = BitrotReader::new(std::io::Cursor::new(inline.to_vec()), frame_cap, algo, false);
    let mut shard = vec![0u8; shard_total];
    let mut off = 0usize;
    for _ in 0..n_blocks {
        let want = (shard_total - off).min(frame_cap);
        let read = reader
            .read(&mut shard[off..off + want])
            .await
            .map_err(|e| Error::other(format!("{}: bitrot verification failed: {e}", xl_path.display())))?;
        off += read;
    }
    if off != shard_total {
        return Err(Error::other(format!(
            "{}: short shard read ({off} of {shard_total} bytes)",
            xl_path.display()
        )));
    }

    Ok((fi, shard))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_hex(s: &str) -> Vec<u8> {
        let s: String = s.chars().filter(|c| c.is_ascii_hexdigit()).collect();
        s.as_bytes()
            .chunks(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect()
    }

    /// End-to-end over the drive layout this tool reads: a MinIO-written
    /// bucket-metadata xl.meta (real fixture from backlog#580) placed at
    /// `<drive>/.rustfs.sys/buckets/<bucket>/.metadata.bin/xl.meta` must
    /// reconstruct to a parseable `.metadata.bin` blob — bitrot verified,
    /// without going through `parse_all_configs`.
    #[tokio::test]
    async fn reconstructs_bucket_metadata_blob_from_a_single_drive() {
        let xlmeta = decode_hex(include_str!("../../crates/ecstore/tests/fixtures/minio/bucket_metadata_full.xlmeta.hex"));

        let dir = tempfile::tempdir().expect("tempdir");
        let obj_dir = dir
            .path()
            .join(META_BUCKET)
            .join(BUCKET_META_PREFIX)
            .join("interop")
            .join(BUCKET_METADATA_FILE);
        std::fs::create_dir_all(&obj_dir).expect("create drive layout");
        std::fs::write(obj_dir.join("xl.meta"), &xlmeta).expect("write xl.meta");

        let opts = InspectBucketMetaOpts {
            paths: vec![dir.path().to_string_lossy().into_owned()],
            bucket: "interop".to_string(),
            raw: false,
            out: None,
        };
        let blob = reconstruct_metadata_blob(&opts).await.expect("reconstruct blob");

        BucketMetadata::check_header(&blob).expect("valid .metadata.bin header");
        let bm = BucketMetadata::unmarshal(&blob[4..]).expect("unmarshal");
        assert_eq!(bm.name, "interop");
        assert!(!bm.lifecycle_config_xml.is_empty(), "raw lifecycle XML exported");
        assert!(!bm.versioning_config_xml.is_empty(), "raw versioning XML exported");
    }

    /// A bucket with no metadata object on any drive must fail with a message
    /// naming every drive, not panic or fabricate an empty result.
    #[tokio::test]
    async fn missing_metadata_reports_every_drive() {
        let dir = tempfile::tempdir().expect("tempdir");
        let opts = InspectBucketMetaOpts {
            paths: vec![dir.path().to_string_lossy().into_owned()],
            bucket: "absent".to_string(),
            raw: false,
            out: None,
        };
        let err = reconstruct_metadata_blob(&opts).await.expect_err("must fail");
        let message = err.to_string();
        assert!(message.contains("no drive yielded"), "diagnostic prefix present: {message}");
        assert!(message.contains(&dir.path().to_string_lossy().into_owned()), "drive named: {message}");
    }

    /// Corrupting one byte of the inline shard must fail bitrot verification —
    /// the tool never exports bytes that did not verify.
    #[tokio::test]
    async fn corrupted_inline_shard_fails_bitrot_verification() {
        let mut xlmeta = decode_hex(include_str!("../../crates/ecstore/tests/fixtures/minio/bucket_metadata_full.xlmeta.hex"));
        // Flip a byte near the end, inside the inline data region (the xl.meta
        // header/versions live at the front).
        let target = xlmeta.len() - 64;
        xlmeta[target] ^= 0xff;

        let dir = tempfile::tempdir().expect("tempdir");
        let obj_dir = dir
            .path()
            .join(META_BUCKET)
            .join(BUCKET_META_PREFIX)
            .join("interop")
            .join(BUCKET_METADATA_FILE);
        std::fs::create_dir_all(&obj_dir).expect("create drive layout");
        std::fs::write(obj_dir.join("xl.meta"), &xlmeta).expect("write xl.meta");

        let opts = InspectBucketMetaOpts {
            paths: vec![dir.path().to_string_lossy().into_owned()],
            bucket: "interop".to_string(),
            raw: false,
            out: None,
        };
        let result = reconstruct_metadata_blob(&opts).await;
        assert!(result.is_err(), "corrupted shard must not reconstruct");
    }

    #[test]
    fn export_list_contains_only_persisted_xml_configs() {
        let bm = BucketMetadata::default();
        let configs = stored_configs(&bm);

        assert_eq!(configs.len(), 13);
        assert!(configs.iter().all(|(name, _, _)| name.ends_with(".xml")));
    }

    #[tokio::test]
    async fn rejects_invalid_bucket_before_reading_drive_paths() {
        let dir = tempfile::tempdir().expect("tempdir");
        let opts = InspectBucketMetaOpts {
            paths: vec![dir.path().to_string_lossy().into_owned()],
            bucket: "../escape".to_string(),
            raw: false,
            out: None,
        };

        let err = reconstruct_metadata_blob(&opts).await.expect_err("invalid bucket must fail");
        assert!(err.to_string().contains("invalid bucket"), "unexpected error: {err}");
    }

    #[tokio::test]
    async fn raw_export_cannot_write_inside_a_source_drive() {
        let xlmeta = decode_hex(include_str!("../../crates/ecstore/tests/fixtures/minio/bucket_metadata_full.xlmeta.hex"));
        let dir = tempfile::tempdir().expect("tempdir");
        let obj_dir = dir
            .path()
            .join(META_BUCKET)
            .join(BUCKET_META_PREFIX)
            .join("interop")
            .join(BUCKET_METADATA_FILE);
        std::fs::create_dir_all(&obj_dir).expect("create drive layout");
        std::fs::write(obj_dir.join("xl.meta"), &xlmeta).expect("write xl.meta");

        let opts = InspectBucketMetaOpts {
            paths: vec![dir.path().to_string_lossy().into_owned()],
            bucket: "interop".to_string(),
            raw: true,
            out: Some(dir.path().join("forensic-export")),
        };

        let err = execute_bucket_meta(&opts)
            .await
            .expect_err("source drives must remain read-only");
        assert!(err.to_string().contains("source drive"), "unexpected error: {err}");
    }

    #[tokio::test]
    async fn raw_export_rejects_an_existing_output_directory() {
        let drive = tempfile::tempdir().expect("drive tempdir");
        let export = tempfile::tempdir().expect("existing export directory");
        let sentinel = export.path().join("sentinel");
        std::fs::write(&sentinel, b"unchanged").expect("write sentinel");
        let opts = InspectBucketMetaOpts {
            paths: vec![drive.path().to_string_lossy().into_owned()],
            bucket: "interop".to_string(),
            raw: true,
            out: Some(export.path().to_path_buf()),
        };

        let err = execute_bucket_meta(&opts)
            .await
            .expect_err("existing output must be rejected");
        assert!(err.to_string().contains("already exists"), "unexpected error: {err}");
        assert_eq!(std::fs::read(sentinel).expect("read sentinel"), b"unchanged");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn raw_export_rejects_a_symlink_to_a_source_drive_without_writing() {
        use std::os::unix::fs::symlink;

        let drive = tempfile::tempdir().expect("drive tempdir");
        let target = drive.path().join("existing-target");
        std::fs::create_dir(&target).expect("create target");
        let sentinel = target.join("sentinel");
        std::fs::write(&sentinel, b"source-bytes").expect("write sentinel");
        let export_parent = tempfile::tempdir().expect("export parent");
        let out = export_parent.path().join("raw");
        symlink(&target, &out).expect("create output symlink");
        let opts = InspectBucketMetaOpts {
            paths: vec![drive.path().to_string_lossy().into_owned()],
            bucket: "interop".to_string(),
            raw: true,
            out: Some(out),
        };

        let err = execute_bucket_meta(&opts).await.expect_err("output symlink must be rejected");
        assert!(err.to_string().contains("already exists"), "unexpected error: {err}");
        assert_eq!(std::fs::read(sentinel).expect("read sentinel"), b"source-bytes");
    }

    #[tokio::test]
    async fn raw_export_file_creation_never_overwrites_an_existing_file() {
        let export = tempfile::tempdir().expect("export tempdir");
        let path = export.path().join("versioning.xml");
        std::fs::write(&path, b"original").expect("write existing file");

        let err = write_new_file(&path, b"replacement")
            .await
            .expect_err("existing file must not be overwritten");
        assert_eq!(err.kind(), ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(path).expect("read existing file"), b"original");
    }

    #[tokio::test]
    async fn raw_export_writes_every_persisted_xml_field_including_empty_ones() {
        let xlmeta = decode_hex(include_str!("../../crates/ecstore/tests/fixtures/minio/bucket_metadata_full.xlmeta.hex"));
        let drive = tempfile::tempdir().expect("drive tempdir");
        let export = tempfile::tempdir().expect("export tempdir");
        let obj_dir = drive
            .path()
            .join(META_BUCKET)
            .join(BUCKET_META_PREFIX)
            .join("interop")
            .join(BUCKET_METADATA_FILE);
        std::fs::create_dir_all(&obj_dir).expect("create drive layout");
        std::fs::write(obj_dir.join("xl.meta"), &xlmeta).expect("write xl.meta");

        let out = export.path().join("raw");
        let opts = InspectBucketMetaOpts {
            paths: vec![drive.path().to_string_lossy().into_owned()],
            bucket: "interop".to_string(),
            raw: true,
            out: Some(out.clone()),
        };
        execute_bucket_meta(&opts).await.expect("raw export");

        let files = std::fs::read_dir(&out).expect("read export directory").count();
        assert_eq!(files, 14, ".metadata.bin plus all 13 persisted XML fields");
        assert_eq!(std::fs::read(out.join("cors.xml")).expect("empty CORS field exists"), b"");
    }

    #[test]
    fn reconstructs_from_parity_when_a_data_shard_is_missing() {
        let payload = b"bucket metadata needs parity recovery";
        let erasure = Erasure::try_new(2, 2, 64).expect("valid erasure geometry");
        let encoded = erasure.encode_data(payload).expect("encode payload");
        let shards = encoded
            .into_iter()
            .enumerate()
            .filter(|(index, _)| *index != 0)
            .map(|(index, data)| DriveShard {
                drive: format!("drive-{index}"),
                index: index + 1,
                data: data.to_vec(),
                file_info: FileInfo::default(),
            })
            .collect();

        let body = reconstruct_shards(&erasure, payload.len(), shards).expect("reconstruct from parity");
        assert_eq!(body, payload);
    }

    fn quorum_test_shard(drive: &str, mod_time: OffsetDateTime, index: usize) -> DriveShard {
        let file_info = FileInfo {
            size: 32,
            mod_time: Some(mod_time),
            erasure: rustfs_filemeta::ErasureInfo {
                data_blocks: 2,
                parity_blocks: 2,
                block_size: 64,
                distribution: vec![1, 2, 3, 4],
                index,
                ..Default::default()
            },
            ..Default::default()
        };
        DriveShard {
            drive: drive.to_string(),
            index,
            data: vec![index as u8; 16],
            file_info,
        }
    }

    #[test]
    fn metadata_selection_ignores_stale_first_drive_and_is_order_independent() {
        let stale_time = OffsetDateTime::from_unix_timestamp(1).expect("stale timestamp");
        let current_time = OffsetDateTime::from_unix_timestamp(2).expect("current timestamp");
        let stale_first = vec![
            quorum_test_shard("stale-1", stale_time, 1),
            quorum_test_shard("stale-2", stale_time, 2),
            quorum_test_shard("current-2", current_time, 2),
            quorum_test_shard("current-1", current_time, 1),
        ];
        let current_first = vec![
            quorum_test_shard("current-1", current_time, 1),
            quorum_test_shard("current-2", current_time, 2),
            quorum_test_shard("stale-1", stale_time, 1),
            quorum_test_shard("stale-2", stale_time, 2),
        ];

        for shards in [stale_first, current_first] {
            let (selected, selected_shards) = select_quorum_shards(shards).expect("current identity reaches read quorum");
            assert_eq!(selected.mod_time, Some(current_time));
            assert_eq!(selected_shards.len(), 2);
            assert!(selected_shards.iter().all(|shard| shard.drive.starts_with("current-")));
        }
    }

    #[test]
    fn metadata_selection_requires_distinct_erasure_indices_for_quorum() {
        let current_time = OffsetDateTime::from_unix_timestamp(2).expect("current timestamp");
        let shards = vec![
            quorum_test_shard("current-a", current_time, 1),
            quorum_test_shard("current-b", current_time, 1),
        ];

        let Err(err) = select_quorum_shards(shards) else {
            panic!("duplicate shard indices must not form quorum");
        };
        assert!(err.to_string().contains("reached read quorum"), "unexpected error: {err}");
    }
}
