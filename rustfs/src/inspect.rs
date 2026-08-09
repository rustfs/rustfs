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
use uuid::Uuid;

const META_BUCKET: &str = ".rustfs.sys";
const BUCKET_META_PREFIX: &str = "buckets";
const BUCKET_METADATA_FILE: &str = ".metadata.bin";
const BUCKET_INCARNATION_FILE: &str = ".bucket-incarnation";
const MAX_XL_META_BYTES: u64 = 4 * 1024 * 1024;
const MAX_BUCKET_METADATA_BYTES: usize = 16 * 1024 * 1024;
const MAX_SHARD_SOURCE_BYTES: u64 = 20 * 1024 * 1024;

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
    eprintln!(
        "warning: mount source drives read-only for forensic use; writable mounts allow atime changes and path replacement races"
    );
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

    let blob = reconstruct_system_object(opts, BUCKET_METADATA_FILE).await?;
    BucketMetadata::check_header(&blob).map_err(|e| Error::other(format!("reconstructed blob failed header check: {e}")))?;
    let metadata = BucketMetadata::unmarshal(&blob[4..]).map_err(|e| Error::other(format!("unmarshal bucket metadata: {e}")))?;
    validate_bucket_incarnation(opts, &metadata).await?;
    Ok(blob)
}

async fn validate_bucket_incarnation(opts: &InspectBucketMetaOpts, metadata: &BucketMetadata) -> Result<()> {
    let mut sidecar_present = false;
    for drive in &opts.paths {
        let path = system_object_dir(drive, &opts.bucket, BUCKET_INCARNATION_FILE).join("xl.meta");
        match std::fs::symlink_metadata(&path) {
            Ok(_) => sidecar_present = true,
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => {
                return Err(Error::new(
                    error.kind(),
                    format!("inspect bucket incarnation sidecar {}: {error}", path.display()),
                ));
            }
        }
    }

    if !sidecar_present {
        if metadata.bucket_incarnation_id.is_nil() {
            return Ok(());
        }
        return Err(Error::other(format!(
            "bucket incarnation sidecar is missing for new-format metadata: {}",
            opts.bucket
        )));
    }

    let bytes = reconstruct_system_object(opts, BUCKET_INCARNATION_FILE).await?;
    let incarnation =
        Uuid::from_slice(&bytes).map_err(|error| Error::other(format!("persisted bucket incarnation is invalid: {error}")))?;
    if incarnation.is_nil() {
        return Err(Error::other("persisted bucket incarnation is nil"));
    }
    if !metadata.bucket_incarnation_id.is_nil() && metadata.bucket_incarnation_id != incarnation {
        return Err(Error::other("bucket incarnation sidecar does not match bucket metadata"));
    }
    Ok(())
}

fn system_object_dir(drive: &str, bucket: &str, object: &str) -> PathBuf {
    Path::new(drive)
        .join(META_BUCKET)
        .join(BUCKET_META_PREFIX)
        .join(bucket)
        .join(object)
}

async fn reconstruct_system_object(opts: &InspectBucketMetaOpts, object: &str) -> Result<Vec<u8>> {
    let mut shards: Vec<DriveShard> = Vec::new();
    let mut failures: Vec<String> = Vec::new();

    for drive in &opts.paths {
        let object_dir = system_object_dir(drive, &opts.bucket, object);
        let xl_path = object_dir.join("xl.meta");
        match read_drive_shard(drive, object, &object_dir, &xl_path).await {
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
            "no drive yielded a readable shard for object {object:?} in bucket {:?}:\n  {}",
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
            let data_blocks = group.file_info.erasure.data_blocks;
            let parity_blocks = group.file_info.erasure.parity_blocks;
            let Some(total) = data_blocks.checked_add(parity_blocks) else {
                return false;
            };
            let write_quorum = if data_blocks == parity_blocks {
                data_blocks.checked_add(1)
            } else {
                Some(data_blocks)
            };
            let Some(write_quorum) = write_quorum else {
                return false;
            };
            let mut indices = group
                .shards
                .iter()
                .map(|shard| shard.index)
                .filter(|index| *index > 0 && *index <= total)
                .collect::<Vec<_>>();
            indices.sort_unstable();
            indices.dedup();
            data_blocks > 0 && indices.len() >= write_quorum
        })
        .collect::<Vec<_>>();

    let Some(latest) = eligible.iter().map(|group| group.file_info.mod_time).max() else {
        return Err(Error::other("no consistent object-metadata identity reached write quorum"));
    };
    let latest_count = eligible.iter().filter(|group| group.file_info.mod_time == latest).count();
    if latest_count != 1 {
        return Err(Error::other(format!(
            "{latest_count} different object-metadata identities reached write quorum at the latest modification time"
        )));
    }

    let selected = eligible
        .iter()
        .position(|group| group.file_info.mod_time == latest)
        .ok_or_else(|| Error::other("latest read-quorum identity disappeared during selection"))?;
    let mut group = eligible.swap_remove(selected);
    let total = group
        .file_info
        .erasure
        .data_blocks
        .checked_add(group.file_info.erasure.parity_blocks)
        .ok_or_else(|| Error::other("erasure shard count overflow"))?;
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

/// Read one drive's xl.meta and return its bitrot-verified inline or `part.1` shard.
async fn read_drive_shard(drive: &str, object: &str, object_dir: &Path, xl_path: &Path) -> Result<(FileInfo, Vec<u8>)> {
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
        .into_fileinfo(META_BUCKET, object, "", true, false, true)
        .map_err(|e| Error::other(format!("resolve version in {}: {e}", xl_path.display())))?;
    fi.validate_for_metadata_read()
        .map_err(|error| Error::other(format!("{} on drive {drive}: invalid persisted metadata: {error}", xl_path.display())))?;

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
            "{}: system object body is {size} bytes, above the {MAX_BUCKET_METADATA_BYTES}-byte inspection limit",
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
    let streaming = matches!(algo, HashAlgorithm::HighwayHash256S | HashAlgorithm::HighwayHash256SLegacy);
    let hash_bytes = if streaming {
        n_blocks
            .checked_mul(algo.size())
            .ok_or_else(|| Error::other(format!("{}: bitrot framing size overflow", xl_path.display())))?
    } else {
        0
    };
    let framed = shard_total
        .checked_add(hash_bytes)
        .ok_or_else(|| Error::other(format!("{}: shard source size overflow", xl_path.display())))?;

    let source = if let Some(inline) = fi.data.as_ref() {
        inline.to_vec()
    } else {
        let Some(part) = fi.parts.first().filter(|part| part.number == 1) else {
            return Err(Error::other(format!("{}: system object has no supported part.1", xl_path.display())));
        };
        if fi.parts.len() != 1 {
            return Err(Error::other(format!(
                "{}: system object has {} parts; offline inspection supports exactly one",
                xl_path.display(),
                fi.parts.len()
            )));
        }
        let mut part_path = object_dir.to_path_buf();
        if let Some(data_dir) = fi.data_dir {
            part_path.push(data_dir.to_string());
        }
        part_path.push(format!("part.{}", part.number));
        let metadata = tokio::fs::metadata(&part_path)
            .await
            .map_err(|error| Error::other(format!("inspect {} on drive {drive}: {error}", part_path.display())))?;
        if metadata.len() > MAX_SHARD_SOURCE_BYTES {
            return Err(Error::other(format!(
                "{}: shard source is {} bytes, above the {}-byte inspection limit",
                part_path.display(),
                metadata.len(),
                MAX_SHARD_SOURCE_BYTES
            )));
        }
        tokio::fs::read(&part_path)
            .await
            .map_err(|error| Error::other(format!("read {}: {error}", part_path.display())))?
    };

    // Writers differ in shard padding (RustFS legacy even padding vs MinIO's
    // exact sizes), so trust the bytes actually present when the computed
    // framing disagrees but a single-frame interpretation is consistent.
    let (shard_total, n_blocks) = if framed == source.len() {
        (shard_total, n_blocks)
    } else if streaming && source.len() >= algo.size() && n_blocks <= 1 {
        (source.len() - algo.size(), 1)
    } else {
        return Err(Error::other(format!(
            "{}: shard source is {} bytes but the erasure layout expects {framed} ({} data + {n_blocks} bitrot hashes of {}); refusing to guess",
            xl_path.display(),
            source.len(),
            shard_total,
            if streaming { algo.size() } else { 0 }
        )));
    };

    if !streaming {
        if checksum_info.hash.len() != algo.size() {
            return Err(Error::other(format!(
                "{}: legacy whole-file bitrot checksum has {} bytes, expected {} for {:?}",
                xl_path.display(),
                checksum_info.hash.len(),
                algo.size(),
                algo
            )));
        }
        let actual = algo.hash_encode(&source);
        let matches = actual.as_ref() == checksum_info.hash.as_ref();
        drop(actual);
        if !matches {
            return Err(Error::other(format!("{}: legacy whole-file bitrot checksum mismatch", xl_path.display())));
        }
        return Ok((fi, source));
    }

    // Per-frame data capacity: the erasure shard size, except in the
    // single-frame fallback where the whole shard is one bitrot frame.
    let frame_cap = if n_blocks <= 1 {
        shard_total.max(1)
    } else {
        erasure.shard_size().max(1)
    };
    let mut reader = BitrotReader::new(std::io::Cursor::new(source), frame_cap, algo, false);
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
    use rustfs_filemeta::{ChecksumInfo, ObjectPartInfo};

    fn decode_hex(s: &str) -> Vec<u8> {
        let s: String = s.chars().filter(|c| c.is_ascii_hexdigit()).collect();
        s.as_bytes()
            .chunks(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect()
    }

    fn streaming_frame(raw: &[u8], frame_size: usize, algorithm: &HashAlgorithm) -> Vec<u8> {
        let mut framed = Vec::new();
        for block in raw.chunks(frame_size) {
            let hash = algorithm.hash_encode(block);
            framed.extend_from_slice(hash.as_ref());
            framed.extend_from_slice(block);
        }
        framed
    }

    fn encode_object_shards(erasure: &Erasure, payload: &[u8]) -> Vec<Vec<u8>> {
        let mut shards = vec![Vec::new(); erasure.data_shards + erasure.parity_shards];
        for block in payload.chunks(erasure.block_size) {
            let encoded = erasure.encode_data(block).expect("encode test block");
            for (target, shard) in shards.iter_mut().zip(encoded) {
                target.extend_from_slice(&shard);
            }
        }
        shards
    }

    struct ModernFixtureLayout<'a> {
        inline: bool,
        distribution: &'a [usize],
        declared_size: usize,
    }

    async fn write_modern_object_fixture(
        drives: &[tempfile::TempDir],
        indices: &[usize],
        bucket: &str,
        object: &str,
        payload: &[u8],
        inline: bool,
    ) {
        write_modern_object_fixture_with_distribution(drives, indices, bucket, object, payload, inline, &[1, 2, 3, 4]).await;
    }

    async fn write_modern_object_fixture_with_distribution(
        drives: &[tempfile::TempDir],
        indices: &[usize],
        bucket: &str,
        object: &str,
        payload: &[u8],
        inline: bool,
        distribution: &[usize],
    ) {
        write_modern_object_fixture_with_layout(
            drives,
            indices,
            bucket,
            object,
            payload,
            ModernFixtureLayout {
                inline,
                distribution,
                declared_size: payload.len(),
            },
        )
        .await;
    }

    async fn write_modern_object_fixture_with_layout(
        drives: &[tempfile::TempDir],
        indices: &[usize],
        bucket: &str,
        object: &str,
        payload: &[u8],
        layout: ModernFixtureLayout<'_>,
    ) {
        let erasure = Erasure::try_new(2, 2, 64).expect("test erasure geometry");
        let encoded = encode_object_shards(&erasure, payload);
        let data_dir = Uuid::parse_str("11111111-1111-1111-1111-111111111111").expect("test data dir");
        let algorithm = HashAlgorithm::HighwayHash256S;

        for (drive, index) in drives.iter().zip(indices.iter().copied()) {
            let raw = encoded[index - 1].as_slice();
            let framed = streaming_frame(raw, erasure.shard_size(), &algorithm);
            let mut file_info = FileInfo::new(object, 2, 2);
            file_info.name = object.to_string();
            file_info.data_dir = Some(data_dir);
            file_info.mod_time = Some(OffsetDateTime::from_unix_timestamp(10).expect("test timestamp"));
            file_info.size = layout.declared_size as i64;
            file_info.parts = vec![ObjectPartInfo {
                number: 1,
                size: layout.declared_size,
                actual_size: layout.declared_size as i64,
                ..Default::default()
            }];
            file_info.erasure.block_size = 64;
            file_info.erasure.index = index;
            file_info.erasure.distribution = layout.distribution.to_vec();
            file_info.erasure.checksums = vec![ChecksumInfo {
                part_number: 1,
                algorithm: algorithm.clone(),
                ..Default::default()
            }];
            if layout.inline {
                file_info.data = Some(framed.clone().into());
                file_info.set_inline_data();
            }

            let mut file_meta = FileMeta::new();
            file_meta.add_version(file_info).expect("add test object version");
            let object_dir = system_object_dir(drive.path().to_string_lossy().as_ref(), bucket, object);
            std::fs::create_dir_all(&object_dir).expect("create test object directory");
            std::fs::write(object_dir.join("xl.meta"), file_meta.marshal_msg().expect("marshal test xl.meta"))
                .expect("write test xl.meta");
            if !layout.inline {
                let part_dir = object_dir.join(data_dir.to_string());
                std::fs::create_dir_all(&part_dir).expect("create test part directory");
                std::fs::write(part_dir.join("part.1"), framed).expect("write test part");
            }
        }
    }

    fn write_non_inline_part_shape_fixture(drive: &tempfile::TempDir, part_numbers: &[usize]) -> (String, PathBuf) {
        let object = BUCKET_METADATA_FILE;
        let data_dir = Uuid::parse_str("11111111-1111-1111-1111-111111111111").expect("test data dir");
        let mut file_info = FileInfo::new(object, 2, 2);
        file_info.name = object.to_string();
        file_info.data_dir = Some(data_dir);
        file_info.mod_time = Some(OffsetDateTime::from_unix_timestamp(10).expect("test timestamp"));
        file_info.size = part_numbers.len() as i64;
        file_info.parts = part_numbers
            .iter()
            .map(|number| ObjectPartInfo {
                number: *number,
                size: 1,
                actual_size: 1,
                ..Default::default()
            })
            .collect();
        file_info.erasure.block_size = 64;
        file_info.erasure.index = 1;
        file_info.erasure.distribution = vec![1, 2, 3, 4];
        file_info.erasure.checksums = part_numbers
            .iter()
            .map(|number| ChecksumInfo {
                part_number: *number,
                algorithm: HashAlgorithm::HighwayHash256S,
                ..Default::default()
            })
            .collect();

        let mut file_meta = FileMeta::new();
        file_meta.add_version(file_info).expect("add part-shape fixture");
        let drive_path = drive.path().to_string_lossy().into_owned();
        let object_dir = system_object_dir(&drive_path, "interop", object);
        std::fs::create_dir_all(&object_dir).expect("create part-shape fixture directory");
        std::fs::write(object_dir.join("xl.meta"), file_meta.marshal_msg().expect("marshal part-shape fixture"))
            .expect("write part-shape fixture");
        (drive_path, object_dir)
    }

    fn xml_sentinel_metadata() -> BucketMetadata {
        let timestamp = |seconds| OffsetDateTime::from_unix_timestamp(seconds).expect("sentinel timestamp");
        let mut metadata = BucketMetadata::default();
        metadata.notification_config_xml = b"notification-sentinel".to_vec();
        metadata.notification_config_updated_at = timestamp(1);
        metadata.lifecycle_config_xml = b"lifecycle-sentinel".to_vec();
        metadata.lifecycle_config_updated_at = timestamp(2);
        metadata.object_lock_config_xml = b"object-lock-sentinel".to_vec();
        metadata.object_lock_config_updated_at = timestamp(3);
        metadata.versioning_config_xml = b"versioning-sentinel".to_vec();
        metadata.versioning_config_updated_at = timestamp(4);
        metadata.encryption_config_xml = b"encryption-sentinel".to_vec();
        metadata.encryption_config_updated_at = timestamp(5);
        metadata.tagging_config_xml = b"tagging-sentinel".to_vec();
        metadata.tagging_config_updated_at = timestamp(6);
        metadata.replication_config_xml = b"replication-sentinel".to_vec();
        metadata.replication_config_updated_at = timestamp(7);
        metadata.cors_config_xml = b"cors-sentinel".to_vec();
        metadata.cors_config_updated_at = timestamp(8);
        metadata.logging_config_xml = b"logging-sentinel".to_vec();
        metadata.logging_config_updated_at = timestamp(9);
        metadata.website_config_xml = b"website-sentinel".to_vec();
        metadata.website_config_updated_at = timestamp(10);
        metadata.accelerate_config_xml = b"accelerate-sentinel".to_vec();
        metadata.accelerate_config_updated_at = timestamp(11);
        metadata.request_payment_config_xml = b"request-payment-sentinel".to_vec();
        metadata.request_payment_config_updated_at = timestamp(12);
        metadata.public_access_block_config_xml = b"public-access-block-sentinel".to_vec();
        metadata.public_access_block_config_updated_at = timestamp(13);
        metadata
    }

    fn expected_xml_sentinels() -> Vec<(&'static str, &'static [u8], OffsetDateTime)> {
        let timestamp = |seconds| OffsetDateTime::from_unix_timestamp(seconds).expect("sentinel timestamp");
        vec![
            ("notification.xml", b"notification-sentinel", timestamp(1)),
            ("lifecycle.xml", b"lifecycle-sentinel", timestamp(2)),
            ("object-lock.xml", b"object-lock-sentinel", timestamp(3)),
            ("versioning.xml", b"versioning-sentinel", timestamp(4)),
            ("bucket-encryption.xml", b"encryption-sentinel", timestamp(5)),
            ("tagging.xml", b"tagging-sentinel", timestamp(6)),
            ("replication.xml", b"replication-sentinel", timestamp(7)),
            ("cors.xml", b"cors-sentinel", timestamp(8)),
            ("logging.xml", b"logging-sentinel", timestamp(9)),
            ("website.xml", b"website-sentinel", timestamp(10)),
            ("accelerate.xml", b"accelerate-sentinel", timestamp(11)),
            ("request-payment.xml", b"request-payment-sentinel", timestamp(12)),
            ("public-access-block.xml", b"public-access-block-sentinel", timestamp(13)),
        ]
    }

    fn marshal_bucket_metadata_blob(metadata: &BucketMetadata) -> Vec<u8> {
        let mut blob = Vec::new();
        blob.extend_from_slice(&1_u16.to_le_bytes());
        blob.extend_from_slice(&1_u16.to_le_bytes());
        blob.extend_from_slice(&metadata.marshal_msg().expect("marshal bucket metadata"));
        blob
    }

    fn inspect_opts(drives: &[tempfile::TempDir]) -> InspectBucketMetaOpts {
        InspectBucketMetaOpts {
            paths: drives
                .iter()
                .map(|drive| drive.path().to_string_lossy().into_owned())
                .collect(),
            bucket: "interop".to_string(),
            raw: false,
            out: None,
        }
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
        // Corrupt an XML value without damaging MessagePack framing. With
        // verification bypassed this still unmarshals, so only the bitrot
        // boundary can make the test fail.
        let needle = b"primary:webhook";
        let target = xlmeta
            .windows(needle.len())
            .position(|window| window == needle)
            .expect("fixture contains notification destination");
        xlmeta[target] = b'q';

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
        let error = reconstruct_metadata_blob(&opts)
            .await
            .expect_err("corrupted shard must not reconstruct");
        assert!(error.to_string().contains("bitrot verification failed"), "unexpected error: {error}");
    }

    #[test]
    fn export_list_contains_only_persisted_xml_configs() {
        let bm = xml_sentinel_metadata();
        let configs = stored_configs(&bm);

        assert_eq!(configs, expected_xml_sentinels());
    }

    #[tokio::test]
    async fn rejects_invalid_bucket_before_reading_drive_paths() {
        let dir = tempfile::tempdir().expect("tempdir");
        for bucket in ["../escape", "Uppercase", "under_score", "colon:name"] {
            let opts = InspectBucketMetaOpts {
                paths: vec![dir.path().to_string_lossy().into_owned()],
                bucket: bucket.to_string(),
                raw: false,
                out: None,
            };

            let err = reconstruct_metadata_blob(&opts).await.expect_err("invalid bucket must fail");
            assert!(err.to_string().contains("invalid bucket"), "{bucket}: unexpected error: {err}");
        }
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

    #[cfg(unix)]
    #[test]
    fn raw_export_resolves_a_symlinked_parent_before_comparing_source_drives() {
        use std::os::unix::fs::symlink;

        let drive = tempfile::tempdir().expect("drive tempdir");
        let export_parent = tempfile::tempdir().expect("export parent");
        let alias = export_parent.path().join("source-alias");
        symlink(drive.path(), &alias).expect("create source alias");
        let out = alias.join("new-export");
        assert!(!out.exists(), "output tail must not exist yet");

        let error = ensure_output_outside_source_drives(&out, &[drive.path().to_string_lossy().into_owned()])
            .expect_err("symlinked source-drive aliases must be rejected");
        assert!(error.to_string().contains("inside source drive"), "unexpected error: {error}");
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

    #[tokio::test]
    async fn raw_export_writes_blob_and_every_xml_field_byte_for_byte() {
        let metadata = xml_sentinel_metadata();
        let blob = marshal_bucket_metadata_blob(&metadata);
        let drives = (0..3)
            .map(|_| tempfile::tempdir().expect("drive tempdir"))
            .collect::<Vec<_>>();
        write_modern_object_fixture(&drives, &[1, 2, 3], "interop", BUCKET_METADATA_FILE, &blob, true).await;
        let export = tempfile::tempdir().expect("export tempdir");
        let out = export.path().join("raw");
        let opts = InspectBucketMetaOpts {
            paths: drives
                .iter()
                .map(|drive| drive.path().to_string_lossy().into_owned())
                .collect(),
            bucket: "interop".to_string(),
            raw: true,
            out: Some(out.clone()),
        };

        execute_bucket_meta(&opts).await.expect("raw export");

        assert_eq!(std::fs::read_dir(&out).expect("read export directory").count(), 14);
        assert_eq!(std::fs::read(out.join(BUCKET_METADATA_FILE)).expect("read raw metadata blob"), blob);
        for (name, expected_bytes, _) in expected_xml_sentinels() {
            assert_eq!(
                std::fs::read(out.join(name)).unwrap_or_else(|error| panic!("read {name}: {error}")),
                expected_bytes,
                "{name} must preserve exact persisted bytes"
            );
        }
    }

    #[tokio::test]
    async fn reconstructs_complete_drive_layout_from_parity_when_a_data_shard_is_missing() {
        let payload = decode_hex(include_str!("../../crates/ecstore/tests/fixtures/minio/bucket_metadata.blob.hex"));
        let drives = (0..3)
            .map(|_| tempfile::tempdir().expect("drive tempdir"))
            .collect::<Vec<_>>();
        write_modern_object_fixture(&drives, &[2, 3, 4], "interop", BUCKET_METADATA_FILE, &payload, true).await;

        let raw = reconstruct_system_object(&inspect_opts(&drives), BUCKET_METADATA_FILE)
            .await
            .expect("reconstruct raw system object");
        assert_eq!(raw, payload, "drive fixture must reproduce the source blob exactly");
        let body = reconstruct_metadata_blob(&inspect_opts(&drives))
            .await
            .expect("reconstruct from parity drives");
        assert_eq!(body, payload, "missing data shard must be restored byte-for-byte from parity");
    }

    #[tokio::test]
    async fn reconstructs_a_valid_non_inline_part_one() {
        let payload = decode_hex(include_str!("../../crates/ecstore/tests/fixtures/minio/bucket_metadata.blob.hex"));
        let drives = (0..3)
            .map(|_| tempfile::tempdir().expect("drive tempdir"))
            .collect::<Vec<_>>();
        write_modern_object_fixture(&drives, &[1, 2, 3], "interop", BUCKET_METADATA_FILE, &payload, false).await;

        let body = reconstruct_metadata_blob(&inspect_opts(&drives))
            .await
            .expect("reconstruct non-inline metadata");
        assert_eq!(body, payload);
    }

    #[tokio::test]
    async fn rejects_non_inline_objects_without_exactly_one_part_one() {
        let only_part_two = tempfile::tempdir().expect("drive tempdir");
        let (drive, object_dir) = write_non_inline_part_shape_fixture(&only_part_two, &[2]);
        let error = read_drive_shard(&drive, BUCKET_METADATA_FILE, &object_dir, &object_dir.join("xl.meta"))
            .await
            .expect_err("part.2 without part.1 must fail");
        assert!(error.to_string().contains("no supported part.1"), "unexpected error: {error}");

        let two_parts = tempfile::tempdir().expect("drive tempdir");
        let (drive, object_dir) = write_non_inline_part_shape_fixture(&two_parts, &[1, 2]);
        let error = read_drive_shard(&drive, BUCKET_METADATA_FILE, &object_dir, &object_dir.join("xl.meta"))
            .await
            .expect_err("part.1 plus part.2 must fail");
        assert!(error.to_string().contains("supports exactly one"), "unexpected error: {error}");
    }

    #[tokio::test]
    async fn rejects_under_limit_inline_source_with_inconsistent_framing() {
        let payload = b"small fixture data";
        let drives = vec![tempfile::tempdir().expect("drive tempdir")];
        write_modern_object_fixture_with_layout(
            &drives,
            &[1],
            "interop",
            BUCKET_METADATA_FILE,
            payload,
            ModernFixtureLayout {
                inline: true,
                distribution: &[1, 2, 3, 4],
                declared_size: 128,
            },
        )
        .await;

        let drive = drives[0].path().to_string_lossy().into_owned();
        let object_dir = system_object_dir(&drive, "interop", BUCKET_METADATA_FILE);
        let error = read_drive_shard(&drive, BUCKET_METADATA_FILE, &object_dir, &object_dir.join("xl.meta"))
            .await
            .expect_err("inconsistent under-limit framing must fail closed");
        assert!(error.to_string().contains("refusing to guess"), "unexpected error: {error}");
        assert!(!error.to_string().contains("inspection limit"), "unexpected error: {error}");
    }

    #[tokio::test]
    async fn rejects_write_quorum_with_an_invalid_erasure_distribution() {
        let payload = decode_hex(include_str!("../../crates/ecstore/tests/fixtures/minio/bucket_metadata.blob.hex"));
        let drives = (0..3)
            .map(|_| tempfile::tempdir().expect("drive tempdir"))
            .collect::<Vec<_>>();
        write_modern_object_fixture_with_distribution(
            &drives,
            &[1, 2, 3],
            "interop",
            BUCKET_METADATA_FILE,
            &payload,
            true,
            &[1, 1, 3, 4],
        )
        .await;

        let error = reconstruct_system_object(&inspect_opts(&drives), BUCKET_METADATA_FILE)
            .await
            .expect_err("invalid metadata on a full write quorum must fail at the drive boundary");
        assert!(error.to_string().contains("invalid persisted metadata"), "unexpected error: {error}");
        assert!(error.to_string().contains("xl.meta"), "error must identify a drive path: {error}");
    }

    #[tokio::test]
    async fn rejects_xlmeta_above_the_inspection_limit() {
        let drive = tempfile::tempdir().expect("drive tempdir");
        let object_dir = system_object_dir(drive.path().to_string_lossy().as_ref(), "interop", BUCKET_METADATA_FILE);
        std::fs::create_dir_all(&object_dir).expect("create test object directory");
        let xl_path = object_dir.join("xl.meta");
        let file = std::fs::File::create(&xl_path).expect("create oversized xl.meta");
        file.set_len(MAX_XL_META_BYTES + 1).expect("size oversized xl.meta");

        let drive_path = drive.path().to_string_lossy().into_owned();
        let error = read_drive_shard(&drive_path, BUCKET_METADATA_FILE, &object_dir, &xl_path)
            .await
            .expect_err("oversized xl.meta must fail before parsing");
        assert!(error.to_string().contains("xl.meta is"), "unexpected error: {error}");
        assert!(error.to_string().contains("inspection limit"), "unexpected error: {error}");
    }

    #[tokio::test]
    async fn rejects_non_inline_part_above_the_inspection_limit() {
        let payload = decode_hex(include_str!("../../crates/ecstore/tests/fixtures/minio/bucket_metadata.blob.hex"));
        let drives = vec![tempfile::tempdir().expect("drive tempdir")];
        write_modern_object_fixture(&drives, &[1], "interop", BUCKET_METADATA_FILE, &payload, false).await;

        let object_dir = system_object_dir(drives[0].path().to_string_lossy().as_ref(), "interop", BUCKET_METADATA_FILE);
        let part_path = object_dir.join("11111111-1111-1111-1111-111111111111/part.1");
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&part_path)
            .expect("open test part");
        file.set_len(MAX_SHARD_SOURCE_BYTES + 1).expect("size oversized part");

        let drive_path = drives[0].path().to_string_lossy().into_owned();
        let error = read_drive_shard(&drive_path, BUCKET_METADATA_FILE, &object_dir, &object_dir.join("xl.meta"))
            .await
            .expect_err("oversized non-inline part must fail before reading");
        assert!(error.to_string().contains("shard source is"), "unexpected error: {error}");
        assert!(error.to_string().contains("inspection limit"), "unexpected error: {error}");
    }

    #[tokio::test]
    async fn rejects_declared_object_size_above_the_reconstruction_limit() {
        let payload = b"small fixture data";
        let drives = vec![tempfile::tempdir().expect("drive tempdir")];
        write_modern_object_fixture_with_layout(
            &drives,
            &[1],
            "interop",
            BUCKET_METADATA_FILE,
            payload,
            ModernFixtureLayout {
                inline: true,
                distribution: &[1, 2, 3, 4],
                declared_size: MAX_BUCKET_METADATA_BYTES + 1,
            },
        )
        .await;

        let object_dir = system_object_dir(drives[0].path().to_string_lossy().as_ref(), "interop", BUCKET_METADATA_FILE);
        let drive_path = drives[0].path().to_string_lossy().into_owned();
        let error = read_drive_shard(&drive_path, BUCKET_METADATA_FILE, &object_dir, &object_dir.join("xl.meta"))
            .await
            .expect_err("oversized reconstructed object must fail before shard allocation");
        assert!(error.to_string().contains("system object body is"), "unexpected error: {error}");
        assert!(error.to_string().contains("inspection limit"), "unexpected error: {error}");
    }

    fn bucket_metadata_blob(incarnation: Uuid) -> Vec<u8> {
        let mut metadata = BucketMetadata::new("interop");
        metadata.bucket_incarnation_id = incarnation;
        marshal_bucket_metadata_blob(&metadata)
    }

    #[tokio::test]
    async fn bucket_incarnation_sidecar_must_match_persisted_metadata() {
        let metadata_incarnation = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").expect("metadata incarnation");
        let stale_incarnation = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").expect("stale incarnation");
        let drives = (0..3)
            .map(|_| tempfile::tempdir().expect("drive tempdir"))
            .collect::<Vec<_>>();
        let blob = bucket_metadata_blob(metadata_incarnation);
        write_modern_object_fixture(&drives, &[1, 2, 3], "interop", BUCKET_METADATA_FILE, &blob, true).await;

        let missing = reconstruct_metadata_blob(&inspect_opts(&drives))
            .await
            .expect_err("new-format metadata without a sidecar must fail");
        assert!(missing.to_string().contains("sidecar is missing"), "unexpected error: {missing}");

        write_modern_object_fixture(
            &drives,
            &[1, 2, 3],
            "interop",
            BUCKET_INCARNATION_FILE,
            stale_incarnation.as_bytes(),
            true,
        )
        .await;

        let err = reconstruct_metadata_blob(&inspect_opts(&drives))
            .await
            .expect_err("stale bucket metadata must be rejected");
        assert!(err.to_string().contains("does not match"), "unexpected error: {err}");

        write_modern_object_fixture(
            &drives,
            &[1, 2, 3],
            "interop",
            BUCKET_INCARNATION_FILE,
            metadata_incarnation.as_bytes(),
            true,
        )
        .await;
        let restored = reconstruct_metadata_blob(&inspect_opts(&drives))
            .await
            .expect("matching sidecar must pass");
        assert_eq!(restored, blob);
    }

    #[tokio::test]
    async fn bucket_incarnation_rejects_nil_and_malformed_sidecars() {
        let drives = (0..3)
            .map(|_| tempfile::tempdir().expect("drive tempdir"))
            .collect::<Vec<_>>();
        let legacy_blob = bucket_metadata_blob(Uuid::nil());
        write_modern_object_fixture(&drives, &[1, 2, 3], "interop", BUCKET_METADATA_FILE, &legacy_blob, true).await;

        write_modern_object_fixture(&drives, &[1, 2, 3], "interop", BUCKET_INCARNATION_FILE, Uuid::nil().as_bytes(), true).await;
        let nil_error = reconstruct_metadata_blob(&inspect_opts(&drives))
            .await
            .expect_err("a persisted nil bucket incarnation must fail");
        assert!(nil_error.to_string().contains("incarnation is nil"), "unexpected error: {nil_error}");

        write_modern_object_fixture(&drives, &[1, 2, 3], "interop", BUCKET_INCARNATION_FILE, &[0x5a; 15], true).await;
        let malformed_error = reconstruct_metadata_blob(&inspect_opts(&drives))
            .await
            .expect_err("a malformed bucket incarnation must fail");
        assert!(
            malformed_error
                .to_string()
                .contains("persisted bucket incarnation is invalid"),
            "unexpected error: {malformed_error}"
        );
    }

    #[tokio::test]
    async fn legacy_v1_whole_file_bitrot_fixtures_support_all_persisted_algorithms() {
        let payload = decode_hex(include_str!("../../crates/ecstore/tests/fixtures/minio/bucket_metadata.blob.hex"));
        let erasure = Erasure::try_new(4, 2, 1_048_576).expect("legacy erasure geometry");
        let encoded = erasure.encode_data(&payload).expect("encode legacy payload");
        let algorithms = [
            ("sha256", HashAlgorithm::SHA256),
            ("highwayhash256", HashAlgorithm::HighwayHash256),
            ("blake2b", HashAlgorithm::BLAKE2b512),
            ("md5", HashAlgorithm::Md5),
        ];

        for (name, algorithm) in algorithms {
            let drives = (0..4)
                .map(|_| tempfile::tempdir().expect("drive tempdir"))
                .collect::<Vec<_>>();
            for (offset, drive) in drives.iter().enumerate() {
                let index = offset + 1;
                let raw = encoded[index - 1].as_ref();
                let hash = algorithm.hash_encode(raw);
                let xlmeta = rustfs_filemeta::test_data::create_legacy_v1_object_xlmeta_with_checksum(
                    index,
                    name,
                    hash.as_ref(),
                    payload.len(),
                )
                .expect("create legacy V1 fixture");
                let object_dir = system_object_dir(drive.path().to_string_lossy().as_ref(), "interop", BUCKET_METADATA_FILE);
                let part_dir = object_dir.join("fedcba98-7654-3210-fedc-ba9876543210");
                std::fs::create_dir_all(&part_dir).expect("create legacy fixture directory");
                std::fs::write(object_dir.join("xl.meta"), xlmeta).expect("write legacy xl.meta");
                std::fs::write(part_dir.join("part.1"), raw).expect("write legacy whole-file shard");
            }

            let restored = reconstruct_metadata_blob(&inspect_opts(&drives))
                .await
                .unwrap_or_else(|error| panic!("{name} legacy fixture failed: {error}"));
            assert_eq!(restored, payload, "{name} fixture must reconstruct byte-for-byte");

            let object_dir = system_object_dir(drives[0].path().to_string_lossy().as_ref(), "interop", BUCKET_METADATA_FILE);
            let part_path = object_dir.join("fedcba98-7654-3210-fedc-ba9876543210/part.1");
            let mut corrupt = std::fs::read(&part_path).expect("read legacy whole-file shard");
            corrupt[0] ^= 0xff;
            std::fs::write(&part_path, corrupt).expect("corrupt legacy whole-file shard");
            let drive = drives[0].path().to_string_lossy().into_owned();
            let err = read_drive_shard(&drive, BUCKET_METADATA_FILE, &object_dir, &object_dir.join("xl.meta"))
                .await
                .expect_err("whole-file bitrot corruption must fail at the drive boundary");
            assert!(err.to_string().contains("checksum mismatch"), "{name}: unexpected error: {err}");
        }
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
    fn metadata_selection_uses_newest_write_quorum_independent_of_order() {
        let stale_time = OffsetDateTime::from_unix_timestamp(1).expect("stale timestamp");
        let current_time = OffsetDateTime::from_unix_timestamp(2).expect("current timestamp");
        let stale_first = vec![
            quorum_test_shard("stale-1", stale_time, 1),
            quorum_test_shard("stale-2", stale_time, 2),
            quorum_test_shard("stale-3", stale_time, 3),
            quorum_test_shard("current-3", current_time, 3),
            quorum_test_shard("current-2", current_time, 2),
            quorum_test_shard("current-1", current_time, 1),
        ];
        let current_first = vec![
            quorum_test_shard("current-1", current_time, 1),
            quorum_test_shard("current-2", current_time, 2),
            quorum_test_shard("current-3", current_time, 3),
            quorum_test_shard("stale-1", stale_time, 1),
            quorum_test_shard("stale-2", stale_time, 2),
            quorum_test_shard("stale-3", stale_time, 3),
        ];

        for shards in [stale_first, current_first] {
            let (selected, selected_shards) = select_quorum_shards(shards).expect("current identity reaches write quorum");
            assert_eq!(selected.mod_time, Some(current_time));
            assert_eq!(selected_shards.len(), 3);
            assert!(selected_shards.iter().all(|shard| shard.drive.starts_with("current-")));
        }
    }

    #[test]
    fn metadata_selection_rejects_two_write_quorums_at_the_same_modification_time() {
        let mod_time = OffsetDateTime::from_unix_timestamp(2).expect("test timestamp");
        let mut shards = Vec::new();
        for index in 1..=3 {
            shards.push(quorum_test_shard(&format!("identity-a-{index}"), mod_time, index));
            let mut other = quorum_test_shard(&format!("identity-b-{index}"), mod_time, index);
            other.file_info.size = 64;
            shards.push(other);
        }

        let Err(error) = select_quorum_shards(shards) else {
            panic!("two different write-quorum identities at one timestamp must fail closed");
        };
        assert!(
            error.to_string().contains("2 different object-metadata identities"),
            "unexpected error: {error}"
        );
        assert!(error.to_string().contains("latest modification time"), "unexpected error: {error}");
    }

    #[test]
    fn metadata_selection_rejects_conflicting_verified_bytes_for_one_erasure_index() {
        let mod_time = OffsetDateTime::from_unix_timestamp(2).expect("test timestamp");
        let mut shards = vec![
            quorum_test_shard("drive-1", mod_time, 1),
            quorum_test_shard("drive-2", mod_time, 2),
            quorum_test_shard("drive-3", mod_time, 3),
        ];
        let mut conflict = quorum_test_shard("conflicting-drive-1", mod_time, 1);
        conflict.data[0] ^= 0xff;
        shards.push(conflict);

        let Err(error) = select_quorum_shards(shards) else {
            panic!("conflicting verified bytes for one erasure index must fail closed");
        };
        assert!(error.to_string().contains("conflicting verified bytes"), "unexpected error: {error}");
        assert!(error.to_string().contains("erasure shard 1"), "unexpected error: {error}");
    }

    #[test]
    fn metadata_selection_rejects_old_and_new_read_quorums_without_a_committed_group() {
        let stale_time = OffsetDateTime::from_unix_timestamp(1).expect("stale timestamp");
        let current_time = OffsetDateTime::from_unix_timestamp(2).expect("current timestamp");
        let shards = vec![
            quorum_test_shard("stale-1", stale_time, 1),
            quorum_test_shard("stale-2", stale_time, 2),
            quorum_test_shard("current-3", current_time, 3),
            quorum_test_shard("current-4", current_time, 4),
        ];

        let Err(err) = select_quorum_shards(shards) else {
            panic!("two uncommitted read quorums must fail closed");
        };
        assert!(err.to_string().contains("write quorum"), "unexpected error: {err}");
    }

    #[test]
    fn metadata_selection_requires_distinct_erasure_indices_for_quorum() {
        let current_time = OffsetDateTime::from_unix_timestamp(2).expect("current timestamp");
        let shards = vec![
            quorum_test_shard("current-a", current_time, 1),
            quorum_test_shard("current-b", current_time, 1),
            quorum_test_shard("current-c", current_time, 2),
        ];

        let Err(err) = select_quorum_shards(shards) else {
            panic!("duplicate shard indices must not form quorum");
        };
        assert!(err.to_string().contains("reached write quorum"), "unexpected error: {err}");
    }
}
