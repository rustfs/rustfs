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

//! In-process fault-injection primitives for reliability E2E tests.
//!
//! `DiskFaultHarness` runs a single-node, multi-disk RustFS server whose
//! per-disk data directories can be manipulated while the process is alive:
//!
//! - [`DiskFaultHarness::take_disk_offline`] renames a disk directory to
//!   `<dir>.offline`. Every subsequent per-request open on that volume fails
//!   with "not found", which RustFS treats as a faulted drive. This simulates
//!   a disk that disappears at the mount-point level (unplugged/unmounted),
//!   not an EIO-returning half-dead disk.
//! - [`DiskFaultHarness::bring_disk_online`] restores the renamed directory.
//!   If the server recreated a skeleton directory at the original path in the
//!   meantime, the skeleton is discarded first.
//! - [`DiskFaultHarness::replace_disk_with_empty`] retires the directory and
//!   creates a fresh empty one, simulating a fresh-disk replacement that the
//!   server should detect as unformatted and heal.
//! - [`DiskFaultHarness::corrupt_object_shard`] flips bytes in the middle of
//!   one erasure shard (`part.*` file) of an object, simulating silent bitrot.
//!   Only shard data is touched, never `xl.meta`.
//! - [`DiskFaultHarness::kill_server`] / [`DiskFaultHarness::restart_server`]
//!   SIGKILL the process and restart it with the same volumes and port so
//!   heal-after-restart can be exercised.

use crate::common::{RustFSTestEnvironment, local_http_client};
use http::header::{CONTENT_TYPE, HOST};
use rustfs_signer::constants::UNSIGNED_PAYLOAD;
use rustfs_signer::sign_v4;
use s3s::Body;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::path::{Path, PathBuf};
use tracing::info;
use uuid::Uuid;
use walkdir::WalkDir;

type ChaosResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

/// Physical `xl.meta` and shard-file census for one object version on one disk.
///
/// A successful S3 GET only proves that a quorum can serve an object. Replacement
/// tests need this lower-level record to prove that the rebuilt target holds the
/// `xl.meta` selected for a specific version and every `part.N` it declares.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct VersionShardCensus {
    pub version_id: Option<String>,
    pub has_xl_meta: bool,
    pub data_dir: Option<String>,
    pub erasure_index: Option<usize>,
    pub expected_part_numbers: BTreeSet<usize>,
    pub present_part_fingerprints: BTreeMap<usize, PartShardFingerprint>,
    pub inline_data_fingerprint: Option<PartShardFingerprint>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PartShardFingerprint {
    pub size: u64,
    pub sha256: String,
}

impl VersionShardCensus {
    pub(crate) fn is_complete(&self) -> bool {
        self.has_xl_meta
            && self.expected_part_numbers.len() == self.present_part_fingerprints.len()
            && self
                .expected_part_numbers
                .iter()
                .all(|part_number| self.present_part_fingerprints.contains_key(part_number))
    }

    pub(crate) fn matches_manifest(&self, manifest: &Self) -> bool {
        self.version_id == manifest.version_id
            && self.is_complete()
            && manifest.is_complete()
            && self.data_dir == manifest.data_dir
            && self.erasure_index == manifest.erasure_index
            && self.expected_part_numbers == manifest.expected_part_numbers
            && self.present_part_fingerprints == manifest.present_part_fingerprints
            && self.inline_data_fingerprint == manifest.inline_data_fingerprint
    }
}

fn sha256_hex(data: &[u8]) -> String {
    let digest = Sha256::digest(data);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn shard_fingerprint(data: &[u8]) -> ChaosResult<PartShardFingerprint> {
    Ok(PartShardFingerprint {
        size: u64::try_from(data.len())?,
        sha256: sha256_hex(data),
    })
}

/// Single-node RustFS server with `disk_count` local volume directories that
/// can be faulted individually while the server is running.
pub struct DiskFaultHarness {
    pub env: RustFSTestEnvironment,
    root: PathBuf,
    disks: Vec<PathBuf>,
    extra_env: Vec<(String, String)>,
}

impl DiskFaultHarness {
    /// Create the volume directories (`disk0..diskN-1`) under a fresh temp root.
    /// The server is not started yet; call [`Self::start_server`].
    pub async fn new(disk_count: usize) -> ChaosResult<Self> {
        if disk_count < 2 {
            return Err("disk fault harness needs at least 2 disks".into());
        }

        let env = RustFSTestEnvironment::new().await?;
        let root = PathBuf::from(env.temp_dir.clone());
        let disks: Vec<PathBuf> = (0..disk_count).map(|i| root.join(format!("disk{i}"))).collect();
        for disk in &disks {
            std::fs::create_dir_all(disk)?;
        }

        Ok(Self {
            env,
            root,
            disks,
            // All disk directories live on one filesystem, so the startup
            // same-disk check must be bypassed like other multi-disk e2e tests.
            extra_env: vec![("RUSTFS_UNSAFE_BYPASS_DISK_CHECK".to_string(), "true".to_string())],
        })
    }

    /// Add an extra environment variable for the server process (applies to
    /// subsequent [`Self::start_server`] / [`Self::restart_server`] calls).
    pub fn set_env<K, V>(&mut self, key: K, value: V)
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.extra_env.push((key.into(), value.into()));
    }

    pub fn disk_path(&self, disk_index: usize) -> &Path {
        &self.disks[disk_index]
    }

    fn offline_path(&self, disk_index: usize) -> PathBuf {
        let disk = &self.disks[disk_index];
        let mut name = disk.file_name().expect("disk path has a file name").to_os_string();
        name.push(".offline");
        disk.with_file_name(name)
    }

    /// Start (or restart after [`Self::kill_server`]) the RustFS server with
    /// all volume directories and the same address.
    pub async fn start_server(&mut self) -> ChaosResult<()> {
        // The common helper always appends `env.temp_dir` as the final storage
        // path: point it at the last disk for startup and pass the remaining
        // disks explicitly, then restore it to the root so Drop cleans up
        // every disk directory.
        let (last_disk, head_disks) = self.disks.split_last().expect("at least two disks");
        self.env.temp_dir = last_disk.to_string_lossy().to_string();

        let disk_args: Vec<String> = head_disks.iter().map(|d| d.to_string_lossy().to_string()).collect();
        let arg_refs: Vec<&str> = disk_args.iter().map(String::as_str).collect();
        let env_refs: Vec<(&str, &str)> = self.extra_env.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();

        let result = self.env.start_rustfs_server_with_env(arg_refs, &env_refs).await;
        self.env.temp_dir = self.root.to_string_lossy().to_string();
        result
    }

    /// SIGKILL the server process (simulates a crash / power loss for the
    /// process; no graceful shutdown runs).
    pub fn kill_server(&mut self) {
        self.env.stop_server();
    }

    /// Restart the server with the same volumes and port after [`Self::kill_server`].
    pub async fn restart_server(&mut self) -> ChaosResult<()> {
        self.start_server().await
    }

    /// Make one volume directory unavailable to the running server by
    /// renaming it to `<dir>.offline`. Path-based opens on the old location
    /// fail immediately, so the drive turns faulted from the server's view.
    pub fn take_disk_offline(&self, disk_index: usize) -> ChaosResult<()> {
        let disk = &self.disks[disk_index];
        let offline = self.offline_path(disk_index);
        if offline.exists() {
            return Err(format!("disk {disk_index} is already offline").into());
        }
        std::fs::rename(disk, &offline)?;
        info!("Took disk {} offline: {:?} -> {:?}", disk_index, disk, offline);
        Ok(())
    }

    /// Restore a disk previously taken offline with [`Self::take_disk_offline`].
    pub fn bring_disk_online(&self, disk_index: usize) -> ChaosResult<()> {
        let disk = &self.disks[disk_index];
        let offline = self.offline_path(disk_index);
        if !offline.exists() {
            return Err(format!("disk {disk_index} is not offline").into());
        }
        // The running server may have recreated a skeleton directory at the
        // original path (e.g. via degraded writes); discard it before
        // restoring the original data.
        if disk.exists() {
            std::fs::remove_dir_all(disk)?;
        }
        std::fs::rename(&offline, disk)?;
        info!("Brought disk {} back online: {:?}", disk_index, disk);
        Ok(())
    }

    /// Retire a disk directory and create a fresh empty one in its place,
    /// simulating a fresh-disk replacement. Intended to be used while the
    /// server is stopped; the server should treat the empty directory as an
    /// unformatted drive and heal it.
    pub fn replace_disk_with_empty(&self, disk_index: usize) -> ChaosResult<()> {
        let disk = &self.disks[disk_index];
        let retired = self.root.join(format!(
            "{}.retired-{}",
            disk.file_name().expect("disk path has a file name").to_string_lossy(),
            Uuid::new_v4()
        ));
        std::fs::rename(disk, &retired)?;
        std::fs::create_dir_all(disk)?;
        info!("Replaced disk {} with an empty directory (old data at {:?})", disk_index, retired);
        Ok(())
    }

    /// Flip bytes in the middle of one erasure shard of `bucket/key` stored on
    /// the given disk (bitrot simulation). Corrupts a `part.*` data file, never
    /// `xl.meta`. Fails if the object has no shard file on that disk (e.g. the
    /// object is small enough to be inlined into `xl.meta`).
    pub fn corrupt_object_shard(&self, disk_index: usize, bucket: &str, key: &str) -> ChaosResult<PathBuf> {
        let object_dir = self.disks[disk_index].join(bucket).join(key);
        let mut part_files: Vec<PathBuf> = WalkDir::new(&object_dir)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_type().is_file())
            .filter(|entry| entry.file_name().to_string_lossy().starts_with("part."))
            .map(|entry| entry.into_path())
            .collect();
        part_files.sort();

        let Some(part_file) = part_files.first() else {
            return Err(format!("no part.* shard file found under {object_dir:?}; object data may be inlined in xl.meta").into());
        };

        let mut data = std::fs::read(part_file)?;
        if data.len() < 32 {
            return Err(format!("shard file {part_file:?} is too small to corrupt safely ({} bytes)", data.len()).into());
        }
        let mid = data.len() / 2;
        for byte in &mut data[mid..mid + 8] {
            *byte ^= 0xFF;
        }
        std::fs::write(part_file, data)?;
        info!("Corrupted 8 bytes in the middle of shard {:?}", part_file);
        Ok(part_file.clone())
    }

    /// Whether the object's `xl.meta` exists on the given disk. Useful for
    /// polling heal progress on a replaced disk.
    pub fn object_metadata_exists_on_disk(&self, disk_index: usize, bucket: &str, key: &str) -> bool {
        self.disks[disk_index].join(bucket).join(key).join("xl.meta").is_file()
    }

    /// Census the physical files selected by `version_id` on one disk.
    ///
    /// Missing metadata and missing shard files are represented in the returned
    /// census rather than as an error so callers can poll replacement progress.
    /// Invalid metadata or an unknown requested version remains an error: treating
    /// either as an incomplete rebuild would hide corruption or a wrong-version
    /// recovery result.
    pub(crate) fn census_object_version(
        &self,
        disk_index: usize,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> ChaosResult<VersionShardCensus> {
        census_object_version_on_disk(&self.disks[disk_index], bucket, key, version_id)
    }
}

/// Census one physical object version without requiring a single-node harness.
/// Cluster replacement tests use the same evidence as the disk-fault tests.
pub(crate) fn census_object_version_on_disk(
    disk: &Path,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> ChaosResult<VersionShardCensus> {
    let version_id = version_id.map(str::to_owned);
    let object_dir = disk.join(bucket).join(key);
    let meta_path = object_dir.join("xl.meta");
    if !meta_path.is_file() {
        return Ok(VersionShardCensus {
            version_id,
            has_xl_meta: false,
            data_dir: None,
            erasure_index: None,
            expected_part_numbers: BTreeSet::new(),
            present_part_fingerprints: BTreeMap::new(),
            inline_data_fingerprint: None,
        });
    }

    let metadata = rustfs_filemeta::FileMeta::load(&std::fs::read(&meta_path)?)?;
    let file_info = metadata.into_fileinfo(bucket, key, version_id.as_deref().unwrap_or_default(), true, false, true)?;
    let expected_part_numbers = if file_info.inline_data() {
        BTreeSet::new()
    } else {
        file_info.parts.iter().map(|part| part.number).collect()
    };
    let data_dir = file_info.data_dir.map(|id| id.to_string());
    let erasure_index = Some(file_info.erasure.index);
    let inline_data_fingerprint = file_info.data.as_deref().map(shard_fingerprint).transpose()?;
    let part_dir = data_dir.as_ref().map_or_else(|| object_dir.clone(), |id| object_dir.join(id));
    let present_part_fingerprints = match std::fs::read_dir(&part_dir) {
        Ok(entries) => {
            let mut fingerprints = BTreeMap::new();
            for entry in entries {
                let entry = entry?;
                if !entry.file_type()?.is_file() {
                    continue;
                }
                let file_name = entry.file_name();
                let Some(part_number) = file_name
                    .to_str()
                    .and_then(|name| name.strip_prefix("part."))
                    .and_then(|number| number.parse::<usize>().ok())
                else {
                    continue;
                };
                let data = std::fs::read(entry.path())?;
                fingerprints.insert(part_number, shard_fingerprint(&data)?);
            }
            fingerprints
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
        Err(error) => return Err(error.into()),
    };

    Ok(VersionShardCensus {
        version_id,
        has_xl_meta: true,
        data_dir,
        erasure_index,
        expected_part_numbers,
        present_part_fingerprints,
        inline_data_fingerprint,
    })
}

/// `POST` a signed (SigV4, service `s3`) admin request without relying on the
/// external `awscurl` binary. Mirrors the admin heal calls used by the heal
/// regression suite.
pub async fn signed_admin_post(url: &str, body: Option<&str>, access_key: &str, secret_key: &str) -> ChaosResult<String> {
    let uri = url.parse::<http::Uri>()?;
    let authority = uri.authority().ok_or("request URL missing authority")?.to_string();
    let mut request = http::Request::builder()
        .method(http::Method::POST)
        .uri(uri)
        .header(HOST, authority)
        .header("x-amz-content-sha256", UNSIGNED_PAYLOAD);

    if body.is_some() {
        request = request.header(CONTENT_TYPE, "application/json");
    }

    let content_len = body.map(str::len).unwrap_or_default() as i64;
    let signed = sign_v4(request.body(Body::empty())?, content_len, access_key, secret_key, "", "us-east-1");

    let mut request_builder = local_http_client().post(url);
    for (name, value) in signed.headers() {
        request_builder = request_builder.header(name, value);
    }
    if let Some(body) = body {
        request_builder = request_builder.body(body.to_string());
    }

    let response = request_builder.send().await?;
    let status = response.status();
    let body = response.text().await?;
    if !status.is_success() {
        return Err(format!("admin POST failed: {status} {body}").into());
    }

    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn complete_census() -> VersionShardCensus {
        VersionShardCensus {
            version_id: Some("version".to_string()),
            has_xl_meta: true,
            data_dir: Some("data-dir".to_string()),
            erasure_index: Some(3),
            expected_part_numbers: BTreeSet::from([1]),
            present_part_fingerprints: BTreeMap::from([(1, shard_fingerprint(b"part").unwrap())]),
            inline_data_fingerprint: None,
        }
    }

    #[test]
    fn shard_fingerprint_uses_physical_length_and_sha256() {
        assert_eq!(
            shard_fingerprint(b"abc").unwrap(),
            PartShardFingerprint {
                size: 3,
                sha256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".to_string(),
            }
        );
    }

    #[test]
    fn manifest_requires_matching_inline_payload() {
        let mut expected = complete_census();
        expected.expected_part_numbers.clear();
        expected.present_part_fingerprints.clear();
        expected.inline_data_fingerprint = Some(shard_fingerprint(b"expected").unwrap());
        let mut changed = expected.clone();
        changed.inline_data_fingerprint = Some(shard_fingerprint(b"changed").unwrap());
        assert!(expected.matches_manifest(&expected));
        assert!(!changed.matches_manifest(&expected));
    }
}
