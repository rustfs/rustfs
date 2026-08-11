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

//! Privileged Linux replacement-heal E2E.
//!
//! These ignored tests exercise the documented drive-swap path from rustfs#5869:
//! a 3-node x 4-drive cluster, a restarted target node whose absent endpoint is
//! observed by the scanner, a blank replacement mounted back at the same
//! endpoint, and automatic recovery without an Admin deep-heal request. The final
//! assertion is a per-version physical census of the replacement target's
//! `xl.meta` and `part.N` files.

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use crate::chaos::{VersionShardCensus, census_object_version_on_disk};
    use crate::common::{ClusterTopology, RustFSTestClusterEnvironment, admin_request, init_logging};
    use aws_sdk_s3::Client;
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::{BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, VersioningConfiguration};
    use http::Method;
    use serial_test::serial;
    use sha2::{Digest, Sha256};
    use std::collections::BTreeSet;
    use std::error::Error;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use tokio::time::{Duration, Instant, interval};
    use tracing::info;

    const ENABLE_ENV: &str = "RUSTFS_PRIVILEGED_REPLACEMENT_E2E";
    const NAMESPACE_ENV: &str = "RUSTFS_PRIVILEGED_REPLACEMENT_E2E_IN_NAMESPACE";
    const TARGET_NODE: usize = 1;
    const TARGET_DRIVE: usize = 0;
    const MOUNT_SIZE: &str = "size=128m,mode=0700";
    const ABSENT_SCANNER_OBSERVATION_TIMEOUT_SECS: u64 = 180;
    const REPLACEMENT_RECOVERY_DIR: &str = ".rustfs.sys/buckets/ahm-replacement";
    const REPLACEMENT_INTENT_SUFFIX: &str = "_ahm_replacement_intent.json";
    const REPLACEMENT_COMPLETION_PROOF_SUFFIX: &str = "_ahm_replacement_completion_proof.json";
    const RESUME_CHECKPOINT_SUFFIX: &str = "_ahm_checkpoint.json";

    #[derive(Debug)]
    struct BaselineVersion {
        bucket: String,
        key: String,
        version_id: Option<String>,
        body_sha256: Option<String>,
        expected: VersionShardCensus,
    }

    struct MountNamespaceGuard {
        mounts: Vec<PathBuf>,
    }

    struct FaultableBlockMount {
        target: PathBuf,
        image: PathBuf,
        loop_device: String,
        dm_name: String,
        mounted: bool,
        dm_created: bool,
    }

    impl MountNamespaceGuard {
        fn new() -> Result<Self, Box<dyn Error + Send + Sync>> {
            verify_isolated_mount_namespace()?;
            run_command("mount", ["--make-rprivate", "/"])?;
            Ok(Self { mounts: Vec::new() })
        }

        fn mount_tmpfs(&mut self, target: &Path, label: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
            mount_tmpfs(target, label)?;
            self.mounts.push(target.to_path_buf());
            Ok(())
        }
    }

    impl Drop for MountNamespaceGuard {
        fn drop(&mut self) {
            for mount in self.mounts.iter().rev() {
                let _ = detach_mount(mount);
            }
        }
    }

    impl FaultableBlockMount {
        fn mount(target: &Path, image_root: &Path, label: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
            fs::create_dir_all(image_root)?;
            let image = image_root.join(format!("{label}.img"));
            let file = fs::File::create(&image)?;
            file.set_len(256 * 1024 * 1024)?;
            drop(file);

            let image_arg = path_to_string(&image, "loop image")?;
            let loop_device = run_command_stdout("losetup", &["--find", "--show", &image_arg])?;
            if loop_device.is_empty() {
                return Err("losetup --find --show returned an empty loop device".into());
            }

            run_command_dynamic("mkfs.ext4", &["-F", &loop_device])?;
            let sectors = run_command_stdout("blockdev", &["--getsz", &loop_device])?;
            let dm_name = format!("rustfs_e2e_{label}_{}", std::process::id());
            let table = format!("0 {sectors} linear {loop_device} 0");
            let mapper = format!("/dev/mapper/{dm_name}");
            run_command_dynamic("dmsetup", &["create", &dm_name, "--table", &table])?;

            let target_arg = path_to_string(target, "faultable mount target")?;
            run_command_dynamic("mount", &[&mapper, &target_arg])?;

            Ok(Self {
                target: target.to_path_buf(),
                image,
                loop_device,
                dm_name,
                mounted: true,
                dm_created: true,
            })
        }

        fn make_unavailable(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
            let sectors = run_command_stdout("blockdev", &["--getsz", &self.loop_device])?;
            let error_table = format!("0 {sectors} error");
            run_command_dynamic("dmsetup", &["suspend", &self.dm_name])?;
            run_command_dynamic("dmsetup", &["load", &self.dm_name, "--table", &error_table])?;
            run_command_dynamic("dmsetup", &["resume", &self.dm_name])
        }

        fn restore_available(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
            let sectors = run_command_stdout("blockdev", &["--getsz", &self.loop_device])?;
            let linear_table = format!("0 {sectors} linear {} 0", self.loop_device);
            run_command_dynamic("dmsetup", &["suspend", &self.dm_name])?;
            run_command_dynamic("dmsetup", &["load", &self.dm_name, "--table", &linear_table])?;
            run_command_dynamic("dmsetup", &["resume", &self.dm_name])
        }

        fn cleanup(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
            let mut first_error: Option<Box<dyn Error + Send + Sync>> = None;
            if self.dm_created {
                let _ = self.restore_available();
            }
            if self.mounted {
                if let Err(error) = detach_mount(&self.target) {
                    first_error.get_or_insert(error);
                } else {
                    self.mounted = false;
                }
            }
            if self.dm_created {
                if let Err(error) = run_command_dynamic("dmsetup", &["remove", "-f", &self.dm_name]) {
                    first_error.get_or_insert(error);
                } else {
                    self.dm_created = false;
                }
            }
            if !self.loop_device.is_empty() {
                if let Err(error) = run_command_dynamic("losetup", &["-d", &self.loop_device]) {
                    first_error.get_or_insert(error);
                } else {
                    self.loop_device.clear();
                }
            }
            if self.image.exists()
                && let Err(error) = fs::remove_file(&self.image)
            {
                first_error.get_or_insert(error.into());
            }
            if let Some(error) = first_error {
                return Err(error);
            }
            Ok(())
        }
    }

    impl Drop for FaultableBlockMount {
        fn drop(&mut self) {
            let _ = self.cleanup();
        }
    }

    fn run_command<const N: usize>(program: &str, args: [&str; N]) -> Result<(), Box<dyn Error + Send + Sync>> {
        run_command_dynamic(program, &args)
    }

    fn run_command_dynamic(program: &str, args: &[&str]) -> Result<(), Box<dyn Error + Send + Sync>> {
        let output = Command::new(program).args(args).output()?;
        if output.status.success() {
            return Ok(());
        }
        Err(format!(
            "{program} {} failed with status {}: stdout={} stderr={}",
            args.join(" "),
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
        .into())
    }

    fn run_command_stdout(program: &str, args: &[&str]) -> Result<String, Box<dyn Error + Send + Sync>> {
        let output = Command::new(program).args(args).output()?;
        if output.status.success() {
            return Ok(String::from_utf8_lossy(&output.stdout).trim().to_string());
        }
        Err(format!(
            "{program} {} failed with status {}: stdout={} stderr={}",
            args.join(" "),
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
        .into())
    }

    fn path_to_string(path: &Path, label: &str) -> Result<String, Box<dyn Error + Send + Sync>> {
        path.to_str()
            .map(str::to_owned)
            .ok_or_else(|| format!("{label} path is not UTF-8: {path:?}").into())
    }

    fn mount_namespace_link(proc_entry: &str) -> Result<String, Box<dyn Error + Send + Sync>> {
        Ok(fs::read_link(format!("/proc/{proc_entry}/ns/mnt"))?
            .to_string_lossy()
            .into_owned())
    }

    fn parent_pid() -> Result<String, Box<dyn Error + Send + Sync>> {
        let status = fs::read_to_string("/proc/self/status")?;
        for line in status.lines() {
            if let Some(ppid) = line.strip_prefix("PPid:") {
                return Ok(ppid.trim().to_string());
            }
        }
        Err("/proc/self/status does not contain PPid".into())
    }

    fn verify_isolated_mount_namespace() -> Result<(), Box<dyn Error + Send + Sync>> {
        let current = mount_namespace_link("self")?;
        let parent = mount_namespace_link(&parent_pid()?)?;
        if current == parent {
            return Err(format!(
                "privileged replacement E2E must run in a private mount namespace before mounting test drives; current namespace {current} still matches parent"
            )
            .into());
        }
        Ok(())
    }

    fn mount_tmpfs(target: &Path, label: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        let target = target
            .to_str()
            .ok_or_else(|| format!("tmpfs target path is not UTF-8: {target:?}"))?;
        run_command("mount", ["-t", "tmpfs", "-o", MOUNT_SIZE, label, target])
    }

    fn detach_mount(target: &Path) -> Result<(), Box<dyn Error + Send + Sync>> {
        let target = target
            .to_str()
            .ok_or_else(|| format!("umount target path is not UTF-8: {target:?}"))?;
        run_command("umount", [target])
    }

    fn privileged_run_enabled() -> Result<bool, Box<dyn Error + Send + Sync>> {
        let enabled = std::env::var(ENABLE_ENV)
            .ok()
            .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"));
        if !enabled {
            info!("{ENABLE_ENV}=1 is not set; privileged replacement E2E is skipped");
            return Ok(false);
        }
        Ok(true)
    }

    fn run_current_test_in_mount_namespace(test_name: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        let test_binary = std::env::current_exe()?;
        let status = Command::new("unshare")
            .arg("--mount")
            .arg("--propagation")
            .arg("private")
            .arg("--")
            .arg(test_binary)
            .arg("--exact")
            .arg(test_name)
            .arg("--ignored")
            .arg("--nocapture")
            .env(NAMESPACE_ENV, "1")
            .status()?;
        if status.success() {
            return Ok(());
        }
        Err(format!("{ENABLE_ENV}=1 requires root or CAP_SYS_ADMIN; unshare exited with status {status}").into())
    }

    fn payload(len: usize, seed: u8) -> Vec<u8> {
        let mut next = seed;
        (0..len)
            .map(|_| {
                let byte = next;
                next = next.wrapping_add(31);
                byte
            })
            .collect()
    }

    fn sha256_hex(data: &[u8]) -> String {
        let digest = Sha256::digest(data);
        digest.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    async fn put_object_version(
        client: &Client,
        bucket: &str,
        key: &str,
        body: Vec<u8>,
    ) -> Result<(Option<String>, String), Box<dyn Error + Send + Sync>> {
        let digest = sha256_hex(&body);
        let output = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(body))
            .send()
            .await?;
        Ok((output.version_id().map(str::to_owned), digest))
    }

    async fn put_multipart_version(
        client: &Client,
        bucket: &str,
        key: &str,
        parts: Vec<Vec<u8>>,
    ) -> Result<(Option<String>, String), Box<dyn Error + Send + Sync>> {
        let body = parts.iter().flatten().copied().collect::<Vec<_>>();
        let digest = sha256_hex(&body);
        let create = client.create_multipart_upload().bucket(bucket).key(key).send().await?;
        let upload_id = create
            .upload_id()
            .ok_or("create_multipart_upload returned no upload id")?
            .to_string();
        let mut completed_parts = Vec::with_capacity(parts.len());
        for (index, part) in parts.into_iter().enumerate() {
            let part_number = i32::try_from(index + 1)?;
            let uploaded = client
                .upload_part()
                .bucket(bucket)
                .key(key)
                .upload_id(&upload_id)
                .part_number(part_number)
                .body(ByteStream::from(part))
                .send()
                .await?;
            completed_parts.push(
                CompletedPart::builder()
                    .part_number(part_number)
                    .e_tag(uploaded.e_tag().ok_or("upload_part returned no etag")?)
                    .build(),
            );
        }
        let completed = client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(completed_parts)).build())
            .send()
            .await?;
        Ok((completed.version_id().map(str::to_owned), digest))
    }

    async fn seed_baseline(client: &Client, target_disk: &Path) -> Result<Vec<BaselineVersion>, Box<dyn Error + Send + Sync>> {
        let plain_bucket = "priv-replacement-plain";
        let versioned_bucket = "priv-replacement-versions";
        let null_bucket = "priv-replacement-null";

        client.create_bucket().bucket(plain_bucket).send().await?;
        client.create_bucket().bucket(versioned_bucket).send().await?;
        client.create_bucket().bucket(null_bucket).send().await?;
        client
            .put_bucket_versioning()
            .bucket(versioned_bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send()
            .await?;
        let mut versions = Vec::new();
        let (version_id, body_sha256) =
            put_object_version(client, plain_bucket, "objects/single-part.bin", payload(512 * 1024, 1)).await?;
        versions.push((plain_bucket, "objects/single-part.bin", version_id, Some(body_sha256)));

        let (version_id, body_sha256) = put_multipart_version(
            client,
            plain_bucket,
            "objects/multipart.bin",
            vec![payload(5 * 1024 * 1024, 2), payload(1024 * 1024, 3)],
        )
        .await?;
        versions.push((plain_bucket, "objects/multipart.bin", version_id, Some(body_sha256)));

        let (version_id, body_sha256) =
            put_object_version(client, versioned_bucket, "history/object.bin", payload(512 * 1024, 4)).await?;
        versions.push((versioned_bucket, "history/object.bin", version_id, Some(body_sha256)));
        let (version_id, body_sha256) =
            put_object_version(client, versioned_bucket, "history/object.bin", payload(768 * 1024, 5)).await?;
        versions.push((versioned_bucket, "history/object.bin", version_id, Some(body_sha256)));
        let deleted = client
            .delete_object()
            .bucket(versioned_bucket)
            .key("history/object.bin")
            .send()
            .await?;
        versions.push((versioned_bucket, "history/object.bin", deleted.version_id().map(str::to_owned), None));

        let (version_id, body_sha256) = put_multipart_version(
            client,
            versioned_bucket,
            "history/multipart.bin",
            vec![payload(5 * 1024 * 1024, 6), payload(2 * 1024 * 1024, 7)],
        )
        .await?;
        versions.push((versioned_bucket, "history/multipart.bin", version_id, Some(body_sha256)));

        let (version_id, body_sha256) =
            put_object_version(client, null_bucket, "null/current.bin", payload(512 * 1024, 8)).await?;
        versions.push((null_bucket, "null/current.bin", version_id, Some(body_sha256)));

        versions
            .into_iter()
            .map(|(bucket, key, version_id, body_sha256)| {
                let expected = census_object_version_on_disk(target_disk, bucket, key, version_id.as_deref())?;
                if !expected.is_complete() {
                    return Err(format!("baseline census is incomplete for {bucket}/{key}@{version_id:?}: {expected:?}").into());
                }
                Ok(BaselineVersion {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    version_id,
                    body_sha256,
                    expected,
                })
            })
            .collect()
    }

    async fn verify_bodies(client: &Client, versions: &[BaselineVersion]) -> Result<(), Box<dyn Error + Send + Sync>> {
        for version in versions {
            let Some(expected_sha256) = &version.body_sha256 else {
                continue;
            };
            let mut request = client.get_object().bucket(&version.bucket).key(&version.key);
            if let Some(version_id) = &version.version_id {
                request = request.version_id(version_id);
            }
            let response = request.send().await?;
            let body = response.body.collect().await?.into_bytes();
            assert_eq!(
                sha256_hex(&body),
                *expected_sha256,
                "body hash changed for {}/{}@{:?}",
                version.bucket,
                version.key,
                version.version_id
            );
        }
        Ok(())
    }

    async fn replacement_status(
        cluster: &RustFSTestClusterEnvironment,
    ) -> Result<serde_json::Value, Box<dyn Error + Send + Sync>> {
        let (status, body) = admin_request(
            &cluster.nodes[0].url,
            Method::GET,
            "/rustfs/admin/v4/heal/replacement-recovery",
            None,
            &cluster.access_key,
            &cluster.secret_key,
        )
        .await?;
        if !status.is_success() {
            return Err(format!("replacement recovery status failed: {status} {body}").into());
        }
        Ok(serde_json::from_str(&body)?)
    }

    fn replacement_artifact_targets_disk(
        artifact: &serde_json::Value,
        target_disk: &Path,
    ) -> Result<bool, Box<dyn Error + Send + Sync>> {
        let target = target_disk.to_string_lossy();
        Ok(artifact["replacement_targets"]
            .as_array()
            .ok_or("replacement artifact has no replacement_targets")?
            .iter()
            .any(|slot| slot.as_str().is_some_and(|slot| slot.contains(target.as_ref()))))
    }

    fn assert_no_replacement_admission_artifacts(
        cluster: &RustFSTestClusterEnvironment,
        target_disk: &Path,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        for node in &cluster.nodes {
            for drive in &node.data_dirs {
                let drive = Path::new(drive);
                let recovery_dir = drive.join(REPLACEMENT_RECOVERY_DIR);
                match fs::read_dir(&recovery_dir) {
                    Ok(entries) => {
                        for entry in entries {
                            let entry = entry?;
                            let file_name = entry.file_name().to_string_lossy().into_owned();
                            if file_name.ends_with(REPLACEMENT_INTENT_SUFFIX)
                                || file_name.ends_with(REPLACEMENT_COMPLETION_PROOF_SUFFIX)
                            {
                                let artifact: serde_json::Value = serde_json::from_slice(&fs::read(entry.path())?)?;
                                if replacement_artifact_targets_disk(&artifact, target_disk)? {
                                    return Err(format!(
                                        "absent replacement target was admitted before it became ready: {:?}",
                                        entry.path()
                                    )
                                    .into());
                                }
                                continue;
                            }
                            if file_name.ends_with(RESUME_CHECKPOINT_SUFFIX) {
                                return Err(format!(
                                    "replacement created checkpoint while target was absent: {:?}",
                                    entry.path()
                                )
                                .into());
                            }
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(_) if drive == target_disk => {}
                    Err(error) => {
                        return Err(format!("failed to read replacement recovery dir {recovery_dir:?}: {error}").into());
                    }
                }

                let bucket_meta_dir = drive.join(".rustfs.sys").join("buckets");
                let entries = match fs::read_dir(&bucket_meta_dir) {
                    Ok(entries) => entries,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(_) if drive == target_disk => continue,
                    Err(error) => return Err(format!("failed to read bucket metadata dir {bucket_meta_dir:?}: {error}").into()),
                };
                for entry in entries {
                    let entry = entry?;
                    let file_name = entry.file_name().to_string_lossy().into_owned();
                    if file_name.ends_with(RESUME_CHECKPOINT_SUFFIX) {
                        return Err(format!("replacement created checkpoint while target was absent: {:?}", entry.path()).into());
                    }
                }
            }
        }

        let marker = target_disk.join(".rustfs.sys").join("healing.bin");
        if marker.exists() {
            return Err(format!("replacement created healing marker while target was absent: {marker:?}").into());
        }
        Ok(())
    }

    fn log_tail(log: &str) -> String {
        let mut lines = log.lines().rev().take(80).collect::<Vec<_>>();
        lines.reverse();
        lines.join("\n")
    }

    fn log_len(path: &Path) -> Result<u64, Box<dyn Error + Send + Sync>> {
        match fs::metadata(path) {
            Ok(metadata) => Ok(metadata.len()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(error) => Err(format!("failed to stat target node log {path:?}: {error}").into()),
        }
    }

    fn log_from_offset(path: &Path, offset: u64) -> Result<String, Box<dyn Error + Send + Sync>> {
        let log = match fs::read(path) {
            Ok(log) => log,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(format!("failed to read target node log {path:?}: {error}").into()),
        };
        let start = usize::try_from(offset).unwrap_or(usize::MAX).min(log.len());
        Ok(String::from_utf8_lossy(&log[start..]).into_owned())
    }

    async fn wait_for_live_disk_loss_observation(
        log_path: &Path,
        target_disk: &Path,
        start_offset: u64,
        timeout_secs: u64,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let deadline = Instant::now() + Duration::from_secs(timeout_secs);
        let mut tick = interval(Duration::from_secs(1));
        let target = target_disk.to_string_lossy();
        loop {
            let log = log_from_offset(log_path, start_offset)?;
            let mut saw_live_loss = false;
            for line in log.lines() {
                if line.contains("check_failed") && line.contains(target.as_ref()) {
                    saw_live_loss = true;
                    continue;
                }
                if saw_live_loss && line.contains("Heal auto disk scanner idle") {
                    return Ok(());
                }
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "scanner did not finish a live target-loss scan for {target_disk:?} within {timeout_secs}s; log tail:\n{}",
                    log_tail(&log)
                )
                .into());
            }
            tick.tick().await;
        }
    }

    fn require_definitive_replacement_status(
        status: &serde_json::Value,
        context: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if status["cluster"]["definitive"].as_bool().unwrap_or(false) {
            return Ok(());
        }
        Err(format!("{context} requires a definitive cluster replacement status: {status}").into())
    }

    async fn assert_no_replacement_status_records(
        cluster: &RustFSTestClusterEnvironment,
        target_disk: &Path,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let status = replacement_status(cluster).await?;
        require_definitive_replacement_status(&status, "live missing replacement status check")?;
        let states = target_record_states(&status, target_disk);
        if states.is_empty() {
            return Ok(());
        }
        Err(format!(
            "live missing replacement target must not have durable recovery records; observed states {states:?} in status {status}"
        )
        .into())
    }

    fn target_record_has_state(status: &serde_json::Value, target_disk: &Path, states: &[&str]) -> bool {
        let present_states = target_record_states(status, target_disk);
        states.iter().any(|state| present_states.contains(*state))
    }

    fn target_record_states(status: &serde_json::Value, target_disk: &Path) -> BTreeSet<String> {
        let target = target_disk.to_string_lossy();
        status["cluster"]["records"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|record| {
                let state = record["state"].as_str()?;
                let target_matches = record["targetSlots"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(serde_json::Value::as_str)
                    .any(|slot| slot.contains(target.as_ref()));
                target_matches.then(|| state.to_string())
            })
            .collect()
    }

    fn incomplete_versions(
        target_disk: &Path,
        versions: &[BaselineVersion],
    ) -> Result<BTreeSet<String>, Box<dyn Error + Send + Sync>> {
        let mut missing = BTreeSet::new();
        for version in versions {
            let actual =
                census_object_version_on_disk(target_disk, &version.bucket, &version.key, version.version_id.as_deref())?;
            if !actual.matches_manifest(&version.expected) {
                missing.insert(format!("{}/{}@{:?}: {actual:?}", version.bucket, version.key, version.version_id));
            }
        }
        Ok(missing)
    }

    async fn wait_for_completed_replacement_with_census(
        cluster: &RustFSTestClusterEnvironment,
        target_disk: &Path,
        versions: &[BaselineVersion],
        timeout_secs: u64,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let deadline = Instant::now() + Duration::from_secs(timeout_secs);
        let mut tick = interval(Duration::from_secs(1));
        loop {
            let status = replacement_status(cluster).await?;
            let missing = incomplete_versions(target_disk, versions)?;
            if require_definitive_replacement_status(&status, "replacement completion poll").is_err() {
                if Instant::now() >= deadline {
                    return Err(format!(
	                        "replacement recovery status never became definitive within {timeout_secs}s while waiting for physical census; latest status: {status}; missing: {missing:?}"
	                    )
	                    .into());
                }
                tick.tick().await;
                continue;
            }
            if target_record_has_state(&status, target_disk, &["completed"]) {
                if !missing.is_empty() {
                    return Err(format!(
	                        "replacement status reached completed before target physical census matched baseline: {missing:?}; status: {status}"
	                    )
	                    .into());
                }
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(format!(
	                    "replacement target did not reach completed with matching physical census within {timeout_secs}s: missing={missing:?}; status={status}"
	                )
	                .into());
            }
            tick.tick().await;
        }
    }

    async fn run_replacement_e2e(parity: usize, test_name: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        if !privileged_run_enabled()? {
            return Ok(());
        }
        if std::env::var_os(NAMESPACE_ENV).is_none() {
            return run_current_test_in_mount_namespace(test_name);
        }
        verify_isolated_mount_namespace()?;

        let mut mount_ns = MountNamespaceGuard::new()?;
        let mut cluster = RustFSTestClusterEnvironment::with_topology(ClusterTopology::single_pool_multidrive(3, 4)).await?;
        let target_log_path = PathBuf::from(&cluster.temp_dir).join(format!("replacement-node{TARGET_NODE}.log"));
        cluster.set_node_capture_log_path(TARGET_NODE, target_log_path.to_string_lossy())?;
        let target_disk = PathBuf::from(&cluster.nodes[TARGET_NODE].data_dirs[TARGET_DRIVE]);
        // Each drive below is an independent tmpfs mount, so this privileged
        // path must exercise the production distinct-device/readiness fences.
        cluster.extra_env.retain(|(key, _)| key != "RUSTFS_UNSAFE_BYPASS_DISK_CHECK");
        let image_root = PathBuf::from(&cluster.temp_dir).join("replacement-faultable-images");
        let mut target_mount = None;
        for (node_index, node) in cluster.nodes.iter().enumerate() {
            for (drive_index, drive) in node.data_dirs.iter().enumerate() {
                let drive = Path::new(drive);
                if node_index == TARGET_NODE && drive_index == TARGET_DRIVE {
                    target_mount = Some(FaultableBlockMount::mount(
                        drive,
                        &image_root,
                        &format!("p{parity}_node{node_index}_drive{drive_index}"),
                    )?);
                } else {
                    mount_ns.mount_tmpfs(drive, &format!("rustfs-e2e-p{parity}-node{node_index}-drive{drive_index}"))?;
                }
            }
        }
        let mut target_mount = target_mount.ok_or("target drive was not mounted with the faultable block fixture")?;

        cluster.set_env("RUSTFS_HEAL_ENABLED", "true");
        cluster.set_env("RUSTFS_SCANNER_ENABLED", "true");
        cluster.set_env("RUSTFS_HEAL_INTERVAL_SECS", "1");
        cluster.set_env("RUSTFS_SCANNER_CYCLE", "1");
        cluster.set_env("RUSTFS_SCANNER_START_DELAY_SECS", "0");
        cluster.set_env("RUSTFS_STORAGE_CLASS_STANDARD", format!("EC:{parity}"));
        cluster.set_node_env(TARGET_NODE, "RUST_LOG", "rustfs=info,rustfs::heal::manager=debug,rustfs_notify=debug")?;
        cluster.start().await?;

        let clients = cluster.create_all_clients()?;
        let versions = seed_baseline(&clients[0], &target_disk).await?;
        verify_bodies(&clients[0], &versions).await?;

        let live_loss_log_offset = log_len(&target_log_path)?;
        target_mount.make_unavailable()?;
        wait_for_live_disk_loss_observation(
            &target_log_path,
            &target_disk,
            live_loss_log_offset,
            ABSENT_SCANNER_OBSERVATION_TIMEOUT_SECS,
        )
        .await?;
        assert_no_replacement_status_records(&cluster, &target_disk).await?;
        assert_no_replacement_admission_artifacts(&cluster, &target_disk)?;

        cluster.stop_node(TARGET_NODE)?;
        target_mount.cleanup()?;
        mount_ns.mount_tmpfs(&target_disk, &format!("rustfs-e2e-p{parity}-replacement"))?;
        let missing_before_restart = incomplete_versions(&target_disk, &versions)?;
        assert_eq!(
            missing_before_restart.len(),
            versions.len(),
            "blank replacement must start without any baseline version shards"
        );
        cluster.start_node(TARGET_NODE).await?;

        wait_for_completed_replacement_with_census(&cluster, &target_disk, &versions, 420).await?;
        verify_bodies(&clients[0], &versions).await?;

        Ok(())
    }

    /// Linux mount namespaces are per-thread; keep mount setup and process
    /// spawning on one OS thread so child RustFS nodes inherit the test mounts.
    #[tokio::test(flavor = "current_thread")]
    #[serial]
    #[ignore = "requires Linux root/CAP_SYS_ADMIN and RUSTFS_PRIVILEGED_REPLACEMENT_E2E=1"]
    async fn test_privileged_3x4_auto_replacement_rebuilds_ec8_plus_4_without_admin_heal()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        run_replacement_e2e(
            4,
            "replacement_privileged_e2e_test::tests::test_privileged_3x4_auto_replacement_rebuilds_ec8_plus_4_without_admin_heal",
        )
        .await
    }

    /// Linux mount namespaces are per-thread; keep mount setup and process
    /// spawning on one OS thread so child RustFS nodes inherit the test mounts.
    #[tokio::test(flavor = "current_thread")]
    #[serial]
    #[ignore = "requires Linux root/CAP_SYS_ADMIN and RUSTFS_PRIVILEGED_REPLACEMENT_E2E=1"]
    async fn test_privileged_3x4_auto_replacement_rebuilds_ec6_plus_6_without_admin_heal()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        run_replacement_e2e(
            6,
            "replacement_privileged_e2e_test::tests::test_privileged_3x4_auto_replacement_rebuilds_ec6_plus_6_without_admin_heal",
        )
        .await
    }
}
