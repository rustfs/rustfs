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
//! a 3-node x 4-drive cluster, a runtime target detach observed by the scanner,
//! a blank replacement mounted back at the same endpoint, and automatic recovery
//! without an Admin deep-heal request. The final assertion is a per-version
//! physical census of the replacement target's `xl.meta` and `part.N` files.

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
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    use std::path::{Path, PathBuf};
    use tokio::time::{Duration, Instant, interval};
    use tracing::info;

    const ENABLE_ENV: &str = "RUSTFS_PRIVILEGED_REPLACEMENT_E2E";
    const TARGET_NODE: usize = 1;
    const TARGET_DRIVE: usize = 0;
    const MOUNT_SIZE: &str = "size=128m,mode=0700";

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

    impl MountNamespaceGuard {
        fn new() -> Result<Self, Box<dyn Error + Send + Sync>> {
            let rc = unsafe { libc::unshare(libc::CLONE_NEWNS) };
            if rc != 0 {
                return Err(format!("unshare(CLONE_NEWNS) failed: {}", std::io::Error::last_os_error()).into());
            }
            mount_private_root()?;
            Ok(Self { mounts: Vec::new() })
        }

        fn mount_tmpfs(&mut self, target: &Path, label: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
            mount_tmpfs(target, label)?;
            self.mounts.push(target.to_path_buf());
            Ok(())
        }

        fn detach(&mut self, target: &Path) -> Result<(), Box<dyn Error + Send + Sync>> {
            detach_mount(target)?;
            self.mounts.retain(|mounted| mounted != target);
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

    fn c_path(path: &Path) -> Result<CString, Box<dyn Error + Send + Sync>> {
        Ok(CString::new(path.as_os_str().as_bytes())?)
    }

    fn mount_private_root() -> Result<(), Box<dyn Error + Send + Sync>> {
        let root = CString::new("/")?;
        let rc = unsafe {
            libc::mount(
                std::ptr::null(),
                root.as_ptr(),
                std::ptr::null(),
                (libc::MS_REC | libc::MS_PRIVATE) as libc::c_ulong,
                std::ptr::null(),
            )
        };
        if rc != 0 {
            return Err(format!("making the mount namespace private failed: {}", std::io::Error::last_os_error()).into());
        }
        Ok(())
    }

    fn mount_tmpfs(target: &Path, label: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        let source = CString::new(label)?;
        let target_c = c_path(target)?;
        let fstype = CString::new("tmpfs")?;
        let data = CString::new(MOUNT_SIZE)?;
        let rc = unsafe {
            libc::mount(
                source.as_ptr(),
                target_c.as_ptr(),
                fstype.as_ptr(),
                (libc::MS_NOSUID | libc::MS_NODEV) as libc::c_ulong,
                data.as_ptr().cast(),
            )
        };
        if rc != 0 {
            return Err(format!("mount(tmpfs) failed for {target:?}: {}", std::io::Error::last_os_error()).into());
        }
        Ok(())
    }

    fn detach_mount(target: &Path) -> Result<(), Box<dyn Error + Send + Sync>> {
        let target_c = c_path(target)?;
        let rc = unsafe { libc::umount2(target_c.as_ptr(), libc::MNT_DETACH) };
        if rc != 0 {
            return Err(format!("umount2(MNT_DETACH) failed for {target:?}: {}", std::io::Error::last_os_error()).into());
        }
        Ok(())
    }

    fn privileged_run_enabled() -> Result<bool, Box<dyn Error + Send + Sync>> {
        let enabled = std::env::var(ENABLE_ENV)
            .ok()
            .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"));
        if !enabled {
            info!("{ENABLE_ENV}=1 is not set; privileged replacement E2E is skipped");
            return Ok(false);
        }
        if unsafe { libc::geteuid() } != 0 {
            return Err(
                format!("{ENABLE_ENV}=1 requires root or CAP_SYS_ADMIN so the test can unshare and mount tmpfs drives").into(),
            );
        }
        Ok(true)
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

    fn target_record_has_state(status: &serde_json::Value, target_disk: &Path, states: &[&str]) -> bool {
        let target = target_disk.to_string_lossy();
        status["cluster"]["records"].as_array().into_iter().flatten().any(|record| {
            let state_matches = record["state"].as_str().is_some_and(|state| states.contains(&state));
            let target_matches = record["targetSlots"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(serde_json::Value::as_str)
                .any(|slot| slot.contains(target.as_ref()));
            state_matches && target_matches
        })
    }

    async fn wait_for_replacement_state(
        cluster: &RustFSTestClusterEnvironment,
        target_disk: &Path,
        states: &[&str],
        timeout_secs: u64,
    ) -> Result<serde_json::Value, Box<dyn Error + Send + Sync>> {
        let deadline = Instant::now() + Duration::from_secs(timeout_secs);
        let mut tick = interval(Duration::from_secs(1));
        loop {
            let status = replacement_status(cluster).await?;
            if target_record_has_state(&status, target_disk, states) {
                return Ok(status);
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "replacement recovery status did not reach {states:?} for {target_disk:?} within {timeout_secs}s: {status}"
                )
                .into());
            }
            tick.tick().await;
        }
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

    async fn wait_for_physical_census(
        target_disk: &Path,
        versions: &[BaselineVersion],
        timeout_secs: u64,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let deadline = Instant::now() + Duration::from_secs(timeout_secs);
        let mut tick = interval(Duration::from_secs(1));
        loop {
            let missing = incomplete_versions(target_disk, versions)?;
            if missing.is_empty() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "replacement target physical census did not match baseline within {timeout_secs}s: {missing:?}"
                )
                .into());
            }
            tick.tick().await;
        }
    }

    async fn run_replacement_e2e(parity: usize) -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        if !privileged_run_enabled()? {
            return Ok(());
        }

        let mut mount_ns = MountNamespaceGuard::new()?;
        let mut cluster = RustFSTestClusterEnvironment::with_topology(ClusterTopology::single_pool_multidrive(3, 4)).await?;
        // Each drive below is an independent tmpfs mount, so this privileged
        // path must exercise the production distinct-device/readiness fences.
        cluster.extra_env.retain(|(key, _)| key != "RUSTFS_UNSAFE_BYPASS_DISK_CHECK");
        for (node_index, node) in cluster.nodes.iter().enumerate() {
            for (drive_index, drive) in node.data_dirs.iter().enumerate() {
                mount_ns.mount_tmpfs(Path::new(drive), &format!("rustfs-e2e-p{parity}-node{node_index}-drive{drive_index}"))?;
            }
        }

        cluster.set_env("RUSTFS_HEAL_ENABLED", "true");
        cluster.set_env("RUSTFS_SCANNER_ENABLED", "true");
        cluster.set_env("RUSTFS_HEAL_INTERVAL_SECS", "1");
        cluster.set_env("RUSTFS_SCANNER_CYCLE", "1");
        cluster.set_env("RUSTFS_SCANNER_START_DELAY_SECS", "0");
        cluster.set_env("RUSTFS_STORAGE_CLASS_STANDARD", format!("EC:{parity}"));
        cluster.start().await?;

        let clients = cluster.create_all_clients()?;
        let target_disk = PathBuf::from(&cluster.nodes[TARGET_NODE].data_dirs[TARGET_DRIVE]);
        let versions = seed_baseline(&clients[0], &target_disk).await?;
        verify_bodies(&clients[0], &versions).await?;

        mount_ns.detach(&target_disk)?;
        let missing_after_detach = incomplete_versions(&target_disk, &versions)?;
        assert_eq!(
            missing_after_detach.len(),
            versions.len(),
            "detached target path must not still expose baseline shards"
        );
        wait_for_replacement_state(&cluster, &target_disk, &["waiting_for_replacement", "running"], 180).await?;

        cluster.stop_node(TARGET_NODE)?;
        mount_ns.mount_tmpfs(&target_disk, &format!("rustfs-e2e-p{parity}-replacement"))?;
        let missing_before_restart = incomplete_versions(&target_disk, &versions)?;
        assert_eq!(
            missing_before_restart.len(),
            versions.len(),
            "blank replacement must start without any baseline version shards"
        );
        cluster.start_node(TARGET_NODE).await?;

        wait_for_replacement_state(&cluster, &target_disk, &["completed"], 420).await?;
        wait_for_physical_census(&target_disk, &versions, 420).await?;
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
        run_replacement_e2e(4).await
    }

    /// Linux mount namespaces are per-thread; keep mount setup and process
    /// spawning on one OS thread so child RustFS nodes inherit the test mounts.
    #[tokio::test(flavor = "current_thread")]
    #[serial]
    #[ignore = "requires Linux root/CAP_SYS_ADMIN and RUSTFS_PRIVILEGED_REPLACEMENT_E2E=1"]
    async fn test_privileged_3x4_auto_replacement_rebuilds_ec6_plus_6_without_admin_heal()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        run_replacement_e2e(6).await
    }
}
