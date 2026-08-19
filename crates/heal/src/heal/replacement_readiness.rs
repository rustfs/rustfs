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

use std::{fs, path::Path};

#[cfg(test)]
use super::Endpoint;
use super::{DiskStore, HealDiskExt as _, local_disk_map_read, resume::ReplacementTargetIdentity};

pub(crate) async fn auto_replacement_target_ready(disk: &DiskStore, local_disks: &[DiskStore]) -> bool {
    auto_replacement_target_identity(disk, local_disks).await.is_some()
}

pub(crate) async fn auto_replacement_target_identity(
    disk: &DiskStore,
    local_disks: &[DiskStore],
) -> Option<ReplacementTargetIdentity> {
    let lease_root = disk.replacement_mount_lease_root()?;
    let endpoint = disk.endpoint().to_string();
    let sibling_lease_roots = local_disks
        .iter()
        .filter(|sibling| sibling.endpoint().is_local && sibling.endpoint().to_string() != endpoint)
        .map(|sibling| sibling.replacement_mount_lease_root())
        .collect::<Option<Vec<_>>>()?;
    tokio::task::spawn_blocking(move || {
        let canonical_path = fs::canonicalize(&lease_root).ok()?;
        let metadata = fs::metadata(&lease_root).ok()?;
        let Ok(target_device_ids) = rustfs_utils::os::get_physical_device_ids(lease_root.to_string_lossy().as_ref()) else {
            return None;
        };
        let Ok(root_device_ids) = rustfs_utils::os::get_physical_device_ids("/") else {
            return None;
        };
        if target_device_ids.is_empty()
            || root_device_ids.is_empty()
            || target_device_ids.iter().any(|target| root_device_ids.contains(target))
            || !rustfs_utils::os::is_mount_point(&canonical_path).unwrap_or(false)
        {
            return None;
        }

        if sibling_lease_roots.iter().any(|sibling_lease_root| {
            rustfs_utils::os::get_physical_device_ids(sibling_lease_root.to_string_lossy().as_ref())
                .map(|ids| ids.iter().any(|id| target_device_ids.contains(id)))
                .unwrap_or(true)
        }) {
            return None;
        }

        let filesystem_identity = filesystem_identity(&metadata, &canonical_path)?;

        Some(ReplacementTargetIdentity {
            endpoint,
            canonical_path: canonical_path.to_string_lossy().into_owned(),
            physical_device_ids: target_device_ids,
            filesystem_identity,
        })
    })
    .await
    .ok()
    .flatten()
}

pub(crate) async fn auto_replacement_targets_ready(targets: &[String]) -> bool {
    auto_replacement_target_identities(targets).await.is_some()
}

pub(crate) async fn auto_replacement_target_identities(targets: &[String]) -> Option<Vec<ReplacementTargetIdentity>> {
    let local_disk_map = local_disk_map_read().await;
    let local_disks = local_disk_map
        .values()
        .flatten()
        .filter(|disk| disk.endpoint().is_local)
        .cloned()
        .collect::<Vec<_>>();
    drop(local_disk_map);

    let mut identities = Vec::with_capacity(targets.len());
    for target in targets {
        let disk = local_disks.iter().find(|disk| disk.endpoint().to_string() == *target)?;
        identities.push(auto_replacement_target_identity(disk, &local_disks).await?);
    }
    identities.sort_by(|left, right| left.endpoint.cmp(&right.endpoint));
    identities.dedup_by(|left, right| left.endpoint == right.endpoint);
    (identities.len() == targets.len()).then_some(identities)
}

#[cfg(target_os = "linux")]
fn filesystem_identity(metadata: &fs::Metadata, canonical_path: &Path) -> Option<String> {
    use std::os::unix::fs::MetadataExt as _;

    let escaped_path = canonical_path.to_string_lossy().replace(' ', "\\040");
    let mountinfo = fs::read_to_string("/proc/self/mountinfo").ok()?;
    let mount_id = mountinfo.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        let mount_id = fields.next()?;
        fields.next()?;
        fields.next()?;
        fields.next()?;
        (fields.next()? == escaped_path).then_some(mount_id)
    })?;
    Some(format!("{mount_id}:{}:{}", metadata.dev(), metadata.ino()))
}

#[cfg(all(unix, not(target_os = "linux")))]
fn filesystem_identity(metadata: &fs::Metadata, _canonical_path: &Path) -> Option<String> {
    use std::os::unix::fs::MetadataExt as _;

    Some(format!("{}:{}", metadata.dev(), metadata.ino()))
}

#[cfg(not(unix))]
fn filesystem_identity(_metadata: &fs::Metadata, _canonical_path: &Path) -> Option<String> {
    None
}

#[cfg(test)]
mod tests {
    use super::super::{DiskOption, new_disk};
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn runtime_environment_cannot_bypass_mount_admission() {
        temp_env::async_with_vars(
            [
                ("RUSTFS_TEST_AUTO_REPLACEMENT_READINESS_BYPASS", Some("1")),
                ("RUSTFS_E2E_AUTO_REPLACEMENT_READINESS_BYPASS", Some("1")),
            ],
            async {
                let temp = TempDir::new().expect("temporary replacement root should be created");
                let endpoint =
                    Endpoint::try_from(temp.path().to_string_lossy().as_ref()).expect("replacement endpoint should parse");

                let disk = new_disk(
                    &endpoint,
                    &DiskOption {
                        cleanup: false,
                        health_check: false,
                    },
                )
                .await
                .expect("temporary disk should initialize");
                assert!(!auto_replacement_target_ready(&disk, std::slice::from_ref(&disk)).await);
            },
        )
        .await;
    }

    #[cfg(target_os = "linux")]
    mod linux_privileged_tests {
        use super::*;
        use std::error::Error;
        use std::path::Path;
        use std::process::Command;

        const ENABLE_ENV: &str = "RUSTFS_PRIVILEGED_MOUNT_READINESS_TESTS";
        const NAMESPACE_ENV: &str = "RUSTFS_PRIVILEGED_MOUNT_READINESS_TESTS_IN_NAMESPACE";
        const MOUNT_SIZE: &str = "size=32m,mode=0700";

        struct MountGuard {
            mounts: Vec<std::path::PathBuf>,
        }

        impl MountGuard {
            fn new() -> Result<Self, Box<dyn Error + Send + Sync>> {
                run_command("mount", &["--make-rprivate", "/"])?;
                Ok(Self { mounts: Vec::new() })
            }

            fn mount_tmpfs(&mut self, target: &Path, label: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
                mount_tmpfs(target, label)?;
                self.mounts.push(target.to_path_buf());
                Ok(())
            }

            fn mount_bind(&mut self, source: &Path, target: &Path) -> Result<(), Box<dyn Error + Send + Sync>> {
                mount_bind(source, target)?;
                self.mounts.push(target.to_path_buf());
                Ok(())
            }
        }

        impl Drop for MountGuard {
            fn drop(&mut self) {
                for mount in self.mounts.iter().rev() {
                    let _ = detach_mount(mount);
                }
            }
        }

        fn run_command(program: &str, args: &[&str]) -> Result<(), Box<dyn Error + Send + Sync>> {
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

        fn path_to_string(path: &Path, label: &str) -> Result<String, Box<dyn Error + Send + Sync>> {
            path.to_str()
                .map(str::to_owned)
                .ok_or_else(|| format!("{label} path is not UTF-8: {path:?}").into())
        }

        fn mount_tmpfs(target: &Path, label: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
            let target = path_to_string(target, "tmpfs target")?;
            run_command("mount", &["-t", "tmpfs", "-o", MOUNT_SIZE, label, &target])
        }

        fn mount_bind(source: &Path, target: &Path) -> Result<(), Box<dyn Error + Send + Sync>> {
            let source = path_to_string(source, "bind source")?;
            let target = path_to_string(target, "bind target")?;
            run_command("mount", &["--bind", &source, &target])
        }

        fn detach_mount(target: &Path) -> Result<(), Box<dyn Error + Send + Sync>> {
            let target = path_to_string(target, "umount target")?;
            run_command("umount", &[&target])
        }

        fn privileged_enabled() -> Result<bool, Box<dyn Error + Send + Sync>> {
            let enabled = std::env::var(ENABLE_ENV)
                .ok()
                .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"));
            if !enabled {
                return Ok(false);
            }
            Ok(true)
        }

        fn run_current_test_in_mount_namespace() -> Result<(), Box<dyn Error + Send + Sync>> {
            let test_name = std::thread::current()
                .name()
                .ok_or("privileged mount readiness test thread is unnamed")?
                .to_owned();
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
            Err(format!("{ENABLE_ENV}=1 requires Linux root or CAP_SYS_ADMIN; unshare exited with status {status}").into())
        }

        fn run_privileged_mount_test<F, Fut>(test: F) -> Result<(), Box<dyn Error + Send + Sync>>
        where
            F: FnOnce(MountGuard) -> Fut + Send + 'static,
            Fut: std::future::Future<Output = Result<(), Box<dyn Error + Send + Sync>>> + 'static,
        {
            if !privileged_enabled()? {
                return Ok(());
            }
            if std::env::var_os(NAMESPACE_ENV).is_none() {
                return run_current_test_in_mount_namespace();
            }

            let guard = MountGuard::new()?;
            let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
            runtime.block_on(test(guard))
        }

        #[test]
        #[ignore = "requires Linux root/CAP_SYS_ADMIN and RUSTFS_PRIVILEGED_MOUNT_READINESS_TESTS=1"]
        fn auto_replacement_readiness_accepts_an_independent_mount() -> Result<(), Box<dyn Error + Send + Sync>> {
            run_privileged_mount_test(|mut mounts| async move {
                let temp = TempDir::new().expect("temporary replacement roots should be created");
                let target = temp.path().join("target");
                let sibling = temp.path().join("sibling");
                std::fs::create_dir(&target).expect("target mountpoint should be created");
                std::fs::create_dir(&sibling).expect("sibling mountpoint should be created");
                mounts.mount_tmpfs(&target, "rustfs-readiness-target")?;
                mounts.mount_tmpfs(&sibling, "rustfs-readiness-sibling")?;

                let target_endpoint = Endpoint::try_from(target.to_string_lossy().as_ref())?;
                let sibling_endpoint = Endpoint::try_from(sibling.to_string_lossy().as_ref())?;
                let target_disk = new_disk(
                    &target_endpoint,
                    &DiskOption {
                        cleanup: false,
                        health_check: false,
                    },
                )
                .await?;
                let sibling_disk = new_disk(
                    &sibling_endpoint,
                    &DiskOption {
                        cleanup: false,
                        health_check: false,
                    },
                )
                .await?;

                let identity = auto_replacement_target_identity(&target_disk, &[target_disk.clone(), sibling_disk.clone()]).await;
                assert!(
                    identity.is_some(),
                    "a separately mounted replacement target with no sibling device overlap must be admitted"
                );
                Ok(())
            })
        }

        #[test]
        #[ignore = "requires Linux root/CAP_SYS_ADMIN and RUSTFS_PRIVILEGED_MOUNT_READINESS_TESTS=1"]
        fn auto_replacement_readiness_rejects_a_same_device_sibling_bind_mount() -> Result<(), Box<dyn Error + Send + Sync>> {
            run_privileged_mount_test(|mut mounts| async move {
                let temp = TempDir::new().expect("temporary replacement roots should be created");
                let source = temp.path().join("source");
                let target = temp.path().join("target");
                let sibling = temp.path().join("sibling");
                std::fs::create_dir(&source).expect("source mountpoint should be created");
                std::fs::create_dir(&target).expect("target mountpoint should be created");
                std::fs::create_dir(&sibling).expect("sibling mountpoint should be created");
                mounts.mount_tmpfs(&source, "rustfs-readiness-shared-source")?;
                mounts.mount_bind(&source, &target)?;
                mounts.mount_bind(&source, &sibling)?;

                let target_endpoint = Endpoint::try_from(target.to_string_lossy().as_ref())?;
                let sibling_endpoint = Endpoint::try_from(sibling.to_string_lossy().as_ref())?;
                let target_disk = new_disk(
                    &target_endpoint,
                    &DiskOption {
                        cleanup: false,
                        health_check: false,
                    },
                )
                .await?;
                let sibling_disk = new_disk(
                    &sibling_endpoint,
                    &DiskOption {
                        cleanup: false,
                        health_check: false,
                    },
                )
                .await?;

                assert!(
                    auto_replacement_target_identity(&target_disk, &[target_disk.clone(), sibling_disk.clone()])
                        .await
                        .is_none(),
                    "replacement readiness must reject a target sharing its physical device with a sibling endpoint"
                );
                Ok(())
            })
        }
    }
}
