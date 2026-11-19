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

use super::{
    DiskOption, DiskStore, Endpoint, HealDiskExt as _, local_disk_map_read, new_disk, resume::ReplacementTargetIdentity,
};

/// Whether automatic replacement may fall back to the set-wide format heal when
/// a target cannot pass the independent-mount admission.
///
/// Directory-backed deployments already declare, through
/// `RUSTFS_UNSAFE_BYPASS_DISK_CHECK`, that their endpoints are plain
/// directories sharing a device with the host root. Those endpoints can never
/// satisfy [`auto_replacement_target_identity`], so without this fallback a
/// runtime-wiped or replaced directory disk would stay deferred forever. The
/// admission check itself is never bypassed; the fallback only routes the heal
/// through the ordinary format path that formats every unformatted disk in the
/// set, which is exactly what the pre-admission `heal_disk` path did.
pub(crate) fn directory_backed_replacement_fallback_enabled() -> bool {
    if cfg!(target_os = "linux") && std::env::var_os(rustfs_config::ENV_HEAL_REPLACEMENT_MOUNT_ROOTS).is_some() {
        return false;
    }
    rustfs_utils::get_env_bool_with_aliases(
        rustfs_config::ENV_UNSAFE_BYPASS_DISK_CHECK,
        &[rustfs_config::ENV_MINIO_CI],
        rustfs_config::DEFAULT_UNSAFE_BYPASS_DISK_CHECK,
    )
}

pub(crate) async fn auto_replacement_target_ready(disk: &DiskStore, local_disks: &[DiskStore]) -> bool {
    auto_replacement_target_identity(disk, local_disks).await.is_some()
}

pub(crate) async fn auto_replacement_target_identity(
    disk: &DiskStore,
    local_disks: &[DiskStore],
) -> Option<ReplacementTargetIdentity> {
    let disk = disk.clone();
    let endpoint = disk.endpoint().to_string();
    let siblings = local_disks
        .iter()
        .filter(|sibling| sibling.endpoint().is_local && sibling.endpoint().to_string() != endpoint)
        .cloned()
        .collect::<Vec<_>>();
    tokio::task::spawn_blocking(move || {
        let lease_root = disk.replacement_mount_lease_root()?;
        let canonical_path = fs::canonicalize(&lease_root).ok()?;
        let filesystem_identity = filesystem_identity(&lease_root, &canonical_path, Path::new(&disk.endpoint().get_file_path()))?;
        let Ok(target_device_ids) = rustfs_utils::os::get_physical_device_ids(lease_root.to_string_lossy().as_ref()) else {
            return None;
        };
        let Ok(root_device_ids) = rustfs_utils::os::get_physical_device_ids("/") else {
            return None;
        };
        if target_device_ids.is_empty()
            || root_device_ids.is_empty()
            || target_device_ids.iter().any(|target| root_device_ids.contains(target))
        {
            return None;
        }

        for sibling in siblings {
            let sibling_lease_root = sibling.replacement_mount_lease_root()?;
            rustfs_utils::os::get_physical_device_ids(sibling_lease_root.to_string_lossy().as_ref())
                .ok()
                .filter(|ids| !ids.iter().any(|id| target_device_ids.contains(id)))?;
        }

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

fn local_replacement_endpoint(target: &str, local_grid_hosts: &[String]) -> Option<Endpoint> {
    let mut endpoint = Endpoint::try_from(target).ok()?;
    if endpoint.is_local {
        return Some(endpoint);
    }

    let grid_host = endpoint.grid_host();
    if grid_host.is_empty() || !local_grid_hosts.iter().any(|local_host| local_host == &grid_host) {
        return None;
    }

    endpoint.is_local = true;
    Some(endpoint)
}

pub(super) async fn replacement_target_disk(target: &str, local_disks: &[DiskStore]) -> Option<DiskStore> {
    if let Some(disk) = local_disks.iter().find(|disk| disk.endpoint().to_string() == target) {
        return Some(disk.clone());
    }

    let local_grid_hosts = local_disks.iter().map(|disk| disk.endpoint().grid_host()).collect::<Vec<_>>();
    let endpoint = local_replacement_endpoint(target, &local_grid_hosts)?;

    new_disk(
        &endpoint,
        &DiskOption {
            cleanup: false,
            health_check: false,
        },
    )
    .await
    .ok()
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
        let disk = replacement_target_disk(target, &local_disks).await?;
        identities.push(auto_replacement_target_identity(&disk, &local_disks).await?);
    }
    identities.sort_by(|left, right| left.endpoint.cmp(&right.endpoint));
    identities.dedup_by(|left, right| left.endpoint == right.endpoint);
    (identities.len() == targets.len()).then_some(identities)
}

#[cfg(target_os = "linux")]
fn filesystem_identity(lease_root: &Path, canonical_path: &Path, configured_path: &Path) -> Option<String> {
    use std::os::{fd::AsRawFd as _, unix::fs::MetadataExt as _};

    // Use the configured boundary even when its mount is absent. Using the
    // currently visible enclosing mount could admit a directory exposed by unmount.
    let roots = std::env::var_os(rustfs_config::ENV_HEAL_REPLACEMENT_MOUNT_ROOTS);
    let expected_mount = roots
        .as_ref()
        .into_iter()
        .flat_map(std::env::split_paths)
        .filter(|root| root.is_absolute() && (configured_path.starts_with(root) || canonical_path.starts_with(root)))
        .max_by_key(|root| root.components().count());
    let expected_mount = expected_mount.as_deref().unwrap_or(canonical_path);
    if !canonical_path.starts_with(expected_mount) {
        return None;
    }
    let expected_mount = expected_mount
        .to_str()?
        .replace('\\', "\\134")
        .replace('\t', "\\011")
        .replace('\n', "\\012")
        .replace(' ', "\\040");

    let directory = fs::File::open(lease_root).ok()?;
    let metadata = directory.metadata().ok()?;
    let fdinfo = fs::read_to_string(format!("/proc/self/fdinfo/{}", directory.as_raw_fd())).ok()?;
    let mount_id = fdinfo.lines().find_map(|line| line.strip_prefix("mnt_id:").map(str::trim))?;
    let mountinfo = fs::read_to_string("/proc/self/mountinfo").ok()?;
    let mounted_at_expected_root = mountinfo.lines().any(|line| {
        let mut fields = line.split_ascii_whitespace();
        fields.next() == Some(mount_id) && fields.nth(3) == Some(expected_mount.as_str())
    });
    if !mounted_at_expected_root {
        return None;
    }
    Some(format!("{mount_id}:{}:{}", metadata.dev(), metadata.ino()))
}

#[cfg(all(unix, not(target_os = "linux")))]
fn filesystem_identity(lease_root: &Path, canonical_path: &Path, _configured_path: &Path) -> Option<String> {
    use std::os::unix::fs::MetadataExt as _;

    if !rustfs_utils::os::is_mount_point(canonical_path).ok()? {
        return None;
    }
    let metadata = fs::metadata(lease_root).ok()?;
    Some(format!("{}:{}", metadata.dev(), metadata.ino()))
}

#[cfg(not(unix))]
fn filesystem_identity(_lease_root: &Path, _canonical_path: &Path, _configured_path: &Path) -> Option<String> {
    None
}

#[cfg(test)]
mod tests {
    use super::super::{DiskOption, new_disk};
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn local_replacement_endpoint_accepts_a_url_on_a_registered_local_grid_host() {
        let local_grid_hosts = vec!["http://127.0.0.1:9000".to_owned()];
        let endpoint = local_replacement_endpoint("http://127.0.0.1:9000/replacement", &local_grid_hosts)
            .expect("matching local grid host should be accepted");

        assert!(endpoint.is_local);
    }

    #[test]
    fn local_replacement_endpoint_rejects_a_url_on_an_unregistered_grid_host() {
        let local_grid_hosts = vec!["http://127.0.0.1:9000".to_owned()];

        assert!(local_replacement_endpoint("http://127.0.0.1:9001/replacement", &local_grid_hosts).is_none());
    }

    #[test]
    fn local_replacement_endpoint_keeps_a_local_path_local() {
        let endpoint = local_replacement_endpoint("/replacement", &[]).expect("local path should be accepted");

        assert!(endpoint.is_local);
    }

    #[test]
    fn directory_backed_fallback_is_off_by_default() {
        temp_env::with_vars(
            [
                (rustfs_config::ENV_UNSAFE_BYPASS_DISK_CHECK, None::<&str>),
                (rustfs_config::ENV_MINIO_CI, None::<&str>),
            ],
            || assert!(!directory_backed_replacement_fallback_enabled()),
        );
    }

    #[test]
    fn directory_backed_fallback_follows_the_disk_check_bypass() {
        temp_env::with_vars(
            [
                (rustfs_config::ENV_UNSAFE_BYPASS_DISK_CHECK, Some("true")),
                (rustfs_config::ENV_MINIO_CI, None::<&str>),
            ],
            || assert!(directory_backed_replacement_fallback_enabled()),
        );
        temp_env::with_vars(
            [
                (rustfs_config::ENV_UNSAFE_BYPASS_DISK_CHECK, Some("false")),
                (rustfs_config::ENV_MINIO_CI, Some("true")),
            ],
            || {
                assert!(
                    !directory_backed_replacement_fallback_enabled(),
                    "the canonical key must win over the alias"
                )
            },
        );
    }

    #[tokio::test]
    async fn runtime_environment_cannot_bypass_mount_admission() {
        temp_env::async_with_vars(
            [
                ("RUSTFS_TEST_AUTO_REPLACEMENT_READINESS_BYPASS", Some("1")),
                ("RUSTFS_E2E_AUTO_REPLACEMENT_READINESS_BYPASS", Some("1")),
                (rustfs_config::ENV_UNSAFE_BYPASS_DISK_CHECK, Some("true")),
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
            Ok(enabled)
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
        fn auto_replacement_readiness_requires_the_configured_mount_for_a_data_subdirectory()
        -> Result<(), Box<dyn Error + Send + Sync>> {
            run_privileged_mount_test(|mut mounts| async move {
                let temp = TempDir::new()?;
                let parent = temp.path().join("parent");
                let mount = parent.join("replacement disk");
                let data = mount.join("data");
                fs::create_dir(&parent)?;
                mounts.mount_tmpfs(&parent, "rustfs-readiness-parent")?;
                fs::create_dir_all(&data)?;
                mounts.mount_tmpfs(&mount, "rustfs-readiness-child")?;
                fs::create_dir(&data)?;

                let endpoint = Endpoint::try_from(path_to_string(&data, "data directory")?.as_str())?;
                let options = DiskOption {
                    cleanup: false,
                    health_check: false,
                };
                let disk = new_disk(&endpoint, &options).await?;
                let key = rustfs_config::ENV_HEAL_REPLACEMENT_MOUNT_ROOTS;
                temp_env::async_with_vars([(key, None::<&str>)], async {
                    assert!(auto_replacement_target_identity(&disk, &[]).await.is_none());
                })
                .await;

                let roots = std::env::join_paths([&parent, &mount])?;
                temp_env::async_with_vars(
                    [
                        (key, Some(roots.to_str().ok_or("mount roots are not UTF-8")?)),
                        (rustfs_config::ENV_UNSAFE_BYPASS_DISK_CHECK, Some("true")),
                    ],
                    async {
                        assert!(!directory_backed_replacement_fallback_enabled());
                        let identity = auto_replacement_target_identity(&disk, &[])
                            .await
                            .expect("data directory on its configured independent mount should be admitted");
                        assert_eq!(Path::new(&identity.canonical_path), data.canonicalize()?);

                        run_command("umount", &["-l", &path_to_string(&mount, "replacement mount")?])?;
                        assert!(auto_replacement_target_identity(&disk, &[]).await.is_none());

                        let exposed_disk = new_disk(&endpoint, &options).await?;
                        assert!(
                            auto_replacement_target_identity(&exposed_disk, &[]).await.is_none(),
                            "a new handle on the exposed parent filesystem must also be rejected"
                        );
                        Ok(())
                    },
                )
                .await
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
