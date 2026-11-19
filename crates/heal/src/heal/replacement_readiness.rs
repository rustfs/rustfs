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

use std::collections::HashSet;

use super::{
    DiskOption, DiskStore, Endpoint, HealDiskExt as _, local_disk_map_read, new_disk, resume::ReplacementTargetIdentity,
};

/// Whether automatic replacement may fall back to the set-wide format heal when
/// a target cannot pass the independent-mount admission.
///
/// Directory-backed deployments already declare, through
/// `RUSTFS_UNSAFE_BYPASS_DISK_CHECK`, that their endpoints are plain
/// directories sharing a device with the host root. Those endpoints can never
/// pass the independent-mount check, so without this fallback a
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
    probe_replacement_mounts(std::slice::from_ref(disk), local_disks)
        .await
        .is_some()
}

pub(super) async fn probe_replacement_mounts(
    disks: &[DiskStore],
    local_disks: &[DiskStore],
) -> Option<Vec<ReplacementTargetIdentity>> {
    if disks.is_empty() {
        return Some(Vec::new());
    }
    let disks = disks.to_vec();
    let local_disks = local_disks.to_vec();
    tokio::task::spawn_blocking(move || {
        let mount_roots = std::env::var_os(rustfs_config::ENV_HEAL_REPLACEMENT_MOUNT_ROOTS)
            .as_ref()
            .into_iter()
            .flat_map(std::env::split_paths)
            .collect::<Vec<_>>();
        let mut identities = Vec::with_capacity(disks.len());
        for disk in &disks {
            let mount = disk.replacement_mount(&mount_roots)?;
            if mount.physical_device_ids.is_empty() {
                return None;
            }
            identities.push(ReplacementTargetIdentity {
                endpoint: disk.endpoint().to_string(),
                canonical_path: mount.canonical_path.into_os_string().into_string().ok()?,
                physical_device_ids: mount.physical_device_ids,
                filesystem_identity: mount.filesystem_identity,
            });
        }

        let mut target_devices = HashSet::new();
        for target in &identities {
            if target
                .physical_device_ids
                .iter()
                .any(|device| !target_devices.insert(device.as_str()))
            {
                return None;
            }
        }
        let root_devices = rustfs_utils::os::get_physical_device_ids("/").ok()?;
        if root_devices.is_empty() || root_devices.iter().any(|device| target_devices.contains(device.as_str())) {
            return None;
        }
        let targets: HashSet<_> = identities.iter().map(|identity| identity.endpoint.as_str()).collect();
        for sibling in &local_disks {
            let endpoint = sibling.endpoint();
            if endpoint.is_local && !targets.contains(endpoint.to_string().as_str()) {
                let root = sibling.replacement_mount_lease_root()?;
                let devices = rustfs_utils::os::get_physical_device_ids(root.to_str()?).ok()?;
                if devices.iter().any(|device| target_devices.contains(device.as_str())) {
                    return None;
                }
            }
        }
        Some(identities)
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

pub(super) async fn local_replacement_disks() -> Vec<DiskStore> {
    local_disk_map_read()
        .await
        .values()
        .flatten()
        .filter(|disk| disk.endpoint().is_local)
        .cloned()
        .collect()
}

pub(super) struct ReplacementTargets {
    pub(super) disks: Vec<DiskStore>,
    local_disks: Vec<DiskStore>,
}

impl ReplacementTargets {
    pub(super) async fn open(targets: &[String]) -> Option<Self> {
        let mut ordered: Vec<_> = targets.iter().map(String::as_str).collect();
        ordered.sort_unstable();
        if ordered.windows(2).any(|pair| pair[0] == pair[1]) {
            return None;
        }

        let registry = local_disk_map_read().await;
        let requested: Vec<_> = ordered
            .into_iter()
            .map(|target| {
                let disk = registry
                    .get(target)
                    .and_then(Option::as_ref)
                    .filter(|disk| disk.endpoint().is_local)
                    .cloned();
                (target, disk)
            })
            .collect();
        let local_disks = registry
            .values()
            .flatten()
            .filter(|disk| disk.endpoint().is_local)
            .cloned()
            .collect::<Vec<_>>();
        drop(registry);
        let mut disks = Vec::with_capacity(requested.len());
        let mut hosts = None;
        for (target, disk) in requested {
            let disk = match disk {
                Some(disk) => disk,
                None => {
                    let local_hosts = hosts
                        .get_or_insert_with(|| local_disks.iter().map(|disk| disk.endpoint().grid_host()).collect::<Vec<_>>());
                    let endpoint = local_replacement_endpoint(target, local_hosts)?;
                    new_disk(
                        &endpoint,
                        &DiskOption {
                            cleanup: false,
                            health_check: false,
                        },
                    )
                    .await
                    .ok()?
                }
            };
            disks.push(disk);
        }
        Some(Self { disks, local_disks })
    }

    pub(super) async fn identities(&self) -> Option<Vec<ReplacementTargetIdentity>> {
        probe_replacement_mounts(&self.disks, &self.local_disks).await
    }
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
        use std::process::Command;
        use std::{fs, path::Path};

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
                let temp = TempDir::new()?;
                let parent = temp.path().join("parent");
                let target = parent.join("replacement disk");
                let data = target.join("data");
                let sibling = temp.path().join("sibling");
                fs::create_dir(&parent)?;
                mounts.mount_tmpfs(&parent, "rustfs-readiness-parent")?;
                // Leave a directory at the same path below the target mount.
                fs::create_dir_all(&data)?;
                mounts.mount_tmpfs(&target, "rustfs-readiness-target")?;
                fs::create_dir(&data)?;
                fs::create_dir(&sibling)?;
                mounts.mount_tmpfs(&sibling, "rustfs-readiness-sibling")?;

                let target_endpoint = Endpoint::try_from(target.to_string_lossy().as_ref())?;
                let sibling_endpoint = Endpoint::try_from(sibling.to_string_lossy().as_ref())?;
                let options = DiskOption {
                    cleanup: false,
                    health_check: false,
                };
                let target_disk = new_disk(&target_endpoint, &options).await?;
                let sibling_disk = new_disk(&sibling_endpoint, &options).await?;

                assert!(
                    auto_replacement_target_ready(&target_disk, &[target_disk.clone(), sibling_disk.clone()]).await,
                    "a separately mounted replacement target with no sibling device overlap must be admitted"
                );
                let disks = [target_disk, sibling_disk];
                let mut identities = probe_replacement_mounts(&disks, &disks)
                    .await
                    .expect("independently mounted replacement targets should be admitted together");
                assert_eq!(identities[0].endpoint, disks[0].endpoint().to_string());
                assert_eq!(identities[1].endpoint, disks[1].endpoint().to_string());
                identities.sort_unstable_by(|left, right| left.endpoint.cmp(&right.endpoint));
                let endpoints = [identities[1].endpoint.clone(), identities[0].endpoint.clone()];
                let execution = crate::heal::replacement_execution::ReplacementExecution::acquire(&endpoints).await?;
                assert_eq!(execution.identities(), identities);
                assert!(
                    crate::heal::replacement_execution::ReplacementExecution::acquire(&endpoints)
                        .await
                        .is_err(),
                    "a second executor must not take leases on the same targets"
                );
                drop(execution);

                let endpoint = Endpoint::try_from(path_to_string(&data, "data directory")?.as_str())?;
                let disk = new_disk(&endpoint, &options).await?;
                let key = rustfs_config::ENV_HEAL_REPLACEMENT_MOUNT_ROOTS;
                temp_env::async_with_vars([(key, None::<&str>)], async {
                    assert!(!auto_replacement_target_ready(&disk, &[]).await);
                })
                .await;

                let roots = std::env::join_paths([&parent, &target])?;
                temp_env::async_with_vars(
                    [
                        (key, Some(roots.to_str().ok_or("mount roots are not UTF-8")?)),
                        (rustfs_config::ENV_UNSAFE_BYPASS_DISK_CHECK, Some("true")),
                    ],
                    async {
                        assert!(!directory_backed_replacement_fallback_enabled());
                        let identities = probe_replacement_mounts(std::slice::from_ref(&disk), &[])
                            .await
                            .expect("data directory on its configured independent mount should be admitted");
                        assert_eq!(Path::new(&identities[0].canonical_path), data.canonicalize()?);
                        let reopened = new_disk(&endpoint, &options).await?;
                        assert_eq!(probe_replacement_mounts(&[reopened], &[]).await, Some(identities));

                        run_command("umount", &["-l", &path_to_string(&target, "replacement mount")?])?;
                        assert!(!auto_replacement_target_ready(&disk, &[]).await);

                        let exposed_disk = new_disk(&endpoint, &options).await?;
                        assert!(
                            !auto_replacement_target_ready(&exposed_disk, &[]).await,
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
                    !auto_replacement_target_ready(&target_disk, &[target_disk.clone(), sibling_disk.clone()]).await,
                    "replacement readiness must reject a target sharing its physical device with a sibling endpoint"
                );
                assert!(
                    probe_replacement_mounts(&[target_disk, sibling_disk], &[]).await.is_none(),
                    "a batch must reject overlapping targets even before they are registered"
                );
                Ok(())
            })
        }
    }
}
