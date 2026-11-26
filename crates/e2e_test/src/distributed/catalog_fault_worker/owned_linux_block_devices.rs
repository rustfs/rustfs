// Copyright 2026 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Task-owned media for the three native Catalog recovery cases.

use super::{Evidence, TestResult};
use std::path::PathBuf;
use std::sync::Arc;

/// Re-execute the exact case before creating its runtime's storage resources.
/// A successful child must report one executed test, including all assertions.
pub(super) async fn namespace_child(test_name: &str) -> TestResult<bool> {
    #[cfg(not(target_os = "linux"))]
    {
        let _ = test_name;
        Err("D07 device recovery requires Linux, unshare, loop devices and mount privileges".into())
    }
    #[cfg(target_os = "linux")]
    {
        if linux::isolated_namespace()? {
            linux::verify_private_namespace()?;
            return Ok(true);
        }
        let test_name = test_name.split_once("::").ok_or("missing crate in test name")?.1;
        let output = tokio::process::Command::new("unshare")
            .args(["--mount", "--propagation", "private", "--"])
            .arg(std::env::current_exe()?)
            .args(["--exact", test_name, "--nocapture", "--test-threads=1", "--color=never"])
            .kill_on_drop(true)
            .output()
            .await?;
        use std::io::Write;
        std::io::stdout().write_all(&output.stdout)?;
        std::io::stderr().write_all(&output.stderr)?;
        super::check(output.status.success(), format!("D07 namespace child failed: {}", output.status))?;
        let stdout = std::str::from_utf8(&output.stdout)?;
        super::check(
            stdout
                .lines()
                .any(|line| line.starts_with("test result: ok. 1 passed; 0 failed; 0 ignored;")),
            "D07 namespace child did not execute exactly one successful case",
        )?;
        Ok(false)
    }
}

pub(super) struct OwnedLinuxBlockDevices {
    #[cfg(target_os = "linux")]
    state: Arc<std::sync::Mutex<linux::State>>,
}

impl OwnedLinuxBlockDevices {
    pub(super) async fn new(root: PathBuf, endpoints: Vec<PathBuf>, evidence: Arc<Evidence>) -> TestResult<Self> {
        #[cfg(target_os = "linux")]
        {
            let state = tokio::task::spawn_blocking(move || linux::State::new(root, endpoints, evidence)).await??;
            Ok(Self {
                state: Arc::new(std::sync::Mutex::new(state)),
            })
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (root, endpoints, evidence);
            Err("D07 owned block storage requires Linux".into())
        }
    }

    // The only owner lock is taken on blocking threads, never across an await.
    #[cfg(target_os = "linux")]
    async fn run<T: Send + 'static>(
        &self,
        action: impl FnOnce(&mut linux::State) -> TestResult<T> + Send + 'static,
    ) -> TestResult<T> {
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || {
            let mut owner = state.lock().map_err(|_| "D07 device owner poisoned")?;
            action(&mut owner)
        })
        .await?
    }

    pub(super) async fn provision(&self) -> TestResult {
        #[cfg(target_os = "linux")]
        {
            self.run(linux::State::provision).await
        }
        #[cfg(not(target_os = "linux"))]
        {
            Err("D07 owned block storage requires Linux".into())
        }
    }

    pub(super) async fn retain_snapshot(&self, path: PathBuf) -> TestResult {
        #[cfg(target_os = "linux")]
        {
            self.run(move |state| state.retain_snapshot(path)).await
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = path;
            Err("D07 owned block storage requires Linux".into())
        }
    }

    pub(super) async fn replace(&self, node: usize, snapshot: Option<PathBuf>) -> TestResult {
        #[cfg(target_os = "linux")]
        {
            self.run(move |state| state.replace(node, snapshot)).await
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (node, snapshot);
            Err("D07 owned block storage requires Linux".into())
        }
    }

    /// Call only after every owned worker has been confirmed exited and reaped.
    pub(super) async fn cleanup(&self) -> TestResult {
        #[cfg(target_os = "linux")]
        {
            self.run(linux::State::cleanup).await
        }
        #[cfg(not(target_os = "linux"))]
        {
            Err("D07 owned block storage requires Linux".into())
        }
    }

    /// The unfinished-case Drop path also reports this result to its evidence.
    pub(super) fn cleanup_sync(&self) -> TestResult {
        #[cfg(target_os = "linux")]
        {
            self.state.lock().map_err(|_| "D07 device owner poisoned")?.cleanup()
        }
        #[cfg(not(target_os = "linux"))]
        {
            Err("D07 owned block storage requires Linux".into())
        }
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use super::super::{check, copy_owned_contents, inventory};
    use super::*;
    use serde::{Deserialize, Serialize};
    use serde_json::{Value, json};
    use std::collections::BTreeSet;
    use std::ffi::OsString;
    use std::fs::{self, File, Metadata, OpenOptions};
    use std::io;
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
    use std::path::Path;
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};
    use uuid::Uuid;

    const IMAGE_BYTES: u64 = 256 * 1024 * 1024;
    const COMMAND_TIMEOUT: Duration = Duration::from_secs(30);
    const MAX_COMMAND_BYTES: u64 = 1024 * 1024;
    const LOOP_COLUMNS: &str = "NAME,BACK-FILE,BACK-INO,BACK-MAJ:MIN,MAJ:MIN,OFFSET,SIZELIMIT,RO,AUTOCLEAR";

    #[derive(Clone, Debug, PartialEq, Eq, Serialize)]
    struct Identity {
        device: u64,
        inode: u64,
        rdev: u64,
        mode: u32,
    }
    impl Identity {
        fn of(metadata: &Metadata) -> Self {
            Self {
                device: metadata.dev(),
                inode: metadata.ino(),
                rdev: metadata.rdev(),
                mode: metadata.mode(),
            }
        }
        fn path(path: &Path) -> TestResult<Self> {
            let metadata = fs::symlink_metadata(path)?;
            check(!metadata.file_type().is_symlink(), "owned path became a symlink")?;
            Ok(Self::of(&metadata))
        }
        fn verify(&self, path: &Path, held: &File) -> TestResult {
            check(
                Self::path(path)? == *self && Self::of(&held.metadata()?) == *self,
                format!("owned identity changed: {}", path.display()),
            )
        }
    }

    fn device_number(device: u64) -> String {
        // Linux dev_t layout, including the extended major/minor bits.
        let major = ((device >> 8) & 0xfff) | ((device >> 32) & 0xfffff000);
        let minor = (device & 0xff) | ((device >> 12) & 0xffffff00);
        format!("{major}:{minor}")
    }

    fn fd_path(file: &File) -> PathBuf {
        PathBuf::from(format!("/proc/{}/fd/{}", std::process::id(), file.as_raw_fd()))
    }

    #[derive(Clone, Debug, Deserialize, Serialize)]
    struct Mapping {
        name: PathBuf,
        #[serde(rename = "back-file")]
        backing: PathBuf,
        #[serde(rename = "back-ino")]
        backing_inode: u64,
        #[serde(rename = "back-maj:min")]
        backing_device: String,
        #[serde(rename = "maj:min")]
        device: String,
        offset: u64,
        sizelimit: u64,
        ro: bool,
        autoclear: bool,
    }
    #[derive(Deserialize)]
    struct Mappings {
        loopdevices: Vec<Mapping>,
    }

    fn validate_loop_name(path: &Path) -> TestResult {
        let name = path.file_name().and_then(|v| v.to_str()).ok_or("invalid loop name")?;
        let number = name.strip_prefix("loop").ok_or("not a loop device")?;
        check(
            path.parent() == Some(Path::new("/dev")) && !number.is_empty() && number.bytes().all(|v| v.is_ascii_digit()),
            "unowned device path",
        )
    }

    fn verify_mapping(mapping: &Mapping, backing: &Identity, block: Option<&Identity>) -> TestResult {
        validate_loop_name(&mapping.name)?;
        check(
            mapping.backing_inode == backing.inode && mapping.backing_device == device_number(backing.device),
            "loop backing identity mismatch",
        )?;
        check(
            mapping.offset == 0 && mapping.sizelimit == 0 && !mapping.ro && !mapping.autoclear,
            "loop parameters changed",
        )?;
        if let Some(block) = block {
            check(mapping.device == device_number(block.rdev), "loop device identity mismatch")?;
        }
        Ok(())
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize)]
    struct Mount {
        id: u64,
        device: String,
        root: String,
        target: PathBuf,
        filesystem: String,
    }

    fn unescape_mount(value: &str) -> TestResult<String> {
        let mut bytes = Vec::new();
        let mut input = value.bytes();
        while let Some(byte) = input.next() {
            if byte == b'\\' {
                let a = input.next().ok_or("short mount escape")?;
                let b = input.next().ok_or("short mount escape")?;
                let c = input.next().ok_or("short mount escape")?;
                check([a, b, c].iter().all(|v| matches!(v, b'0'..=b'7')), "invalid mount escape")?;
                let value = u16::from(a - b'0') * 64 + u16::from(b - b'0') * 8 + u16::from(c - b'0');
                bytes.push(u8::try_from(value)?);
            } else {
                bytes.push(byte);
            }
        }
        Ok(String::from_utf8(bytes)?)
    }

    fn mounts(text: &str) -> TestResult<Vec<Mount>> {
        text.lines()
            .map(|line| {
                let (left, right) = line.split_once(" - ").ok_or("invalid mountinfo separator")?;
                let fields: Vec<_> = left.split_whitespace().collect();
                check(fields.len() >= 6, "short mountinfo record")?;
                Ok(Mount {
                    id: fields[0].parse()?,
                    device: fields[2].to_owned(),
                    root: unescape_mount(fields[3])?,
                    target: PathBuf::from(unescape_mount(fields[4])?),
                    filesystem: right.split_whitespace().next().ok_or("missing filesystem")?.to_owned(),
                })
            })
            .collect()
    }

    fn verify_mount_record(expected: &Option<Mount>, observed: &Option<Mount>) -> TestResult {
        check(expected.is_some() && observed == expected, "owned mount identity changed")
    }

    fn validate_endpoint(root: &Path, path: &Path) -> TestResult {
        check(path.parent() == Some(root) && path.canonicalize()? == path, "endpoint is outside owner")?;
        check(!fs::symlink_metadata(path)?.file_type().is_symlink(), "endpoint is a symlink")
    }

    fn mount_at(path: &Path) -> TestResult<Option<Mount>> {
        let found: Vec<_> = mounts(&fs::read_to_string("/proc/self/mountinfo")?)?
            .into_iter()
            .filter(|mount| mount.target == path)
            .collect();
        check(found.len() <= 1, "multiple mounts cover owned endpoint")?;
        Ok(found.into_iter().next())
    }

    fn namespace() -> io::Result<PathBuf> {
        fs::read_link("/proc/self/ns/mnt")
    }
    pub(super) fn isolated_namespace() -> TestResult<bool> {
        let status = fs::read_to_string("/proc/self/status")?;
        let parent: u32 = status
            .lines()
            .find_map(|line| line.strip_prefix("PPid:"))
            .ok_or("missing parent PID")?
            .trim()
            .parse()?;
        Ok(namespace()? != fs::read_link(format!("/proc/{parent}/ns/mnt"))?)
    }
    pub(super) fn verify_private_namespace() -> TestResult {
        check(isolated_namespace()?, "D07 mount namespace still matches parent")?;
        for line in fs::read_to_string("/proc/self/mountinfo")?.lines() {
            let left = line.split_once(" - ").ok_or("invalid mountinfo")?.0;
            check(
                !left
                    .split_whitespace()
                    .skip(6)
                    .any(|field| field.starts_with("shared:") || field.starts_with("master:")),
                "D07 requires private mount propagation",
            )?;
        }
        Ok(())
    }

    struct Medium {
        path: PathBuf,
        file: File,
        identity: Identity,
        mapping: Option<Mapping>,
        block: Option<File>,
        block_identity: Option<Identity>,
        detached: bool,
    }
    struct Endpoint {
        path: PathBuf,
        directory: File,
        identity: Identity,
        mount: Option<Mount>,
        mounted_root: Option<File>,
        mounted_identity: Option<Identity>,
        medium: Option<usize>,
    }
    struct Snapshot {
        path: PathBuf,
        held: File,
        identity: Identity,
        inventory: Value,
    }
    pub(super) struct State {
        root: PathBuf,
        root_file: File,
        root_identity: Identity,
        media_root: PathBuf,
        media_file: File,
        media_identity: Identity,
        namespace: PathBuf,
        evidence: Arc<Evidence>,
        media: Vec<Medium>,
        endpoints: Vec<Endpoint>,
        replaced: bool,
        cleaned: bool,
        closing: bool,
        snapshot: Option<Snapshot>,
    }

    impl State {
        pub(super) fn new(root: PathBuf, endpoints: Vec<PathBuf>, evidence: Arc<Evidence>) -> TestResult<Self> {
            verify_private_namespace()?;
            check(root.is_absolute() && root.canonicalize()? == root, "cluster root is not canonical")?;
            let root_identity = Identity::path(&root)?;
            let root_file = File::open(&root)?;
            check(
                root_file.metadata()?.is_dir() && root_identity.mode & 0o777 == 0o700,
                "cluster owner permissions mismatch",
            )?;
            check(
                endpoints.len() == 4 && endpoints.iter().collect::<BTreeSet<_>>().len() == 4,
                "D07 requires four distinct endpoints",
            )?;
            let endpoints = endpoints
                .into_iter()
                .map(|path| {
                    validate_endpoint(&root, &path)?;
                    let identity = Identity::path(&path)?;
                    let directory = File::open(&path)?;
                    check(
                        directory.metadata()?.is_dir() && fs::read_dir(&path)?.next().is_none() && mount_at(&path)?.is_none(),
                        "endpoint is not an empty owned directory",
                    )?;
                    Ok(Endpoint {
                        path,
                        directory,
                        identity,
                        mount: None,
                        mounted_root: None,
                        mounted_identity: None,
                        medium: None,
                    })
                })
                .collect::<TestResult<Vec<_>>>()?;
            let media_root = root.join(format!("block-devices-{}", Uuid::new_v4()));
            fs::create_dir(&media_root)?;
            fs::set_permissions(&media_root, fs::Permissions::from_mode(0o700))?;
            let media_file = File::open(&media_root)?;
            let media_identity = Identity::path(&media_root)?;
            let mut media = Vec::new();
            for index in 0..5 {
                let path = media_root.join(format!("medium-{index}.img"));
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .open(&path)?;
                file.set_len(IMAGE_BYTES)?;
                file.sync_all()?;
                let identity = Identity::of(&file.metadata()?);
                media.push(Medium {
                    path,
                    file,
                    identity,
                    mapping: None,
                    block: None,
                    block_identity: None,
                    detached: false,
                });
            }
            let state = Self {
                root,
                root_file,
                root_identity,
                media_root,
                media_file,
                media_identity,
                namespace: namespace()?,
                evidence,
                media,
                endpoints,
                replaced: false,
                cleaned: false,
                closing: false,
                snapshot: None,
            };
            state.record("owned-media-created")?;
            Ok(state)
        }

        fn verify_owner(&self) -> TestResult {
            check(namespace()? == self.namespace, "owner mount namespace changed")?;
            verify_private_namespace()?;
            self.root_identity.verify(&self.root, &self.root_file)?;
            self.media_identity.verify(&self.media_root, &self.media_file)?;
            for medium in &self.media {
                medium.identity.verify(&medium.path, &medium.file)?;
                check(
                    medium.file.metadata()?.is_file() && medium.file.metadata()?.len() == IMAGE_BYTES,
                    "backing type or length changed",
                )?;
            }
            Ok(())
        }

        fn record(&self, label: &str) -> TestResult {
            self.evidence.record(label, &json!({"namespace":self.namespace,"cluster_root":self.root,"owner":self.media_root,"owner_identity":self.media_identity,"physical_device_independence":false,"media":self.media.iter().map(|m| json!({"path":m.path,"identity":m.identity,"mapping":m.mapping,"block_identity":m.block_identity,"detached":m.detached})).collect::<Vec<_>>(),"endpoints":self.endpoints.iter().map(|e| json!({"path":e.path,"directory_identity":e.identity,"mount":e.mount,"mounted_identity":e.mounted_identity,"medium":e.medium})).collect::<Vec<_>>(),"cleaned":self.cleaned,"closing":self.closing}))
        }

        fn command(&self, program: &str, args: &[OsString]) -> TestResult<Vec<u8>> {
            self.verify_owner()?;
            let id = Uuid::new_v4();
            let stdout_path = self.media_root.join(format!("command-{id}.stdout"));
            let stderr_path = self.media_root.join(format!("command-{id}.stderr"));
            let output = |path: &Path| OpenOptions::new().write(true).create_new(true).mode(0o600).open(path);
            let stdout = output(&stdout_path)?;
            let stderr = output(&stderr_path)?;
            self.evidence.record(
                "device-command-start",
                &json!({"id":id,"program":program,"arguments":args,"stdout":stdout_path,"stderr":stderr_path}),
            )?;
            let mut child = Command::new(program)
                .args(args)
                .env("LC_ALL", "C")
                .stdin(Stdio::null())
                .stdout(stdout)
                .stderr(stderr)
                .spawn()?;
            if let Err(error) = self
                .evidence
                .record("device-command-spawned", &json!({"id":id,"pid":child.id()}))
            {
                let killed = child.kill().map_err(Into::into);
                let waited = child.wait().map(|_| ()).map_err(Into::into);
                return super::super::combine_results(super::super::combine_results(Err(error), killed), waited)
                    .map(|()| Vec::new());
            }
            let deadline = Instant::now() + COMMAND_TIMEOUT;
            let status = loop {
                match child.try_wait() {
                    Ok(Some(status)) => break status,
                    Ok(None) => {}
                    Err(error) => {
                        let killed: TestResult = child.kill().map_err(Into::into);
                        let waited: TestResult = child.wait().map(|_| ()).map_err(Into::into);
                        return super::super::combine_results(super::super::combine_results(Err(error.into()), killed), waited)
                            .map(|()| Vec::new());
                    }
                }
                if Instant::now() >= deadline {
                    let killed = child.kill();
                    let waited = child.wait();
                    self.evidence.record(
                        "device-command-timeout",
                        &json!({"id":id,"kill":format!("{killed:?}"),"wait":format!("{waited:?}")}),
                    )?;
                    return Err(format!(
                        "{program} timed out; raw output: {} / {}",
                        stdout_path.display(),
                        stderr_path.display()
                    )
                    .into());
                }
                std::thread::sleep(Duration::from_millis(20));
            };
            self.evidence
                .record("device-command-complete", &json!({"id":id,"status":status.to_string()}))?;
            check(
                status.success(),
                format!(
                    "{program} failed with {status}; raw output: {} / {}",
                    stdout_path.display(),
                    stderr_path.display()
                ),
            )?;
            check(
                fs::metadata(&stdout_path)?.len() <= MAX_COMMAND_BYTES && fs::metadata(&stderr_path)?.len() <= MAX_COMMAND_BYTES,
                "device command output exceeds limit; original output retained",
            )?;
            Ok(fs::read(stdout_path)?)
        }

        fn associations(&self, index: usize) -> TestResult<Vec<Mapping>> {
            let output = self.command(
                "losetup",
                &[
                    "--list".into(),
                    "--json".into(),
                    "--output".into(),
                    LOOP_COLUMNS.into(),
                    "--associated".into(),
                    fd_path(&self.media[index].file).into_os_string(),
                ],
            )?;
            let mappings: Mappings = serde_json::from_slice(&output)?;
            check(mappings.loopdevices.len() <= 1, "owned backing has multiple loop associations")?;
            for mapping in &mappings.loopdevices {
                verify_mapping(mapping, &self.media[index].identity, self.media[index].block_identity.as_ref())?;
                check(
                    Identity::path(&mapping.backing)? == self.media[index].identity,
                    "reported backing path changed",
                )?;
            }
            Ok(mappings.loopdevices)
        }

        fn claim(&mut self, index: usize) -> TestResult {
            let found = self.associations(index)?;
            let Some(mapping) = found.into_iter().next() else {
                return Ok(());
            };
            validate_loop_name(&mapping.name)?;
            let metadata = fs::symlink_metadata(&mapping.name)?;
            check(metadata.file_type().is_block_device(), "claimed path is not a block device")?;
            let block = OpenOptions::new().read(true).write(true).open(&mapping.name)?;
            let identity = Identity::of(&metadata);
            identity.verify(&mapping.name, &block)?;
            verify_mapping(&mapping, &self.media[index].identity, Some(&identity))?;
            self.media[index].mapping = Some(mapping);
            self.media[index].block = Some(block);
            self.media[index].block_identity = Some(identity);
            self.record("loop-claimed")
        }

        fn verify_medium(&self, index: usize) -> TestResult {
            self.verify_owner()?;
            let medium = &self.media[index];
            let expected = medium.mapping.as_ref().ok_or("medium has no registered mapping")?;
            medium
                .block_identity
                .as_ref()
                .ok_or("missing block identity")?
                .verify(&expected.name, medium.block.as_ref().ok_or("missing block owner")?)?;
            let found = self.associations(index)?;
            check(found.len() == 1 && found[0].name == expected.name, "owned loop association changed")
        }

        fn require_active(&self) -> TestResult {
            check(!self.closing, "D07 storage owner has started cleanup")
        }

        pub(super) fn provision(&mut self) -> TestResult {
            self.require_active()?;
            for index in 0..self.media.len() {
                check(self.associations(index)?.is_empty(), "new backing already associated")?;
                let allocation = self.command(
                    "losetup",
                    &[
                        "--find".into(),
                        "--show".into(),
                        "--nooverlap".into(),
                        fd_path(&self.media[index].file).into_os_string(),
                    ],
                );
                // Reconcile even when allocation failed after changing kernel state.
                let claim = self.claim(index);
                super::super::combine_results(allocation.map(|_| ()), claim)?;
                self.verify_medium(index)?;
                self.command(
                    "mkfs.ext4",
                    &[
                        "-F".into(),
                        "-q".into(),
                        fd_path(self.media[index].block.as_ref().ok_or("missing block handle")?).into_os_string(),
                    ],
                )?;
            }
            for node in 0..self.endpoints.len() {
                self.mount(node, node)?;
            }
            self.verify_device_domains()?;
            self.record("owned-devices-ready")
        }

        fn mount(&mut self, node: usize, medium: usize) -> TestResult {
            self.verify_medium(medium)?;
            let endpoint = &self.endpoints[node];
            check(
                endpoint.mount.is_none() && mount_at(&endpoint.path)?.is_none(),
                "endpoint already mounted",
            )?;
            endpoint.identity.verify(&endpoint.path, &endpoint.directory)?;
            check(fs::read_dir(&endpoint.path)?.next().is_none(), "unmounted endpoint is not empty")?;
            self.endpoints[node].medium = Some(medium);
            let result = self.command(
                "mount",
                &[
                    "--no-canonicalize".into(),
                    "-t".into(),
                    "ext4".into(),
                    "-o".into(),
                    "nodev,nosuid".into(),
                    fd_path(self.media[medium].block.as_ref().ok_or("missing block handle")?).into_os_string(),
                    self.endpoints[node].path.as_os_str().to_owned(),
                ],
            );
            // Save a completed mount even if the command's exit/reporting failed.
            let capture = self.capture_mount(node, medium);
            super::super::combine_results(result.map(|_| ()), capture)?;
            self.verify_mount(node)?;
            let lost_found = self.endpoints[node].path.join("lost+found");
            if lost_found.exists() {
                check(
                    fs::symlink_metadata(&lost_found)?.is_dir() && !fs::symlink_metadata(&lost_found)?.file_type().is_symlink(),
                    "unexpected lost+found type",
                )?;
                fs::remove_dir(&lost_found)?;
            }
            check(fs::read_dir(&self.endpoints[node].path)?.next().is_none(), "new filesystem is not empty")?;
            self.record("owned-filesystem-mounted")
        }

        fn capture_mount(&mut self, node: usize, medium: usize) -> TestResult {
            let endpoint = &self.endpoints[node];
            let mount = mount_at(&endpoint.path)?.ok_or("mount command did not install endpoint")?;
            check(
                mount.filesystem == "ext4"
                    && mount.root == "/"
                    && mount.device == self.media[medium].mapping.as_ref().ok_or("missing loop mapping")?.device,
                "mounted device does not match owner",
            )?;
            let root = File::open(&endpoint.path)?;
            let identity = Identity::path(&endpoint.path)?;
            identity.verify(&endpoint.path, &root)?;
            check(
                device_number(identity.device) == mount.device && identity.device != endpoint.identity.device,
                "endpoint did not cross the owned device boundary",
            )?;
            self.endpoints[node].mount = Some(mount);
            self.endpoints[node].mounted_root = Some(root);
            self.endpoints[node].mounted_identity = Some(identity);
            Ok(())
        }

        fn verify_mount(&self, node: usize) -> TestResult {
            let endpoint = &self.endpoints[node];
            let medium = endpoint.medium.ok_or("endpoint has no owned medium")?;
            self.verify_medium(medium)?;
            verify_mount_record(&endpoint.mount, &mount_at(&endpoint.path)?)?;
            endpoint
                .mounted_identity
                .as_ref()
                .ok_or("missing mounted identity")?
                .verify(&endpoint.path, endpoint.mounted_root.as_ref().ok_or("missing mounted handle")?)
        }

        fn verify_device_domains(&self) -> TestResult {
            let root_devices = rustfs_utils::os::get_physical_device_ids("/")?;
            let owner_devices = rustfs_utils::os::get_physical_device_ids(self.root.to_str().ok_or("non-UTF8 root")?)?;
            check(!root_devices.is_empty() && !owner_devices.is_empty(), "root device evidence is empty")?;
            let mut seen: BTreeSet<String> = root_devices.iter().chain(&owner_devices).cloned().collect();
            let mut observed = Vec::new();
            for node in 0..self.endpoints.len() {
                self.verify_mount(node)?;
                let devices =
                    rustfs_utils::os::get_physical_device_ids(self.endpoints[node].path.to_str().ok_or("non-UTF8 endpoint")?)?;
                check(
                    !devices.is_empty() && devices.iter().all(|device| !seen.contains(device)),
                    "owned target overlaps root or sibling device",
                )?;
                seen.extend(devices.iter().cloned());
                observed.push(json!({"node":node,"devices":devices,"mount":self.endpoints[node].mount}));
            }
            self.evidence.record(
                "owned-device-domains",
                &json!({"root_devices":root_devices,"owner_devices":owner_devices,"nodes":observed,"physical_device_independence":false}),
            )
        }

        pub(super) fn retain_snapshot(&mut self, path: PathBuf) -> TestResult {
            self.require_active()?;
            self.verify_owner()?;
            check(self.snapshot.is_none(), "D07 snapshot already retained")?;
            validate_endpoint(&self.root, &path)?;
            check(
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("snapshot-3-")),
                "unexpected snapshot owner",
            )?;
            let held = File::open(&path)?;
            let identity = Identity::path(&path)?;
            identity.verify(&path, &held)?;
            let inventory = inventory(&path)?;
            self.snapshot = Some(Snapshot {
                path,
                held,
                identity,
                inventory,
            });
            Ok(())
        }

        fn unmount(&mut self, node: usize) -> TestResult {
            self.verify_mount(node)?;
            let expected = self.endpoints[node].mount.clone();
            // A directory handle on this filesystem would keep normal umount busy.
            self.endpoints[node].mounted_root = None;
            self.command("umount", &[self.endpoints[node].path.as_os_str().to_owned()])?;
            check(mount_at(&self.endpoints[node].path)?.is_none(), "owned mount remains after umount")?;
            self.endpoints[node]
                .identity
                .verify(&self.endpoints[node].path, &self.endpoints[node].directory)?;
            self.endpoints[node].mounted_root = None;
            self.endpoints[node].mounted_identity = None;
            self.endpoints[node].mount = None;
            self.endpoints[node].medium = None;
            self.evidence
                .record("owned-filesystem-unmounted", &json!({"node":node,"mount":expected}))
        }

        pub(super) fn replace(&mut self, node: usize, snapshot: Option<PathBuf>) -> TestResult {
            self.require_active()?;
            check(
                node == 3 && !self.replaced,
                "D07 replacement requires the original node3 and unused spare",
            )?;
            self.verify_mount(node)?;
            match &snapshot {
                Some(source) => {
                    let retained = self.snapshot.as_ref().ok_or("snapshot has no original owner")?;
                    check(*source == retained.path, "snapshot differs from original owned copy")?;
                    retained.identity.verify(source, &retained.held)?;
                    check(inventory(source)? == retained.inventory, "snapshot contents changed before replacement")?;
                }
                None => check(self.snapshot.is_none(), "owned stale snapshot omitted from replacement")?,
            }
            let before = inventory(&self.endpoints[node].path)?;
            let previous = self.endpoints[node].medium.ok_or("missing old medium")?;
            let old_mount = self.endpoints[node].mount.clone();
            self.unmount(node)?;
            self.mount(node, 4)?;
            self.replaced = true;
            check(
                self.endpoints[node].mount != old_mount && previous != 4,
                "replacement retained the original mount/device",
            )?;
            if let Some(source) = snapshot {
                let retained = self.snapshot.as_ref().ok_or("snapshot has no original owner")?;
                check(source == retained.path, "snapshot differs from original owned copy")?;
                retained.identity.verify(&source, &retained.held)?;
                check(inventory(&source)? == retained.inventory, "snapshot contents changed before restore")?;
                copy_owned_contents(&source, &self.endpoints[node].path)?;
                retained.identity.verify(&source, &retained.held)?;
                check(inventory(&source)? == retained.inventory, "snapshot contents changed during restore")?;
            } else {
                check(self.snapshot.is_none(), "owned stale snapshot omitted from replacement")?;
            }
            self.verify_device_domains()?;
            self.verify_medium(previous)?;
            check(
                self.media[previous].path.exists() && self.endpoints[node].path.exists(),
                "replacement did not preserve old medium and current directory",
            )?;
            let after = inventory(&self.endpoints[node].path)?;
            self.evidence.record("directory-replaced", &json!({"node":node,"before":before,"retired":self.media[previous].path,"after":after,"old_mount":old_mount,"replacement_mount":self.endpoints[node].mount}))?;
            self.record("owned-device-replaced")
        }

        fn detach(&mut self, index: usize) -> TestResult {
            if self.media[index].detached {
                return Ok(());
            }
            if self.media[index].mapping.is_none() {
                self.claim(index)?;
            }
            if self.media[index].mapping.is_none() {
                self.media[index].detached = true;
                return Ok(());
            }
            self.verify_medium(index)?;
            let device = self.media[index].mapping.as_ref().ok_or("missing owned device")?.name.clone();
            let device_number = self.media[index]
                .mapping
                .as_ref()
                .ok_or("missing owned device")?
                .device
                .clone();
            check(
                !mounts(&fs::read_to_string("/proc/self/mountinfo")?)?
                    .iter()
                    .any(|mount| mount.device == device_number),
                "owned medium still has a mount",
            )?;
            // Keep the open block handle through LOOP_CLR_FD so a different
            // backing cannot occupy this device before the detach request.
            self.command("losetup", &["--detach".into(), device.into_os_string()])?;
            self.media[index].block = None;
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                let output = self.command(
                    "losetup",
                    &[
                        "--list".into(),
                        "--json".into(),
                        "--output".into(),
                        LOOP_COLUMNS.into(),
                        "--associated".into(),
                        fd_path(&self.media[index].file).into_os_string(),
                    ],
                )?;
                let found: Mappings = serde_json::from_slice(&output)?;
                if found.loopdevices.is_empty() {
                    break;
                }
                check(Instant::now() < deadline, "owned loop remains associated after detach")?;
                std::thread::sleep(Duration::from_millis(20));
            }
            self.media[index].detached = true;
            self.record("owned-loop-detached")
        }

        pub(super) fn cleanup(&mut self) -> TestResult {
            // Queued blocking actions can outlive their cancelled awaiting future.
            // Close admission under this same mutex, including failed cleanup.
            self.closing = true;
            if self.cleaned {
                return Ok(());
            }
            self.verify_owner()?;
            // Stop at an uncertain mount identity. Never detach underneath it.
            for node in (0..self.endpoints.len()).rev() {
                if self.endpoints[node].mount.is_some() {
                    self.unmount(node)?;
                } else {
                    check(
                        mount_at(&self.endpoints[node].path)?.is_none(),
                        "unregistered mount at owned endpoint; retain media",
                    )?;
                }
            }
            for index in (0..self.media.len()).rev() {
                self.detach(index)?;
            }
            self.cleaned = true;
            self.record("owned-media-cleanup-complete")
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn backing_owner_rejects_replacement_and_symlink() -> TestResult {
            let directory = tempfile::tempdir()?;
            let path = directory.path().join("owned.img");
            let held = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&path)?;
            let identity = Identity::path(&path)?;
            identity.verify(&path, &held)?;
            fs::rename(&path, directory.path().join("retained.img"))?;
            fs::write(&path, b"replacement")?;
            assert!(identity.verify(&path, &held).is_err());
            fs::remove_file(&path)?;
            std::os::unix::fs::symlink(directory.path().join("retained.img"), &path)?;
            assert!(identity.verify(&path, &held).is_err());
            Ok(())
        }

        #[test]
        fn endpoint_rejects_nonowned_and_redirected_paths() -> TestResult {
            let owner = tempfile::tempdir()?;
            let foreign = tempfile::tempdir()?;
            let root = owner.path().canonicalize()?;
            let path = root.join("node0");
            fs::create_dir(&path)?;
            validate_endpoint(&root, &path)?;
            assert!(validate_endpoint(&root, foreign.path()).is_err());
            fs::remove_dir(&path)?;
            std::os::unix::fs::symlink(foreign.path(), &path)?;
            assert!(validate_endpoint(&root, &path).is_err());
            Ok(())
        }

        #[test]
        fn loop_mapping_rejects_foreign_backing_device_and_parameters() -> TestResult {
            let identity = Identity {
                device: 2049,
                inode: 42,
                rdev: 0,
                mode: 0o100600,
            };
            let mapping = Mapping {
                name: "/dev/loop7".into(),
                backing: "/owned/medium.img".into(),
                backing_inode: 42,
                backing_device: device_number(2049),
                device: "7:7".into(),
                offset: 0,
                sizelimit: 0,
                ro: false,
                autoclear: false,
            };
            verify_mapping(&mapping, &identity, None)?;
            for field in 0..7 {
                let mut changed = mapping.clone();
                match field {
                    0 => changed.name = "/dev/sda".into(),
                    1 => changed.backing_inode += 1,
                    2 => changed.backing_device = "8:2".into(),
                    3 => changed.offset = 1,
                    4 => changed.sizelimit = 1,
                    5 => changed.ro = true,
                    _ => changed.autoclear = true,
                }
                assert!(verify_mapping(&changed, &identity, None).is_err(), "field {field}");
            }
            assert!(
                verify_mapping(
                    &mapping,
                    &identity,
                    Some(&Identity {
                        rdev: 0,
                        ..identity.clone()
                    })
                )
                .is_err()
            );
            for path in ["/dev/loop", "/dev/loop7/../loop8", "/tmp/loop7", "/dev/loop7extra"] {
                assert!(validate_loop_name(Path::new(path)).is_err(), "{path}");
            }
            Ok(())
        }

        #[test]
        fn mount_identity_preserves_id_device_root_and_escaped_path() -> TestResult {
            let original = mounts("123 5 7:7 / /owned/node\\0403 rw - ext4 /dev/loop7 rw\n")?;
            assert_eq!(original.len(), 1);
            assert_eq!(original[0].target, Path::new("/owned/node 3"));
            for changed in [
                "124 5 7:7 / /owned/node\\0403 rw - ext4 /dev/loop7 rw",
                "123 5 7:8 / /owned/node\\0403 rw - ext4 /dev/loop8 rw",
                "123 5 7:7 /other /owned/node\\0403 rw - ext4 /dev/loop7 rw",
            ] {
                assert!(verify_mount_record(&Some(original[0].clone()), &Some(mounts(changed)?[0].clone())).is_err());
            }
            verify_mount_record(&Some(original[0].clone()), &Some(original[0].clone()))?;
            assert!(verify_mount_record(&Some(original[0].clone()), &None).is_err());
            assert!(verify_mount_record(&None, &None).is_err());
            assert!(mounts("invalid").is_err());
            assert!(unescape_mount("bad\\999").is_err());
            Ok(())
        }

        #[test]
        fn cancelled_queued_provision_cannot_run_after_cleanup() -> TestResult {
            use std::future::Future;
            use std::task::Poll;

            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .max_blocking_threads(1)
                .build()?;
            runtime.block_on(async {
                for already_cleaned in [false, true] {
                    let directory = tempfile::tempdir()?;
                    let root = directory.path().canonicalize()?;
                    let root_file = File::open(&root)?;
                    let identity = Identity::path(&root)?;
                    let evidence = Arc::new(Evidence::new(root.join("evidence"))?);
                    // This admission-only owner contains no block media or mounts.
                    let owner = OwnedLinuxBlockDevices {
                        state: Arc::new(std::sync::Mutex::new(State {
                            root: root.clone(),
                            root_file: root_file.try_clone()?,
                            root_identity: identity.clone(),
                            media_root: root,
                            media_file: root_file,
                            media_identity: identity,
                            namespace: namespace()?,
                            evidence,
                            media: Vec::new(),
                            endpoints: Vec::new(),
                            replaced: false,
                            cleaned: already_cleaned,
                            closing: false,
                            snapshot: None,
                        })),
                    };
                    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
                    let (release_tx, release_rx) = std::sync::mpsc::channel();
                    let blocker = tokio::task::spawn_blocking(move || {
                        started_tx.send(()).expect("announce occupied blocking thread");
                        release_rx
                            .recv_timeout(Duration::from_secs(5))
                            .expect("release occupied blocking thread");
                    });
                    started_rx.await?;
                    let (result_tx, result_rx) = tokio::sync::oneshot::channel::<TestResult>();
                    let mut pending = Box::pin(owner.run(move |state| {
                        let result = state.provision();
                        result_tx.send(result).map_err(|_| "cancelled-operation observer closed")?;
                        Ok(())
                    }));
                    std::future::poll_fn(|context| {
                        assert!(pending.as_mut().poll(context).is_pending(), "blocking operation must remain queued");
                        Poll::Ready(())
                    })
                    .await;
                    drop(pending);
                    let preflight = owner.state.lock().map_err(|_| "test owner poisoned")?.verify_owner();
                    let cleanup = owner.cleanup_sync();
                    if already_cleaned || preflight.is_ok() {
                        cleanup?;
                    } else {
                        assert_eq!(
                            cleanup.expect_err("preserve cleanup preflight error").to_string(),
                            preflight.expect_err("failed preflight").to_string()
                        );
                    }
                    release_tx.send(())?;
                    let outcome = tokio::time::timeout(Duration::from_secs(5), result_rx).await??;
                    assert_eq!(
                        outcome.expect_err("queued provision after cleanup must fail").to_string(),
                        "D07 storage owner has started cleanup"
                    );
                    blocker.await?;
                    let mut state = owner.state.lock().map_err(|_| "test owner poisoned")?;
                    assert!(state.closing);
                    assert!(state.media.is_empty() && state.endpoints.is_empty());
                    assert_eq!(
                        state
                            .retain_snapshot(PathBuf::new())
                            .expect_err("closed snapshot admission")
                            .to_string(),
                        "D07 storage owner has started cleanup"
                    );
                    assert_eq!(
                        state.replace(3, None).expect_err("closed replacement admission").to_string(),
                        "D07 storage owner has started cleanup"
                    );
                }
                Ok(())
            })
        }
    }
}
