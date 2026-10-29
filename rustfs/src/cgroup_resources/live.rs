// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

//! Live process-cgroup observations for background admission. Sampling belongs
//! on a blocking worker, never on an object or request path. These files are
//! individually sampled; this is not an atomic kernel resource reservation.

use std::ffi::{OsStr, OsString};
use std::fs;
use std::io::{self, Read};
use std::path::{Component, Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ResourceSample {
    pub(crate) cpu_millis: u64,
    pub(crate) memory_limit: u64,
    pub(crate) memory_headroom: u64,
    pub(crate) cpu_usage_usec: Option<u64>,
    pub(crate) cpu_throttled_usec: Option<u64>,
    pub(crate) cpu_stall_usec: Option<u64>,
    pub(crate) memory_stall_usec: Option<u64>,
    pub(crate) io_stall_usec: Option<u64>,
    /// Changing cgroup membership invalidates cumulative counter differences.
    pub(crate) identity: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CpuQuotaSample {
    pub(crate) identity: (u64, u64),
    pub(crate) quota: u64,
    pub(crate) period: u64,
    pub(crate) cpu_millis: u64,
    pub(crate) usage_usec: Option<u64>,
    pub(crate) throttled_usec: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CpuCoreSample {
    pub(crate) id: u32,
    // user, nice, system, idle, iowait, irq, softirq and steal; guest time
    // is already included in user/nice and must not be counted again.
    pub(crate) counters: [u64; 8],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CpuCoreObservation {
    pub(crate) ticks_per_second: u64,
    pub(crate) cores: Vec<CpuCoreSample>,
    pub(crate) sysfs_identity: (u64, u64),
    pub(crate) cpu_root_identity: (u64, u64),
    pub(crate) topology: Vec<(u32, SiblingObservation)>,
    pub(crate) threads: Vec<ThreadObservation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CpuLimit {
    quota: Option<u64>,
    period: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LevelLimits {
    cpu: Option<CpuLimit>,
    memory: Option<Option<u64>>,
}

struct LimitsObservation {
    cpu_millis: u64,
    memory_limit: u64,
    memory_headroom: u64,
    levels: Vec<LevelLimits>,
}

impl LimitsObservation {
    fn confirm(mut self, later: Self) -> io::Result<Self> {
        if self.levels != later.levels || self.cpu_millis != later.cpu_millis || self.memory_limit != later.memory_limit {
            return Err(invalid());
        }
        self.memory_headroom = self.memory_headroom.min(later.memory_headroom);
        Ok(self)
    }
}

#[derive(Debug)]
struct Scope {
    mount: PathBuf,
    leaf: PathBuf,
    components: Vec<OsString>,
    directories: Vec<fs::File>,
}

fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "incomplete process resource observation")
}

fn bounded_file(file: fs::File, limit: usize) -> io::Result<String> {
    // procfs reports zero file lengths. Bound the read itself and reject the
    // entire observation on overflow, even if an early prefix looks valid.
    let mut contents = String::new();
    file.take(u64::try_from(limit).map_err(|_| invalid())? + 1)
        .read_to_string(&mut contents)?;
    if contents.len() > limit {
        return Err(invalid());
    }
    Ok(contents)
}

fn resource_text_at(directory: &fs::File, name: &str) -> io::Result<String> {
    bounded_file(open_relative(directory, OsStr::new(name), false)?, 64 * 1024)
}

fn open_relative(directory: &fs::File, name: &OsStr, is_directory: bool) -> io::Result<fs::File> {
    use rustix::fs::{Mode, OFlags};
    let mut flags = OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW;
    if is_directory {
        flags |= OFlags::DIRECTORY;
    }
    #[cfg(target_os = "linux")]
    let fd = rustix::fs::openat2(
        directory,
        name,
        flags,
        Mode::empty(),
        rustix::fs::ResolveFlags::BENEATH | rustix::fs::ResolveFlags::NO_SYMLINKS | rustix::fs::ResolveFlags::NO_XDEV,
    )?;
    // This module is compiled on non-Linux only for parser tests.
    #[cfg(not(target_os = "linux"))]
    let fd = rustix::fs::openat(directory, name, flags, Mode::empty())?;
    Ok(fs::File::from(fd))
}

#[cfg(any(target_os = "linux", test))]
fn physical_cgroup_root(is_directory: bool, filesystem_type: u64, inode: u64) -> bool {
    const CGROUP2_SUPER_MAGIC: u64 = 0x6367_7270;
    is_directory && filesystem_type == CGROUP2_SUPER_MAGIC && inode == 1
}

#[cfg(target_os = "linux")]
fn open_physical_root(path: &Path) -> io::Result<fs::File> {
    use std::os::unix::fs::MetadataExt;

    let root = fs::File::open(path)?;
    let metadata = root.metadata()?;
    let filesystem = rustix::fs::fstatfs(&root)?;
    // A cgroup namespace can present a subtree as `/`. The kernfs hierarchy
    // root has inode 1; the mountinfo path alone cannot prove that identity.
    if !physical_cgroup_root(metadata.is_dir(), filesystem.f_type as u64, metadata.ino()) {
        return Err(invalid());
    }
    Ok(root)
}

#[cfg(not(target_os = "linux"))]
fn open_physical_root(_path: &Path) -> io::Result<fs::File> {
    Err(io::Error::new(io::ErrorKind::Unsupported, "cgroup2 root proof requires Linux"))
}

fn absolute_path(value: &str) -> io::Result<PathBuf> {
    let path = PathBuf::from(value);
    if !path.is_absolute() || value.split('/').any(|part| part == "." || part == "..") || path.components().count() > 256 {
        return Err(invalid());
    }
    Ok(path)
}

fn mount_path(value: &str) -> io::Result<PathBuf> {
    // mountinfo escapes whitespace and backslashes, not arbitrary UTF-8 bytes.
    let mut decoded = String::new();
    let mut chars = value.chars();
    while let Some(character) = chars.next() {
        if character != '\\' {
            decoded.push(character);
            continue;
        }
        let escape: String = chars.by_ref().take(3).collect();
        decoded.push(match escape.as_str() {
            "040" => ' ',
            "011" => '\t',
            "012" => '\n',
            "134" => '\\',
            _ => return Err(invalid()),
        });
    }
    absolute_path(&decoded)
}

fn contains_process(directory: &fs::File, pid: u32) -> bool {
    let Ok(file) = open_relative(directory, OsStr::new("cgroup.procs"), false) else {
        return false;
    };
    let Ok(contents) = bounded_file(file, 1024 * 1024) else {
        return false;
    };
    contents.lines().any(|line| line.trim().parse::<u32>().ok() == Some(pid))
}

fn resolve_scope_with_root(
    membership: &str,
    mounts: &str,
    pid: u32,
    open_root: &impl Fn(&Path) -> io::Result<fs::File>,
) -> io::Result<Scope> {
    let mut unified = membership.lines().filter_map(|line| line.strip_prefix("0::"));
    let member = absolute_path(unified.next().ok_or_else(invalid)?)?;
    if unified.next().is_some() {
        return Err(invalid());
    }
    let mut selected: Option<(usize, Scope)> = None;
    for line in mounts.lines() {
        let Some((before, after)) = line.split_once(" - ") else {
            continue;
        };
        if after.split_whitespace().next() != Some("cgroup2") {
            continue;
        }
        let mut fields = before.split_whitespace();
        let root = mount_path(fields.nth(3).ok_or_else(invalid)?)?;
        let mount = mount_path(fields.next().ok_or_else(invalid)?)?;
        // The first form handles a bind-mounted hierarchy. The second handles
        // a cgroup namespace whose membership is relative to its namespace root.
        // Verify the PID in either case: existence alone can select a sibling.
        let relative = member.strip_prefix(&root).ok();
        let namespaced = member.strip_prefix(Path::new("/")).map_err(|_| invalid())?;
        for suffix in relative.into_iter().chain(std::iter::once(namespaced)) {
            let leaf = mount.join(suffix);
            let Ok(root_directory) = open_root(&mount) else {
                continue;
            };
            let mut directories = vec![root_directory];
            let mut components = Vec::new();
            let mut complete = true;
            for component in suffix.components() {
                let Component::Normal(name) = component else {
                    complete = false;
                    break;
                };
                let Some(parent) = directories.last() else {
                    complete = false;
                    break;
                };
                let Ok(directory) = open_relative(parent, name, true) else {
                    complete = false;
                    break;
                };
                directories.push(directory);
                components.push(name.to_os_string());
            }
            if !complete || !directories.last().is_some_and(|directory| contains_process(directory, pid)) {
                continue;
            }
            let depth = root.components().count();
            // Prefer the widest visible hierarchy so ancestor limits are read.
            if selected.as_ref().is_none_or(|(previous, _)| depth < *previous) {
                selected = Some((
                    depth,
                    Scope {
                        leaf,
                        mount: mount.clone(),
                        components,
                        directories,
                    },
                ));
            }
            break;
        }
    }
    selected.map(|(_, scope)| scope).ok_or_else(invalid)
}

impl Scope {
    fn leaf_directory(&self) -> io::Result<&fs::File> {
        self.directories.last().ok_or_else(invalid)
    }

    fn revalidate(&self, open_root: &impl Fn(&Path) -> io::Result<fs::File>) -> io::Result<()> {
        let mut current = open_root(&self.mount)?;
        let mut expected = self.directories.iter();
        if file_identity(&current)? != file_identity(expected.next().ok_or_else(invalid)?)? {
            return Err(invalid());
        }
        for name in &self.components {
            current = open_relative(&current, name, true)?;
            if file_identity(&current)? != file_identity(expected.next().ok_or_else(invalid)?)? {
                return Err(invalid());
            }
        }
        if expected.next().is_some() {
            return Err(invalid());
        }
        Ok(())
    }
}

fn cpu_count(list: &str) -> io::Result<u64> {
    let mut count = 0_u64;
    let mut previous = None;
    for item in list.trim().split(',') {
        let (first, last) = item.split_once('-').unwrap_or((item, item));
        let first = first.parse::<u64>().map_err(|_| invalid())?;
        let last = last.parse::<u64>().map_err(|_| invalid())?;
        if first > last || previous.is_some_and(|previous| previous >= first) {
            return Err(invalid());
        }
        count = count
            .checked_add(last.checked_sub(first).and_then(|n| n.checked_add(1)).ok_or_else(invalid)?)
            .ok_or_else(invalid)?;
        previous = Some(last);
    }
    if count == 0 { Err(invalid()) } else { Ok(count) }
}

fn allowed_cpu_ids(list: &str) -> io::Result<Vec<u32>> {
    // A larger or malformed mask is unavailable, never an empty CPU set.
    const MAX_CPU_IDS: u64 = 65_536;
    let count = cpu_count(list)?;
    if count > MAX_CPU_IDS {
        return Err(invalid());
    }
    let mut cpus = Vec::with_capacity(usize::try_from(count).map_err(|_| invalid())?);
    for item in list.trim().split(',') {
        let (first, last) = item.split_once('-').unwrap_or((item, item));
        let first = first.parse::<u32>().map_err(|_| invalid())?;
        let last = last.parse::<u32>().map_err(|_| invalid())?;
        cpus.extend(first..=last);
    }
    Ok(cpus)
}

fn parse_cpu_cores(contents: &str, expected: &[u32]) -> io::Result<Vec<CpuCoreSample>> {
    let mut cores = Vec::with_capacity(expected.len());
    for line in contents.lines() {
        let mut fields = line.split_whitespace();
        let Some(name) = fields.next() else { continue };
        let Some(id) = name.strip_prefix("cpu").filter(|id| !id.is_empty()) else {
            continue;
        };
        let Ok(id) = id.parse::<u32>() else { continue };
        if expected.binary_search(&id).is_err() {
            continue;
        }
        let mut counters = [0_u64; 8];
        for counter in &mut counters {
            *counter = fields.next().ok_or_else(invalid)?.parse().map_err(|_| invalid())?;
        }
        cores.push(CpuCoreSample { id, counters });
        if cores.len() > expected.len() {
            return Err(invalid());
        }
    }
    cores.sort_unstable_by_key(|core| core.id);
    if cores.len() != expected.len() || !cores.iter().map(|core| core.id).eq(expected.iter().copied()) {
        return Err(invalid());
    }
    Ok(cores)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SiblingObservation {
    pub(crate) cpu_identity: (u64, u64),
    pub(crate) topology_identity: (u64, u64),
    pub(crate) siblings: Vec<u32>,
}

fn read_sibling_topology(
    cpu_root: &fs::File,
    allowed: &[u32],
) -> io::Result<std::collections::BTreeMap<u32, SiblingObservation>> {
    use std::collections::{BTreeMap, BTreeSet, VecDeque};

    const MAX_CPUS: usize = 65_536;
    const MAX_MEMBERS: usize = 1_048_576;
    let mut pending = VecDeque::new();
    let mut scheduled = BTreeSet::new();
    for &id in allowed {
        if scheduled.insert(id) {
            pending.push_back(id);
        }
    }
    if pending.is_empty() || scheduled.len() > MAX_CPUS {
        return Err(invalid());
    }
    let mut observations = BTreeMap::new();
    let mut members = 0_usize;
    while let Some(id) = pending.pop_front() {
        let name = format!("cpu{id}");
        let cpu = open_relative(cpu_root, OsStr::new(&name), true)?;
        let topology = open_relative(&cpu, OsStr::new("topology"), true)?;
        let siblings = allowed_cpu_ids(&resource_text_at(&topology, "thread_siblings_list")?)?;
        if siblings.binary_search(&id).is_err() {
            return Err(invalid());
        }
        members = members.checked_add(siblings.len()).ok_or_else(invalid)?;
        if members > MAX_MEMBERS {
            return Err(invalid());
        }
        for &sibling in &siblings {
            if scheduled.insert(sibling) {
                if scheduled.len() > MAX_CPUS {
                    return Err(invalid());
                }
                pending.push_back(sibling);
            }
        }
        observations.insert(
            id,
            SiblingObservation {
                cpu_identity: file_identity(&cpu)?,
                topology_identity: file_identity(&topology)?,
                siblings,
            },
        );
    }
    // All members of a reported core must agree, including siblings outside
    // this process's affinity. An incomplete or changing topology is unknown.
    for observation in observations.values() {
        if observation.siblings.iter().any(|sibling| {
            observations
                .get(sibling)
                .is_none_or(|other| other.siblings != observation.siblings)
        }) {
            return Err(invalid());
        }
    }
    Ok(observations)
}

#[cfg(target_os = "linux")]
fn open_sysfs_root() -> io::Result<fs::File> {
    use std::os::unix::fs::MetadataExt;

    const SYSFS_MAGIC: u64 = 0x6265_6572;
    let root = fs::File::open("/sys")?;
    let metadata = root.metadata()?;
    if !metadata.is_dir() || metadata.ino() != 1 || rustix::fs::fstatfs(&root)?.f_type as u64 != SYSFS_MAGIC {
        return Err(invalid());
    }
    Ok(root)
}

#[cfg(target_os = "linux")]
fn open_sysfs_cpus(root: &fs::File) -> io::Result<fs::File> {
    let devices = open_relative(root, OsStr::new("devices"), true)?;
    let system = open_relative(&devices, OsStr::new("system"), true)?;
    open_relative(&system, OsStr::new("cpu"), true)
}

#[cfg(target_os = "linux")]
fn verify_proc_root(root: &fs::File) -> io::Result<()> {
    use std::os::unix::fs::MetadataExt;

    const PROC_SUPER_MAGIC: u64 = 0x9fa0;
    let metadata = root.metadata()?;
    if !metadata.is_dir() || metadata.ino() != 1 || rustix::fs::fstatfs(root)?.f_type as u64 != PROC_SUPER_MAGIC {
        return Err(invalid());
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn open_proc_root(path: &Path) -> io::Result<fs::File> {
    let root = fs::File::open(path)?;
    verify_proc_root(&root)?;
    Ok(root)
}

#[cfg(not(target_os = "linux"))]
fn open_proc_root(_path: &Path) -> io::Result<fs::File> {
    Err(io::Error::new(io::ErrorKind::Unsupported, "procfs root proof requires Linux"))
}

fn open_proc_process(root: &fs::File, pid: u32) -> io::Result<fs::File> {
    let pid = pid.to_string();
    #[cfg(target_os = "linux")]
    let self_link = {
        use rustix::fs::{Mode, OFlags, ResolveFlags};
        // O_PATH|NOFOLLOW obtains the magic link itself, without traversing
        // it or a bind mount. readlinkat with an empty path reads that fd.
        let link = rustix::fs::openat2(
            root,
            "self",
            OFlags::PATH | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
            ResolveFlags::BENEATH | ResolveFlags::NO_SYMLINKS | ResolveFlags::NO_XDEV,
        )?;
        rustix::fs::readlinkat(&link, "", Vec::new())?
    };
    // Non-Linux is compiled only for synthetic parser tests.
    #[cfg(not(target_os = "linux"))]
    let self_link = rustix::fs::readlinkat(root, "self", Vec::new())?;
    if self_link.to_bytes() != pid.as_bytes() {
        return Err(invalid());
    }
    open_relative(root, OsStr::new(&pid), true)
}

#[cfg(target_os = "linux")]
fn read_cpu_cores(root: &fs::File, allowed: &[u32], threads: &[ThreadObservation]) -> io::Result<CpuCoreObservation> {
    verify_proc_root(root)?;
    let sysfs = open_sysfs_root()?;
    let sysfs_identity = file_identity(&sysfs)?;
    let cpu_root = open_sysfs_cpus(&sysfs)?;
    let cpu_root_identity = file_identity(&cpu_root)?;
    let topology = read_sibling_topology(&cpu_root, allowed)?;
    let monitored: Vec<_> = topology.keys().copied().collect();
    let stat = open_relative(root, OsStr::new("stat"), false)?;
    let contents = bounded_file(stat, 4 * 1024 * 1024)?;
    let cores = parse_cpu_cores(&contents, &monitored)?;
    let ticks_per_second = rustix::param::clock_ticks_per_second();
    if ticks_per_second == 0 || i64::try_from(ticks_per_second).is_err() {
        return Err(invalid());
    }
    let current_sysfs = open_sysfs_root()?;
    let current_cpu_root = open_sysfs_cpus(&current_sysfs)?;
    if file_identity(&current_sysfs)? != sysfs_identity
        || file_identity(&current_cpu_root)? != cpu_root_identity
        || read_sibling_topology(&current_cpu_root, allowed)? != topology
    {
        return Err(invalid());
    }
    Ok(CpuCoreObservation {
        ticks_per_second,
        cores,
        sysfs_identity,
        cpu_root_identity,
        topology: topology.into_iter().collect(),
        threads: threads.to_vec(),
    })
}

#[cfg(not(target_os = "linux"))]
fn read_cpu_cores(_root: &fs::File, _allowed: &[u32], _threads: &[ThreadObservation]) -> io::Result<CpuCoreObservation> {
    Err(io::Error::new(io::ErrorKind::Unsupported, "per-core observation requires Linux"))
}

fn allowed_cpus(status: &str) -> io::Result<&str> {
    let mut values = status.lines().filter_map(|line| line.strip_prefix("Cpus_allowed_list:"));
    let value = values.next().ok_or_else(invalid)?.trim();
    if value.is_empty() || values.next().is_some() {
        return Err(invalid());
    }
    Ok(value)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ThreadObservation {
    pub(crate) id: u32,
    pub(crate) identity: (u64, u64),
    pub(crate) allowed: Vec<u32>,
}

fn matching_threads(tasks: &fs::File, pid: u32, membership: &str, leader_allowed: &str) -> io::Result<Vec<ThreadObservation>> {
    // A threaded cgroup can constrain a worker more tightly than the leader.
    // Observe every affinity separately; an incomplete inventory is unknown.
    const MAX_THREADS: usize = 4096;
    const MAX_MEMBERS: usize = 1_048_576;
    let mut threads = Vec::new();
    let mut members = 0_usize;
    let mut entries = rustix::fs::Dir::read_from(tasks)?;
    while let Some(entry) = entries.read() {
        let entry = entry?;
        let name = entry.file_name().to_str().map_err(|_| invalid())?;
        if name == "." || name == ".." {
            continue;
        }
        if threads.len() >= MAX_THREADS {
            return Err(invalid());
        }
        let tid = name.parse::<u32>().map_err(|_| invalid())?;
        if tid == 0 || tid.to_string() != name {
            return Err(invalid());
        }
        let thread = open_relative(tasks, OsStr::new(name), true)?;
        if resource_text_at(&thread, "cgroup")? != membership {
            return Err(invalid());
        }
        let status = resource_text_at(&thread, "status")?;
        let allowed = allowed_cpu_ids(allowed_cpus(&status)?)?;
        members = members.checked_add(allowed.len()).ok_or_else(invalid)?;
        if members > MAX_MEMBERS || (tid == pid && allowed != allowed_cpu_ids(leader_allowed)?) {
            return Err(invalid());
        }
        threads.push(ThreadObservation {
            id: tid,
            identity: file_identity(&thread)?,
            allowed,
        });
    }
    threads.sort_unstable_by_key(|thread| thread.id);
    if threads.binary_search_by_key(&pid, |thread| thread.id).is_err() {
        return Err(invalid());
    }
    Ok(threads)
}

fn thread_cpu_envelope(threads: &[ThreadObservation]) -> io::Result<(u64, Vec<u32>)> {
    let min_count = threads.iter().map(|thread| thread.allowed.len()).min().ok_or_else(invalid)?;
    let mut union = std::collections::BTreeSet::new();
    for thread in threads {
        union.extend(thread.allowed.iter().copied());
    }
    if min_count == 0 || union.len() > 65_536 {
        return Err(invalid());
    }
    let millis = u64::try_from(min_count)
        .map_err(|_| invalid())?
        .checked_mul(1000)
        .ok_or_else(invalid)?;
    Ok((millis, union.into_iter().collect()))
}

fn cpu_limit(value: &str) -> io::Result<CpuLimit> {
    let mut fields = value.split_whitespace();
    let quota = fields.next().ok_or_else(invalid)?;
    let period = fields.next().ok_or_else(invalid)?.parse::<u64>().map_err(|_| invalid())?;
    if period == 0 || fields.next().is_some() {
        return Err(invalid());
    }
    if quota == "max" {
        return Ok(CpuLimit { quota: None, period });
    }
    let quota = quota.parse::<u64>().map_err(|_| invalid())?;
    if quota == 0 {
        return Err(invalid());
    }
    Ok(CpuLimit {
        quota: Some(quota),
        period,
    })
}

impl CpuLimit {
    fn millis(self) -> io::Result<Option<u64>> {
        self.quota
            .map(|quota| {
                let millis = u128::from(quota) * 1000 / u128::from(self.period);
                u64::try_from(millis).map_err(|_| invalid())
            })
            .transpose()
    }
}

fn memory_limit(value: &str) -> io::Result<Option<u64>> {
    if value.trim() == "max" {
        return Ok(None);
    }
    value.trim().parse::<u64>().map(Some).map_err(|_| invalid())
}

fn scalar(directory: &fs::File, name: &str) -> io::Result<u64> {
    resource_text_at(directory, name)?
        .trim()
        .parse::<u64>()
        .map_err(|_| invalid())
}

fn counter(contents: &str, key: &str) -> Option<u64> {
    let mut values = contents.lines().filter_map(|line| {
        let (name, value) = line.split_once(' ')?;
        (name == key).then_some(value)
    });
    let value = values.next()?.trim().parse().ok()?;
    values.next().is_none().then_some(value)
}

fn pressure(directory: &fs::File, name: &str) -> Option<u64> {
    let contents = resource_text_at(directory, name).ok()?;
    let mut some = contents.lines().filter_map(|line| line.strip_prefix("some "));
    let line = some.next()?;
    if some.next().is_some() {
        return None;
    }
    let mut totals = line.split_whitespace().filter_map(|field| field.strip_prefix("total="));
    let value = totals.next()?.parse().ok()?;
    totals.next().is_none().then_some(value)
}

fn host_memory(contents: &str, key: &str) -> io::Result<u64> {
    let mut values = contents.lines().filter_map(|line| line.strip_prefix(key));
    let mut fields = values.next().ok_or_else(invalid)?.split_whitespace();
    let value = fields.next().ok_or_else(invalid)?.parse::<u64>().map_err(|_| invalid())?;
    if fields.next() != Some("kB") || fields.next().is_some() || values.next().is_some() {
        return Err(invalid());
    }
    value.checked_mul(1024).ok_or_else(invalid)
}

fn read_limits(scope: &Scope, mut cpus: u64, mut memory: u64, mut headroom: u64) -> io::Result<LimitsObservation> {
    let mut levels = Vec::with_capacity(scope.directories.len());
    for (index, directory) in scope.directories.iter().enumerate() {
        let root = index == 0;
        let cpu = match resource_text_at(directory, "cpu.max") {
            Ok(value) => {
                let limit = cpu_limit(&value)?;
                if let Some(limit) = limit.millis()? {
                    cpus = cpus.min(limit);
                }
                Some(limit)
            }
            Err(error) if root && error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        let memory_max = match resource_text_at(directory, "memory.max") {
            Ok(value) => {
                let limit = memory_limit(&value)?;
                if let Some(limit) = limit {
                    memory = memory.min(limit);
                    // Parent usage includes siblings. Subtracting just our leaf's
                    // usage would overstate the capacity available to this process.
                    headroom = headroom.min(limit.saturating_sub(scalar(directory, "memory.current")?));
                }
                Some(limit)
            }
            Err(error) if root && error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        levels.push(LevelLimits { cpu, memory: memory_max });
    }
    if levels.is_empty() {
        return Err(invalid());
    }
    Ok(LimitsObservation {
        cpu_millis: cpus,
        memory_limit: memory,
        memory_headroom: headroom,
        levels,
    })
}

pub(crate) fn sample() -> io::Result<ResourceSample> {
    sample_at_with_roots(
        Path::new("/proc"),
        std::process::id(),
        &open_physical_root,
        &open_proc_root,
        false,
        &read_cpu_cores,
    )
    .map(|(sample, _, _)| sample)
}

pub(crate) fn sample_for_admission() -> io::Result<(ResourceSample, Vec<CpuQuotaSample>, Option<CpuCoreObservation>)> {
    sample_at(Path::new("/proc"), std::process::id())
}

fn sample_at(proc_root: &Path, pid: u32) -> io::Result<(ResourceSample, Vec<CpuQuotaSample>, Option<CpuCoreObservation>)> {
    sample_at_with_roots(proc_root, pid, &open_physical_root, &open_proc_root, true, &read_cpu_cores)
}

fn sample_at_with_roots(
    proc_root: &Path,
    pid: u32,
    open_root: &impl Fn(&Path) -> io::Result<fs::File>,
    open_proc: &impl Fn(&Path) -> io::Result<fs::File>,
    pressure_details: bool,
    read_cores: &impl Fn(&fs::File, &[u32], &[ThreadObservation]) -> io::Result<CpuCoreObservation>,
) -> io::Result<(ResourceSample, Vec<CpuQuotaSample>, Option<CpuCoreObservation>)> {
    use std::hash::{Hash, Hasher};
    let proc = open_proc(proc_root)?;
    let proc_identity = file_identity(&proc)?;
    let process = open_proc_process(&proc, pid)?;
    let process_identity = file_identity(&process)?;
    let tasks = open_relative(&process, OsStr::new("task"), true)?;
    let tasks_identity = file_identity(&tasks)?;
    let membership = resource_text_at(&process, "cgroup")?;
    let mounts = bounded_file(open_relative(&process, OsStr::new("mountinfo"), false)?, 4 * 1024 * 1024)?;
    let scope = resolve_scope_with_root(&membership, &mounts, pid, open_root)?;
    let leaf = scope.leaf_directory()?;
    let directory_identity = file_identity(leaf)?;
    let status = resource_text_at(&process, "status")?;
    let allowed = allowed_cpus(&status)?;
    let threads = matching_threads(&tasks, pid, &membership, allowed)?;
    // Each mask includes its thread's cpuset. Cap by the narrowest mask,
    // and observe contention on the union of every thread's mask.
    let (cpus, monitored_affinity) = thread_cpu_envelope(&threads)?;
    let memory = resource_text_at(&proc, "meminfo")?;
    let total = host_memory(&memory, "MemTotal:")?;
    let available = host_memory(&memory, "MemAvailable:")?;
    if available > total {
        return Err(invalid());
    }
    let limits = read_limits(&scope, cpus, total, available)?;
    let stat = resource_text_at(leaf, "cpu.stat").ok();
    let mut quota_ancestors = Vec::new();
    if pressure_details {
        for (directory, level) in scope
            .directories
            .iter()
            .zip(&limits.levels)
            .take(scope.directories.len().saturating_sub(1))
        {
            let Some(limit @ CpuLimit { quota: Some(quota), .. }) = level.cpu else {
                continue;
            };
            let stat = resource_text_at(directory, "cpu.stat").ok();
            quota_ancestors.push(CpuQuotaSample {
                identity: file_identity(directory)?,
                quota,
                period: limit.period,
                // The common thread affinity is no larger than any ancestor's
                // effective cpuset. Using it can overstate shared pressure, but
                // cannot hide a narrower CPU set beneath a larger quota.
                cpu_millis: limit.millis()?.ok_or_else(invalid)?.min(cpus),
                usage_usec: stat.as_deref().and_then(|stat| counter(stat, "usage_usec")),
                throttled_usec: stat.as_deref().and_then(|stat| counter(stat, "throttled_usec")),
            });
        }
    }
    let cpu_stall_usec = pressure(leaf, "cpu.pressure");
    let memory_stall_usec = pressure(leaf, "memory.pressure");
    let io_stall_usec = pressure(leaf, "io.pressure");
    let cpu_cores = pressure_details
        .then(|| read_cores(&proc, &monitored_affinity, &threads).ok())
        .flatten();
    let second_memory = resource_text_at(&proc, "meminfo")?;
    let second_total = host_memory(&second_memory, "MemTotal:")?;
    let second_available = host_memory(&second_memory, "MemAvailable:")?;
    if second_total != total || second_available > second_total {
        return Err(invalid());
    }
    let limits = limits.confirm(read_limits(&scope, cpus, second_total, second_available)?)?;
    let mut identity = std::collections::hash_map::DefaultHasher::new();
    membership.hash(&mut identity);
    scope.leaf.hash(&mut identity);
    for directory in &scope.directories {
        file_identity(directory)?.hash(&mut identity);
    }
    let sample = ResourceSample {
        cpu_millis: limits.cpu_millis,
        memory_limit: limits.memory_limit,
        memory_headroom: limits.memory_headroom,
        cpu_usage_usec: stat.as_deref().and_then(|stat| counter(stat, "usage_usec")),
        cpu_throttled_usec: stat.as_deref().and_then(|stat| counter(stat, "throttled_usec")),
        cpu_stall_usec,
        memory_stall_usec,
        io_stall_usec,
        identity: identity.finish(),
    };
    if resource_text_at(&process, "cgroup")? != membership
        || bounded_file(open_relative(&process, OsStr::new("mountinfo"), false)?, 4 * 1024 * 1024)? != mounts
        || allowed_cpus(&resource_text_at(&process, "status")?)? != allowed
        || matching_threads(&tasks, pid, &membership, allowed)? != threads
        || !contains_process(leaf, pid)
        || file_identity(leaf)? != directory_identity
    {
        return Err(invalid());
    }
    let current_proc = open_proc(proc_root)?;
    let current_process = open_proc_process(&current_proc, pid)?;
    if file_identity(&current_proc)? != proc_identity
        || file_identity(&current_process)? != process_identity
        || file_identity(&open_relative(&current_process, OsStr::new("task"), true)?)? != tasks_identity
    {
        return Err(invalid());
    }
    scope.revalidate(open_root)?;
    Ok((sample, quota_ancestors, cpu_cores))
}

fn file_identity(directory: &fs::File) -> io::Result<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    let metadata = directory.metadata()?;
    if !metadata.is_dir() {
        return Err(invalid());
    }
    Ok((metadata.dev(), metadata.ino()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct Fixture {
        root: PathBuf,
        proc: PathBuf,
        mount: PathBuf,
        leaf: PathBuf,
    }

    impl Fixture {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let root = std::env::temp_dir().join(format!(
                "rustfs-resource-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&root).expect("unique fixture directory");
            let proc = root.join("proc");
            let mount = root.join("cgroup space");
            let leaf = mount.join("parent/worker");
            fs::create_dir_all(proc.join("42/task/42")).expect("leader task fixture");
            std::os::unix::fs::symlink("42", proc.join("self")).expect("process link fixture");
            fs::create_dir_all(&leaf).expect("cgroup fixture");
            for directory in [proc.join("self"), proc.join("self/task/42")] {
                fs::write(directory.join("cgroup"), "0::/parent/worker\n").expect("membership");
                fs::write(directory.join("status"), "Cpus_allowed_list:\t0-3,8-11\n").expect("effective affinity");
            }
            fs::write(proc.join("meminfo"), "MemTotal: 8388608 kB\nMemAvailable: 4194304 kB\n").expect("host memory");
            fs::write(leaf.join("cgroup.procs"), "42\n").expect("process identity");
            fs::write(leaf.join("cpu.stat"), "usage_usec 15000\nthrottled_usec 30\n").expect("cpu stats");
            for name in ["cpu.pressure", "memory.pressure", "io.pressure"] {
                fs::write(
                    leaf.join(name),
                    "some avg10=0.00 avg60=0.00 avg300=0.00 total=7\nfull avg10=0.00 total=1\n",
                )
                .expect("pressure stats");
            }
            let fixture = Self { root, proc, mount, leaf };
            fixture.mountinfo("/");
            fixture.limits("parent", "400000 100000", "3221225472", "2147483648");
            fixture.limits("parent/worker", "200000 100000", "max", "1073741824");
            fixture
        }

        fn mountinfo(&self, root: &str) {
            let path = self.mount.to_str().expect("fixture path").replace(' ', "\\040");
            fs::write(
                self.proc.join("self/mountinfo"),
                format!("1 0 0:28 {root} {path} rw shared:3 - cgroup2 cgroup rw\n"),
            )
            .expect("mountinfo");
        }

        fn limits(&self, suffix: &str, cpu: &str, memory: &str, used: &str) {
            let path = self.mount.join(suffix);
            fs::write(path.join("cpu.max"), cpu).expect("quota");
            fs::write(path.join("memory.max"), memory).expect("memory limit");
            fs::write(path.join("memory.current"), used).expect("memory usage");
        }

        fn parser_admission_sample(&self) -> io::Result<(ResourceSample, Vec<CpuQuotaSample>, Option<CpuCoreObservation>)> {
            // These files exercise parsing only; a temporary directory is not
            // a kernel cgroup root and cannot authorize production admission.
            sample_at_with_roots(
                &self.proc,
                42,
                &|path| fs::File::open(path),
                &|path| fs::File::open(path),
                true,
                &read_cpu_cores,
            )
        }

        fn parser_sample(&self) -> io::Result<ResourceSample> {
            self.parser_admission_sample().map(|(sample, _, _)| sample)
        }

        fn set_thread_field(&self, name: &str, value: &str) {
            for directory in [self.proc.join("self"), self.proc.join("self/task/42")] {
                fs::write(directory.join(name), value).expect("update process and leader fixture");
            }
        }

        fn sample(&self) -> ResourceSample {
            self.parser_sample().expect("complete parser fixture sample")
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.root).expect("remove this fixture only");
        }
    }

    #[test]
    fn hierarchy_root_proof_requires_cgroup2_and_the_kernel_root_inode() {
        const CGROUP2: u64 = 0x6367_7270;
        assert!(physical_cgroup_root(true, CGROUP2, 1));
        assert!(!physical_cgroup_root(true, CGROUP2, 2), "a namespace root can be a non-root cgroup");
        assert!(!physical_cgroup_root(true, 0, 1), "an inode number alone is not a cgroup proof");
        assert!(!physical_cgroup_root(false, CGROUP2, 1));
    }

    #[test]
    fn per_cpu_parser_uses_the_exact_mask_without_double_counting_guest() {
        let stat = "cpu 1 2 3 4 5 6 7 8 90 91\n\
                    cpu0 1 2 3 4 5 6 7 8 90 91\n\
                    cpu1 11 12 13 14 15 16 17 18 190 191\n\
                    cpu2 21 22 23 24 25 26 27 28 290 291\n";
        let cores = parse_cpu_cores(stat, &[0, 2]).expect("two visible CPUs");
        assert_eq!(cores.len(), 2);
        assert_eq!(
            cores[0],
            CpuCoreSample {
                id: 0,
                counters: [1, 2, 3, 4, 5, 6, 7, 8]
            }
        );
        assert_eq!(
            cores[1],
            CpuCoreSample {
                id: 2,
                counters: [21, 22, 23, 24, 25, 26, 27, 28]
            }
        );
        assert!(parse_cpu_cores(stat, &[0, 3]).is_err(), "an offline or missing CPU is unknown");
        assert!(parse_cpu_cores("cpu0 1 2 3 4 5 6 7\n", &[0]).is_err(), "missing fields are unknown");
        assert!(parse_cpu_cores("cpu0 1 2 3 4 5 6 7 8\ncpu0 1 2 3 4 5 6 7 8\n", &[0]).is_err());
        assert!(allowed_cpu_ids("0-65536").is_err(), "oversized inventories remain unknown");
    }

    #[test]
    fn smt_sibling_outside_affinity_is_observed_and_incomplete_topology_is_unknown() {
        let fixture = Fixture::new();
        let cpus = fixture.root.join("cpus");
        for id in [0, 1] {
            let topology = cpus.join(format!("cpu{id}/topology"));
            fs::create_dir_all(&topology).expect("synthetic CPU topology");
            fs::write(topology.join("thread_siblings_list"), "0-1\n").expect("synthetic sibling set");
        }
        let root = fs::File::open(&cpus).expect("synthetic CPU directory");
        let topology = read_sibling_topology(&root, &[0]).expect("complete sibling topology");
        let monitored: Vec<_> = topology.keys().copied().collect();
        assert_eq!(monitored, [0, 1], "the other SMT thread is outside the affinity mask");
        assert!(parse_cpu_cores("cpu0 0 0 0 100 0 0 0 0\ncpu1 100 0 0 0 0 0 0 0\n", &monitored).is_ok());
        assert!(parse_cpu_cores("cpu0 0 0 0 100 0 0 0 0\n", &monitored).is_err());

        fs::write(cpus.join("cpu1/topology/thread_siblings_list"), "1\n").expect("inconsistent sibling set");
        assert!(read_sibling_topology(&root, &[0]).is_err());
        fs::remove_file(cpus.join("cpu1/topology/thread_siblings_list")).expect("missing sibling topology");
        assert!(read_sibling_topology(&root, &[0]).is_err());
    }

    #[test]
    fn synthetic_proc_files_do_not_authorize_cpu_pressure() {
        let fixture = Fixture::new();
        fs::write(fixture.proc.join("stat"), "cpu0 1 2 3 4 5 6 7 8\n").expect("synthetic proc stat");
        let fake_proc = fs::File::open(&fixture.proc).expect("synthetic proc root");
        assert!(read_cpu_cores(&fake_proc, &[0], &[]).is_err());
        assert!(fixture.parser_admission_sample().expect("parser fixture").2.is_none());
    }

    #[test]
    fn metrics_sample_skips_admission_only_cpu_details() {
        let fixture = Fixture::new();
        let reads = std::cell::Cell::new(0);
        let read_cores = |_: &fs::File, _: &[u32], _: &[ThreadObservation]| -> io::Result<CpuCoreObservation> {
            reads.set(reads.get() + 1);
            Err(invalid())
        };
        let open = |path: &Path| fs::File::open(path);
        let (metrics, ancestors, cores) =
            sample_at_with_roots(&fixture.proc, 42, &open, &open, false, &read_cores).expect("metrics parser fixture");
        assert_eq!(reads.get(), 0, "metrics must not read per-CPU topology");
        assert!(ancestors.is_empty());
        assert!(cores.is_none());

        let (admission, ancestors, cores) =
            sample_at_with_roots(&fixture.proc, 42, &open, &open, true, &read_cores).expect("admission parser fixture");
        assert_eq!(reads.get(), 1, "admission still checks per-CPU topology");
        assert!(!ancestors.is_empty(), "admission still observes finite ancestor quotas");
        assert!(cores.is_none(), "a failed topology read stays unavailable");
        assert_eq!(metrics, admission, "both modes report the same verified base resources");
    }

    #[test]
    fn unproven_mounts_cannot_authorize_a_resource_sample() {
        let fixture = Fixture::new();
        fixture.limits("", "max 100000", "max", "0");
        for root in ["/host-slice", "/"] {
            fixture.mountinfo(root);
            assert!(fixture.parser_sample().is_ok(), "the synthetic files are otherwise readable");
            assert!(
                sample_at(&fixture.proc, 42).is_err(),
                "a synthetic mount or an unproven namespace root must not become resource authority"
            );
            assert!(
                sample_at_with_roots(&fixture.proc, 42, &open_physical_root, &open_proc_root, false, &read_cpu_cores).is_err(),
                "the metrics sample must also report an unproven root as unavailable"
            );
        }
    }

    #[test]
    fn resource_files_stay_with_open_directories_and_replaced_paths_are_rejected() {
        let fixture = Fixture::new();
        let membership = fs::read_to_string(fixture.proc.join("self/cgroup")).expect("membership");
        let mounts = fs::read_to_string(fixture.proc.join("self/mountinfo")).expect("mount table");
        let open_fixture_root = |path: &Path| fs::File::open(path);
        let scope = resolve_scope_with_root(&membership, &mounts, 42, &open_fixture_root).expect("synthetic parser scope");
        assert!(scope.revalidate(&open_fixture_root).is_ok());
        let original = resource_text_at(&scope.directories[1], "memory.max").expect("original ancestor limit");

        fs::rename(fixture.mount.join("parent"), fixture.mount.join("held-parent")).expect("move original ancestor");
        fs::create_dir_all(fixture.mount.join("parent/worker")).expect("replacement path");
        fixture.limits("parent", "max 100000", "max", "0");
        assert_eq!(resource_text_at(&scope.directories[1], "memory.max").expect("pinned ancestor"), original);
        assert!(
            scope.revalidate(&open_fixture_root).is_err(),
            "the sampled path no longer names the opened ancestor"
        );
    }

    #[test]
    fn worker_membership_is_required_and_each_affinity_constrains_the_envelope() {
        let fixture = Fixture::new();
        let worker = fixture.proc.join("self/task/43");
        fs::create_dir(&worker).expect("worker task fixture");
        fs::write(worker.join("cgroup"), "0::/parent/worker\n").expect("matching membership");
        fs::write(worker.join("status"), "Cpus_allowed_list: 0-3,8-11\n").expect("matching affinity");
        assert!(fixture.parser_sample().is_ok(), "consistent leader and worker are observable");

        fs::write(worker.join("cgroup"), "0::/parent/worker/threaded\n").expect("different thread cgroup");
        assert!(fixture.parser_sample().is_err(), "a stricter threaded subgroup is not the leader's scope");
        fs::write(worker.join("cgroup"), "0::/parent/worker\n").expect("restore membership");
        fs::write(worker.join("status"), "Cpus_allowed_list: 12\n").expect("different thread affinity");
        let sample = fixture.parser_sample().expect("different masks can be bounded together");
        assert_eq!(sample.cpu_millis, 1000, "the narrowest worker bounds concurrency");
        let tasks = fs::File::open(fixture.proc.join("42/task")).expect("synthetic task directory");
        let threads = matching_threads(&tasks, 42, "0::/parent/worker\n", "0-3,8-11").expect("thread inventory");
        let (capacity, monitored) = thread_cpu_envelope(&threads).expect("thread envelope");
        assert_eq!(capacity, 1000);
        assert_eq!(monitored, [0, 1, 2, 3, 8, 9, 10, 11, 12], "all thread affinities contribute to pressure");
        fs::remove_file(worker.join("status")).expect("make the thread observation incomplete");
        assert!(fixture.parser_sample().is_err(), "an unreadable thread cannot be ignored");
    }

    #[test]
    fn process_identity_and_thread_files_must_not_be_redirected() {
        let fixture = Fixture::new();
        assert!(fixture.parser_sample().is_ok());
        fs::remove_file(fixture.proc.join("self")).expect("remove process link");
        std::os::unix::fs::symlink("43", fixture.proc.join("self")).expect("mismatched process link");
        assert!(fixture.parser_sample().is_err(), "a different proc PID namespace is not process identity");

        fs::remove_file(fixture.proc.join("self")).expect("remove process link");
        std::os::unix::fs::symlink("42", fixture.proc.join("self")).expect("restore process link");
        let status = fixture.proc.join("42/task/42/status");
        fs::remove_file(&status).expect("remove thread status");
        std::os::unix::fs::symlink("../../status", &status).expect("redirect thread status");
        assert!(fixture.parser_sample().is_err(), "thread observations cannot follow symlinks");
    }

    #[test]
    fn changed_limits_are_unknown_and_headroom_uses_the_more_conservative_read() {
        let fixture = Fixture::new();
        let membership = fs::read_to_string(fixture.proc.join("self/cgroup")).expect("membership");
        let mounts = fs::read_to_string(fixture.proc.join("self/mountinfo")).expect("mount table");
        let scope = resolve_scope_with_root(&membership, &mounts, 42, &|path| fs::File::open(path)).expect("parser scope");
        let first = read_limits(&scope, 8000, 8 * 1024 * 1024 * 1024, 4 * 1024 * 1024 * 1024).expect("initial limits");
        fixture.limits("parent", "50000 100000", "2147483648", "1073741824");
        let changed = read_limits(&scope, 8000, 8 * 1024 * 1024 * 1024, 4 * 1024 * 1024 * 1024).expect("changed limits");
        assert!(
            first.confirm(changed).is_err(),
            "a hot shrink within a sample is not a stable observation"
        );

        let first = read_limits(&scope, 8000, 8 * 1024 * 1024 * 1024, 4 * 1024 * 1024 * 1024).expect("initial usage");
        fixture.limits("parent", "50000 100000", "2147483648", "2147483640");
        let later = read_limits(&scope, 8000, 8 * 1024 * 1024 * 1024, 4 * 1024 * 1024 * 1024).expect("later usage");
        assert_eq!(first.confirm(later).expect("unchanged caps").memory_headroom, 8);
    }

    #[test]
    fn shared_parent_cpu_counters_remain_separate_from_the_idle_leaf() {
        let fixture = Fixture::new();
        fixture.limits("parent", "100000 100000", "3221225472", "2147483648");
        fs::write(fixture.leaf.join("cpu.stat"), "usage_usec 0\nthrottled_usec 0\n").expect("idle leaf");
        fs::write(fixture.mount.join("parent/cpu.stat"), "usage_usec 1000000\nthrottled_usec 0\n").expect("parent counters");
        let (first, ancestors, _) = fixture.parser_admission_sample().expect("first synthetic observation");
        assert_eq!(first.cpu_usage_usec, Some(0));
        assert_eq!(ancestors.len(), 1);
        assert_eq!(
            (ancestors[0].quota, ancestors[0].period, ancestors[0].usage_usec),
            (100000, 100000, Some(1000000))
        );

        fs::write(fixture.mount.join("parent/cpu.stat"), "usage_usec 2000000\nthrottled_usec 20000\n")
            .expect("sibling consumed and throttled the parent");
        let (second, later, _) = fixture.parser_admission_sample().expect("second synthetic observation");
        assert_eq!(second.cpu_usage_usec, Some(0));
        assert_eq!((later[0].usage_usec, later[0].throttled_usec), (Some(2000000), Some(20000)));
        assert_eq!(later[0].identity, ancestors[0].identity);

        fixture.limits("parent", "400000 100000", "3221225472", "2147483648");
        fixture.set_thread_field("status", "Cpus_allowed_list: 0\n");
        let (_, restricted, _) = fixture.parser_admission_sample().expect("restricted affinity observation");
        assert_eq!(restricted[0].quota, 400000);
        assert_eq!(restricted[0].cpu_millis, 1000, "a wider quota cannot hide the thread affinity");
    }

    #[test]
    fn nested_limits_include_parent_usage_and_live_hot_shrink() {
        let fixture = Fixture::new();
        let first = fixture.sample();
        assert_eq!(first.cpu_millis, 2000);
        assert_eq!(first.memory_limit, 3 * 1024 * 1024 * 1024);
        assert_eq!(first.memory_headroom, 1024 * 1024 * 1024);
        assert_eq!(first.cpu_usage_usec, Some(15000));
        assert_eq!(first.cpu_throttled_usec, Some(30));
        assert_eq!(first.io_stall_usec, Some(7));
        // A sibling consumes parent memory; leaf usage alone is insufficient.
        fixture.limits("parent", "50000 100000", "2147483648", "2147483640");
        let second = fixture.sample();
        assert_eq!(second.cpu_millis, 500);
        assert_eq!(second.memory_limit, 2 * 1024 * 1024 * 1024);
        assert_eq!(second.memory_headroom, 8);
        assert_eq!(second.identity, first.identity);
        fixture.set_thread_field("status", "Cpus_allowed_list: 8\n");
        fixture.limits("parent", "max 100000", "max", "0");
        assert_eq!(fixture.sample().cpu_millis, 1000);
    }

    #[test]
    fn bind_mount_resolves_membership_relative_to_mount_root() {
        let fixture = Fixture::new();
        fixture.mountinfo("/host-slice");
        fixture.limits("", "100000 100000", "max", "0");
        fixture.set_thread_field("cgroup", "0::/host-slice/parent/worker\n");
        assert_eq!(fixture.sample().cpu_millis, 1000);
    }

    #[test]
    fn cgroup_namespace_uses_verified_relative_membership() {
        let fixture = Fixture::new();
        fixture.mountinfo("/host-slice");
        fixture.limits("", "max 100000", "max", "0");
        assert_eq!(fixture.sample().cpu_millis, 2000);
        fs::write(fixture.leaf.join("cgroup.procs"), "41\n").expect("wrong process");
        assert!(fixture.parser_sample().is_err());
    }

    #[test]
    fn missing_or_malformed_limits_are_not_host_fallback() {
        let fixture = Fixture::new();
        for value in ["", "max", "200000 0", "100000 100000 extra", "-1 100000", "0 100000"] {
            fs::write(fixture.leaf.join("cpu.max"), value).expect("malformed quota");
            assert!(fixture.parser_sample().is_err(), "accepted {value}");
        }
        fixture.limits("parent/worker", "max 100000", "max", "0");
        fs::remove_file(fixture.mount.join("parent/memory.max")).expect("missing ancestor cap");
        assert!(fixture.parser_sample().is_err());
    }

    #[test]
    fn absent_pressure_remains_unknown_and_memory_exhaustion_is_zero() {
        let fixture = Fixture::new();
        fs::remove_file(fixture.leaf.join("io.pressure")).expect("missing PSI");
        fs::write(fixture.leaf.join("cpu.stat"), "usage_usec 0\n").expect("missing throttle field");
        fixture.limits("parent", "1 100000", "1", "2");
        let sample = fixture.sample();
        assert_eq!(sample.cpu_millis, 0);
        assert_eq!(sample.memory_headroom, 0);
        assert_eq!(sample.cpu_usage_usec, Some(0));
        assert_eq!(sample.cpu_throttled_usec, None);
        assert_eq!(sample.io_stall_usec, None);
    }

    #[test]
    fn replacement_identity_and_migration_are_observable() {
        let fixture = Fixture::new();
        let before = fixture.sample().identity;
        let old = fixture.mount.join("parent/old-worker");
        fs::rename(&fixture.leaf, &old).expect("replace cgroup");
        fs::create_dir(&fixture.leaf).expect("replacement directory");
        fixture.limits("parent/worker", "max 100000", "max", "0");
        fs::write(fixture.leaf.join("cgroup.procs"), "42\n").expect("replacement membership");
        assert_ne!(fixture.sample().identity, before);
        fixture.set_thread_field("cgroup", "0::/parent/old-worker\n");
        assert_ne!(fixture.sample().identity, before);
    }

    #[test]
    fn cpu_lists_and_mount_paths_reject_ambiguous_or_overflow_inputs() {
        assert_eq!(cpu_count("0-3,8,10-12").expect("disjoint cpus"), 8);
        for value in [
            "",
            "2-1",
            "1,1",
            "0-3,2-4",
            "18446744073709551615-18446744073709551615,0",
            "0-18446744073709551615",
        ] {
            assert!(cpu_count(value).is_err(), "accepted {value}");
        }
        for value in ["relative", "/a/../b", "/a/./b", "/bad\\000path", "/truncated\\04"] {
            assert!(mount_path(value).is_err(), "accepted {value}");
        }
        assert_eq!(mount_path("/a\\040b/\\134").expect("escapes"), Path::new("/a b/\\"));
    }

    #[test]
    fn invisible_or_duplicate_membership_is_not_guessed() {
        let fixture = Fixture::new();
        let mounts = fs::read_to_string(fixture.proc.join("self/mountinfo")).expect("mounts");
        for membership in [
            "0::/../hidden\n",
            "0::/parent/worker\n0::/parent/worker\n",
            "1:cpu:/parent/worker\n",
        ] {
            assert!(resolve_scope_with_root(membership, &mounts, 42, &|path| fs::File::open(path)).is_err());
        }
    }

    #[test]
    fn oversized_required_resource_files_fail_closed() {
        for relative in ["cpu.max", "memory.max", "memory.current"] {
            let fixture = Fixture::new();
            fixture.limits("parent/worker", "200000 100000", "2147483648", "1073741824");
            let path = fixture.leaf.join(relative);
            let original = fs::read_to_string(&path).expect("resource value");
            fs::write(path, format!("{original}{}", " ".repeat(64 * 1024))).expect("oversized resource");
            assert!(fixture.parser_sample().is_err(), "accepted oversized {relative}");
        }
    }

    #[test]
    fn oversized_optional_counters_remain_unknown() {
        let fixture = Fixture::new();
        let padding = " ".repeat(64 * 1024);
        fs::write(fixture.leaf.join("cpu.stat"), format!("usage_usec 1\nthrottled_usec 2\n{padding}")).expect("large stats");
        fs::write(fixture.leaf.join("io.pressure"), format!("some total=3\n{padding}")).expect("large PSI");
        let sample = fixture.sample();
        assert_eq!(sample.cpu_usage_usec, None);
        assert_eq!(sample.cpu_throttled_usec, None);
        assert_eq!(sample.io_stall_usec, None);
    }

    #[test]
    fn oversized_membership_and_mount_tables_are_not_partially_accepted() {
        let fixture = Fixture::new();
        let path = fixture.proc.join("self/mountinfo");
        let original = fs::read_to_string(&path).expect("mount table");
        fs::write(&path, format!("{original}{}", "\n".repeat(4 * 1024 * 1024))).expect("large mount table");
        assert!(fixture.parser_sample().is_err());
        fs::write(path, original).expect("restore mount table");
        fs::write(fixture.leaf.join("cgroup.procs"), format!("42\n{}", "1\n".repeat(512 * 1024))).expect("large PID table");
        assert!(fixture.parser_sample().is_err());
    }
}
