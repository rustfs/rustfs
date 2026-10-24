// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

//! Live process-cgroup observations for background admission. Sampling belongs
//! on a blocking worker, never on an object or request path. These files are
//! individually sampled; this is not an atomic kernel resource reservation.

use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

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

#[derive(Debug, PartialEq, Eq)]
struct Scope {
    mount: PathBuf,
    leaf: PathBuf,
    hierarchy_root: bool,
}

fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "incomplete process resource observation")
}

fn bounded_text(path: &Path, limit: usize) -> io::Result<String> {
    // procfs reports zero file lengths. Bound the read itself and reject the
    // entire observation on overflow, even if an early prefix looks valid.
    let mut contents = String::new();
    fs::File::open(path)?
        .take(u64::try_from(limit).map_err(|_| invalid())? + 1)
        .read_to_string(&mut contents)?;
    if contents.len() > limit {
        return Err(invalid());
    }
    Ok(contents)
}

fn resource_text(path: &Path) -> io::Result<String> {
    bounded_text(path, 64 * 1024)
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

fn contains_process(directory: &Path, pid: u32) -> bool {
    let Ok(contents) = bounded_text(&directory.join("cgroup.procs"), 1024 * 1024) else {
        return false;
    };
    contents.lines().any(|line| line.trim().parse::<u32>().ok() == Some(pid))
}

fn resolve_scope(membership: &str, mounts: &str, pid: u32) -> io::Result<Scope> {
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
            if !contains_process(&leaf, pid) {
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
                        hierarchy_root: root == Path::new("/"),
                    },
                ));
            }
            break;
        }
    }
    selected.map(|(_, scope)| scope).ok_or_else(invalid)
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

fn quota_millis(value: &str) -> io::Result<Option<u64>> {
    let mut fields = value.split_whitespace();
    let quota = fields.next().ok_or_else(invalid)?;
    let period = fields.next().ok_or_else(invalid)?.parse::<u64>().map_err(|_| invalid())?;
    if period == 0 || fields.next().is_some() {
        return Err(invalid());
    }
    if quota == "max" {
        return Ok(None);
    }
    let quota = quota.parse::<u64>().map_err(|_| invalid())?;
    if quota == 0 {
        return Err(invalid());
    }
    let millis = u128::from(quota) * 1000 / u128::from(period);
    Ok(Some(u64::try_from(millis).map_err(|_| invalid())?))
}

fn memory_limit(value: &str) -> io::Result<Option<u64>> {
    if value.trim() == "max" {
        return Ok(None);
    }
    value.trim().parse::<u64>().map(Some).map_err(|_| invalid())
}

fn scalar(path: &Path) -> io::Result<u64> {
    resource_text(path)?.trim().parse::<u64>().map_err(|_| invalid())
}

fn counter(contents: &str, key: &str) -> Option<u64> {
    let mut values = contents.lines().filter_map(|line| {
        let (name, value) = line.split_once(' ')?;
        (name == key).then_some(value)
    });
    let value = values.next()?.trim().parse().ok()?;
    values.next().is_none().then_some(value)
}

fn pressure(path: &Path) -> Option<u64> {
    let contents = resource_text(path).ok()?;
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

fn read_limits(scope: &Scope, mut cpus: u64, mut memory: u64, mut headroom: u64) -> io::Result<(u64, u64, u64)> {
    for directory in scope.leaf.ancestors() {
        if !directory.starts_with(&scope.mount) {
            return Err(invalid());
        }
        let root = directory == scope.mount && scope.hierarchy_root;
        match resource_text(&directory.join("cpu.max")) {
            Ok(value) => {
                if let Some(limit) = quota_millis(&value)? {
                    cpus = cpus.min(limit);
                }
            }
            Err(error) if root && error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        match resource_text(&directory.join("memory.max")) {
            Ok(value) => {
                if let Some(limit) = memory_limit(&value)? {
                    memory = memory.min(limit);
                    // Parent usage includes siblings. Subtracting just our leaf's
                    // usage would overstate the capacity available to this process.
                    headroom = headroom.min(limit.saturating_sub(scalar(&directory.join("memory.current"))?));
                }
            }
            Err(error) if root && error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        if directory == scope.mount {
            return Ok((cpus, memory, headroom));
        }
    }
    Err(invalid())
}

pub(crate) fn sample() -> io::Result<ResourceSample> {
    sample_at(Path::new("/proc"), std::process::id())
}

fn sample_at(proc_root: &Path, pid: u32) -> io::Result<ResourceSample> {
    use std::hash::{Hash, Hasher};
    let process = proc_root.join("self");
    let membership = resource_text(&process.join("cgroup"))?;
    let mounts = bounded_text(&process.join("mountinfo"), 4 * 1024 * 1024)?;
    let scope = resolve_scope(&membership, &mounts, pid)?;
    let directory_identity = directory_identity(&scope.leaf)?;
    let status = resource_text(&process.join("status"))?;
    let allowed = status
        .lines()
        .find_map(|line| line.strip_prefix("Cpus_allowed_list:"))
        .ok_or_else(invalid)?;
    // Linux's effective affinity mask already includes the task's cpuset.
    let cpus = cpu_count(allowed)?.checked_mul(1000).ok_or_else(invalid)?;
    let memory = resource_text(&proc_root.join("meminfo"))?;
    let total = host_memory(&memory, "MemTotal:")?;
    let available = host_memory(&memory, "MemAvailable:")?;
    if available > total {
        return Err(invalid());
    }
    let (cpu_millis, memory_limit, memory_headroom) = read_limits(&scope, cpus, total, available)?;
    let stat = resource_text(&scope.leaf.join("cpu.stat")).ok();
    let mut identity = std::collections::hash_map::DefaultHasher::new();
    membership.hash(&mut identity);
    scope.leaf.hash(&mut identity);
    directory_identity.hash(&mut identity);
    let sample = ResourceSample {
        cpu_millis,
        memory_limit,
        memory_headroom,
        cpu_usage_usec: stat.as_deref().and_then(|stat| counter(stat, "usage_usec")),
        cpu_throttled_usec: stat.as_deref().and_then(|stat| counter(stat, "throttled_usec")),
        cpu_stall_usec: pressure(&scope.leaf.join("cpu.pressure")),
        memory_stall_usec: pressure(&scope.leaf.join("memory.pressure")),
        io_stall_usec: pressure(&scope.leaf.join("io.pressure")),
        identity: identity.finish(),
    };
    if resource_text(&process.join("cgroup"))? != membership
        || bounded_text(&process.join("mountinfo"), 4 * 1024 * 1024)? != mounts
        || !contains_process(&scope.leaf, pid)
        || self::directory_identity(&scope.leaf)? != directory_identity
    {
        return Err(invalid());
    }
    Ok(sample)
}

fn directory_identity(path: &Path) -> io::Result<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    let metadata = fs::metadata(path)?;
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
            fs::create_dir_all(proc.join("self")).expect("proc fixture");
            fs::create_dir_all(&leaf).expect("cgroup fixture");
            fs::write(proc.join("self/cgroup"), "0::/parent/worker\n").expect("membership");
            fs::write(proc.join("self/status"), "Cpus_allowed_list:\t0-3,8-11\n").expect("effective affinity");
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

        fn sample(&self) -> ResourceSample {
            sample_at(&self.proc, 42).expect("complete process resource sample")
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.root).expect("remove this fixture only");
        }
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
        fs::write(fixture.proc.join("self/status"), "Cpus_allowed_list: 8\n").expect("cpuset shrink");
        fixture.limits("parent", "max 100000", "max", "0");
        assert_eq!(fixture.sample().cpu_millis, 1000);
    }

    #[test]
    fn bind_mount_resolves_membership_relative_to_mount_root() {
        let fixture = Fixture::new();
        fixture.mountinfo("/host-slice");
        fixture.limits("", "100000 100000", "max", "0");
        fs::write(fixture.proc.join("self/cgroup"), "0::/host-slice/parent/worker\n").expect("bind membership");
        assert_eq!(fixture.sample().cpu_millis, 1000);
    }

    #[test]
    fn cgroup_namespace_uses_verified_relative_membership() {
        let fixture = Fixture::new();
        fixture.mountinfo("/host-slice");
        fixture.limits("", "max 100000", "max", "0");
        assert_eq!(fixture.sample().cpu_millis, 2000);
        fs::write(fixture.leaf.join("cgroup.procs"), "41\n").expect("wrong process");
        assert!(sample_at(&fixture.proc, 42).is_err());
    }

    #[test]
    fn missing_or_malformed_limits_are_not_host_fallback() {
        let fixture = Fixture::new();
        for value in ["", "max", "200000 0", "100000 100000 extra", "-1 100000", "0 100000"] {
            fs::write(fixture.leaf.join("cpu.max"), value).expect("malformed quota");
            assert!(sample_at(&fixture.proc, 42).is_err(), "accepted {value}");
        }
        fixture.limits("parent/worker", "max 100000", "max", "0");
        fs::remove_file(fixture.mount.join("parent/memory.max")).expect("missing ancestor cap");
        assert!(sample_at(&fixture.proc, 42).is_err());
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
        fs::write(fixture.proc.join("self/cgroup"), "0::/parent/old-worker\n").expect("migrate process");
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
            assert!(resolve_scope(membership, &mounts, 42).is_err());
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
            assert!(sample_at(&fixture.proc, 42).is_err(), "accepted oversized {relative}");
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
        assert!(sample_at(&fixture.proc, 42).is_err());
        fs::write(path, original).expect("restore mount table");
        fs::write(fixture.leaf.join("cgroup.procs"), format!("42\n{}", "1\n".repeat(512 * 1024))).expect("large PID table");
        assert!(sample_at(&fixture.proc, 42).is_err());
    }
}
