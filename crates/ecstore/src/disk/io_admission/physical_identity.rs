// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

const MAX_TOPOLOGY_NODES: usize = 256;
const MAX_TOPOLOGY_DEPTH: usize = 32;
const MAX_ATTRIBUTE_BYTES: u64 = 128;

/// Kernel device numbers can be reused after removal. `diskseq` distinguishes
/// those incarnations; this identity does not authorize storage mutations.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct PhysicalDeviceIdentity {
    pub(super) major: u32,
    pub(super) minor: u32,
    pub(super) generation: u64,
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

pub(super) fn attribute(path: &Path) -> io::Result<String> {
    let mut value = String::new();
    File::open(path)?.take(MAX_ATTRIBUTE_BYTES + 1).read_to_string(&mut value)?;
    if value.len() > MAX_ATTRIBUTE_BYTES as usize {
        return Err(invalid("physical device attribute exceeds its bound"));
    }
    Ok(value.trim().to_owned())
}

fn number(value: &str) -> io::Result<u64> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid("invalid physical device number"));
    }
    value.parse().map_err(|_| invalid("physical device number overflow"))
}

fn identity(path: &Path) -> io::Result<PhysicalDeviceIdentity> {
    let generation = number(&attribute(&path.join("diskseq"))?)?;
    if generation == 0 {
        return Err(invalid("physical device generation is missing"));
    }
    let device = attribute(&path.join("dev"))?;
    let (major, minor) = device
        .split_once(':')
        .ok_or_else(|| invalid("invalid physical device identity"))?;
    let major = u32::try_from(number(major)?).map_err(|_| invalid("physical major number overflow"))?;
    let minor = u32::try_from(number(minor)?).map_err(|_| invalid("physical minor number overflow"))?;
    if number(&attribute(&path.join("diskseq"))?)? != generation {
        return Err(invalid("physical device changed during discovery"));
    }
    Ok(PhysicalDeviceIdentity {
        major,
        minor,
        generation,
    })
}

#[derive(Debug, Eq, PartialEq)]
enum NodeTopology {
    Partition {
        device: String,
        number: u64,
        parent: PathBuf,
    },
    Whole {
        identity: PhysicalDeviceIdentity,
        children: Vec<PathBuf>,
    },
}

fn canonical_beneath(path: &Path, root: &Path) -> io::Result<PathBuf> {
    let path = path.canonicalize()?;
    if !path.starts_with(root) {
        return Err(invalid("physical device topology escapes sysfs"));
    }
    Ok(path)
}

fn node_topology(path: &Path, root: &Path) -> io::Result<NodeTopology> {
    if canonical_beneath(path, root)? != path {
        return Err(invalid("physical device path changed during discovery"));
    }
    match attribute(&path.join("partition")) {
        Ok(partition) => {
            let number = number(&partition)?;
            if number == 0 {
                return Err(invalid("physical partition number is missing"));
            }
            let parent = path.parent().ok_or_else(|| invalid("physical partition has no parent"))?;
            return Ok(NodeTopology::Partition {
                device: attribute(&path.join("dev"))?,
                number,
                parent: canonical_beneath(parent, root)?,
            });
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let identity = identity(path)?;
    let mut children = Vec::new();
    for entry in std::fs::read_dir(path.join("slaves"))? {
        if children.len() >= MAX_TOPOLOGY_NODES {
            return Err(invalid("physical device topology fanout exceeds its bound"));
        }
        children.push(canonical_beneath(&entry?.path(), root)?);
    }
    children.sort_unstable();
    children.dedup();
    Ok(NodeTopology::Whole { identity, children })
}

struct TopologySnapshot {
    root: PathBuf,
    entry: PathBuf,
    first: PathBuf,
    nodes: BTreeMap<PathBuf, NodeTopology>,
    leaves: BTreeSet<PhysicalDeviceIdentity>,
}

impl TopologySnapshot {
    fn discover(sysfs_root: &Path, major: u64, minor: u64) -> io::Result<Self> {
        let root = sysfs_root.canonicalize()?;
        let entry = root.join(format!("dev/block/{major}:{minor}"));
        let first = canonical_beneath(&entry, &root)?;
        if attribute(&first.join("dev"))? != format!("{major}:{minor}") {
            return Err(invalid("physical device entry does not match its device number"));
        }
        let mut stack = vec![(first.clone(), 0, false)];
        let mut active = BTreeSet::new();
        let mut nodes = BTreeMap::new();
        let mut leaves = BTreeMap::<PhysicalDeviceIdentity, PathBuf>::new();
        while let Some((path, depth, exiting)) = stack.pop() {
            if exiting {
                active.remove(&path);
                continue;
            }
            if depth > MAX_TOPOLOGY_DEPTH {
                return Err(invalid("physical device topology is too deep"));
            }
            if active.contains(&path) {
                return Err(invalid("physical device topology contains a cycle"));
            }
            if nodes.contains_key(&path) {
                continue;
            }
            if nodes.len() >= MAX_TOPOLOGY_NODES {
                return Err(invalid("physical device topology exceeds its node bound"));
            }
            let topology = node_topology(&path, &root)?;
            active.insert(path.clone());
            stack.push((path.clone(), depth, true));
            match &topology {
                NodeTopology::Partition { parent, .. } => stack.push((parent.clone(), depth + 1, false)),
                NodeTopology::Whole { identity, children } => {
                    if stack.len() + children.len() > MAX_TOPOLOGY_NODES * 2 {
                        return Err(invalid("physical device topology fanout exceeds its bound"));
                    }
                    for child in children {
                        stack.push((child.clone(), depth + 1, false));
                    }
                    if children.is_empty()
                        && let Some(previous) = leaves.insert(*identity, path.clone())
                        && previous != path
                    {
                        return Err(invalid("physical device identity is ambiguous"));
                    }
                }
            }
            nodes.insert(path, topology);
        }
        if leaves.is_empty() {
            return Err(invalid("physical device topology has no leaves"));
        }
        Ok(Self {
            root,
            entry,
            first,
            nodes,
            leaves: leaves.into_keys().collect(),
        })
    }

    fn validate(&self) -> io::Result<()> {
        if canonical_beneath(&self.entry, &self.root)? != self.first {
            return Err(invalid("physical device entry changed during discovery"));
        }
        for (path, expected) in &self.nodes {
            if &node_topology(path, &self.root)? != expected {
                return Err(invalid("physical device topology changed during discovery"));
            }
        }
        Ok(())
    }
}

/// Resolve a bounded sysfs topology once, before installing an admission owner.
/// Missing topology or generation evidence is unknown, never a name-based owner.
/// Validate intermediate mappings too: stable leaves alone do not prove that a
/// device-mapper table still points to those leaves. This is an observation, not
/// a kernel topology lock; the admission owner must expire and refresh it.
pub(super) fn resolve_physical_devices(sysfs_root: &Path, major: u64, minor: u64) -> io::Result<Vec<PhysicalDeviceIdentity>> {
    let snapshot = TopologySnapshot::discover(sysfs_root, major, minor)?;
    snapshot.validate()?;
    Ok(snapshot.leaves.into_iter().collect())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct Fixture(PathBuf);

    impl Fixture {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let stamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos();
            let root = std::env::temp_dir().join(format!(
                "physical-identity-{}-{stamp}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&root).expect("exclusive test root");
            std::fs::create_dir_all(root.join("sys/dev/block")).expect("sysfs fixture");
            Self(root)
        }

        fn root(&self) -> PathBuf {
            self.0.join("sys")
        }

        fn device(&self, name: &str, major: u32, minor: u32, generation: u64) -> PathBuf {
            let path = self.root().join("devices").join(name);
            std::fs::create_dir_all(path.join("slaves")).expect("device fixture");
            std::fs::write(path.join("dev"), format!("{major}:{minor}\n")).expect("device number");
            std::fs::write(path.join("diskseq"), format!("{generation}\n")).expect("device generation");
            symlink(&path, self.root().join(format!("dev/block/{major}:{minor}"))).expect("sysfs link");
            path
        }

        fn link(&self, parent: &Path, child: &Path, name: &str) {
            symlink(child, parent.join("slaves").join(name)).expect("slave link");
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).expect("remove owned fixture");
        }
    }

    #[test]
    fn partitions_share_the_leaf_generation_and_replacement_changes_identity() {
        let fixture = Fixture::new();
        let leaf = fixture.device("sda", 8, 0, 41);
        let first = fixture.device("sda/sda1", 8, 1, 41);
        let second = fixture.device("sda/sda2", 8, 2, 41);
        std::fs::write(first.join("partition"), "1\n").expect("first partition");
        std::fs::write(second.join("partition"), "2\n").expect("second partition");
        let old = resolve_physical_devices(&fixture.root(), 8, 1).expect("first leaf");
        assert_eq!(old, resolve_physical_devices(&fixture.root(), 8, 2).expect("alias leaf"));
        assert_eq!(
            old,
            vec![PhysicalDeviceIdentity {
                major: 8,
                minor: 0,
                generation: 41
            }]
        );
        std::fs::write(leaf.join("diskseq"), "42\n").expect("replacement generation");
        let new = resolve_physical_devices(&fixture.root(), 8, 1).expect("replacement leaf");
        assert_ne!(old, new);
        assert_eq!(new[0].generation, 42);
    }

    #[test]
    fn overlapping_stacks_deduplicate_shared_physical_leaves() {
        let fixture = Fixture::new();
        let a = fixture.device("a", 8, 0, 1);
        let b = fixture.device("b", 8, 16, 2);
        let inner = fixture.device("dm-0", 253, 0, 3);
        let outer = fixture.device("dm-1", 253, 1, 4);
        fixture.link(&inner, &a, "a");
        fixture.link(&inner, &b, "b");
        fixture.link(&outer, &inner, "dm-0");
        fixture.link(&outer, &a, "a-again");
        assert_eq!(
            resolve_physical_devices(&fixture.root(), 253, 0).expect("inner"),
            resolve_physical_devices(&fixture.root(), 253, 1).expect("outer")
        );
        assert_eq!(resolve_physical_devices(&fixture.root(), 253, 1).expect("leaves").len(), 2);
    }

    #[test]
    fn missing_or_invalid_generation_never_falls_back_to_a_device_name() {
        let fixture = Fixture::new();
        let leaf = fixture.device("a", 8, 0, 1);
        for value in ["0", "-1", "18446744073709551616", "+1"] {
            std::fs::write(leaf.join("diskseq"), value).expect("bad generation");
            assert!(resolve_physical_devices(&fixture.root(), 8, 0).is_err(), "accepted {value}");
        }
        std::fs::remove_file(leaf.join("diskseq")).expect("missing generation");
        assert_eq!(
            resolve_physical_devices(&fixture.root(), 8, 0)
                .expect_err("unknown generation")
                .kind(),
            io::ErrorKind::NotFound
        );
        assert!(resolve_physical_devices(&fixture.root(), 99, 99).is_err());
    }

    #[test]
    fn topology_cycles_and_escaping_links_are_rejected() {
        let fixture = Fixture::new();
        let a = fixture.device("a", 8, 0, 1);
        let b = fixture.device("b", 8, 16, 2);
        fixture.link(&a, &b, "b");
        fixture.link(&b, &a, "a");
        assert!(
            resolve_physical_devices(&fixture.root(), 8, 0)
                .expect_err("cycle")
                .to_string()
                .contains("cycle")
        );
        std::fs::remove_file(b.join("slaves/a")).expect("remove cycle edge");
        fixture.link(&b, &fixture.0, "outside");
        assert!(
            resolve_physical_devices(&fixture.root(), 8, 0)
                .expect_err("escape")
                .to_string()
                .contains("escapes")
        );
    }

    #[test]
    fn intermediate_mapping_changes_are_rejected_even_when_old_leaves_survive() {
        let fixture = Fixture::new();
        let a = fixture.device("a", 8, 0, 1);
        let b = fixture.device("b", 8, 16, 2);
        let mapper = fixture.device("dm-0", 253, 0, 3);
        fixture.link(&mapper, &a, "backing");
        let snapshot = TopologySnapshot::discover(&fixture.root(), 253, 0).expect("initial mapping");
        snapshot.validate().expect("unchanged topology");
        std::fs::remove_file(mapper.join("slaves/backing")).expect("replace mapping");
        fixture.link(&mapper, &b, "backing");
        assert_eq!(identity(&a).expect("old leaf still exists").generation, 1);
        assert!(snapshot.validate().is_err(), "stable old leaves cannot certify a changed mapper");
        let current = resolve_physical_devices(&fixture.root(), 253, 0).expect("new complete mapping");
        assert_eq!(current, vec![identity(&b).expect("new leaf")]);
        let snapshot = TopologySnapshot::discover(&fixture.root(), 253, 0).expect("new mapping");
        std::fs::write(mapper.join("diskseq"), "4\n").expect("replace mapper incarnation");
        assert!(
            snapshot.validate().is_err(),
            "an intermediate generation change also invalidates discovery"
        );
    }

    #[test]
    fn entry_and_partition_changes_are_not_hidden_by_an_unchanged_parent() {
        let fixture = Fixture::new();
        fixture.device("a", 8, 0, 1);
        let partition = fixture.device("a/p1", 8, 1, 1);
        std::fs::write(partition.join("partition"), "1\n").expect("partition index");
        let snapshot = TopologySnapshot::discover(&fixture.root(), 8, 1).expect("partition topology");
        std::fs::write(partition.join("partition"), "2\n").expect("changed partition");
        assert!(snapshot.validate().is_err());
        let b = fixture.device("b", 8, 16, 2);
        let snapshot = TopologySnapshot::discover(&fixture.root(), 8, 0).expect("first entry");
        let entry = fixture.root().join("dev/block/8:0");
        std::fs::remove_file(&entry).expect("replace entry");
        symlink(&b, &entry).expect("changed entry target");
        assert!(snapshot.validate().is_err());
        assert!(
            resolve_physical_devices(&fixture.root(), 8, 0).is_err(),
            "entry device numbers must match"
        );
    }

    #[test]
    fn missing_slave_directory_is_unknown_not_a_physical_leaf() {
        let fixture = Fixture::new();
        let mapper = fixture.device("dm-0", 253, 0, 1);
        std::fs::remove_dir(mapper.join("slaves")).expect("missing topology evidence");
        assert!(resolve_physical_devices(&fixture.root(), 253, 0).is_err());
    }

    #[test]
    fn topology_depth_and_attribute_sizes_are_bounded() {
        let fixture = Fixture::new();
        let first = fixture.device("d0", 253, 0, 1);
        let mut previous = first.clone();
        for index in 1..=MAX_TOPOLOGY_DEPTH + 1 {
            let child = fixture.device(&format!("d{index}"), 253, u32::try_from(index).expect("bounded index"), 1);
            fixture.link(&previous, &child, "next");
            previous = child;
        }
        assert!(
            resolve_physical_devices(&fixture.root(), 253, 0)
                .expect_err("depth bound")
                .to_string()
                .contains("deep")
        );
        std::fs::remove_file(first.join("slaves/next")).expect("make first node a leaf");
        std::fs::write(first.join("diskseq"), "1".repeat(129)).expect("oversized attribute");
        assert!(
            resolve_physical_devices(&fixture.root(), 253, 0)
                .expect_err("attribute bound")
                .to_string()
                .contains("attribute")
        );
    }

    #[test]
    fn exact_node_bound_passes_and_one_more_node_is_rejected() {
        let fixture = Fixture::new();
        let root = fixture.device("root", 253, 0, 1);
        for index in 0..MAX_TOPOLOGY_NODES - 1 {
            let leaf = fixture.device(&format!("leaf-{index}"), 8, u32::try_from(index).expect("bounded index"), 2);
            fixture.link(&root, &leaf, &format!("leaf-{index}"));
        }
        assert_eq!(
            resolve_physical_devices(&fixture.root(), 253, 0)
                .expect("exact node bound")
                .len(),
            MAX_TOPOLOGY_NODES - 1
        );
        let extra = fixture.device("extra", 9, 0, 3);
        fixture.link(&root, &extra, "extra");
        assert!(
            resolve_physical_devices(&fixture.root(), 253, 0).is_err(),
            "reject a graph above the node bound"
        );
    }
}
