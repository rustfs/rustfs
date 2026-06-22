use crate::layout::format::{DistributionAlgoVersion, FormatV3};
use std::collections::HashSet;
use std::io::{Error, Result};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StaticSetLayoutSnapshot {
    pub(crate) deployment_id: Uuid,
    pub(crate) set_count: usize,
    pub(crate) drives_per_set: usize,
    pub(crate) disk_ids: Vec<Vec<Uuid>>,
    pub(crate) distribution_algo: DistributionAlgoVersion,
}

impl StaticSetLayoutSnapshot {
    pub(crate) fn from_format(format: &FormatV3) -> Self {
        let disk_ids = format.erasure.sets.clone();

        Self {
            deployment_id: format.id,
            set_count: disk_ids.len(),
            drives_per_set: disk_ids.first().map_or(0, Vec::len),
            disk_ids,
            distribution_algo: format.erasure.distribution_algo.clone(),
        }
    }

    pub(crate) fn disk_position(&self, disk_id: Uuid) -> Option<SetDiskPosition> {
        for (set_index, set) in self.disk_ids.iter().enumerate() {
            for (disk_index, id) in set.iter().enumerate() {
                if *id == disk_id {
                    return Some(SetDiskPosition { set_index, disk_index });
                }
            }
        }

        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SetDiskPosition {
    pub(crate) set_index: usize,
    pub(crate) disk_index: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeSetLayoutPlan {
    pub(crate) sets: Vec<Vec<RuntimeSetDrivePlan>>,
    lock_hosts_by_set: Vec<Vec<String>>,
}

impl RuntimeSetLayoutPlan {
    pub(crate) fn from_endpoint_hosts<S>(set_count: usize, drives_per_set: usize, endpoint_hosts: &[S]) -> Result<Self>
    where
        S: AsRef<str>,
    {
        if set_count == 0 || drives_per_set == 0 {
            return Err(Error::other("set count and drive count must be non-zero"));
        }

        let expected = set_count
            .checked_mul(drives_per_set)
            .ok_or_else(|| Error::other("set layout size overflow"))?;
        if endpoint_hosts.len() != expected {
            return Err(Error::other(format!(
                "endpoint host count {} does not match set layout {}x{}",
                endpoint_hosts.len(),
                set_count,
                drives_per_set
            )));
        }

        let mut sets = Vec::with_capacity(set_count);
        let mut lock_hosts_by_set = Vec::with_capacity(set_count);

        for set_index in 0..set_count {
            let mut drives = Vec::with_capacity(drives_per_set);
            let mut seen_hosts = HashSet::with_capacity(drives_per_set);
            let mut lock_hosts = Vec::with_capacity(drives_per_set);

            for disk_index in 0..drives_per_set {
                let flat_disk_index = set_index * drives_per_set + disk_index;
                let endpoint_host = endpoint_hosts[flat_disk_index].as_ref().to_owned();

                if seen_hosts.insert(endpoint_host.clone()) {
                    lock_hosts.push(endpoint_host.clone());
                }

                drives.push(RuntimeSetDrivePlan {
                    set_index,
                    disk_index,
                    flat_disk_index,
                    endpoint_host,
                });
            }

            sets.push(drives);
            lock_hosts_by_set.push(lock_hosts);
        }

        Ok(Self { sets, lock_hosts_by_set })
    }

    pub(crate) fn lock_hosts_for_set(&self, set_index: usize) -> Option<&[String]> {
        self.lock_hosts_by_set.get(set_index).map(Vec::as_slice)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeSetDrivePlan {
    pub(crate) set_index: usize,
    pub(crate) disk_index: usize,
    pub(crate) flat_disk_index: usize,
    pub(crate) endpoint_host: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_eset_001_static_layout_snapshot_preserves_format_distribution() {
        let format = FormatV3::new(2, 4);
        let snapshot = StaticSetLayoutSnapshot::from_format(&format);

        assert_eq!(snapshot.deployment_id, format.id);
        assert_eq!(snapshot.set_count, 2);
        assert_eq!(snapshot.drives_per_set, 4);
        assert_eq!(snapshot.disk_ids, format.erasure.sets);
        assert_eq!(snapshot.distribution_algo, format.erasure.distribution_algo);

        let first_disk_id = format.erasure.sets[0][0];
        let last_disk_id = format.erasure.sets[1][3];
        assert_eq!(
            snapshot.disk_position(first_disk_id),
            Some(SetDiskPosition {
                set_index: 0,
                disk_index: 0,
            })
        );
        assert_eq!(
            snapshot.disk_position(last_disk_id),
            Some(SetDiskPosition {
                set_index: 1,
                disk_index: 3,
            })
        );
        assert_eq!(snapshot.disk_position(Uuid::new_v4()), None);
    }

    #[test]
    fn test_eset_002_runtime_plan_preserves_flat_disk_and_lock_host_mapping() {
        let endpoint_hosts = [
            "node-a:9000",
            "node-a:9000",
            "node-b:9000",
            "node-c:9000",
            "node-d:9000",
            "node-d:9000",
        ];

        let plan = RuntimeSetLayoutPlan::from_endpoint_hosts(2, 3, &endpoint_hosts)
            .expect("valid endpoint host count should build a runtime set layout plan");

        assert_eq!(plan.sets.len(), 2);
        assert_eq!(plan.sets[0].len(), 3);
        assert_eq!(plan.sets[0][0].flat_disk_index, 0);
        assert_eq!(plan.sets[0][2].flat_disk_index, 2);
        assert_eq!(plan.sets[1][0].flat_disk_index, 3);
        assert_eq!(plan.sets[1][2].set_index, 1);
        assert_eq!(plan.sets[1][2].disk_index, 2);
        assert_eq!(plan.sets[1][2].endpoint_host, "node-d:9000");

        let first_set_hosts = vec!["node-a:9000".to_owned(), "node-b:9000".to_owned()];
        let second_set_hosts = vec!["node-c:9000".to_owned(), "node-d:9000".to_owned()];
        assert_eq!(plan.lock_hosts_for_set(0), Some(first_set_hosts.as_slice()));
        assert_eq!(plan.lock_hosts_for_set(1), Some(second_set_hosts.as_slice()));
        assert_eq!(plan.lock_hosts_for_set(2), None);
        assert!(RuntimeSetLayoutPlan::from_endpoint_hosts(2, 3, &endpoint_hosts[..5]).is_err());
    }
}
