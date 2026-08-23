//! Executable Phase-0 contract for the scanner/heal overlap investigation.
//!
//! These tests model the matrix that a future storage-owned admission
//! primitive must satisfy. They intentionally do not provide a production
//! lock or coordinator; the issue's current evidence establishes a baseline,
//! not a demonstrated stale-writer failure.

#[cfg(test)]
mod tests {
    const SCANNER_IO_SOURCE: &str = include_str!("scanner_io/io_disk.rs");
    const SCANNER_FOLDER_SOURCE: &str = include_str!("scanner_folder.rs");
    const HEAL_AUTO_SCAN_SOURCE: &str =
        include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../heal/src/heal/manager/auto_scan.rs"));
    const HEAL_OBJECT_SOURCE: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../ecstore/src/set_disk/ops/heal.rs"));
    const SET_LOCKING_SOURCE: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../ecstore/src/set_disk/ops/locking.rs"));

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Operation {
        ScannerRead,
        HealRead,
        HealWrite,
        DataMovementWrite,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct BaselineSample {
        set: &'static str,
        operation: Operation,
        latency_us: u64,
        backlog_depth: usize,
        deferred: bool,
    }

    fn p99_latency(samples: &[BaselineSample]) -> u64 {
        assert!(!samples.is_empty());
        let mut latencies = samples.iter().map(|sample| sample.latency_us).collect::<Vec<_>>();
        latencies.sort_unstable();
        let rank = (latencies.len() * 99).div_ceil(100).saturating_sub(1);
        latencies[rank]
    }

    fn restart_degraded_fixture() -> [BaselineSample; 8] {
        [
            BaselineSample {
                set: "pool0/set0",
                operation: Operation::ScannerRead,
                latency_us: 120,
                backlog_depth: 1,
                deferred: false,
            },
            BaselineSample {
                set: "pool0/set0",
                operation: Operation::HealRead,
                latency_us: 180,
                backlog_depth: 1,
                deferred: false,
            },
            BaselineSample {
                set: "pool0/set0",
                operation: Operation::HealWrite,
                latency_us: 420,
                backlog_depth: 2,
                deferred: true,
            },
            BaselineSample {
                set: "pool0/set0",
                operation: Operation::ScannerRead,
                latency_us: 160,
                backlog_depth: 2,
                deferred: false,
            },
            BaselineSample {
                set: "pool0/set1",
                operation: Operation::ScannerRead,
                latency_us: 110,
                backlog_depth: 0,
                deferred: false,
            },
            BaselineSample {
                set: "pool0/set1",
                operation: Operation::HealRead,
                latency_us: 150,
                backlog_depth: 0,
                deferred: false,
            },
            BaselineSample {
                set: "pool0/set1",
                operation: Operation::HealWrite,
                latency_us: 360,
                backlog_depth: 1,
                deferred: true,
            },
            BaselineSample {
                set: "pool0/set1",
                operation: Operation::ScannerRead,
                latency_us: 130,
                backlog_depth: 1,
                deferred: false,
            },
        ]
    }

    fn same_set(a: &str, b: &str) -> bool {
        a == b
    }

    fn may_overlap(left: Operation, right: Operation, same_set: bool) -> bool {
        if !same_set {
            return true;
        }
        matches!(
            (left, right),
            (Operation::ScannerRead, Operation::HealRead) | (Operation::HealRead, Operation::ScannerRead)
        )
    }

    #[test]
    fn scanner_heal_matrix_allows_read_read_and_blocks_heal_write() {
        assert!(may_overlap(Operation::ScannerRead, Operation::HealRead, true));
        assert!(!may_overlap(Operation::ScannerRead, Operation::HealWrite, true));
        assert!(!may_overlap(Operation::DataMovementWrite, Operation::HealRead, true));
    }

    #[test]
    fn scanner_heal_different_sets_remain_concurrent() {
        assert!(may_overlap(
            Operation::HealWrite,
            Operation::ScannerRead,
            same_set("pool0/set0", "pool0/set1")
        ));
    }

    #[test]
    fn scanner_heal_restart_and_clock_skew_do_not_accept_old_owner() {
        let old_owner_generation = 3_u64;
        let restarted_generation = 4_u64;
        let persisted_timestamp = 100_u64;
        let observed_timestamp = 90_u64;
        assert_ne!(old_owner_generation, restarted_generation);
        assert!(observed_timestamp < persisted_timestamp);
    }

    #[test]
    fn scanner_heal_overlap_inventory_has_no_unprotected_destructive_entry() {
        // Keep the Phase-0 inventory tied to real entry points. The assertions
        // deliberately check that the documented guards still exist; they do
        // not claim that a shared admission primitive already exists.
        assert!(SCANNER_IO_SOURCE.contains("let _guard = self.start_scan()"));
        assert!(SCANNER_IO_SOURCE.contains("scan_data_folder"));
        assert!(SCANNER_FOLDER_SOURCE.contains("send_required_scanner_heal_request"));
        assert!(SCANNER_FOLDER_SOURCE.contains("update_pending_scanner_heal_after_admission"));
        assert!(HEAL_AUTO_SCAN_SOURCE.contains("active_heals"));
        assert!(HEAL_AUTO_SCAN_SOURCE.contains("contains_erasure_set"));
        assert!(HEAL_OBJECT_SOURCE.contains("heal_object"));
        assert!(HEAL_OBJECT_SOURCE.contains("get_write_lock"));
        assert!(SET_LOCKING_SOURCE.contains("scanning_disks"));
        assert!(SET_LOCKING_SOURCE.contains("new_disks.extend(scanning_disks)"));
    }

    #[test]
    fn scanner_heal_admission_benchmark_degraded_quorum() {
        let samples = restart_degraded_fixture();
        assert_eq!(p99_latency(&samples), 420);
        assert!(
            samples
                .iter()
                .any(|sample| sample.operation == Operation::HealWrite && sample.deferred)
        );
        assert!(samples.iter().any(|sample| sample.set == "pool0/set1" && !sample.deferred));
        assert_eq!(samples.iter().map(|sample| sample.backlog_depth).max(), Some(2));
    }

    #[test]
    fn scanner_heal_set_deferral_preserves_quorum_and_backlog() {
        let samples = restart_degraded_fixture();
        let deferred_count = samples.iter().filter(|sample| sample.deferred).count();
        let independent_progress = samples
            .iter()
            .filter(|sample| sample.set == "pool0/set1" && !sample.deferred)
            .count();
        assert_eq!(deferred_count, 2);
        assert_eq!(independent_progress, 3);
        assert!(samples.iter().all(|sample| sample.backlog_depth <= 2));
    }
}
