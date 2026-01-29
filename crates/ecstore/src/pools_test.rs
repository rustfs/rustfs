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

#[cfg(test)]
mod capacity_dedup_tests {
    use crate::pools::{
        fallback_free_capacity_dedup, fallback_total_capacity_dedup, get_total_usable_capacity, get_total_usable_capacity_free,
    };

    #[test]
    fn test_single_disk_no_duplication() {
        let disks = vec![rustfs_madmin::Disk {
            endpoint: "node1".to_string(),
            drive_path: "/mnt/disk1".to_string(),
            pool_index: 0,
            set_index: 0,
            disk_index: 0,
            total_space: 2_000_000_000_000, // 2TB
            available_space: 500_000_000_000,
            used_space: 1_500_000_000_000,
            state: "ok".to_string(),
            ..Default::default()
        }];

        let info = rustfs_madmin::StorageInfo {
            backend: rustfs_madmin::BackendInfo {
                standard_sc_data: vec![1],
                ..Default::default()
            },
            disks: disks.clone(),
        };

        let total = get_total_usable_capacity(&disks, &info);
        let free = get_total_usable_capacity_free(&disks, &info);

        assert_eq!(total, 2_000_000_000_000, "Total capacity should be 2TB");
        assert_eq!(free, 500_000_000_000, "Free capacity should be 500GB");
    }

    #[test]
    fn test_duplicate_disk_entries_deduped() {
        // Simulate the same disk appearing 232 times
        let mut disks = Vec::new();
        for _ in 0..232 {
            disks.push(rustfs_madmin::Disk {
                endpoint: "node1".to_string(),
                drive_path: "/mnt/disk1".to_string(),
                pool_index: 0,
                set_index: 0,
                disk_index: 0,
                total_space: 2_000_000_000_000,
                available_space: 500_000_000_000,
                used_space: 1_500_000_000_000,
                state: "ok".to_string(),
                ..Default::default()
            });
        }

        let info = rustfs_madmin::StorageInfo {
            backend: rustfs_madmin::BackendInfo {
                standard_sc_data: vec![1],
                ..Default::default()
            },
            disks: disks.clone(),
        };

        let total = get_total_usable_capacity(&disks, &info);
        let free = get_total_usable_capacity_free(&disks, &info);

        // Should only be counted once, not 232 times
        assert_eq!(total, 2_000_000_000_000, "Duplicate disks should be counted only once");
        assert_eq!(free, 500_000_000_000, "Free capacity should not be multiplied");

        // If not deduplicated, the result would be:
        // total = 2TB × 232 = 464TB ❌
    }

    #[test]
    fn test_four_disk_erasure_coding() {
        // 4-disk erasure coding: 2 data disks + 2 parity disks
        let disks = vec![
            rustfs_madmin::Disk {
                endpoint: "node1".to_string(),
                drive_path: "/mnt/disk1".to_string(),
                pool_index: 0,
                set_index: 0,
                disk_index: 0,
                total_space: 1_000_000_000_000, // 1TB
                available_space: 250_000_000_000,
                state: "ok".to_string(),
                ..Default::default()
            },
            rustfs_madmin::Disk {
                endpoint: "node1".to_string(),
                drive_path: "/mnt/disk2".to_string(),
                pool_index: 0,
                set_index: 0,
                disk_index: 1,
                total_space: 1_000_000_000_000,
                available_space: 250_000_000_000,
                state: "ok".to_string(),
                ..Default::default()
            },
            rustfs_madmin::Disk {
                endpoint: "node1".to_string(),
                drive_path: "/mnt/disk3".to_string(),
                pool_index: 0,
                set_index: 0,
                disk_index: 2,
                total_space: 1_000_000_000_000,
                available_space: 250_000_000_000,
                state: "ok".to_string(),
                ..Default::default()
            },
            rustfs_madmin::Disk {
                endpoint: "node1".to_string(),
                drive_path: "/mnt/disk4".to_string(),
                pool_index: 0,
                set_index: 0,
                disk_index: 3,
                total_space: 1_000_000_000_000,
                available_space: 250_000_000_000,
                state: "ok".to_string(),
                ..Default::default()
            },
        ];

        let info = rustfs_madmin::StorageInfo {
            backend: rustfs_madmin::BackendInfo {
                standard_sc_data: vec![2],   // 2 data disks
                standard_sc_parity: Some(2), // 2 parity disks
                ..Default::default()
            },
            disks: disks.clone(),
        };

        let total = get_total_usable_capacity(&disks, &info);

        // Only count data disks (disk_index < 2)
        assert_eq!(total, 2_000_000_000_000, "Should count only data disks (2 × 1TB)");
    }

    #[test]
    fn test_fallback_dedup() {
        // Test deduplication capability of fallback functions
        let mut disks = Vec::new();

        // Add duplicate disks
        for _ in 0..100 {
            disks.push(rustfs_madmin::Disk {
                endpoint: "node1".to_string(),
                drive_path: "/mnt/disk1".to_string(),
                total_space: 2_000_000_000_000,
                available_space: 500_000_000_000,
                state: "ok".to_string(),
                ..Default::default()
            });
        }

        let total = fallback_total_capacity_dedup(&disks);
        let free = fallback_free_capacity_dedup(&disks);

        assert_eq!(total, 2_000_000_000_000);
        assert_eq!(free, 500_000_000_000);
    }
}
