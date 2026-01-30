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
mod store_dedup_tests {
    use crate::store::ECStore;

    #[test]
    fn test_deduplicate_disks() {
        let mut disks = Vec::new();

        // Add original disk
        disks.push(rustfs_madmin::Disk {
            endpoint: "node1".to_string(),
            drive_path: "/mnt/disk1".to_string(),
            pool_index: 0,
            set_index: 0,
            disk_index: 0,
            total_space: 1_000_000_000_000,
            ..Default::default()
        });

        // Add 231 duplicates
        for _ in 0..231 {
            disks.push(rustfs_madmin::Disk {
                endpoint: "node1".to_string(),
                drive_path: "/mnt/disk1".to_string(),
                pool_index: 0,
                set_index: 0,
                disk_index: 0,
                total_space: 1_000_000_000_000,
                ..Default::default()
            });
        }

        assert_eq!(disks.len(), 232);

        let deduped = ECStore::deduplicate_disks(disks);

        assert_eq!(deduped.len(), 1, "Should deduplicate to 1 unique disk");
    }

    #[test]
    fn test_deduplicate_multiple_unique_disks() {
        let disks = vec![
            rustfs_madmin::Disk {
                endpoint: "node1".to_string(),
                drive_path: "/mnt/disk1".to_string(),
                pool_index: 0,
                set_index: 0,
                disk_index: 0,
                total_space: 1_000_000_000_000,
                ..Default::default()
            },
            rustfs_madmin::Disk {
                endpoint: "node1".to_string(),
                drive_path: "/mnt/disk2".to_string(),
                pool_index: 0,
                set_index: 0,
                disk_index: 1,
                total_space: 1_000_000_000_000,
                ..Default::default()
            },
            // Duplicate disk1
            rustfs_madmin::Disk {
                endpoint: "node1".to_string(),
                drive_path: "/mnt/disk1".to_string(),
                pool_index: 0,
                set_index: 0,
                disk_index: 0,
                total_space: 1_000_000_000_000,
                ..Default::default()
            },
        ];

        let deduped = ECStore::deduplicate_disks(disks);

        assert_eq!(deduped.len(), 2, "Should keep 2 unique disks");
    }
}
