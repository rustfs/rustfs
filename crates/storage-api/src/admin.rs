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

use std::fmt::Debug;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DiskSetSelector {
    pub pool_idx: usize,
    pub set_idx: usize,
}

impl DiskSetSelector {
    pub const fn new(pool_idx: usize, set_idx: usize) -> Self {
        Self { pool_idx, set_idx }
    }
}

#[async_trait::async_trait]
pub trait StorageAdminApi: Send + Sync + Debug {
    type BackendInfo: Send + Sync + Debug + 'static;
    type StorageInfo: Send + Sync + Debug + 'static;
    type Disk: Send + Sync + Debug + 'static;
    type Error: std::error::Error + Send + Sync + 'static;

    async fn backend_info(&self) -> Self::BackendInfo;
    async fn storage_info(&self) -> Self::StorageInfo;
    async fn local_storage_info(&self) -> Self::StorageInfo;
    async fn disk_set_inventory(&self, selector: DiskSetSelector) -> Result<Vec<Option<Self::Disk>>, Self::Error>;
    fn set_drive_counts(&self) -> Vec<usize>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct TestError;

    impl std::fmt::Display for TestError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("test storage admin error")
        }
    }

    impl std::error::Error for TestError {}

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestBackendInfo {
        drives_per_set: Vec<usize>,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestStorageInfo {
        disk_count: usize,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestDisk {
        uuid: String,
        disk_index: usize,
    }

    #[derive(Debug)]
    struct FakeStorageAdmin;

    #[async_trait::async_trait]
    impl StorageAdminApi for FakeStorageAdmin {
        type BackendInfo = TestBackendInfo;
        type StorageInfo = TestStorageInfo;
        type Disk = TestDisk;
        type Error = TestError;

        async fn backend_info(&self) -> Self::BackendInfo {
            TestBackendInfo { drives_per_set: vec![2] }
        }

        async fn storage_info(&self) -> Self::StorageInfo {
            TestStorageInfo { disk_count: 2 }
        }

        async fn local_storage_info(&self) -> Self::StorageInfo {
            self.storage_info().await
        }

        async fn disk_set_inventory(&self, selector: DiskSetSelector) -> Result<Vec<Option<Self::Disk>>, Self::Error> {
            assert_eq!(selector, DiskSetSelector::new(0, 0));
            Ok(vec![Some(disk("disk-0", 0)), None])
        }

        fn set_drive_counts(&self) -> Vec<usize> {
            vec![2]
        }
    }

    fn disk(uuid: &str, disk_index: usize) -> TestDisk {
        TestDisk {
            uuid: uuid.to_owned(),
            disk_index,
        }
    }

    #[tokio::test]
    async fn fake_storage_admin_api_exposes_disk_inventory_contract() {
        let api = FakeStorageAdmin;

        let disks = api
            .disk_set_inventory(DiskSetSelector::new(0, 0))
            .await
            .expect("fake disk inventory should be available");

        assert_eq!(api.backend_info().await.drives_per_set, vec![2]);
        assert_eq!(api.storage_info().await.disk_count, 2);
        assert_eq!(api.set_drive_counts(), vec![2]);
        assert_eq!(disks.len(), 2);
        assert_eq!(disks[0].as_ref().map(|disk| disk.uuid.as_str()), Some("disk-0"));
        assert!(disks[1].is_none());
    }
}
