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

use super::*;

#[async_trait::async_trait]
pub(crate) trait CatalogObjectReader: Send + Sync {
    async fn read_object_limited(
        &self,
        bucket: &str,
        object: &str,
        max_size: usize,
    ) -> TableCatalogStoreResult<Option<TableCatalogObject>>;
    async fn object_metadata(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<Option<TableCatalogObjectMetadata>>;
    async fn object_exists(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<bool>;
}

type PublicationReadLease = Vec<Arc<parking_lot::Mutex<TableCatalogLockGuard>>>;

impl<B> TableCommitLockPublication<'_, B> {
    pub(super) fn reader_lease(&self, bucket: Option<&str>) -> TableCatalogStoreResult<PublicationReadLease> {
        let state = self.state.lock();
        if !state.prepared
            || state
                .table
                .as_ref()
                .is_none_or(|held| bucket.is_some_and(|bucket| held.0 != bucket))
            || state.guards.is_empty()
        {
            return Err(TableCatalogStoreError::Conflict(
                "catalog publication reader has no matching prepared table".into(),
            ));
        }
        let guards: PublicationReadLease = state
            .migration_read_guard
            .iter()
            .chain(state.guards.iter())
            .cloned()
            .collect();
        if guards.iter().any(|guard| guard.lock().is_lock_lost()) {
            return Err(TableCatalogStoreError::Conflict("catalog publication reader lost its guard".into()));
        }
        Ok(guards)
    }

    fn validate_reader_lease(&self, bucket: &str, lease: &PublicationReadLease) -> TableCatalogStoreResult<()> {
        let current = self.reader_lease(Some(bucket))?;
        if current.len() != lease.len() || current.iter().zip(lease).any(|(current, held)| !Arc::ptr_eq(current, held)) {
            return Err(TableCatalogStoreError::Conflict("catalog publication changed during object read".into()));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl<B: TableCatalogObjectBackend> CatalogObjectReader for TableCommitLockPublication<'_, B> {
    async fn read_object_limited(
        &self,
        bucket: &str,
        object: &str,
        max_size: usize,
    ) -> TableCatalogStoreResult<Option<TableCatalogObject>> {
        let lease = self.reader_lease(Some(bucket))?;
        let result = self.backend.read_object_limited(bucket, object, max_size).await;
        self.validate_reader_lease(bucket, &lease)?;
        result
    }
    async fn object_metadata(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<Option<TableCatalogObjectMetadata>> {
        let lease = self.reader_lease(Some(bucket))?;
        let result = self.backend.object_metadata(bucket, object).await;
        self.validate_reader_lease(bucket, &lease)?;
        result
    }
    async fn object_exists(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<bool> {
        let lease = self.reader_lease(Some(bucket))?;
        let result = self.backend.object_exists(bucket, object).await;
        self.validate_reader_lease(bucket, &lease)?;
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table_catalog::test_support::TestCatalogObjectBackend;

    #[tokio::test]
    async fn lock_publication_reader_checks_resource_completion_and_guard_lifetime() {
        let backend = TestCatalogObjectBackend::default();
        let paths = TableCatalogObjectPaths::default();
        let publication = TableCommitLockPublication::new(&backend, &paths);
        assert!(publication.prepared_object_reader().is_err());
        publication.prepare("analytics", "default", "events").await.unwrap();
        backend.seed_object("analytics", "metadata.json", b"data".to_vec()).await;
        let reader = publication.prepared_object_reader().unwrap();
        assert!(reader.read_object_limited("another", "metadata.json", 4).await.is_err());
        assert_eq!(
            reader
                .read_object_limited("analytics", "metadata.json", 4)
                .await
                .unwrap()
                .unwrap()
                .data,
            b"data"
        );
        assert!(reader.object_metadata("analytics", "metadata.json").await.unwrap().is_some());
        assert!(reader.object_exists("analytics", "metadata.json").await.unwrap());

        let pause = backend.pause_next_read("analytics", "metadata.json").await;
        let reading = async { reader.read_object_limited("analytics", "metadata.json", 4).await };
        let completing = async {
            tokio::time::timeout(StdDuration::from_secs(5), pause.wait_started())
                .await
                .unwrap();
            let lease = publication.reader_lease(Some("analytics")).unwrap();
            let weak = Arc::downgrade(&lease[0]);
            drop(lease);
            publication.complete();
            assert!(weak.upgrade().is_some(), "active read must retain the actual guard");
            pause.release();
            weak
        };
        let (result, guard) = tokio::join!(reading, completing);
        assert!(matches!(result, Err(TableCatalogStoreError::Conflict(_))));
        assert!(guard.upgrade().is_none(), "completed read releases its guard lease");
        assert!(reader.object_exists("analytics", "metadata.json").await.is_err());
    }
}
