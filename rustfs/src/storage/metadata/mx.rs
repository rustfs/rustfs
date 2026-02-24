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

use bytes::Bytes;
use rustfs_ecstore::error::{Error, Result};
use std::path::Path;
use std::sync::Arc;
use surrealmx::{Database, DatabaseOptions, PersistenceOptions};

/// Abstract Storage Manager Interface (SurrealMX wrapper)
#[async_trait::async_trait]
pub trait StorageManager: Send + Sync {
    /// Writes data payload. Returns a location ID or confirms CAS write.
    async fn write_data(&self, content_hash: &str, data: Bytes) -> Result<()>;

    /// Reads data payload.
    async fn read_data(&self, content_hash: &str) -> Result<Bytes>;

    /// Checks existence for deduplication.
    async fn exists(&self, content_hash: &str) -> bool;

    /// Deletes data payload.
    async fn delete_data(&self, content_hash: &str) -> Result<()>;
}

/// Concrete implementation using SurrealMX
pub struct MxStorageManager {
    db: Arc<Database>,
}

impl MxStorageManager {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        // Configure SurrealMX options
        // In a real deployment, you would configure persistence paths and memory limits here.
        let opts = DatabaseOptions::default();
        let persistence_opts = PersistenceOptions::new(path.as_ref());

        let db = Database::new_with_persistence(opts, persistence_opts).map_err(|e| Error::other(e.to_string()))?;
        Ok(Self { db: Arc::new(db) })
    }
}

#[async_trait::async_trait]
impl StorageManager for MxStorageManager {
    async fn write_data(&self, content_hash: &str, data: Bytes) -> Result<()> {
        let db = self.db.clone();
        let key = content_hash.to_string();
        let val = data.to_vec();

        // SurrealMX operations are blocking, so we spawn them on a blocking thread
        tokio::task::spawn_blocking(move || {
            let mut tx = db.transaction(true);
            tx.put(&key, &val).map_err(|e| Error::other(e.to_string()))?;
            tx.commit().map_err(|e| Error::other(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| Error::other(e.to_string()))?
    }

    async fn read_data(&self, content_hash: &str) -> Result<Bytes> {
        let db = self.db.clone();
        let key = content_hash.to_string();

        tokio::task::spawn_blocking(move || {
            let tx = db.transaction(false);
            let res = tx.get(&key).map_err(|e| Error::other(e.to_string()))?;
            match res {
                Some(val) => Ok(val),
                None => Err(Error::other("Data not found in SurrealMX")),
            }
        })
        .await
        .map_err(|e| Error::other(e.to_string()))?
    }

    async fn exists(&self, content_hash: &str) -> bool {
        let db = self.db.clone();
        let key = content_hash.to_string();

        tokio::task::spawn_blocking(move || {
            let tx = db.transaction(false);
            matches!(tx.get(&key), Ok(Some(_)))
        })
        .await
        .unwrap_or(false)
    }

    async fn delete_data(&self, content_hash: &str) -> Result<()> {
        let db = self.db.clone();
        let key = content_hash.to_string();

        tokio::task::spawn_blocking(move || {
            let mut tx = db.transaction(true);
            tx.del(&key).map_err(|e| Error::other(e.to_string()))?;
            tx.commit().map_err(|e| Error::other(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| Error::other(e.to_string()))?
    }
}
