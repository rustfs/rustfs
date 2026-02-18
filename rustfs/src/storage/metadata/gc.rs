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

use crate::storage::metadata::mx::StorageManager;
use std::sync::Arc;
use std::time::Duration;
use surrealkv::Tree as KvStore;
use tokio::time::interval;
use tracing::{error, info};

pub struct GarbageCollector {
    kv_store: Arc<KvStore>,
    storage_manager: Arc<dyn StorageManager>,
}

impl GarbageCollector {
    pub fn new(kv_store: Arc<KvStore>, storage_manager: Arc<dyn StorageManager>) -> Self {
        Self {
            kv_store,
            storage_manager,
        }
    }

    pub async fn start(self: Arc<Self>) {
        let mut interval = interval(Duration::from_secs(3600)); // Run every hour

        loop {
            interval.tick().await;
            if let Err(e) = self.run_gc().await {
                error!("GC failed: {}", e);
            }
        }
    }

    async fn run_gc(&self) -> Result<(), String> {
        info!("Starting GC...");

        let mut candidates = Vec::new();

        {
            let mut tx = self.kv_store.begin().map_err(|e| e.to_string())?;
            let mut iter = tx.range("refs/".as_bytes(), "refs0".as_bytes()).map_err(|e| e.to_string())?;

            while iter.valid() {
                let key = iter.key();
                let val = iter.value().map_err(|e| e.to_string())?;

                if val.len() == 8 {
                    let mut buf = [0u8; 8];
                    buf.copy_from_slice(&val);
                    let count = u64::from_be_bytes(buf);

                    if count == 0 {
                        // Use user_key() to convert InternalKeyRef to &[u8]
                        if let Ok(key_str) = std::str::from_utf8(key.user_key()) {
                            if let Some(hash) = key_str.strip_prefix("refs/") {
                                candidates.push(hash.to_string());
                            }
                        }
                    }
                }

                iter.next().map_err(|e| e.to_string())?;
            }
        }

        for hash in candidates {
            // 1. Delete Data
            if let Err(e) = self.storage_manager.delete_data(&hash).await {
                error!("GC: Failed to delete data for {}: {:?}", hash, e);
                continue;
            }

            // 2. Delete Key (Atomic check)
            let mut tx = self.kv_store.begin().map_err(|e| e.to_string())?;
            let key = format!("refs/{}", hash);

            let should_delete = if let Some(val) = tx.get(key.as_bytes()).map_err(|e| e.to_string())? {
                if val.len() == 8 {
                    let mut buf = [0u8; 8];
                    buf.copy_from_slice(&val);
                    u64::from_be_bytes(buf) == 0
                } else {
                    false
                }
            } else {
                false // Already gone
            };

            if should_delete {
                tx.delete(key.as_bytes()).map_err(|e| e.to_string())?;
                tx.commit().await.map_err(|e| e.to_string())?;
                info!("GC: Collected {}", hash);
            }
        }

        info!("GC finished.");
        Ok(())
    }
}
