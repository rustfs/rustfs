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

use std::{collections::HashMap, path::Path, sync::Arc, time::Duration};

use async_trait::async_trait;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::{
    LockApi,
    drwmutex::{DRWMutex, Options},
    lrwmutex::LRWMutex,
};
use std::io::Result;

pub type RWLockerImpl = Box<dyn RWLocker + Send + Sync>;

#[async_trait]
pub trait RWLocker {
    async fn get_lock(&mut self, opts: &Options) -> Result<bool>;
    async fn un_lock(&mut self) -> Result<()>;
    async fn get_u_lock(&mut self, opts: &Options) -> Result<bool>;
    async fn un_r_lock(&mut self) -> Result<()>;
}

#[derive(Debug)]
struct NsLock {
    reference: usize,
    lock: LRWMutex,
}

#[derive(Debug, Default)]
pub struct NsLockMap {
    is_dist_erasure: bool,
    lock_map: RwLock<HashMap<String, NsLock>>,
}

impl NsLockMap {
    pub fn new(is_dist_erasure: bool) -> Self {
        Self {
            is_dist_erasure,
            ..Default::default()
        }
    }

    async fn lock(
        &mut self,
        volume: &String,
        path: &String,
        lock_source: &str,
        ops_id: &str,
        read_lock: bool,
        timeout: Duration,
    ) -> bool {
        let resource = Path::new(volume).join(path).to_str().unwrap().to_string();
        let mut w_lock_map = self.lock_map.write().await;
        let nslk = w_lock_map.entry(resource.clone()).or_insert(NsLock {
            reference: 0,
            lock: LRWMutex::default(),
        });
        nslk.reference += 1;

        let locked = if read_lock {
            nslk.lock.get_r_lock(ops_id, lock_source, &timeout).await
        } else {
            nslk.lock.get_lock(ops_id, lock_source, &timeout).await
        };

        if !locked {
            nslk.reference -= 1;
            if nslk.reference == 0 {
                w_lock_map.remove(&resource);
            }
        }

        locked
    }

    async fn un_lock(&mut self, volume: &String, path: &String, read_lock: bool) {
        let resource = Path::new(volume).join(path).to_str().unwrap().to_string();
        let mut w_lock_map = self.lock_map.write().await;
        if let Some(nslk) = w_lock_map.get_mut(&resource) {
            if read_lock {
                nslk.lock.un_r_lock().await;
            } else {
                nslk.lock.un_lock().await;
            }

            nslk.reference -= 1;

            if nslk.reference == 0 {
                w_lock_map.remove(&resource);
            }
        }
    }
}

pub struct WrapperLocker(pub Arc<RwLock<RWLockerImpl>>);

impl Drop for WrapperLocker {
    fn drop(&mut self) {
        let inner = self.0.clone();
        tokio::spawn(async move {
            let _ = inner.write().await.un_lock().await;
        });
    }
}

pub async fn new_nslock(
    ns: Arc<RwLock<NsLockMap>>,
    owner: String,
    volume: String,
    paths: Vec<String>,
    lockers: Vec<LockApi>,
) -> WrapperLocker {
    if ns.read().await.is_dist_erasure {
        let names = paths
            .iter()
            .map(|path| Path::new(&volume).join(path).to_str().unwrap().to_string())
            .collect();
        return WrapperLocker(Arc::new(RwLock::new(Box::new(DistLockInstance::new(owner, names, lockers)))));
    }

    WrapperLocker(Arc::new(RwLock::new(Box::new(LocalLockInstance::new(ns, volume, paths)))))
}

struct DistLockInstance {
    lock: Box<DRWMutex>,
    ops_id: String,
}

impl DistLockInstance {
    fn new(owner: String, names: Vec<String>, lockers: Vec<LockApi>) -> Self {
        let ops_id = Uuid::new_v4().to_string();
        Self {
            lock: Box::new(DRWMutex::new(owner, names, lockers)),
            ops_id,
        }
    }
}

#[async_trait]
impl RWLocker for DistLockInstance {
    async fn get_lock(&mut self, opts: &Options) -> Result<bool> {
        let source = "".to_string();

        Ok(self.lock.get_lock(&self.ops_id, &source, opts).await)
    }

    async fn un_lock(&mut self) -> Result<()> {
        self.lock.un_lock().await;
        Ok(())
    }

    async fn get_u_lock(&mut self, opts: &Options) -> Result<bool> {
        let source = "".to_string();

        Ok(self.lock.get_r_lock(&self.ops_id, &source, opts).await)
    }

    async fn un_r_lock(&mut self) -> Result<()> {
        self.lock.un_r_lock().await;
        Ok(())
    }
}

struct LocalLockInstance {
    ns: Arc<RwLock<NsLockMap>>,
    volume: String,
    paths: Vec<String>,
    ops_id: String,
}

impl LocalLockInstance {
    fn new(ns: Arc<RwLock<NsLockMap>>, volume: String, paths: Vec<String>) -> Self {
        let ops_id = Uuid::new_v4().to_string();
        Self {
            ns,
            volume,
            paths,
            ops_id,
        }
    }
}

#[async_trait]
impl RWLocker for LocalLockInstance {
    async fn get_lock(&mut self, opts: &Options) -> Result<bool> {
        let source = "".to_string();
        let read_lock = false;
        let mut success = vec![false; self.paths.len()];
        for (idx, path) in self.paths.iter().enumerate() {
            if !self
                .ns
                .write()
                .await
                .lock(&self.volume, path, &source, &self.ops_id, read_lock, opts.timeout)
                .await
            {
                for (i, x) in success.iter().enumerate() {
                    if *x {
                        self.ns.write().await.un_lock(&self.volume, &self.paths[i], read_lock).await;
                    }
                }

                return Ok(false);
            }

            success[idx] = true;
        }
        Ok(true)
    }

    async fn un_lock(&mut self) -> Result<()> {
        let read_lock = false;
        for path in self.paths.iter() {
            self.ns.write().await.un_lock(&self.volume, path, read_lock).await;
        }

        Ok(())
    }

    async fn get_u_lock(&mut self, opts: &Options) -> Result<bool> {
        let source = "".to_string();
        let read_lock = true;
        let mut success = Vec::with_capacity(self.paths.len());
        for (idx, path) in self.paths.iter().enumerate() {
            if !self
                .ns
                .write()
                .await
                .lock(&self.volume, path, &source, &self.ops_id, read_lock, opts.timeout)
                .await
            {
                for (i, x) in success.iter().enumerate() {
                    if *x {
                        self.ns.write().await.un_lock(&self.volume, &self.paths[i], read_lock).await;
                    }
                }

                return Ok(false);
            }

            success[idx] = true;
        }
        Ok(true)
    }

    async fn un_r_lock(&mut self) -> Result<()> {
        let read_lock = true;
        for path in self.paths.iter() {
            self.ns.write().await.un_lock(&self.volume, path, read_lock).await;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::{sync::Arc, time::Duration};

    use std::io::Result;
    use tokio::sync::RwLock;

    use crate::{
        drwmutex::Options,
        namespace_lock::{NsLockMap, new_nslock},
    };

    #[tokio::test]
    async fn test_local_instance() -> Result<()> {
        let ns_lock_map = Arc::new(RwLock::new(NsLockMap::default()));
        let ns = new_nslock(
            Arc::clone(&ns_lock_map),
            "local".to_string(),
            "test".to_string(),
            vec!["foo".to_string()],
            Vec::new(),
        )
        .await;

        let result =
            ns.0.write()
                .await
                .get_lock(&Options {
                    timeout: Duration::from_secs(5),
                    retry_interval: Duration::from_secs(1),
                })
                .await?;

        assert!(result);
        Ok(())
    }
}
