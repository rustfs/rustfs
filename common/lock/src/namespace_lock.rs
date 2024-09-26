use std::{collections::HashMap, path::Path, sync::Arc, time::Duration};

use async_trait::async_trait;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::{
    drwmutex::{DRWMutex, Options},
    lrwmutex::LRWMutex,
    LockApi,
};
use common::error::Result;

pub type RWLockerImpl = Box<dyn RWLocker + Send>;

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
    async fn lock(
        &mut self,
        volume: &String,
        path: &String,
        lock_source: &String,
        ops_id: &String,
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

        let locked: bool;
        if read_lock {
            locked = nslk.lock.get_r_lock(ops_id, lock_source, &timeout).await;
        } else {
            locked = nslk.lock.get_lock(ops_id, lock_source, &timeout).await;
        }

        if !locked {
            nslk.reference -= 1;
            if nslk.reference == 0 {
                w_lock_map.remove(&resource);
            }
        }

        return locked;
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

            nslk.reference -= 0;

            if nslk.reference == 0 {
                w_lock_map.remove(&resource);
            }
        } else {
            return;
        }
    }
}

pub async fn new_nslock(
    ns: Arc<RwLock<NsLockMap>>,
    owner: String,
    volume: String,
    paths: Vec<String>,
    lockers: Vec<LockApi>,
) -> RWLockerImpl {
    if ns.read().await.is_dist_erasure {
        let names = paths
            .iter()
            .map(|path| Path::new(&volume).join(path).to_str().unwrap().to_string())
            .collect();
        return Box::new(DistLockInstance::new(owner, names, lockers));
    }

    Box::new(LocalLockInstance::new(ns, volume, paths))
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
        Ok(self.lock.un_lock().await)
    }

    async fn get_u_lock(&mut self, opts: &Options) -> Result<bool> {
        let source = "".to_string();

        Ok(self.lock.get_r_lock(&self.ops_id, &source, opts).await)
    }

    async fn un_r_lock(&mut self) -> Result<()> {
        Ok(self.lock.un_r_lock().await)
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

    use common::error::Result;
    use tokio::sync::RwLock;

    use crate::{
        drwmutex::Options,
        namespace_lock::{new_nslock, NsLockMap},
    };

    #[tokio::test]
    async fn test_local_instance() -> Result<()> {
        let ns_lock_map = Arc::new(RwLock::new(NsLockMap::default()));
        let mut ns = new_nslock(
            Arc::clone(&ns_lock_map),
            "local".to_string(),
            "test".to_string(),
            vec!["foo".to_string()],
            Vec::new(),
        )
        .await;

        let result = ns
            .get_lock(&Options {
                timeout: Duration::from_secs(5),
                retry_interval: Duration::from_secs(1),
            })
            .await?;

        assert_eq!(result, true);
        Ok(())
    }
}
