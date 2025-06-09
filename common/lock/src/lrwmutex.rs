use std::time::{Duration, Instant};

use rand::Rng;
use tokio::{sync::RwLock, time::sleep};
use tracing::info;

#[derive(Debug, Default)]
pub struct LRWMutex {
    id: RwLock<String>,
    source: RwLock<String>,
    is_write: RwLock<bool>,
    refrence: RwLock<usize>,
}

impl LRWMutex {
    pub async fn lock(&self) -> bool {
        let is_write = true;
        let id = self.id.read().await.clone();
        let source = self.source.read().await.clone();
        let timeout = Duration::from_secs(10000);
        self.look_loop(
            &id, &source, &timeout, // big enough
            is_write,
        )
        .await
    }

    pub async fn get_lock(&self, id: &str, source: &str, timeout: &Duration) -> bool {
        let is_write = true;
        self.look_loop(id, source, timeout, is_write).await
    }

    pub async fn r_lock(&self) -> bool {
        let is_write: bool = false;
        let id = self.id.read().await.clone();
        let source = self.source.read().await.clone();
        let timeout = Duration::from_secs(10000);
        self.look_loop(
            &id, &source, &timeout, // big enough
            is_write,
        )
        .await
    }

    pub async fn get_r_lock(&self, id: &str, source: &str, timeout: &Duration) -> bool {
        let is_write = false;
        self.look_loop(id, source, timeout, is_write).await
    }

    async fn inner_lock(&self, id: &str, source: &str, is_write: bool) -> bool {
        *self.id.write().await = id.to_string();
        *self.source.write().await = source.to_string();

        let mut locked = false;
        if is_write {
            if *self.refrence.read().await == 0 && !*self.is_write.read().await {
                *self.refrence.write().await = 1;
                *self.is_write.write().await = true;
                locked = true;
            }
        } else if !*self.is_write.read().await {
            *self.refrence.write().await += 1;
            locked = true;
        }

        locked
    }

    async fn look_loop(&self, id: &str, source: &str, timeout: &Duration, is_write: bool) -> bool {
        let start = Instant::now();
        loop {
            if self.inner_lock(id, source, is_write).await {
                return true;
            } else {
                if Instant::now().duration_since(start) > *timeout {
                    return false;
                }
                let sleep_time: u64;
                {
                    let mut rng = rand::rng();
                    sleep_time = rng.random_range(10..=50);
                }
                sleep(Duration::from_millis(sleep_time)).await;
            }
        }
    }

    pub async fn un_lock(&self) {
        let is_write = true;
        if !self.unlock(is_write).await {
            info!("Trying to un_lock() while no Lock() is active")
        }
    }

    pub async fn un_r_lock(&self) {
        let is_write = false;
        if !self.unlock(is_write).await {
            info!("Trying to un_r_lock() while no Lock() is active")
        }
    }

    async fn unlock(&self, is_write: bool) -> bool {
        let mut unlocked = false;
        if is_write {
            if *self.is_write.read().await && *self.refrence.read().await == 1 {
                *self.refrence.write().await = 0;
                *self.is_write.write().await = false;
                unlocked = true;
            }
        } else if !*self.is_write.read().await && *self.refrence.read().await > 0 {
            *self.refrence.write().await -= 1;
            unlocked = true;
        }

        unlocked
    }

    pub async fn force_un_lock(&self) {
        *self.refrence.write().await = 0;
        *self.is_write.write().await = false;
    }
}

#[cfg(test)]
mod test {
    use std::{sync::Arc, time::Duration};

    use common::error::Result;
    use tokio::time::sleep;

    use crate::lrwmutex::LRWMutex;

    #[tokio::test]
    async fn test_lock_unlock() -> Result<()> {
        let l_rw_lock = LRWMutex::default();
        let id = "foo";
        let source = "dandan";
        let timeout = Duration::from_secs(5);
        assert!(l_rw_lock.get_lock(id, source, &timeout).await);
        l_rw_lock.un_lock().await;

        l_rw_lock.lock().await;

        assert!(!l_rw_lock.get_r_lock(id, source, &timeout).await);
        l_rw_lock.un_lock().await;
        assert!(l_rw_lock.get_r_lock(id, source, &timeout).await);

        Ok(())
    }

    #[tokio::test]
    async fn multi_thread_test() -> Result<()> {
        let l_rw_lock = Arc::new(LRWMutex::default());
        let id = "foo";
        let source = "dandan";

        let one_fn = async {
            let one = Arc::clone(&l_rw_lock);
            let timeout = Duration::from_secs(1);
            assert!(one.get_lock(id, source, &timeout).await);
            sleep(Duration::from_secs(5)).await;
            l_rw_lock.un_lock().await;
        };

        let two_fn = async {
            let two = Arc::clone(&l_rw_lock);
            let timeout = Duration::from_secs(2);
            assert!(!two.get_r_lock(id, source, &timeout).await);
            sleep(Duration::from_secs(5)).await;
            assert!(two.get_r_lock(id, source, &timeout).await);
            two.un_r_lock().await;
        };

        tokio::join!(one_fn, two_fn);

        Ok(())
    }
}
