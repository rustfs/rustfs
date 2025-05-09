use std::sync::Arc;
use tokio::sync::{Mutex, Notify};
use tracing::info;

pub struct Workers {
    available: Mutex<usize>, // Available working slots
    notify: Notify,          // Used to notify waiting tasks
    limit: usize,            // Maximum number of concurrent jobs
}

impl Workers {
    // Create a Workers object that allows up to n jobs to execute concurrently
    pub fn new(n: usize) -> Result<Arc<Workers>, &'static str> {
        if n == 0 {
            return Err("n must be > 0");
        }

        Ok(Arc::new(Workers {
            available: Mutex::new(n),
            notify: Notify::new(),
            limit: n,
        }))
    }

    // Give a job a chance to be executed
    pub async fn take(&self) {
        loop {
            let mut available = self.available.lock().await;
            info!("worker take, {}", *available);
            if *available == 0 {
                drop(available);
                self.notify.notified().await;
            } else {
                *available -= 1;
                break;
            }
        }
    }

    // Release a job's slot
    pub async fn give(&self) {
        let mut available = self.available.lock().await;
        info!("worker give, {}", *available);
        *available += 1; // Increase available slots
        self.notify.notify_one(); // Notify a waiting task
    }

    // Wait for all concurrent jobs to complete
    pub async fn wait(&self) {
        loop {
            {
                let available = self.available.lock().await;
                if *available == self.limit {
                    break;
                }
            }
            // Wait until all slots are freed
            self.notify.notified().await;
        }
        info!("worker wait end");
    }

    pub async fn available(&self) -> usize {
        *self.available.lock().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_workers() {
        let workers = Arc::new(Workers::new(5).unwrap());

        for _ in 0..5 {
            let workers = workers.clone();
            tokio::spawn(async move {
                workers.take().await;
                sleep(Duration::from_secs(3)).await;
            });
        }

        for _ in 0..5 {
            workers.give().await;
        }
        // Sleep: wait for spawn task started
        sleep(Duration::from_secs(1)).await;
        workers.wait().await;
        if workers.available().await != workers.limit {
            unreachable!();
        }
    }
}
