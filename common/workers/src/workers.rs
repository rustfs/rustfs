use std::sync::Arc;
use tokio::sync::{Mutex, Notify};
use tracing::info;

pub struct Workers {
    available: Mutex<usize>, // 可用的工作槽
    notify: Notify,          // 用于通知等待的任务
    limit: usize,            // 最大并发工作数
}

impl Workers {
    // 创建 Workers 对象，允许最多 n 个作业并发执行
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

    // 让一个作业获得执行的机会
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

    // 让一个作业释放其机会
    pub async fn give(&self) {
        let mut available = self.available.lock().await;
        info!("worker give, {}", *available);
        *available += 1; // 增加可用槽
        self.notify.notify_one(); // 通知一个等待的任务
    }

    // 等待所有并发作业完成
    pub async fn wait(&self) {
        loop {
            {
                let available = self.available.lock().await;
                if *available == self.limit {
                    break;
                }
            }
            // 等待直到所有槽都被释放
            self.notify.notified().await;
        }
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
