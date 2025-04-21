use crate::Error;
use crate::Log;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs::{create_dir_all, File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::RwLock;

/// `EventStore` is a struct that manages the storage of event logs.
pub struct EventStore {
    path: String,
    lock: Arc<RwLock<()>>,
}

impl EventStore {
    pub async fn new(path: &str) -> Result<Self, Error> {
        create_dir_all(path).await?;
        Ok(Self {
            path: path.to_string(),
            lock: Arc::new(RwLock::new(())),
        })
    }

    pub async fn save_logs(&self, logs: &[Log]) -> Result<(), Error> {
        let _guard = self.lock.write().await;
        let file_path = format!(
            "{}/events_{}.jsonl",
            self.path,
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
        );
        let file = OpenOptions::new().create(true).append(true).open(&file_path).await?;
        let mut writer = BufWriter::new(file);
        for log in logs {
            let line = serde_json::to_string(log)?;
            writer.write_all(line.as_bytes()).await?;
            writer.write_all(b"\n").await?;
        }
        writer.flush().await?;
        Ok(())
    }

    pub async fn load_logs(&self) -> Result<Vec<Log>, Error> {
        let _guard = self.lock.read().await;
        let mut logs = Vec::new();
        let mut entries = tokio::fs::read_dir(&self.path).await?;
        while let Some(entry) = entries.next_entry().await? {
            let file = File::open(entry.path()).await?;
            let reader = BufReader::new(file);
            let mut lines = reader.lines();
            while let Some(line) = lines.next_line().await? {
                let log: Log = serde_json::from_str(&line)?;
                logs.push(log);
            }
        }
        Ok(logs)
    }
}
