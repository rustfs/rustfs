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

use crate::sinks::Sink;
use crate::{LogRecord, UnifiedLogEntry};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::fs::OpenOptions;
use tokio::io;
use tokio::io::AsyncWriteExt;

/// File Sink Implementation
pub struct FileSink {
    path: String,
    buffer_size: usize,
    writer: Arc<tokio::sync::Mutex<io::BufWriter<tokio::fs::File>>>,
    entry_count: std::sync::atomic::AtomicUsize,
    last_flush: std::sync::atomic::AtomicU64,
    flush_interval_ms: u64, // Time between flushes
    flush_threshold: usize, // Number of entries before flush
}

impl FileSink {
    /// Create a new FileSink instance
    pub async fn new(
        path: String,
        buffer_size: usize,
        flush_interval_ms: u64,
        flush_threshold: usize,
    ) -> Result<Self, io::Error> {
        // check if the file exists
        let file_exists = tokio::fs::metadata(&path).await.is_ok();
        // if the file not exists, create it
        if !file_exists {
            tokio::fs::create_dir_all(std::path::Path::new(&path).parent().unwrap()).await?;
            tracing::debug!("File does not exist, creating it. Path: {:?}", path)
        }
        let file = if file_exists {
            // If the file exists, open it in append mode
            tracing::debug!("FileSink: File exists, opening in append mode. Path: {:?}", path);
            OpenOptions::new().append(true).create(true).open(&path).await?
        } else {
            // If the file does not exist, create it
            tracing::debug!("FileSink: File does not exist, creating a new file.");
            // Create the file and write a header or initial content if needed
            OpenOptions::new().create(true).truncate(true).write(true).open(&path).await?
        };
        let writer = io::BufWriter::with_capacity(buffer_size, file);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        Ok(FileSink {
            path,
            buffer_size,
            writer: Arc::new(tokio::sync::Mutex::new(writer)),
            entry_count: std::sync::atomic::AtomicUsize::new(0),
            last_flush: std::sync::atomic::AtomicU64::new(now),
            flush_interval_ms,
            flush_threshold,
        })
    }

    #[allow(dead_code)]
    async fn initialize_writer(&mut self) -> io::Result<()> {
        let file = tokio::fs::File::create(&self.path).await?;

        // Use buffer_size to create a buffer writer with a specified capacity
        let buf_writer = io::BufWriter::with_capacity(self.buffer_size, file);

        // Replace the original writer with the new Mutex
        self.writer = Arc::new(tokio::sync::Mutex::new(buf_writer));
        Ok(())
    }

    // Get the current buffer size
    #[allow(dead_code)]
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    // How to dynamically adjust the buffer size
    #[allow(dead_code)]
    pub async fn set_buffer_size(&mut self, new_size: usize) -> io::Result<()> {
        if self.buffer_size != new_size {
            self.buffer_size = new_size;
            // Reinitialize the writer directly, without checking is_some()
            self.initialize_writer().await?;
        }
        Ok(())
    }

    // Check if flushing is needed based on count or time
    fn should_flush(&self) -> bool {
        // Check entry count threshold
        if self.entry_count.load(std::sync::atomic::Ordering::Relaxed) >= self.flush_threshold {
            return true;
        }

        // Check time threshold
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let last = self.last_flush.load(std::sync::atomic::Ordering::Relaxed);
        now - last >= self.flush_interval_ms
    }
}

#[async_trait]
impl Sink for FileSink {
    async fn write(&self, entry: &UnifiedLogEntry) {
        let line = format!("{entry:?}\n");
        let mut writer = self.writer.lock().await;

        if let Err(e) = writer.write_all(line.as_bytes()).await {
            eprintln!(
                "Failed to write log to file {}: {},entry timestamp:{:?}",
                self.path,
                e,
                entry.get_timestamp()
            );
            return;
        }

        // Only flush periodically to improve performance
        // Logic to determine when to flush could be added here
        // Increment the entry count
        self.entry_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Check if we should flush
        if self.should_flush() {
            if let Err(e) = writer.flush().await {
                eprintln!("Failed to flush log file {}: {}", self.path, e);
                return;
            }

            // Reset counters
            self.entry_count.store(0, std::sync::atomic::Ordering::Relaxed);

            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            self.last_flush.store(now, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

impl Drop for FileSink {
    fn drop(&mut self) {
        let writer = self.writer.clone();
        let path = self.path.clone();

        tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let mut writer = writer.lock().await;
                if let Err(e) = writer.flush().await {
                    eprintln!("Failed to flush log file {path}: {e}");
                }
            });
        });
    }
}
