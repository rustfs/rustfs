//! Streaming handlers for object upload and download with encryption support

use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

use futures_util::stream::StreamExt;
use crate::storage::ecfs::FS;
use crate::error::RustFsError;


/// Progress tracking for streaming operations
#[derive(Debug, Clone)]
pub struct StreamingProgress {
    pub bytes_processed: u64,
    pub total_bytes: Option<u64>,
    pub percentage: Option<f32>,
}

/// Configuration for streaming operations
#[derive(Debug, Clone)]
pub struct StreamingConfig {
    pub chunk_size: usize,
    pub max_bandwidth: Option<u64>, // bytes per second
    pub enable_progress_tracking: bool,
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self {
            chunk_size: 64 * 1024, // 64KB chunks
            max_bandwidth: None,
            enable_progress_tracking: true,
        }
    }
}

/// Streaming upload handler with encryption support
pub struct StreamingUploadHandler {
    fs: FS,
    config: StreamingConfig,
    progress_callback: Option<Box<dyn Fn(StreamingProgress) + Send + Sync>>,
}

impl StreamingUploadHandler {
    pub fn new(fs: FS, config: StreamingConfig) -> Self {
        Self {
            fs,
            config,
            progress_callback: None,
        }
    }

    pub fn with_progress_callback<F>(mut self, callback: F) -> Self
    where
        F: Fn(StreamingProgress) + Send + Sync + 'static,
    {
        self.progress_callback = Some(Box::new(callback));
        self
    }

    /// Handle streaming upload with optional encryption
    pub async fn handle_upload<R>(
        &self,
        bucket: &str,
        key: &str,
        reader: R,
        content_length: Option<u64>,
        _encryption_metadata: Option<()>,
    ) -> Result<String, RustFsError>
    where
        R: AsyncRead + Send + Unpin + 'static,
    {
        let mut total_bytes = 0u64;
        let mut reader = Box::pin(reader);

        // Create streaming cipher if encryption is enabled
        let mut streaming_reader: Pin<Box<dyn AsyncRead + Send>> = if let Some(metadata) = encryption_metadata {
            let cipher = StreamingCipher::new(metadata)?;
            Box::pin(cipher.wrap_reader(reader))
        } else {
            reader
        };

        // Create throttled reader if bandwidth limit is set
        if let Some(max_bandwidth) = self.config.max_bandwidth {
            streaming_reader = Box::pin(ThrottledReader::new(streaming_reader, max_bandwidth));
        }

        // Process upload in chunks
        let mut buffer = vec![0u8; self.config.chunk_size];
        let mut upload_parts = Vec::new();

        loop {
            let bytes_read = match streaming_reader.as_mut().read(&mut buffer).await {
                Ok(0) => break, // EOF
                Ok(n) => n,
                Err(e) => return Err(RustFsError::IoError(e)),
            };

            total_bytes += bytes_read as u64;

            // Store chunk (in real implementation, this would be multipart upload)
            upload_parts.push(buffer[..bytes_read].to_vec());

            // Report progress
            if self.config.enable_progress_tracking {
                if let Some(callback) = &self.progress_callback {
                    let progress = StreamingProgress {
                        bytes_processed: total_bytes,
                        total_bytes: content_length,
                        percentage: content_length.map(|total| (total_bytes as f32 / total as f32) * 100.0),
                    };
                    callback(progress);
                }
            }
        }

        // Implement actual multipart upload completion
        use crate::storage::new_object_layer_fn;
        use rustfs_ecstore::store_api::{ObjectOptions, UploadedPart};
        use std::collections::HashMap;
        
        let Some(store) = new_object_layer_fn() else {
            return Err(RustFsError::InternalError("Storage not initialized".to_string()));
        };
        
        // Create multipart upload
        let opts = ObjectOptions {
            version_id: None,
            part_number: None,
            user_defined: HashMap::new(),
            ..Default::default()
        };
        
        let upload_result = store
            .new_multipart_upload(bucket, key, &opts)
            .await
            .map_err(|e| RustFsError::InternalError(format!("Failed to create multipart upload: {}", e)))?;
        
        let upload_id = upload_result.upload_id.clone();
        
        // Upload parts
        let mut uploaded_parts = Vec::new();
        for (part_number, part_data) in upload_parts.iter().enumerate() {
            let part_num = (part_number + 1) as i32;
            
            let part_result = store
                .put_object_part(
                    bucket,
                    key,
                    &upload_id,
                    part_num,
                    part_data.as_slice(),
                    &opts,
                )
                .await
                .map_err(|e| RustFsError::InternalError(format!("Failed to upload part {}: {}", part_num, e)))?;
            
            uploaded_parts.push(UploadedPart {
                part_number: part_num,
                etag: part_result.etag,
            });
        }
        
        // Complete multipart upload
        let _complete_result = store
            .complete_multipart_upload(bucket, key, &upload_id, uploaded_parts, opts)
            .await
            .map_err(|e| RustFsError::InternalError(format!("Failed to complete multipart upload: {}", e)))?;
        
        Ok(upload_id)
    }
}

/// Streaming download handler with decryption support
pub struct StreamingDownloadHandler {
    fs: FS,
    config: StreamingConfig,
    progress_callback: Option<Box<dyn Fn(StreamingProgress) + Send + Sync>>,
}

impl StreamingDownloadHandler {
    pub fn new(fs: FS, config: StreamingConfig) -> Self {
        Self {
            fs,
            config,
            progress_callback: None,
        }
    }

    pub fn with_progress_callback<F>(mut self, callback: F) -> Self
    where
        F: Fn(StreamingProgress) + Send + Sync + 'static,
    {
        self.progress_callback = Some(Box::new(callback));
        self
    }

    /// Handle streaming download with optional decryption
    pub async fn handle_download<W>(
        &self,
        bucket: &str,
        key: &str,
        writer: W,
        _encryption_metadata: Option<()>,
    ) -> Result<u64, RustFsError>
    where
        W: AsyncWrite + Send + Unpin + 'static,
    {
        let mut total_bytes = 0u64;
        let mut writer = Box::pin(writer);

        // Get object reader from storage
        use crate::storage::new_object_layer_fn;
        use rustfs_ecstore::store_api::{ObjectOptions, HTTPRangeSpec};
        use std::collections::HashMap;
        
        let Some(store) = new_object_layer_fn() else {
            return Err(RustFsError::InternalError("Storage not initialized".to_string()));
        };
        
        let opts = ObjectOptions {
            version_id: None,
            part_number: None,
            ..Default::default()
        };
        
        let h = HashMap::new();
        
        let reader = store
            .get_object_reader(bucket, key, None, h, &opts)
            .await
            .map_err(|e| RustFsError::InternalError(format!("Failed to get object reader: {}", e)))?;
        
        let object_reader = reader.stream;

        // Create streaming reader (decryption handled by storage layer)
        let mut streaming_reader: Pin<Box<dyn AsyncRead + Send>> = Box::pin(object_reader);

        // Create throttled reader if bandwidth limit is set
        if let Some(max_bandwidth) = self.config.max_bandwidth {
            streaming_reader = Box::pin(ThrottledReader::new(streaming_reader, max_bandwidth));
        }

        // Stream data in chunks
        let mut buffer = vec![0u8; self.config.chunk_size];

        loop {
            let bytes_read = match streaming_reader.as_mut().read(&mut buffer).await {
                Ok(0) => break, // EOF
                Ok(n) => n,
                Err(e) => return Err(RustFsError::IoError(e)),
            };

            // Write to output
            if let Err(e) = writer.as_mut().write_all(&buffer[..bytes_read]).await {
                return Err(RustFsError::IoError(e));
            }

            total_bytes += bytes_read as u64;

            // Report progress
            if self.config.enable_progress_tracking {
                if let Some(callback) = &self.progress_callback {
                    let progress = StreamingProgress {
                        bytes_processed: total_bytes,
                        total_bytes: None, // Unknown for downloads
                        percentage: None,
                    };
                    callback(progress);
                }
            }
        }

        Ok(total_bytes)
    }
}

/// Bandwidth throttling reader
struct ThrottledReader<R> {
    inner: R,
    max_bytes_per_second: u64,
    last_read_time: std::time::Instant,
    bytes_read_in_window: u64,
}

impl<R> ThrottledReader<R> {
    fn new(inner: R, max_bytes_per_second: u64) -> Self {
        Self {
            inner,
            max_bytes_per_second,
            last_read_time: std::time::Instant::now(),
            bytes_read_in_window: 0,
        }
    }

    fn should_throttle(&mut self, bytes_to_read: usize) -> Option<std::time::Duration> {
        let now = std::time::Instant::now();
        let elapsed = now.duration_since(self.last_read_time);

        // Reset window if more than 1 second has passed
        if elapsed.as_secs() >= 1 {
            self.last_read_time = now;
            self.bytes_read_in_window = 0;
        }

        let new_total = self.bytes_read_in_window + bytes_to_read as u64;
        if new_total > self.max_bytes_per_second {
            let excess_bytes = new_total - self.max_bytes_per_second;
            let delay_ms = (excess_bytes * 1000) / self.max_bytes_per_second;
            Some(std::time::Duration::from_millis(delay_ms))
        } else {
            None
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for ThrottledReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // Check if we need to throttle
        if let Some(_delay) = self.should_throttle(buf.remaining()) {
            // In a real implementation, we would use a timer here
            // For now, just proceed without delay
        }

        let result = Pin::new(&mut self.inner).poll_read(cx, buf);
        
        if let Poll::Ready(Ok(())) = &result {
            self.bytes_read_in_window += buf.filled().len() as u64;
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn test_streaming_config_default() {
        let config = StreamingConfig::default();
        assert_eq!(config.chunk_size, 64 * 1024);
        assert!(config.max_bandwidth.is_none());
        assert!(config.enable_progress_tracking);
    }

    #[tokio::test]
    async fn test_throttled_reader() {
        let data = b"hello world";
        let reader = std::io::Cursor::new(data);
        let mut throttled = ThrottledReader::new(reader, 1024); // 1KB/s

        let mut buffer = Vec::new();
        let result = throttled.read_to_end(&mut buffer).await;
        assert!(result.is_ok());
        assert_eq!(buffer, data);
    }

    #[tokio::test]
    async fn test_streaming_progress() {
        let progress = StreamingProgress {
            bytes_processed: 1024,
            total_bytes: Some(2048),
            percentage: Some(50.0),
        };

        assert_eq!(progress.bytes_processed, 1024);
        assert_eq!(progress.total_bytes, Some(2048));
        assert_eq!(progress.percentage, Some(50.0));
    }
}