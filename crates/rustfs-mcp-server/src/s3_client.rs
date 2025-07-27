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

use anyhow::{Context, Result};
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::{Client, Config as S3Config};
use serde::{Deserialize, Serialize};
use std::path::Path;
use tracing::{debug, info};

use crate::config::Config;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketInfo {
    pub name: String,
    pub creation_date: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectInfo {
    pub key: String,
    pub size: Option<i64>,
    pub last_modified: Option<String>,
    pub etag: Option<String>,
    pub storage_class: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct ListObjectsOptions {
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub max_keys: Option<i32>,
    pub continuation_token: Option<String>,
    pub start_after: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListObjectsResult {
    pub objects: Vec<ObjectInfo>,
    pub common_prefixes: Vec<String>,
    pub is_truncated: bool,
    pub next_continuation_token: Option<String>,
    pub max_keys: Option<i32>,
    pub key_count: i32,
}

#[derive(Debug, Clone, Default)]
pub struct UploadFileOptions {
    pub content_type: Option<String>,
    pub metadata: Option<std::collections::HashMap<String, String>>,
    pub storage_class: Option<String>,
    pub server_side_encryption: Option<String>,
    pub cache_control: Option<String>,
    pub content_disposition: Option<String>,
    pub content_encoding: Option<String>,
    pub content_language: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadResult {
    pub bucket: String,
    pub key: String,
    pub etag: String,
    pub location: String,
    pub version_id: Option<String>,
    pub file_size: u64,
    pub content_type: String,
    pub upload_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct S3Client {
    client: Client,
}

impl S3Client {
    pub async fn new(config: &Config) -> Result<Self> {
        info!("Initializing S3 client from configuration");

        let access_key = config.access_key_id();
        let secret_key = config.secret_access_key();

        debug!("Using AWS region: {}", config.region);
        if let Some(ref endpoint) = config.endpoint_url {
            debug!("Using custom endpoint: {}", endpoint);
        }

        let credentials = Credentials::new(access_key, secret_key, None, None, "rustfs-mcp-server");

        let mut config_builder = S3Config::builder()
            .credentials_provider(credentials)
            .region(Region::new(config.region.clone()))
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest());

        // Set force path style if custom endpoint or explicitly requested
        let should_force_path_style = config.endpoint_url.is_some() || config.force_path_style;
        if should_force_path_style {
            config_builder = config_builder.force_path_style(true);
        }

        if let Some(endpoint) = &config.endpoint_url {
            config_builder = config_builder.endpoint_url(endpoint);
        }

        let s3_config = config_builder.build();
        let client = Client::from_conf(s3_config);

        info!("S3 client initialized successfully");

        Ok(Self { client })
    }

    pub async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
        debug!("Listing S3 buckets");

        let response = self.client.list_buckets().send().await.context("Failed to list S3 buckets")?;

        let buckets: Vec<BucketInfo> = response
            .buckets()
            .iter()
            .map(|bucket| {
                let name = bucket.name().unwrap_or("unknown").to_string();
                let creation_date = bucket
                    .creation_date()
                    .map(|dt| dt.fmt(aws_sdk_s3::primitives::DateTimeFormat::DateTime).unwrap());

                BucketInfo { name, creation_date }
            })
            .collect();

        debug!("Found {} buckets", buckets.len());
        Ok(buckets)
    }

    pub async fn list_objects_v2(
        &self,
        bucket_name: &str,
        options: ListObjectsOptions,
    ) -> Result<ListObjectsResult> {
        debug!("Listing objects in bucket '{}' with options: {:?}", bucket_name, options);

        let mut request = self.client.list_objects_v2().bucket(bucket_name);

        if let Some(prefix) = options.prefix {
            request = request.prefix(prefix);
        }

        if let Some(delimiter) = options.delimiter {
            request = request.delimiter(delimiter);
        }

        if let Some(max_keys) = options.max_keys {
            request = request.max_keys(max_keys);
        }

        if let Some(continuation_token) = options.continuation_token {
            request = request.continuation_token(continuation_token);
        }

        if let Some(start_after) = options.start_after {
            request = request.start_after(start_after);
        }

        let response = request
            .send()
            .await
            .context(format!("Failed to list objects in bucket '{}'", bucket_name))?;

        let objects: Vec<ObjectInfo> = response
            .contents()
            .iter()
            .map(|obj| {
                let key = obj.key().unwrap_or("unknown").to_string();
                let size = obj.size();
                let last_modified = obj
                    .last_modified()
                    .map(|dt| dt.fmt(aws_sdk_s3::primitives::DateTimeFormat::DateTime).unwrap());
                let etag = obj.e_tag().map(|e| e.to_string());
                let storage_class = obj.storage_class().map(|sc| sc.as_str().to_string());

                ObjectInfo {
                    key,
                    size,
                    last_modified,
                    etag,
                    storage_class,
                }
            })
            .collect();

        let common_prefixes: Vec<String> = response
            .common_prefixes()
            .iter()
            .filter_map(|cp| cp.prefix())
            .map(|p| p.to_string())
            .collect();

        let result = ListObjectsResult {
            objects,
            common_prefixes,
            is_truncated: response.is_truncated().unwrap_or(false),
            next_continuation_token: response.next_continuation_token().map(|t| t.to_string()),
            max_keys: response.max_keys(),
            key_count: response.key_count().unwrap_or(0),
        };

        debug!(
            "Found {} objects and {} common prefixes in bucket '{}'",
            result.objects.len(),
            result.common_prefixes.len(),
            bucket_name
        );

        Ok(result)
    }

    pub async fn upload_file(
        &self,
        local_path: &str,
        bucket_name: &str,
        object_key: &str,
        options: UploadFileOptions,
    ) -> Result<UploadResult> {
        info!("Starting file upload: '{}' -> s3://{}/{}", local_path, bucket_name, object_key);

        // Validate and canonicalize file path for security
        let path = Path::new(local_path);
        let canonical_path = path
            .canonicalize()
            .context(format!("Failed to resolve file path: {}", local_path))?;

        // Check if file exists and is readable
        if !canonical_path.exists() {
            anyhow::bail!("File does not exist: {}", local_path);
        }

        if !canonical_path.is_file() {
            anyhow::bail!("Path is not a file: {}", local_path);
        }

        // Get file metadata
        let metadata = tokio::fs::metadata(&canonical_path)
            .await
            .context(format!("Failed to read file metadata: {}", local_path))?;
        
        let file_size = metadata.len();
        debug!("File size: {} bytes", file_size);

        // Auto-detect content type if not provided
        let content_type = options.content_type.unwrap_or_else(|| {
            let detected = mime_guess::from_path(&canonical_path)
                .first_or_octet_stream()
                .to_string();
            debug!("Auto-detected content type: {}", detected);
            detected
        });

        // Read file content into memory for better compatibility with RustFS
        let file_content = tokio::fs::read(&canonical_path)
            .await
            .context(format!("Failed to read file content: {}", local_path))?;
        
        let byte_stream = ByteStream::from(file_content);

        // Prepare upload request
        let mut request = self.client
            .put_object()
            .bucket(bucket_name)
            .key(object_key)
            .body(byte_stream)
            .content_type(&content_type)
            .content_length(file_size as i64);

        // Add optional parameters
        if let Some(storage_class) = &options.storage_class {
            request = request.storage_class(storage_class.as_str().into());
        }

        if let Some(cache_control) = &options.cache_control {
            request = request.cache_control(cache_control);
        }

        if let Some(content_disposition) = &options.content_disposition {
            request = request.content_disposition(content_disposition);
        }

        if let Some(content_encoding) = &options.content_encoding {
            request = request.content_encoding(content_encoding);
        }

        if let Some(content_language) = &options.content_language {
            request = request.content_language(content_language);
        }

        if let Some(sse) = &options.server_side_encryption {
            request = request.server_side_encryption(sse.as_str().into());
        }

        // Add metadata if provided
        if let Some(metadata_map) = &options.metadata {
            for (key, value) in metadata_map {
                request = request.metadata(key, value);
            }
        }

        // Execute upload
        debug!("Executing S3 put_object request");
        let response = request
            .send()
            .await
            .context(format!("Failed to upload file to s3://{}/{}", bucket_name, object_key))?;

        // Extract response information
        let etag = response.e_tag().unwrap_or("unknown").to_string();
        let version_id = response.version_id().map(|v| v.to_string());
        
        // Construct result URL (this is a simplified approach)
        let location = format!("s3://{}/{}", bucket_name, object_key);

        let upload_result = UploadResult {
            bucket: bucket_name.to_string(),
            key: object_key.to_string(),
            etag,
            location,
            version_id,
            file_size,
            content_type,
            upload_id: None, // Only used for multipart uploads
        };

        info!(
            "File upload completed successfully: {} bytes uploaded to s3://{}/{}",
            file_size, bucket_name, object_key
        );

        Ok(upload_result)
    }

    pub async fn health_check(&self) -> Result<()> {
        debug!("Performing S3 health check");

        self.client.list_buckets().send().await.context("S3 health check failed")?;

        debug!("S3 health check passed");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore] // Requires AWS credentials
    async fn test_s3_client_creation() {
        let config = Config {
            access_key_id: Some("test_key".to_string()),
            secret_access_key: Some("test_secret".to_string()),
            region: "us-east-1".to_string(),
            ..Config::default()
        };

        let result = S3Client::new(&config).await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_bucket_info_serialization() {
        let bucket = BucketInfo {
            name: "test-bucket".to_string(),
            creation_date: Some("2024-01-01T00:00:00Z".to_string()),
        };

        let json = serde_json::to_string(&bucket).unwrap();
        let deserialized: BucketInfo = serde_json::from_str(&json).unwrap();

        assert_eq!(bucket.name, deserialized.name);
        assert_eq!(bucket.creation_date, deserialized.creation_date);
    }
}
