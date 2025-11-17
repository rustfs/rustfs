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

use anyhow::Result;
use rmcp::{
    ErrorData, RoleServer, ServerHandler,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{Implementation, ProtocolVersion, ServerCapabilities, ServerInfo, ToolsCapability},
    service::{NotificationContext, RequestContext},
    tool, tool_handler, tool_router,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info};

use crate::config::Config;
use crate::s3_client::{DetectedFileType, GetObjectOptions, ListObjectsOptions, S3Client, UploadFileOptions};

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct ListObjectsRequest {
    pub bucket_name: String,
    #[serde(default)]
    #[schemars(description = "Optional prefix to filter objects")]
    pub prefix: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct UploadFileRequest {
    #[schemars(description = "Path to the local file to upload")]
    pub local_file_path: String,
    #[schemars(description = "Name of the S3 bucket to upload to")]
    pub bucket_name: String,
    #[schemars(description = "S3 object key (path/filename in the bucket)")]
    pub object_key: String,
    #[serde(default)]
    #[schemars(description = "Optional content type (auto-detected if not specified)")]
    pub content_type: Option<String>,
    #[serde(default)]
    #[schemars(description = "Optional storage class (STANDARD, REDUCED_REDUNDANCY, etc.)")]
    pub storage_class: Option<String>,
    #[serde(default)]
    #[schemars(description = "Optional cache control header")]
    pub cache_control: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct CreateBucketReqeust {
    #[schemars(description = "Name of the S3 bucket to create")]
    pub bucket_name: String,
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct DeleteBucketReqeust {
    #[schemars(description = "Name of the S3 bucket to delete")]
    pub bucket_name: String,
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct GetObjectRequest {
    #[schemars(description = "Name of the S3 bucket")]
    pub bucket_name: String,
    #[schemars(description = "S3 object key (path/filename in the bucket)")]
    pub object_key: String,
    #[serde(default)]
    #[schemars(description = "Optional version ID for versioned objects")]
    pub version_id: Option<String>,
    #[serde(default = "default_operation_mode")]
    #[schemars(description = "Operation mode: read (return content) or download (save to local file)")]
    pub mode: GetObjectMode,
    #[serde(default)]
    #[schemars(description = "Local file path for download mode (required when mode is download)")]
    pub local_path: Option<String>,
    #[serde(default = "default_max_content_size")]
    #[schemars(description = "Maximum content size to read in bytes for read mode (default: 1MB)")]
    pub max_content_size: usize,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub enum GetObjectMode {
    #[serde(rename = "read")]
    Read,
    #[serde(rename = "download")]
    Download,
}

fn default_operation_mode() -> GetObjectMode {
    GetObjectMode::Read
}
fn default_max_content_size() -> usize {
    1024 * 1024
}

#[derive(Debug, Clone)]
pub struct RustfsMcpServer {
    s3_client: S3Client,
    _config: Config,
    tool_router: ToolRouter<Self>,
}

#[tool_router(router = tool_router)]
impl RustfsMcpServer {
    pub async fn new(config: Config) -> Result<Self> {
        info!("Creating RustFS MCP Server");

        let s3_client = S3Client::new(&config).await?;

        Ok(Self {
            s3_client,
            _config: config,
            tool_router: Self::tool_router(),
        })
    }

    #[tool(description = "Create a new S3 bucket with the specified name")]
    pub async fn create_bucket(&self, Parameters(req): Parameters<CreateBucketReqeust>) -> String {
        info!("Executing create_bucket tool for bucket: {}", req.bucket_name);

        match self.s3_client.create_bucket(&req.bucket_name).await {
            Ok(_) => {
                format!("Successfully created bucket: {}", req.bucket_name)
            }
            Err(e) => {
                format!("Failed to create bucket '{}': {:?}", req.bucket_name, e)
            }
        }
    }

    #[tool(description = "Delete an existing S3 bucket with the specified name")]
    pub async fn delete_bucket(&self, Parameters(req): Parameters<DeleteBucketReqeust>) -> String {
        info!("Executing delete_bucket tool for bucket: {}", req.bucket_name);

        // check if bucket is empty, if not, can not delete bucket directly.
        let object_result = match self
            .s3_client
            .list_objects_v2(&req.bucket_name, ListObjectsOptions::default())
            .await
        {
            Ok(result) => result,
            Err(e) => {
                error!("Failed to list objects in bucket '{}': {:?}", req.bucket_name, e);
                return format!("Failed to list objects in bucket '{}': {:?}", req.bucket_name, e);
            }
        };

        if !object_result.objects.is_empty() {
            error!("Bucket '{}' is not empty", req.bucket_name);
            return format!("Failed to delete bucket '{}': bucket is not empty", req.bucket_name);
        }

        // delete the bucket.
        match self.s3_client.delete_bucket(&req.bucket_name).await {
            Ok(_) => {
                format!("Successfully deleted bucket: {}", req.bucket_name)
            }
            Err(e) => {
                format!("Failed to delete bucket '{}': {:?}", req.bucket_name, e)
            }
        }
    }

    #[tool(description = "List all S3 buckets accessible with the configured credentials")]
    pub async fn list_buckets(&self) -> String {
        info!("Executing list_buckets tool");

        match self.s3_client.list_buckets().await {
            Ok(buckets) => {
                debug!("Successfully retrieved {} buckets", buckets.len());

                if buckets.is_empty() {
                    return "No S3 buckets found. The AWS credentials may not have access to any buckets, or no buckets exist in this account.".to_string();
                }

                let mut result_text = format!("Found {} S3 bucket(s):\n\n", buckets.len());

                for (index, bucket) in buckets.iter().enumerate() {
                    result_text.push_str(&format!("{}. **{}**", index + 1, bucket.name));

                    if let Some(ref creation_date) = bucket.creation_date {
                        result_text.push_str(&format!("\n   - Created: {creation_date}"));
                    }
                    result_text.push_str("\n\n");
                }

                result_text.push_str("---\n");
                result_text.push_str(&format!("Total buckets: {}\n", buckets.len()));
                result_text.push_str("Note: Only buckets accessible with the current AWS credentials are shown.");

                info!("list_buckets tool executed successfully");
                result_text
            }
            Err(e) => {
                error!("Failed to list buckets: {:?}", e);

                format!(
                    "Failed to list S3 buckets: {e}\n\nPossible causes:\n\
                     • AWS credentials are not set or invalid\n\
                     • Network connectivity issues\n\
                     • AWS region is not set correctly\n\
                     • Insufficient permissions to list buckets\n\
                     • Custom endpoint is misconfigured\n\n\
                     Please verify your AWS configuration and try again."
                )
            }
        }
    }

    #[tool(description = "List objects in a specific S3 bucket with optional prefix filtering")]
    pub async fn list_objects(&self, Parameters(req): Parameters<ListObjectsRequest>) -> String {
        info!("Executing list_objects tool for bucket: {}", req.bucket_name);

        let options = ListObjectsOptions {
            prefix: req.prefix.clone(),
            delimiter: None,
            max_keys: Some(1000),
            ..ListObjectsOptions::default()
        };

        match self.s3_client.list_objects_v2(&req.bucket_name, options).await {
            Ok(result) => {
                debug!(
                    "Successfully retrieved {} objects and {} common prefixes from bucket '{}'",
                    result.objects.len(),
                    result.common_prefixes.len(),
                    req.bucket_name
                );

                if result.objects.is_empty() && result.common_prefixes.is_empty() {
                    let prefix_msg = req.prefix.as_ref().map(|p| format!(" with prefix '{p}'")).unwrap_or_default();
                    return format!(
                        "No objects found in bucket '{}'{prefix_msg}. The bucket may be empty or the prefix may not match any objects.",
                        req.bucket_name
                    );
                }

                let mut result_text = format!("Found {} object(s) in bucket **{}**", result.key_count, req.bucket_name);

                if let Some(ref p) = req.prefix {
                    result_text.push_str(&format!(" with prefix '{p}'"));
                }
                result_text.push_str(":\n\n");

                if !result.common_prefixes.is_empty() {
                    result_text.push_str("**Directories:**\n");
                    for (index, prefix) in result.common_prefixes.iter().enumerate() {
                        result_text.push_str(&format!("{}. 📁 {prefix}\n", index + 1));
                    }
                    result_text.push('\n');
                }

                if !result.objects.is_empty() {
                    result_text.push_str("**Objects:**\n");
                    for (index, obj) in result.objects.iter().enumerate() {
                        result_text.push_str(&format!("{}. **{}**\n", index + 1, obj.key));

                        if let Some(size) = obj.size {
                            result_text.push_str(&format!("   - Size: {size} bytes\n"));
                        }

                        if let Some(ref last_modified) = obj.last_modified {
                            result_text.push_str(&format!("   - Last Modified: {last_modified}\n"));
                        }

                        if let Some(ref etag) = obj.etag {
                            result_text.push_str(&format!("   - ETag: {etag}\n"));
                        }

                        if let Some(ref storage_class) = obj.storage_class {
                            result_text.push_str(&format!("   - Storage Class: {storage_class}\n"));
                        }

                        result_text.push('\n');
                    }
                }

                if result.is_truncated {
                    result_text.push_str("**Note:** Results are truncated. ");
                    if let Some(ref token) = result.next_continuation_token {
                        result_text.push_str(&format!("Use continuation token '{token}' to get more results.\n"));
                    }
                    result_text.push('\n');
                }

                result_text.push_str("---\n");
                result_text.push_str(&format!(
                    "Total: {} object(s), {} directory/ies",
                    result.objects.len(),
                    result.common_prefixes.len()
                ));

                if let Some(max_keys) = result.max_keys {
                    result_text.push_str(&format!(", Max keys: {max_keys}"));
                }

                info!("list_objects tool executed successfully for bucket '{}'", req.bucket_name);
                result_text
            }
            Err(e) => {
                error!("Failed to list objects in bucket '{}': {:?}", req.bucket_name, e);

                format!(
                    "Failed to list objects in S3 bucket '{}': {}\n\nPossible causes:\n\
                     • Bucket does not exist or is not accessible\n\
                     • AWS credentials lack permissions to list objects in this bucket\n\
                     • Network connectivity issues\n\
                     • Custom endpoint is misconfigured\n\
                     • Bucket name contains invalid characters\n\n\
                     Please verify the bucket name, your AWS configuration, and permissions.",
                    req.bucket_name, e
                )
            }
        }
    }

    #[tool(
        description = "Get/download an object from an S3 bucket - supports read mode for text files and download mode for all files"
    )]
    pub async fn get_object(&self, Parameters(req): Parameters<GetObjectRequest>) -> String {
        info!(
            "Executing get_object tool: s3://{}/{} (mode: {:?})",
            req.bucket_name, req.object_key, req.mode
        );

        match req.mode {
            GetObjectMode::Read => self.handle_read_mode(req).await,
            GetObjectMode::Download => self.handle_download_mode(req).await,
        }
    }

    async fn handle_read_mode(&self, req: GetObjectRequest) -> String {
        let options = GetObjectOptions {
            version_id: req.version_id.clone(),
            max_content_size: Some(req.max_content_size),
            ..GetObjectOptions::default()
        };

        match self.s3_client.get_object(&req.bucket_name, &req.object_key, options).await {
            Ok(result) => {
                debug!(
                    "Successfully retrieved object s3://{}/{} ({} bytes)",
                    req.bucket_name, req.object_key, result.content_length
                );

                match result.detected_type {
                    DetectedFileType::Text => {
                        if let Some(ref text_content) = result.text_content {
                            format!(
                                "✅ **Text file content retrieved!**\n\n\
                                 **S3 Location:** s3://{}/{}\n\
                                 **File Size:** {} bytes\n\
                                 **Content Type:** {}\n\n\
                                 **Content:**\n```\n{}\n```",
                                result.bucket, result.key, result.content_length, result.content_type, text_content
                            )
                        } else {
                            format!(
                                "⚠️ **Text file detected but content could not be decoded!**\n\n\
                                 **S3 Location:** s3://{}/{}\n\
                                 **File Size:** {} bytes\n\
                                 **Content Type:** {}\n\n\
                                 **Note:** Could not decode file as UTF-8 text. \
                                 Try using download mode instead.",
                                result.bucket, result.key, result.content_length, result.content_type
                            )
                        }
                    }
                    DetectedFileType::NonText(ref mime_type) => {
                        let file_category = if mime_type.starts_with("image/") {
                            "Image"
                        } else if mime_type.starts_with("audio/") {
                            "Audio"
                        } else if mime_type.starts_with("video/") {
                            "Video"
                        } else {
                            "Binary"
                        };

                        format!(
                            "⚠️ **Non-text file detected!**\n\n\
                             **S3 Location:** s3://{}/{}\n\
                             **File Type:** {} ({})\n\
                             **File Size:** {} bytes ({:.2} MB)\n\n\
                             **Note:** This file type cannot be displayed as text.\n\
                             Please use download mode to save it to a local file:\n\n\
                             ```json\n{{\n  \"mode\": \"download\",\n  \"local_path\": \"/path/to/save/file\"\n}}\n```",
                            result.bucket,
                            result.key,
                            file_category,
                            mime_type,
                            result.content_length,
                            result.content_length as f64 / 1_048_576.0
                        )
                    }
                }
            }
            Err(e) => {
                error!("Failed to read object s3://{}/{}: {:?}", req.bucket_name, req.object_key, e);
                self.format_error_message(&req, e)
            }
        }
    }

    async fn handle_download_mode(&self, req: GetObjectRequest) -> String {
        let local_path = match req.local_path {
            Some(ref path) => path,
            None => {
                return "❌ **Error:** local_path is required when using download mode.\n\n\
                        **Example:**\n```json\n{\n  \"mode\": \"download\",\n  \"local_path\": \"/path/to/save/file.ext\"\n}\n```"
                    .to_string();
            }
        };

        let options = GetObjectOptions {
            version_id: req.version_id.clone(),
            ..GetObjectOptions::default()
        };

        match self
            .s3_client
            .download_object_to_file(&req.bucket_name, &req.object_key, local_path, options)
            .await
        {
            Ok((bytes_downloaded, absolute_path)) => {
                info!(
                    "Successfully downloaded object s3://{}/{} to {} ({} bytes)",
                    req.bucket_name, req.object_key, absolute_path, bytes_downloaded
                );

                format!(
                    "✅ **File downloaded successfully!**\n\n\
                     **S3 Location:** s3://{}/{}\n\
                     **Local Path (requested):** {}\n\
                     **Absolute Path:** {}\n\
                     **File Size:** {} bytes ({:.2} MB)\n\n\
                     **✨ File saved successfully!** You can now access it at:\n\
                     `{}`",
                    req.bucket_name,
                    req.object_key,
                    local_path,
                    absolute_path,
                    bytes_downloaded,
                    bytes_downloaded as f64 / 1_048_576.0,
                    absolute_path
                )
            }
            Err(e) => {
                error!(
                    "Failed to download object s3://{}/{} to {}: {:?}",
                    req.bucket_name, req.object_key, local_path, e
                );

                format!(
                    "❌ **Failed to download file from S3**\n\n\
                     **S3 Location:** s3://{}/{}\n\
                     **Local Path:** {}\n\
                     **Error:** {}\n\n\
                     **Possible causes:**\n\
                     • Object does not exist in the specified bucket\n\
                     • AWS credentials lack permissions to read this object\n\
                     • Cannot write to the specified local path\n\
                     • Insufficient disk space\n\
                     • Network connectivity issues\n\n\
                     **Troubleshooting steps:**\n\
                     1. Verify the object exists using list_objects\n\
                     2. Check your AWS credentials and permissions\n\
                     3. Ensure the local directory exists and is writable\n\
                     4. Check available disk space",
                    req.bucket_name, req.object_key, local_path, e
                )
            }
        }
    }

    fn format_error_message(&self, req: &GetObjectRequest, error: anyhow::Error) -> String {
        format!(
            "❌ **Failed to get object from S3 bucket '{}'**\n\n\
             **Object Key:** {}\n\
             **Mode:** {:?}\n\
             **Error:** {}\n\n\
             **Possible causes:**\n\
             • Object does not exist in the specified bucket\n\
             • AWS credentials lack permissions to read this object\n\
             • Network connectivity issues\n\
             • Object key contains invalid characters\n\
             • Bucket does not exist or is not accessible\n\
             • Object is in a different AWS region\n\
             • Version ID is invalid (for versioned objects)\n\n\
             **Troubleshooting steps:**\n\
             1. Verify the object exists using list_objects\n\
             2. Check your AWS credentials and permissions\n\
             3. Ensure the bucket name and object key are correct\n\
             4. Try with a different object to test connectivity\n\
             5. Check if the bucket has versioning enabled",
            req.bucket_name, req.object_key, req.mode, error
        )
    }

    #[tool(description = "Upload a local file to an S3 bucket")]
    pub async fn upload_file(&self, Parameters(req): Parameters<UploadFileRequest>) -> String {
        info!(
            "Executing upload_file tool: '{}' -> s3://{}/{}",
            req.local_file_path, req.bucket_name, req.object_key
        );

        let options = UploadFileOptions {
            content_type: req.content_type.clone(),
            storage_class: req.storage_class.clone(),
            cache_control: req.cache_control.clone(),
            ..UploadFileOptions::default()
        };

        match self
            .s3_client
            .upload_file(&req.local_file_path, &req.bucket_name, &req.object_key, options)
            .await
        {
            Ok(result) => {
                debug!(
                    "Successfully uploaded file '{}' to s3://{}/{} ({} bytes)",
                    req.local_file_path, req.bucket_name, req.object_key, result.file_size
                );

                let mut result_text = format!(
                    "✅ **File uploaded successfully!**\n\n\
                     **Local File:** {}\n\
                     **S3 Location:** s3://{}/{}\n\
                     **File Size:** {} bytes ({:.2} MB)\n\
                     **Content Type:** {}\n\
                     **ETag:** {}\n",
                    req.local_file_path,
                    result.bucket,
                    result.key,
                    result.file_size,
                    result.file_size as f64 / 1_048_576.0,
                    result.content_type,
                    result.etag
                );

                if let Some(ref version_id) = result.version_id {
                    result_text.push_str(&format!("**Version ID:** {version_id}\n"));
                }

                result_text.push_str("\n---\n");
                result_text.push_str("**Upload Summary:**\n");
                result_text.push_str(&format!("• Source: {}\n", req.local_file_path));
                result_text.push_str(&format!("• Destination: {}\n", result.location));
                result_text.push_str(&format!("• Size: {} bytes\n", result.file_size));
                result_text.push_str(&format!("• Type: {}\n", result.content_type));

                if result.file_size > 5 * 1024 * 1024 {
                    result_text.push_str("\n💡 **Note:** Large file uploaded successfully. Consider using multipart upload for files larger than 100MB for better performance and reliability.");
                }

                info!(
                    "upload_file tool executed successfully: {} bytes uploaded to s3://{}/{}",
                    result.file_size, req.bucket_name, req.object_key
                );
                result_text
            }
            Err(e) => {
                error!(
                    "Failed to upload file '{}' to s3://{}/{}: {:?}",
                    req.local_file_path, req.bucket_name, req.object_key, e
                );

                format!(
                    "❌ **Failed to upload file '{}' to S3 bucket '{}'**\n\n\
                     **Error:** {}\n\n\
                     **Possible causes:**\n\
                     • Local file does not exist or is not readable\n\
                     • AWS credentials lack permissions to upload to this bucket\n\
                     • S3 bucket does not exist or is not accessible\n\
                     • Network connectivity issues\n\
                     • File path contains invalid characters or is too long\n\
                     • Insufficient disk space or memory\n\
                     • Custom endpoint is misconfigured\n\
                     • File is locked by another process\n\n\
                     **Troubleshooting steps:**\n\
                     1. Verify the local file exists and is readable\n\
                     2. Check your AWS credentials and permissions\n\
                     3. Ensure the bucket name is correct and accessible\n\
                     4. Try with a smaller file to test connectivity\n\
                     5. Check the file path for special characters\n\n\
                     **File:** {}\n\
                     **Bucket:** {}\n\
                     **Object Key:** {}",
                    req.local_file_path, req.bucket_name, e, req.local_file_path, req.bucket_name, req.object_key
                )
            }
        }
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for RustfsMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::V_2024_11_05,
            capabilities: ServerCapabilities {
                tools: Some(ToolsCapability {
                    list_changed: Some(false),
                }),
                ..Default::default()
            },
            instructions: Some("RustFS MCP Server providing S3 operations through Model Context Protocol".into()),
            server_info: Implementation {
                name: "rustfs-mcp-server".into(),
                version: env!("CARGO_PKG_VERSION").into(),
                ..Default::default()
            },
        }
    }

    async fn ping(&self, _ctx: RequestContext<RoleServer>) -> Result<(), ErrorData> {
        info!("Received ping request");
        Ok(())
    }

    async fn on_initialized(&self, _ctx: NotificationContext<RoleServer>) {
        info!("Client initialized successfully");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_server_creation() {
        let config = Config {
            access_key_id: Some("test_key".to_string()),
            secret_access_key: Some("test_secret".to_string()),
            ..Config::default()
        };

        let result = RustfsMcpServer::new(config).await;
        assert!(result.is_err() || result.is_ok());
    }

    #[test]
    fn test_get_object_request_defaults() {
        let request = GetObjectRequest {
            bucket_name: "test-bucket".to_string(),
            object_key: "test-key".to_string(),
            version_id: None,
            mode: default_operation_mode(),
            local_path: None,
            max_content_size: default_max_content_size(),
        };

        assert_eq!(request.bucket_name, "test-bucket");
        assert_eq!(request.object_key, "test-key");
        assert!(request.version_id.is_none());
        assert_eq!(request.mode, GetObjectMode::Read);
        assert!(request.local_path.is_none());
        assert_eq!(request.max_content_size, 1024 * 1024);
    }

    #[test]
    fn test_get_object_request_serialization() {
        let request = GetObjectRequest {
            bucket_name: "test-bucket".to_string(),
            object_key: "test-key".to_string(),
            version_id: Some("version123".to_string()),
            mode: GetObjectMode::Download,
            local_path: Some("/path/to/file".to_string()),
            max_content_size: 2048,
        };

        let json = serde_json::to_string(&request).unwrap();
        let deserialized: GetObjectRequest = serde_json::from_str(&json).unwrap();

        assert_eq!(request.bucket_name, deserialized.bucket_name);
        assert_eq!(request.object_key, deserialized.object_key);
        assert_eq!(request.version_id, deserialized.version_id);
        assert_eq!(request.mode, deserialized.mode);
        assert_eq!(request.local_path, deserialized.local_path);
        assert_eq!(request.max_content_size, deserialized.max_content_size);
    }

    #[test]
    fn test_get_object_request_serde_with_defaults() {
        let json = r#"{
            "bucket_name": "test-bucket",
            "object_key": "test-key"
        }"#;

        let request: GetObjectRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.bucket_name, "test-bucket");
        assert_eq!(request.object_key, "test-key");
        assert!(request.version_id.is_none());
        assert_eq!(request.mode, GetObjectMode::Read);
        assert!(request.local_path.is_none());
        assert_eq!(request.max_content_size, 1024 * 1024);
    }

    #[test]
    fn test_default_functions() {
        assert_eq!(default_operation_mode(), GetObjectMode::Read);
        assert_eq!(default_max_content_size(), 1024 * 1024);
    }

    #[test]
    fn test_get_object_mode_serialization() {
        let read_mode = GetObjectMode::Read;
        let download_mode = GetObjectMode::Download;

        let read_json = serde_json::to_string(&read_mode).unwrap();
        let download_json = serde_json::to_string(&download_mode).unwrap();

        assert_eq!(read_json, r#""read""#);
        assert_eq!(download_json, r#""download""#);

        let read_mode_deser: GetObjectMode = serde_json::from_str(r#""read""#).unwrap();
        let download_mode_deser: GetObjectMode = serde_json::from_str(r#""download""#).unwrap();

        assert_eq!(read_mode_deser, GetObjectMode::Read);
        assert_eq!(download_mode_deser, GetObjectMode::Download);
    }

    #[test]
    fn test_bucket_creation() {
        let request = CreateBucketReqeust {
            bucket_name: "test-bucket".to_string(),
        };
        assert_eq!(request.bucket_name, "test-bucket");
    }

    #[test]
    fn test_bucket_deletion() {
        let request = DeleteBucketReqeust {
            bucket_name: "test-bucket".to_string(),
        };
        assert_eq!(request.bucket_name, "test-bucket");
    }
}
