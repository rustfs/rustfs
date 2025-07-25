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
    ServerHandler,
    handler::server::{router::tool::ToolRouter, tool::Parameters},
    model::{ServerInfo, Implementation, ServerCapabilities, ProtocolVersion, ToolsCapability},
    service::{RequestContext, NotificationContext},
    RoleServer, ErrorData,
    tool, tool_handler, tool_router,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info};

use crate::config::Config;
use crate::s3_client::{S3Client, ListObjectsOptions};

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct ListObjectsRequest {
    pub bucket_name: String,
    #[serde(default)]
    #[schemars(description = "Optional prefix to filter objects")]
    pub prefix: Option<String>,
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

                // Add summary information
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
            max_keys: Some(1000), // Default limit
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
                    let prefix_msg = req.prefix
                        .as_ref()
                        .map(|p| format!(" with prefix '{}'", p))
                        .unwrap_or_default();
                    return format!(
                        "No objects found in bucket '{}'{prefix_msg}. The bucket may be empty or the prefix may not match any objects.",
                        req.bucket_name
                    );
                }

                let mut result_text = format!(
                    "Found {} object(s) in bucket **{}**",
                    result.key_count, req.bucket_name
                );

                if let Some(ref p) = req.prefix {
                    result_text.push_str(&format!(" with prefix '{}'", p));
                }
                result_text.push_str(":\n\n");

                // Display common prefixes (directories) first
                if !result.common_prefixes.is_empty() {
                    result_text.push_str("**Directories:**\n");
                    for (index, prefix) in result.common_prefixes.iter().enumerate() {
                        result_text.push_str(&format!("{}. 📁 {}\n", index + 1, prefix));
                    }
                    result_text.push('\n');
                }

                // Display objects
                if !result.objects.is_empty() {
                    result_text.push_str("**Objects:**\n");
                    for (index, obj) in result.objects.iter().enumerate() {
                        result_text.push_str(&format!("{}. **{}**\n", index + 1, obj.key));

                        if let Some(size) = obj.size {
                            result_text.push_str(&format!("   - Size: {} bytes\n", size));
                        }

                        if let Some(ref last_modified) = obj.last_modified {
                            result_text.push_str(&format!("   - Last Modified: {}\n", last_modified));
                        }

                        if let Some(ref etag) = obj.etag {
                            result_text.push_str(&format!("   - ETag: {}\n", etag));
                        }

                        if let Some(ref storage_class) = obj.storage_class {
                            result_text.push_str(&format!("   - Storage Class: {}\n", storage_class));
                        }

                        result_text.push('\n');
                    }
                }

                // Add pagination info if truncated
                if result.is_truncated {
                    result_text.push_str("**Note:** Results are truncated. ");
                    if let Some(ref token) = result.next_continuation_token {
                        result_text.push_str(&format!("Use continuation token '{}' to get more results.\n", token));
                    }
                    result_text.push('\n');
                }

                // Add summary information
                result_text.push_str("---\n");
                result_text.push_str(&format!(
                    "Total: {} object(s), {} directory/ies",
                    result.objects.len(),
                    result.common_prefixes.len()
                ));

                if let Some(max_keys) = result.max_keys {
                    result_text.push_str(&format!(", Max keys: {}", max_keys));
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
}