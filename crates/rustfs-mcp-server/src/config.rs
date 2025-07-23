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
use clap::Parser;
use tracing::info;

/// Configuration for RustFS MCP Server
#[derive(Parser, Debug, Clone)]
#[command(
    name = "rustfs-mcp-server",
    about = "RustFS MCP (Model Context Protocol) Server for S3 operations",
    version,
    long_about = r#"
RustFS MCP Server - Model Context Protocol server for S3 operations

This server provides S3 operations through the Model Context Protocol (MCP),
allowing AI assistants to interact with S3-compatible storage systems.

ENVIRONMENT VARIABLES:
  All command-line options can also be set via environment variables.
  Command-line arguments take precedence over environment variables.

EXAMPLES:
  # Using command-line arguments
  rustfs-mcp-server --access-key-id AKIAIOSFODNN7EXAMPLE --secret-access-key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

  # Using environment variables
  export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
  export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
  rustfs-mcp-server

  # Mixed usage (command-line overrides environment)
  export AWS_REGION=us-east-1
  rustfs-mcp-server --access-key-id mykey --secret-access-key mysecret --endpoint-url http://localhost:9000
"#
)]
pub struct Config {
    /// AWS Access Key ID
    #[arg(
        long = "access-key-id",
        env = "AWS_ACCESS_KEY_ID",
        help = "AWS Access Key ID for S3 authentication"
    )]
    pub access_key_id: Option<String>,

    /// AWS Secret Access Key
    #[arg(
        long = "secret-access-key",
        env = "AWS_SECRET_ACCESS_KEY",
        help = "AWS Secret Access Key for S3 authentication"
    )]
    pub secret_access_key: Option<String>,

    /// AWS Region
    #[arg(
        long = "region",
        env = "AWS_REGION",
        default_value = "us-east-1",
        help = "AWS region to use for S3 operations"
    )]
    pub region: String,

    /// Custom S3 endpoint URL
    #[arg(
        long = "endpoint-url",
        env = "AWS_ENDPOINT_URL",
        help = "Custom S3 endpoint URL (for MinIO, LocalStack, etc.)"
    )]
    pub endpoint_url: Option<String>,

    /// Log level
    #[arg(
        long = "log-level",
        env = "RUST_LOG",
        default_value = "rustfs_mcp_server=info",
        help = "Log level configuration"
    )]
    pub log_level: String,

    /// Force path-style addressing
    #[arg(
        long = "force-path-style",
        help = "Force path-style S3 addressing (automatically enabled for custom endpoints)"
    )]
    pub force_path_style: bool,
}

impl Config {
    /// Parse configuration from command line arguments and environment variables
    pub fn new() -> Self {
        Config::parse()
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<()> {
        if self.access_key_id.is_none() {
            anyhow::bail!(
                "AWS Access Key ID is required. Set via --access-key-id or AWS_ACCESS_KEY_ID environment variable"
            );
        }

        if self.secret_access_key.is_none() {
            anyhow::bail!(
                "AWS Secret Access Key is required. Set via --secret-access-key or AWS_SECRET_ACCESS_KEY environment variable"
            );
        }

        Ok(())
    }

    /// Get AWS Access Key ID (guaranteed to be Some after validation)
    pub fn access_key_id(&self) -> &str {
        self.access_key_id.as_ref().expect("Access key ID should be validated")
    }

    /// Get AWS Secret Access Key (guaranteed to be Some after validation)
    pub fn secret_access_key(&self) -> &str {
        self.secret_access_key.as_ref().expect("Secret access key should be validated")
    }

    /// Log current configuration (without sensitive data)
    pub fn log_configuration(&self) {
        let access_key_display = self.access_key_id
            .as_ref()
            .map(|key| {
                if key.len() > 8 {
                    format!("{}...{}", &key[..4], &key[key.len() - 4..])
                } else {
                    "*".repeat(key.len())
                }
            })
            .unwrap_or_else(|| "Not set".to_string());

        let endpoint_display = self.endpoint_url
            .as_ref()
            .map(|url| format!("Custom endpoint: {url}"))
            .unwrap_or_else(|| "Default AWS endpoints".to_string());

        info!("Configuration:");
        info!("  AWS Region: {}", self.region);
        info!("  AWS Access Key ID: {}", access_key_display);
        info!("  AWS Secret Access Key: [HIDDEN]");
        info!("  S3 Endpoint: {}", endpoint_display);
        info!("  Force Path Style: {}", self.force_path_style);
        info!("  Log Level: {}", self.log_level);
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            access_key_id: None,
            secret_access_key: None,
            region: "us-east-1".to_string(),
            endpoint_url: None,
            log_level: "rustfs_mcp_server=info".to_string(),
            force_path_style: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_validation_success() {
        let config = Config {
            access_key_id: Some("test_key".to_string()),
            secret_access_key: Some("test_secret".to_string()),
            ..Config::default()
        };

        assert!(config.validate().is_ok());
        assert_eq!(config.access_key_id(), "test_key");
        assert_eq!(config.secret_access_key(), "test_secret");
    }

    #[test]
    fn test_config_validation_missing_access_key() {
        let config = Config {
            access_key_id: None,
            secret_access_key: Some("test_secret".to_string()),
            ..Config::default()
        };

        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Access Key ID"));
    }

    #[test]
    fn test_config_validation_missing_secret_key() {
        let config = Config {
            access_key_id: Some("test_key".to_string()),
            secret_access_key: None,
            ..Config::default()
        };

        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Secret Access Key"));
    }

    #[test]
    fn test_config_default() {
        let config = Config::default();
        assert_eq!(config.region, "us-east-1");
        assert_eq!(config.log_level, "rustfs_mcp_server=info");
        assert!(!config.force_path_style);
        assert!(config.access_key_id.is_none());
        assert!(config.secret_access_key.is_none());
        assert!(config.endpoint_url.is_none());
    }
}