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

//! Bucket encryption configuration handlers
//! 
//! This module provides complete implementation for S3 bucket encryption management:
//! - PUT /bucket/{bucket}/encryption - Set bucket encryption configuration
//! - GET /bucket/{bucket}/encryption - Get bucket encryption configuration  
//! - DELETE /bucket/{bucket}/encryption - Delete bucket encryption configuration
//! - GET /bucket/encryptions - List all bucket encryption configurations
//!
//! All handlers are fully implemented and integrated with the FS bucket encryption manager.

use crate::{
    admin::router::Operation,
    auth::{check_key_valid, get_session_token},
    storage::ecfs::FS,
};
use rustfs_kms::{BucketEncryptionConfig, BucketEncryptionAlgorithm};

use http::StatusCode;
use matchit::Params;

#[derive(Debug, Serialize, Deserialize)]
pub struct BucketEncryptionConfiguration {
    pub rules: Vec<EncryptionRule>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EncryptionRule {
    pub apply_server_side_encryption_by_default: ApplyServerSideEncryptionByDefault,
    pub bucket_key_enabled: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ApplyServerSideEncryptionByDefault {
    #[serde(rename = "SSEAlgorithm")]
    pub sse_algorithm: String,
    #[serde(rename = "KMSMasterKeyID", skip_serializing_if = "Option::is_none")]
    pub kms_master_key_id: Option<String>,
}
use rustfs_policy::auth::Credentials;
use s3s::{
    Body, S3Request, S3Response, S3Result, S3Error, S3ErrorCode,
};
use serde::{Deserialize, Serialize};
use serde_urlencoded;
use tracing::{info, warn};
use chrono;


/// Request structure for setting bucket encryption
#[derive(Debug, Serialize, Deserialize)]
pub struct PutBucketEncryptionRequest {
    /// Encryption algorithm to use
    pub algorithm: BucketEncryptionAlgorithm,
    /// KMS key ID for encrypting data keys
    pub kms_key_id: String,
    /// Whether to encrypt object metadata
    pub encrypt_metadata: bool,
    /// Additional encryption context
    pub encryption_context: Option<std::collections::HashMap<String, String>>,
}

/// Response structure for getting bucket encryption
#[derive(Debug, Serialize, Deserialize)]
pub struct GetBucketEncryptionResponse {
    /// Whether encryption is enabled
    pub enabled: bool,
    /// Encryption algorithm
    pub algorithm: BucketEncryptionAlgorithm,
    /// KMS key ID
    pub kms_key_id: String,
    /// Whether metadata is encrypted
    pub encrypt_metadata: bool,
    /// Encryption context
    pub encryption_context: std::collections::HashMap<String, String>,
    /// Configuration timestamps
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl From<BucketEncryptionConfig> for GetBucketEncryptionResponse {
    fn from(config: BucketEncryptionConfig) -> Self {
        Self {
            enabled: config.enabled,
            algorithm: config.algorithm,
            kms_key_id: config.kms_key_id,
            encrypt_metadata: config.encrypt_metadata,
            encryption_context: config.encryption_context,
            created_at: config.created_at,
            updated_at: config.updated_at,
        }
    }
}

/// Handler for PUT /bucket/{bucket}/encryption
pub struct PutBucketEncryptionHandler {
    pub fs: std::sync::Arc<FS>,
}

#[async_trait::async_trait]
impl Operation for PutBucketEncryptionHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        // Extract bucket name from path parameters
        let bucket_name = params.get("bucket").ok_or_else(|| {
            S3Error::with_message(S3ErrorCode::InvalidRequest, "Missing bucket name")
        })?;

        // Parse credentials from query parameters
        let input_cred: Credentials = serde_urlencoded::from_str(req.uri.query().unwrap_or(""))
            .map_err(|e| S3Error::with_message(S3ErrorCode::InvalidRequest, format!("Invalid query parameters: {}", e)))?;

        // Validate authentication
        check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        // Parse request body
        let mut input = req.input;
        let body = match input.store_all_unlimited().await {
            Ok(b) => b,
            Err(e) => {
                warn!("get body failed, e: {:?}", e);
                return Err(S3Error::with_message(S3ErrorCode::InvalidRequest, "get body failed".to_string()));
            }
        };

        let request: PutBucketEncryptionRequest = serde_json::from_slice(&body)
            .map_err(|e| S3Error::with_message(S3ErrorCode::MalformedXML, format!("Invalid JSON: {}", e)))?;

        // Create bucket encryption configuration
        let config = BucketEncryptionConfig {
            enabled: true,
            algorithm: request.algorithm,
            kms_key_id: request.kms_key_id,
            encrypt_metadata: request.encrypt_metadata,
            encryption_context: request.encryption_context.unwrap_or_default(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };

        // Store the bucket encryption configuration using the manager
        if let Some(manager) = self.fs.bucket_encryption_manager() {
            manager.set_bucket_encryption(bucket_name, config.clone()).await
                .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("Failed to store encryption config: {}", e)))?;
        } else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Bucket encryption manager not available"));
        }

        info!("Setting bucket encryption for bucket: {}", bucket_name);
        info!("Encryption config: {:?}", config);
        
        let response = serde_json::json!({
            "message": "Bucket encryption configuration updated successfully",
            "bucket": bucket_name,
            "config": config
        });

        Ok(S3Response::new((StatusCode::OK, Body::from(response.to_string()))))
    }
}

/// Handler for GET /bucket/{bucket}/encryption
pub struct GetBucketEncryptionHandler {
    pub fs: std::sync::Arc<FS>,
}

#[async_trait::async_trait]
impl Operation for GetBucketEncryptionHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        // Extract bucket name from path parameters
        let bucket_name = params.get("bucket").ok_or_else(|| {
            S3Error::with_message(S3ErrorCode::InvalidRequest, "Missing bucket name")
        })?;

        // Parse credentials from query parameters
        let input_cred: Credentials = serde_urlencoded::from_str(req.uri.query().unwrap_or(""))
            .map_err(|e| S3Error::with_message(S3ErrorCode::InvalidRequest, format!("Invalid query parameters: {}", e)))?;

        // Validate authentication
        check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        info!("Getting bucket encryption for bucket: {}", bucket_name);
        
        // Retrieve the bucket encryption configuration using the manager
        if let Some(manager) = self.fs.bucket_encryption_manager() {
            match manager.get_bucket_encryption(bucket_name).await {
                Ok(Some(config)) => {
                    let response: GetBucketEncryptionResponse = config.into();
                    let response_json = serde_json::to_string(&response)
                        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("JSON serialization error: {}", e)))?;
                    Ok(S3Response::new((StatusCode::OK, Body::from(response_json.into_bytes()))))
                }
                Ok(None) => {
                    Err(S3Error::with_message(S3ErrorCode::NoSuchBucketPolicy, "No encryption configuration found for bucket"))
                }
                Err(e) => {
                    Err(S3Error::with_message(S3ErrorCode::InternalError, format!("Failed to retrieve encryption config: {}", e)))
                }
            }
        } else {
            Err(S3Error::with_message(S3ErrorCode::InternalError, "Bucket encryption manager not available"))
        }
    }
}

/// Handler for DELETE /bucket/{bucket}/encryption
pub struct DeleteBucketEncryptionHandler {
    pub fs: std::sync::Arc<FS>,
}

#[async_trait::async_trait]
impl Operation for DeleteBucketEncryptionHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        // Extract bucket name from path parameters
        let bucket_name = params.get("bucket").ok_or_else(|| {
            S3Error::with_message(S3ErrorCode::InvalidRequest, "Missing bucket name")
        })?;

        // Parse credentials from query parameters
        let input_cred: Credentials = serde_urlencoded::from_str(req.uri.query().unwrap_or(""))
            .map_err(|e| S3Error::with_message(S3ErrorCode::InvalidRequest, format!("Invalid query parameters: {}", e)))?;

        // Validate authentication
        check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        info!("Deleting bucket encryption for bucket: {}", bucket_name);
        
        // Delete the bucket encryption configuration using the manager
        if let Some(manager) = self.fs.bucket_encryption_manager() {
            match manager.delete_bucket_encryption(bucket_name).await {
                Ok(()) => {
                    Ok(S3Response::new((StatusCode::NO_CONTENT, Body::empty())))
                }
                Err(e) => {
                    Err(S3Error::with_message(S3ErrorCode::InternalError, format!("Failed to delete encryption config: {}", e)))
                }
            }
        } else {
            Err(S3Error::with_message(S3ErrorCode::InternalError, "Bucket encryption manager not available"))
        }
    }
}

/// Handler for GET /bucket/encryptions (list all bucket encryption configurations)
pub struct ListBucketEncryptionsHandler {
    pub fs: std::sync::Arc<FS>,
}

#[async_trait::async_trait]
impl Operation for ListBucketEncryptionsHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        // Parse credentials from query parameters
        let input_cred: Credentials = serde_urlencoded::from_str(req.uri.query().unwrap_or(""))
            .map_err(|e| S3Error::with_message(S3ErrorCode::InvalidRequest, format!("Invalid query parameters: {}", e)))?;

        // Validate authentication
        check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        info!("Listing all bucket encryption configurations");
        
        // List all bucket encryption configurations using the manager
        if let Some(manager) = self.fs.bucket_encryption_manager() {
            match manager.list_bucket_encryptions().await {
                Ok(configs) => {
                    let response = serde_json::json!({
                        "bucket_encryptions": configs
                    });
                    Ok(S3Response::new((StatusCode::OK, Body::from(response.to_string().into_bytes()))))
                }
                Err(e) => {
                    Err(S3Error::with_message(S3ErrorCode::InternalError, format!("Failed to list encryption configs: {}", e)))
                }
            }
        } else {
            Err(S3Error::with_message(S3ErrorCode::InternalError, "Bucket encryption manager not available"))
        }
    }
}