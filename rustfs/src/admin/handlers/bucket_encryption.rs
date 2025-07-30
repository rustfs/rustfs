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

use crate::{
    admin::router::Operation,
    auth::{check_key_valid, get_session_token},
};

use http::StatusCode;
use matchit::Params;
// Simplified bucket encryption types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BucketEncryptionAlgorithm {
    AES256,
    #[serde(rename = "aws:kms")]
    AwsKms,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketEncryptionConfig {
    pub enabled: bool,
    pub algorithm: BucketEncryptionAlgorithm,
    pub kms_key_id: String,
    pub encrypt_metadata: bool,
    pub encryption_context: std::collections::HashMap<String, String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

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
pub struct PutBucketEncryptionHandler {}

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

        // TODO: Implement actual bucket encryption configuration storage
        // For now, return success response
        info!("Setting bucket encryption for bucket: {}", bucket_name);
        info!("Encryption config: {:?}", config);
        
        let response_body = "Bucket encryption configuration set";
        Ok(S3Response::new((StatusCode::OK, Body::from(response_body.as_bytes().to_vec()))))
    }
}

/// Handler for GET /bucket/{bucket}/encryption
pub struct GetBucketEncryptionHandler {}

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

        // TODO: Implement actual bucket encryption configuration retrieval
        // For now, return a sample configuration
        info!("Getting bucket encryption for bucket: {}", bucket_name);
        
        let response = GetBucketEncryptionResponse {
            enabled: true,
            algorithm: BucketEncryptionAlgorithm::AES256,
            kms_key_id: "sample-key-id".to_string(),
            encrypt_metadata: false,
            encryption_context: std::collections::HashMap::new(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let response_json = serde_json::to_string(&response)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("Failed to serialize response: {}", e)))?;

        Ok(S3Response::new((StatusCode::OK, Body::from(response_json.into_bytes()))))
    }
}

/// Handler for DELETE /bucket/{bucket}/encryption
pub struct DeleteBucketEncryptionHandler {}

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

        // TODO: Implement actual bucket encryption configuration deletion
        // For now, return success response
        info!("Deleting bucket encryption for bucket: {}", bucket_name);
        
        let response_body = "Bucket encryption configuration deleted";
        Ok(S3Response::new((StatusCode::OK, Body::from(response_body.as_bytes().to_vec()))))
    }
}

/// Handler for GET /bucket/encryptions (list all bucket encryption configurations)
pub struct ListBucketEncryptionsHandler {}

#[async_trait::async_trait]
impl Operation for ListBucketEncryptionsHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        // Parse credentials from query parameters
        let input_cred: Credentials = serde_urlencoded::from_str(req.uri.query().unwrap_or(""))
            .map_err(|e| S3Error::with_message(S3ErrorCode::InvalidRequest, format!("Invalid query parameters: {}", e)))?;

        // Validate authentication
        check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        // TODO: Implement actual bucket encryption configurations listing
        // For now, return empty list
        info!("Listing all bucket encryption configurations");
        
        let response: std::collections::HashMap<String, GetBucketEncryptionResponse> = std::collections::HashMap::new();
        let response_json = serde_json::to_string(&response)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("Failed to serialize response: {}", e)))?;

        Ok(S3Response::new((StatusCode::OK, Body::from(response_json.into_bytes()))))
    }
}