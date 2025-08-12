//! KMS (Key Management Service) handlers for RustFS admin API
//!
//! This module provides handlers for managing KMS operations including:
//! - Dynamic KMS configuration
//! - Key creation, listing, and status management
//! - Key enable/disable operations
//! - KMS health status checking

use rustfs_filemeta::headers::RESERVED_METADATA_PREFIX_LOWER;
use std::collections::HashMap as StdHashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use hyper::StatusCode;
use matchit::Params;
use percent_encoding::percent_decode_str as decode;
use rustfs_kms::KmsError;
use s3s::{Body, S3Request, S3Response, S3Result};
use serde::Serialize;
use tracing::{error, info, warn};

use super::super::router::Operation;
use base64::Engine;
use rustfs_ecstore::global::new_object_layer_fn;
use rustfs_ecstore::store::ECStore;
use rustfs_ecstore::store_api::{BucketOptions, ObjectOptions, StorageAPI};

// ==================== Request/Response Structures ====================

/// KMS key creation response
#[derive(Debug, Serialize)]
pub struct CreateKeyResponse {
    #[serde(rename = "keyId")]
    pub key_id: String,
    #[serde(rename = "keyName")]
    pub key_name: String,
    #[serde(rename = "status")]
    pub status: String,
    #[serde(rename = "createdAt")]
    pub created_at: String,
}

/// KMS key status response
#[derive(Debug, Serialize)]
pub struct KeyStatusResponse {
    #[serde(rename = "keyId")]
    pub key_id: String,
    #[serde(rename = "keyName")]
    pub key_name: String,
    #[serde(rename = "status")]
    pub status: String,
    #[serde(rename = "createdAt")]
    pub created_at: String,
    #[serde(rename = "algorithm")]
    pub algorithm: String,
}

/// KMS keys list response
#[derive(Debug, Serialize)]
pub struct ListKeysResponse {
    #[serde(rename = "keys")]
    pub keys: Vec<KeyStatusResponse>,
}

/// KMS status response
#[derive(Debug, Serialize)]
pub struct KmsStatusResponse {
    #[serde(rename = "status")]
    pub status: String,
    #[serde(rename = "backend")]
    pub backend: String,
    #[serde(rename = "healthy")]
    pub healthy: bool,
}

/// KMS error response
#[derive(Debug, Serialize)]
pub struct KmsErrorResponse {
    #[serde(rename = "code")]
    pub code: String,
    #[serde(rename = "message")]
    pub message: String,
    #[serde(rename = "description")]
    pub description: String,
}

/// KMS configuration request
#[derive(serde::Deserialize, serde::Serialize)]
pub struct ConfigureKmsRequest {
    pub kms_type: String,
    pub vault_address: Option<String>,
    pub vault_token: Option<String>,
    pub vault_namespace: Option<String>,
    pub vault_mount_path: Option<String>,
    pub vault_timeout_seconds: Option<u64>,
    pub vault_app_role_id: Option<String>,
    pub vault_app_role_secret_id: Option<String>,
}

/// KMS configuration response
#[derive(serde::Serialize)]
pub struct ConfigureKmsResponse {
    pub success: bool,
    pub message: String,
    pub kms_type: String,
}

/// Rewrap request
#[derive(serde::Deserialize)]
pub struct RewrapRequest {
    /// Base64 of ciphertext with RustFS header
    pub ciphertext_b64: String,
    /// Optional JSON map for encryption context
    pub context: Option<StdHashMap<String, String>>,
}

/// Rewrap response
#[derive(serde::Serialize)]
pub struct RewrapResponse {
    pub ciphertext_b64: String,
}

/// Batch rewrap request for a bucket/prefix
#[derive(serde::Deserialize, Default)]
pub struct BatchRewrapRequest {
    /// Bucket to scan
    pub bucket: String,
    /// Optional prefix filter
    pub prefix: Option<String>,
    /// If false, use delimiter "/" to only list one level; default true (recursive)
    #[serde(default = "default_true")]
    pub recursive: bool,
    /// Page size for listing (1..=1000). Default 1000
    pub page_size: Option<i32>,
    /// Maximum number of objects to process in this call. If None, process all
    pub max_objects: Option<usize>,
    /// If true, don't modify anything, only count what would be rewrapped
    #[serde(default)]
    pub dry_run: bool,
}

fn default_true() -> bool {
    true
}

#[derive(serde::Serialize, Default)]
pub struct BatchRewrapResultItem {
    pub key: String,
    pub status: String,
    pub error: Option<String>,
}

/// Batch rewrap summary response
#[derive(serde::Serialize, Default)]
pub struct BatchRewrapResponse {
    pub bucket: String,
    pub prefix: Option<String>,
    pub processed: usize,
    pub rewrapped: usize,
    pub skipped: usize,
    pub errors: Vec<BatchRewrapResultItem>,
}

// ==================== Error Handling ====================

impl From<KmsError> for KmsErrorResponse {
    fn from(err: KmsError) -> Self {
        match err {
            KmsError::KeyNotFound { key_id } => KmsErrorResponse {
                code: "NoSuchKey".to_string(),
                message: format!("Key '{key_id}' not found"),
                description: "The specified key does not exist".to_string(),
            },
            KmsError::KeyExists { key_id } => KmsErrorResponse {
                code: "KeyAlreadyExists".to_string(),
                message: format!("Key '{key_id}' already exists"),
                description: "A key with this name already exists".to_string(),
            },
            KmsError::BackendError { service, message } => KmsErrorResponse {
                code: "BackendError".to_string(),
                message: format!("{service} backend error: {message}"),
                description: "KMS backend operation failed".to_string(),
            },
            _ => KmsErrorResponse {
                code: "InternalError".to_string(),
                message: err.to_string(),
                description: "Internal KMS error occurred".to_string(),
            },
        }
    }
}

// ==================== Helper Functions ====================

/// Extract query parameters from URL query string
fn kms_extract_query_params(query: &str) -> StdHashMap<String, String> {
    let mut params = StdHashMap::new();

    if query.is_empty() {
        return params;
    }

    for pair in query.split('&') {
        let mut parts = pair.splitn(2, '=');
        if let (Some(key), Some(value)) = (parts.next(), parts.next()) {
            let decoded_key = decode(key).decode_utf8_lossy();
            let decoded_value = decode(value).decode_utf8_lossy();
            params.insert(decoded_key.to_string(), decoded_value.to_string());
        }
    }

    params
}

/// Create a successful KMS response
fn kms_success_response<T: Serialize>(data: T) -> S3Response<(StatusCode, Body)> {
    match serde_json::to_vec(&data) {
        Ok(json) => S3Response::new((StatusCode::OK, Body::from(json))),
        Err(_) => S3Response::new((StatusCode::INTERNAL_SERVER_ERROR, Body::empty())),
    }
}

/// Create an error KMS response
fn kms_error_response(status: StatusCode, error: KmsErrorResponse) -> S3Response<(StatusCode, Body)> {
    match serde_json::to_vec(&error) {
        Ok(json) => S3Response::new((status, Body::from(json))),
        Err(_) => S3Response::new((StatusCode::INTERNAL_SERVER_ERROR, Body::empty())),
    }
}

/// Format SystemTime as RFC3339 string
fn format_system_time(time: SystemTime) -> String {
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => {
            let secs = duration.as_secs();
            let nanos = duration.subsec_nanos();
            let datetime = chrono::DateTime::<chrono::Utc>::from_timestamp(secs as i64, nanos);
            match datetime {
                Some(dt) => dt.to_rfc3339(),
                None => "1970-01-01T00:00:00Z".to_string(),
            }
        }
        Err(_) => "1970-01-01T00:00:00Z".to_string(),
    }
}

// ==================== KMS Operations ====================

/// Configure KMS handler
///
/// This handler allows dynamic configuration of the KMS at runtime.
/// It accepts a JSON payload with KMS configuration and sets up the global KMS instance.
pub struct ConfigureKms;

#[async_trait::async_trait]
impl Operation for ConfigureKms {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        info!("Processing KMS configuration request");

        // Parse request body
        let mut input = req.input;
        let body = match input.store_all_unlimited().await {
            Ok(b) => b,
            Err(e) => {
                error!("Failed to read request body: {}", e);
                return Ok(kms_error_response(
                    StatusCode::BAD_REQUEST,
                    KmsErrorResponse {
                        code: "InvalidRequest".to_string(),
                        message: "Failed to read request body".to_string(),
                        description: format!("Error: {e}"),
                    },
                ));
            }
        };

        let config_request: ConfigureKmsRequest = match serde_json::from_slice(&body) {
            Ok(req) => req,
            Err(err) => {
                error!("Failed to parse KMS configuration request: {}", err);
                return Ok(kms_error_response(
                    StatusCode::BAD_REQUEST,
                    KmsErrorResponse {
                        code: "InvalidRequest".to_string(),
                        message: "Invalid JSON format".to_string(),
                        description: format!("Error: {err}"),
                    },
                ));
            }
        };

        // Create KMS configuration
        let kms_config = match config_request.kms_type.as_str() {
            "vault" => {
                use rustfs_kms::{KmsConfig, KmsType};

                let vault_address = match config_request.vault_address {
                    Some(addr) => addr,
                    None => {
                        return Ok(kms_error_response(
                            StatusCode::BAD_REQUEST,
                            KmsErrorResponse {
                                code: "InvalidConfiguration".to_string(),
                                message: "vault_address is required for Vault KMS".to_string(),
                                description: "Missing vault_address field".to_string(),
                            },
                        ));
                    }
                };

                let auth_method = if let (Some(role_id), Some(secret_id)) =
                    (config_request.vault_app_role_id, config_request.vault_app_role_secret_id)
                {
                    rustfs_kms::VaultAuthMethod::AppRole { role_id, secret_id }
                } else if let Some(token) = config_request.vault_token {
                    rustfs_kms::VaultAuthMethod::Token { token }
                } else {
                    return Ok(kms_error_response(
                        StatusCode::BAD_REQUEST,
                        KmsErrorResponse {
                            code: "InvalidConfiguration".to_string(),
                            message: "Either vault_token or both vault_app_role_id and vault_app_role_secret_id must be provided"
                                .to_string(),
                            description: "Vault authentication method not specified".to_string(),
                        },
                    ));
                };

                KmsConfig {
                    kms_type: KmsType::Vault,
                    default_key_id: None,
                    backend_config: rustfs_kms::BackendConfig::Vault(Box::new(rustfs_kms::VaultConfig {
                        address: match vault_address.parse() {
                            Ok(url) => url,
                            Err(e) => {
                                return Ok(kms_error_response(
                                    StatusCode::BAD_REQUEST,
                                    KmsErrorResponse {
                                        code: "InvalidConfiguration".to_string(),
                                        message: "Invalid vault address format".to_string(),
                                        description: format!("Error parsing URL: {e}"),
                                    },
                                ));
                            }
                        },
                        auth_method,
                        namespace: config_request.vault_namespace,
                        mount_path: config_request.vault_mount_path.unwrap_or_else(|| "transit".to_string()),
                        tls_config: None,
                        headers: std::collections::HashMap::new(),
                    })),
                    timeout_secs: config_request.vault_timeout_seconds.unwrap_or(30),
                    retry_attempts: 3,
                    enable_audit: true,
                    audit_log_path: None,
                }
            }
            "local" => {
                use rustfs_kms::{KmsConfig, KmsType};
                KmsConfig {
                    kms_type: KmsType::Local,
                    default_key_id: None,
                    backend_config: rustfs_kms::BackendConfig::Local(rustfs_kms::LocalConfig::default()),
                    timeout_secs: 30,
                    retry_attempts: 3,
                    enable_audit: true,
                    audit_log_path: None,
                }
            }
            _ => {
                return Ok(kms_error_response(
                    StatusCode::BAD_REQUEST,
                    KmsErrorResponse {
                        code: "UnsupportedKmsType".to_string(),
                        message: format!("Unsupported KMS type: {}", config_request.kms_type),
                        description: "Supported types: vault, local".to_string(),
                    },
                ));
            }
        };

        // Create and configure KMS manager
        match rustfs_kms::KmsManager::new(kms_config).await {
            Ok(kms_manager) => {
                let kms_manager = std::sync::Arc::new(kms_manager);

                // Configure global KMS
                match rustfs_kms::configure_global_kms(kms_manager.clone()) {
                    Ok(()) => {
                        info!("Successfully configured KMS with type: {}", config_request.kms_type);

                        // Initialize encryption service
                        let encryption_service =
                            std::sync::Arc::new(rustfs_kms::ObjectEncryptionService::new((*kms_manager).clone()));

                        // Initialize global encryption service
                        if let Err(err) = rustfs_kms::init_global_encryption_service(encryption_service) {
                            error!("Failed to initialize encryption service: {}", err);
                            return Ok(kms_error_response(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                KmsErrorResponse {
                                    code: "ConfigurationFailed".to_string(),
                                    message: "Failed to initialize encryption service".to_string(),
                                    description: format!("Error: {err}"),
                                },
                            ));
                        }

                        let response = ConfigureKmsResponse {
                            success: true,
                            message: "KMS configured successfully".to_string(),
                            kms_type: config_request.kms_type,
                        };
                        Ok(kms_success_response(response))
                    }
                    Err(err) => {
                        error!("Failed to configure global KMS: {}", err);
                        Ok(kms_error_response(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            KmsErrorResponse {
                                code: "ConfigurationFailed".to_string(),
                                message: "Failed to configure global KMS".to_string(),
                                description: format!("Error: {err}"),
                            },
                        ))
                    }
                }
            }
            Err(err) => {
                error!("Failed to create KMS manager: {}", err);
                Ok(kms_error_response(
                    StatusCode::BAD_REQUEST,
                    KmsErrorResponse {
                        code: "InvalidConfiguration".to_string(),
                        message: "Failed to create KMS manager".to_string(),
                        description: format!("Error: {err}"),
                    },
                ))
            }
        }
    }
}

/// Create a new KMS master key
/// POST /rustfs/admin/v3/kms/key/create
pub struct CreateKmsKey;

#[async_trait::async_trait]
impl Operation for CreateKmsKey {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        info!("Processing KMS key creation request");

        // Extract key name from query parameters
        let query_params = kms_extract_query_params(req.uri.query().unwrap_or(""));
        let key_name = query_params
            .get("keyName")
            .or_else(|| query_params.get("key"))
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("rustfs-key-{}", uuid::Uuid::new_v4()));
        let algorithm = query_params
            .get("algorithm")
            .map(|s| s.to_string())
            .unwrap_or_else(|| "AES-256".to_string());

        // Get global KMS instance
        let kms = match rustfs_kms::get_global_kms() {
            Some(kms) => kms,
            None => {
                warn!("KMS not configured");
                return Ok(kms_error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    KmsErrorResponse {
                        code: "KMSNotConfigured".to_string(),
                        message: "KMS is not configured".to_string(),
                        description: "Key Management Service is not available".to_string(),
                    },
                ));
            }
        };

        // Create the key
        match kms.create_key(&key_name, &algorithm, None).await {
            Ok(key_info) => {
                info!("Successfully created KMS key: {}", key_info.key_id);
                let response = CreateKeyResponse {
                    key_id: key_info.key_id.clone(),
                    key_name: key_info.key_id.clone(), // MasterKey uses key_id as name
                    status: format!("{:?}", key_info.status),
                    created_at: format_system_time(key_info.created_at),
                };
                Ok(kms_success_response(response))
            }
            Err(err) => {
                error!("Failed to create KMS key '{}': {}", key_name, err);
                Ok(kms_error_response(StatusCode::BAD_REQUEST, KmsErrorResponse::from(err)))
            }
        }
    }
}

/// Get KMS key status
/// GET /rustfs/admin/v3/kms/key/status
pub struct GetKmsKeyStatus;

#[async_trait::async_trait]
impl Operation for GetKmsKeyStatus {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        info!("Processing KMS key status request");

        // Extract key name from query parameters
        let query_params = kms_extract_query_params(req.uri.query().unwrap_or(""));
        let key_name = match query_params.get("keyName").or_else(|| query_params.get("key")) {
            Some(name) => name.to_string(),
            None => {
                warn!("Key name not provided in request");
                return Ok(kms_error_response(
                    StatusCode::BAD_REQUEST,
                    KmsErrorResponse {
                        code: "MissingParameter".to_string(),
                        message: "Key name is required".to_string(),
                        description: "keyName parameter must be provided".to_string(),
                    },
                ));
            }
        };

        // Get global KMS instance
        let kms = match rustfs_kms::get_global_kms() {
            Some(kms) => kms,
            None => {
                warn!("KMS not configured");
                return Ok(kms_error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    KmsErrorResponse {
                        code: "KMSNotConfigured".to_string(),
                        message: "KMS is not configured".to_string(),
                        description: "Key Management Service is not available".to_string(),
                    },
                ));
            }
        };

        // Get key information
        match kms.describe_key(&key_name, None).await {
            Ok(key_info) => {
                info!("Successfully retrieved KMS key info: {}", key_info.key_id);
                let response = KeyStatusResponse {
                    key_id: key_info.key_id.clone(),
                    key_name: key_info.name.clone(),
                    status: format!("{:?}", key_info.status),
                    created_at: format_system_time(key_info.created_at),
                    algorithm: key_info.algorithm.clone(),
                };
                Ok(kms_success_response(response))
            }
            Err(err) => {
                error!("Failed to get KMS key '{}' info: {}", key_name, err);
                Ok(kms_error_response(StatusCode::NOT_FOUND, KmsErrorResponse::from(err)))
            }
        }
    }
}

/// List all KMS keys
/// GET /rustfs/admin/v3/kms/key/list
pub struct ListKmsKeys;

#[async_trait::async_trait]
impl Operation for ListKmsKeys {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        info!("Processing KMS key list request");

        // Get global KMS instance
        let kms = match rustfs_kms::get_global_kms() {
            Some(kms) => kms,
            None => {
                warn!("KMS not configured");
                return Ok(kms_error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    KmsErrorResponse {
                        code: "KMSNotConfigured".to_string(),
                        message: "KMS is not configured".to_string(),
                        description: "Key Management Service is not available".to_string(),
                    },
                ));
            }
        };

        // List keys
        match kms.list_keys(&rustfs_kms::ListKeysRequest::default(), None).await {
            Ok(list_response) => {
                info!("Successfully listed {} KMS keys", list_response.keys.len());
                let keys: Vec<KeyStatusResponse> = list_response
                    .keys
                    .into_iter()
                    .map(|key_info| KeyStatusResponse {
                        key_id: key_info.key_id.clone(),
                        key_name: key_info.name.clone(),
                        status: format!("{:?}", key_info.status),
                        created_at: format_system_time(key_info.created_at),
                        algorithm: key_info.algorithm.clone(),
                    })
                    .collect();

                let response = ListKeysResponse { keys };
                Ok(kms_success_response(response))
            }
            Err(err) => {
                error!("Failed to list KMS keys: {}", err);
                Ok(kms_error_response(StatusCode::INTERNAL_SERVER_ERROR, KmsErrorResponse::from(err)))
            }
        }
    }
}

/// Rotate a KMS key
/// POST /rustfs/admin/v3/kms/key/rotate
pub struct RotateKmsKey;

#[async_trait::async_trait]
impl Operation for RotateKmsKey {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        info!("Processing KMS key rotate request");

        // Extract key name from query parameters
        let query_params = kms_extract_query_params(req.uri.query().unwrap_or(""));
        let key_name = match query_params.get("keyName").or_else(|| query_params.get("key")) {
            Some(name) => name.to_string(),
            None => {
                return Ok(kms_error_response(
                    StatusCode::BAD_REQUEST,
                    KmsErrorResponse {
                        code: "MissingParameter".to_string(),
                        message: "Key name is required".to_string(),
                        description: "keyName parameter must be provided".to_string(),
                    },
                ));
            }
        };

        // Get global KMS instance
        let kms = match rustfs_kms::get_global_kms() {
            Some(kms) => kms,
            None => {
                return Ok(kms_error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    KmsErrorResponse {
                        code: "KMSNotConfigured".to_string(),
                        message: "KMS is not configured".to_string(),
                        description: "Key Management Service is not available".to_string(),
                    },
                ));
            }
        };

        match kms.rotate_key(&key_name, None).await {
            Ok(key_info) => {
                info!("Successfully rotated KMS key: {}", key_name);
                let response = KeyStatusResponse {
                    key_id: key_info.key_id.clone(),
                    key_name: key_info.key_id.clone(),
                    status: format!("{:?}", key_info.status),
                    created_at: format_system_time(key_info.created_at),
                    algorithm: key_info.algorithm.clone(),
                };
                Ok(kms_success_response(response))
            }
            Err(err) => {
                error!("Failed to rotate KMS key '{}': {}", key_name, err);
                Ok(kms_error_response(StatusCode::BAD_REQUEST, KmsErrorResponse::from(err)))
            }
        }
    }
}

/// Enable KMS key
/// PUT /rustfs/admin/v3/kms/key/enable
pub struct EnableKmsKey;

#[async_trait::async_trait]
impl Operation for EnableKmsKey {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        info!("Processing KMS key enable request");

        let query_params = kms_extract_query_params(req.uri.query().unwrap_or(""));
        let key_name = match query_params.get("keyName").or_else(|| query_params.get("key")) {
            Some(name) => name.to_string(),
            None => {
                return Ok(kms_error_response(
                    StatusCode::BAD_REQUEST,
                    KmsErrorResponse {
                        code: "MissingParameter".to_string(),
                        message: "Key name is required".to_string(),
                        description: "keyName parameter must be provided".to_string(),
                    },
                ));
            }
        };

        let kms = match rustfs_kms::get_global_kms() {
            Some(kms) => kms,
            None => {
                return Ok(kms_error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    KmsErrorResponse {
                        code: "KMSNotConfigured".to_string(),
                        message: "KMS is not configured".to_string(),
                        description: "Key Management Service is not available".to_string(),
                    },
                ));
            }
        };

        match kms.enable_key(&key_name, None).await {
            Ok(_) => {
                info!("Successfully enabled KMS key: {}", key_name);
                Ok(S3Response::new((StatusCode::OK, Body::empty())))
            }
            Err(err) => {
                error!("Failed to enable KMS key '{}': {}", key_name, err);
                Ok(kms_error_response(StatusCode::BAD_REQUEST, KmsErrorResponse::from(err)))
            }
        }
    }
}

/// Disable KMS key
/// PUT /rustfs/admin/v3/kms/key/disable
pub struct DisableKmsKey;

#[async_trait::async_trait]
impl Operation for DisableKmsKey {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        info!("Processing KMS key disable request");

        let query_params = kms_extract_query_params(req.uri.query().unwrap_or(""));
        let key_name = match query_params.get("keyName").or_else(|| query_params.get("key")) {
            Some(name) => name.to_string(),
            None => {
                return Ok(kms_error_response(
                    StatusCode::BAD_REQUEST,
                    KmsErrorResponse {
                        code: "MissingParameter".to_string(),
                        message: "Key name is required".to_string(),
                        description: "keyName parameter must be provided".to_string(),
                    },
                ));
            }
        };

        let kms = match rustfs_kms::get_global_kms() {
            Some(kms) => kms,
            None => {
                return Ok(kms_error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    KmsErrorResponse {
                        code: "KMSNotConfigured".to_string(),
                        message: "KMS is not configured".to_string(),
                        description: "Key Management Service is not available".to_string(),
                    },
                ));
            }
        };

        // 对 Vault 后端返回更清晰的提示
        let backend = kms.backend_info().backend_type;
        if backend.eq_ignore_ascii_case("vault") {
            return Ok(kms_error_response(
                StatusCode::NOT_IMPLEMENTED,
                KmsErrorResponse {
                    code: "OperationNotSupported".to_string(),
                    message: "Disable is not supported by Vault transit engine".to_string(),
                    description: "Vault Transit 不支持显式禁用密钥，如需阻止使用请调整策略或移除密钥".to_string(),
                },
            ));
        }

        match kms.disable_key(&key_name, None).await {
            Ok(_) => {
                info!("Successfully disabled KMS key: {}", key_name);
                Ok(S3Response::new((StatusCode::OK, Body::empty())))
            }
            Err(err) => {
                error!("Failed to disable KMS key '{}': {}", key_name, err);
                Ok(kms_error_response(StatusCode::BAD_REQUEST, KmsErrorResponse::from(err)))
            }
        }
    }
}

/// Get KMS status
/// GET /rustfs/admin/v3/kms/status
pub struct GetKmsStatus;

#[async_trait::async_trait]
impl Operation for GetKmsStatus {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        info!("Processing KMS status request");

        let kms = match rustfs_kms::get_global_kms() {
            Some(kms) => kms,
            None => {
                return Ok(kms_error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    KmsErrorResponse {
                        code: "KMSNotConfigured".to_string(),
                        message: "KMS is not configured".to_string(),
                        description: "Key Management Service is not available".to_string(),
                    },
                ));
            }
        };

        let backend = kms.backend_info().backend_type;
        match kms.health_check_with_encryption_status().await {
            Ok(hs) => {
                let healthy = hs.kms_healthy && hs.encryption_working;
                let status = if healthy {
                    "OK"
                } else if hs.kms_healthy {
                    "Degraded"
                } else {
                    "Failed"
                };
                let response = KmsStatusResponse {
                    status: status.to_string(),
                    backend,
                    healthy,
                };
                Ok(kms_success_response(response))
            }
            Err(err) => {
                error!("KMS health check failed: {}", err);
                let response = KmsStatusResponse {
                    status: "Failed".to_string(),
                    backend,
                    healthy: false,
                };
                Ok(kms_success_response(response))
            }
        }
    }
}

/// Rewrap wrapped data key (or any KMS ciphertext with RustFS header)
/// POST /rustfs/admin/v3/kms/rewrap
pub struct RewrapCiphertext;

#[async_trait::async_trait]
impl Operation for RewrapCiphertext {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let kms = match rustfs_kms::get_global_kms() {
            Some(kms) => kms,
            None => {
                return Ok(kms_error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    KmsErrorResponse {
                        code: "KMSNotConfigured".to_string(),
                        message: "KMS is not configured".to_string(),
                        description: "Key Management Service is not available".to_string(),
                    },
                ));
            }
        };

        let mut input = req.input;
        let body = match input.store_all_unlimited().await {
            Ok(b) => b,
            Err(e) => {
                return Ok(kms_error_response(
                    StatusCode::BAD_REQUEST,
                    KmsErrorResponse {
                        code: "InvalidRequest".to_string(),
                        message: "Failed to read body".to_string(),
                        description: e.to_string(),
                    },
                ));
            }
        };
        let payload: RewrapRequest = match serde_json::from_slice(&body) {
            Ok(v) => v,
            Err(e) => {
                return Ok(kms_error_response(
                    StatusCode::BAD_REQUEST,
                    KmsErrorResponse {
                        code: "InvalidJSON".to_string(),
                        message: "Invalid JSON".to_string(),
                        description: e.to_string(),
                    },
                ));
            }
        };

        let ciphertext = match base64::engine::general_purpose::STANDARD.decode(payload.ciphertext_b64.as_bytes()) {
            Ok(v) => v,
            Err(e) => {
                return Ok(kms_error_response(
                    StatusCode::BAD_REQUEST,
                    KmsErrorResponse {
                        code: "InvalidBase64".to_string(),
                        message: "ciphertext_b64 is invalid".to_string(),
                        description: e.to_string(),
                    },
                ));
            }
        };
        let context = payload.context.unwrap_or_default();

        match kms.rewrap_ciphertext(&ciphertext, &context).await {
            Ok(new_ct) => {
                let b64 = base64::engine::general_purpose::STANDARD.encode(new_ct);
                Ok(kms_success_response(RewrapResponse { ciphertext_b64: b64 }))
            }
            Err(err) => Ok(kms_error_response(StatusCode::BAD_REQUEST, KmsErrorResponse::from(err))),
        }
    }
}

/// Batch rewrap all encrypted objects in a bucket (optionally under a prefix)
/// POST /rustfs/admin/v3/kms/rewrap-bucket
pub struct BatchRewrapBucket;

#[async_trait::async_trait]
impl Operation for BatchRewrapBucket {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let kms = match rustfs_kms::get_global_kms() {
            Some(kms) => kms,
            None => {
                return Ok(kms_error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    KmsErrorResponse {
                        code: "KMSNotConfigured".to_string(),
                        message: "KMS is not configured".to_string(),
                        description: "Key Management Service is not available".to_string(),
                    },
                ));
            }
        };

        let Some(store) = new_object_layer_fn() else {
            return Ok(kms_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                KmsErrorResponse {
                    code: "StorageNotInitialized".to_string(),
                    message: "Object storage is not initialized".to_string(),
                    description: "ECStore is not available".to_string(),
                },
            ));
        };

        // Parse request body
        let mut input = req.input;
        let body = match input.store_all_unlimited().await {
            Ok(b) => b,
            Err(e) => {
                return Ok(kms_error_response(
                    StatusCode::BAD_REQUEST,
                    KmsErrorResponse {
                        code: "InvalidRequest".to_string(),
                        message: "Failed to read body".to_string(),
                        description: e.to_string(),
                    },
                ));
            }
        };
        let payload: BatchRewrapRequest = match serde_json::from_slice(&body) {
            Ok(v) => v,
            Err(e) => {
                return Ok(kms_error_response(
                    StatusCode::BAD_REQUEST,
                    KmsErrorResponse {
                        code: "InvalidJSON".to_string(),
                        message: "Invalid JSON".to_string(),
                        description: e.to_string(),
                    },
                ));
            }
        };

        let bucket = payload.bucket.clone();
        let prefix = payload.prefix.clone().unwrap_or_default();
        let recursive = payload.recursive;
        let page_size = payload.page_size.unwrap_or(1000).clamp(1, 1000);
        let max_objects = payload.max_objects.unwrap_or(usize::MAX);
        let dry_run = payload.dry_run;

        // Ensure bucket exists
        if let Err(e) = <ECStore as StorageAPI>::get_bucket_info(store.as_ref(), &bucket, &BucketOptions::default()).await {
            return Ok(kms_error_response(
                StatusCode::BAD_REQUEST,
                KmsErrorResponse {
                    code: "NoSuchBucket".to_string(),
                    message: format!("Bucket '{bucket}' not found"),
                    description: e.to_string(),
                },
            ));
        }

        let mut processed: usize = 0;
        let mut rewrapped: usize = 0;
        let mut skipped: usize = 0;
        let mut errors: Vec<BatchRewrapResultItem> = Vec::new();
        let mut continuation: Option<String> = None;

        loop {
            let delimiter = if recursive { None } else { Some("/".to_string()) };
            let page = match <ECStore as StorageAPI>::list_objects_v2(
                store.clone(),
                &bucket,
                &prefix,
                continuation.clone(),
                delimiter,
                page_size,
                false,
                None,
            )
            .await
            {
                Ok(p) => p,
                Err(e) => {
                    return Ok(kms_error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        KmsErrorResponse {
                            code: "ListFailed".to_string(),
                            message: "Failed to list objects".to_string(),
                            description: e.to_string(),
                        },
                    ));
                }
            };

            for oi in page.objects.into_iter() {
                if processed >= max_objects {
                    break;
                }
                if oi.is_dir {
                    continue;
                }

                processed += 1;

                // Prefer internal sealed context
                let mut enc_ctx: StdHashMap<String, String> = StdHashMap::new();
                if let Some(json) = oi
                    .user_defined
                    .get(&format!("{RESERVED_METADATA_PREFIX_LOWER}{}", "sse-context"))
                {
                    if let Ok(map) = serde_json::from_str::<StdHashMap<String, String>>(json) {
                        enc_ctx.extend(map);
                    }
                }
                // Ensure bucket/key are present to bind AAD
                if !enc_ctx.contains_key("bucket") {
                    enc_ctx.insert("bucket".to_string(), bucket.clone());
                }
                if !enc_ctx.contains_key("key") {
                    enc_ctx.insert("key".to_string(), oi.name.clone());
                }

                // Read sealed wrapped DEK from internal field
                let Some(wrapped_b64) = oi.user_defined.get(&format!("{RESERVED_METADATA_PREFIX_LOWER}{}", "sse-key")) else {
                    skipped += 1;
                    continue;
                };

                if dry_run {
                    // Count as rewrapped candidate, no changes
                    rewrapped += 1;
                    continue;
                }

                let wrapped_bytes = match base64::engine::general_purpose::STANDARD.decode(wrapped_b64.as_bytes()) {
                    Ok(b) => b,
                    Err(e) => {
                        errors.push(BatchRewrapResultItem {
                            key: oi.name.clone(),
                            status: "base64_error".to_string(),
                            error: Some(e.to_string()),
                        });
                        continue;
                    }
                };

                match kms.rewrap_ciphertext(&wrapped_bytes, &enc_ctx).await {
                    Ok(new_ct) => {
                        let new_b64 = base64::engine::general_purpose::STANDARD.encode(new_ct);
                        let mut md = StdHashMap::new();
                        md.insert(format!("{RESERVED_METADATA_PREFIX_LOWER}{}", "sse-key"), new_b64);

                        let popts = ObjectOptions {
                            version_id: oi.version_id.map(|v| v.to_string()),
                            eval_metadata: Some(md),
                            ..Default::default()
                        };

                        match <ECStore as StorageAPI>::put_object_metadata(store.as_ref(), &bucket, &oi.name, &popts).await {
                            Ok(_) => {
                                rewrapped += 1;
                            }
                            Err(e) => {
                                errors.push(BatchRewrapResultItem {
                                    key: oi.name.clone(),
                                    status: "update_failed".to_string(),
                                    error: Some(e.to_string()),
                                });
                            }
                        }
                    }
                    Err(e) => {
                        errors.push(BatchRewrapResultItem {
                            key: oi.name.clone(),
                            status: "rewrap_failed".to_string(),
                            error: Some(e.to_string()),
                        });
                    }
                }
            }

            if processed >= max_objects {
                break;
            }

            if page.is_truncated {
                continuation = page.next_continuation_token;
                if continuation.is_none() {
                    break;
                }
            } else {
                break;
            }
        }

        let response = BatchRewrapResponse {
            bucket,
            prefix: if prefix.is_empty() { None } else { Some(prefix) },
            processed,
            rewrapped,
            skipped,
            errors,
        };
        Ok(kms_success_response(response))
    }
}
