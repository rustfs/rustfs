// sse.rs - Server-Side Encryption interfaces and common implementations
// This file implements the core interfaces for Server-Side Encryption in RustFS

use crate::Error;
use http::{HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::{Once, RwLock};
use tracing::debug;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};

#[cfg(feature = "kms")]
use std::sync::OnceLock;

// KMS client initialization - only available with kms feature
#[cfg(feature = "kms")]
static INIT_KMS_CLIENT: OnceLock<()> = OnceLock::new();

/// SSE specifies the type of server-side encryption used
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SSE {
    /// SSE-C indicates the object was encrypted using client-provided key
    SSEC,
    /// SSE-S3 indicates the object was encrypted using server-managed key
    SSES3,
    /// SSE-KMS indicates the object was encrypted using a KMS-managed key
    SSEKMS,
}

impl fmt::Display for SSE {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SSE::SSEC => write!(f, "SSE-C"),
            SSE::SSES3 => write!(f, "SSE-S3"),
            SSE::SSEKMS => write!(f, "SSE-KMS"),
        }
    }
}

/// Algorithm represents encryption algorithm used
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Algorithm {
    /// AES256 is the AES-256 encryption algorithm with GCM mode
    AES256,
    /// AWSKMS is the encryption algorithm using AWS KMS
    AWSKMS,
}

impl fmt::Display for Algorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Algorithm::AES256 => write!(f, "AES256"),
            Algorithm::AWSKMS => write!(f, "aws:kms"),
        }
    }
}

/// S3 SSE Headers - 使用小写以符合AWS S3标准
pub const SSE_HEADER: &str = "x-amz-server-side-encryption";
pub const SSE_C_HEADER: &str = "x-amz-server-side-encryption-customer-algorithm";
pub const SSE_C_KEY_HEADER: &str = "x-amz-server-side-encryption-customer-key";
pub const SSE_C_KEY_MD5_HEADER: &str = "x-amz-server-side-encryption-customer-key-md5";
pub const SSE_KMS_KEY_ID_HEADER: &str = "x-amz-server-side-encryption-aws-kms-key-id";
pub const SSE_KMS_CONTEXT_HEADER: &str = "x-amz-server-side-encryption-context";

/// SSE Copy Headers - 使用小写以符合AWS S3标准
pub const SSE_COPY_C_HEADER: &str = "x-amz-copy-source-server-side-encryption-customer-algorithm";
pub const SSE_COPY_C_KEY_HEADER: &str = "x-amz-copy-source-server-side-encryption-customer-key";
pub const SSE_COPY_C_KEY_MD5_HEADER: &str = "x-amz-copy-source-server-side-encryption-customer-key-md5";

/// SSEOptions contain encryption options specified by the user
#[derive(Clone, Debug, Default)]
pub struct SSEOptions {
    pub sse_type: Option<SSE>,
    pub algorithm: Option<Algorithm>,
    pub customer_key: Option<Vec<u8>>,
    pub customer_key_md5: Option<String>,
    pub kms_key_id: Option<String>,
    pub kms_context: Option<String>,
}

impl SSEOptions {
    /// Create a new empty SSEOptions
    pub fn new() -> Self {
        Default::default()
    }

    /// Extracts SSE options from request headers
    pub fn from_headers(headers: &HeaderMap) -> Result<Self, Error> {
        let mut options = Self::new();
        
        // Check for SSE-C
        if let Some(val) = headers.get(SSE_C_HEADER) {
            if val == "AES256" {
                options.sse_type = Some(SSE::SSEC);
                options.algorithm = Some(Algorithm::AES256);
                
                // SSE-C requires the customer-provided key
                if let Some(key) = headers.get(SSE_C_KEY_HEADER) {
                    let key_str = key.to_str().map_err(|_| Error::ErrInvalidSSECustomerKey)?;
                    match BASE64_STANDARD.decode(key_str.as_bytes()) {
                        Ok(decoded_key) => {
                            if decoded_key.len() != 32 {
                                return Err(Error::ErrInvalidSSECustomerKey);
                            }
                            options.customer_key = Some(decoded_key);
                        },
                        Err(_) => return Err(Error::ErrInvalidSSECustomerKey),
                    }
                } else {
                    return Err(Error::ErrMissingSSEKey);
                }
                
                // MD5 is optional but should be validated if provided
                if let Some(md5) = headers.get(SSE_C_KEY_MD5_HEADER) {
                    options.customer_key_md5 = Some(md5.to_str().unwrap_or("").to_string());
                    // TODO: Validate MD5 against the decoded key
                }
            } else {
                return Err(Error::ErrInvalidSSEAlgorithm);
            }
        }
        
        // Check for SSE-S3 or SSE-KMS
        if let Some(val) = headers.get(SSE_HEADER) {
            let val_str = val.to_str().unwrap_or("");
            
            if val_str == "AES256" {
                options.sse_type = Some(SSE::SSES3);
                options.algorithm = Some(Algorithm::AES256);
            } else if val_str == "aws:kms" {
                options.sse_type = Some(SSE::SSEKMS);
                options.algorithm = Some(Algorithm::AWSKMS);
                
                // KMS requires a key ID
                if let Some(key_id) = headers.get(SSE_KMS_KEY_ID_HEADER) {
                    options.kms_key_id = Some(key_id.to_str().unwrap_or("").to_string());
                }
                
                // KMS context is optional
                if let Some(context) = headers.get(SSE_KMS_CONTEXT_HEADER) {
                    options.kms_context = Some(context.to_str().unwrap_or("").to_string());
                }
            } else {
                return Err(Error::ErrInvalidSSEAlgorithm);
            }
        }
        
        Ok(options)
    }
    
    /// Extracts SSE options from copy source headers
    pub fn from_copy_source_headers(headers: &HeaderMap) -> Result<Self, Error> {
        let mut options = Self::new();
        
        // Check for SSE-C copy headers
        if let Some(val) = headers.get(SSE_COPY_C_HEADER) {
            if val == "AES256" {
                options.sse_type = Some(SSE::SSEC);
                options.algorithm = Some(Algorithm::AES256);
                
                // SSE-C requires the customer-provided key
                if let Some(key) = headers.get(SSE_COPY_C_KEY_HEADER) {
                    match BASE64_STANDARD.decode(key.as_bytes()) {
                        Ok(decoded_key) => {
                            options.customer_key = Some(decoded_key);
                        },
                        Err(_) => return Err(Error::ErrInvalidSSECustomerKey),
                    }
                } else {
                    return Err(Error::ErrMissingSSEKey);
                }
                
                // MD5 is optional but should be validated if provided
                if let Some(md5) = headers.get(SSE_COPY_C_KEY_MD5_HEADER) {
                    options.customer_key_md5 = Some(md5.to_str().unwrap_or("").to_string());
                    // TODO: Validate MD5 if present
                }
            } else {
                return Err(Error::ErrInvalidSSEAlgorithm);
            }
        }
        
        Ok(options)
    }

    /// Adds SSE headers to response based on options
    pub fn add_headers_to_response(&self, headers: &mut HeaderMap) {
        match self.sse_type {
            Some(SSE::SSES3) => {
                headers.insert(SSE_HEADER, HeaderValue::from_static("AES256"));
            }
            Some(SSE::SSEKMS) => {
                headers.insert(SSE_HEADER, HeaderValue::from_static("aws:kms"));
                if let Some(key_id) = &self.kms_key_id {
                    if let Ok(val) = HeaderValue::from_str(key_id) {
                        headers.insert(SSE_KMS_KEY_ID_HEADER, val);
                    }
                }
            }
            Some(SSE::SSEC) => {
                headers.insert(SSE_C_HEADER, HeaderValue::from_static("AES256"));
                // We don't include the key in the response, only the algorithm
            }
            None => {}
        }
    }
}

/// DefaultKMSConfig represents the default KMS configuration for encryption
#[derive(Clone, Debug)]
pub struct DefaultKMSConfig {
    pub endpoint: String, 
    pub key_id: String,
    pub token: String,
    pub ca_path: Option<String>,
    pub skip_tls_verify: bool,
    pub client_cert_path: Option<String>,
    pub client_key_path: Option<String>,
}

impl Default for DefaultKMSConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:8200".to_string(),
            key_id: "default".to_string(),
            token: "root".to_string(),
            ca_path: None,
            skip_tls_verify: false,
            client_cert_path: None,
            client_key_path: None,
        }
    }
}

#[allow(dead_code)]
// KMS initialization status tracking - using thread-safe RwLock instead of unsafe
static INIT_KMS: Once = Once::new();
static KMS_INIT_ERROR: RwLock<Option<String>> = RwLock::new(None);

lazy_static::lazy_static! {
    /// Default KMS configuration from environment variables
    static ref DEFAULT_KMS_CONFIG: Option<DefaultKMSConfig> = {
        // Check if KMS is enabled
        if std::env::var("RUSTFS_KMS_ENABLED").unwrap_or_default() != "true" {
            return None;
        }

        // Basic required parameters
        let endpoint = std::env::var("RUSTFS_KMS_VAULT_ENDPOINT").ok()?;
        let key_id = std::env::var("RUSTFS_KMS_VAULT_KEY_NAME").ok()?;
        let token = std::env::var("RUSTFS_KMS_VAULT_TOKEN").ok()?;
        
        // Optional TLS configuration parameters
        let ca_path = std::env::var("RUSTFS_KMS_VAULT_CAPATH").ok();
        let skip_tls_verify = std::env::var("RUSTFS_KMS_VAULT_SKIP_TLS_VERIFY")
            .map(|v| v == "true" || v == "1" || v == "yes")
            .unwrap_or(false);
        
        // Client certificate configuration
        let client_cert_path = std::env::var("RUSTFS_KMS_VAULT_CLIENT_CERT").ok();
        let client_key_path = std::env::var("RUSTFS_KMS_VAULT_CLIENT_KEY").ok();
        
        Some(DefaultKMSConfig {
            endpoint,
            key_id,
            token,
            ca_path,
            skip_tls_verify,
            client_cert_path,
            client_key_path
        })
    };
}

/// Get the default KMS configuration
pub fn get_default_kms_config() -> Option<DefaultKMSConfig> {
    DEFAULT_KMS_CONFIG.clone()
}

/// Check if KMS initialization was successful - thread-safe version
pub fn is_kms_initialized() -> bool {
    match get_default_kms_config() {
        Some(_) => match KMS_INIT_ERROR.read() {
            Ok(error) => error.is_none(),
            Err(_) => false, // If lock is poisoned, consider uninitialized
        },
        None => false,
    }
}

/// Get KMS initialization error if any - thread-safe version
pub fn get_kms_init_error() -> Option<String> {
    match KMS_INIT_ERROR.read() {
        Ok(error) => error.clone(),
        Err(_) => Some("Failed to read KMS init error status".to_string()),
    }
}

/// Initialize KMS client on system startup
#[cfg(feature = "kms")]
pub fn init_kms() {
    use ecstore::config::{GLOBAL_KmsConfig, kms};
    use crate::sse_kms::RustyVaultKMSClient;
    
    // Get KMS configuration from global config
    if let Some(kms_config) = GLOBAL_KmsConfig.get() {
        if kms_config.enabled {
            info!("Initializing KMS client with configuration");
            
            // Test KMS configuration
            let rt = tokio::runtime::Handle::try_current();
            let test_result = if let Ok(handle) = rt {
                // We're in an async context
                handle.block_on(async {
                    kms_config.test_connection().await
                })
            } else {
                // We're not in an async context, create a new runtime
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async {
                    kms_config.test_connection().await
                })
            };
            
            match test_result {
                Ok(_) => {
                    // Create and set global KMS client
                    let client = RustyVaultKMSClient::new(
                        kms_config.endpoint.clone(),
                        kms_config.token.clone(),
                        kms_config.key_name.clone(),
                    );
                    
                    if let Err(e) = RustyVaultKMSClient::set_global_client(client) {
                        let error_msg = format!("Failed to set global KMS client: {}", e);
                        error!("{}", error_msg);
                        if let Ok(mut kms_error) = KMS_INIT_ERROR.write() {
                            *kms_error = Some(error_msg);
                        }
                    } else {
                        info!("KMS client initialized successfully");
                        // Clear any previous errors
                        if let Ok(mut kms_error) = KMS_INIT_ERROR.write() {
                            *kms_error = None;
                        }
                    }
                }
                Err(e) => {
                    let error_msg = format!("KMS connection test failed: {}", e);
                    error!("{}", error_msg);
                    if let Ok(mut kms_error) = KMS_INIT_ERROR.write() {
                        *kms_error = Some(error_msg);
                    }
                }
            }
        } else {
            debug!("KMS is disabled in configuration");
        }
    } else {
        // Check environment variables for KMS configuration
        if std::env::var("RUSTFS_KMS_ENABLED").unwrap_or_default() == "true" {
            warn!("KMS enabled via environment variable but no configuration found");
            
            // Try to initialize with environment variables
            let kvs = kms::KVS::new(); // Empty KVS to trigger env var fallback
            match kms::lookup_config(&kvs) {
                Ok(config) => {
                    if config.enabled {
                        let client = RustyVaultKMSClient::new(
                            config.endpoint,
                            config.token,
                            config.key_name,
                        );
                        
                        if let Err(e) = RustyVaultKMSClient::set_global_client(client) {
                            let error_msg = format!("Failed to set global KMS client from env vars: {}", e);
                            error!("{}", error_msg);
                            if let Ok(mut kms_error) = KMS_INIT_ERROR.write() {
                                *kms_error = Some(error_msg);
                            }
                        } else {
                            info!("KMS client initialized from environment variables");
                        }
                    }
                }
                Err(e) => {
                    let error_msg = format!("Failed to parse KMS config from environment: {}", e);
                    error!("{}", error_msg);
                    if let Ok(mut kms_error) = KMS_INIT_ERROR.write() {
                        *kms_error = Some(error_msg);
                    }
                }
            }
        } else {
            debug!("KMS not enabled");
        }
    }
}

/// Lazy initialize KMS client when needed
#[cfg(feature = "kms")]
pub fn ensure_kms_client() -> Result<(), Error> {
    use crate::sse_kms::RustyVaultKMSClient;
    
    INIT_KMS_CLIENT.get_or_init(|| {
        // Initialize KMS client with default configuration or environment variables
        let config = get_default_kms_config().unwrap_or_default();
        
        let client = RustyVaultKMSClient::new(
            config.endpoint,
            config.token,
            config.key_id,
        );
        
        if let Err(e) = RustyVaultKMSClient::set_global_client(client) {
            eprintln!("Failed to set global KMS client: {}", e);
        }
    });
    
    Ok(())
}

#[cfg(not(feature = "kms"))]
pub fn ensure_kms_client() -> Result<(), Error> {
    Err(Error::ErrMissingKMSConfig)
}

/// No-op implementation when KMS feature is not enabled
#[cfg(not(feature = "kms"))]
pub fn init_kms() {
    debug!("KMS feature is not enabled");
}

/// Trait for objects that can be encrypted and decrypted
pub trait Encryptable {
    /// Encrypt data using the provided encryption method
    fn encrypt(&self, data: &[u8], options: &SSEOptions) -> Result<Vec<u8>, Error>;
    
    /// Decrypt data using the provided decryption method
    fn decrypt(&self, data: &[u8], options: &SSEOptions) -> Result<Vec<u8>, Error>;
}