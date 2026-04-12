//! S3 Authentication
//!
//! This module provides authentication support for S3 services, including AWS Signature
//! Version 4 and Version 2 verification.
//!
//! # Overview
//!
//! The authentication system verifies that incoming requests are signed with valid AWS
//! credentials. The main components are:
//!
//! - [`S3Auth`]: Trait for implementing authentication providers
//! - [`SimpleAuth`]: Simple in-memory authentication for testing and development
//! - [`SecretKey`]: Represents an AWS secret key
//! - [`Credentials`]: Represents authenticated credentials
//!
//! # Example
//!
//! ```
//! use s3s::auth::SimpleAuth;
//! use s3s::service::S3ServiceBuilder;
//! use s3s::{S3, S3Request, S3Response, S3Result};
//! use s3s::dto::{GetObjectInput, GetObjectOutput};
//!
//! #[derive(Clone)]
//! struct MyS3;
//!
//! #[async_trait::async_trait]
//! impl S3 for MyS3 {
//! #   async fn get_object(&self, _req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
//! #       Err(s3s::s3_error!(NotImplemented))
//! #   }
//!     // Implement S3 operations
//! }
//!
//! // Create an auth provider with a single user
//! let auth = SimpleAuth::from_single("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
//!
//! // Configure the service with authentication
//! let mut builder = S3ServiceBuilder::new(MyS3);
//! builder.set_auth(auth);
//! let service = builder.build();
//! ```
//!
//! # Custom Authentication
//!
//! You can implement custom authentication by implementing the [`S3Auth`] trait:
//!
//! ```
//! use s3s::auth::{S3Auth, SecretKey};
//! use s3s::S3Result;
//!
//! struct DatabaseAuth {
//!     // Your database connection
//! }
//!
//! #[async_trait::async_trait]
//! impl S3Auth for DatabaseAuth {
//!     async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
//!         // Query your database for the secret key
//!         // Return Err(s3_error!(InvalidAccessKeyId)) if not found
//! #       Err(s3s::s3_error!(InvalidAccessKeyId))
//!     }
//! }
//! ```
//!
//! # Security
//!
//! - Secret keys should be stored securely (e.g., in a secure database or secrets manager)
//! - Use HTTPS in production to prevent credential theft
//! - Rotate credentials regularly
//! - Use [`SimpleAuth`] only for testing, not production

mod secret_key;
pub use self::secret_key::{Credentials, SecretKey};

mod simple_auth;
pub use self::simple_auth::SimpleAuth;

use crate::error::S3Result;

/// S3 Authentication Provider
///
/// This trait defines the interface for authenticating S3 requests using AWS signatures.
/// Implementations should retrieve the secret key associated with an access key,
/// which is then used to verify the request signature.
///
/// # Example
///
/// ```
/// use s3s::auth::{S3Auth, SecretKey};
/// use s3s::S3Result;
///
/// struct MyAuth;
///
/// #[async_trait::async_trait]
/// impl S3Auth for MyAuth {
///     async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
///         // Look up the secret key for this access key
///         // This might involve a database query, API call, etc.
///         
///         if access_key == "AKIAIOSFODNN7EXAMPLE" {
///             Ok(SecretKey::from("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"))
///         } else {
///             Err(s3s::s3_error!(InvalidAccessKeyId))
///         }
///     }
/// }
/// ```
#[async_trait::async_trait]
pub trait S3Auth: Send + Sync + 'static {
    /// Gets the corresponding secret key for the given access key.
    ///
    /// This method is called during request authentication to retrieve the secret key
    /// needed to verify the request signature. Implementations typically query a
    /// database or credential store.
    ///
    /// # Arguments
    ///
    /// * `access_key` - The AWS access key ID from the request
    ///
    /// # Returns
    ///
    /// * `Ok(SecretKey)` - The secret key associated with the access key
    /// * `Err(S3Error)` - If the access key is not found or invalid
    ///
    /// # Errors
    ///
    /// Should return `InvalidAccessKeyId` error if the access key is not found.
    ///
    /// # Example
    ///
    /// ```
    /// use s3s::auth::{S3Auth, SecretKey};
    /// use s3s::S3Result;
    ///
    /// struct MyAuth;
    ///
    /// #[async_trait::async_trait]
    /// impl S3Auth for MyAuth {
    ///     async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
    ///         // In a real implementation, you might query a database:
    ///         // let secret = database.query("SELECT secret_key FROM users WHERE access_key = ?", access_key).await?;
    ///         
    ///         // For this example, just check a hardcoded key
    ///         if access_key == "test-key" {
    ///             Ok(SecretKey::from("test-secret"))
    ///         } else {
    ///             Err(s3s::s3_error!(InvalidAccessKeyId))
    ///         }
    ///     }
    /// }
    /// ```
    async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey>;
}
