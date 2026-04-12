//! S3 Service Adapter
//!
//! `s3s` is an ergonomic adapter for building S3-compatible services. It implements the
//! Amazon S3 REST API as a generic [hyper](https://github.com/hyperium/hyper) service,
//! allowing S3-compatible services to focus on the S3 API itself without worrying about
//! the HTTP layer.
//!
//! # Features
//!
//! - **S3 REST API Implementation**: Comprehensive support for Amazon S3 REST API
//! - **HTTP Layer Abstraction**: Built on top of [hyper](https://github.com/hyperium/hyper) and [tower](https://github.com/tower-rs/tower)
//! - **Type Safety**: Generated data types from AWS Smithy models
//! - **Authentication**: Support for AWS Signature Version 4 and Version 2
//! - **Flexible Configuration**: Customizable service configuration with hot-reload support
//! - **Extensibility**: Custom routes, access control, and validation
//!
//! # Architecture
//!
//! The `s3s` crate converts HTTP requests to S3 operation inputs, calls user-defined
//! services, and converts operation outputs or errors back to HTTP responses. This allows
//! you to implement just the S3 business logic while `s3s` handles all the HTTP protocol
//! details.
//!
//! # Getting Started
//!
//! To build an S3-compatible service:
//!
//! 1. Implement the [`S3`] trait for your service
//! 2. Create an [`S3Service`](service::S3Service) using [`S3ServiceBuilder`](service::S3ServiceBuilder)
//! 3. Configure optional components (auth, access control, etc.)
//! 4. Serve the service using hyper or your favorite HTTP framework
//!
//! # Example
//!
//! ```rust,no_run
//! use s3s::{S3, S3Request, S3Response, S3Result};
//! use s3s::service::S3ServiceBuilder;
//! use s3s::dto::{GetObjectInput, GetObjectOutput};
//!
//! // 1. Implement the S3 trait
//! #[derive(Clone)]
//! struct MyS3Service;
//!
//! #[async_trait::async_trait]
//! impl S3 for MyS3Service {
//!     async fn get_object(
//!         &self,
//!         req: S3Request<GetObjectInput>
//!     ) -> S3Result<S3Response<GetObjectOutput>> {
//!         // Your implementation here
//!         Err(s3s::s3_error!(NotImplemented))
//!     }
//!     // Implement other S3 operations as needed
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // 2. Create the S3 service
//!     let service = S3ServiceBuilder::new(MyS3Service).build();
//!
//!     // 3. Serve it (example with hyper)
//!     // See the examples directory for complete server implementations
//!     Ok(())
//! }
//! ```
//!
//! # Modules
//!
//! - [`service`]: Core service implementation and builder
//! - [`auth`]: S3 authentication (Signature V4, Signature V2)
//! - [`access`]: Access control and authorization
//! - [`config`]: Service configuration and settings
//! - [`dto`]: Data transfer objects (generated from AWS Smithy models)
//! - [`host`]: Virtual host parsing and handling
//! - [`route`]: Custom route support
//! - [`validation`]: Bucket and object name validation
//! - [`stream`]: Streaming utilities
//! - [`checksum`]: Checksum algorithms
//! - [`crypto`]: Cryptographic utilities
//! - [`header`]: HTTP header handling
//! - [`path`]: S3 path handling
//! - [`post_policy`]: POST object policy support
//! - [`region`]: AWS region name type
//! - [`xml`]: XML serialization/deserialization
//!
//! # Security
//!
//! ⚠️ **Important**: `S3Service` and other adapters in this crate have no built-in security
//! protection. If exposed to the Internet directly, they may be vulnerable to attacks.
//!
//! It is the user's responsibility to implement security enhancements such as:
//! - HTTP body length limits
//! - Rate limiting
//! - Back pressure
//! - Network-level security (firewalls, VPNs, etc.)
//!
//! # Examples
//!
//! The crate includes several examples demonstrating different use cases:
//!
//! - `axum`: Integration with the Axum web framework
//! - `https`: Running an S3 service with HTTPS/TLS
//! - See the `examples` directory for more
//!
//! # Integration with aws-sdk-s3
//!
//! For integration with the official AWS SDK and useful types, see the
//! [`s3s-aws`](https://docs.rs/s3s-aws) crate.
//!
//! # Sample Implementation
//!
//! For a sample implementation and testing, see the
//! [`s3s-fs`](https://docs.rs/s3s-fs) crate, which implements the S3 API
//! on top of a file system.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![allow(
    clippy::bool_assert_comparison,  // I don't like `assert!(!expression)`. It's very misleading.
    clippy::multiple_crate_versions, // Sometimes not fixable
    clippy::module_name_repetitions,
    clippy::single_match_else,
    clippy::wildcard_imports,
    clippy::let_underscore_untyped,
    clippy::inline_always,
    clippy::needless_continue,
)]

#[macro_use]
mod utils;

#[macro_use]
mod error;

mod http;
mod ops;
mod protocol;
mod s3_op;
mod s3_trait;
mod sig_v2;
mod sig_v4;
mod time;

pub mod access;
pub mod auth;
pub mod checksum;
pub mod config;
pub mod crypto;
pub mod dto;
pub mod header;
pub mod host;
pub mod path;
pub mod post_policy;
pub mod region;
pub mod route;
pub mod service;
pub mod stream;
pub mod validation;
pub mod xml;

pub use self::error::*;
pub use self::http::Body;
pub use self::s3_op::S3Operation;
pub use self::s3_trait::S3;

pub use self::protocol::HttpError;
pub use self::protocol::HttpRequest;
pub use self::protocol::HttpResponse;
pub use self::protocol::S3Request;
pub use self::protocol::S3Response;
pub use self::protocol::TrailingHeaders;
