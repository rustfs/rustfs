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

//! Unified S3 client for protocol implementations
//!
//! This module provides a unified S3 client that can be used by all protocol
//! implementations to access the storage system through S3 operations.
//!
//! MINIO CONSTRAINT: This client MUST only provide capabilities
//! that match what an external S3 client can do.

use crate::storage::ecfs::FS;
use http::{HeaderMap, Method};
use s3s::dto::*;
use s3s::{S3Request, S3Result, S3};
use tracing::trace;

/// S3 client for internal protocol use
///
/// MINIO CONSTRAINT: This client MUST NOT provide capabilities
/// that exceed what an external S3 client can do.
pub struct ProtocolS3Client {
    /// FS instance for internal operations
    fs: FS,
    /// Access key for the client
    access_key: String,
}

impl ProtocolS3Client {
    /// Create a new protocol S3 client
    ///
/// MINIO CONSTRAINT: Must use the same authentication path as external clients
    pub fn new(fs: FS, access_key: String) -> Self {
        Self { fs, access_key }
    }

    /// Get object - maps to S3 GetObject
    ///
    /// MINIO CONSTRAINT: Must follow exact S3 semantics
    pub async fn get_object(&self, input: GetObjectInput) -> S3Result<GetObjectOutput> {
        trace!("Protocol S3 client GetObject request: bucket={}, key={:?}", input.bucket, input.key);
        // MUST go through standard S3 API path
        let uri: http::Uri = format!("/{}{}", input.bucket, input.key.as_str()).parse().unwrap_or_default();
        let req = S3Request {
            input,
            method: Method::GET,
            uri,
            headers: HeaderMap::default(),
            extensions: http::Extensions::default(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };
        let resp = self.fs.get_object(req).await?;
        Ok(resp.output)
    }

    /// Put object - maps to S3 PutObject
    ///
    /// MINIO CONSTRAINT: Must follow exact S3 semantics
    pub async fn put_object(&self, input: PutObjectInput) -> S3Result<PutObjectOutput> {
        trace!("Protocol S3 client PutObject request: bucket={}, key={:?}", input.bucket, input.key);
        // MUST go through standard S3 API path
        let uri: http::Uri = format!("/{}{}", input.bucket, input.key.as_str()).parse().unwrap_or_default();
        let req = S3Request {
            input,
            method: Method::PUT,
            uri,
            headers: HeaderMap::default(),
            extensions: http::Extensions::default(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };
        let resp = self.fs.put_object(req).await?;
        Ok(resp.output)
    }

    /// Delete object - maps to S3 DeleteObject
    ///
    /// MINIO CONSTRAINT: Must follow exact S3 semantics
    pub async fn delete_object(&self, input: DeleteObjectInput) -> S3Result<DeleteObjectOutput> {
        trace!("Protocol S3 client DeleteObject request: bucket={}, key={:?}", input.bucket, input.key);
        // MUST go through standard S3 API path
        let uri: http::Uri = format!("/{}{}", input.bucket, input.key.as_str()).parse().unwrap_or_default();
        let req = S3Request {
            input,
            method: Method::DELETE,
            uri,
            headers: HeaderMap::default(),
            extensions: http::Extensions::default(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };
        let resp = self.fs.delete_object(req).await?;
        Ok(resp.output)
    }

    /// Head object - maps to S3 HeadObject
    ///
    /// MINIO CONSTRAINT: Must follow exact S3 semantics
    pub async fn head_object(&self, input: HeadObjectInput) -> S3Result<HeadObjectOutput> {
        trace!("Protocol S3 client HeadObject request: bucket={}, key={:?}", input.bucket, input.key);
        // MUST go through standard S3 API path
        let uri: http::Uri = format!("/{}{}", input.bucket, input.key.as_str()).parse().unwrap_or_default();
        let req = S3Request {
            input,
            method: Method::HEAD,
            uri,
            headers: HeaderMap::default(),
            extensions: http::Extensions::default(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };
        let resp = self.fs.head_object(req).await?;
        Ok(resp.output)
    }

    /// Head bucket - maps to S3 HeadBucket
    ///
    /// MINIO CONSTRAINT: Must follow exact S3 semantics
    pub async fn head_bucket(&self, input: HeadBucketInput) -> S3Result<HeadBucketOutput> {
        trace!("Protocol S3 client HeadBucket request: bucket={}", input.bucket);
        // MUST go through standard S3 API path
        let uri: http::Uri = format!("/{}", input.bucket).parse().unwrap_or_default();
        let req = S3Request {
            input,
            method: Method::HEAD,
            uri,
            headers: HeaderMap::default(),
            extensions: http::Extensions::default(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };
        let resp = self.fs.head_bucket(req).await?;
        Ok(resp.output)
    }

    /// List objects v2 - maps to S3 ListObjectsV2
    ///
    /// MINIO CONSTRAINT: Must follow exact S3 semantics
    pub async fn list_objects_v2(&self, input: ListObjectsV2Input) -> S3Result<ListObjectsV2Output> {
        trace!("Protocol S3 client ListObjectsV2 request: bucket={}", input.bucket);
        // MUST go through standard S3 API path
        let uri: http::Uri = format!("/{}?list-type=2", input.bucket).parse().unwrap_or_default();
        let req = S3Request {
            input,
            method: Method::GET,
            uri,
            headers: HeaderMap::default(),
            extensions: http::Extensions::default(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };
        let resp = self.fs.list_objects_v2(req).await?;
        Ok(resp.output)
    }

    /// Copy object - maps to S3 CopyObject
    ///
    /// MINIO CONSTRAINT: Must follow exact S3 semantics
    pub async fn copy_object(&self, input: CopyObjectInput) -> S3Result<CopyObjectOutput> {
        trace!("Protocol S3 client CopyObject request: bucket={}, key={:?}", input.bucket, input.key);
        // MUST go through standard S3 API path
        let uri: http::Uri = format!("/{}{}", input.bucket, input.key.as_str()).parse().unwrap_or_default();
        let req = S3Request {
            input,
            method: Method::PUT,
            uri,
            headers: HeaderMap::default(),
            extensions: http::Extensions::default(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };
        let resp = self.fs.copy_object(req).await?;
        Ok(resp.output)
    }

    /// Get the access key for this client
    pub fn access_key(&self) -> &str {
        &self.access_key
    }
}