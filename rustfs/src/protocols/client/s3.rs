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

use crate::storage::ecfs::FS;
use http::{HeaderMap, Method};
use rustfs_credentials;
use s3s::dto::*;
use s3s::{S3, S3Request, S3Result};
use tokio_stream::Stream;
use tracing::trace;

/// S3 client for internal protocol use
pub struct ProtocolS3Client {
    /// FS instance for internal operations
    fs: FS,
    /// Access key for the client
    access_key: String,
}

impl ProtocolS3Client {
    /// Create a new protocol S3 client
    pub fn new(fs: FS, access_key: String) -> Self {
        Self { fs, access_key }
    }

    /// Get object - maps to S3 GetObject
    pub async fn get_object(&self, input: GetObjectInput) -> S3Result<GetObjectOutput> {
        trace!(
            "Protocol S3 client GetObject request: bucket={}, key={:?}, access_key={}",
            input.bucket, input.key, self.access_key
        );
        // Go through standard S3 API path
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
    pub async fn put_object(&self, input: PutObjectInput) -> S3Result<PutObjectOutput> {
        trace!(
            "Protocol S3 client PutObject request: bucket={}, key={:?}, access_key={}",
            input.bucket, input.key, self.access_key
        );
        let uri: http::Uri = format!("/{}{}", input.bucket, input.key.as_str()).parse().unwrap_or_default();

        // Set required headers for put operation
        let mut headers = HeaderMap::default();
        if let Some(ref body) = input.body {
            let (lower, upper) = body.size_hint();
            if let Some(len) = upper {
                headers.insert("content-length", len.to_string().parse().unwrap());
            } else if lower > 0 {
                headers.insert("content-length", lower.to_string().parse().unwrap());
            }
        }

        let req = S3Request {
            input,
            method: Method::PUT,
            uri,
            headers,
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
    pub async fn delete_object(&self, input: DeleteObjectInput) -> S3Result<DeleteObjectOutput> {
        trace!(
            "Protocol S3 client DeleteObject request: bucket={}, key={:?}, access_key={}",
            input.bucket, input.key, self.access_key
        );
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
    pub async fn head_object(&self, input: HeadObjectInput) -> S3Result<HeadObjectOutput> {
        trace!(
            "Protocol S3 client HeadObject request: bucket={}, key={:?}, access_key={}",
            input.bucket, input.key, self.access_key
        );
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
    pub async fn head_bucket(&self, input: HeadBucketInput) -> S3Result<HeadBucketOutput> {
        trace!(
            "Protocol S3 client HeadBucket request: bucket={}, access_key={}",
            input.bucket, self.access_key
        );
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
    pub async fn list_objects_v2(&self, input: ListObjectsV2Input) -> S3Result<ListObjectsV2Output> {
        trace!(
            "Protocol S3 client ListObjectsV2 request: bucket={}, access_key={}",
            input.bucket, self.access_key
        );
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

    /// List buckets - maps to S3 ListBuckets
    /// Note: This requires credentials and ReqInfo because list_buckets performs credential validation
    pub async fn list_buckets(&self, input: ListBucketsInput, secret_key: &str) -> S3Result<ListBucketsOutput> {
        trace!("Protocol S3 client ListBuckets request: access_key={}", self.access_key);

        // Create proper credentials with the real secret key from authentication
        let credentials = Some(s3s::auth::Credentials {
            access_key: self.access_key.clone(),
            secret_key: secret_key.to_string().into(),
        });

        // Check if user is the owner (admin)
        let is_owner = if let Some(global_cred) = rustfs_credentials::get_global_action_cred() {
            self.access_key == global_cred.access_key
        } else {
            false
        };

        // Create ReqInfo for authorization (required by list_buckets)
        let mut extensions = http::Extensions::default();
        extensions.insert(crate::storage::access::ReqInfo {
            cred: Some(rustfs_credentials::Credentials {
                access_key: self.access_key.clone(),
                secret_key: secret_key.to_string(),
                session_token: String::new(),
                expiration: None,
                status: String::new(),
                parent_user: String::new(),
                groups: None,
                claims: None,
                name: None,
                description: None,
            }),
            is_owner,
            bucket: None,
            object: None,
            version_id: None,
            region: None,
        });

        let req = S3Request {
            input,
            method: Method::GET,
            uri: http::Uri::from_static("/"),
            headers: HeaderMap::default(),
            extensions,
            credentials,
            region: None,
            service: None,
            trailing_headers: None,
        };
        let resp = self.fs.list_buckets(req).await?;
        Ok(resp.output)
    }

    /// Create bucket - maps to S3 CreateBucket
    pub async fn create_bucket(&self, input: CreateBucketInput) -> S3Result<CreateBucketOutput> {
        trace!(
            "Protocol S3 client CreateBucket request: bucket={:?}, access_key={}",
            input.bucket, self.access_key
        );
        let bucket_str = input.bucket.as_str();
        let uri: http::Uri = format!("/{}", bucket_str).parse().unwrap_or_default();
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
        let resp = self.fs.create_bucket(req).await?;
        Ok(resp.output)
    }

    /// Delete bucket - maps to S3 DeleteBucket
    pub async fn delete_bucket(&self, input: DeleteBucketInput) -> S3Result<DeleteBucketOutput> {
        trace!(
            "Protocol S3 client DeleteBucket request: bucket={}, access_key={}",
            input.bucket, self.access_key
        );
        let uri: http::Uri = format!("/{}", input.bucket).parse().unwrap_or_default();
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
        let resp = self.fs.delete_bucket(req).await?;
        Ok(resp.output)
    }
}
