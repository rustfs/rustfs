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

/// Request parameters for creating S3 requests
#[derive(Debug)]
struct RequestParams<'a> {
    bucket: Option<String>,
    object: Option<String>,
    access_key: &'a str,
    secret_key: &'a str,
}

/// Protocol storage client that implements the StorageBackend trait
#[derive(Clone, Debug)]
pub struct ProtocolStorageClient {
    /// FS instance for storage operations
    fs: FS,
}

impl ProtocolStorageClient {
    /// Create a new protocol storage client
    pub fn new(fs: FS) -> Self {
        Self { fs }
    }

    /// Create a proper S3Request with ReqInfo extension for authorization
    async fn create_request<T>(
        &self,
        input: T,
        method: Method,
        uri: http::Uri,
        params: RequestParams<'_>,
    ) -> S3Result<S3Request<T>> {
        let mut extensions = http::Extensions::default();

        let is_owner = if let Some(global_cred) = rustfs_credentials::get_global_action_cred() {
            params.access_key == global_cred.access_key
        } else {
            false
        };

        let credentials = Some(s3s::auth::Credentials {
            access_key: params.access_key.to_string(),
            secret_key: params.secret_key.to_string().into(),
        });

        extensions.insert(crate::storage::access::ReqInfo {
            cred: Some(rustfs_credentials::Credentials {
                access_key: params.access_key.to_string(),
                secret_key: params.secret_key.to_string(),
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
            bucket: params.bucket,
            object: params.object,
            version_id: None,
            region: None,
        });

        let req = S3Request {
            input,
            method,
            uri,
            headers: HeaderMap::default(),
            extensions,
            credentials,
            region: None,
            service: None,
            trailing_headers: None,
        };
        Ok(req)
    }
}

#[async_trait::async_trait]
impl rustfs_protocols::common::client::s3::StorageBackend for ProtocolStorageClient {
    type Error = s3s::S3Error;

    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        access_key: &str,
        secret_key: &str,
        start_pos: Option<u64>,
    ) -> Result<GetObjectOutput, Self::Error> {
        trace!(
            "Protocol storage client GetObject request: bucket={}, key={}, start_pos={:?}",
            bucket, key, start_pos
        );

        let mut builder = GetObjectInput::builder().bucket(bucket.to_string()).key(key.to_string());

        // Add range header if start_pos is specified
        if let Some(start) = start_pos {
            let range = s3s::dto::Range::Int {
                first: start,
                last: None, // Read to end of file
            };
            builder = builder.range(Some(range));
        }

        let input = builder.build().map_err(|e| {
            s3s::S3Error::with_message(s3s::S3ErrorCode::InvalidRequest, format!("Failed to build GetObjectInput: {}", e))
        })?;

        let uri: http::Uri = format!("/{}{}", bucket, key).parse().unwrap_or_default();
        let req = self
            .create_request(
                input,
                Method::GET,
                uri,
                RequestParams {
                    bucket: Some(bucket.to_string()),
                    object: Some(key.to_string()),
                    access_key,
                    secret_key,
                },
            )
            .await?;

        match self.fs.get_object(req).await {
            Ok(response) => Ok(response.output),
            Err(e) => Err(e),
        }
    }

    async fn put_object(
        &self,
        input: PutObjectInput,
        access_key: &str,
        secret_key: &str,
    ) -> Result<PutObjectOutput, Self::Error> {
        trace!("Protocol storage client PutObject request: bucket={}, key={:?}", input.bucket, input.key);

        let bucket = input.bucket.clone();
        let key = input.key.clone();
        let uri: http::Uri = format!("/{}{}", bucket, key).parse().unwrap_or_default();

        let mut headers = HeaderMap::default();
        if let Some(ref body) = input.body {
            let (lower, upper) = body.size_hint();
            if let Some(len) = upper {
                headers.insert("content-length", len.to_string().parse().unwrap());
            } else if lower > 0 {
                headers.insert("content-length", lower.to_string().parse().unwrap());
            }
        }

        let req = self
            .create_request(
                input,
                Method::PUT,
                uri,
                RequestParams {
                    bucket: Some(bucket),
                    object: Some(key),
                    access_key,
                    secret_key,
                },
            )
            .await?;
        let req = S3Request { headers, ..req };

        match self.fs.put_object(req).await {
            Ok(response) => Ok(response.output),
            Err(e) => Err(e),
        }
    }

    async fn delete_object(
        &self,
        bucket: &str,
        key: &str,
        access_key: &str,
        secret_key: &str,
    ) -> Result<DeleteObjectOutput, Self::Error> {
        trace!("Protocol storage client DeleteObject request: bucket={}, key={}", bucket, key);

        let input = DeleteObjectInput::builder()
            .bucket(bucket.to_string())
            .key(key.to_string())
            .build()
            .map_err(|e| {
                s3s::S3Error::with_message(s3s::S3ErrorCode::InvalidRequest, format!("Failed to build DeleteObjectInput: {}", e))
            })?;

        let uri: http::Uri = format!("/{}{}", bucket, key).parse().unwrap_or_default();
        let req = self
            .create_request(
                input,
                Method::DELETE,
                uri,
                RequestParams {
                    bucket: Some(bucket.to_string()),
                    object: Some(key.to_string()),
                    access_key,
                    secret_key,
                },
            )
            .await?;

        match self.fs.delete_object(req).await {
            Ok(response) => Ok(response.output),
            Err(e) => Err(e),
        }
    }

    async fn head_object(
        &self,
        bucket: &str,
        key: &str,
        access_key: &str,
        secret_key: &str,
    ) -> Result<HeadObjectOutput, Self::Error> {
        trace!("Protocol storage client HeadObject request: bucket={}, key={}", bucket, key);

        let input = HeadObjectInput::builder()
            .bucket(bucket.to_string())
            .key(key.to_string())
            .build()
            .map_err(|e| {
                s3s::S3Error::with_message(s3s::S3ErrorCode::InvalidRequest, format!("Failed to build HeadObjectInput: {}", e))
            })?;

        let uri: http::Uri = format!("/{}{}", bucket, key).parse().unwrap_or_default();
        let req = self
            .create_request(
                input,
                Method::HEAD,
                uri,
                RequestParams {
                    bucket: Some(bucket.to_string()),
                    object: Some(key.to_string()),
                    access_key,
                    secret_key,
                },
            )
            .await?;

        match self.fs.head_object(req).await {
            Ok(response) => Ok(response.output),
            Err(e) => Err(e),
        }
    }

    async fn head_bucket(&self, bucket: &str, access_key: &str, secret_key: &str) -> Result<HeadBucketOutput, Self::Error> {
        trace!("Protocol storage client HeadBucket request: bucket={}", bucket);

        let input = HeadBucketInput::builder().bucket(bucket.to_string()).build().map_err(|e| {
            s3s::S3Error::with_message(s3s::S3ErrorCode::InvalidRequest, format!("Failed to build HeadBucketInput: {}", e))
        })?;

        let uri: http::Uri = format!("/{}", bucket).parse().unwrap_or_default();
        let req = self
            .create_request(
                input,
                Method::HEAD,
                uri,
                RequestParams {
                    bucket: Some(bucket.to_string()),
                    object: None,
                    access_key,
                    secret_key,
                },
            )
            .await?;

        match self.fs.head_bucket(req).await {
            Ok(response) => Ok(response.output),
            Err(e) => Err(e),
        }
    }

    async fn list_objects_v2(
        &self,
        input: ListObjectsV2Input,
        access_key: &str,
        secret_key: &str,
    ) -> Result<ListObjectsV2Output, Self::Error> {
        trace!("Protocol storage client ListObjectsV2 request: bucket={}", input.bucket);

        let bucket = input.bucket.clone();
        let uri: http::Uri = format!("/{}?list-type=2", bucket).parse().unwrap_or_default();
        let req = self
            .create_request(
                input,
                Method::GET,
                uri,
                RequestParams {
                    bucket: Some(bucket),
                    object: None,
                    access_key,
                    secret_key,
                },
            )
            .await?;

        match self.fs.list_objects_v2(req).await {
            Ok(response) => Ok(response.output),
            Err(e) => Err(e),
        }
    }

    async fn list_buckets(&self, access_key: &str, secret_key: &str) -> Result<ListBucketsOutput, Self::Error> {
        trace!("Protocol storage client ListBuckets request: access_key={}", access_key);

        let input = ListBucketsInput::builder().build().map_err(|e| {
            s3s::S3Error::with_message(s3s::S3ErrorCode::InvalidRequest, format!("Failed to build ListBucketsInput: {}", e))
        })?;

        let req = self
            .create_request(
                input,
                Method::GET,
                http::Uri::from_static("/"),
                RequestParams {
                    bucket: None,
                    object: None,
                    access_key,
                    secret_key,
                },
            )
            .await?;

        match self.fs.list_buckets(req).await {
            Ok(response) => Ok(response.output),
            Err(e) => Err(e),
        }
    }

    async fn create_bucket(&self, bucket: &str, access_key: &str, secret_key: &str) -> Result<CreateBucketOutput, Self::Error> {
        trace!("Protocol storage client CreateBucket request: bucket={}", bucket);

        let input = CreateBucketInput::builder().bucket(bucket.to_string()).build().map_err(|e| {
            s3s::S3Error::with_message(s3s::S3ErrorCode::InvalidRequest, format!("Failed to build CreateBucketInput: {}", e))
        })?;

        let uri: http::Uri = format!("/{}", bucket).parse().unwrap_or_default();
        let req = self
            .create_request(
                input,
                Method::PUT,
                uri,
                RequestParams {
                    bucket: Some(bucket.to_string()),
                    object: None,
                    access_key,
                    secret_key,
                },
            )
            .await?;

        match self.fs.create_bucket(req).await {
            Ok(response) => Ok(response.output),
            Err(e) => Err(e),
        }
    }

    async fn get_object_range(
        &self,
        bucket: &str,
        key: &str,
        access_key: &str,
        secret_key: &str,
        start_pos: u64,
        length: u64,
    ) -> Result<GetObjectOutput, Self::Error> {
        trace!(
            "Protocol storage client GetObjectRange request: bucket={}, key={}, start={}, length={}",
            bucket, key, start_pos, length
        );

        let range = s3s::dto::Range::Int {
            first: start_pos,
            last: Some(start_pos + length - 1),
        };

        let input = GetObjectInput::builder()
            .bucket(bucket.to_string())
            .key(key.to_string())
            .range(Some(range))
            .build()
            .map_err(|e| {
                s3s::S3Error::with_message(s3s::S3ErrorCode::InvalidRequest, format!("Failed to build GetObjectInput: {}", e))
            })?;

        let uri: http::Uri = format!("/{}{}", bucket, key).parse().unwrap_or_default();
        let req = self
            .create_request(
                input,
                Method::GET,
                uri,
                RequestParams {
                    bucket: Some(bucket.to_string()),
                    object: Some(key.to_string()),
                    access_key,
                    secret_key,
                },
            )
            .await?;

        match self.fs.get_object(req).await {
            Ok(response) => Ok(response.output),
            Err(e) => Err(e),
        }
    }

    async fn delete_bucket(&self, bucket: &str, access_key: &str, secret_key: &str) -> Result<DeleteBucketOutput, Self::Error> {
        trace!("Protocol storage client DeleteBucket request: bucket={}", bucket);

        let input = DeleteBucketInput::builder().bucket(bucket.to_string()).build().map_err(|e| {
            s3s::S3Error::with_message(s3s::S3ErrorCode::InvalidRequest, format!("Failed to build DeleteBucketInput: {}", e))
        })?;

        let uri: http::Uri = format!("/{}", bucket).parse().unwrap_or_default();
        let req = self
            .create_request(
                input,
                Method::DELETE,
                uri,
                RequestParams {
                    bucket: Some(bucket.to_string()),
                    object: None,
                    access_key,
                    secret_key,
                },
            )
            .await?;

        match self.fs.delete_bucket(req).await {
            Ok(response) => Ok(response.output),
            Err(e) => Err(e),
        }
    }
}
