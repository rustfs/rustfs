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

use async_trait::async_trait;
use s3s::dto::*;

#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Error type for this storage backend
    type Error: std::error::Error + Send + Sync + 'static;
    /// Get object content and metadata
    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        access_key: &str,
        secret_key: &str,
        start_pos: Option<u64>,
    ) -> Result<GetObjectOutput, Self::Error>;
    async fn get_object_range(
        &self,
        bucket: &str,
        key: &str,
        access_key: &str,
        secret_key: &str,
        start_pos: u64,
        length: u64,
    ) -> Result<GetObjectOutput, Self::Error>;
    /// Put object content with metadata
    async fn put_object(&self, input: PutObjectInput, access_key: &str, secret_key: &str)
    -> Result<PutObjectOutput, Self::Error>;
    /// Delete an object
    async fn delete_object(
        &self,
        bucket: &str,
        key: &str,
        access_key: &str,
        secret_key: &str,
    ) -> Result<DeleteObjectOutput, Self::Error>;
    /// Get object metadata without content
    async fn head_object(
        &self,
        bucket: &str,
        key: &str,
        access_key: &str,
        secret_key: &str,
    ) -> Result<HeadObjectOutput, Self::Error>;
    /// Check if bucket exists and get metadata
    async fn head_bucket(&self, bucket: &str, access_key: &str, secret_key: &str) -> Result<HeadBucketOutput, Self::Error>;
    /// List objects in a bucket with pagination
    async fn list_objects_v2(
        &self,
        input: ListObjectsV2Input,
        access_key: &str,
        secret_key: &str,
    ) -> Result<ListObjectsV2Output, Self::Error>;
    /// List all buckets (requires authentication)
    async fn list_buckets(&self, access_key: &str, secret_key: &str) -> Result<ListBucketsOutput, Self::Error>;
    /// Create a new bucket
    async fn create_bucket(&self, bucket: &str, access_key: &str, secret_key: &str) -> Result<CreateBucketOutput, Self::Error>;
    /// Delete a bucket (must be empty)
    async fn delete_bucket(&self, bucket: &str, access_key: &str, secret_key: &str) -> Result<DeleteBucketOutput, Self::Error>;
}
