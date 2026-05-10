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
    /// Server-side copy of an object from one bucket+key to another.
    /// The input carries the full S3 surface (content type, metadata map,
    /// metadata directive, storage class, SSE config, conditional-copy
    /// headers) so protocol drivers can map client-supplied metadata
    /// onto the destination object.
    async fn copy_object(
        &self,
        input: CopyObjectInput,
        access_key: &str,
        secret_key: &str,
    ) -> Result<CopyObjectOutput, Self::Error>;
    /// Initiate a multipart upload. Returns an upload_id that identifies
    /// the in-progress upload for subsequent UploadPart, CompleteMultipartUpload,
    /// and AbortMultipartUpload calls. The input carries the full S3 surface
    /// (content type, cache control, metadata map, storage class, SSE config,
    /// object lock settings) so protocol drivers can map client-supplied
    /// metadata into the upload at creation time.
    async fn create_multipart_upload(
        &self,
        input: CreateMultipartUploadInput,
        access_key: &str,
        secret_key: &str,
    ) -> Result<CreateMultipartUploadOutput, Self::Error>;
    /// Upload one part of a multipart upload. The part_number must be in
    /// the range 1 to the 10 000-part S3 limit. The returned ETag
    /// identifies the part in the subsequent CompleteMultipartUpload call.
    async fn upload_part(
        &self,
        input: UploadPartInput,
        access_key: &str,
        secret_key: &str,
    ) -> Result<UploadPartOutput, Self::Error>;
    /// Assemble the parts listed in the input into the final object.
    /// The parts list must be sorted by part_number with no duplicates.
    async fn complete_multipart_upload(
        &self,
        input: CompleteMultipartUploadInput,
        access_key: &str,
        secret_key: &str,
    ) -> Result<CompleteMultipartUploadOutput, Self::Error>;
    /// Abort an in-progress multipart upload. Releases any storage
    /// associated with the upload_id. Idempotent: calling abort on an
    /// already-aborted upload_id returns success. The input carries the
    /// cross-account and conditional-abort fields (expected_bucket_owner,
    /// if_match_initiated_time) that non-SFTP consumers may need.
    async fn abort_multipart_upload(
        &self,
        input: AbortMultipartUploadInput,
        access_key: &str,
        secret_key: &str,
    ) -> Result<AbortMultipartUploadOutput, Self::Error>;
    /// Copy a byte range from an existing object into a part of an
    /// in-progress multipart upload. Used by rename for objects larger
    /// than the 5 GiB single-shot CopyObject limit.
    async fn upload_part_copy(
        &self,
        input: UploadPartCopyInput,
        access_key: &str,
        secret_key: &str,
    ) -> Result<UploadPartCopyOutput, Self::Error>;
}
