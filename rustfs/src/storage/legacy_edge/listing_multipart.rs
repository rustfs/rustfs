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

//! Legacy s3s edge of the object and multipart-upload listings, CreateMultipartUpload and
//! AbortMultipartUpload: batch B2 of the gateway migration (rustfs/backlog#2765).
//!
//! `impl S3 for FS` in `ecfs.rs` calls the `*_edge` fn of each operation here. The bodies are the
//! former trait method bodies, unchanged; the batch that moves these operations onto the gateway
//! owns this file and is the only one that edits it.

use crate::storage::ecfs::*;
use crate::storage::helper::audit_only_operation;
use crate::storage::s3_api;
use rustfs_io_metrics::record_s3_op;
use rustfs_s3_ops::S3Operation;
use tracing::instrument;

impl FS {
    #[instrument(name = "abort_multipart_upload", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn abort_multipart_upload_edge(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        let usecase = s3_api::multipart_usecase_for(self);
        audit_only_operation(req, S3Operation::AbortMultipartUpload, |req| usecase.execute_abort_multipart_upload(req)).await
    }

    #[instrument(name = "create_multipart_upload", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn create_multipart_upload_edge(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        crate::hp_guard!("S3::create_multipart_upload");
        let usecase = s3_api::multipart_usecase_for(self);
        usecase.execute_create_multipart_upload(req).await
    }

    pub(in crate::storage::ecfs) async fn list_multipart_uploads_edge(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        record_s3_op(S3Operation::ListMultipartUploads);
        let usecase = s3_api::multipart_usecase_for(self);
        usecase.execute_list_multipart_uploads(req).await
    }

    pub(in crate::storage::ecfs) async fn list_object_versions_edge(
        &self,
        req: S3Request<ListObjectVersionsInput>,
    ) -> S3Result<S3Response<ListObjectVersionsOutput>> {
        record_s3_op(S3Operation::ListObjectVersions);
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_list_object_versions(req).await
    }

    #[instrument(name = "list_objects", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn list_objects_edge(
        &self,
        req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        record_s3_op(S3Operation::ListObjects);
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_list_objects(req).await
    }

    #[instrument(name = "list_objects_v2", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn list_objects_v2_edge(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        crate::hp_guard!("S3::list_objects_v2");
        record_s3_op(S3Operation::ListObjectsV2);
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_list_objects_v2(req).await
    }

    #[instrument(name = "list_parts", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn list_parts_edge(
        &self,
        req: S3Request<ListPartsInput>,
    ) -> S3Result<S3Response<ListPartsOutput>> {
        record_s3_op(S3Operation::ListParts);
        let usecase = s3_api::multipart_usecase_for(self);
        usecase.execute_list_parts(req).await
    }
}
