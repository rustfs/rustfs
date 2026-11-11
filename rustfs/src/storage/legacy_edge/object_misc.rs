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

//! Legacy s3s edge of CopyObject, UploadPartCopy, CompleteMultipartUpload, DeleteObject,
//! DeleteObjects, RestoreObject and SelectObjectContent: batch E of the gateway migration
//! (rustfs/backlog#2765).
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
    #[instrument(name = "complete_multipart_upload", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn complete_multipart_upload_edge(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        crate::hp_guard!("S3::complete_multipart_upload");
        let usecase = s3_api::multipart_usecase_for(self);
        Box::pin(usecase.execute_complete_multipart_upload(req)).await
    }

    /// Copy an object from one location to another
    #[instrument(name = "copy_object", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn copy_object_edge(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let usecase = s3_api::object_usecase_for(self);
        usecase.execute_copy_object(req).await
    }

    /// Delete an object
    #[instrument(name = "delete_object", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn delete_object_edge(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        crate::hp_guard!("S3::delete_object");
        let request_id = req
            .extensions
            .get::<crate::storage::request_context::RequestContext>()
            .map(|ctx| ctx.request_id.clone());
        let usecase = s3_api::object_usecase_for(self);
        let result = Box::pin(usecase.execute_delete_object(req)).await;
        if let Err(error) = &result {
            log_object_internal_error("DeleteObject", request_id.as_deref(), error);
        }
        result
    }

    /// Delete multiple objects
    #[instrument(name = "delete_objects", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn delete_objects_edge(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let usecase = s3_api::object_usecase_for(self);
        usecase.execute_delete_objects(req).await
    }

    pub(in crate::storage::ecfs) async fn restore_object_edge(
        &self,
        req: S3Request<RestoreObjectInput>,
    ) -> S3Result<S3Response<RestoreObjectOutput>> {
        let usecase = s3_api::object_usecase_for(self);
        usecase.execute_restore_object(req).await
    }

    pub(in crate::storage::ecfs) async fn select_object_content_edge(
        &self,
        req: S3Request<SelectObjectContentInput>,
    ) -> S3Result<S3Response<SelectObjectContentOutput>> {
        let usecase = s3_api::object_usecase_for(self);
        usecase.execute_select_object_content(req).await
    }

    #[instrument(name = "upload_part_copy", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn upload_part_copy_edge(
        &self,
        req: S3Request<UploadPartCopyInput>,
    ) -> S3Result<S3Response<UploadPartCopyOutput>> {
        record_s3_op(S3Operation::UploadPartCopy);
        let usecase = s3_api::multipart_usecase_for(self);
        audit_only_operation(req, S3Operation::UploadPartCopy, |req| Box::pin(usecase.execute_upload_part_copy(req))).await
    }
}
