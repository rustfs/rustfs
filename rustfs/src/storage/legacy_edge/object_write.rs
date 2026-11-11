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

//! Legacy s3s edge of PutObject and UploadPart (PostObject has no trait method of its own and
//! reaches PutObject): batch D of the gateway migration (rustfs/backlog#2765).
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
    #[instrument(name = "put_object", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn put_object_edge(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        crate::hp_guard!("S3::put_object");
        let usecase = s3_api::object_usecase_for(self);
        usecase.execute_put_object(self, req).await
    }

    #[instrument(name = "upload_part", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn upload_part_edge(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        crate::hp_guard!("S3::upload_part");
        record_s3_op(S3Operation::UploadPart);
        let usecase = s3_api::multipart_usecase_for(self);
        audit_only_operation(req, S3Operation::UploadPart, |req| usecase.execute_upload_part(req)).await
    }
}
