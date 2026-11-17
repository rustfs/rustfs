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

//! Legacy s3s edge of object ACL, tagging, legal hold, retention and torrent: batch B1 of the
//! gateway migration (rustfs/backlog#2765).
//!
//! `impl S3 for FS` in `ecfs.rs` calls the `*_edge` fn of each operation here. The bodies are the
//! former trait method bodies, unchanged; the batch that moves these operations onto the gateway
//! owns this file and is the only one that edits it.

use crate::app::object_meta::ObjectMetaUsecase;
use crate::app::object_meta::torrent::answer_get_object_torrent;
use crate::storage::access::{RequestedObjectLegalHold, RequestedObjectLockRetention};
use crate::storage::ecfs::*;
use rustfs_gateway_types::compat::s3s_5761ddfe::generated::ops::{
    delete_object_tagging, get_object_acl, get_object_legal_hold, get_object_retention, get_object_tagging, get_object_torrent,
    put_object_acl, put_object_legal_hold, put_object_retention,
};
use rustfs_io_metrics::record_s3_op;
use rustfs_s3_ops::S3Operation;
use tracing::instrument;

/// Preserve every tag and its optional fields for validation by the shared use case.
fn gateway_tags(tag_set: &TagSet) -> Vec<rustfs_gateway_types::dto::Tag> {
    tag_set
        .iter()
        .map(|tag| rustfs_gateway_types::dto::Tag {
            key: tag.key.clone(),
            value: tag.value.clone(),
        })
        .collect()
}

fn object_meta_usecase(fs: &FS) -> ObjectMetaUsecase {
    ObjectMetaUsecase::with_context(fs.server_ctx().app_context())
}

impl FS {
    pub(in crate::storage::ecfs) async fn delete_object_tagging_edge(
        &self,
        req: S3Request<DeleteObjectTaggingInput>,
    ) -> S3Result<S3Response<DeleteObjectTaggingOutput>> {
        record_s3_op(S3Operation::DeleteObjectTagging);
        let envelope = crate::app::RequestEnvelope::from_s3s(&req);
        let output = object_meta_usecase(self)
            .delete_object_tagging_fields(envelope, &req.input.bucket, &req.input.key, req.input.version_id)
            .await?;
        delete_object_tagging::output_to_s3s(output)
            .map(S3Response::new)
            .map_err(|error| crate::app::dto_values::seam_error(error).into())
    }

    pub(in crate::storage::ecfs) async fn get_object_acl_edge(
        &self,
        req: S3Request<GetObjectAclInput>,
    ) -> S3Result<S3Response<GetObjectAclOutput>> {
        record_s3_op(S3Operation::GetObjectAcl);
        let envelope = crate::app::RequestEnvelope::from_s3s(&req);
        let output = object_meta_usecase(self)
            .get_object_acl_fields(envelope, &req.input.bucket, &req.input.key, req.input.version_id)
            .await?;
        get_object_acl::output_to_s3s(output)
            .map(S3Response::new)
            .map_err(|error| crate::app::dto_values::seam_error(error).into())
    }

    pub(in crate::storage::ecfs) async fn get_object_legal_hold_edge(
        &self,
        req: S3Request<GetObjectLegalHoldInput>,
    ) -> S3Result<S3Response<GetObjectLegalHoldOutput>> {
        record_s3_op(S3Operation::GetObjectLegalHold);
        let envelope = crate::app::RequestEnvelope::from_s3s(&req);
        let output = object_meta_usecase(self)
            .get_object_legal_hold_fields(envelope, &req.input.bucket, &req.input.key, req.input.version_id)
            .await?;
        get_object_legal_hold::output_to_s3s(output)
            .map(S3Response::new)
            .map_err(|error| crate::app::dto_values::seam_error(error).into())
    }

    pub(in crate::storage::ecfs) async fn get_object_retention_edge(
        &self,
        req: S3Request<GetObjectRetentionInput>,
    ) -> S3Result<S3Response<GetObjectRetentionOutput>> {
        record_s3_op(S3Operation::GetObjectRetention);
        let envelope = crate::app::RequestEnvelope::from_s3s(&req);
        let output = object_meta_usecase(self)
            .get_object_retention_fields(envelope, &req.input.bucket, &req.input.key, req.input.version_id)
            .await?;
        get_object_retention::output_to_s3s(output)
            .map(S3Response::new)
            .map_err(|error| crate::app::dto_values::seam_error(error).into())
    }

    pub(in crate::storage::ecfs) async fn get_object_tagging_edge(
        &self,
        req: S3Request<GetObjectTaggingInput>,
    ) -> S3Result<S3Response<GetObjectTaggingOutput>> {
        record_s3_op(S3Operation::GetObjectTagging);
        let envelope = crate::app::RequestEnvelope::from_s3s(&req);
        let output = object_meta_usecase(self)
            .get_object_tagging_fields(envelope, &req.input.bucket, &req.input.key, req.input.version_id)
            .await?;
        get_object_tagging::output_to_s3s(output)
            .map(S3Response::new)
            .map_err(|error| crate::app::dto_values::seam_error(error).into())
    }

    pub(in crate::storage::ecfs) async fn get_object_torrent_edge(
        &self,
        _req: S3Request<GetObjectTorrentInput>,
    ) -> S3Result<S3Response<GetObjectTorrentOutput>> {
        record_s3_op(S3Operation::GetObjectTorrent);
        let output = answer_get_object_torrent()?;
        get_object_torrent::output_to_s3s(output)
            .map(S3Response::new)
            .map_err(|error| crate::app::dto_values::seam_error(error).into())
    }

    pub(in crate::storage::ecfs) async fn put_object_acl_edge(
        &self,
        req: S3Request<PutObjectAclInput>,
    ) -> S3Result<S3Response<PutObjectAclOutput>> {
        record_s3_op(S3Operation::PutObjectAcl);
        let envelope = crate::app::RequestEnvelope::from_s3s(&req);
        let output = object_meta_usecase(self)
            .put_object_acl_fields(
                envelope,
                &req.input.bucket,
                &req.input.key,
                req.input.version_id,
                req.input.access_control_policy.is_some(),
            )
            .await?;
        put_object_acl::output_to_s3s(output)
            .map(S3Response::new)
            .map_err(|error| crate::app::dto_values::seam_error(error).into())
    }

    pub(in crate::storage::ecfs) async fn put_object_legal_hold_edge(
        &self,
        req: S3Request<PutObjectLegalHoldInput>,
    ) -> S3Result<S3Response<PutObjectLegalHoldOutput>> {
        record_s3_op(S3Operation::PutObjectLegalHold);
        let envelope = crate::app::RequestEnvelope::from_s3s(&req);
        let input = req.input;
        let output = object_meta_usecase(self)
            .put_object_legal_hold_fields(
                envelope,
                &input.bucket,
                &input.key,
                input.version_id,
                input.legal_hold.as_ref().map(RequestedObjectLegalHold::from),
            )
            .await?;
        put_object_legal_hold::output_to_s3s(output)
            .map(S3Response::new)
            .map_err(|error| crate::app::dto_values::seam_error(error).into())
    }

    pub(in crate::storage::ecfs) async fn put_object_retention_edge(
        &self,
        req: S3Request<PutObjectRetentionInput>,
    ) -> S3Result<S3Response<PutObjectRetentionOutput>> {
        record_s3_op(S3Operation::PutObjectRetention);
        let envelope = crate::app::RequestEnvelope::from_s3s(&req);
        let input = req.input;
        let output = object_meta_usecase(self)
            .put_object_retention_fields(
                envelope,
                &input.bucket,
                &input.key,
                input.version_id,
                input.retention.as_ref().map(RequestedObjectLockRetention::from),
            )
            .await?;
        put_object_retention::output_to_s3s(output)
            .map(S3Response::new)
            .map_err(|error| crate::app::dto_values::seam_error(error).into())
    }

    #[instrument(name = "put_object_tagging", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn put_object_tagging_edge(
        &self,
        req: S3Request<PutObjectTaggingInput>,
    ) -> S3Result<S3Response<PutObjectTaggingOutput>> {
        record_s3_op(S3Operation::PutObjectTagging);
        let envelope = crate::app::RequestEnvelope::from_s3s(&req);
        let PutObjectTaggingInput {
            bucket,
            key,
            version_id,
            tagging,
            ..
        } = req.input;
        let output = object_meta_usecase(self)
            .put_object_tagging_fields(envelope, &bucket, &key, version_id, gateway_tags(&tagging.tag_set))
            .await?;
        Ok(S3Response::new(PutObjectTaggingOutput {
            version_id: output.version_id,
        }))
    }
}
