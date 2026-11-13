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

//! Legacy s3s edge of the bucket CORS, tagging, public access block and encryption configurations:
//! batch A1 of the gateway migration (rustfs/backlog#2765).
//!
//! `impl S3 for FS` in `ecfs.rs` calls the `*_edge` fn of each operation here. The bodies are the
//! former trait method bodies, unchanged; the batch that moves these operations onto the gateway
//! owns this file and is the only one that edits it.

use crate::storage::ecfs::*;
use crate::storage::helper::audit_only_operation;
use crate::storage::s3_api;
use crate::storage::{persisted_from_s3s, persisted_to_s3s, tagging_from_s3s, tagging_to_s3s};
use rustfs_gateway_types::compat::s3s_5761ddfe::generated::ops::{delete_bucket_cors, put_bucket_cors};
use rustfs_io_metrics::record_s3_op;
use rustfs_s3_ops::S3Operation;
use tracing::instrument;

impl FS {
    #[instrument(name = "delete_bucket_cors", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn delete_bucket_cors_edge(
        &self,
        req: S3Request<DeleteBucketCorsInput>,
    ) -> S3Result<S3Response<DeleteBucketCorsOutput>> {
        record_s3_op(S3Operation::DeleteBucketCors);
        let usecase = s3_api::bucket_usecase_for(self);
        through_gateway_seam(
            req,
            delete_bucket_cors::input_from_s3s,
            delete_bucket_cors::output_to_s3s,
            |envelope, input| usecase.execute_delete_bucket_cors(envelope, input),
        )
        .await
    }

    pub(in crate::storage::ecfs) async fn delete_bucket_encryption_edge(
        &self,
        req: S3Request<DeleteBucketEncryptionInput>,
    ) -> S3Result<S3Response<DeleteBucketEncryptionOutput>> {
        let usecase = s3_api::bucket_usecase_for(self);
        audit_only_operation(req, S3Operation::DeleteBucketEncryption, |req| {
            usecase.execute_delete_bucket_encryption(req)
        })
        .await
    }

    #[instrument(name = "delete_bucket_tagging", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn delete_bucket_tagging_edge(
        &self,
        req: S3Request<DeleteBucketTaggingInput>,
    ) -> S3Result<S3Response<DeleteBucketTaggingOutput>> {
        let usecase = s3_api::bucket_usecase_for(self);
        audit_only_operation(req, S3Operation::DeleteBucketTagging, |req| usecase.execute_delete_bucket_tagging(req)).await
    }

    #[instrument(name = "delete_public_access_block", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn delete_public_access_block_edge(
        &self,
        req: S3Request<DeletePublicAccessBlockInput>,
    ) -> S3Result<S3Response<DeletePublicAccessBlockOutput>> {
        let usecase = s3_api::bucket_usecase_for(self);
        audit_only_operation(req, S3Operation::DeletePublicAccessBlock, |req| {
            usecase.execute_delete_public_access_block(req)
        })
        .await
    }

    #[instrument(name = "get_bucket_cors", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn get_bucket_cors_edge(
        &self,
        req: S3Request<GetBucketCorsInput>,
    ) -> S3Result<S3Response<GetBucketCorsOutput>> {
        record_s3_op(S3Operation::GetBucketCors);
        let usecase = s3_api::bucket_usecase_for(self);
        let cors_configuration = usecase.execute_get_bucket_cors(req).await?;
        let cors_configuration: CORSConfiguration = persisted_to_s3s(&cors_configuration)?;
        Ok(S3Response::new(GetBucketCorsOutput {
            cors_rules: Some(cors_configuration.cors_rules),
        }))
    }

    pub(in crate::storage::ecfs) async fn get_bucket_encryption_edge(
        &self,
        req: S3Request<GetBucketEncryptionInput>,
    ) -> S3Result<S3Response<GetBucketEncryptionOutput>> {
        record_s3_op(S3Operation::GetBucketEncryption);
        let usecase = s3_api::bucket_usecase_for(self);
        let server_side_encryption_configuration = usecase
            .execute_get_bucket_encryption(req)
            .await?
            .as_ref()
            .map(persisted_to_s3s::<_, ServerSideEncryptionConfiguration>)
            .transpose()?;
        Ok(S3Response::new(GetBucketEncryptionOutput {
            server_side_encryption_configuration,
        }))
    }

    #[instrument(name = "get_bucket_tagging", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn get_bucket_tagging_edge(
        &self,
        req: S3Request<GetBucketTaggingInput>,
    ) -> S3Result<S3Response<GetBucketTaggingOutput>> {
        record_s3_op(S3Operation::GetBucketTagging);
        let usecase = s3_api::bucket_usecase_for(self);
        let tagging = usecase.execute_get_bucket_tagging(req).await?;
        Ok(S3Response::new(GetBucketTaggingOutput {
            tag_set: tagging_to_s3s(&tagging).tag_set,
        }))
    }

    #[instrument(name = "get_public_access_block", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn get_public_access_block_edge(
        &self,
        req: S3Request<GetPublicAccessBlockInput>,
    ) -> S3Result<S3Response<GetPublicAccessBlockOutput>> {
        record_s3_op(S3Operation::GetPublicAccessBlock);
        let usecase = s3_api::bucket_usecase_for(self);
        let config = usecase.execute_get_public_access_block(req).await?;
        Ok(S3Response::new(GetPublicAccessBlockOutput {
            public_access_block_configuration: Some(persisted_to_s3s(&config)?),
        }))
    }

    #[instrument(name = "put_bucket_cors", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn put_bucket_cors_edge(
        &self,
        req: S3Request<PutBucketCorsInput>,
    ) -> S3Result<S3Response<PutBucketCorsOutput>> {
        record_s3_op(S3Operation::PutBucketCors);
        let usecase = s3_api::bucket_usecase_for(self);
        through_gateway_seam(req, put_bucket_cors::input_from_s3s, put_bucket_cors::output_to_s3s, |envelope, input| {
            usecase.execute_put_bucket_cors(envelope, input)
        })
        .await
    }

    pub(in crate::storage::ecfs) async fn put_bucket_encryption_edge(
        &self,
        req: S3Request<PutBucketEncryptionInput>,
    ) -> S3Result<S3Response<PutBucketEncryptionOutput>> {
        let prepared = persisted_from_s3s(&req.input.server_side_encryption_configuration);
        let usecase = s3_api::bucket_usecase_for(self);
        audit_only_operation(req, S3Operation::PutBucketEncryption, |req| {
            usecase.execute_put_bucket_encryption(req, prepared)
        })
        .await
    }

    #[instrument(name = "put_public_access_block", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn put_public_access_block_edge(
        &self,
        req: S3Request<PutPublicAccessBlockInput>,
    ) -> S3Result<S3Response<PutPublicAccessBlockOutput>> {
        let prepared = persisted_from_s3s(&req.input.public_access_block_configuration);
        let usecase = s3_api::bucket_usecase_for(self);
        audit_only_operation(req, S3Operation::PutPublicAccessBlock, |req| {
            usecase.execute_put_public_access_block(req, prepared)
        })
        .await
    }

    #[instrument(name = "put_bucket_tagging", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn put_bucket_tagging_edge(
        &self,
        req: S3Request<PutBucketTaggingInput>,
    ) -> S3Result<S3Response<PutBucketTaggingOutput>> {
        let tagging = tagging_from_s3s(&req.input.tagging);
        let usecase = s3_api::bucket_usecase_for(self);
        audit_only_operation(req, S3Operation::PutBucketTagging, |req| usecase.execute_put_bucket_tagging(req, tagging)).await
    }
}
