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

//! Legacy s3s edge of bucket create, delete, head and location, and the website, accelerate,
//! logging and request-payment configurations: batch A4 of the gateway migration
//! (rustfs/backlog#2765).
//!
//! `impl S3 for FS` in `ecfs.rs` calls the `*_edge` fn of each operation here. The bodies are the
//! former trait method bodies, unchanged; the batch that moves these operations onto the gateway
//! owns this file and is the only one that edits it.

use crate::storage::ecfs::*;
use crate::storage::s3_api;
use crate::storage::storage_api::ecfs_consumer::contract::bucket::{BucketOperations, BucketOptions};
use crate::storage::storage_api::ecstore_bucket::utils::serialize as serialize_bucket_config;
use crate::storage::{persisted_from_s3s, persisted_to_s3s};
use rustfs_gateway_types::compat::s3s_5761ddfe::generated::ops::{create_bucket, delete_bucket, head_bucket};
use rustfs_gateway_types::compat::s3s_5761ddfe::get_bucket_location;
use rustfs_io_metrics::record_s3_op;
use rustfs_s3_ops::S3Operation;
use tracing::instrument;

impl FS {
    #[instrument(
        name = "create_bucket",
        level = "debug",
        skip(self, req),
        fields(start_time=?time::OffsetDateTime::now_utc())
    )]
    pub(in crate::storage::ecfs) async fn create_bucket_edge(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        record_s3_op(S3Operation::CreateBucket);
        let usecase = s3_api::bucket_usecase_for(self);
        through_gateway_seam(req, create_bucket::input_from_s3s, create_bucket::output_to_s3s, |envelope, input| {
            usecase.execute_create_bucket(envelope, input)
        })
        .await
    }

    /// Delete a bucket
    #[instrument(name = "delete_bucket", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn delete_bucket_edge(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        record_s3_op(S3Operation::DeleteBucket);
        let usecase = s3_api::bucket_usecase_for(self);
        through_gateway_seam(
            req,
            delete_bucket::input_from_s3s,
            delete_bucket::output_to_s3s,
            |envelope, (input, _)| {
                // The legacy-only members (the decoded `x-minio-force-delete`) are not read: the use
                // case reads both force-delete spellings from the headers.
                usecase.execute_delete_bucket(envelope, input)
            },
        )
        .await
    }

    pub(in crate::storage::ecfs) async fn delete_bucket_website_edge(
        &self,
        req: S3Request<DeleteBucketWebsiteInput>,
    ) -> S3Result<S3Response<DeleteBucketWebsiteOutput>> {
        let envelope = RequestEnvelope::from_s3s(&req);
        s3_api::bucket_usecase_for(self)
            .execute_delete_bucket_website(envelope, &req.input.bucket)
            .await?;
        Ok(S3Response::new(DeleteBucketWebsiteOutput::default()))
    }

    pub(in crate::storage::ecfs) async fn get_bucket_accelerate_configuration_edge(
        &self,
        req: S3Request<GetBucketAccelerateConfigurationInput>,
    ) -> S3Result<S3Response<GetBucketAccelerateConfigurationOutput>> {
        let config = s3_api::bucket_usecase_for(self)
            .execute_get_bucket_accelerate_configuration(&req.input.bucket)
            .await?;
        Ok(S3Response::new(GetBucketAccelerateConfigurationOutput {
            status: config.status.map(BucketAccelerateStatus::from),
            ..Default::default()
        }))
    }

    /// Get bucket location
    #[instrument(name = "get_bucket_location", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn get_bucket_location_edge(
        &self,
        req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        record_s3_op(S3Operation::GetBucketLocation);
        let usecase = s3_api::bucket_usecase_for(self);
        through_gateway_seam(
            req,
            get_bucket_location::input_from_s3s,
            |output| Ok(get_bucket_location::output_to_s3s(output)),
            |_, input| usecase.execute_get_bucket_location(input),
        )
        .await
    }

    pub(in crate::storage::ecfs) async fn get_bucket_request_payment_edge(
        &self,
        req: S3Request<GetBucketRequestPaymentInput>,
    ) -> S3Result<S3Response<GetBucketRequestPaymentOutput>> {
        let config = s3_api::bucket_usecase_for(self)
            .execute_get_bucket_request_payment(&req.input.bucket)
            .await?;
        Ok(S3Response::new(GetBucketRequestPaymentOutput {
            payer: Some(Payer::from(config.payer)),
        }))
    }

    pub(in crate::storage::ecfs) async fn get_bucket_website_edge(
        &self,
        req: S3Request<GetBucketWebsiteInput>,
    ) -> S3Result<S3Response<GetBucketWebsiteOutput>> {
        let config = s3_api::bucket_usecase_for(self)
            .execute_get_bucket_website(&req.input.bucket)
            .await?;
        let config: WebsiteConfiguration = persisted_to_s3s(&config)?;
        Ok(S3Response::new(GetBucketWebsiteOutput {
            error_document: config.error_document,
            index_document: config.index_document,
            redirect_all_requests_to: config.redirect_all_requests_to,
            routing_rules: config.routing_rules,
        }))
    }

    #[instrument(name = "head_bucket", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn head_bucket_edge(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        record_s3_op(S3Operation::HeadBucket);
        if req.input.bucket.is_empty() {
            // Protocol adapters use the empty bucket for the storage root.
            let Some(store) = self.server_ctx.object_store() else {
                return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
            };
            store
                .get_bucket_info(&req.input.bucket, &BucketOptions::default())
                .await
                .map_err(crate::error::ApiError::from)?;
            return Ok(S3Response::new(HeadBucketOutput::default()));
        }

        let usecase = s3_api::bucket_usecase_for(self);
        through_gateway_seam(req, head_bucket::input_from_s3s, head_bucket::output_to_s3s, |_, input| {
            usecase.execute_head_bucket(input)
        })
        .await
    }

    pub(in crate::storage::ecfs) async fn put_bucket_accelerate_configuration_edge(
        &self,
        req: S3Request<PutBucketAccelerateConfigurationInput>,
    ) -> S3Result<S3Response<PutBucketAccelerateConfigurationOutput>> {
        let envelope = RequestEnvelope::from_s3s(&req);
        let usecase = s3_api::bucket_usecase_for(self);
        usecase
            .execute_put_bucket_accelerate_configuration(envelope, &req.input.bucket, || {
                Ok(persisted_from_s3s(&req.input.accelerate_configuration)?)
            })
            .await?;
        Ok(S3Response::new(PutBucketAccelerateConfigurationOutput::default()))
    }

    pub(in crate::storage::ecfs) async fn get_bucket_logging_edge(
        &self,
        req: S3Request<GetBucketLoggingInput>,
    ) -> S3Result<S3Response<GetBucketLoggingOutput>> {
        record_s3_op(S3Operation::GetBucketLogging);
        let config = s3_api::bucket_usecase_for(self)
            .execute_get_bucket_logging(&req.input.bucket)
            .await?;
        let config: BucketLoggingStatus = persisted_to_s3s(&config)?;
        Ok(S3Response::new(GetBucketLoggingOutput {
            logging_enabled: config.logging_enabled,
        }))
    }

    pub(in crate::storage::ecfs) async fn put_bucket_logging_edge(
        &self,
        req: S3Request<PutBucketLoggingInput>,
    ) -> S3Result<S3Response<PutBucketLoggingOutput>> {
        record_s3_op(S3Operation::PutBucketLogging);
        let envelope = RequestEnvelope::from_s3s(&req);
        s3_api::bucket_usecase_for(self)
            .execute_put_bucket_logging(envelope, &req.input.bucket, || {
                let config: rustfs_gateway_types::persistence::PersistedBucketLoggingStatus =
                    persisted_from_s3s(&req.input.bucket_logging_status)?;
                serialize_bucket_config(&config).map_err(|err| rustfs_s3_types::s3_error!(MalformedXML, "{err}"))
            })
            .await?;
        Ok(S3Response::new(PutBucketLoggingOutput::default()))
    }

    pub(in crate::storage::ecfs) async fn put_bucket_request_payment_edge(
        &self,
        req: S3Request<PutBucketRequestPaymentInput>,
    ) -> S3Result<S3Response<PutBucketRequestPaymentOutput>> {
        let envelope = RequestEnvelope::from_s3s(&req);
        let usecase = s3_api::bucket_usecase_for(self);
        usecase
            .execute_put_bucket_request_payment(envelope, &req.input.bucket, || {
                Ok(persisted_from_s3s(&req.input.request_payment_configuration)?)
            })
            .await?;
        Ok(S3Response::new(PutBucketRequestPaymentOutput::default()))
    }

    pub(in crate::storage::ecfs) async fn put_bucket_website_edge(
        &self,
        req: S3Request<PutBucketWebsiteInput>,
    ) -> S3Result<S3Response<PutBucketWebsiteOutput>> {
        let envelope = RequestEnvelope::from_s3s(&req);
        let usecase = s3_api::bucket_usecase_for(self);
        usecase
            .execute_put_bucket_website(envelope, &req.input.bucket, || Ok(persisted_from_s3s(&req.input.website_configuration)?))
            .await?;
        Ok(S3Response::new(PutBucketWebsiteOutput::default()))
    }
}
