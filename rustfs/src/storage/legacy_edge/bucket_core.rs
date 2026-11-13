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

use crate::app::dto_values::stored_bucket_name;
use crate::storage::access::bucket_config_mutation_incarnation;
use crate::storage::ecfs::*;
use crate::storage::helper::audit_only_operation;
use crate::storage::s3_api;
use crate::storage::storage_api::ecfs_consumer::contract::bucket::{BucketOperations, BucketOptions};
use crate::storage::storage_api::ecstore_bucket::utils::serialize as serialize_bucket_config;
use crate::storage::validate_website_configuration;
use crate::storage::{
    BUCKET_ACCELERATE_CONFIG, BUCKET_LOGGING_CONFIG, BUCKET_REQUEST_PAYMENT_CONFIG, BUCKET_WEBSITE_CONFIG, StorageError,
    delete_bucket_metadata_config_if_incarnation, get_bucket_accelerate_config, get_bucket_logging_config,
    get_bucket_request_payment_config, get_bucket_website_config, update_bucket_metadata_config_if_incarnation,
};
use crate::storage::{persisted_from_s3s, persisted_to_s3s};
use rustfs_gateway_types::compat::s3s_5761ddfe::generated::ops::head_bucket;
use rustfs_gateway_types::dto::HeadBucketInput as HeadBucketGatewayInput;
use rustfs_io_metrics::record_s3_op;
use rustfs_s3_ops::S3Operation;
use tracing::instrument;

/// Handler bodies that [`S3`] runs through `audit_only_operation`.
impl FS {
    async fn put_bucket_logging_inner(
        &self,
        req: S3Request<PutBucketLoggingInput>,
    ) -> S3Result<S3Response<PutBucketLoggingOutput>> {
        let expected_incarnation_id = bucket_config_mutation_incarnation(&req, &req.input.bucket)?;
        record_s3_op(S3Operation::PutBucketLogging);
        let Some(store) = self.server_ctx.object_store() else {
            return Err(s3_error!(InternalError, "Not init"));
        };
        store
            .get_bucket_info(&req.input.bucket, &BucketOptions::default())
            .await
            .map_err(crate::error::ApiError::from)?;

        let bucket_logging_status = persisted_from_s3s::<_, rustfs_gateway_types::persistence::PersistedBucketLoggingStatus>(
            &req.input.bucket_logging_status,
        )?;
        let logging_config = serialize_bucket_config(&bucket_logging_status)
            .map_err(|err| S3Error::with_message(S3ErrorCode::MalformedXML, format!("{err}")))?;
        update_bucket_metadata_config_if_incarnation(
            &req.input.bucket,
            BUCKET_LOGGING_CONFIG,
            logging_config,
            expected_incarnation_id,
        )
        .await
        .map_err(crate::error::ApiError::from)?;

        Ok(S3Response::new(PutBucketLoggingOutput::default()))
    }
}

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
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_create_bucket(req).await
    }

    /// Delete a bucket
    #[instrument(name = "delete_bucket", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn delete_bucket_edge(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_delete_bucket(req).await
    }

    pub(in crate::storage::ecfs) async fn delete_bucket_website_edge(
        &self,
        req: S3Request<DeleteBucketWebsiteInput>,
    ) -> S3Result<S3Response<DeleteBucketWebsiteOutput>> {
        let expected_incarnation_id = bucket_config_mutation_incarnation(&req, &req.input.bucket)?;
        let Some(store) = self.server_ctx.object_store() else {
            return Err(s3_error!(InternalError, "Not init"));
        };

        store
            .get_bucket_info(&req.input.bucket, &BucketOptions::default())
            .await
            .map_err(crate::error::ApiError::from)?;

        delete_bucket_metadata_config_if_incarnation(&req.input.bucket, BUCKET_WEBSITE_CONFIG, expected_incarnation_id)
            .await
            .map_err(crate::error::ApiError::from)?;

        Ok(S3Response::new(DeleteBucketWebsiteOutput::default()))
    }

    pub(in crate::storage::ecfs) async fn get_bucket_accelerate_configuration_edge(
        &self,
        req: S3Request<GetBucketAccelerateConfigurationInput>,
    ) -> S3Result<S3Response<GetBucketAccelerateConfigurationOutput>> {
        let Some(store) = self.server_ctx.object_store() else {
            return Err(s3_error!(InternalError, "Not init"));
        };

        store
            .get_bucket_info(&req.input.bucket, &BucketOptions::default())
            .await
            .map_err(crate::error::ApiError::from)?;

        match get_bucket_accelerate_config(&req.input.bucket).await {
            Ok((accelerate, _)) => Ok(S3Response::new(GetBucketAccelerateConfigurationOutput {
                status: accelerate.status.map(BucketAccelerateStatus::from),
                ..Default::default()
            })),
            Err(StorageError::ConfigNotFound) => Ok(S3Response::new(GetBucketAccelerateConfigurationOutput::default())),
            Err(err) => Err(crate::error::ApiError::from(err).into()),
        }
    }

    /// Get bucket location
    #[instrument(name = "get_bucket_location", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn get_bucket_location_edge(
        &self,
        req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        record_s3_op(S3Operation::GetBucketLocation);
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_get_bucket_location(req).await
    }

    pub(in crate::storage::ecfs) async fn get_bucket_request_payment_edge(
        &self,
        req: S3Request<GetBucketRequestPaymentInput>,
    ) -> S3Result<S3Response<GetBucketRequestPaymentOutput>> {
        let Some(store) = self.server_ctx.object_store() else {
            return Err(s3_error!(InternalError, "Not init"));
        };

        store
            .get_bucket_info(&req.input.bucket, &BucketOptions::default())
            .await
            .map_err(crate::error::ApiError::from)?;

        match get_bucket_request_payment_config(&req.input.bucket).await {
            Ok((payment, _)) => Ok(S3Response::new(GetBucketRequestPaymentOutput {
                payer: Some(Payer::from(payment.payer)),
            })),
            Err(StorageError::ConfigNotFound) => Ok(S3Response::new(GetBucketRequestPaymentOutput {
                payer: Some(Payer::from_static(Payer::BUCKET_OWNER)),
            })),
            Err(err) => Err(crate::error::ApiError::from(err).into()),
        }
    }

    pub(in crate::storage::ecfs) async fn get_bucket_website_edge(
        &self,
        req: S3Request<GetBucketWebsiteInput>,
    ) -> S3Result<S3Response<GetBucketWebsiteOutput>> {
        let Some(store) = self.server_ctx.object_store() else {
            return Err(s3_error!(InternalError, "Not init"));
        };

        store
            .get_bucket_info(&req.input.bucket, &BucketOptions::default())
            .await
            .map_err(crate::error::ApiError::from)?;

        match get_bucket_website_config(&req.input.bucket).await {
            Ok((website, _)) => {
                let website: WebsiteConfiguration = persisted_to_s3s(&website)?;
                Ok(S3Response::new(GetBucketWebsiteOutput {
                    error_document: website.error_document,
                    index_document: website.index_document,
                    redirect_all_requests_to: website.redirect_all_requests_to,
                    routing_rules: website.routing_rules,
                }))
            }
            Err(StorageError::ConfigNotFound) => Err(s3_error!(NoSuchWebsiteConfiguration)),
            Err(err) => Err(crate::error::ApiError::from(err).into()),
        }
    }

    #[instrument(name = "head_bucket", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn head_bucket_edge(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        record_s3_op(S3Operation::HeadBucket);
        // Interim until the re-pin onto rustfs/gateway#1610: the seam's input conversion holds the
        // bucket to the AWS grammar, which refuses names RustFS holds, so the name is taken under the
        // floor here and only the output crosses the seam.
        let input = HeadBucketGatewayInput {
            bucket: stored_bucket_name(&req.input.bucket)?,
            ..Default::default()
        };
        let output = s3_api::bucket_usecase_for(self).execute_head_bucket(input).await?;
        Ok(S3Response::new(head_bucket::output_to_s3s(output).map_err(seam_error)?))
    }

    pub(in crate::storage::ecfs) async fn put_bucket_accelerate_configuration_edge(
        &self,
        req: S3Request<PutBucketAccelerateConfigurationInput>,
    ) -> S3Result<S3Response<PutBucketAccelerateConfigurationOutput>> {
        let expected_incarnation_id = bucket_config_mutation_incarnation(&req, &req.input.bucket)?;
        let Some(store) = self.server_ctx.object_store() else {
            return Err(s3_error!(InternalError, "Not init"));
        };
        store
            .get_bucket_info(&req.input.bucket, &BucketOptions::default())
            .await
            .map_err(crate::error::ApiError::from)?;

        let accelerate_configuration = persisted_from_s3s::<
            _,
            rustfs_gateway_types::persistence::PersistedAccelerateConfiguration,
        >(&req.input.accelerate_configuration)?;
        let accelerate_config = serialize_bucket_config(&accelerate_configuration)
            .map_err(|err| S3Error::with_message(S3ErrorCode::MalformedXML, format!("{err}")))?;
        update_bucket_metadata_config_if_incarnation(
            &req.input.bucket,
            BUCKET_ACCELERATE_CONFIG,
            accelerate_config,
            expected_incarnation_id,
        )
        .await
        .map_err(crate::error::ApiError::from)?;

        Ok(S3Response::new(PutBucketAccelerateConfigurationOutput::default()))
    }

    pub(in crate::storage::ecfs) async fn get_bucket_logging_edge(
        &self,
        req: S3Request<GetBucketLoggingInput>,
    ) -> S3Result<S3Response<GetBucketLoggingOutput>> {
        record_s3_op(S3Operation::GetBucketLogging);
        let Some(store) = self.server_ctx.object_store() else {
            return Err(s3_error!(InternalError, "Not init"));
        };
        store
            .get_bucket_info(&req.input.bucket, &BucketOptions::default())
            .await
            .map_err(crate::error::ApiError::from)?;

        match get_bucket_logging_config(&req.input.bucket).await {
            Ok((logging, _)) => {
                let logging: BucketLoggingStatus = persisted_to_s3s(&logging)?;
                Ok(S3Response::new(GetBucketLoggingOutput {
                    logging_enabled: logging.logging_enabled,
                }))
            }
            Err(StorageError::ConfigNotFound) => Ok(S3Response::new(GetBucketLoggingOutput::default())),
            Err(err) => Err(crate::error::ApiError::from(err).into()),
        }
    }

    pub(in crate::storage::ecfs) async fn put_bucket_logging_edge(
        &self,
        req: S3Request<PutBucketLoggingInput>,
    ) -> S3Result<S3Response<PutBucketLoggingOutput>> {
        audit_only_operation(req, S3Operation::PutBucketLogging, |req| self.put_bucket_logging_inner(req)).await
    }

    pub(in crate::storage::ecfs) async fn put_bucket_request_payment_edge(
        &self,
        req: S3Request<PutBucketRequestPaymentInput>,
    ) -> S3Result<S3Response<PutBucketRequestPaymentOutput>> {
        let expected_incarnation_id = bucket_config_mutation_incarnation(&req, &req.input.bucket)?;
        let Some(store) = self.server_ctx.object_store() else {
            return Err(s3_error!(InternalError, "Not init"));
        };
        store
            .get_bucket_info(&req.input.bucket, &BucketOptions::default())
            .await
            .map_err(crate::error::ApiError::from)?;

        let request_payment_configuration = persisted_from_s3s::<
            _,
            rustfs_gateway_types::persistence::PersistedRequestPaymentConfiguration,
        >(&req.input.request_payment_configuration)?;
        let payment_config = serialize_bucket_config(&request_payment_configuration)
            .map_err(|err| S3Error::with_message(S3ErrorCode::MalformedXML, format!("{err}")))?;
        update_bucket_metadata_config_if_incarnation(
            &req.input.bucket,
            BUCKET_REQUEST_PAYMENT_CONFIG,
            payment_config,
            expected_incarnation_id,
        )
        .await
        .map_err(crate::error::ApiError::from)?;

        Ok(S3Response::new(PutBucketRequestPaymentOutput::default()))
    }

    pub(in crate::storage::ecfs) async fn put_bucket_website_edge(
        &self,
        req: S3Request<PutBucketWebsiteInput>,
    ) -> S3Result<S3Response<PutBucketWebsiteOutput>> {
        let expected_incarnation_id = bucket_config_mutation_incarnation(&req, &req.input.bucket)?;
        let Some(store) = self.server_ctx.object_store() else {
            return Err(s3_error!(InternalError, "Not init"));
        };
        store
            .get_bucket_info(&req.input.bucket, &BucketOptions::default())
            .await
            .map_err(crate::error::ApiError::from)?;

        let website_configuration = persisted_from_s3s::<_, rustfs_gateway_types::persistence::PersistedWebsiteConfiguration>(
            &req.input.website_configuration,
        )?;
        validate_website_configuration(&website_configuration)?;

        let website_config = serialize_bucket_config(&website_configuration)
            .map_err(|err| S3Error::with_message(S3ErrorCode::MalformedXML, format!("{err}")))?;
        update_bucket_metadata_config_if_incarnation(
            &req.input.bucket,
            BUCKET_WEBSITE_CONFIG,
            website_config,
            expected_incarnation_id,
        )
        .await
        .map_err(crate::error::ApiError::from)?;

        Ok(S3Response::new(PutBucketWebsiteOutput::default()))
    }
}
