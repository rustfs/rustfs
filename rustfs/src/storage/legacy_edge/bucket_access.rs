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

//! Legacy s3s edge of bucket policy, policy status, versioning, notification and ACL: batch A2 of
//! the gateway migration (rustfs/backlog#2765).
//!
//! `impl S3 for FS` in `ecfs.rs` calls the `*_edge` fn of each operation here. The bodies are the
//! former trait method bodies, unchanged; the batch that moves these operations onto the gateway
//! owns this file and is the only one that edits it.

use crate::error::ApiError;
use crate::storage::ecfs::*;
use crate::storage::helper::audit_only_operation;
use crate::storage::s3_api;
use crate::storage::storage_api::ecfs_consumer::contract::bucket::{BucketOperations, BucketOptions};
use crate::storage::{persisted_from_s3s, persisted_to_s3s};
use rustfs_gateway_types::compat::s3s_5761ddfe::generated::ops::{
    get_bucket_acl, get_bucket_policy, get_bucket_policy_status, get_bucket_versioning,
};
use rustfs_io_metrics::record_s3_op;
use rustfs_s3_ops::S3Operation;
use tracing::instrument;

/// Handler bodies that [`S3`] runs through `audit_only_operation`.
impl FS {
    async fn put_bucket_acl_inner(&self, req: S3Request<PutBucketAclInput>) -> S3Result<S3Response<PutBucketAclOutput>> {
        let PutBucketAclInput {
            bucket,
            access_control_policy,
            ..
        } = req.input;
        record_s3_op(S3Operation::PutBucketAcl);

        let Some(store) = self.server_ctx.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        if access_control_policy.is_some() {
            return Err(s3_error!(
                NotImplemented,
                "ACL XML grants are not supported; use canned ACL headers or omit ACL"
            ));
        }

        Ok(S3Response::new(PutBucketAclOutput::default()))
    }
}

impl FS {
    pub(in crate::storage::ecfs) async fn delete_bucket_policy_edge(
        &self,
        req: S3Request<DeleteBucketPolicyInput>,
    ) -> S3Result<S3Response<DeleteBucketPolicyOutput>> {
        let usecase = s3_api::bucket_usecase_for(self);
        audit_only_operation(req, S3Operation::DeleteBucketPolicy, |req| usecase.execute_delete_bucket_policy(req)).await
    }

    pub(in crate::storage::ecfs) async fn get_bucket_acl_edge(
        &self,
        req: S3Request<GetBucketAclInput>,
    ) -> S3Result<S3Response<GetBucketAclOutput>> {
        record_s3_op(S3Operation::GetBucketAcl);
        let usecase = s3_api::bucket_usecase_for(self);
        through_gateway_seam(req, get_bucket_acl::input_from_s3s, get_bucket_acl::output_to_s3s, |_, input| {
            usecase.execute_get_bucket_acl(input)
        })
        .await
    }

    pub(in crate::storage::ecfs) async fn get_bucket_notification_configuration_edge(
        &self,
        req: S3Request<GetBucketNotificationConfigurationInput>,
    ) -> S3Result<S3Response<GetBucketNotificationConfigurationOutput>> {
        record_s3_op(S3Operation::GetBucketNotificationConfiguration);
        let usecase = s3_api::bucket_usecase_for(self);
        let notification_configuration = usecase
            .execute_get_bucket_notification_configuration(req)
            .await?
            .as_ref()
            .map(persisted_to_s3s::<_, NotificationConfiguration>)
            .transpose()?;
        match notification_configuration {
            Some(NotificationConfiguration {
                event_bridge_configuration,
                lambda_function_configurations,
                queue_configurations,
                topic_configurations,
            }) => Ok(S3Response::new(GetBucketNotificationConfigurationOutput {
                event_bridge_configuration,
                lambda_function_configurations,
                queue_configurations,
                topic_configurations,
            })),
            None => Ok(S3Response::new(GetBucketNotificationConfigurationOutput::default())),
        }
    }

    pub(in crate::storage::ecfs) async fn get_bucket_policy_edge(
        &self,
        req: S3Request<GetBucketPolicyInput>,
    ) -> S3Result<S3Response<GetBucketPolicyOutput>> {
        record_s3_op(S3Operation::GetBucketPolicy);
        let usecase = s3_api::bucket_usecase_for(self);
        through_gateway_seam(req, get_bucket_policy::input_from_s3s, get_bucket_policy::output_to_s3s, |_, input| {
            usecase.execute_get_bucket_policy(input)
        })
        .await
    }

    pub(in crate::storage::ecfs) async fn get_bucket_policy_status_edge(
        &self,
        req: S3Request<GetBucketPolicyStatusInput>,
    ) -> S3Result<S3Response<GetBucketPolicyStatusOutput>> {
        record_s3_op(S3Operation::GetBucketPolicyStatus);
        let usecase = s3_api::bucket_usecase_for(self);
        through_gateway_seam(
            req,
            get_bucket_policy_status::input_from_s3s,
            get_bucket_policy_status::output_to_s3s,
            |envelope, input| usecase.execute_get_bucket_policy_status(envelope, input),
        )
        .await
    }

    #[instrument(name = "get_bucket_versioning", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn get_bucket_versioning_edge(
        &self,
        req: S3Request<GetBucketVersioningInput>,
    ) -> S3Result<S3Response<GetBucketVersioningOutput>> {
        record_s3_op(S3Operation::GetBucketVersioning);
        let usecase = s3_api::bucket_usecase_for(self);
        through_gateway_seam(
            req,
            get_bucket_versioning::input_from_s3s,
            get_bucket_versioning::output_to_s3s,
            |_, input| usecase.execute_get_bucket_versioning(input),
        )
        .await
    }

    pub(in crate::storage::ecfs) async fn put_bucket_acl_edge(
        &self,
        req: S3Request<PutBucketAclInput>,
    ) -> S3Result<S3Response<PutBucketAclOutput>> {
        audit_only_operation(req, S3Operation::PutBucketAcl, |req| self.put_bucket_acl_inner(req)).await
    }

    pub(in crate::storage::ecfs) async fn put_bucket_notification_configuration_edge(
        &self,
        req: S3Request<PutBucketNotificationConfigurationInput>,
    ) -> S3Result<S3Response<PutBucketNotificationConfigurationOutput>> {
        let prepared = persisted_from_s3s(&req.input.notification_configuration);
        let usecase = s3_api::bucket_usecase_for(self);
        audit_only_operation(req, S3Operation::PutBucketNotificationConfiguration, |req| {
            usecase.execute_put_bucket_notification_configuration(req, prepared)
        })
        .await
    }

    pub(in crate::storage::ecfs) async fn put_bucket_policy_edge(
        &self,
        req: S3Request<PutBucketPolicyInput>,
    ) -> S3Result<S3Response<PutBucketPolicyOutput>> {
        let usecase = s3_api::bucket_usecase_for(self);
        audit_only_operation(req, S3Operation::PutBucketPolicy, |req| usecase.execute_put_bucket_policy(req)).await
    }

    #[instrument(name = "put_bucket_versioning", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn put_bucket_versioning_edge(
        &self,
        req: S3Request<PutBucketVersioningInput>,
    ) -> S3Result<S3Response<PutBucketVersioningOutput>> {
        let prepared = persisted_from_s3s(&req.input.versioning_configuration);
        let usecase = s3_api::bucket_usecase_for(self);
        audit_only_operation(req, S3Operation::PutBucketVersioning, |req| {
            usecase.execute_put_bucket_versioning(req, prepared)
        })
        .await
    }
}
