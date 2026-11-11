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

//! `ListBuckets` on the gateway stack.
//!
//! Responsible for: converting one accepted request into the envelope and input the RustFS use
//! case takes, and its answer back.
//! NOT responsible for: deciding which buckets the caller sees. The legacy access hook for this
//! operation is empty and so is this one: `DefaultBucketUsecase::execute_list_buckets` decides
//! per caller (every bucket with `s3:ListAllMyBuckets`, otherwise the buckets the caller may
//! list or locate, `AccessDenied` when there is none), for both stacks.
//! Upstream: the gateway's dispatch. Downstream: `crate::app::bucket_usecase`.

use super::super::GatewayBackend;
use super::super::access;
use super::super::error::handler_error;
use crate::app::RequestEnvelope;
use crate::app::bucket_usecase::DefaultBucketUsecase;
use rustfs_gateway::{Handler, HandlerResult, Operation, Req, Resp, dto};
use rustfs_io_metrics::record_s3_op;
use rustfs_s3_ops::S3Operation;

impl Handler<dto::ListBuckets> for GatewayBackend {
    async fn call(&self, request: Req<dto::ListBuckets>) -> HandlerResult<dto::ListBuckets> {
        record_s3_op(S3Operation::ListBuckets);
        let mut envelope = RequestEnvelope::from_gateway(request.context(), self.server_ctx()).map_err(handler_error)?;
        access::check(self.server_ctx(), &mut envelope, <dto::ListBuckets as Operation>::NAME)
            .await
            .map_err(handler_error)?;
        let usecase = DefaultBucketUsecase::with_context(self.server_ctx().app_context());
        let output = usecase
            .execute_list_buckets(envelope, request.into_input())
            .await
            .map_err(handler_error)?;
        Ok(Resp::new(output))
    }
}
