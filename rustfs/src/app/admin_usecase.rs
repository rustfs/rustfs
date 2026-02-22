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

//! Admin application use-case contracts.
#![allow(dead_code)]

use crate::app::context::AppContext;
use crate::error::ApiError;
use rustfs_ecstore::admin_server_info::get_server_info;
use rustfs_madmin::InfoMessage;
use std::sync::Arc;

pub type AdminUsecaseResult<T> = Result<T, ApiError>;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryServerInfoRequest {
    pub include_pools: bool,
}

pub struct QueryServerInfoResponse {
    pub info: InfoMessage,
}

#[async_trait::async_trait]
pub trait AdminUsecase: Send + Sync {
    async fn query_server_info(&self, req: QueryServerInfoRequest) -> AdminUsecaseResult<QueryServerInfoResponse>;
}

#[derive(Clone)]
pub struct DefaultAdminUsecase {
    context: Arc<AppContext>,
}

impl DefaultAdminUsecase {
    pub fn new(context: Arc<AppContext>) -> Self {
        Self { context }
    }

    pub fn context(&self) -> Arc<AppContext> {
        self.context.clone()
    }
}

#[async_trait::async_trait]
impl AdminUsecase for DefaultAdminUsecase {
    async fn query_server_info(&self, req: QueryServerInfoRequest) -> AdminUsecaseResult<QueryServerInfoResponse> {
        let _ = self.context.object_store();
        let info = get_server_info(req.include_pools).await;
        Ok(QueryServerInfoResponse { info })
    }
}
