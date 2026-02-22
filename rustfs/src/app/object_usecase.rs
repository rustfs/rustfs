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

//! Object application use-case contracts.
#![allow(dead_code)]

use crate::app::context::AppContext;
use crate::error::ApiError;
use std::sync::Arc;

pub type ObjectUsecaseResult<T> = Result<T, ApiError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutObjectRequest {
    pub bucket: String,
    pub key: String,
    pub content_length: Option<i64>,
    pub content_type: Option<String>,
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PutObjectResponse {
    pub etag: String,
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetObjectRequest {
    pub bucket: String,
    pub key: String,
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GetObjectResponse {
    pub etag: Option<String>,
    pub content_length: i64,
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteObjectRequest {
    pub bucket: String,
    pub key: String,
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeleteObjectResponse {
    pub delete_marker: Option<bool>,
    pub version_id: Option<String>,
}

#[async_trait::async_trait]
pub trait ObjectUsecase: Send + Sync {
    async fn put_object(&self, req: PutObjectRequest) -> ObjectUsecaseResult<PutObjectResponse>;

    async fn get_object(&self, req: GetObjectRequest) -> ObjectUsecaseResult<GetObjectResponse>;

    async fn delete_object(&self, req: DeleteObjectRequest) -> ObjectUsecaseResult<DeleteObjectResponse>;
}

#[derive(Clone)]
pub struct DefaultObjectUsecase {
    context: Arc<AppContext>,
}

impl DefaultObjectUsecase {
    pub fn new(context: Arc<AppContext>) -> Self {
        Self { context }
    }

    pub fn context(&self) -> Arc<AppContext> {
        self.context.clone()
    }
}
