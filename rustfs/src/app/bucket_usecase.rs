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

//! Bucket application use-case contracts.
#![allow(dead_code)]

use crate::app::context::AppContext;
use crate::error::ApiError;
use std::sync::Arc;

pub type BucketUsecaseResult<T> = Result<T, ApiError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateBucketRequest {
    pub bucket: String,
    pub object_lock_enabled: Option<bool>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CreateBucketResponse;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteBucketRequest {
    pub bucket: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeleteBucketResponse;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadBucketRequest {
    pub bucket: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HeadBucketResponse;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListObjectsV2Request {
    pub bucket: String,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub continuation_token: Option<String>,
    pub max_keys: Option<i32>,
    pub fetch_owner: Option<bool>,
    pub start_after: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListObjectsV2Item {
    pub key: String,
    pub etag: Option<String>,
    pub size: i64,
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListObjectsV2Response {
    pub objects: Vec<ListObjectsV2Item>,
    pub common_prefixes: Vec<String>,
    pub key_count: i32,
    pub is_truncated: bool,
    pub next_continuation_token: Option<String>,
}

#[async_trait::async_trait]
pub trait BucketUsecase: Send + Sync {
    async fn create_bucket(&self, req: CreateBucketRequest) -> BucketUsecaseResult<CreateBucketResponse>;

    async fn delete_bucket(&self, req: DeleteBucketRequest) -> BucketUsecaseResult<DeleteBucketResponse>;

    async fn head_bucket(&self, req: HeadBucketRequest) -> BucketUsecaseResult<HeadBucketResponse>;

    async fn list_objects_v2(&self, req: ListObjectsV2Request) -> BucketUsecaseResult<ListObjectsV2Response>;
}

#[derive(Clone)]
pub struct DefaultBucketUsecase {
    context: Arc<AppContext>,
}

impl DefaultBucketUsecase {
    pub fn new(context: Arc<AppContext>) -> Self {
        Self { context }
    }

    pub fn context(&self) -> Arc<AppContext> {
        self.context.clone()
    }
}
