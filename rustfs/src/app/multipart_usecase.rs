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

//! Multipart application use-case contracts.
#![allow(dead_code)]

use crate::app::context::AppContext;
use crate::error::ApiError;
use std::collections::HashMap;
use std::sync::Arc;

pub type MultipartUsecaseResult<T> = Result<T, ApiError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateMultipartUploadRequest {
    pub bucket: String,
    pub key: String,
    pub metadata: HashMap<String, String>,
    pub content_type: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CreateMultipartUploadResponse {
    pub upload_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadPartRequest {
    pub bucket: String,
    pub key: String,
    pub upload_id: String,
    pub part_number: i32,
    pub content_length: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UploadPartResponse {
    pub etag: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompleteMultipartUploadPart {
    pub part_number: i32,
    pub etag: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompleteMultipartUploadRequest {
    pub bucket: String,
    pub key: String,
    pub upload_id: String,
    pub parts: Vec<CompleteMultipartUploadPart>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompleteMultipartUploadResponse {
    pub etag: Option<String>,
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbortMultipartUploadRequest {
    pub bucket: String,
    pub key: String,
    pub upload_id: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AbortMultipartUploadResponse;

#[async_trait::async_trait]
pub trait MultipartUsecase: Send + Sync {
    async fn create_multipart_upload(
        &self,
        req: CreateMultipartUploadRequest,
    ) -> MultipartUsecaseResult<CreateMultipartUploadResponse>;

    async fn upload_part(&self, req: UploadPartRequest) -> MultipartUsecaseResult<UploadPartResponse>;

    async fn complete_multipart_upload(
        &self,
        req: CompleteMultipartUploadRequest,
    ) -> MultipartUsecaseResult<CompleteMultipartUploadResponse>;

    async fn abort_multipart_upload(
        &self,
        req: AbortMultipartUploadRequest,
    ) -> MultipartUsecaseResult<AbortMultipartUploadResponse>;
}

#[derive(Clone)]
pub struct DefaultMultipartUsecase {
    context: Arc<AppContext>,
}

impl DefaultMultipartUsecase {
    pub fn new(context: Arc<AppContext>) -> Self {
        Self { context }
    }

    pub fn context(&self) -> Arc<AppContext> {
        self.context.clone()
    }
}
