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

#![cfg(test)]

//! Legacy s3s names used by app-layer compatibility tests.
//!
//! The acceptance grep for the app use-case migration covers `rustfs/src/app`
//! and `rustfs/src/storage/s3_api`. These re-exports stay outside both trees
//! so the tests can keep their existing paths through `app::storage_api`.

pub(crate) use s3s::dto::RestoreRequest as LegacyRestoreRequest;

pub(crate) mod request_body {
    pub(crate) use s3s::dto::StreamingBlob;
    pub(crate) use s3s::stream::{ByteStream, DynByteStream, RemainingLength};
}

pub(crate) mod s3 {
    pub(crate) use s3s::Body;
    pub(crate) use s3s::S3Request;
    pub(crate) use s3s::TrailingHeaders;
    pub(crate) use s3s::auth::Credentials;
    pub(crate) use s3s::dto;
    pub(crate) use s3s::dto::PutObjectInput;
    pub(crate) use s3s::dto::StreamingBlob;
    pub(crate) use s3s::stream::upload_stream::UploadStreamError;
    pub(crate) use s3s::xml;
    pub(crate) use s3s::{S3Error, S3ErrorCode, S3Result};
}
