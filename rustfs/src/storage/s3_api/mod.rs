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

#![allow(dead_code)]

//! Facade modules for incremental S3 API extraction from `ecfs.rs`.
//!
//! This file intentionally starts as skeleton-only. Behavior remains in place
//! until each helper is moved with dedicated small refactor steps.

use crate::app::{
    bucket_usecase::DefaultBucketUsecase, multipart_usecase::DefaultMultipartUsecase, object_usecase::DefaultObjectUsecase,
};

pub(crate) mod acl;
pub(crate) mod bucket;
pub(crate) mod common;
pub(crate) mod multipart;
pub(crate) mod tagging;

pub(crate) fn default_bucket_usecase() -> DefaultBucketUsecase {
    DefaultBucketUsecase::from_global()
}

pub(crate) fn default_multipart_usecase() -> DefaultMultipartUsecase {
    DefaultMultipartUsecase::from_global()
}

pub(crate) fn default_object_usecase() -> DefaultObjectUsecase {
    DefaultObjectUsecase::from_global()
}

/// Resolve the object use-case for a server's request path (backlog#1052 S6):
/// bind it to the server's own application context so it resolves that
/// server's store instead of the ambient process default.
pub(crate) fn object_usecase_for(fs: &crate::storage::ecfs::FS) -> DefaultObjectUsecase {
    DefaultObjectUsecase::with_context(fs.server_ctx().app_context())
}

pub(crate) fn bucket_usecase_for(fs: &crate::storage::ecfs::FS) -> DefaultBucketUsecase {
    DefaultBucketUsecase::with_context(fs.server_ctx().app_context())
}

pub(crate) fn multipart_usecase_for(fs: &crate::storage::ecfs::FS) -> DefaultMultipartUsecase {
    DefaultMultipartUsecase::with_context(fs.server_ctx().app_context())
}
