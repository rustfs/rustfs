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

//! Fixtures and test-only imports shared by the multipart use-case tests.

use super::*;
pub(super) use crate::app::storage_api::s3::dto::CompletedMultipartUpload;
pub(super) use crate::app::storage_api::s3::dto::StorageClass;
pub(super) use http::Extensions;
pub(super) use http::Method;
pub(super) use rustfs_filemeta::ObjectPartInfo;
pub(super) use rustfs_utils::http::AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER;
pub(super) use rustfs_utils::http::AMZ_OBJECT_LOCK_MODE_LOWER;
pub(super) use rustfs_utils::http::AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER;
pub(super) use std::io::Cursor;
pub(super) use temp_env::async_with_vars;
pub(super) use tokio::io::AsyncReadExt;

pub(super) fn s3_op_total(op: S3Operation) -> u64 {
    rustfs_io_metrics::s3_op_metrics_snapshot()
        .into_iter()
        .find(|snapshot| snapshot.op == op.as_str())
        .map(|snapshot| snapshot.total)
        .unwrap_or_default()
}

pub(super) fn build_request<T>(input: T, method: Method) -> S3Request<T> {
    S3Request {
        input,
        method,
        uri: Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions: Extensions::new(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

pub(super) fn make_usecase() -> DefaultMultipartUsecase {
    DefaultMultipartUsecase::without_context()
}
