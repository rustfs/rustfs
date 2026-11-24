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

use crate::app::object_usecase::DefaultObjectUsecase;
use crate::app::{RequestEnvelope, RequestHead};
use rustfs_gateway_types::{BucketName, ObjectKey, dto as gateway_dto};

/// The method-only HEAD fixture uses `/`; operation fields carry the bucket and key.
pub(crate) fn head_envelope(headers: http::HeaderMap, extensions: http::Extensions) -> RequestEnvelope {
    RequestEnvelope::from_parts(
        RequestHead {
            method: http::Method::HEAD,
            uri: http::Uri::from_static("/"),
            headers,
            credentials: None,
            region: None,
            service: None,
        },
        extensions,
    )
}

pub(crate) fn head_object_input(bucket: &str, key: &str, version_id: Option<String>) -> gateway_dto::HeadObjectInput {
    gateway_dto::HeadObjectInput {
        bucket: BucketName::new(bucket).expect("test bucket name"),
        key: ObjectKey::new(key).expect("test object key"),
        version_id,
        ..Default::default()
    }
}

/// Runs a gateway HEAD input through the production field projection.
#[cfg(test)]
pub(crate) async fn head_object_gateway(
    usecase: &DefaultObjectUsecase,
    envelope: crate::app::RequestEnvelope,
    input: rustfs_gateway_types::dto::HeadObjectInput,
) -> rustfs_s3_types::S3Result<crate::app::Answered<crate::app::object_usecase::HeadObjectOutputFacts>> {
    rustfs_io_metrics::record_s3_op(rustfs_s3_ops::S3Operation::HeadObject);
    head_object_fields(usecase, envelope, input.into()).await
}

/// Calls `DefaultObjectUsecase::execute_head_object_fields` with owned inputs.
pub(crate) async fn head_object_fields(
    usecase: &DefaultObjectUsecase,
    envelope: crate::app::RequestEnvelope,
    input: crate::app::object_usecase::HeadObjectFields,
) -> rustfs_s3_types::S3Result<crate::app::Answered<crate::app::object_usecase::HeadObjectOutputFacts>> {
    usecase.execute_head_object_fields(envelope, input).await
}
