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

use crate::app::object::GetObjectBody;
use crate::app::object_usecase::DefaultObjectUsecase;

pub(crate) fn get_envelope(headers: http::HeaderMap, extensions: http::Extensions) -> crate::app::RequestEnvelope {
    crate::app::RequestEnvelope::from_parts(
        crate::app::RequestHead {
            method: http::Method::GET,
            uri: http::Uri::from_static("/"),
            headers,
            credentials: None,
            region: None,
            service: None,
        },
        extensions,
    )
}

pub(crate) fn get_object_input(bucket: &str, key: &str, version_id: Option<String>) -> rustfs_gateway_types::dto::GetObjectInput {
    rustfs_gateway_types::dto::GetObjectInput {
        bucket: rustfs_gateway_types::BucketName::new(bucket).expect("test bucket name"),
        key: rustfs_gateway_types::ObjectKey::new(key).expect("test object key"),
        version_id,
        ..Default::default()
    }
}

pub(crate) async fn get_object_gateway(
    usecase: &DefaultObjectUsecase,
    envelope: crate::app::RequestEnvelope,
    input: rustfs_gateway_types::dto::GetObjectInput,
) -> rustfs_s3_types::S3Result<crate::app::Answered<crate::app::object_usecase::GetObjectOutputFacts>> {
    rustfs_io_metrics::record_s3_op(rustfs_s3_ops::S3Operation::GetObject);
    get_object_fields(usecase, envelope, input.into()).await
}

/// Reads a successful GET body to completion where the caller verifies its bytes.
pub(crate) async fn read_get_body(body: GetObjectBody) -> bytes::Bytes {
    use http_body_util::BodyExt;
    use rustfs_gateway_stream::Body;

    let body = match body {
        GetObjectBody::Bytes(bytes) => Body::from_bytes(bytes),
        GetObjectBody::Stream(stream) => Body::from_stream(stream).expect("valid GET stream"),
    };
    let collected = body.collect().await.expect("GET body must be readable");
    assert!(collected.trailers().is_none(), "unexpected GET response trailers");
    collected.to_bytes()
}

/// Calls `DefaultObjectUsecase::execute_get_object_fields` with owned inputs.
pub(crate) async fn get_object_fields(
    usecase: &DefaultObjectUsecase,
    envelope: crate::app::RequestEnvelope,
    input: crate::app::object_usecase::GetObjectFields,
) -> rustfs_s3_types::S3Result<crate::app::Answered<crate::app::object_usecase::GetObjectOutputFacts>> {
    usecase.execute_get_object_fields(envelope, input).await
}
