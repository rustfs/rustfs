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

//! Request and response projections for protocol reads and single-object deletion.
//! Authorization stays on the protocol session edge; storage runs in the client's server context.

use super::{ProtocolStorageClient, RequestParams};
use crate::app::object_usecase::{DefaultObjectUsecase, GetObjectBody, GetObjectOutputFacts, HeadObjectOutputFacts};
use crate::app::{RequestCredentials, RequestEnvelope, RequestHead};
use crate::runtime_sources::current_action_credentials;
use crate::storage_api::protocols::client::{ReqInfo, RequestContext};
use http::{HeaderMap, Method, Uri};
use rustfs_gateway_types::{ETag, Timestamp, dto};
use rustfs_s3_types::{S3Error, S3ErrorCode, S3Result};

impl ProtocolStorageClient {
    pub(super) async fn get_object_owned(
        &self,
        bucket: &str,
        key: &str,
        credentials: &rustfs_credentials::Credentials,
        range: Option<rustfs_gateway_types::ByteRange>,
    ) -> S3Result<dto::GetObjectOutput> {
        let uri = super::build_object_uri(bucket, key, &[])?;
        let envelope = Self::create_envelope(
            Method::GET,
            uri,
            RequestParams {
                bucket: Some(bucket.to_owned()),
                object: Some(key.to_owned()),
                credentials,
            },
        );
        rustfs_io_metrics::record_s3_op(rustfs_s3_ops::S3Operation::GetObject);
        self.owned_usecase()
            .execute_get_object_fields(
                envelope,
                crate::app::object_usecase::GetObjectFields {
                    bucket: bucket.to_owned(),
                    key: key.to_owned(),
                    version_id: None,
                    part_number: None,
                    range,
                    sse_customer_key: None,
                    sse_customer_key_md5: None,
                },
            )
            .await
            .and_then(|answered| get_output(answered.output))
    }

    pub(super) fn owned_usecase(&self) -> DefaultObjectUsecase {
        DefaultObjectUsecase::with_context(self.fs.server_ctx().app_context())
    }

    pub(super) fn create_envelope(method: Method, uri: Uri, params: RequestParams<'_>) -> RequestEnvelope {
        let is_owner = current_action_credentials().is_some_and(|root| params.credentials.access_key == root.access_key);
        let credentials =
            RequestCredentials::new(params.credentials.access_key.clone(), params.credentials.secret_key.clone(), None)
                .unwrap_or_else(|_| RequestCredentials::without_secret_key(params.credentials.access_key.clone(), None));
        let mut extensions = http::Extensions::new();
        extensions.insert(ReqInfo {
            cred: Some(params.credentials.clone()),
            is_owner,
            bucket: params.bucket,
            object: params.object,
            version_id: None,
            replication_request_authorized: false,
            region: None,
            request_context: Some(RequestContext::fallback()),
            suppress_denial_log: false,
            suppress_denial_audit: false,
        });
        RequestEnvelope::from_parts(
            RequestHead {
                method,
                uri,
                headers: HeaderMap::new(),
                credentials: Some(credentials),
                region: None,
                service: None,
            },
            extensions,
        )
    }
}

fn etag(opaque: String, weak: bool) -> S3Result<ETag> {
    if weak { ETag::new_weak(opaque) } else { ETag::new(opaque) }
        .map_err(|_| S3Error::with_message(S3ErrorCode::InternalError, "Object response contains an invalid ETag"))
}

// Protocol clients received millisecond timestamps through the legacy response codec.
fn millisecond_timestamp(value: Timestamp) -> Timestamp {
    Timestamp::from_secs_nanos(value.secs(), value.subsec_millis() * 1_000_000)
        .expect("a millisecond remainder is below one second") // subsec_millis is at most 999.
}

pub(super) fn get_output(mut output: GetObjectOutputFacts) -> S3Result<dto::GetObjectOutput> {
    output.dto.last_modified = output.dto.last_modified.map(millisecond_timestamp);
    output.dto.object_lock_retain_until_date = output.dto.object_lock_retain_until_date.map(millisecond_timestamp);
    output.dto.e_tag = output.etag.map(|tag| etag(tag.opaque, tag.weak)).transpose()?;
    output.dto.body = output
        .body
        .map(|body| match body {
            GetObjectBody::Bytes(bytes) => Ok(rustfs_gateway_stream::ByteStream::from_bytes(bytes)),
            GetObjectBody::Stream(stream) => rustfs_gateway_stream::ByteStream::new(stream)
                .map_err(|_| S3Error::with_message(S3ErrorCode::InternalError, "Invalid GetObject body capabilities")),
        })
        .transpose()?;
    Ok(output.dto)
}

pub(super) fn head_output(mut output: HeadObjectOutputFacts) -> S3Result<dto::HeadObjectOutput> {
    output.dto.last_modified = output.dto.last_modified.map(millisecond_timestamp);
    output.dto.object_lock_retain_until_date = output.dto.object_lock_retain_until_date.map(millisecond_timestamp);
    output.dto.e_tag = output.etag.map(|tag| etag(tag.opaque, tag.weak)).transpose()?;
    Ok(output.dto)
}
