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

use super::{
    ADMIN_PREFIX, AdminOperation, ApiError, Body, Method, Operation, Params, S3ErrorCode, S3Request, S3Response, S3Result,
    S3Router, StatusCode, Uri, admin_error, is_reserved_or_invalid_bucket, is_valid_object_prefix, json_response,
    validate_heal_admin_request,
};
use crate::admin::runtime_sources::object_store_from_req;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use uuid::Uuid;

const ROOT: &str = "/v4/heal/delete-residue";
const DEFAULT_LIMIT: u32 = 32;

pub(super) fn register(router: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    let root = format!("{ADMIN_PREFIX}{ROOT}");
    router.insert(Method::GET, &root, AdminOperation(&ResidueOperation::List))?;
    router.insert(Method::GET, &format!("{root}/inspect"), AdminOperation(&ResidueOperation::Inspect))?;
    router.insert(Method::POST, &format!("{root}/discard"), AdminOperation(&ResidueOperation::Discard))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListQuery {
    bucket: String,
    cursor: Option<Uuid>,
    limit: Option<u32>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Target {
    bucket: String,
    object: String,
    pool: usize,
    set: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct DiscardRequest {
    bucket: String,
    object: String,
    pool: usize,
    set: usize,
    expected_fingerprint: String,
}

fn query<T: DeserializeOwned>(uri: &Uri) -> S3Result<T> {
    let query = uri.query().unwrap_or_default();
    // Reject invalid UTF-8 before form decoding, which otherwise replaces it and can select a different key.
    percent_encoding::percent_decode_str(query)
        .decode_utf8()
        .map_err(|_| admin_error(S3ErrorCode::InvalidArgument, "invalid query encoding"))?;
    serde_urlencoded::from_str(query).map_err(|_| admin_error(S3ErrorCode::InvalidArgument, "invalid delete residue query"))
}

fn validate_bucket(bucket: &str) -> S3Result<()> {
    if bucket.contains('/') || is_reserved_or_invalid_bucket(bucket, false) {
        return Err(admin_error(S3ErrorCode::InvalidArgument, "invalid bucket name"));
    }
    Ok(())
}

fn validate_target(bucket: &str, object: &str) -> S3Result<()> {
    validate_bucket(bucket)?;
    if object.is_empty() || object.len() > 1024 || object.starts_with('/') || !is_valid_object_prefix(object) {
        return Err(admin_error(S3ErrorCode::InvalidArgument, "invalid exact object key"));
    }
    Ok(())
}

fn response(result: &impl Serialize) -> S3Result<S3Response<(StatusCode, Body)>> {
    let bytes = serde_json::to_vec(result)
        .map_err(|_| admin_error(S3ErrorCode::InternalError, "failed to encode delete residue response"))?;
    Ok(json_response(StatusCode::OK, bytes))
}

enum ResidueOperation {
    List,
    Inspect,
    Discard,
}

#[async_trait::async_trait]
impl Operation for ResidueOperation {
    async fn call(&self, mut req: S3Request<Body>, _: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_heal_admin_request(&req).await?;
        let store = object_store_from_req(&req)
            .ok_or_else(|| admin_error(S3ErrorCode::ServiceUnavailable, "object store is not ready"))?;
        match self {
            Self::List => {
                let q: ListQuery = query(&req.uri)?;
                validate_bucket(&q.bucket)?;
                let page = store
                    .list_delete_residue_candidates(&q.bucket, q.cursor, q.limit.unwrap_or(DEFAULT_LIMIT))
                    .await
                    .map_err(ApiError::from)?;
                response(&page)
            }
            Self::Inspect => {
                let q: Target = query(&req.uri)?;
                validate_target(&q.bucket, &q.object)?;
                let preview = store
                    .inspect_delete_residue(&q.bucket, &q.object, q.pool, q.set)
                    .await
                    .map_err(ApiError::from)?;
                response(&preview)
            }
            Self::Discard => {
                if req.uri.query().is_some() {
                    return Err(admin_error(S3ErrorCode::InvalidArgument, "discard accepts only a JSON body"));
                }
                let body =
                    req.input.store_all_limited(16 * 1024).await.map_err(|_| {
                        admin_error(S3ErrorCode::InvalidRequest, "delete residue request too large or unreadable")
                    })?;
                let q: DiscardRequest = serde_json::from_slice(&body)
                    .map_err(|_| admin_error(S3ErrorCode::InvalidRequest, "invalid delete residue discard request"))?;
                validate_target(&q.bucket, &q.object)?;
                if q.expected_fingerprint.len() != 64
                    || !q
                        .expected_fingerprint
                        .bytes()
                        .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
                {
                    return Err(admin_error(S3ErrorCode::InvalidArgument, "invalid inspection fingerprint"));
                }
                let result = store
                    .discard_delete_residue(&q.bucket, &q.object, q.pool, q.set, &q.expected_fingerprint)
                    .await
                    .map_err(ApiError::from)?;
                response(&result)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delete_residue_requires_an_exact_unambiguous_target() {
        let uri = "/?bucket=bucket&object=a%2Fb%252Fc%2Bd&pool=0&set=3".parse().unwrap();
        let q: Target = query(&uri).unwrap();
        assert_eq!((q.object.as_str(), q.pool, q.set), ("a/b%2Fc+d", 0, 3));
        assert!(query::<Target>(&"/?bucket=bucket&object=%ff&pool=0&set=0".parse().unwrap()).is_err());
        assert!(query::<ListQuery>(&"/?bucket=one&bucket=two".parse().unwrap()).is_err());
        assert!(query::<ListQuery>(&"/?bucket=one&remove=true".parse().unwrap()).is_err());
        assert!(validate_target("bucket", "../elsewhere").is_err());
        assert!(validate_target("bucket/", "a").is_err());
        assert!(validate_target(".rustfs.sys", "a").is_err());
        assert!(validate_target("bucket", "").is_err());
        assert!(serde_json::from_str::<DiscardRequest>(r#"{"bucket":"bucket","object":"a","pool":0,"set":0}"#).is_err());
    }
}
