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

//! Prefix-level bucket usage admin handler (rustfs/backlog#1872).
//!
//! `GET /rustfs/admin/v3/usage/{bucket}?prefix=&max-entries=` answers
//! "what does this bucket / this prefix hold" from the scanner's per-set
//! usage caches, with a one-level sub-prefix breakdown — the data console
//! buckets view MinIO serves from `loadPrefixUsageFromBackend`.

use crate::admin::auth::authorize_admin_request;
use crate::admin::handlers::system::data_usage_info_gate_actions;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::server::ADMIN_PREFIX;
use http::{HeaderMap, HeaderValue, StatusCode};
use hyper::Method;
use matchit::Params;
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};

const JSON_CONTENT_TYPE: &str = "application/json";
const DEFAULT_MAX_ENTRIES: usize = 1000;
const MAX_ENTRIES_LIMIT: usize = 10_000;

pub struct BucketPrefixUsageHandler {}

pub fn register_usage_prefix_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/usage/{bucket}").as_str(),
        AdminOperation(&BucketPrefixUsageHandler {}),
    )?;
    Ok(())
}

/// Parse `prefix` and `max-entries` from the query string. Unknown keys are
/// rejected so a typo'd parameter cannot silently change the answer's shape.
fn parse_usage_prefix_query(query: Option<&str>) -> S3Result<(String, usize)> {
    let mut prefix: Option<String> = None;
    let mut max_entries: Option<usize> = None;
    for (key, value) in url::form_urlencoded::parse(query.unwrap_or_default().as_bytes()) {
        match key.as_ref() {
            "prefix" => prefix = Some(value.into_owned()),
            "max-entries" => {
                max_entries = Some(
                    value
                        .parse::<usize>()
                        .map_err(|_| s3_error!(InvalidArgument, "max-entries must be a positive integer"))?,
                );
            }
            other => return Err(s3_error!(InvalidArgument, "unknown query parameter: {other}")),
        }
    }
    let max_entries = max_entries.unwrap_or(DEFAULT_MAX_ENTRIES).clamp(1, MAX_ENTRIES_LIMIT);
    Ok((prefix.unwrap_or_default(), max_entries))
}

#[async_trait::async_trait]
impl Operation for BucketPrefixUsageHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        // The shared gate reports the same `InvalidRequest` "get cred failed" this
        // handler has always returned for a credential-less request, so it needs no
        // message-preserving pre-check.
        authorize_admin_request(&req, data_usage_info_gate_actions()).await?;

        let bucket = params.get("bucket").unwrap_or_default().to_string();
        if bucket.is_empty() {
            return Err(s3_error!(InvalidRequest, "bucket path parameter is required"));
        }
        let (prefix, max_entries) = parse_usage_prefix_query(req.uri.query())?;

        // Authorization is bucket-scoped by the same any-of gate as the
        // datausageinfo route; the bucket name itself is validated by the
        // scanner layer, which rejects reserved/invalid names.
        let response = rustfs_scanner::bucket_prefix_usage(&bucket, &prefix, max_entries)
            .await
            .map_err(|err| s3_error!(InvalidArgument, "{}", err))?;

        let data = serde_json::to_vec(&response)
            .map_err(|_| S3Error::with_message(S3ErrorCode::InternalError, "parse prefix usage failed"))?;
        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, HeaderValue::from_static(JSON_CONTENT_TYPE));

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}

#[cfg(test)]
mod tests {
    use super::{BucketPrefixUsageHandler, DEFAULT_MAX_ENTRIES, MAX_ENTRIES_LIMIT, parse_usage_prefix_query};
    use crate::admin::router::Operation;
    use s3s::S3Error;

    fn query(raw: &str) -> Result<(String, usize), S3Error> {
        parse_usage_prefix_query(Some(raw))
    }

    /// This endpoint authorizes through the shared admin gate, whose
    /// credential-less rejection is the same `InvalidRequest` "get cred failed"
    /// the handler returned inline before (rustfs/backlog#1829), so no
    /// message-preserving pre-check is needed here.
    #[tokio::test]
    async fn prefix_usage_handler_keeps_its_missing_credentials_message() {
        let req = s3s::S3Request {
            input: s3s::Body::from(String::new()),
            method: http::Method::GET,
            uri: http::Uri::from_static("/rustfs/admin/v3/usage/bucket"),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };

        let err = BucketPrefixUsageHandler {}
            .call(req, matchit::Params::new())
            .await
            .expect_err("a request without credentials must be rejected");
        assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidRequest);
        assert_eq!(err.message(), Some("get cred failed"));
    }

    #[test]
    fn defaults_apply_when_no_query_is_given() {
        assert_eq!(parse_usage_prefix_query(None).unwrap(), (String::new(), DEFAULT_MAX_ENTRIES));
        assert_eq!(query("").unwrap(), (String::new(), DEFAULT_MAX_ENTRIES));
    }

    #[test]
    fn prefix_round_trips_url_encoded_characters() {
        let (prefix, _) = query("prefix=pre%2Ffix%20name").unwrap();
        assert_eq!(prefix, "pre/fix name");
    }

    #[test]
    fn max_entries_parses_and_clamps_to_documented_bounds() {
        assert_eq!(query("max-entries=5").unwrap().1, 5);
        assert_eq!(query("max-entries=0").unwrap().1, 1, "zero must clamp up, not mean unlimited");
        assert_eq!(query("max-entries=99999999").unwrap().1, MAX_ENTRIES_LIMIT);
        assert!(query("max-entries=-3").is_err());
        assert!(query("max-entries=abc").is_err());
    }

    #[test]
    fn unknown_parameters_are_rejected_not_ignored() {
        assert!(
            query("prefixes=x").is_err(),
            "a typo'd parameter must fail the request, not widen the query"
        );
    }
}
