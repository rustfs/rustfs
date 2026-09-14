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

//! Pre-migration key inventory for the switch to the RustFS S3 gateway (rustfs/gateway#754).
//!
//! The gateway refuses some object keys on every operation that names one, reads and deletes
//! included, so an object stored under such a key today would be unreachable through the gateway
//! after the switch. This read-only endpoint lists every stored key the gateway would refuse, so
//! an operator can copy those objects to a safe key through the current stack before switching.
//!
//! The rules mirror the gateway's single key normalisation (`rustfs/gateway`,
//! `crates/types/src/scalar/naming.rs`: the residual-encoding check, then `floor_check_key`), for a
//! client that percent-encodes the stored key exactly once. A `.` or `..` segment and a `//` run are
//! absent from the report because this store refuses them on every write
//! (`is_valid_object_prefix`), so no stored key can carry one.

use crate::admin::auth::authorize_admin_request;
use crate::admin::handlers::admin_json_response;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::runtime_sources::current_object_store_handle;
use crate::admin::storage_api::contract::bucket::{BucketOperations as _, BucketOptions};
use crate::admin::storage_api::contract::list::ListOperations as _;
use crate::admin::storage_api::s3::{self, Body, S3ErrorCode, S3Request, S3Response, S3Result};
use crate::error::ApiError;
use crate::server::ADMIN_PREFIX;
use http::StatusCode;
use hyper::Method;
use matchit::Params;
use rustfs_policy::policy::action::{Action, AdminAction};
use serde::Serialize;
use std::collections::BTreeMap;

pub const GATEWAY_KEY_INVENTORY_ROUTE_SUFFIX: &str = "/v3/gateway-key-inventory";

/// The gateway's key length limit, in UTF-8 bytes.
const GATEWAY_MAX_KEY_BYTES: usize = 1024;
const LIST_PAGE_KEYS: i32 = 1000;
const DEFAULT_MAX_FINDINGS: usize = 1000;
const MAX_FINDINGS_LIMIT: usize = 10_000;

/// Why the gateway would refuse a stored key. One variant per gateway rule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GatewayKeyRefusal {
    /// A literal `%2F`, `%5C` or `%2E%2E`, which the gateway refuses after its single decode.
    EncodedSeparator,
    /// An empty key.
    Empty,
    /// Longer than 1024 UTF-8 bytes.
    TooLong,
    /// A NUL byte.
    Nul,
    /// A C0 control, DEL or C1 control.
    ControlCharacter,
    /// A leading `//` or `\`, or a drive root such as `C:/` or `c:\`.
    AbsoluteOrUnc,
    /// A `..` segment delimited by `/` or `\`.
    TraversalSegment,
}

impl GatewayKeyRefusal {
    /// Stable report label.
    pub const fn slug(self) -> &'static str {
        match self {
            Self::EncodedSeparator => "encoded_separator",
            Self::Empty => "empty",
            Self::TooLong => "too_long",
            Self::Nul => "nul",
            Self::ControlCharacter => "control_character",
            Self::AbsoluteOrUnc => "absolute_or_unc",
            Self::TraversalSegment => "traversal_segment",
        }
    }
}

/// The rule under which the gateway would refuse `key`, first rule first, or `None` when the
/// gateway reaches it.
pub fn gateway_key_refusal(key: &str) -> Option<GatewayKeyRefusal> {
    let lower = key.to_ascii_lowercase();
    if lower.contains("%2f") || lower.contains("%5c") || lower.contains("%2e%2e") {
        return Some(GatewayKeyRefusal::EncodedSeparator);
    }
    if key.is_empty() {
        return Some(GatewayKeyRefusal::Empty);
    }
    if key.len() > GATEWAY_MAX_KEY_BYTES {
        return Some(GatewayKeyRefusal::TooLong);
    }
    if key.contains('\0') {
        return Some(GatewayKeyRefusal::Nul);
    }
    if key.chars().any(char::is_control) {
        return Some(GatewayKeyRefusal::ControlCharacter);
    }
    if key.starts_with("//") || key.starts_with('\\') || is_drive_rooted(key) {
        return Some(GatewayKeyRefusal::AbsoluteOrUnc);
    }
    if key.split(['/', '\\']).any(|segment| segment == "..") {
        return Some(GatewayKeyRefusal::TraversalSegment);
    }
    None
}

fn is_drive_rooted(key: &str) -> bool {
    let bytes = key.as_bytes();
    bytes.len() >= 3 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':' && (bytes[2] == b'/' || bytes[2] == b'\\')
}

#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct GatewayKeyFinding {
    pub bucket: String,
    pub key: String,
    pub rule: &'static str,
}

#[derive(Debug, Serialize)]
pub struct GatewayKeyInventoryReport {
    pub buckets_scanned: usize,
    /// Distinct keys seen; the versions of one key count once.
    pub keys_scanned: u64,
    pub refused_keys: u64,
    pub by_rule: BTreeMap<&'static str, u64>,
    pub findings: Vec<GatewayKeyFinding>,
    /// True when more keys are refused than `findings` lists; the counts stay complete.
    pub findings_truncated: bool,
    #[serde(skip)]
    max_findings: usize,
    #[serde(skip)]
    last: Option<(String, String)>,
}

impl GatewayKeyInventoryReport {
    pub fn new(max_findings: usize) -> Self {
        Self {
            buckets_scanned: 0,
            keys_scanned: 0,
            refused_keys: 0,
            by_rule: BTreeMap::new(),
            findings: Vec::new(),
            findings_truncated: false,
            max_findings,
            last: None,
        }
    }

    pub fn begin_bucket(&mut self) {
        self.buckets_scanned += 1;
        self.last = None;
    }

    /// Records one listed entry. A version listing returns every version of a key back to back,
    /// so an entry repeating the previous key is the same key and is not counted again.
    pub fn record(&mut self, bucket: &str, key: &str) {
        if self.last.as_ref().is_some_and(|(b, k)| b == bucket && k == key) {
            return;
        }
        self.last = Some((bucket.to_owned(), key.to_owned()));
        self.keys_scanned += 1;
        let Some(rule) = gateway_key_refusal(key) else {
            return;
        };
        self.refused_keys += 1;
        *self.by_rule.entry(rule.slug()).or_default() += 1;
        if self.findings.len() < self.max_findings {
            self.findings.push(GatewayKeyFinding {
                bucket: bucket.to_owned(),
                key: key.to_owned(),
                rule: rule.slug(),
            });
        } else {
            self.findings_truncated = true;
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct InventoryQuery {
    bucket: Option<String>,
    max_findings: usize,
}

fn parse_inventory_query(query: Option<&str>) -> S3Result<InventoryQuery> {
    let mut bucket = None;
    let mut max_findings = None;
    for (key, value) in url::form_urlencoded::parse(query.unwrap_or_default().as_bytes()) {
        match key.as_ref() {
            "bucket" => {
                crate::storage::ecstore_bucket::utils::check_valid_bucket_name_strict(&value)
                    .map_err(|_| s3::error(S3ErrorCode::InvalidArgument, "invalid bucket name"))?;
                bucket = Some(value.into_owned());
            }
            "max-findings" => {
                max_findings = Some(
                    value
                        .parse::<usize>()
                        .map_err(|_| s3::error(S3ErrorCode::InvalidArgument, "max-findings must be a positive integer"))?,
                );
            }
            other => return Err(s3::error(S3ErrorCode::InvalidArgument, format!("unknown query parameter: {other}"))),
        }
    }
    Ok(InventoryQuery {
        bucket,
        max_findings: max_findings.unwrap_or(DEFAULT_MAX_FINDINGS).clamp(1, MAX_FINDINGS_LIMIT),
    })
}

pub fn register_gateway_key_inventory_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::GET,
        format!("{ADMIN_PREFIX}{GATEWAY_KEY_INVENTORY_ROUTE_SUFFIX}").as_str(),
        AdminOperation(&GatewayKeyInventoryHandler {}),
    )?;
    Ok(())
}

pub struct GatewayKeyInventoryHandler {}

#[async_trait::async_trait]
impl Operation for GatewayKeyInventoryHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let cred = authorize_admin_request(&req, vec![Action::AdminAction(AdminAction::InspectDataAction)]).await?;
        let query = parse_inventory_query(req.uri.query())?;
        let store =
            current_object_store_handle().ok_or_else(|| s3::error(S3ErrorCode::InternalError, "object store not initialized"))?;

        let buckets = match query.bucket {
            Some(bucket) => vec![bucket],
            None => store
                .list_bucket(&BucketOptions::default())
                .await
                .map_err(ApiError::from)?
                .into_iter()
                .map(|bucket| bucket.name)
                .collect(),
        };

        let mut report = GatewayKeyInventoryReport::new(query.max_findings);
        for bucket in buckets {
            report.begin_bucket();
            let (mut marker, mut version_marker) = (None, None);
            loop {
                let page = store
                    .clone()
                    .list_object_versions(&bucket, "", marker, version_marker, None, LIST_PAGE_KEYS)
                    .await
                    .map_err(ApiError::from)?;
                for object in &page.objects {
                    report.record(&bucket, &object.name);
                }
                if !page.is_truncated || (page.next_marker.is_none() && page.next_version_idmarker.is_none()) {
                    break;
                }
                marker = page.next_marker;
                version_marker = page.next_version_idmarker;
            }
        }

        admin_json_response(req.uri.path(), &cred.secret_key, StatusCode::OK, &report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderMap;

    #[test]
    fn keys_the_gateway_reaches_are_not_reported() {
        let long = "k".repeat(GATEWAY_MAX_KEY_BYTES);
        for key in [
            "photos/2026/a.jpg",
            "a/./b",
            "a//b",
            "/leading",
            "dir/",
            "100%done",
            "%25",
            "a\\b",
            "C:x",
            "...",
            "a..b",
            "é/ü",
            long.as_str(),
        ] {
            assert_eq!(gateway_key_refusal(key), None, "{key:?}");
        }
    }

    #[test]
    fn each_gateway_rule_is_reported_by_name() {
        let too_long = "k".repeat(GATEWAY_MAX_KEY_BYTES + 1);
        let cases = [
            ("a%2Fb", GatewayKeyRefusal::EncodedSeparator),
            ("a%5cb", GatewayKeyRefusal::EncodedSeparator),
            ("%2E%2e/x", GatewayKeyRefusal::EncodedSeparator),
            ("", GatewayKeyRefusal::Empty),
            (too_long.as_str(), GatewayKeyRefusal::TooLong),
            ("a\0b", GatewayKeyRefusal::Nul),
            ("a\tb", GatewayKeyRefusal::ControlCharacter),
            ("a\u{7f}", GatewayKeyRefusal::ControlCharacter),
            ("a\u{85}b", GatewayKeyRefusal::ControlCharacter),
            ("//server/share", GatewayKeyRefusal::AbsoluteOrUnc),
            ("\\lead", GatewayKeyRefusal::AbsoluteOrUnc),
            ("C:/x", GatewayKeyRefusal::AbsoluteOrUnc),
            ("c:\\x", GatewayKeyRefusal::AbsoluteOrUnc),
            ("..", GatewayKeyRefusal::TraversalSegment),
            ("a/../b", GatewayKeyRefusal::TraversalSegment),
            ("a\\..\\b", GatewayKeyRefusal::TraversalSegment),
            ("a/..", GatewayKeyRefusal::TraversalSegment),
        ];
        for (key, rule) in cases {
            assert_eq!(gateway_key_refusal(key), Some(rule), "{key:?}");
        }
    }

    #[test]
    fn versions_of_one_key_count_once_and_counts_outlive_the_findings_cap() {
        let mut report = GatewayKeyInventoryReport::new(1);
        report.begin_bucket();
        for key in ["ok", "a\tb", "a\tb", "C:/x", "fine"] {
            report.record("b1", key);
        }
        report.begin_bucket();
        report.record("b2", "a\tb");

        assert_eq!(report.buckets_scanned, 2);
        assert_eq!(report.keys_scanned, 5);
        assert_eq!(report.refused_keys, 3);
        assert_eq!(report.by_rule.get("control_character"), Some(&2));
        assert_eq!(report.by_rule.get("absolute_or_unc"), Some(&1));
        assert_eq!(
            report.findings,
            [GatewayKeyFinding {
                bucket: "b1".to_owned(),
                key: "a\tb".to_owned(),
                rule: "control_character",
            }]
        );
        assert!(report.findings_truncated);

        let encoded = serde_json::to_value(&report).expect("report serializes");
        assert_eq!(encoded["by_rule"]["control_character"], 2);
        assert_eq!(encoded["findings"][0]["key"], "a\tb");
        assert!(encoded.get("max_findings").is_none());
    }

    #[test]
    fn query_defaults_clamps_and_refuses_unknown_parameters() {
        assert_eq!(
            parse_inventory_query(None).expect("empty query"),
            InventoryQuery {
                bucket: None,
                max_findings: DEFAULT_MAX_FINDINGS,
            }
        );
        let parsed = parse_inventory_query(Some("bucket=photos&max-findings=0")).expect("valid query");
        assert_eq!(parsed.bucket.as_deref(), Some("photos"));
        assert_eq!(parsed.max_findings, 1, "zero must clamp up, not mean unlimited");
        assert_eq!(
            parse_inventory_query(Some("max-findings=99999999"))
                .expect("valid")
                .max_findings,
            MAX_FINDINGS_LIMIT
        );
        assert!(parse_inventory_query(Some("max-findings=-1")).is_err());
        assert!(parse_inventory_query(Some("bucket=Bad_Name")).is_err());
        assert!(parse_inventory_query(Some("prefix=x")).is_err());
    }

    #[test]
    fn query_errors_keep_their_s3_code_status_and_message() {
        let cases = [
            ("bucket=Bad_Name", "invalid bucket name"),
            ("max-findings=-1", "max-findings must be a positive integer"),
            ("prefix=x", "unknown query parameter: prefix"),
        ];
        for (query, message) in cases {
            let err = parse_inventory_query(Some(query)).expect_err(query);
            assert_eq!(err.code(), &S3ErrorCode::InvalidArgument, "{query}");
            assert_eq!(err.status_code(), Some(StatusCode::BAD_REQUEST), "{query}");
            assert_eq!(err.message(), Some(message), "{query}");
        }

        let err = s3::error(S3ErrorCode::InternalError, "object store not initialized");
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
        assert_eq!(err.status_code(), Some(StatusCode::INTERNAL_SERVER_ERROR));
        assert_eq!(err.message(), Some("object store not initialized"));
    }

    #[tokio::test]
    async fn a_request_without_credentials_is_refused_before_any_listing() {
        let req = S3Request {
            input: Body::from(String::new()),
            method: Method::GET,
            uri: http::Uri::from_static("/rustfs/admin/v3/gateway-key-inventory"),
            headers: HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };

        let err = GatewayKeyInventoryHandler {}
            .call(req, Params::new())
            .await
            .expect_err("a request without credentials must be refused");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
    }
}
