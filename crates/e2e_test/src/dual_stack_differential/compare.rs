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

//! What one stack answered, and how two answers are compared.
//!
//! An [`Observation`] keeps only what a client acts on: the HTTP status, the S3 error code, the
//! allowlisted response headers and a few typed fields decoded from the response. The error
//! message is kept for diagnostics and never compared: message wording is judged by the wire
//! corpus, not here. The comparator is pure so it can be tested without a server.

use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// Response headers compared exactly (after [`normalized_header_value`]).
const HEADER_ALLOWLIST: [&str; 7] = [
    "content-type",
    "x-amz-version-id",
    "etag",
    "x-amz-delete-marker",
    "content-range",
    "accept-ranges",
    "x-amz-request-charged",
];

/// Every response header with this prefix is compared as well.
const HEADER_ALLOWLIST_PREFIX: &str = "x-amz-server-side-encryption";

/// Placeholder for a field one side did not send.
const ABSENT: &str = "<absent>";

/// The lowercase header name if the comparator looks at this header.
pub(crate) fn compared_header(name: &str) -> Option<String> {
    let name = name.to_ascii_lowercase();
    (HEADER_ALLOWLIST.contains(&name.as_str()) || name.starts_with(HEADER_ALLOWLIST_PREFIX)).then_some(name)
}

/// A version ID is minted per process, so its value never matches across two servers; only
/// whether it is the literal `null` (an unversioned write) carries meaning.
fn normalized_header_value(name: &str, value: &str) -> String {
    if name == "x-amz-version-id" && value != "null" {
        "<version-id>".to_owned()
    } else {
        value.to_owned()
    }
}

/// One request's outcome as a client sees it.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct Observation {
    /// The S3 operation, named as in the `s3s::S3` trait (`GetObject`).
    pub op: &'static str,
    /// Which request of the script this was (`normal`, `negative`, or a setup label).
    pub case: &'static str,
    /// HTTP status; 0 when no response arrived.
    pub status: u16,
    /// The S3 error code, or the SDK's error kind when there was no service error.
    pub error_code: Option<String>,
    /// Allowlisted headers only, lowercase names, normalized values.
    pub headers: BTreeMap<String, String>,
    /// Typed values decoded from the response body, never free text.
    pub fields: BTreeMap<String, String>,
    /// Diagnostic only; never compared.
    #[serde(skip)]
    pub message: Option<String>,
}

impl Observation {
    /// Builds an observation, keeping only the allowlisted headers.
    pub(crate) fn new<'h>(
        op: &'static str,
        case: &'static str,
        status: u16,
        error_code: Option<String>,
        headers: impl IntoIterator<Item = (&'h str, &'h str)>,
    ) -> Self {
        let headers = headers
            .into_iter()
            .filter_map(|(name, value)| compared_header(name).map(|name| (name, value)))
            .map(|(name, value)| {
                let value = normalized_header_value(&name, value);
                (name, value)
            })
            .collect();
        Self {
            op,
            case,
            status,
            error_code,
            headers,
            fields: BTreeMap::new(),
            message: None,
        }
    }

    pub(crate) fn with_field(mut self, name: &str, value: impl Into<String>) -> Self {
        self.fields.insert(name.to_owned(), value.into());
        self
    }

    pub(crate) fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }
}

/// One field on which the two stacks answered differently.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct Difference {
    pub op: String,
    pub case: String,
    pub field: String,
    pub a: String,
    pub b: String,
}

/// Every difference between two transcripts of the same script, matched by `(op, case)`.
pub(crate) fn compare(a: &[Observation], b: &[Observation]) -> Vec<Difference> {
    let mut differences = Vec::new();
    let (index_a, duplicates_a) = index(a);
    let (index_b, duplicates_b) = index(b);
    for (op, case) in duplicates_a.union(&duplicates_b) {
        let count = |side: &[Observation]| {
            side.iter()
                .filter(|observation| (observation.op, observation.case) == (*op, *case))
                .count()
                .to_string()
        };
        differences.push(difference(op, case, "duplicate_case", count(a), count(b)));
    }
    let keys: BTreeSet<_> = index_a.keys().chain(index_b.keys()).copied().collect();
    for key @ (op, case) in keys {
        match (index_a.get(&key), index_b.get(&key)) {
            (Some(a), Some(b)) => compare_one(a, b, &mut differences),
            (a, b) => {
                let presence = |side: Option<&&Observation>| if side.is_some() { "present" } else { ABSENT }.to_owned();
                differences.push(difference(op, case, "observation", presence(a), presence(b)));
            }
        }
    }
    differences
}

type Key = (&'static str, &'static str);

fn index(side: &[Observation]) -> (BTreeMap<Key, &Observation>, BTreeSet<Key>) {
    let mut index = BTreeMap::new();
    let mut duplicates = BTreeSet::new();
    for observation in side {
        let key = (observation.op, observation.case);
        if index.insert(key, observation).is_some() {
            duplicates.insert(key);
        }
    }
    (index, duplicates)
}

fn difference(op: &str, case: &str, field: &str, a: String, b: String) -> Difference {
    Difference {
        op: op.to_owned(),
        case: case.to_owned(),
        field: field.to_owned(),
        a,
        b,
    }
}

fn compare_one(a: &Observation, b: &Observation, differences: &mut Vec<Difference>) {
    let mut field = |name: String, value_a: Option<&str>, value_b: Option<&str>| {
        if value_a != value_b {
            let show = |value: Option<&str>| value.unwrap_or(ABSENT).to_owned();
            differences.push(difference(a.op, a.case, &name, show(value_a), show(value_b)));
        }
    };
    field(
        "status".to_owned(),
        Some(a.status.to_string().as_str()),
        Some(b.status.to_string().as_str()),
    );
    field("error_code".to_owned(), a.error_code.as_deref(), b.error_code.as_deref());
    let headers: BTreeSet<_> = a.headers.keys().chain(b.headers.keys()).collect();
    for name in headers {
        field(
            format!("header.{name}"),
            a.headers.get(name).map(String::as_str),
            b.headers.get(name).map(String::as_str),
        );
    }
    let fields: BTreeSet<_> = a.fields.keys().chain(b.fields.keys()).collect();
    for name in fields {
        field(
            format!("body.{name}"),
            a.fields.get(name).map(String::as_str),
            b.fields.get(name).map(String::as_str),
        );
    }
}

/// Operations observed on both sides.
pub(crate) fn compared_ops(a: &[Observation], b: &[Observation]) -> BTreeSet<&'static str> {
    let ops_b: BTreeSet<_> = b.iter().map(|observation| observation.op).collect();
    a.iter()
        .map(|observation| observation.op)
        .filter(|op| ops_b.contains(op))
        .collect()
}

/// `(op, case)` pairs observed on both sides.
pub(crate) fn compared_cases(a: &[Observation], b: &[Observation]) -> usize {
    let cases_b: BTreeSet<_> = b.iter().map(|observation| (observation.op, observation.case)).collect();
    a.iter()
        .map(|observation| (observation.op, observation.case))
        .collect::<BTreeSet<_>>()
        .intersection(&cases_b)
        .count()
}

/// The JSON report written for every run.
#[derive(Debug, Serialize)]
pub(crate) struct Report {
    pub schema: u32,
    pub run: String,
    pub stack_a: String,
    pub stack_b: String,
    pub region_a: Option<String>,
    pub region_b: Option<String>,
    pub compared_ops: usize,
    pub compared_cases: usize,
    pub differences: Vec<Difference>,
}

impl Report {
    /// Writes the report as `<directory>/<run>.json` and returns its path.
    pub(crate) fn write_to(&self, directory: &Path) -> std::io::Result<PathBuf> {
        std::fs::create_dir_all(directory)?;
        let path = directory.join(format!("{}.json", self.run));
        let json = serde_json::to_vec_pretty(self).map_err(std::io::Error::other)?;
        std::fs::write(&path, json)?;
        Ok(path)
    }
}

/// One line per difference, for a failing assertion.
pub(crate) fn render(differences: &[Difference]) -> String {
    differences
        .iter()
        .map(|d| format!("({}, {}, {}, a={:?}, b={:?})", d.op, d.case, d.field, d.a, d.b))
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(op: &'static str) -> Observation {
        Observation::new(op, "normal", 200, None, [("content-type", "application/xml")])
    }

    fn fields(differences: &[Difference]) -> Vec<&str> {
        differences.iter().map(|d| d.field.as_str()).collect()
    }

    #[test]
    fn identical_transcripts_have_no_difference() {
        let a = vec![
            ok("HeadBucket"),
            ok("GetBucketLocation").with_field("location_constraint", "us-east-1"),
        ];
        let b = a.clone();
        assert_eq!(compare(&a, &b), Vec::new());
        assert_eq!(compared_ops(&a, &b).len(), 2);
    }

    #[test]
    fn a_status_difference_is_reported() {
        let a = vec![ok("PutBucketAcl")];
        let b = vec![Observation::new(
            "PutBucketAcl",
            "normal",
            403,
            None,
            [("content-type", "application/xml")],
        )];
        assert_eq!(
            compare(&a, &b),
            vec![Difference {
                op: "PutBucketAcl".into(),
                case: "normal".into(),
                field: "status".into(),
                a: "200".into(),
                b: "403".into(),
            }]
        );
    }

    #[test]
    fn an_error_code_difference_is_reported() {
        let a = vec![Observation::new("GetObject", "negative", 404, Some("NoSuchKey".into()), [])];
        let b = vec![Observation::new(
            "GetObject",
            "negative",
            404,
            Some("NoSuchBucket".into()),
            [],
        )];
        let differences = compare(&a, &b);
        assert_eq!(fields(&differences), ["error_code"]);
        assert_eq!((differences[0].a.as_str(), differences[0].b.as_str()), ("NoSuchKey", "NoSuchBucket"));
    }

    #[test]
    fn an_error_code_on_one_side_only_is_reported() {
        let a = vec![Observation::new("HeadBucket", "negative", 404, Some("NotFound".into()), [])];
        let b = vec![Observation::new("HeadBucket", "negative", 404, None, [])];
        let differences = compare(&a, &b);
        assert_eq!(fields(&differences), ["error_code"]);
        assert_eq!(differences[0].b, ABSENT);
    }

    #[test]
    fn an_allowlisted_header_difference_is_reported() {
        let a = vec![Observation::new("HeadObject", "normal", 200, None, [("ETag", "\"aa\"")])];
        let b = vec![Observation::new("HeadObject", "normal", 200, None, [("etag", "\"bb\"")])];
        assert_eq!(fields(&compare(&a, &b)), ["header.etag"]);
    }

    #[test]
    fn a_header_sent_by_one_side_only_is_reported() {
        let a = vec![Observation::new(
            "DeleteObject",
            "normal",
            204,
            None,
            [("x-amz-delete-marker", "true")],
        )];
        let b = vec![Observation::new("DeleteObject", "normal", 204, None, [])];
        let differences = compare(&a, &b);
        assert_eq!(fields(&differences), ["header.x-amz-delete-marker"]);
        assert_eq!((differences[0].a.as_str(), differences[0].b.as_str()), ("true", ABSENT));
    }

    #[test]
    fn every_server_side_encryption_header_is_compared() {
        let header = "x-amz-server-side-encryption-customer-key-md5";
        let a = vec![Observation::new("PutObject", "sse-c", 200, None, [(header, "one")])];
        let b = vec![Observation::new("PutObject", "sse-c", 200, None, [(header, "two")])];
        assert_eq!(fields(&compare(&a, &b)), [format!("header.{header}")]);
    }

    #[test]
    fn headers_outside_the_allowlist_are_not_compared() {
        let a = vec![Observation::new(
            "ListBuckets",
            "normal",
            200,
            None,
            [
                ("date", "Mon"),
                ("x-amz-request-id", "1"),
                ("server", "a"),
                ("content-length", "10"),
            ],
        )];
        let b = vec![Observation::new(
            "ListBuckets",
            "normal",
            200,
            None,
            [
                ("date", "Tue"),
                ("x-amz-request-id", "2"),
                ("server", "b"),
                ("content-length", "11"),
            ],
        )];
        assert_eq!(compare(&a, &b), Vec::new());
        assert!(a[0].headers.is_empty(), "non-allowlisted headers were kept: {:?}", a[0].headers);
    }

    #[test]
    fn distinct_version_ids_are_not_a_difference() {
        let a = vec![Observation::new(
            "PutObject",
            "normal",
            200,
            None,
            [("x-amz-version-id", "0b1c")],
        )];
        let b = vec![Observation::new(
            "PutObject",
            "normal",
            200,
            None,
            [("x-amz-version-id", "9f8e")],
        )];
        assert_eq!(compare(&a, &b), Vec::new());
    }

    #[test]
    fn a_null_version_id_against_a_real_one_is_reported() {
        let a = vec![Observation::new(
            "PutObject",
            "normal",
            200,
            None,
            [("x-amz-version-id", "null")],
        )];
        let b = vec![Observation::new(
            "PutObject",
            "normal",
            200,
            None,
            [("x-amz-version-id", "9f8e")],
        )];
        assert_eq!(fields(&compare(&a, &b)), ["header.x-amz-version-id"]);
    }

    #[test]
    fn a_body_field_difference_is_reported() {
        let a = vec![ok("GetBucketLocation").with_field("location_constraint", "us-east-1")];
        let b = vec![ok("GetBucketLocation").with_field("location_constraint", "eu-west-1")];
        let differences = compare(&a, &b);
        assert_eq!(fields(&differences), ["body.location_constraint"]);
        assert_eq!((differences[0].a.as_str(), differences[0].b.as_str()), ("us-east-1", "eu-west-1"));
    }

    #[test]
    fn a_body_field_on_one_side_only_is_reported() {
        let a = vec![ok("ListObjectsV2").with_field("keys", "[]")];
        let b = vec![ok("ListObjectsV2")];
        assert_eq!(fields(&compare(&a, &b)), ["body.keys"]);
    }

    #[test]
    fn error_messages_are_never_compared() {
        let mut a = Observation::new("GetObject", "negative", 404, Some("NoSuchKey".into()), []);
        let mut b = a.clone();
        a.message = Some("The specified key does not exist.".into());
        b.message = Some("Object not found".into());
        assert_eq!(compare(&[a], &[b]), Vec::new());
    }

    #[test]
    fn a_case_observed_on_one_side_only_is_reported() {
        let a = vec![ok("HeadBucket"), ok("ListBuckets")];
        let b = vec![ok("HeadBucket")];
        let differences = compare(&a, &b);
        assert_eq!(fields(&differences), ["observation"]);
        assert_eq!(differences[0].op, "ListBuckets");
        assert_eq!(compared_ops(&a, &b).into_iter().collect::<Vec<_>>(), ["HeadBucket"]);
    }

    #[test]
    fn a_case_recorded_twice_is_reported() {
        let a = vec![ok("HeadBucket"), ok("HeadBucket")];
        let b = vec![ok("HeadBucket")];
        assert_eq!(fields(&compare(&a, &b)), ["duplicate_case"]);
    }

    #[test]
    fn compared_ops_counts_operations_not_cases() {
        let negative = Observation::new("HeadBucket", "negative", 404, Some("NotFound".into()), []);
        let a = vec![ok("HeadBucket"), negative, ok("ListBuckets")];
        let b = a.clone();
        assert_eq!(compared_ops(&a, &b).len(), 2);
    }

    #[test]
    fn the_report_lists_each_difference_as_op_field_a_b() {
        let directory = std::env::temp_dir().join(format!("dual-stack-report-{}", uuid::Uuid::new_v4()));
        let report = Report {
            schema: 1,
            run: "unit".into(),
            stack_a: "legacy".into(),
            stack_b: "legacy".into(),
            region_a: None,
            region_b: Some("eu-west-1".into()),
            compared_ops: 1,
            compared_cases: 1,
            differences: compare(
                &[ok("GetBucketLocation").with_field("location_constraint", "us-east-1")],
                &[ok("GetBucketLocation").with_field("location_constraint", "eu-west-1")],
            ),
        };
        let path = report.write_to(&directory).expect("write the report");
        let json: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).expect("read the report")).expect("parse");
        let _ = std::fs::remove_dir_all(&directory);
        assert_eq!(path.file_name().and_then(|name| name.to_str()), Some("unit.json"));
        assert_eq!(json["compared_ops"], 1);
        assert_eq!(
            json["differences"],
            serde_json::json!([{
                "op": "GetBucketLocation",
                "case": "normal",
                "field": "body.location_constraint",
                "a": "us-east-1",
                "b": "eu-west-1",
            }])
        );
    }
}
