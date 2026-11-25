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
use sha2::{Digest, Sha256};
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
    /// Captured before SDK response interceptors can rewrite the response.
    #[serde(skip)]
    pub(super) wire_head: Option<WireHead>,
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
            .filter_map(|(name, value)| {
                let name = compared_header(name)?;
                let value = normalized_header_value(&name, value);
                Some((name, value))
            })
            .collect();
        Self {
            op,
            case,
            status,
            error_code,
            headers,
            wire_head: None,
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

/// Whether side A is the same binary under test or a pinned released reference binary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ComparisonMode {
    SameBinary,
    Reference,
}

/// A maintainer-signed behavior change between a released reference binary (side A) and the
/// current candidate binary (side B).
///
/// Exemptions apply only in [`ComparisonMode::Reference`] and must match the exact
/// `(op, case, field, a, b)` tuple with no other unruled difference on the same `(op, case)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct AcceptedChange {
    pub op: &'static str,
    pub case: &'static str,
    pub field: &'static str,
    pub reference_value: &'static str,
    pub candidate_value: &'static str,
    pub ruling: &'static str,
}

impl AcceptedChange {
    pub(crate) fn matches(&self, diff: &Difference) -> bool {
        diff.op == self.op
            && diff.case == self.case
            && diff.field == self.field
            && diff.a == self.reference_value
            && diff.b == self.candidate_value
    }
}

/// Maintainer-signed accepted differences for external reference-binary parity runs.
pub(crate) const ACCEPTED_REFERENCE_CHANGES: &[AcceptedChange] = &[
    // Signed off in rustfs/backlog#2684#issuecomment-7163283798:
    // Commit 8f81762a6 aligned PutBucketPolicy on malformed JSON with AWS S3 (`400 MalformedPolicy`)
    // instead of official 1.0.5's `400 InvalidArgument`.
    AcceptedChange {
        op: "PutBucketPolicy",
        case: "negative",
        field: "error_code",
        reference_value: "InvalidArgument",
        candidate_value: "MalformedPolicy",
        ruling: "rustfs/backlog#2684#issuecomment-7163283798 (commit 8f81762a6)",
    },
];

/// One difference explicitly exempted by an [`AcceptedChange`] ruling in reference mode.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct AcceptedDifference {
    #[serde(flatten)]
    pub difference: Difference,
    pub ruling: String,
}

/// Separates raw differences into unruled failures and maintainer-signed accepted changes.
///
/// Accepted changes are evaluated only in [`ComparisonMode::Reference`]. In
/// [`ComparisonMode::SameBinary`], every difference stays unruled even when rules are supplied.
/// Evaluation is also atomic per `(op, case)`: if any field of a case has an unruled difference
/// (for example, `status` also differs), no field on that case is accepted.
pub(crate) fn partition_differences(
    raw: Vec<Difference>,
    mode: ComparisonMode,
    accepted_changes: &[AcceptedChange],
) -> (Vec<Difference>, Vec<AcceptedDifference>) {
    if mode != ComparisonMode::Reference || accepted_changes.is_empty() {
        return (raw, Vec::new());
    }
    let matched: Vec<Option<&AcceptedChange>> = raw
        .iter()
        .map(|diff| accepted_changes.iter().find(|rule| rule.matches(diff)))
        .collect();
    let unruled_cases: BTreeSet<(&str, &str)> = raw
        .iter()
        .zip(&matched)
        .filter_map(|(diff, rule)| rule.is_none().then_some((diff.op.as_str(), diff.case.as_str())))
        .collect();
    let rulings: Vec<Option<&'static str>> = raw
        .iter()
        .zip(&matched)
        .map(|(diff, rule)| {
            rule.filter(|_| !unruled_cases.contains(&(diff.op.as_str(), diff.case.as_str())))
                .map(|rule| rule.ruling)
        })
        .collect();
    let mut unruled = Vec::new();
    let mut accepted = Vec::new();
    for (diff, ruling) in raw.into_iter().zip(rulings) {
        match ruling {
            Some(ruling) => accepted.push(AcceptedDifference {
                difference: diff,
                ruling: ruling.to_owned(),
            }),
            None => unruled.push(diff),
        }
    }
    (unruled, accepted)
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

/// Status and the digest of unique header names at the transport boundary. Header values are never retained.
#[derive(Clone, Debug, Serialize)]
pub(super) struct WireHead {
    status: u16,
    header_names_sha256: String,
}

impl WireHead {
    pub(super) fn new<'h>(status: u16, names: impl Iterator<Item = &'h str>) -> Self {
        let names: BTreeSet<_> = names.map(str::to_ascii_lowercase).collect();
        let mut digest = Sha256::new();
        for name in names {
            digest.update(name.as_bytes());
            digest.update(b"\n");
        }
        Self {
            status,
            header_names_sha256: hex_simd::encode_to_string(digest.finalize(), hex_simd::AsciiCase::Lower),
        }
    }
}

/// Evidence for one transmitted response, including cases where the two sides agreed.
#[derive(Debug, Serialize)]
pub(super) struct ResponseReceipt {
    op: &'static str,
    case: &'static str,
    #[serde(flatten)]
    head: WireHead,
}

pub(super) fn response_receipts(observations: &[Observation]) -> Vec<ResponseReceipt> {
    observations
        .iter()
        .filter_map(|o| {
            Some(ResponseReceipt {
                op: o.op,
                case: o.case,
                head: o.wire_head.clone()?,
            })
        })
        .collect()
}

#[derive(Debug, Serialize)]
pub(super) struct PairedReceipts {
    responses_a: Vec<ResponseReceipt>,
    responses_b: Vec<ResponseReceipt>,
}

impl PairedReceipts {
    pub(super) fn new(a: &[Observation], b: &[Observation]) -> Self {
        Self {
            responses_a: response_receipts(a),
            responses_b: response_receipts(b),
        }
    }
}

/// The JSON report written for every run.
#[derive(Debug, Serialize)]
pub(crate) struct Report {
    pub schema: u32,
    pub run: String,
    pub stack_a: String,
    pub stack_b: String,
    /// Which side is the pinned release binary: `"a"`, or `None` when both sides are the binary
    /// under test.
    pub reference_side: Option<&'static str>,
    /// `--version` output of the binary each side actually ran.
    pub version_a: String,
    pub version_b: String,
    pub region_a: Option<String>,
    pub region_b: Option<String>,
    pub compared_ops: usize,
    pub compared_cases: usize,
    pub differences: Vec<Difference>,
    pub accepted_differences: Vec<AcceptedDifference>,
    #[serde(flatten)]
    pub(super) responses: PairedReceipts,
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

/// One line per accepted difference, for run diagnostics.
pub(crate) fn render_accepted(differences: &[AcceptedDifference]) -> String {
    differences
        .iter()
        .map(|entry| {
            let d = &entry.difference;
            format!("({}, {}, {}, a={:?}, b={:?}, ruling={:?})", d.op, d.case, d.field, d.a, d.b, entry.ruling)
        })
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
        let mut a = [Observation::new(
            "GetBucketLocation",
            "normal",
            200,
            None,
            [("x-test-only", "private value"), ("Date", "clock a"), ("date", "duplicate")],
        )
        .with_field("location_constraint", "us-east-1")];
        let mut b = [
            Observation::new("GetBucketLocation", "normal", 200, None, [("DATE", "clock b"), ("X-TEST-ONLY", "other")])
                .with_field("location_constraint", "eu-west-1"),
        ];
        a[0].wire_head = Some(WireHead::new(200, ["x-test-only", "Date", "date"].into_iter()));
        b[0].wire_head = Some(WireHead::new(201, ["Date"].into_iter()));
        let report = Report {
            schema: 2,
            run: "unit".into(),
            stack_a: "legacy".into(),
            stack_b: "legacy".into(),
            reference_side: Some("a"),
            version_a: "rustfs 1.0.4".into(),
            version_b: "rustfs 1.1.0".into(),
            region_a: None,
            region_b: Some("eu-west-1".into()),
            compared_ops: 1,
            compared_cases: 1,
            differences: compare(&a, &b),
            accepted_differences: Vec::new(),
            responses: PairedReceipts::new(&a, &b),
        };
        let path = report.write_to(&directory).expect("write the report");
        let json: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).expect("read the report")).expect("parse");
        let _ = std::fs::remove_dir_all(&directory);
        assert_eq!(path.file_name().and_then(|name| name.to_str()), Some("unit.json"));
        let expected_receipt = serde_json::json!([{
            "op": "GetBucketLocation", "case": "normal", "status": 200,
            "header_names_sha256": "fc98ca59b6a14566b52845df9838e7fb48e1e162149d2b6967085ff0a6a20da3",
        }]);
        assert_eq!(json["responses_a"], expected_receipt);
        assert_eq!(
            json["responses_b"],
            serde_json::json!([{
                "op": "GetBucketLocation", "case": "normal", "status": 201,
                "header_names_sha256": "426e0b848d77d7b3d778187d006056ee406221ca567ce2844ca403dc834ff8f1"
            }])
        );
        assert!(response_receipts(&[ok("HeadBucket")]).is_empty());
        assert_eq!(json["compared_ops"], 1);
        assert_eq!(json["reference_side"], "a");
        assert_eq!(json["version_a"], "rustfs 1.0.4");
        assert_eq!(json["version_b"], "rustfs 1.1.0");
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
        assert_eq!(json["accepted_differences"], serde_json::json!([]));
    }

    #[test]
    fn accepted_reference_change_exempts_only_exact_put_bucket_policy_tuple() {
        let a = vec![Observation::new(
            "PutBucketPolicy",
            "negative",
            400,
            Some("InvalidArgument".into()),
            [("content-type", "application/xml")],
        )];
        let b = vec![Observation::new(
            "PutBucketPolicy",
            "negative",
            400,
            Some("MalformedPolicy".into()),
            [("content-type", "application/xml")],
        )];

        let (unruled, accepted) = partition_differences(compare(&a, &b), ComparisonMode::Reference, ACCEPTED_REFERENCE_CHANGES);
        assert!(unruled.is_empty(), "expected 0 unruled differences, got: {unruled:?}");
        assert_eq!(
            accepted,
            vec![AcceptedDifference {
                difference: Difference {
                    op: "PutBucketPolicy".into(),
                    case: "negative".into(),
                    field: "error_code".into(),
                    a: "InvalidArgument".into(),
                    b: "MalformedPolicy".into(),
                },
                ruling: "rustfs/backlog#2684#issuecomment-7163283798 (commit 8f81762a6)".into(),
            }]
        );

        let directory = std::env::temp_dir().join(format!("dual-stack-accepted-report-{}", uuid::Uuid::new_v4()));
        let report = Report {
            schema: 2,
            run: "accepted".into(),
            stack_a: "reference".into(),
            stack_b: "under-test".into(),
            reference_side: Some("a"),
            version_a: "rustfs 1.0.5".into(),
            version_b: "rustfs 1.0.6".into(),
            region_a: None,
            region_b: None,
            compared_ops: 1,
            compared_cases: 1,
            differences: unruled,
            accepted_differences: accepted,
            responses: PairedReceipts::new(&a, &b),
        };
        let path = report.write_to(&directory).expect("write accepted report");
        let json: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).expect("read accepted report")).expect("parse");
        let _ = std::fs::remove_dir_all(&directory);
        assert_eq!(json["differences"], serde_json::json!([]));
        assert_eq!(
            json["accepted_differences"],
            serde_json::json!([{
                "op": "PutBucketPolicy",
                "case": "negative",
                "field": "error_code",
                "a": "InvalidArgument",
                "b": "MalformedPolicy",
                "ruling": "rustfs/backlog#2684#issuecomment-7163283798 (commit 8f81762a6)",
            }])
        );
    }

    #[test]
    fn accepted_reference_changes_never_apply_in_same_binary_mode() {
        let a = vec![Observation::new(
            "PutBucketPolicy",
            "negative",
            400,
            Some("InvalidArgument".into()),
            [("content-type", "application/xml")],
        )];
        let b = vec![Observation::new(
            "PutBucketPolicy",
            "negative",
            400,
            Some("MalformedPolicy".into()),
            [("content-type", "application/xml")],
        )];

        let (unruled, accepted) = partition_differences(compare(&a, &b), ComparisonMode::SameBinary, ACCEPTED_REFERENCE_CHANGES);
        assert!(accepted.is_empty(), "same-binary mode must never accept differences: {accepted:?}");
        assert_eq!(
            unruled,
            vec![Difference {
                op: "PutBucketPolicy".into(),
                case: "negative".into(),
                field: "error_code".into(),
                a: "InvalidArgument".into(),
                b: "MalformedPolicy".into(),
            }]
        );
    }

    #[test]
    fn accepted_reference_change_fails_closed_on_any_mutation_or_extra_field_diff() {
        let cases: &[(&str, Observation, Observation, &[&str])] = &[
            (
                "wrong candidate error code",
                Observation::new("PutBucketPolicy", "negative", 400, Some("InvalidArgument".into()), []),
                Observation::new("PutBucketPolicy", "negative", 400, Some("AccessDenied".into()), []),
                &["error_code"],
            ),
            (
                "missing candidate error code",
                Observation::new("PutBucketPolicy", "negative", 400, Some("InvalidArgument".into()), []),
                Observation::new("PutBucketPolicy", "negative", 400, None, []),
                &["error_code"],
            ),
            (
                "wrong reference error code",
                Observation::new("PutBucketPolicy", "negative", 400, Some("MalformedXML".into()), []),
                Observation::new("PutBucketPolicy", "negative", 400, Some("MalformedPolicy".into()), []),
                &["error_code"],
            ),
            (
                "swapped reference and candidate error codes",
                Observation::new("PutBucketPolicy", "negative", 400, Some("MalformedPolicy".into()), []),
                Observation::new("PutBucketPolicy", "negative", 400, Some("InvalidArgument".into()), []),
                &["error_code"],
            ),
            (
                "different operation",
                Observation::new("GetBucketPolicy", "negative", 400, Some("InvalidArgument".into()), []),
                Observation::new("GetBucketPolicy", "negative", 400, Some("MalformedPolicy".into()), []),
                &["error_code"],
            ),
            (
                "different case",
                Observation::new("PutBucketPolicy", "normal", 400, Some("InvalidArgument".into()), []),
                Observation::new("PutBucketPolicy", "normal", 400, Some("MalformedPolicy".into()), []),
                &["error_code"],
            ),
            (
                "status differs alone",
                Observation::new("PutBucketPolicy", "negative", 400, Some("InvalidArgument".into()), []),
                Observation::new("PutBucketPolicy", "negative", 500, Some("InvalidArgument".into()), []),
                &["status"],
            ),
            (
                "status also differs alongside matching error_code",
                Observation::new("PutBucketPolicy", "negative", 400, Some("InvalidArgument".into()), []),
                Observation::new("PutBucketPolicy", "negative", 500, Some("MalformedPolicy".into()), []),
                &["status", "error_code"],
            ),
            (
                "header also differs alongside matching error_code",
                Observation::new(
                    "PutBucketPolicy",
                    "negative",
                    400,
                    Some("InvalidArgument".into()),
                    [("content-type", "application/xml")],
                ),
                Observation::new(
                    "PutBucketPolicy",
                    "negative",
                    400,
                    Some("MalformedPolicy".into()),
                    [("content-type", "text/plain")],
                ),
                &["error_code", "header.content-type"],
            ),
        ];

        for (label, obs_a, obs_b, expected_fields) in cases {
            let (unruled, accepted) = partition_differences(
                compare(std::slice::from_ref(obs_a), std::slice::from_ref(obs_b)),
                ComparisonMode::Reference,
                ACCEPTED_REFERENCE_CHANGES,
            );
            assert!(accepted.is_empty(), "case `{label}` must not be accepted, got accepted={accepted:?}");
            assert_eq!(fields(&unruled), *expected_fields, "case `{label}` must preserve all unruled differences");
        }

        // An accepted difference on PutBucketPolicy must never mask an unrelated regression.
        let a = vec![
            Observation::new("PutBucketPolicy", "negative", 400, Some("InvalidArgument".into()), []),
            Observation::new("GetObject", "negative", 404, Some("NoSuchKey".into()), []),
        ];
        let b = vec![
            Observation::new("PutBucketPolicy", "negative", 400, Some("MalformedPolicy".into()), []),
            Observation::new("GetObject", "negative", 404, Some("NoSuchBucket".into()), []),
        ];
        let (unruled, accepted) = partition_differences(compare(&a, &b), ComparisonMode::Reference, ACCEPTED_REFERENCE_CHANGES);
        assert_eq!(accepted.len(), 1);
        assert_eq!(accepted[0].difference.op, "PutBucketPolicy");
        assert_eq!(unruled.len(), 1);
        assert_eq!(unruled[0].op, "GetObject");
        assert_eq!(unruled[0].field, "error_code");
    }
}
