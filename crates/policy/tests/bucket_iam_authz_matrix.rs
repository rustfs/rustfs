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

//! Bucket-policy x IAM-policy priority conflict matrix (backlog#1151 sec-8).
//!
//! Individual policy layers are well tested in isolation, but the CROSS-LAYER
//! resolution — which layer wins when IAM and bucket policy disagree — had no
//! coverage. A priority error here is a data-exposure or data-lockout bug, so
//! this table-driven test pins the combination for all four Allow/Deny quadrants
//! plus the anonymous (no-credential) case.
//!
//! Pure evaluation only: it drives `Policy::is_allowed` (IAM,
//! `crates/policy/src/policy/policy.rs:69`) and `BucketPolicy::is_allowed`
//! (bucket, `:207`) directly through a helper that models the request-layer
//! orchestration in `rustfs::storage::access::authorize_request`. No global
//! singletons, no IAM store, no server.
//!
//! ## The orchestration being characterized (authorize_request)
//!
//! For an authenticated, non-owner request on a normal (non-policy-API) action:
//!   1. Bucket explicit-Deny gate — the bucket policy is evaluated as `is_owner`
//!      so ONLY its Deny statements can reject; a matching bucket Deny denies
//!      regardless of what IAM says. This gate runs BEFORE the IAM-allow
//!      short-circuit, which is why a bucket Deny beats an IAM Allow.
//!   2. IAM allow — if the identity policy allows, the request is permitted.
//!   3. Bucket-policy allow fallback — if IAM did not allow, a bucket Allow can
//!      still permit the request.
//!
//! Anonymous requests skip 1-3 and are governed by the bucket policy alone.
//!
//! ## Invariant surfaced by the matrix (read before changing an assertion)
//!
//! A **bucket** explicit Deny is a hard gate and always wins (cases
//! `iam_allow_x_bucket_deny`, `iam_deny_x_bucket_deny`, `anon_x_bucket_deny`).
//! An **IAM** explicit Deny is NOT a hard gate: because step 3 falls back to the
//! bucket policy, a bucket Allow overrides an IAM explicit Deny
//! (`iam_deny_x_bucket_allow` => allowed). This is RustFS's (MinIO-lineage)
//! resolution and it DIVERGES from AWS's "an explicit Deny in any policy always
//! wins". The case below pins the actual behavior; if RustFS later hardens IAM
//! Deny into a hard gate, that case flips to `false` and must be updated
//! deliberately.

use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_policy::policy::{Args, BucketPolicy, BucketPolicyArgs, Policy};
use serde_json::Value;
use std::collections::HashMap;

const BUCKET: &str = "mybucket";
const OBJECT: &str = "obj";
const USER: &str = "TESTUSER";

fn get_object() -> Action {
    Action::S3Action(S3Action::GetObjectAction)
}

fn iam(effect: &str) -> Policy {
    let json = format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"{effect}","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::mybucket/*"]}}]}}"#
    );
    serde_json::from_str(&json).expect("IAM policy JSON should parse")
}

fn bucket(effect: &str) -> BucketPolicy {
    let json = format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"{effect}","Principal":{{"AWS":["*"]}},"Action":["s3:GetObject"],"Resource":["arn:aws:s3:::mybucket/*"]}}]}}"#
    );
    serde_json::from_str(&json).expect("bucket policy JSON should parse")
}

/// Models `rustfs::storage::access::authorize_request` for a normal
/// (non-policy-API) action, using the real policy evaluators. `account.is_empty()`
/// marks an anonymous request. Kept in lockstep with that function's layering;
/// if the orchestration there changes, update this helper and the table together.
async fn authorize(iam_policy: Option<&Policy>, bucket_policy: Option<&BucketPolicy>, account: &str) -> bool {
    let conditions: HashMap<String, Vec<String>> = HashMap::new();
    let claims: HashMap<String, Value> = HashMap::new();
    let groups: Option<Vec<String>> = None;
    let action = get_object();

    let bucket_args = |is_owner: bool| BucketPolicyArgs {
        bucket: BUCKET,
        action,
        is_owner,
        account,
        groups: &groups,
        conditions: &conditions,
        object: OBJECT,
    };

    // Anonymous: bucket policy alone (is_owner = false); a bucket Deny rejects
    // via its own Deny loop, a bucket Allow (Principal *) permits.
    if account.is_empty() {
        return match bucket_policy {
            Some(bp) => bp.is_allowed(&bucket_args(false)).await,
            None => false,
        };
    }

    // (1) Bucket explicit-Deny gate: is_owner isolates Deny statements.
    if let Some(bp) = bucket_policy
        && !bp.is_allowed(&bucket_args(true)).await
    {
        return false;
    }

    // (2) IAM allow.
    if let Some(p) = iam_policy {
        let allowed = p
            .is_allowed(&Args {
                account,
                groups: &groups,
                action,
                bucket: BUCKET,
                conditions: &conditions,
                is_owner: false,
                object: OBJECT,
                claims: &claims,
                deny_only: false,
            })
            .await;
        if allowed {
            return true;
        }
    }

    // (3) Bucket-policy allow fallback.
    if let Some(bp) = bucket_policy
        && bp.is_allowed(&bucket_args(false)).await
    {
        return true;
    }

    false
}

struct Case {
    name: &'static str,
    iam: Option<Policy>,
    bucket: Option<BucketPolicy>,
    account: &'static str,
    expected: bool,
}

#[test]
fn bucket_iam_priority_matrix() {
    let cases = vec![
        // --- Four Allow/Deny quadrants (authenticated, non-owner) ---
        Case {
            name: "iam_allow_x_bucket_allow",
            iam: Some(iam("Allow")),
            bucket: Some(bucket("Allow")),
            account: USER,
            expected: true,
        },
        Case {
            // Bucket explicit Deny wins over IAM Allow (deny gate before IAM short-circuit).
            name: "iam_allow_x_bucket_deny",
            iam: Some(iam("Allow")),
            bucket: Some(bucket("Deny")),
            account: USER,
            expected: false,
        },
        Case {
            // IAM explicit Deny is SOFT: the bucket Allow fallback overrides it.
            // Diverges from AWS "explicit Deny always wins"; pins current behavior.
            name: "iam_deny_x_bucket_allow",
            iam: Some(iam("Deny")),
            bucket: Some(bucket("Allow")),
            account: USER,
            expected: true,
        },
        Case {
            name: "iam_deny_x_bucket_deny",
            iam: Some(iam("Deny")),
            bucket: Some(bucket("Deny")),
            account: USER,
            expected: false,
        },
        // --- No bucket policy: IAM alone governs ---
        Case {
            name: "iam_allow_x_no_bucket",
            iam: Some(iam("Allow")),
            bucket: None,
            account: USER,
            expected: true,
        },
        Case {
            name: "iam_deny_x_no_bucket",
            iam: Some(iam("Deny")),
            bucket: None,
            account: USER,
            expected: false,
        },
        // --- No IAM grant: bucket Allow fallback can still permit ---
        Case {
            name: "iam_empty_x_bucket_allow",
            iam: Some(iam("Deny")), // Deny here == "IAM does not grant"; fallback decides
            bucket: Some(bucket("Allow")),
            account: USER,
            expected: true,
        },
        Case {
            name: "no_grant_anywhere_denies",
            iam: None,
            bucket: None,
            account: USER,
            expected: false,
        },
        // --- Anonymous (no credentials): bucket policy alone ---
        Case {
            name: "anon_x_bucket_allow",
            iam: None,
            bucket: Some(bucket("Allow")),
            account: "",
            expected: true,
        },
        Case {
            name: "anon_x_bucket_deny",
            iam: None,
            bucket: Some(bucket("Deny")),
            account: "",
            expected: false,
        },
        Case {
            name: "anon_x_no_bucket",
            iam: None,
            bucket: None,
            account: "",
            expected: false,
        },
    ];

    for case in &cases {
        let got = pollster::block_on(authorize(case.iam.as_ref(), case.bucket.as_ref(), case.account));
        assert_eq!(
            got, case.expected,
            "quadrant `{}`: expected allowed={}, got {}",
            case.name, case.expected, got
        );
    }
}

/// Within a single IAM policy, an explicit Deny beats a co-resident Allow for the
/// same action (Deny-first evaluation, `policy.rs:71`). This is the intra-policy
/// half of "explicit Deny wins" and is independent of the cross-layer gate above.
#[test]
fn intra_iam_policy_explicit_deny_beats_allow() {
    let policy: Policy = serde_json::from_str(
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::mybucket/*"]},
            {"Effect":"Deny","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::mybucket/*"]}
        ]}"#,
    )
    .expect("IAM policy JSON should parse");

    let conditions: HashMap<String, Vec<String>> = HashMap::new();
    let claims: HashMap<String, Value> = HashMap::new();
    let groups: Option<Vec<String>> = None;
    let allowed = pollster::block_on(policy.is_allowed(&Args {
        account: USER,
        groups: &groups,
        action: get_object(),
        bucket: BUCKET,
        conditions: &conditions,
        is_owner: false,
        object: OBJECT,
        claims: &claims,
        deny_only: false,
    }));
    assert!(!allowed, "an explicit IAM Deny must beat a co-resident Allow for the same action");
}

/// Within a single bucket policy, an explicit Deny beats a co-resident Allow, and
/// the deny holds even for an owner-context evaluation (`policy.rs:208`) — this is
/// exactly the property the authenticated deny gate relies on.
#[test]
fn intra_bucket_policy_explicit_deny_beats_allow_even_for_owner() {
    let policy: BucketPolicy = serde_json::from_str(
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Principal":{"AWS":["*"]},"Action":["s3:GetObject"],"Resource":["arn:aws:s3:::mybucket/*"]},
            {"Effect":"Deny","Principal":{"AWS":["*"]},"Action":["s3:GetObject"],"Resource":["arn:aws:s3:::mybucket/*"]}
        ]}"#,
    )
    .expect("bucket policy JSON should parse");

    let conditions: HashMap<String, Vec<String>> = HashMap::new();
    let groups: Option<Vec<String>> = None;
    for is_owner in [false, true] {
        let allowed = pollster::block_on(policy.is_allowed(&BucketPolicyArgs {
            bucket: BUCKET,
            action: get_object(),
            is_owner,
            account: USER,
            groups: &groups,
            conditions: &conditions,
            object: OBJECT,
        }));
        assert!(!allowed, "an explicit bucket Deny must beat a co-resident Allow (is_owner={is_owner})");
    }
}
