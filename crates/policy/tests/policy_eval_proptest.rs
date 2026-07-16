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

//! Property tests for the IAM policy evaluation algebra (backlog#1151 sec-9).
//!
//! The wildcard Action/Resource matching and Deny-first evaluation in
//! `Policy::is_allowed` were guarded only by example-based tests. These
//! properties pin the three algebraic invariants that authorization safety
//! rests on, over generated inputs:
//!
//!   (a) an explicit Deny matching the request denies it REGARDLESS of how many
//!       Allow statements co-exist or where the Deny sits in the statement list;
//!   (b) a wildcard Allow superset (`s3:*` on `bucket/*`) implies every narrower
//!       concrete Allow: any request allowed by an exact-action/exact-resource
//!       policy is also allowed by the widened policy;
//!   (c) an empty policy denies every non-owner request (default deny).
//!
//! Pure evaluation: no IO, no global state, parallel-safe. Statements are built
//! from JSON exactly like production policies arriving via PutPolicy. Generated
//! bucket/key/action pools avoid wildcard metacharacters so resource patterns
//! stay exact; the wildcard forms under test are introduced deliberately by the
//! properties themselves.

use proptest::prelude::*;
use rustfs_policy::policy::{Args, Policy};
use serde_json::Value;
use std::collections::HashMap;

/// Object-level S3 actions safe to pair with an `arn:aws:s3:::<bucket>/<key>`
/// resource. Kept to real, parseable action names.
const OBJECT_ACTIONS: &[&str] = &[
    "s3:GetObject",
    "s3:PutObject",
    "s3:DeleteObject",
    "s3:GetObjectTagging",
    "s3:PutObjectTagging",
];

fn statement_json(effect: &str, action: &str, resource: &str) -> String {
    format!(r#"{{"Effect":"{effect}","Action":["{action}"],"Resource":["{resource}"]}}"#)
}

fn policy_from_statements(statements: &[String]) -> Policy {
    let json = format!(r#"{{"Version":"2012-10-17","Statement":[{}]}}"#, statements.join(","));
    serde_json::from_str(&json).expect("generated policy JSON should parse")
}

fn is_allowed(policy: &Policy, action: &str, bucket: &str, object: &str) -> bool {
    let conditions: HashMap<String, Vec<String>> = HashMap::new();
    let claims: HashMap<String, Value> = HashMap::new();
    let groups: Option<Vec<String>> = None;
    pollster::block_on(policy.is_allowed(&Args {
        account: "PROPTESTUSER",
        groups: &groups,
        action: action.try_into().expect("action from the fixed pool should parse"),
        bucket,
        conditions: &conditions,
        is_owner: false,
        object,
        claims: &claims,
        deny_only: false,
    }))
}

/// Strategy: a bucket name without wildcard metacharacters.
fn bucket_strategy() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9]{2,7}"
}

/// Strategy: an object key (optionally nested one level) without metacharacters.
fn key_strategy() -> impl Strategy<Value = String> {
    "[a-z0-9]{1,12}(/[a-z0-9]{1,12})?"
}

/// Strategy: one action name from the object-action pool.
fn action_strategy() -> impl Strategy<Value = &'static str> {
    proptest::sample::select(OBJECT_ACTIONS)
}

proptest! {
    /// (a) Deny anywhere wins: a Deny statement matching the request denies it,
    /// no matter how many broad Allow statements surround it or at which index
    /// it appears. A regression here (e.g. Allow short-circuiting before the
    /// Deny scan, or statement order leaking into the decision) is privilege
    /// escalation.
    #[test]
    fn explicit_deny_anywhere_denies(
        bucket in bucket_strategy(),
        key in key_strategy(),
        action in action_strategy(),
        allow_count in 0usize..4,
        deny_pos_seed in 0usize..16,
        broad in proptest::bool::ANY,
    ) {
        // Broad Allow statements that all match the request on their own.
        let allow_resource = if broad {
            "arn:aws:s3:::*".to_string()
        } else {
            format!("arn:aws:s3:::{bucket}/*")
        };
        let mut statements: Vec<String> = (0..allow_count)
            .map(|_| statement_json("Allow", "s3:*", &allow_resource))
            .collect();

        // One Deny that matches the exact request.
        let deny = statement_json("Deny", action, &format!("arn:aws:s3:::{bucket}/{key}"));
        let deny_pos = deny_pos_seed % (statements.len() + 1);
        statements.insert(deny_pos, deny);

        let policy = policy_from_statements(&statements);

        // Sanity: without the Deny, the Allows (when present) do allow.
        if allow_count > 0 {
            let mut allows_only = statements.clone();
            allows_only.remove(deny_pos);
            let allow_policy = policy_from_statements(&allows_only);
            prop_assert!(
                is_allowed(&allow_policy, action, &bucket, &key),
                "sanity: the Allow statements alone should permit {action} on {bucket}/{key}"
            );
        }

        prop_assert!(
            !is_allowed(&policy, action, &bucket, &key),
            "explicit Deny at index {deny_pos} of {} statements must deny {action} on {bucket}/{key}",
            statements.len()
        );
    }

    /// (b) Wildcard superset implies the narrower concrete match: if an
    /// exact-action / exact-resource Allow permits a request, then widening
    /// that policy to `s3:*` on `arn:aws:s3:::<bucket>/*` (and further to
    /// `*` on `arn:aws:s3:::*`) must also permit it. A regression here means
    /// wildcard grants silently stop covering what their concrete forms cover.
    #[test]
    fn wildcard_superset_implies_concrete_match(
        bucket in bucket_strategy(),
        key in key_strategy(),
        action in action_strategy(),
        probe_bucket in bucket_strategy(),
        probe_key in key_strategy(),
        probe_action in action_strategy(),
    ) {
        let narrow = policy_from_statements(&[statement_json(
            "Allow",
            action,
            &format!("arn:aws:s3:::{bucket}/{key}"),
        )]);
        let widened = policy_from_statements(&[statement_json(
            "Allow",
            "s3:*",
            &format!("arn:aws:s3:::{bucket}/*"),
        )]);
        let widest = policy_from_statements(&[statement_json("Allow", "*", "arn:aws:s3:::*")]);

        // The request the narrow policy was built for is allowed at every width.
        prop_assert!(is_allowed(&narrow, action, &bucket, &key), "narrow policy must allow its own grant");
        prop_assert!(is_allowed(&widened, action, &bucket, &key), "s3:* on bucket/* must imply the concrete grant");
        prop_assert!(is_allowed(&widest, action, &bucket, &key), "* on arn:aws:s3:::* must imply the concrete grant");

        // Implication over arbitrary probes: anything the narrow policy allows,
        // the supersets allow too (contrapositive-checked on random requests
        // that may or may not match the narrow grant).
        if is_allowed(&narrow, probe_action, &probe_bucket, &probe_key) {
            prop_assert!(
                is_allowed(&widened, probe_action, &probe_bucket, &probe_key),
                "superset (s3:* on {bucket}/*) must allow whatever the narrow policy allows: {probe_action} on {probe_bucket}/{probe_key}"
            );
            prop_assert!(
                is_allowed(&widest, probe_action, &probe_bucket, &probe_key),
                "superset (* on arn:aws:s3:::*) must allow whatever the narrow policy allows"
            );
        }
    }

    /// (c) Default deny: a policy with no statements denies every non-owner
    /// request, whatever the action/bucket/object.
    #[test]
    fn empty_policy_denies_everything(
        bucket in bucket_strategy(),
        key in key_strategy(),
        action in action_strategy(),
    ) {
        let empty: Policy = serde_json::from_str(r#"{"Version":"2012-10-17","Statement":[]}"#)
            .expect("empty policy JSON should parse");
        prop_assert!(
            !is_allowed(&empty, action, &bucket, &key),
            "empty policy must deny {action} on {bucket}/{key}"
        );

        let default_policy = Policy::default();
        prop_assert!(
            !is_allowed(&default_policy, action, &bucket, &key),
            "Policy::default() must deny {action} on {bucket}/{key}"
        );
    }
}
