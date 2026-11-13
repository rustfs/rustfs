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

use rustfs_iam::sys::SESSION_POLICY_NAME;
use rustfs_policy::policy::{ID, Policy, Statement};
use rustfs_s3_types::{S3Error, S3ErrorCode, S3Result, s3_error};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionPolicyInput {
    #[serde(default, rename = "Id", alias = "ID")]
    id: ID,
    #[serde(default, rename = "Version")]
    version: String,
    #[serde(default, rename = "Statement")]
    statements: Vec<Statement>,
}

fn parse_session_policy(input: &str) -> rustfs_policy::error::Result<Policy> {
    let SessionPolicyInput { id, version, statements } = serde_json::from_str(input)?;
    let policy = Policy { id, version, statements };
    policy.validate()?;
    Ok(policy)
}

pub(crate) fn populate_session_policy(claims: &mut HashMap<String, Value>, policy: &str) -> S3Result<()> {
    if !policy.is_empty() {
        let session_policy = parse_session_policy(policy).map_err(|e| {
            let error_msg = format!(
                "Failed to parse session policy: {}. Please check that the policy is valid JSON format with standard brackets [] for arrays.",
                e
            );
            S3Error::with_message(S3ErrorCode::InvalidRequest, error_msg)
        })?;
        if session_policy.version.is_empty() {
            return Err(s3_error!(InvalidRequest, "invalid policy"));
        }

        let policy_buf = serde_json::to_vec(&session_policy)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("marshal policy err {e}")))?;

        if policy_buf.len() > 2048 {
            return Err(s3_error!(InvalidRequest, "policy too large"));
        }

        claims.insert(
            SESSION_POLICY_NAME.to_string(),
            Value::String(base64_simd::URL_SAFE_NO_PAD.encode_to_string(&policy_buf)),
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_policy_does_not_change_claims() {
        let mut claims = HashMap::new();
        populate_session_policy(&mut claims, "").expect("empty policy should be accepted");
        assert!(claims.is_empty());
    }

    #[test]
    fn session_policy_claim_preserves_the_iam_policy_format() {
        for id_key in [Some("Id"), Some("ID"), None] {
            let mut input = serde_json::json!({
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": ["arn:aws:s3:::bucket/*"]}]
            });
            if let Some(key) = id_key {
                input[key] = "read-objects".into();
            }
            if id_key == Some("Id") {
                assert!(Policy::parse_config(input.to_string().as_bytes()).is_err());
            }
            let mut claims = HashMap::new();
            populate_session_policy(&mut claims, &input.to_string()).expect("valid session policy");

            let encoded = claims[SESSION_POLICY_NAME].as_str().expect("encoded session policy");
            let decoded = base64_simd::URL_SAFE_NO_PAD
                .decode_to_vec(encoded.as_bytes())
                .expect("base64url claim");
            let json: Value = serde_json::from_slice(&decoded).expect("JSON claim");
            assert!(json.get("Id").is_none());
            assert_eq!(json.get("ID").and_then(Value::as_str), id_key.map(|_| "read-objects"));
            let policy = Policy::parse_config(&decoded).expect("claim must be readable by the IAM policy parser");
            assert_eq!(policy.version, "2012-10-17");
            assert_eq!(policy.statements.len(), 1);
        }
    }

    #[test]
    fn invalid_policy_is_rejected_as_invalid_request() {
        for input in [
            "not-json",
            r#"{"Id":"a","ID":"b","Version":"2012-10-17"}"#,
            r#"{"Id":"a","Id":"b","Version":"2012-10-17"}"#,
            r#"{"id":"a","Version":"2012-10-17"}"#,
            r#"{"Id":42,"Version":"2012-10-17"}"#,
            r#"{"Id":"a"}"#,
            r#"{"Id":"a","Version":"invalid"}"#,
            r#"{"Id":"a","Version":"2012-10-17","Statement":[{"Effect":"Allow"}]}"#,
        ] {
            let mut claims = HashMap::new();
            let error = populate_session_policy(&mut claims, input).expect_err("invalid session policy");
            assert_eq!(error.code(), &S3ErrorCode::InvalidRequest, "{input}");
            assert!(claims.is_empty());
        }
    }
}
