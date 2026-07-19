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
use rustfs_policy::policy::Policy;
use s3s::{S3Error, S3ErrorCode, S3Result, s3_error};
use serde_json::Value;
use std::collections::HashMap;

pub(crate) fn populate_session_policy(claims: &mut HashMap<String, Value>, policy: &str) -> S3Result<()> {
    if !policy.is_empty() {
        let session_policy = Policy::parse_config(policy.as_bytes()).map_err(|e| {
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
    fn valid_policy_is_encoded_in_the_session_claim() {
        let mut claims = HashMap::new();
        let policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::bucket/*"]}]}"#;

        populate_session_policy(&mut claims, policy).expect("valid policy should be accepted");

        let encoded = claims
            .get(SESSION_POLICY_NAME)
            .and_then(Value::as_str)
            .expect("session policy claim should be present");
        let decoded = base64_simd::URL_SAFE_NO_PAD
            .decode_to_vec(encoded.as_bytes())
            .expect("session policy claim should be base64url encoded");
        let decoded_policy: Value = serde_json::from_slice(&decoded).expect("session policy claim should contain JSON");
        assert_eq!(decoded_policy["Version"], "2012-10-17");
    }

    #[test]
    fn invalid_policy_is_rejected_as_invalid_request() {
        let mut claims = HashMap::new();
        let error = populate_session_policy(&mut claims, "not-json").expect_err("invalid policy should fail");
        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
    }
}
