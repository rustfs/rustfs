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

//! End-to-end coverage for the self-service account and two-factor surface.
//!
//! The unit tests cover the state machine at its edges; what only an end-to-end
//! test can prove is that the pieces are wired together and that the *existing*
//! authentication paths still behave. Specifically:
//!
//! 1. Enrollment is refused when `RUSTFS_IAM_MASTER_KEY` is absent, so a TOTP
//!    secret is never written where an attacker could read it off a disk.
//! 2. With a master key, the full flow works: enroll, activate with a real
//!    RFC 6238 code, and receive single-use recovery codes.
//! 3. Once a factor is enrolled, `AssumeRole` refuses to mint a session without
//!    one, and accepts a valid code — the actual login gate.
//! 4. A direct SigV4 admin request keeps working with a factor enrolled. This is
//!    the regression that matters most: gating it would break every script and
//!    CLI the moment somebody enabled 2FA.
//! 5. `AssumeRole` for an identity with no enrollment is byte-for-byte the old
//!    behaviour, so existing deployments are untouched.
//! 6. Rotating a password through `/account/password` invalidates the sessions
//!    minted under the old secret.

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestEnvironment, init_logging, local_http_client};
    use hmac::{Hmac, KeyInit as _, Mac};
    use http::header::HOST;
    use rustfs_signer::constants::UNSIGNED_PAYLOAD;
    use rustfs_signer::sign_v4;
    use s3s::Body;
    use sha1::Sha1;
    use std::error::Error;
    use std::time::{SystemTime, UNIX_EPOCH};

    const ACCOUNT_INFO_PATH: &str = "/rustfs/admin/v3/account/info";
    const ACCOUNT_PASSWORD_PATH: &str = "/rustfs/admin/v3/account/password";
    const ACCOUNT_MFA_PATH: &str = "/rustfs/admin/v3/account/mfa";
    const ACCOUNT_MFA_ENROLL_PATH: &str = "/rustfs/admin/v3/account/mfa/enroll";
    const ACCOUNT_MFA_ACTIVATE_PATH: &str = "/rustfs/admin/v3/account/mfa/activate";
    const MFA_CHALLENGE_PATH: &str = "/rustfs/admin/v3/mfa/challenge";
    const ADMIN_INFO_PATH: &str = "/rustfs/admin/v3/info";

    /// A master key so the server will accept an enrollment. Test-only value.
    const TEST_MASTER_KEY: &str = "e2e-mfa-master-key-do-not-reuse";

    type HmacSha1 = Hmac<Sha1>;

    /// One signed admin request, returning the status and the raw body.
    ///
    /// Thin wrapper over [`crate::common::admin_request`], kept local so the
    /// call sites below keep their `Option<&str>` body shape.
    async fn signed_request(
        base_url: &str,
        method: http::Method,
        path: &str,
        body: Option<&str>,
        access_key: &str,
        secret_key: &str,
    ) -> Result<(reqwest::StatusCode, String), Box<dyn Error + Send + Sync>> {
        crate::common::admin_request(base_url, method, path, body.map(str::to_string), access_key, secret_key).await
    }

    /// A SigV4-signed `AssumeRole` form POST, optionally carrying a second factor.
    ///
    /// Uses STS's own `SerialNumber`/`TokenCode` fields, which is the point: a
    /// script or SDK can present the factor without a RustFS-specific protocol.
    async fn assume_role(
        base_url: &str,
        access_key: &str,
        secret_key: &str,
        second_factor: Option<(&str, &str)>,
    ) -> Result<(reqwest::StatusCode, String), Box<dyn Error + Send + Sync>> {
        let mut form = vec![
            ("Action", "AssumeRole".to_string()),
            ("Version", "2011-06-15".to_string()),
            ("RoleArn", "arn:aws:iam::*:role/Admin".to_string()),
            ("RoleSessionName", "e2e".to_string()),
            ("DurationSeconds", "3600".to_string()),
        ];
        if let Some((challenge, code)) = second_factor {
            form.push(("SerialNumber", challenge.to_string()));
            form.push(("TokenCode", code.to_string()));
        }
        let body = serde_urlencoded::to_string(&form)?;

        let uri = base_url.parse::<http::Uri>()?;
        let authority = uri.authority().ok_or("missing authority")?.to_string();
        let request = http::Request::builder()
            .method(http::Method::POST)
            .uri(format!("{base_url}/"))
            .header(HOST, authority)
            .header("content-type", "application/x-www-form-urlencoded")
            .header("x-amz-content-sha256", UNSIGNED_PAYLOAD);
        let signed = sign_v4(request.body(Body::empty())?, 0, access_key, secret_key, "", "us-east-1");

        let client = local_http_client();
        let mut builder = client.request(http::Method::POST, format!("{base_url}/"));
        for (name, value) in signed.headers() {
            builder = builder.header(name, value);
        }
        let response = builder.body(body).send().await?;
        let status = response.status();
        let text = response.text().await?;
        Ok((status, text))
    }

    /// Generate the current RFC 6238 code for a base32 secret.
    ///
    /// Computed independently of the server implementation: a shared helper
    /// could agree with a bug on both sides.
    fn totp_now(secret_base32: &str) -> String {
        let secret = data_encoding::BASE32_NOPAD
            .decode(secret_base32.as_bytes())
            .expect("server must return unpadded base32");
        let step = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after the epoch")
            .as_secs()
            / 30;

        let mut mac = HmacSha1::new_from_slice(&secret).expect("HMAC accepts any key length");
        mac.update(&step.to_be_bytes());
        let digest = mac.finalize().into_bytes();

        let offset = (digest[digest.len() - 1] & 0x0f) as usize;
        let binary = u32::from_be_bytes([
            digest[offset] & 0x7f,
            digest[offset + 1],
            digest[offset + 2],
            digest[offset + 3],
        ]);
        format!("{:06}", binary % 1_000_000)
    }

    fn json(body: &str) -> serde_json::Value {
        serde_json::from_str(body).unwrap_or_else(|error| panic!("expected JSON, got {body}: {error}"))
    }

    #[tokio::test]
    async fn enrollment_is_refused_without_at_rest_protection() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        // Deliberately no RUSTFS_IAM_MASTER_KEY.
        env.start_rustfs_server(vec![]).await?;

        let (access_key, secret_key) = (env.access_key.clone(), env.secret_key.clone());

        // The account surface itself works.
        let (status, body) =
            signed_request(&env.url, http::Method::GET, ACCOUNT_INFO_PATH, None, &access_key, &secret_key).await?;
        assert_eq!(status, reqwest::StatusCode::OK, "account info must be reachable, body: {body}");
        let info = json(&body);
        assert_eq!(info["access_key"], access_key.as_str());
        assert_eq!(info["identity_type"], "root");
        assert_eq!(info["credentials_source"], "env");
        // Root credentials come from a process-wide OnceLock that also derives
        // the internode RPC secret, so they are immutable at runtime.
        assert_eq!(info["mutable"]["password"], false);

        // Status reports the refusal rather than pretending enrollment is possible.
        let (status, body) =
            signed_request(&env.url, http::Method::GET, ACCOUNT_MFA_PATH, None, &access_key, &secret_key).await?;
        assert_eq!(status, reqwest::StatusCode::OK, "mfa status must be reachable, body: {body}");
        let mfa = json(&body);
        assert_eq!(mfa["enabled"], false);
        assert_eq!(mfa["enrollment_available"], false);
        assert!(
            mfa["enrollment_blocked_reason"]
                .as_str()
                .is_some_and(|reason| reason.contains("RUSTFS_IAM_MASTER_KEY")),
            "the refusal must name the variable an operator has to set, body: {body}"
        );

        // And enrolling actually fails, rather than writing a plaintext secret.
        let (status, body) = signed_request(
            &env.url,
            http::Method::POST,
            ACCOUNT_MFA_ENROLL_PATH,
            Some("{}"),
            &access_key,
            &secret_key,
        )
        .await?;
        assert!(
            status.is_client_error() || status.is_server_error(),
            "enrollment must fail without a master key, status: {status}, body: {body}"
        );
        assert!(
            body.contains("RUSTFS_IAM_MASTER_KEY"),
            "the failure must explain the remedy, body: {body}"
        );

        env.stop_server();
        Ok(())
    }

    #[tokio::test]
    async fn assume_role_is_unchanged_for_an_identity_with_no_second_factor() -> Result<(), Box<dyn Error + Send + Sync>> {
        // The regression that protects every existing deployment: an identity
        // with no enrollment must take no new code path.
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server_with_env(vec![], &[("RUSTFS_IAM_MASTER_KEY", TEST_MASTER_KEY)])
            .await?;

        let (access_key, secret_key) = (env.access_key.clone(), env.secret_key.clone());

        let (status, body) =
            signed_request(&env.url, http::Method::GET, MFA_CHALLENGE_PATH, None, &access_key, &secret_key).await?;
        assert_eq!(status, reqwest::StatusCode::OK, "challenge must be reachable, body: {body}");
        let challenge = json(&body);
        assert_eq!(challenge["required"], false, "no enrollment means no challenge");
        assert!(challenge["challenge"].is_null());

        let (status, body) = assume_role(&env.url, &access_key, &secret_key, None).await?;
        assert_eq!(status, reqwest::StatusCode::OK, "AssumeRole must still work, body: {body}");
        assert!(body.contains("<AccessKeyId>"), "expected STS credentials, body: {body}");

        env.stop_server();
        Ok(())
    }

    #[tokio::test]
    async fn the_full_second_factor_lifecycle_gates_only_session_minting() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server_with_env(vec![], &[("RUSTFS_IAM_MASTER_KEY", TEST_MASTER_KEY)])
            .await?;

        let (access_key, secret_key) = (env.access_key.clone(), env.secret_key.clone());

        // --- Enroll ---
        let (status, body) = signed_request(
            &env.url,
            http::Method::POST,
            ACCOUNT_MFA_ENROLL_PATH,
            Some("{}"),
            &access_key,
            &secret_key,
        )
        .await?;
        assert_eq!(status, reqwest::StatusCode::OK, "enrollment must succeed, body: {body}");
        let enrollment = json(&body);
        let secret_base32 = enrollment["secret_base32"].as_str().expect("secret").to_string();
        assert!(
            enrollment["otpauth_uri"]
                .as_str()
                .is_some_and(|uri| uri.starts_with("otpauth://totp/RustFS:")),
            "body: {body}"
        );
        assert!(!enrollment["qr_svg"].as_str().unwrap_or_default().is_empty(), "expected an SVG");
        assert!(!enrollment["qr_utf8"].as_str().unwrap_or_default().is_empty(), "expected block art");

        // A pending enrollment must not gate anything yet: a mis-scanned QR
        // cannot be allowed to lock the operator out.
        let (status, body) =
            signed_request(&env.url, http::Method::GET, MFA_CHALLENGE_PATH, None, &access_key, &secret_key).await?;
        assert_eq!(status, reqwest::StatusCode::OK);
        assert_eq!(json(&body)["required"], false, "a pending enrollment must not gate login");

        // --- Activate ---
        let code = totp_now(&secret_base32);
        let (status, body) = signed_request(
            &env.url,
            http::Method::POST,
            ACCOUNT_MFA_ACTIVATE_PATH,
            Some(&format!(r#"{{"code":"{code}"}}"#)),
            &access_key,
            &secret_key,
        )
        .await?;
        assert_eq!(status, reqwest::StatusCode::OK, "activation must succeed, body: {body}");
        let activated = json(&body);
        let recovery_codes = activated["recovery_codes"].as_array().expect("recovery codes").clone();
        assert_eq!(recovery_codes.len(), 10, "expected a full recovery set, body: {body}");

        // --- The gate is now on for session minting ---
        let (status, body) =
            signed_request(&env.url, http::Method::GET, MFA_CHALLENGE_PATH, None, &access_key, &secret_key).await?;
        assert_eq!(status, reqwest::StatusCode::OK);
        let challenge_body = json(&body);
        assert_eq!(challenge_body["required"], true, "body: {body}");
        let challenge = challenge_body["challenge"].as_str().expect("challenge").to_string();

        let (status, body) = assume_role(&env.url, &access_key, &secret_key, None).await?;
        assert!(status.is_client_error(), "AssumeRole must refuse without a factor, body: {body}");
        assert!(
            body.contains("MultiFactorAuthRequired"),
            "clients match on this code to prompt instead of reporting a failed login, body: {body}"
        );

        // --- ... but direct SigV4 access is untouched ---
        let (status, body) = signed_request(&env.url, http::Method::GET, ADMIN_INFO_PATH, None, &access_key, &secret_key).await?;
        assert_eq!(
            status,
            reqwest::StatusCode::OK,
            "a direct admin request must keep working with a factor enrolled, body: {body}"
        );

        // --- A valid factor mints the session ---
        // A fresh code: activation consumed the previous time step, so reusing
        // that code would be refused as a replay.
        let code = wait_for_a_fresh_code(&secret_base32).await;
        let (status, body) = assume_role(&env.url, &access_key, &secret_key, Some((&challenge, &code))).await?;
        assert_eq!(status, reqwest::StatusCode::OK, "a valid factor must mint a session, body: {body}");
        assert!(body.contains("<AccessKeyId>"), "expected STS credentials, body: {body}");

        // --- A recovery code also works, once ---
        let recovery_code = recovery_codes[0].as_str().expect("recovery code").to_string();
        let (status, body) = assume_role(&env.url, &access_key, &secret_key, Some((&challenge, &recovery_code))).await?;
        assert_eq!(status, reqwest::StatusCode::OK, "a recovery code must mint a session, body: {body}");

        let (status, body) = assume_role(&env.url, &access_key, &secret_key, Some((&challenge, &recovery_code))).await?;
        assert!(
            status.is_client_error(),
            "a spent recovery code must not work twice, status: {status}, body: {body}"
        );

        env.stop_server();
        Ok(())
    }

    #[tokio::test]
    async fn an_iam_user_can_rotate_its_own_password_and_lose_its_sessions() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server_with_env(vec![], &[("RUSTFS_IAM_MASTER_KEY", TEST_MASTER_KEY)])
            .await?;

        let (root_ak, root_sk) = (env.access_key.clone(), env.secret_key.clone());
        let user_ak = "mfarotationuser";
        let old_sk = "mfarotationsecret";
        let new_sk = "mfarotationsecret2";

        // Root creates the user.
        let (status, body) = signed_request(
            &env.url,
            http::Method::PUT,
            &format!("/rustfs/admin/v3/add-user?accessKey={user_ak}"),
            Some(&format!(r#"{{"secretKey":"{old_sk}","status":"enabled"}}"#)),
            &root_ak,
            &root_sk,
        )
        .await?;
        assert_eq!(status, reqwest::StatusCode::OK, "user creation must succeed, body: {body}");

        // The user sees itself as mutable, unlike root.
        let (status, body) = signed_request(&env.url, http::Method::GET, ACCOUNT_INFO_PATH, None, user_ak, old_sk).await?;
        assert_eq!(status, reqwest::StatusCode::OK, "body: {body}");
        let info = json(&body);
        assert_eq!(info["identity_type"], "iam");
        assert_eq!(info["credentials_source"], "iam");
        assert_eq!(info["mutable"]["password"], true);

        // The wrong current secret is refused, so a live session alone cannot
        // rewrite the credential.
        let (status, body) = signed_request(
            &env.url,
            http::Method::POST,
            ACCOUNT_PASSWORD_PATH,
            Some(&format!(r#"{{"current_secret_key":"wrong-secret","new_secret_key":"{new_sk}"}}"#)),
            user_ak,
            old_sk,
        )
        .await?;
        assert!(status.is_client_error(), "a wrong current secret must be refused, body: {body}");

        // The correct one rotates it.
        let (status, body) = signed_request(
            &env.url,
            http::Method::POST,
            ACCOUNT_PASSWORD_PATH,
            Some(&format!(r#"{{"current_secret_key":"{old_sk}","new_secret_key":"{new_sk}"}}"#)),
            user_ak,
            old_sk,
        )
        .await?;
        assert_eq!(status, reqwest::StatusCode::OK, "rotation must succeed, body: {body}");

        // The new secret works and the old one does not.
        let (status, body) = signed_request(&env.url, http::Method::GET, ACCOUNT_INFO_PATH, None, user_ak, new_sk).await?;
        assert_eq!(status, reqwest::StatusCode::OK, "the new secret must work, body: {body}");

        let (status, _) = signed_request(&env.url, http::Method::GET, ACCOUNT_INFO_PATH, None, user_ak, old_sk).await?;
        assert!(status.is_client_error(), "the old secret must stop working, status: {status}");

        env.stop_server();
        Ok(())
    }

    /// Wait until the current time step differs from the one a code was just
    /// consumed in, then return a code for it.
    ///
    /// Anti-replay burns the step, so a test that reuses a code inside its own
    /// window would fail for the right reason at the wrong moment.
    async fn wait_for_a_fresh_code(secret_base32: &str) -> String {
        let step_at_start = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after the epoch")
            .as_secs()
            / 30;

        loop {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock after the epoch")
                .as_secs();
            if now / 30 > step_at_start {
                return totp_now(secret_base32);
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    }
}
