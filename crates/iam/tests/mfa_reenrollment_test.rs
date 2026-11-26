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

#![recursion_limit = "256"]

//! Regression test: re-enrolling over an active second factor requires proving
//! the active factor and honours persisted lockout on repeated failures.

use rustfs_iam::mfa::{MfaServiceError, MfaVerification, TotpSecret, service, store};
use serial_test::serial;
use time::OffsetDateTime;

const ACCESS_KEY: &str = "alice";
const WRONG_RECOVERY_CODE: &str = "ZZZZ-ZZZZ-ZZZZ-ZZZZ-ZZZZ";

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn enrolling_over_an_active_factor_requires_a_code_and_persists_lockout() {
    temp_env::async_with_vars([("RUSTFS_IAM_MASTER_KEY", Some("mfa-reenrollment-test-master-key"))], async {
        let temp_dir =
            tempfile::TempDir::with_prefix("rustfs_iam_mfa_reenrollment_test_").expect("temp directory must be created");
        let ecstore = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(temp_dir.path())
            .init_bucket_metadata(false)
            .build()
            .await
            .ecstore;

        // First enrollment has no active factor to prove.
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
        let enrollment = service::enroll(ecstore.clone(), ACCESS_KEY, None, now).await.expect("enroll");
        let secret = TotpSecret::from_base32(&enrollment.secret_base32).expect("server-issued secret");
        let issued = service::activate(ecstore.clone(), ACCESS_KEY, &secret.code_at(now.unix_timestamp() as u64), now)
            .await
            .expect("activate");

        // Re-enrollment without a current code is rejected without modifying state.
        let later = now + time::Duration::seconds(60);
        assert!(matches!(
            service::enroll(ecstore.clone(), ACCESS_KEY, None, later).await,
            Err(MfaServiceError::AlreadyEnabled)
        ));
        let loaded = store::load(ecstore.clone(), ACCESS_KEY, later).await.expect("load");
        assert!(!loaded.record.has_pending_enrollment(later));
        assert_eq!(loaded.record.failed_attempts, 0);

        // Five wrong re-enrollment codes must persist and trigger lockout.
        for attempt in 1..=5 {
            let err = service::enroll(ecstore.clone(), ACCESS_KEY, Some(WRONG_RECOVERY_CODE), later)
                .await
                .expect_err("wrong re-enrollment code must be refused");
            assert!(matches!(err, MfaServiceError::InvalidCode), "attempt {attempt}: {err}");
        }
        assert!(matches!(
            service::enroll(ecstore.clone(), ACCESS_KEY, Some(&issued.recovery_codes[0]), later).await,
            Err(MfaServiceError::Locked { .. })
        ));
        assert!(matches!(
            service::verify(ecstore.clone(), ACCESS_KEY, &issued.recovery_codes[0], later).await,
            Err(MfaServiceError::Locked { .. })
        ));

        // After lockout expires, the owner's original codes still verify and proven re-enrollment works.
        let after_lock = later + time::Duration::minutes(16);
        let verified = service::verify(ecstore.clone(), ACCESS_KEY, &issued.recovery_codes[0], after_lock)
            .await
            .expect("owner's recovery code must still verify after lockout expires");
        assert_eq!(verified, MfaVerification::RecoveryCode { remaining: 9 });

        let replacement = service::enroll(ecstore.clone(), ACCESS_KEY, Some(&issued.recovery_codes[1]), after_lock)
            .await
            .expect("proven re-enrollment must succeed");
        let replacement = TotpSecret::from_base32(&replacement.secret_base32).expect("server-issued replacement secret");
        let verified = service::verify(
            ecstore.clone(),
            ACCESS_KEY,
            &secret.code_at(after_lock.unix_timestamp() as u64),
            after_lock,
        )
        .await
        .expect("active factor must keep working while replacement is pending");
        assert_eq!(verified, MfaVerification::Totp);

        let confirmed = after_lock + time::Duration::seconds(60);
        service::activate(
            ecstore.clone(),
            ACCESS_KEY,
            &replacement.code_at(confirmed.unix_timestamp() as u64),
            confirmed,
        )
        .await
        .expect("activate replacement");
        assert!(matches!(
            service::verify(ecstore, ACCESS_KEY, &issued.recovery_codes[2], confirmed).await,
            Err(MfaServiceError::InvalidCode)
        ));
    })
    .await;
}
