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

//! Regression test: failed MFA verifications must persist through
//! `mfa::store::update` and engage lockout after consecutive failures.

use rustfs_iam::mfa::{MfaServiceError, MfaVerification, TotpSecret, service, store};
use serial_test::serial;
use time::OffsetDateTime;

const ACCESS_KEY: &str = "alice";
const WRONG_RECOVERY_CODE: &str = "ZZZZ-ZZZZ-ZZZZ-ZZZZ-ZZZZ";

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn failed_verifications_are_persisted_and_lock_the_identity() {
    temp_env::async_with_vars([("RUSTFS_IAM_MASTER_KEY", Some("mfa-lockout-test-master-key"))], async {
        let temp_dir = tempfile::TempDir::with_prefix("rustfs_iam_mfa_lockout_test_").expect("temp directory must be created");
        let ecstore = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(temp_dir.path())
            .init_bucket_metadata(false)
            .build()
            .await
            .ecstore;

        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
        let enrollment = service::enroll(ecstore.clone(), ACCESS_KEY, None, now).await.expect("enroll");
        let secret = TotpSecret::from_base32(&enrollment.secret_base32).expect("server-issued secret");
        let issued = service::activate(ecstore.clone(), ACCESS_KEY, &secret.code_at(now.unix_timestamp() as u64), now)
            .await
            .expect("activate");

        // Four failures must persist without locking yet; a valid code clears them.
        for attempt in 1..=4 {
            let err = service::verify(ecstore.clone(), ACCESS_KEY, WRONG_RECOVERY_CODE, now)
                .await
                .expect_err("wrong code must be refused");
            assert!(matches!(err, MfaServiceError::InvalidCode), "attempt {attempt}: {err}");
        }
        let persisted = store::load(ecstore.clone(), ACCESS_KEY, now)
            .await
            .expect("load after 4 failures");
        assert_eq!(persisted.record.failed_attempts, 4);
        assert!(persisted.record.locked_until.is_none());

        let ok = service::verify(ecstore.clone(), ACCESS_KEY, &issued.recovery_codes[0], now)
            .await
            .expect("valid recovery code must succeed before threshold");
        assert_eq!(ok, MfaVerification::RecoveryCode { remaining: 9 });
        let after_ok = store::load(ecstore.clone(), ACCESS_KEY, now)
            .await
            .expect("load after success");
        assert_eq!(after_ok.record.failed_attempts, 0);

        // Five consecutive failures across verify/regenerate/disable must persist and engage lockout.
        for attempt in 1..=3 {
            let err = service::verify(ecstore.clone(), ACCESS_KEY, WRONG_RECOVERY_CODE, now)
                .await
                .expect_err("wrong code must be refused");
            assert!(matches!(err, MfaServiceError::InvalidCode), "verify attempt {attempt}: {err}");
        }
        let err = service::regenerate_recovery_codes(ecstore.clone(), ACCESS_KEY, WRONG_RECOVERY_CODE, now)
            .await
            .expect_err("wrong code on recovery-codes must be refused");
        assert!(matches!(err, MfaServiceError::InvalidCode), "regenerate attempt 4: {err}");
        let err = service::disable(ecstore.clone(), ACCESS_KEY, WRONG_RECOVERY_CODE, now)
            .await
            .expect_err("wrong code on disable must be refused");
        assert!(matches!(err, MfaServiceError::InvalidCode), "disable attempt 5: {err}");

        let locked = store::load(ecstore.clone(), ACCESS_KEY, now)
            .await
            .expect("load after 5 failures");
        assert_eq!(locked.record.failed_attempts, 5);
        assert!(locked.record.locked_until.is_some());

        // Lockout is checked before evaluating even a valid factor across all verifying operations.
        let err = service::verify(ecstore.clone(), ACCESS_KEY, &issued.recovery_codes[1], now)
            .await
            .expect_err("identity must be locked on verify after five failures");
        assert!(matches!(err, MfaServiceError::Locked { .. }), "expected Locked, got: {err}");

        let err = service::disable(ecstore.clone(), ACCESS_KEY, &issued.recovery_codes[1], now)
            .await
            .expect_err("disable must honour the persisted lockout");
        assert!(matches!(err, MfaServiceError::Locked { .. }), "expected Locked, got: {err}");

        let err = service::regenerate_recovery_codes(ecstore.clone(), ACCESS_KEY, &issued.recovery_codes[1], now)
            .await
            .expect_err("regenerate_recovery_codes must honour the persisted lockout");
        assert!(matches!(err, MfaServiceError::Locked { .. }), "expected Locked, got: {err}");

        // Once the lockout window expires, a valid recovery code succeeds and clears failures.
        let later = now + time::Duration::minutes(16);
        let verified = service::verify(ecstore.clone(), ACCESS_KEY, &issued.recovery_codes[1], later)
            .await
            .expect("valid recovery code must succeed after lockout expires");
        assert_eq!(verified, MfaVerification::RecoveryCode { remaining: 8 });
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn a_refused_code_does_not_create_a_record() {
    temp_env::async_with_vars([("RUSTFS_IAM_MASTER_KEY", Some("mfa-lockout-test-master-key"))], async {
        let temp_dir = tempfile::TempDir::with_prefix("rustfs_iam_mfa_lockout_test_").expect("temp directory must be created");
        let ecstore = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(temp_dir.path())
            .init_bucket_metadata(false)
            .build()
            .await
            .ecstore;

        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid timestamp");

        let err = service::verify(ecstore.clone(), "nobody", "123456", now)
            .await
            .expect_err("identity without a second factor cannot verify one");
        assert!(matches!(err, MfaServiceError::NotEnabled), "{err}");

        let loaded = store::load(ecstore, "nobody", now).await.expect("load");
        assert!(loaded.is_new(), "a refused verification must not create a record");
    })
    .await;
}
