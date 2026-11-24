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

use super::{IAMAuth, iam_lookup_error_to_s3_error, legacy_s3_code_name};
use crate::app::gating_test_env::isolated_multi_pool_ecstore;
use crate::runtime_sources::{AppContext, IamInterface, KmsInterface, ServerContextSlot};
use rustfs_credentials::Credentials;
use rustfs_iam::cache::Cache;
use rustfs_iam::error::Error as IamError;
use rustfs_iam::manager::IamCache;
use rustfs_iam::store::object::ObjectStore;
use rustfs_iam::sys::IamSys;
use rustfs_keystone::KEYSTONE_CREDENTIALS;
use rustfs_kms::KmsServiceManager;
use std::mem::{size_of, size_of_val};
use std::sync::Arc;
use tracing::instrument::WithSubscriber;

struct ContextIam {
    handle: Arc<IamSys<ObjectStore>>,
}

impl IamInterface for ContextIam {
    fn handle(&self) -> Arc<IamSys<ObjectStore>> {
        self.handle.clone()
    }

    fn is_ready(&self) -> bool {
        true
    }
}

struct TestKms;

impl KmsInterface for TestKms {
    fn handle(&self) -> Arc<KmsServiceManager> {
        Arc::new(KmsServiceManager::new())
    }
}

#[tokio::test]
async fn keystone_secret_lookup_requires_validated_token_context() {
    let auth = IAMAuth::new("root", "root-secret");
    for key in ["keystone:x", "keystone:admin"] {
        assert_eq!(legacy_s3_code_name(&auth.get_secret_key(key).await.unwrap_err()), "InvalidAccessKeyId");
        KEYSTONE_CREDENTIALS
            .scope(None, async {
                assert_eq!(legacy_s3_code_name(&auth.get_secret_key(key).await.unwrap_err()), "InvalidAccessKeyId");
            })
            .await;
    }
    let credentials = Credentials {
        access_key: "keystone:validated".to_owned(),
        ..Default::default()
    };
    KEYSTONE_CREDENTIALS
        .scope(Some(credentials), async {
            assert_eq!(
                auth.get_secret_key("keystone:validated")
                    .await
                    .expect("validated Keystone token")
                    .expose(),
                ""
            );
        })
        .await;
    assert_eq!(
        auth.get_secret_key("root").await.expect("regular root credential").expose(),
        "root-secret"
    );
}

#[test]
fn test_iam_auth_creation() {
    let access_key = "test-access-key";
    let iam_auth = IAMAuth::new(access_key, "test-secret-key");

    assert_eq!(size_of_val(&iam_auth), size_of::<IAMAuth>());
}

#[tokio::test]
async fn test_iam_auth_clone_preserves_bootstrap_secret() {
    let iam_auth = IAMAuth::new("test-ak", "test-sk");
    let cloned = iam_auth.clone();

    let secret = cloned.get_secret_key("test-ak").await.expect("cloned root credentials");
    assert_eq!(secret.expose(), "test-sk");
}

#[tokio::test]
async fn test_iam_auth_get_secret_key_empty_access_key() {
    let iam_auth = IAMAuth::new("test-ak", "test-sk");

    let result = iam_auth.get_secret_key("").await;

    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(legacy_s3_code_name(&error), "UnauthorizedAccess");
    assert!(error.message().unwrap_or("").contains("Your account is not signed up"));
}

#[tokio::test]
async fn credential_boundary_rejected_access_key_logs_omit_key_fragments() {
    let (_temp_dir, _disk_paths, store) = isolated_multi_pool_ecstore().await;
    let (send_chan, _receiver) = tokio::sync::mpsc::channel(1);
    let cache = Arc::new(IamCache {
        cache: Cache::default(),
        api: ObjectStore::new(store.clone()),
        state: Default::default(),
        loading: Default::default(),
        roles: std::collections::HashMap::new(),
        send_chan,
        last_timestamp: Default::default(),
        sync_failures: Default::default(),
        sync_successes: Default::default(),
        last_sync_duration_millis: Default::default(),
    });
    let slot = ServerContextSlot::new();
    let context = AppContext::new(
        store,
        Arc::new(ContextIam {
            handle: Arc::new(IamSys::new(cache)),
        }),
        Arc::new(TestKms),
    );
    assert!(slot.install(Arc::new(context)));
    for (slot, reason) in [(slot, "no_such_user"), (ServerContextSlot::new(), "iam_not_initialized")] {
        let auth = IAMAuth::with_server_context("bootstrap-key", "bootstrap-secret", slot);
        let log = tempfile::NamedTempFile::new().expect("capture file");
        let writer = log.reopen().expect("capture writer");
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .without_time()
            .with_writer(move || writer.try_clone().expect("clone capture writer"))
            .finish();
        let error = auth
            .get_secret_key("LEAK-private-access-key-TAIL")
            .with_subscriber(subscriber)
            .await
            .expect_err("unknown credentials must be rejected");
        assert_eq!(legacy_s3_code_name(&error), "InvalidAccessKeyId");
        let captured = std::fs::read_to_string(log.path()).expect("read captured logs");
        assert!(captured.contains(reason), "capture the expected rejection: {captured}");
        for forbidden in ["LEAK", "TAIL", "private-access-key", "access_key="] {
            assert!(!captured.contains(forbidden), "rejected access-key material must not be logged");
        }
    }
}

#[test]
fn test_iam_lookup_error_maps_to_internal_error() {
    let result = iam_lookup_error_to_s3_error(&IamError::Io(std::io::Error::other("load user failed")));

    assert_eq!(legacy_s3_code_name(&result), "InternalError");
    assert_eq!(result.message(), Some("IAM user lookup failed"));
}
