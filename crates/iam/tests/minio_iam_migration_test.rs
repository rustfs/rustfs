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

mod ecstore_test_compat;

use ecstore_test_compat::fixture::try_migrate_iam_config;
use rustfs_credentials::{get_global_action_cred, init_global_action_credentials};
use rustfs_iam::store::object::{
    IAM_CONFIG_POLICY_DB_SERVICE_ACCOUNTS_PREFIX, IAM_CONFIG_POLICY_DB_USERS_PREFIX, IAM_CONFIG_SERVICE_ACCOUNTS_PREFIX,
    IAM_CONFIG_USERS_PREFIX, ObjectStore,
};
use rustfs_iam::store::{Store, UserType};
use rustfs_iam::utils::generate_jwt;
use rustfs_policy::auth::UserIdentity;
use serde_json::{Value, json};
use std::collections::HashMap;

const LEGACY_META_BUCKET: &str = ".minio.sys";
const REGULAR_USER: &str = "minio-user";
const SERVICE_ACCOUNT: &str = "minio-service-account";

async fn seed_legacy_iam_object(env: &rustfs_test_utils::TestECStoreEnv, path: &str, value: &Value) {
    env.put_object_bytes(
        LEGACY_META_BUCKET,
        path,
        serde_json::to_vec(value).expect("legacy IAM object must serialize"),
    )
    .await;
}

fn assert_identity_fields(actual: &UserIdentity, expected: &Value) {
    assert_eq!(
        serde_json::to_value(actual).expect("loaded identity must serialize"),
        *expected,
        "migration must preserve every credential field except expiration",
    );
}

async fn assert_identity_survives(
    store: &ObjectStore,
    identity_path: &str,
    name: &str,
    user_type: UserType,
    source: &Value,
    expected_policy: &Value,
) {
    let mut expected = source.clone();
    expected["credentials"]["expiration"] = Value::Null;

    let persisted: UserIdentity = store
        .load_iam_config(identity_path)
        .await
        .expect("migrated identity must be persisted");
    assert_identity_fields(&persisted, &expected);

    for _ in 0..2 {
        let actual = store
            .load_user_identity(name, user_type)
            .await
            .expect("migrated permanent identity must remain loadable");
        assert_identity_fields(&actual, &expected);
    }

    let mut mappings = HashMap::new();
    store
        .load_mapped_policy(name, user_type, false, &mut mappings)
        .await
        .expect("loading the identity must not delete its policy mapping");
    let actual_policy = mappings.get(name).expect("migrated policy mapping must exist");
    assert_eq!(
        serde_json::to_value(actual_policy).expect("loaded policy mapping must serialize"),
        *expected_policy,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn minio_permanent_identities_survive_migration_and_repeated_iam_loads() {
    if get_global_action_cred().is_none() {
        init_global_action_credentials(Some("MINIOMIGRATIONROOT".to_string()), Some("minio-migration-root-secret".to_string()))
            .expect("root credentials must initialize for JWT validation");
    }

    let temp_dir = tempfile::TempDir::with_prefix("rustfs_minio_iam_migration_").expect("temp directory must be created");
    let env = rustfs_test_utils::TestECStoreEnv::builder()
        .base_dir(temp_dir.path())
        .init_bucket_metadata(false)
        .build()
        .await;
    for disk_path in &env.disk_paths {
        tokio::fs::create_dir_all(disk_path.join(LEGACY_META_BUCKET))
            .await
            .expect("legacy metadata volume must be created");
    }

    let regular_source = json!({
        "version": 1,
        "credentials": {
            "accessKey": REGULAR_USER,
            "secretKey": "regular-user-secret",
            "sessionToken": "",
            "expiration": "0001-01-01T00:00:00Z",
            "status": "on",
            "parentUser": "regular-parent",
            "groups": ["engineering", "operations"],
            "claims": {"tenant": "alpha"},
            "name": "MinIO regular user",
            "description": "migrated regular identity"
        },
        "updatedAt": "2025-03-07T12:00:00Z"
    });
    let service_claims = json!({"sa-policy": "inherited-policy", "tenant": "alpha"});
    let service_secret = "service-account-secret";
    let service_source = json!({
        "version": 1,
        "credentials": {
            "accessKey": SERVICE_ACCOUNT,
            "secretKey": service_secret,
            "sessionToken": generate_jwt(&service_claims, service_secret).expect("service-account JWT must be generated"),
            "expiration": "1970-01-01T00:00:00Z",
            "status": "on",
            "parentUser": REGULAR_USER,
            "groups": ["service-accounts"],
            "claims": service_claims,
            "name": "MinIO service account",
            "description": "migrated service identity"
        },
        "updatedAt": "2025-03-07T12:00:00Z"
    });
    let regular_policy_source = json!({"version": 1, "policy": "readwrite", "updatedAt": "2025-03-07T12:00:00Z"});
    let service_policy_source = json!({"version": 1, "policy": "readonly", "updatedAt": "2025-03-07T12:00:00Z"});

    let regular_identity_path = format!("{}{REGULAR_USER}/identity.json", IAM_CONFIG_USERS_PREFIX.as_str());
    let service_identity_path = format!("{}{SERVICE_ACCOUNT}/identity.json", IAM_CONFIG_SERVICE_ACCOUNTS_PREFIX.as_str());

    seed_legacy_iam_object(&env, &regular_identity_path, &regular_source).await;
    seed_legacy_iam_object(&env, &service_identity_path, &service_source).await;
    seed_legacy_iam_object(
        &env,
        &format!("{}{REGULAR_USER}.json", IAM_CONFIG_POLICY_DB_USERS_PREFIX.as_str()),
        &regular_policy_source,
    )
    .await;
    seed_legacy_iam_object(
        &env,
        &format!("{}{SERVICE_ACCOUNT}.json", IAM_CONFIG_POLICY_DB_SERVICE_ACCOUNTS_PREFIX.as_str()),
        &service_policy_source,
    )
    .await;

    try_migrate_iam_config(env.ecstore.clone(), None).await;

    let store = ObjectStore::new(env.ecstore);
    assert_identity_survives(
        &store,
        &regular_identity_path,
        REGULAR_USER,
        UserType::Reg,
        &regular_source,
        &regular_policy_source,
    )
    .await;
    assert_identity_survives(
        &store,
        &service_identity_path,
        SERVICE_ACCOUNT,
        UserType::Svc,
        &service_source,
        &service_policy_source,
    )
    .await;
}
