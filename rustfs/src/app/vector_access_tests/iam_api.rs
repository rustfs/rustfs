// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Real ObjectStore coverage for checked Vector IAM/resource-policy evaluation.
//! Uses real IAM persistence and verified derived credentials from the shared four-disk fixture.

use super::*;

fn identity_policy(effect: Option<&str>) -> Policy {
    let statement = effect
        .map(|effect| json!({"Effect":effect,"Action":"s3vectors:*","Resource":"*"}))
        .unwrap_or_else(|| json!({"Effect":"Allow","Action":"s3:ListBucket","Resource":"*"}));
    Policy::parse_config(
        &serde_json::to_vec(&json!({
            "Version":"2012-10-17","Statement":vec![statement]
        }))
        .unwrap(),
    )
    .unwrap()
}

fn resource_policy(account: &str, effect: Option<&str>) -> Option<BucketPolicy> {
    effect.map(|effect| {
        serde_json::from_slice(
            &serde_json::to_vec(&json!({
                "Version":"2012-10-17","Statement":[{"Effect":effect,"Principal":{"AWS":account},
                "Action":"s3vectors:*","Resource":"*"}]
            }))
            .unwrap(),
        )
        .unwrap()
    })
}

async fn evaluate_api(
    iam: &IamSys<ObjectStore>,
    credentials: &Credentials,
    fixture: &ResourceFixture,
    operation: Operation,
    policy: Option<&BucketPolicy>,
) -> rustfs_iam::error::Result<bool> {
    let request = request_for(operation);
    let target = fixture.target(operation);
    let conditions = vector_conditions(HashMap::new(), &request, &target, REGION).unwrap();
    let (bucket, object) = target_names(&target);
    let empty = HashMap::new();
    iam.is_allowed_with_resource_policy(
        &Args {
            account: &credentials.access_key,
            groups: &credentials.groups,
            action: Action::S3VectorsAction(primary_action(operation)),
            bucket,
            object,
            conditions: &conditions,
            is_owner: false,
            claims: credentials.claims.as_ref().unwrap_or(&empty),
            deny_only: false,
        },
        policy,
    )
    .await
}

#[test]
#[serial_test::serial]
fn vector_resource_policy_api_real_iam_identity_resource_matrix() {
    run_large_stack_test("vector-resource-policy-matrix", || async {
        let (app, credentials, policy_name) = install_real_iam_user(allow_all(), false).await;
        let iam = app.iam().handle();
        let fixture = ResourceFixture::new();
        for (identity_effect, resource_effect, expected) in [
            (Some("Allow"), None, true),
            (None, Some("Allow"), true),
            (Some("Allow"), Some("Allow"), true),
            (None, None, false),
            (Some("Deny"), Some("Allow"), false),
            (Some("Deny"), None, false),
            (Some("Allow"), Some("Deny"), false),
            (None, Some("Deny"), false),
        ] {
            iam.set_policy(&policy_name, identity_policy(identity_effect)).await.unwrap();
            let policy = resource_policy(&credentials.access_key, resource_effect);
            for operation in Operation::ALL {
                // Creation and service-level calls have no existing bucket policy.
                if matches!(operation, Operation::CreateVectorBucket | Operation::ListVectorBuckets) && policy.is_some() {
                    continue;
                }
                assert_eq!(
                    evaluate_api(iam.as_ref(), &credentials, &fixture, operation, policy.as_ref())
                        .await
                        .unwrap(),
                    expected,
                    "{} I={identity_effect:?} R={resource_effect:?}",
                    operation.name()
                );
            }
        }
        // Empty attachment differs from a named empty policy and remains a valid implicit deny.
        iam.policy_db_set(&credentials.access_key, UserType::Reg, false, "")
            .await
            .unwrap();
        let policy = resource_policy(&credentials.access_key, Some("Allow"));
        assert!(
            evaluate_api(iam.as_ref(), &credentials, &fixture, Operation::GetVectors, policy.as_ref())
                .await
                .unwrap()
        );
    });
}

async fn issue_real_session(app: &AppContext, parent: &Credentials, policy: Option<Value>) -> Credentials {
    let root = app.action_credentials().get().unwrap();
    let mut claims = HashMap::from([
        ("parent".into(), json!(parent.access_key)),
        (
            "exp".into(),
            json!((OffsetDateTime::now_utc() + time::Duration::hours(1)).unix_timestamp()),
        ),
    ]);
    if let Some(policy) = policy {
        claims.insert(
            SESSION_POLICY_NAME.into(),
            json!(base64_simd::URL_SAFE_NO_PAD.encode_to_string(serde_json::to_vec(&policy).unwrap())),
        );
    }
    let secret = rustfs_iam::manager::get_token_signing_key().unwrap_or_else(|| root.secret_key.clone());
    let mut credentials = get_new_credentials_with_metadata(&claims, &secret).unwrap();
    credentials.parent_user = parent.access_key.clone();
    app.iam()
        .handle()
        .set_temp_user_from_parent(&credentials, parent)
        .await
        .unwrap();
    crate::auth::check_key_valid_with_context(&credentials.session_token, &credentials.access_key, Some(app))
        .await
        .unwrap()
        .0
}

#[test]
#[serial_test::serial]
fn vector_resource_policy_api_real_sts_full_session_intersection() {
    run_large_stack_test("vector-resource-policy-sts", || async {
        let (app, parent, policy_name) = install_real_iam_user(allow_all(), false).await;
        let iam = app.iam().handle();
        let fixture = ResourceFixture::new();
        for parent_allowed in [true, false] {
            iam.set_policy(&policy_name, identity_policy(parent_allowed.then_some("Allow")))
                .await
                .unwrap();
            for (session_policy, expected) in [
                (None, true),
                (
                    Some(
                        json!({"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3vectors:GetVectors","Resource":"*"}]}),
                    ),
                    true,
                ),
                (
                    Some(
                        json!({"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"s3vectors:GetVectors","Resource":"*"}]}),
                    ),
                    false,
                ),
                (
                    Some(
                        json!({"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3vectors:GetIndex","Resource":"*"}]}),
                    ),
                    false,
                ),
            ] {
                let session = issue_real_session(&app, &parent, session_policy).await;
                let bucket_allow = resource_policy(&session.access_key, Some("Allow"));
                assert_eq!(
                    evaluate_api(iam.as_ref(), &session, &fixture, Operation::GetVectors, bucket_allow.as_ref())
                        .await
                        .unwrap(),
                    expected,
                    "parent_allowed={parent_allowed}"
                );
                iam.delete_temp_account(&session.access_key, false).await.unwrap();
            }
        }
    });
}

#[test]
#[serial_test::serial]
fn vector_resource_policy_api_real_service_account_session_intersection() {
    run_large_stack_test("vector-resource-policy-service-account", || async {
        let (app, parent, policy_name) = install_real_iam_user(allow_all(), false).await;
        let iam = app.iam().handle();
        let fixture = ResourceFixture::new();
        for (session_policy,expected) in [
            (None,true),
            (Some(Policy::parse_config(br#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3vectors:GetVectors","Resource":"*"}]}"#).unwrap()),true),
            (Some(Policy::parse_config(br#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3vectors:GetIndex","Resource":"*"}]}"#).unwrap()),false),
            (Some(Policy::parse_config(br#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"s3vectors:GetVectors","Resource":"*"}]}"#).unwrap()),false),
        ] {
            let (created,_) = iam.new_service_account(&parent.access_key,None,rustfs_iam::sys::NewServiceAccountOpts {
                session_policy,..Default::default()
            }).await.unwrap();
            let (verified,owner) = crate::auth::check_key_valid_with_context("",&created.access_key,Some(&app)).await.unwrap();
            assert!(!owner);
            let policy = resource_policy(&verified.access_key,Some("Allow"));
            assert_eq!(evaluate_api(iam.as_ref(),&verified,&fixture,Operation::GetVectors,policy.as_ref()).await.unwrap(),expected);
            iam.set_policy(&policy_name,identity_policy(Some("Deny"))).await.unwrap();
            assert!(!evaluate_api(iam.as_ref(),&verified,&fixture,Operation::GetVectors,policy.as_ref()).await.unwrap());
            iam.set_policy(&policy_name,identity_policy(Some("Allow"))).await.unwrap();
        }
    });
}
