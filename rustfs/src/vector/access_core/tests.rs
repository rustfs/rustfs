// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Unit coverage for Vector actions, resource facts, binding, and public-access rules.

use super::test_support::{REGION, ResourceFixture, request_for};
use super::*;
use crate::{storage_api::vector::ManagedTableId, table_catalog::CatalogFormat};
use serde_json::{Value, json};
use uuid::Uuid;

#[test]
fn vector_action_plan_covers_all_21_actions_and_additional_combinations() {
    for operation in Operation::ALL {
        assert_eq!(<&str>::from(primary_action(operation)), format!("s3vectors:{}", operation.name()));
        assert_eq!(required_actions(&request_for(operation)), vec![primary_action(operation)]);
    }
    for filter in [None, Some(Value::Null), Some(json!({"project":"search"}))] {
        for metadata in [None, Some(false), Some(true)] {
            for distance in [None, Some(false), Some(true)] {
                let Request::QueryVectors(mut input) = request_for(Operation::QueryVectors) else { unreachable!() };
                input.filter = filter.clone();
                input.return_metadata = metadata;
                input.return_distance = distance;
                let additional = filter.is_some() || metadata == Some(true);
                let actions = required_actions(&Request::QueryVectors(input));
                assert_eq!(actions.contains(&S3VectorsAction::GetVectorsAction), additional);
                assert_eq!(actions.len(), if additional { 2 } else { 1 });
            }
        }
    }
    for data in [None, Some(false), Some(true)] {
        for metadata in [None, Some(false), Some(true)] {
            let Request::ListVectors(mut input) = request_for(Operation::ListVectors) else { unreachable!() };
            input.return_data = data;
            input.return_metadata = metadata;
            assert_eq!(
                required_actions(&Request::ListVectors(input)).len(),
                if data == Some(true) || metadata == Some(true) { 2 } else { 1 }
            );
        }
    }
    for tags in [
        None,
        Some(BTreeMap::new()),
        Some(BTreeMap::from([("project".into(), "search".into())])),
    ] {
        let Request::CreateIndex(mut index) = request_for(Operation::CreateIndex) else { unreachable!() };
        let Request::CreateVectorBucket(mut bucket) = request_for(Operation::CreateVectorBucket) else {
            unreachable!()
        };
        index.tags = tags.clone();
        bucket.tags = tags.clone();
        for request in [Request::CreateIndex(index), Request::CreateVectorBucket(bucket)] {
            assert_eq!(required_actions(&request).contains(&S3VectorsAction::TagResourceAction), tags.is_some());
        }
    }
}

#[test]
fn vector_condition_sources_keep_current_tags_separate_from_request_and_effective_encryption() {
    let fixture = ResourceFixture::new();
    let request = request_for(Operation::GetVectors);
    let spoofed = HashMap::from([
        ("aws:RequestedRegion".into(), vec!["other-region".into()]),
        ("resourcetag/project".into(), vec!["injected".into()]),
        ("RequestTag/project".into(), vec!["injected".into()]),
        ("s3vectors:sseType".into(), vec!["aws:kms".into()]),
    ]);
    let actual = vector_conditions(spoofed, &request, &fixture.target(request.operation()), REGION).unwrap();
    assert_eq!(actual["RequestedRegion"], [REGION]);
    assert_eq!(actual["ResourceTag/project"], ["search"]);
    assert_eq!(actual["VectorBucketTag/department"], ["research"]);
    assert!(!actual.contains_key("RequestTag/project"));
    assert!(!actual.contains_key("sseType"));
    assert!(!actual.contains_key("resourcetag/project"));
    for config in [
        None,
        Some(dto::EncryptionConfiguration {
            sse_type: None,
            kms_key_arn: None,
        }),
    ] {
        let Request::CreateIndex(mut input) = request_for(Operation::CreateIndex) else { unreachable!() };
        input.encryption_configuration = config.clone();
        input.tags = Some(BTreeMap::from([("project".into(), "new".into())]));
        let actual = vector_conditions(
            HashMap::new(),
            &Request::CreateIndex(input),
            &fixture.target(Operation::CreateIndex),
            REGION,
        )
        .unwrap();
        assert_eq!(actual.get("sseType"), config.as_ref().map(|_| vec!["AES256".to_owned()]).as_ref());
        assert!(!actual.contains_key("kmsKeyArn"));
        assert!(!actual.contains_key("ResourceTag/project"));
        assert_eq!(actual["RequestTag/project"], ["new"]);
        assert_eq!(actual["VectorBucketTag/department"], ["research"]);
    }
}

#[test]
fn vector_binding_refuses_other_region_account_request_and_recreated_identities() {
    let fixture = ResourceFixture::new();
    let request = request_for(Operation::GetVectors);
    let binding = BoundVectorRequest::new(request.clone(), Some(fixture.resource.clone()));
    binding.require_same(&request, Some(&fixture.resource)).unwrap();
    let replacement = ResourceFixture::new();
    assert!(binding.require_same(&request, Some(&replacement.resource)).is_err());
    let another_index = CatalogResourceIdentity::try_new(
        fixture.bucket.handle.clone(),
        ManagedTableId::try_new(Uuid::new_v4().to_string()).unwrap(),
        CatalogFormat::Lance,
    )
    .unwrap();
    assert!(binding.require_same(&request, Some(&another_index)).is_err());
    let Request::GetVectors(mut input) = request.clone() else { unreachable!() };
    input.return_metadata = Some(true);
    assert!(
        binding
            .require_same(&Request::GetVectors(input), Some(&fixture.resource))
            .is_err()
    );
    for arn in [
        "arn:aws:s3vectors:us-east-1:123456789012:bucket/s6-vector-bucket/index/s6-index",
        "arn:aws:s3vectors:us-west-2::bucket/s6-vector-bucket/index/s6-index",
    ] {
        let Request::GetVectors(mut input) = request.clone() else { unreachable!() };
        input.vector_bucket_name = None;
        input.index_name = None;
        input.index_arn = Some(arn.into());
        assert!(validate_target(&Request::GetVectors(input), &fixture.target(Operation::GetVectors), REGION).is_err());
    }
}

#[tokio::test]
async fn vector_bpa_and_bucket_deny_cover_all_21_actions_including_owner_policy_operations() {
    let fixture = ResourceFixture::new();
    let groups = None;
    for operation in Operation::ALL {
        for principal in [json!("*"), json!({"AWS":"*"}), json!({"AWS":["user", "*"]})] {
            let policy: BucketPolicy = serde_json::from_slice(
                &serde_json::to_vec(&json!({
                    "Version":"2012-10-17", "Statement":[{"Effect":"Allow", "Principal":principal,
                    "Action":format!("s3vectors:{}",operation.name()),"Resource":"*"}]
                }))
                .unwrap(),
            )
            .unwrap();
            assert!(matches!(validate_private_bucket_policy(&policy), Err(VectorAccessError::Denied)));
        }
        let mut policy: BucketPolicy = serde_json::from_slice(
            &serde_json::to_vec(&json!({
                "Version":"2012-10-17", "Statement":[{"Effect":"Deny", "Principal":"*",
                "Action":format!("s3vectors:{}",operation.name()),"Resource":"*"}]
            }))
            .unwrap(),
        )
        .unwrap();
        validate_private_bucket_policy(&policy).unwrap();
        let request = request_for(operation);
        let target = fixture.target(operation);
        let conditions = vector_conditions(HashMap::new(), &request, &target, REGION).unwrap();
        let (bucket, object) = target.names();
        assert!(
            bucket_policy_has_deny(
                &policy,
                &BucketPolicyArgs {
                    account: "owner",
                    groups: &groups,
                    action: Action::S3VectorsAction(primary_action(operation)),
                    bucket,
                    object,
                    conditions: &conditions,
                    is_owner: true,
                }
            )
            .await
        );
        policy.statements[0].effect = Effect::Allow;
        policy.statements[0].principal = serde_json::from_str(r#"{"AWS":"specific-user"}"#).unwrap();
        validate_private_bucket_policy(&policy).unwrap();
    }
}
