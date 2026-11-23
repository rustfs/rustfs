// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use crate::testing::RecordingKmsKeyAuthorizer;
use crate::*;
use async_trait::async_trait;
use rustfs_kms::audit::{KmsAuditOperation, KmsAuditRecord, KmsAuditSink};
use rustfs_kms::service::ObjectEncryptionService;
use rustfs_kms::types::OperationContext;
use rustfs_kms::{KmsConfig, KmsServiceManager, KmsServiceStatus};
use rustfs_s3_types::S3ErrorCode;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

struct BoundHost {
    manager: Option<Arc<KmsServiceManager>>,
    service: Option<Arc<ObjectEncryptionService>>,
    unavailable: bool,
    local: Arc<dyn SseDekProvider>,
    authorizer: Arc<RecordingKmsKeyAuthorizer>,
}

impl BoundHost {
    async fn capture(manager: Option<Arc<KmsServiceManager>>, allowed: bool) -> Arc<Self> {
        let service = match &manager {
            Some(manager) => manager.get_encryption_service().await,
            None => None,
        };
        let unavailable = match &manager {
            Some(manager) if service.is_none() => manager.get_status().await != KmsServiceStatus::NotConfigured,
            _ => false,
        };
        Arc::new(Self {
            manager,
            service,
            unavailable,
            local: Arc::new(LocalSseDekProvider::new_with_key([0x71; 32])),
            authorizer: RecordingKmsKeyAuthorizer::new(allowed),
        })
    }

    fn principal(self: &Arc<Self>, context: OperationContext) -> SseKmsPrincipal {
        let request_id = context.operation_id.to_string();
        SseKmsPrincipal::new(context.principal.clone(), None, false, HashMap::new(), HashMap::new())
            .with_managed_operation(&request_id, context, self.clone())
            .with_test_hooks(Some(TestAuthorizationHooks {
                enforced: false,
                authorizer: RecordingKmsKeyAuthorizer::new(true),
            }))
    }
}

#[async_trait]
impl SseHost for BoundHost {
    async fn encryption_service(&self) -> Option<Arc<ObjectEncryptionService>> {
        self.service.clone()
    }
    fn kms_service_manager(&self) -> Option<Arc<KmsServiceManager>> {
        self.manager.clone()
    }
    async fn kms_configured_but_unavailable(&self) -> bool {
        self.unavailable
    }
    fn local_sse_dek_provider(&self) -> Result<Arc<dyn SseDekProvider>, SseError> {
        Ok(self.local.clone())
    }
    async fn bucket_encryption(&self, _: &str) -> Result<Option<BucketEncryption>, SseError> {
        Ok(None)
    }
    fn kms_key_authorizer(&self) -> &dyn KmsKeyAuthorizer {
        self.authorizer.as_ref()
    }
}

fn write<'a>(principal: &'a SseKmsPrincipal, algorithm: &str) -> EncryptionRequest<'a> {
    EncryptionRequest {
        bucket: "bucket",
        key: "object",
        server_side_encryption: Some(algorithm.to_owned()),
        ssekms_key_id: (algorithm == "aws:kms").then(|| "maintenance-key".to_owned()),
        ssekms_context: None,
        sse_customer_algorithm: None,
        sse_customer_key: None,
        sse_customer_key_md5: None,
        content_size: 32,
        principal: Some(principal),
    }
}

fn read<'a>(principal: &'a SseKmsPrincipal, metadata: &'a HashMap<String, String>) -> DecryptionRequest<'a> {
    DecryptionRequest {
        bucket: "bucket",
        key: "object",
        metadata,
        sse_customer_key: None,
        sse_customer_key_md5: None,
        principal: Some(principal),
    }
}

async fn manager() -> Arc<KmsServiceManager> {
    let manager = Arc::new(KmsServiceManager::new());
    manager
        .reconfigure(KmsConfig::static_kms(
            "maintenance-key".to_owned(),
            base64_simd::STANDARD.encode_to_string([0x31; 32]),
        ))
        .await
        .expect("configure actual KMS service");
    manager
}

#[tokio::test]
async fn bound_operations_require_authorization_at_all_six_entry_points() {
    let _guard = crate::tests::lock_sse_test_state().await;
    let ambient = BoundHost::capture(None, true).await;
    let manager = manager().await;
    let writer = BoundHost::capture(Some(manager.clone()), true).await;
    let writer_principal = writer.principal(OperationContext::new("maintenance-account".to_owned()));
    let material = sse_encryption(writer.as_ref(), write(&writer_principal, "aws:kms"))
        .await
        .expect("create actual encrypted object before testing denials")
        .expect("encrypted material");
    let metadata = encryption_material_to_metadata(&material).expect("persist actual encryption metadata");
    let bound = BoundHost::capture(Some(manager), false).await;
    let principal = bound.principal(OperationContext::new("maintenance-account".to_owned()));
    let errors = [
        sse_encryption(ambient.as_ref(), write(&principal, "aws:kms")).await.err(),
        sse_prepare_encryption(
            ambient.as_ref(),
            PrepareEncryptionRequest {
                bucket: "bucket",
                key: "object",
                server_side_encryption: Some("aws:kms".to_owned()),
                ssekms_key_id: Some("maintenance-key".to_owned()),
                ssekms_context: None,
                sse_customer_algorithm: None,
                sse_customer_key: None,
                sse_customer_key_md5: None,
                principal: Some(&principal),
            },
        )
        .await
        .err(),
        sse_decryption(ambient.as_ref(), read(&principal, &metadata)).await.err(),
        classify_sse_read_response(ambient.as_ref(), read(&principal, &metadata))
            .await
            .err(),
        authorize_sse_kms_object_read(ambient.as_ref(), Some(&principal), &metadata)
            .await
            .err(),
        authorize_sse_kms_multipart_write(ambient.as_ref(), Some(&principal), &metadata)
            .await
            .err(),
    ];
    for (entry, error) in errors.into_iter().enumerate() {
        assert_eq!(
            error
                .unwrap_or_else(|| panic!("bound authorization must deny entry {entry}"))
                .code,
            S3ErrorCode::AccessDenied,
            "entry {entry}"
        );
    }
    assert_eq!(bound.authorizer.calls().len(), 6);
    assert!(bound.authorizer.calls().iter().all(|(_, key)| key == "maintenance-key"));
    assert!(ambient.authorizer.calls().is_empty());
}

struct ResetProviders;
impl Drop for ResetProviders {
    fn drop(&mut self) {
        reset_sse_dek_provider();
    }
}

#[tokio::test]
async fn bound_operations_ignore_global_provider_injection_for_kms_and_local_keys() {
    let _guard = crate::tests::lock_sse_test_state().await;
    reset_sse_dek_provider();
    let _reset = ResetProviders;
    let ambient = BoundHost::capture(None, true).await;
    let bound = BoundHost::capture(Some(manager().await), true).await;
    let principal = bound.principal(OperationContext::new("maintenance-account".to_owned()));
    let foreign = manager().await;
    foreign
        .reconfigure(KmsConfig::static_kms(
            "maintenance-key".to_owned(),
            base64_simd::STANDARD.encode_to_string([0x32; 32]),
        ))
        .await
        .expect("configure distinct foreign key material");
    let injected = crate::kms::kms_backed::KmsSseDekProvider::new_with_service_manager(foreign)
        .await
        .expect("construct actual foreign KMS provider");
    let injected: Arc<dyn SseDekProvider> = Arc::new(injected);
    set_sse_dek_provider_for_test(injected.clone());
    assert!(Arc::ptr_eq(
        &crate::kms::provider::kms_sse_dek_provider(ambient.as_ref())
            .await
            .expect("global KMS provider"),
        &injected
    ));
    assert!(Arc::ptr_eq(
        &crate::kms::provider::get_local_sse_dek_provider().expect("global local provider"),
        &injected
    ));

    let material = sse_encryption(ambient.as_ref(), write(&principal, "aws:kms"))
        .await
        .expect("bound KMS write")
        .expect("material");
    let metadata = encryption_material_to_metadata(&material).expect("KMS metadata");
    let restored = sse_decryption(ambient.as_ref(), read(&principal, &metadata))
        .await
        .expect("bound KMS read")
        .expect("material");
    assert_eq!(restored.key_bytes, material.key_bytes);

    let no_service = BoundHost::capture(None, true).await;
    let missing = no_service.principal(OperationContext::new("maintenance-account".to_owned()));
    assert!(
        sse_decryption(ambient.as_ref(), read(&missing, &metadata)).await.is_err(),
        "global provider cannot supply a missing instance"
    );
    let material = sse_encryption(ambient.as_ref(), write(&missing, "AES256"))
        .await
        .expect("bound local write")
        .expect("material");
    let metadata = encryption_material_to_metadata(&material).expect("local metadata");
    let restored = sse_decryption(ambient.as_ref(), read(&missing, &metadata))
        .await
        .expect("bound local read")
        .expect("material");
    assert_eq!(restored.key_bytes, material.key_bytes);
    let local_other = Arc::new(BoundHost {
        local: Arc::new(LocalSseDekProvider::new_with_key([0x72; 32])),
        manager: None,
        service: None,
        unavailable: false,
        authorizer: RecordingKmsKeyAuthorizer::new(true),
    });
    let other = local_other.principal(OperationContext::new("maintenance-account".to_owned()));
    assert!(
        sse_decryption(ambient.as_ref(), read(&other, &metadata)).await.is_err(),
        "local ciphertext stays with its runtime key"
    );
}

#[tokio::test]
async fn bound_unavailable_status_keeps_admission_snapshot_after_manager_changes() {
    let _guard = crate::tests::lock_sse_test_state().await;
    let ambient = BoundHost::capture(None, true).await;
    let manager = Arc::new(KmsServiceManager::new());
    let never_configured = BoundHost::capture(Some(manager.clone()), true).await;
    let first = never_configured.principal(OperationContext::new("maintenance-account".to_owned()));
    manager
        .reconfigure(KmsConfig::static_kms(
            "maintenance-key".to_owned(),
            base64_simd::STANDARD.encode_to_string([0x31; 32]),
        ))
        .await
        .expect("configure manager after admission");
    assert!(manager.get_encryption_service().await.is_some());
    assert_eq!(
        sse_encryption(ambient.as_ref(), write(&first, "aws:kms"))
            .await
            .expect_err("snapshot had no configured KMS")
            .code,
        S3ErrorCode::InvalidRequest
    );

    manager.stop().await.expect("stop configured manager");
    let stopped = BoundHost::capture(Some(manager.clone()), true).await;
    assert!(stopped.unavailable);
    let second = stopped.principal(OperationContext::new("maintenance-account".to_owned()));
    manager.start().await.expect("start manager after admission");
    assert!(manager.get_encryption_service().await.is_some());
    assert_eq!(
        sse_encryption(ambient.as_ref(), write(&second, "aws:kms"))
            .await
            .expect_err("snapshot had unavailable KMS")
            .code,
        S3ErrorCode::ServiceUnavailable
    );
}

#[derive(Default)]
struct AuditRecords(Mutex<Vec<KmsAuditRecord>>);
impl KmsAuditSink for AuditRecords {
    fn emit(&self, record: KmsAuditRecord) {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).push(record);
    }
}

#[tokio::test]
async fn bound_key_description_preserves_operation_context_and_audit_classification() {
    let _guard = crate::tests::lock_sse_test_state().await;
    let manager = Arc::new(KmsServiceManager::new());
    let records = Arc::new(AuditRecords::default());
    manager.set_audit_sink(records.clone());
    manager
        .reconfigure(KmsConfig::static_kms(
            "maintenance-key".to_owned(),
            base64_simd::STANDARD.encode_to_string([0x31; 32]),
        ))
        .await
        .expect("configure audited manager");
    let bound = BoundHost::capture(Some(manager), true).await;
    let ambient = BoundHost::capture(None, true).await;
    let mut context = OperationContext::new("maintenance-account".to_owned());
    context
        .additional_context
        .insert("worker_id".to_owned(), "worker-a".to_owned());
    let request_id = context.operation_id.to_string();
    let audit = KmsRequestAuditScope::register(&request_id);
    let principal = bound.principal(context.clone());
    let (bound_id, bound_context) = principal.managed_operation_context().expect("bound identity");
    assert_eq!(bound_id, request_id);
    assert_eq!(bound_context.operation_id, context.operation_id);
    let mut request = write(&principal, "aws:kms");
    request.ssekms_key_id = Some("missing-key".to_owned());
    let error = sse_encryption(ambient.as_ref(), request)
        .await
        .expect_err("DescribeKey rejects unknown key");
    assert_eq!(error.code, S3ErrorCode::Custom(KMS_KEY_NOT_FOUND_ERROR_CODE.into()));
    let records = records.0.lock().unwrap_or_else(PoisonError::into_inner);
    let described = records
        .iter()
        .find(|record| record.operation == KmsAuditOperation::DescribeKey)
        .expect("actual DescribeKey event");
    assert_eq!(described.operation_id, context.operation_id);
    assert_eq!(described.principal, context.principal);
    assert_eq!(described.context.get("worker_id").map(String::as_str), Some("worker-a"));
    assert!(
        audit
            .audit_tags()
            .iter()
            .any(|(key, value)| *key == "kmsErrorClass" && value.as_str() == Some("key_not_found"))
    );
}
