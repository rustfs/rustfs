// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::table_catalog::VectorEffectiveEncryptionV1;
use rustfs_gateway_types::persistence::{
    PersistedBucketEncryptionConfiguration, PersistedBucketEncryptionRule, PersistedEncryptionByDefault,
};
use rustfs_kms::{
    KmsError,
    types::{DescribeKeyRequest, KeyState, KeyUsage, OperationContext},
};
use rustfs_policy::policy::{
    Args,
    action::{Action, KmsAction},
};

pub(super) fn requested(
    input: Option<&dto::EncryptionConfiguration>,
    inherited: Option<&VectorEffectiveEncryptionV1>,
) -> Result<VectorEffectiveEncryptionV1, ProtocolError> {
    let Some(input) = input else {
        return Ok(inherited.cloned().unwrap_or(VectorEffectiveEncryptionV1::Unencrypted));
    };
    match input.sse_type.unwrap_or(dto::SseType::Aes256) {
        dto::SseType::Aes256 if input.kms_key_arn.is_none() => Ok(VectorEffectiveEncryptionV1::Aes256),
        dto::SseType::Aes256 => Err(ProtocolError::validation("kmsKeyArn", "AES256 cannot specify a KMS key")),
        dto::SseType::AwsKms => {
            let key = input
                .kms_key_arn
                .as_ref()
                .filter(|key| !key.is_empty())
                .ok_or_else(|| ProtocolError::validation("kmsKeyArn", "an explicit KMS key is required"))?;
            // The existing KMS provider resolves the complete identifier supplied by the caller.
            Ok(VectorEffectiveEncryptionV1::AwsKms {
                key_id: key.clone(),
                kms_key_arn: key.clone(),
            })
        }
    }
}

pub(super) fn stored(metadata: &BucketMetadata) -> Result<VectorEffectiveEncryptionV1, ProtocolError> {
    let Some(config) = &metadata.sse_config else {
        return if metadata.encryption_config_xml.is_empty() {
            Ok(VectorEffectiveEncryptionV1::Unencrypted)
        } else {
            Err(unavailable())
        };
    };
    let [rule] = config.rules.as_slice() else {
        return Err(unavailable());
    };
    if rule.bucket_key_enabled == Some(true) || rule.blocked_encryption_types.is_some() {
        return Err(unavailable());
    }
    let default = rule
        .apply_server_side_encryption_by_default
        .as_ref()
        .ok_or_else(unavailable)?;
    match default.sse_algorithm.as_str() {
        "AES256" if default.kms_master_key_id.is_none() => Ok(VectorEffectiveEncryptionV1::Aes256),
        "aws:kms" => {
            let key = default
                .kms_master_key_id
                .as_ref()
                .filter(|key| !key.is_empty())
                .ok_or_else(unavailable)?;
            Ok(VectorEffectiveEncryptionV1::AwsKms {
                key_id: key.clone(),
                kms_key_arn: key.clone(),
            })
        }
        _ => Err(unavailable()),
    }
}

pub(super) fn public_configuration(value: &VectorEffectiveEncryptionV1) -> Option<dto::EncryptionConfiguration> {
    match value {
        VectorEffectiveEncryptionV1::Unencrypted => None,
        VectorEffectiveEncryptionV1::Aes256 => Some(dto::EncryptionConfiguration {
            sse_type: Some(dto::SseType::Aes256),
            kms_key_arn: None,
        }),
        VectorEffectiveEncryptionV1::AwsKms { kms_key_arn, .. } => Some(dto::EncryptionConfiguration {
            sse_type: Some(dto::SseType::AwsKms),
            kms_key_arn: Some(kms_key_arn.clone()),
        }),
    }
}

pub(super) fn configuration_xml(value: &VectorEffectiveEncryptionV1) -> Result<Vec<u8>, ProtocolError> {
    let (algorithm, key) = match value {
        VectorEffectiveEncryptionV1::Unencrypted => return Ok(Vec::new()),
        VectorEffectiveEncryptionV1::Aes256 => ("AES256", None),
        VectorEffectiveEncryptionV1::AwsKms { key_id, .. } => ("aws:kms", Some(key_id.clone())),
    };
    crate::app::storage_api::vector_usecase::serialize(&PersistedBucketEncryptionConfiguration {
        rules: vec![PersistedBucketEncryptionRule {
            apply_server_side_encryption_by_default: Some(PersistedEncryptionByDefault {
                sse_algorithm: algorithm.into(),
                kms_master_key_id: key,
            }),
            ..Default::default()
        }],
    })
    .map_err(|_| unavailable())
}

pub(super) async fn preflight(
    server: &ServerContextSlot,
    envelope: &RequestEnvelope,
    encryption: &VectorEffectiveEncryptionV1,
    writes: bool,
) -> Result<(), ProtocolError> {
    if matches!(encryption, VectorEffectiveEncryptionV1::Unencrypted) {
        return Ok(());
    }
    let app = server.installed_app_context().ok_or_else(unavailable)?;
    let kms = app.kms().handle();
    if let VectorEffectiveEncryptionV1::AwsKms { key_id, .. } = encryption {
        let info = envelope.req_info().ok_or_else(denied)?;
        let credential = info.cred.as_ref().ok_or_else(denied)?;
        let iam = app.iam().handle();
        let claims = credential.claims.clone().unwrap_or_default();
        for action in [
            Some(KmsAction::DecryptAction),
            writes.then_some(KmsAction::GenerateDataKeyAction),
        ]
        .into_iter()
        .flatten()
        {
            let (conditions, is_owner) = admission::authorization_conditions(&app, envelope, Action::KmsAction(action))?;
            if !iam
                .is_allowed(&Args {
                    account: &credential.access_key,
                    groups: &credential.groups,
                    action: Action::KmsAction(action),
                    bucket: "",
                    object: key_id,
                    conditions: &conditions,
                    is_owner,
                    claims: &claims,
                    deny_only: false,
                })
                .await
            {
                return Err(denied());
            }
        }
        let manager = kms.get_manager().await.ok_or_else(unavailable)?;
        let mut context = OperationContext::new(credential.access_key.clone());
        context.source_ip = envelope.remote_addr().map(|address| address.ip().to_string());
        context.user_agent = envelope
            .headers()
            .get(http::header::USER_AGENT)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let metadata = manager
            .describe_key_with_context(DescribeKeyRequest { key_id: key_id.clone() }, &context)
            .await
            .map_err(kms_error)?
            .key_metadata;
        let invalid = match metadata.key_state {
            KeyState::Enabled => None,
            KeyState::Disabled => Some((ErrorCode::KmsDisabled, "KMS key is disabled")),
            KeyState::PendingDeletion | KeyState::PendingImport | KeyState::Unavailable => {
                Some((ErrorCode::KmsInvalidState, "KMS key is in an invalid state"))
            }
        };
        if let Some((code, message)) = invalid {
            return Err(ProtocolError::new(code, message));
        }
        if metadata.key_usage != KeyUsage::EncryptDecrypt {
            return Err(ProtocolError::new(ErrorCode::KmsInvalidKeyUsage, "KMS key cannot encrypt Vector data"));
        }
    }
    if kms.get_encryption_service().await.is_none() {
        return Err(unavailable());
    }
    Ok(())
}

fn kms_error(error: KmsError) -> ProtocolError {
    let (code, message) = match error {
        KmsError::AccessDenied { .. } => (ErrorCode::AccessDenied, "KMS operation failed"),
        KmsError::KeyNotFound { .. } => (ErrorCode::KmsNotFound, "KMS key was not found"),
        KmsError::InvalidKey { .. } | KmsError::InvalidOperation { .. } => {
            (ErrorCode::KmsInvalidState, "KMS key is in an invalid state")
        }
        KmsError::UnsupportedAlgorithm { .. } | KmsError::InvalidKeySize { .. } => {
            (ErrorCode::KmsInvalidKeyUsage, "KMS key cannot encrypt Vector data")
        }
        _ => (ErrorCode::ServiceUnavailable, "KMS operation failed"),
    };
    ProtocolError::new(code, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kms_exceptions_normalize_strictly_by_operation_error_model() {
        let kms_cases = [
            (ErrorCode::KmsDisabled, "KMS key is disabled"),
            (ErrorCode::KmsInvalidKeyUsage, "KMS key cannot encrypt Vector data"),
            (ErrorCode::KmsInvalidState, "KMS key is in an invalid state"),
            (ErrorCode::KmsNotFound, "KMS key was not found"),
        ];
        for operation in Operation::ALL {
            let declares_kms = matches!(
                operation,
                Operation::PutVectors | Operation::GetVectors | Operation::DeleteVectors | Operation::QueryVectors
            );
            assert_eq!(operation_declares_kms_exceptions(operation), declares_kms);
            for (kms_code, expected_message) in kms_cases {
                let original = ProtocolError::new(kms_code, "raw KMS preflight error");
                let normalized = service_error_for_operation(operation, VectorServiceError::Protocol(original.clone()));
                let via_protocol = protocol_error_for_operation(operation, original.clone());
                assert_eq!(normalized, via_protocol);
                if declares_kms {
                    assert_eq!(normalized.code(), kms_code);
                    assert_eq!(normalized, original);
                } else {
                    assert_eq!(normalized.code(), ErrorCode::Validation, "operation {operation:?}");
                    assert_eq!(normalized.status_code(), http::StatusCode::BAD_REQUEST);
                    assert_eq!(normalized.to_string(), format!("ValidationException: {expected_message}"));
                }
            }
            for passthrough in [ErrorCode::AccessDenied, ErrorCode::ServiceUnavailable, ErrorCode::NotFound] {
                let original = ProtocolError::new(passthrough, "passthrough error");
                assert_eq!(
                    service_error_for_operation(operation, VectorServiceError::Protocol(original.clone())),
                    original
                );
            }
        }
    }
}
