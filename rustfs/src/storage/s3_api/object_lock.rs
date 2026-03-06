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

use s3s::dto::{
    GetObjectLegalHoldOutput, GetObjectLockConfigurationOutput, GetObjectRetentionOutput, ObjectLockConfiguration,
    ObjectLockLegalHold, ObjectLockLegalHoldStatus, ObjectLockRetention, ObjectLockRetentionMode, PutObjectLegalHoldOutput,
    PutObjectRetentionOutput, RequestCharged, Timestamp,
};

pub(crate) fn build_get_object_legal_hold_output(legal_hold_status: Option<String>) -> GetObjectLegalHoldOutput {
    let status = legal_hold_status.unwrap_or_else(|| ObjectLockLegalHoldStatus::OFF.to_string());
    GetObjectLegalHoldOutput {
        legal_hold: Some(ObjectLockLegalHold {
            status: Some(ObjectLockLegalHoldStatus::from(status)),
        }),
    }
}

pub(crate) fn build_get_object_lock_configuration_output(
    object_lock_configuration: Option<ObjectLockConfiguration>,
) -> GetObjectLockConfigurationOutput {
    GetObjectLockConfigurationOutput {
        object_lock_configuration,
    }
}

pub(crate) fn build_get_object_retention_output(
    mode: Option<ObjectLockRetentionMode>,
    retain_until_date: Option<Timestamp>,
) -> GetObjectRetentionOutput {
    GetObjectRetentionOutput {
        retention: Some(ObjectLockRetention { mode, retain_until_date }),
    }
}

pub(crate) fn build_put_object_legal_hold_output() -> PutObjectLegalHoldOutput {
    PutObjectLegalHoldOutput {
        request_charged: Some(RequestCharged::from_static(RequestCharged::REQUESTER)),
    }
}

pub(crate) fn build_put_object_retention_output() -> PutObjectRetentionOutput {
    PutObjectRetentionOutput {
        request_charged: Some(RequestCharged::from_static(RequestCharged::REQUESTER)),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_get_object_legal_hold_output, build_get_object_lock_configuration_output, build_get_object_retention_output,
        build_put_object_legal_hold_output, build_put_object_retention_output,
    };
    use s3s::dto::{
        ObjectLockConfiguration, ObjectLockEnabled, ObjectLockLegalHoldStatus, ObjectLockRetentionMode, RequestCharged,
    };
    use time::OffsetDateTime;

    #[test]
    fn test_build_get_object_legal_hold_output_defaults_to_off_when_missing() {
        let output = build_get_object_legal_hold_output(None);
        let status = output
            .legal_hold
            .as_ref()
            .and_then(|hold| hold.status.as_ref())
            .map(ObjectLockLegalHoldStatus::as_str);
        assert_eq!(status, Some(ObjectLockLegalHoldStatus::OFF));
    }

    #[test]
    fn test_build_get_object_legal_hold_output_uses_input_status() {
        let output = build_get_object_legal_hold_output(Some(ObjectLockLegalHoldStatus::ON.to_string()));
        let status = output
            .legal_hold
            .as_ref()
            .and_then(|hold| hold.status.as_ref())
            .map(ObjectLockLegalHoldStatus::as_str);
        assert_eq!(status, Some(ObjectLockLegalHoldStatus::ON));
    }

    #[test]
    fn test_build_get_object_lock_configuration_output_preserves_field() {
        let cfg = ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
            ..Default::default()
        };
        let output = build_get_object_lock_configuration_output(Some(cfg.clone()));
        assert_eq!(output.object_lock_configuration, Some(cfg));
    }

    #[test]
    fn test_build_get_object_retention_output_preserves_fields() {
        let mode = Some(ObjectLockRetentionMode::from_static(ObjectLockRetentionMode::GOVERNANCE));
        let retain_until_date = Some(OffsetDateTime::UNIX_EPOCH.into());
        let output = build_get_object_retention_output(mode.clone(), retain_until_date.clone());

        let retention = output.retention.expect("retention should be present");
        assert_eq!(retention.mode, mode);
        assert_eq!(retention.retain_until_date, retain_until_date);
    }

    #[test]
    fn test_build_put_object_legal_hold_output_sets_request_charged() {
        let output = build_put_object_legal_hold_output();
        assert_eq!(
            output.request_charged.as_ref().map(RequestCharged::as_str),
            Some(RequestCharged::REQUESTER)
        );
    }

    #[test]
    fn test_build_put_object_retention_output_sets_request_charged() {
        let output = build_put_object_retention_output();
        assert_eq!(
            output.request_charged.as_ref().map(RequestCharged::as_str),
            Some(RequestCharged::REQUESTER)
        );
    }
}
