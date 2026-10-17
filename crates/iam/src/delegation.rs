// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Durable restrictions owned by an authenticated user. A record is not an IAM
//! grant or credential; issuing credentials still requires current IAM checks.
use crate::{error::is_err_config_not_found, store::Store};
use serde::{Deserialize, Serialize};

const PREFIX: &str = "config/iam/plugin-delegations/v1";

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Restrictions {
    #[serde(default, skip_serializing_if = "Option::is_none", deserialize_with = "present_value")]
    pub version: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none", deserialize_with = "present_value")]
    pub output_bucket: Option<String>,
    pub deployment: String,
    pub bucket: String,
    pub input_prefix: String,
    pub output_prefix: String,
    pub expires_at_unix_seconds: u64,
}

// An absent v1 field is compatible; an explicitly null v2 field is invalid.
fn present_value<'de, T: Deserialize<'de>, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Option<T>, D::Error> {
    T::deserialize(deserializer).map(Some)
}

impl Restrictions {
    pub fn output_bucket(&self) -> Result<&str, DelegationError> {
        match (self.version, self.output_bucket.as_deref()) {
            (None, None) => Ok(&self.bucket),
            (Some(2), Some(output)) => Ok(output),
            _ => Err(DelegationError::Invalid),
        }
    }

    pub fn validate(&self, now: u64) -> Result<(), DelegationError> {
        let output_bucket = self.output_bucket()?;
        let names: [&str; 5] = [
            &self.deployment,
            &self.bucket,
            &self.input_prefix,
            &self.output_prefix,
            output_bucket,
        ];
        if names
            .iter()
            .any(|s| s.is_empty() || s.len() > 1024 || s.contains("${") || s.chars().any(char::is_control))
            || self.bucket.contains('/')
            || self.bucket.contains('*')
            || self.bucket.contains('?')
            || output_bucket.contains('/')
            || output_bucket.contains('*')
            || output_bucket.contains('?')
            || self.input_prefix.contains('*')
            || self.input_prefix.contains('?')
            || self.output_prefix.contains('*')
            || self.output_prefix.contains('?')
            || self.expires_at_unix_seconds <= now
            || self.expires_at_unix_seconds.saturating_sub(now) > 86400
        {
            return Err(DelegationError::Invalid);
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Delegation {
    pub version: u32,
    pub id: String,
    pub revision: String,
    pub owner_access_key: String,
    pub created_at_unix_seconds: u64,
    pub restrictions: Restrictions,
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Tombstone {
    version: u32,
    id: String,
    revision: String,
    owner_access_key: String,
}

#[derive(Debug, thiserror::Error)]
pub enum DelegationError {
    #[error("invalid delegation restriction")]
    Invalid,
    #[error("delegation not found")]
    NotFound,
    #[error("delegation owner rejected")]
    OwnerRejected,
    #[error("delegation unavailable")]
    Unavailable,
    #[error("delegation revoked")]
    Revoked,
    #[error("delegation expired")]
    Expired,
}

fn record_path(id: &str) -> Result<String, DelegationError> {
    if id.len() != 32 || !id.bytes().all(|c| c.is_ascii_hexdigit()) {
        return Err(DelegationError::Invalid);
    }
    Ok(format!("{PREFIX}/{id}/record.json"))
}

async fn load<T: Store>(store: &T, id: &str) -> Result<Delegation, DelegationError> {
    let record: Delegation = store.load_iam_config_strict(record_path(id)?).await.map_err(|error| {
        if is_err_config_not_found(&error) {
            DelegationError::NotFound
        } else {
            DelegationError::Unavailable
        }
    })?;
    if !matches!(record.version, 1 | 2)
        || record.version != record.restrictions.version.unwrap_or(1)
        || record.id != id
        || record.revision != id
        || record.owner_access_key.is_empty()
    {
        return Err(DelegationError::Unavailable);
    }
    record
        .restrictions
        .validate(record.created_at_unix_seconds)
        .map_err(|_| DelegationError::Unavailable)?;
    Ok(record)
}

pub(crate) async fn create<T: Store>(
    store: &T,
    owner: &str,
    restrictions: Restrictions,
    now: u64,
) -> Result<Delegation, DelegationError> {
    restrictions.validate(now)?;
    if owner.is_empty() {
        return Err(DelegationError::OwnerRejected);
    }
    // IDs are generated exclusively here; no update operation accepts an ID.
    let id = format!("{:032x}", rand::random::<u128>());
    let path = record_path(&id)?;
    match store.load_iam_config_strict::<Delegation>(&path).await {
        Err(error) if is_err_config_not_found(&error) => {}
        _ => return Err(DelegationError::Unavailable),
    }
    let record = Delegation {
        version: restrictions.version.unwrap_or(1),
        revision: id.clone(),
        id,
        owner_access_key: owner.to_owned(),
        created_at_unix_seconds: now,
        restrictions,
    };
    store
        .save_iam_config(&record, path)
        .await
        .map_err(|_| DelegationError::Unavailable)?;
    if load(store, &record.id).await? != record {
        return Err(DelegationError::Unavailable);
    }
    Ok(record)
}

pub(crate) async fn revoke<T: Store>(store: &T, owner: &str, id: &str) -> Result<(), DelegationError> {
    let record = load(store, id).await?;
    if record.owner_access_key != owner {
        return Err(DelegationError::OwnerRejected);
    }
    let tombstone = Tombstone {
        version: 1,
        id: record.id,
        revision: record.revision,
        owner_access_key: record.owner_access_key,
    };
    let path = format!("{PREFIX}/{id}/revoked.json");
    // Every retry writes exactly the same revocation; active records never change.
    store
        .save_iam_config(&tombstone, &path)
        .await
        .map_err(|_| DelegationError::Unavailable)?;
    let saved: Tombstone = store
        .load_iam_config_strict(path)
        .await
        .map_err(|_| DelegationError::Unavailable)?;
    if saved != tombstone {
        return Err(DelegationError::Unavailable);
    }
    Ok(())
}

pub(crate) async fn read_active<T: Store>(store: &T, owner: &str, id: &str, now: u64) -> Result<Delegation, DelegationError> {
    let record = load(store, id).await?;
    if record.owner_access_key != owner {
        return Err(DelegationError::OwnerRejected);
    }
    check_active(store, record, now).await
}

pub(crate) async fn read_owned<T: Store>(store: &T, owner: &str, id: &str) -> Result<Delegation, DelegationError> {
    let record = load(store, id).await?;
    if record.owner_access_key != owner {
        return Err(DelegationError::OwnerRejected);
    }
    Ok(record)
}

pub(crate) async fn read_active_for_broker<T: Store>(store: &T, id: &str, now: u64) -> Result<Delegation, DelegationError> {
    let record = load(store, id).await?;
    check_active(store, record, now).await
}
async fn check_active<T: Store>(store: &T, record: Delegation, now: u64) -> Result<Delegation, DelegationError> {
    let id = &record.id;
    match store
        .load_iam_config_strict::<Tombstone>(format!("{PREFIX}/{id}/revoked.json"))
        .await
    {
        Ok(_) => return Err(DelegationError::Revoked),
        Err(error) if is_err_config_not_found(&error) => {}
        Err(_) => return Err(DelegationError::Unavailable),
    }
    if now >= record.restrictions.expires_at_unix_seconds {
        return Err(DelegationError::Expired);
    }
    Ok(record)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn delegation_v1_json_remains_compatible() {
        let legacy = serde_json::json!({"deployment":"runner-a","bucket":"photos",
            "input_prefix":"inputs/","output_prefix":"outputs/","expires_at_unix_seconds":200});
        let restriction: Restrictions = serde_json::from_value(legacy.clone()).expect("legacy restriction");
        restriction.validate(100).expect("legacy validation");
        assert_eq!(restriction.output_bucket().expect("v1 output"), "photos");
        assert_eq!(serde_json::to_value(restriction).expect("legacy serialization"), legacy);
        let record = serde_json::json!({"version":1,"id":"a".repeat(32),"revision":"a".repeat(32),
            "owner_access_key":"owner","created_at_unix_seconds":100,"restrictions":legacy});
        let decoded: Delegation = serde_json::from_value(record.clone()).expect("legacy record");
        assert_eq!(serde_json::to_value(decoded).expect("legacy record serialization"), record);
    }

    #[test]
    fn delegation_v2_requires_explicit_output_bucket() {
        let valid = serde_json::json!({"version":2,"deployment":"runner-a","bucket":"photos",
            "input_prefix":"inputs/","output_bucket":"artifacts","output_prefix":"outputs/",
            "expires_at_unix_seconds":200});
        let restriction: Restrictions = serde_json::from_value(valid.clone()).expect("v2 restriction");
        restriction.validate(100).expect("explicit output bucket");
        assert_eq!(restriction.output_bucket().expect("v2 output"), "artifacts");
        assert_eq!(serde_json::to_value(restriction).expect("v2 serialization"), valid);
        for key in ["version", "output_bucket"] {
            let mut bad = valid.clone();
            bad.as_object_mut().expect("object").remove(key);
            assert!(
                serde_json::from_value::<Restrictions>(bad).map_or(true, |r| r.validate(100).is_err()),
                "v2 required {key}"
            );
        }
        for (key, value) in [
            ("version", serde_json::json!(1)),
            ("version", serde_json::json!(3)),
            ("version", serde_json::Value::Null),
            ("output_bucket", serde_json::Value::Null),
            ("output_bucket", serde_json::json!("")),
            ("output_bucket", serde_json::json!("other/*")),
            ("output_bucket", serde_json::json!("${aws:username}")),
            ("output_bucket", serde_json::json!("other?")),
            ("output_bucket", serde_json::json!("other\n")),
            ("unknown", serde_json::json!(true)),
        ] {
            let mut bad = valid.clone();
            bad[key] = value;
            assert!(
                serde_json::from_value::<Restrictions>(bad).map_or(true, |r| r.validate(100).is_err()),
                "invalid v2 {key}"
            );
        }
    }

    #[test]
    fn restrictions_are_literal_bounded_and_expiring() {
        let mut restriction = Restrictions {
            version: None,
            output_bucket: None,
            deployment: "runner-a".into(),
            bucket: "photos".into(),
            input_prefix: "inputs/".into(),
            output_prefix: "outputs/".into(),
            expires_at_unix_seconds: 200,
        };
        assert!(restriction.validate(100).is_ok());
        restriction.output_prefix = "outputs/*".into();
        assert!(matches!(restriction.validate(100), Err(DelegationError::Invalid)));
        restriction.output_prefix = "outputs/".into();
        assert!(matches!(restriction.validate(200), Err(DelegationError::Invalid)));
        restriction.expires_at_unix_seconds = 200;
        restriction.input_prefix = "inputs/${aws:username}/".into();
        assert!(
            matches!(restriction.validate(100), Err(DelegationError::Invalid)),
            "C07_LITERAL_SCOPE_BOUND: policy variables are not literal restrictions"
        );
        assert!(record_path("../../users").is_err());
    }
}
