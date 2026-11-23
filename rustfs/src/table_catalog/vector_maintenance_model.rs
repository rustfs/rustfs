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

use super::*;
use crate::storage_api::table::managed::ManagedTableId;
use serde::de::{Error as _, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Value};

pub(crate) const VECTOR_MAINTENANCE_JOB_MAX_BYTES: usize = 65_536;
pub(crate) const VECTOR_MAINTENANCE_RECORD_MAX_BYTES: usize = 4_194_304;
pub(crate) const VECTOR_RETIREMENT_PROGRESS_MAX_BYTES: usize = 262_144;
pub(crate) const VECTOR_RETIREMENT_MAX_TARGETS: usize = 256;
pub(crate) const VECTOR_RETIREMENT_MAX_REFERENCES: usize = 4096;

pub(crate) trait VectorMaintenanceRecord: Serialize + DeserializeOwned {
    const MAX_BYTES: usize = VECTOR_MAINTENANCE_RECORD_MAX_BYTES;
    fn validate(&self) -> TableCatalogStoreResult<()>;
}

fn invalid(message: impl Into<String>) -> TableCatalogStoreError {
    TableCatalogStoreError::Invalid(message.into())
}
fn ensure(condition: bool, message: &str) -> TableCatalogStoreResult<()> {
    if condition { Ok(()) } else { Err(invalid(message)) }
}
fn text(value: &str, max: usize) -> TableCatalogStoreResult<()> {
    ensure(
        !value.is_empty() && value.len() <= max && !value.chars().any(char::is_control),
        "invalid bounded maintenance text",
    )
}
fn nonnil(value: Uuid) -> TableCatalogStoreResult<()> {
    ensure(!value.is_nil(), "nil maintenance identity")
}
fn hash(value: &str) -> TableCatalogStoreResult<()> {
    ensure(
        value.len() == 64 && value.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "invalid maintenance SHA-256",
    )
}
pub(in crate::table_catalog) fn vector_maintenance_timestamp(value: &str) -> TableCatalogStoreResult<OffsetDateTime> {
    let parsed = OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
        .map_err(|_| invalid("invalid maintenance timestamp"))?;
    ensure(
        value.len() == 30 && value.ends_with('Z') && value.as_bytes().get(19) == Some(&b'.') && parsed.year() >= 1970,
        "maintenance timestamp must be UTC with nine fractional digits",
    )?;
    Ok(parsed)
}
pub(in crate::table_catalog) fn maintenance_now() -> String {
    maintenance_format_time(OffsetDateTime::now_utc())
}
pub(in crate::table_catalog) fn maintenance_format_time(now: OffsetDateTime) -> String {
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:09}Z",
        now.year(),
        u8::from(now.month()),
        now.day(),
        now.hour(),
        now.minute(),
        now.second(),
        now.nanosecond()
    )
}

struct StrictMaintenanceJson(Value);
impl<'de> Deserialize<'de> for StrictMaintenanceJson {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct StrictVisitor;
        impl<'de> Visitor<'de> for StrictVisitor {
            type Value = StrictMaintenanceJson;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("strict maintenance JSON")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut values = Map::new();
                while let Some(key) = map.next_key::<String>()? {
                    if values.contains_key(&key) {
                        return Err(A::Error::custom("duplicate maintenance JSON field"));
                    }
                    values.insert(key, map.next_value::<StrictMaintenanceJson>()?.0);
                }
                Ok(StrictMaintenanceJson(Value::Object(values)))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                while let Some(value) = seq.next_element::<StrictMaintenanceJson>()? {
                    values.push(value.0);
                }
                Ok(StrictMaintenanceJson(Value::Array(values)))
            }
            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
                Ok(StrictMaintenanceJson(Value::String(value.to_owned())))
            }
            fn visit_string<E: serde::de::Error>(self, value: String) -> Result<Self::Value, E> {
                Ok(StrictMaintenanceJson(Value::String(value)))
            }
            fn visit_bool<E: serde::de::Error>(self, value: bool) -> Result<Self::Value, E> {
                Ok(StrictMaintenanceJson(Value::Bool(value)))
            }
            fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<Self::Value, E> {
                Ok(StrictMaintenanceJson(value.into()))
            }
            fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<Self::Value, E> {
                Ok(StrictMaintenanceJson(value.into()))
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(StrictMaintenanceJson(Value::Null))
            }
            fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                self.visit_unit()
            }
        }
        deserializer.deserialize_any(StrictVisitor)
    }
}
fn validate_json(value: &Value, depth: usize) -> TableCatalogStoreResult<()> {
    ensure(depth <= 32, "maintenance JSON exceeds maximum depth")?;
    match value {
        Value::Array(values) => {
            for value in values {
                validate_json(value, depth + 1)?;
            }
        }
        Value::Object(values) => {
            let expected: Option<&[&str]> = if values.contains_key("content_sha256") {
                Some(&["object_key", "content_length", "content_sha256", "body_revision", "etag"])
            } else if values.contains_key("storage_bucket_id") {
                Some(&[
                    "version",
                    "storage_bucket_id",
                    "bucket_incarnation",
                    "account_namespace_id",
                    "region",
                    "public_name",
                ])
            } else if values.contains_key("payload_base64") {
                Some(&["encoding_version", "format", "format_version", "metadata", "payload_base64"])
            } else {
                None
            };
            if let Some(expected) = expected {
                ensure(
                    values.len() == expected.len() && expected.iter().all(|k| values.contains_key(*k)),
                    "incomplete embedded maintenance reference",
                )?;
            }
            for (key, value) in values {
                if matches!(
                    key.as_str(),
                    "body_revision"
                        | "bucket_incarnation"
                        | "table_id"
                        | "job_id"
                        | "request_id"
                        | "recorded_by_job_id"
                        | "lease_id"
                        | "worker_boot_id"
                        | "revision"
                        | "retirement_id"
                        | "retirement_revision"
                        | "dispatch_id"
                        | "job_revision"
                        | "last_update_id"
                        | "maintenance_revision"
                        | "last_maintenance_update_id"
                        | "created_by_job_id"
                ) && !value.is_null()
                {
                    let raw = value.as_str().ok_or_else(|| invalid("maintenance UUID requires a string"))?;
                    let id = Uuid::parse_str(raw).map_err(|_| invalid("invalid maintenance UUID"))?;
                    ensure(!id.is_nil() && id.to_string() == raw, "maintenance UUID must be canonical and nonnil")?;
                }
                validate_json(value, depth + 1)?;
            }
        }
        Value::Number(n) => {
            ensure(n.is_i64() || n.is_u64(), "floating point maintenance number")?;
        }
        _ => {}
    }
    Ok(())
}
fn canonicalize(value: Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.into_iter()
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .map(|(k, v)| (k, canonicalize(v)))
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.into_iter().map(canonicalize).collect()),
        value => value,
    }
}
pub(in crate::table_catalog) fn maintenance_canonical_bytes(value: &impl Serialize) -> TableCatalogStoreResult<Vec<u8>> {
    let value = serde_json::to_value(value).map_err(|e| invalid(format!("encode maintenance JSON: {e}")))?;
    validate_json(&value, 0)?;
    serde_json::to_vec(&canonicalize(value)).map_err(|e| invalid(format!("encode maintenance JSON: {e}")))
}
pub(crate) fn decode_vector_maintenance_record<T: VectorMaintenanceRecord>(bytes: &[u8]) -> TableCatalogStoreResult<T> {
    ensure(bytes.len() <= T::MAX_BYTES, "maintenance record exceeds byte limit")?;
    let value: StrictMaintenanceJson =
        serde_json::from_slice(bytes).map_err(|e| invalid(format!("decode maintenance JSON: {e}")))?;
    validate_json(&value.0, 0)?;
    let record: T = serde_json::from_value(value.0).map_err(|e| invalid(format!("decode maintenance record: {e}")))?;
    record.validate()?;
    Ok(record)
}
pub(crate) fn encode_vector_maintenance_record<T: VectorMaintenanceRecord>(record: &T) -> TableCatalogStoreResult<Vec<u8>> {
    record.validate()?;
    let bytes = maintenance_canonical_bytes(record)?;
    ensure(bytes.len() <= T::MAX_BYTES, "maintenance record exceeds byte limit")?;
    Ok(bytes)
}
fn decode_field<T: DeserializeOwned>(mut value: Value) -> Result<T, serde_json::Error> {
    if std::any::type_name::<T>() == "[u8; 16]" {
        let encoded = value
            .as_str()
            .filter(|s| s.len() == 32 && s.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)))
            .ok_or_else(|| serde_json::Error::custom("identity generation requires 32 lowercase hexadecimal characters"))?;
        let bytes = hex_simd::decode_to_vec(encoded.as_bytes()).map_err(serde_json::Error::custom)?;
        value = Value::Array(bytes.into_iter().map(Value::from).collect());
    }
    serde_json::from_value(value)
}
mod generation_hex {
    pub(super) fn serialize<S: serde::Serializer>(value: &[u8; 16], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&hex_simd::encode_to_string(value, hex_simd::AsciiCase::Lower))
    }
}
macro_rules! maintenance_record {
    ($name:ident { $($(#[$attribute:meta])* $field:ident: $ty:ty $(=> $getter:ident)?,)* }) => {
        #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
        pub(crate) struct $name { $($(#[$attribute])* pub(super) $field: $ty,)* }
        impl $name { $($(pub(crate) fn $getter(&self) -> &$ty { &self.$field })?)* }
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let StrictMaintenanceJson(value) = StrictMaintenanceJson::deserialize(deserializer)?;
                validate_json(&value, 0).map_err(D::Error::custom)?;
                let Value::Object(mut fields) = value else { return Err(D::Error::custom("maintenance record must be an object")); };
                $(let $field = decode_field::<$ty>(fields.remove(stringify!($field)).ok_or_else(|| D::Error::missing_field(stringify!($field)))?).map_err(D::Error::custom)?;)*
                if !fields.is_empty() { return Err(D::Error::custom("unknown maintenance record field")); }
                let record = Self { $($field,)* };
                record.validate().map_err(D::Error::custom)?;
                Ok(record)
            }
        }
    };
}
maintenance_record! { VectorMaintenanceResourceV1 {
    bucket: VectorBucketBindingV1 => bucket,
    namespace: String => namespace,
    name: String => name,
    table_id: String => table_id,
} }

maintenance_record! { VectorMaintenanceExecutionIdentityV1 {
    account: String => account,
    #[serde(serialize_with = "generation_hex::serialize")]
    user_generation: [u8; 16] => user_generation,
} }

maintenance_record! { VectorMaintenancePolicyV1 {
    lease_timeout_seconds: u64,
    heartbeat_interval_seconds: u64 => heartbeat_interval_seconds,
    max_attempts: u16,
    retry_initial_backoff_seconds: u64,
    retry_max_backoff_seconds: u64,
} }

maintenance_record! { VectorMaintenanceHeadV1 {
    entry_version: u16 => entry_version,
    state: TableCatalogEntryState => state,
    version_token: String => version_token,
    generation: u64 => generation,
    binding: VectorHeadBindingV1 => binding,
    maintenance: Option<VectorMaintenanceBindingV1> => maintenance,
} }

maintenance_record! { VectorMaintenanceBindingV1 {
    version: u16,
    retirement_count: u32,
    retirement_tip: Option<CatalogMetadataRefRecordV1> => retirement_tip,
} }

maintenance_record! { VectorMaintenanceSourceV1 {
    kind: VectorMaintenanceSourceKind => kind,
    reference: CatalogMetadataRefRecordV1 => reference,
} }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum VectorMaintenanceSourceKind {
    CommitLog,
    CreateIntent,
    DeleteIntent,
    RetirementAuthorization,
    MaintenanceJob,
}

maintenance_record! { VectorMaintenanceInputV1 {
    base_head: Option<VectorMaintenanceHeadV1> => base_head,
    source: Option<VectorMaintenanceSourceV1> => source,
    plan_ref: Option<CatalogMetadataRefRecordV1> => plan_ref,
} }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum VectorMaintenanceKind {
    AnnBuild,
    Compaction,
    CommitRecovery,
    CreateRecovery,
    DeleteRecovery,
    HistoryGc,
    OrphanGc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum VectorMaintenanceJobState {
    Queued,
    Running,
    RetryScheduled,
    RecoveryRequired,
    Succeeded,
    Failed,
    ManualReview,
    PartialRecovered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum VectorMaintenanceOrigin {
    Periodic,
    Recovery,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum VectorMaintenanceFailureCode {
    ResourceChanged,
    IdentityDenied,
    KeyDenied,
    CapacityExceeded,
    BudgetUnsupported,
    StorageUnavailable,
    Conflict,
    CorruptRecord,
    UnknownOutcome,
    RetryExhausted,
    PartialPublication,
    Interrupted,
}

maintenance_record! { VectorMaintenanceLeaseV1 {
    version: u16,
    lease_id: Uuid => lease_id,
    worker_id: String => worker_id,
    worker_boot_id: Uuid => worker_boot_id,
    claim_digest_sha256: String => claim_digest_sha256,
    claimed_at: String,
    heartbeat_at: String,
    expires_at: String => expires_at,
} }

maintenance_record! { VectorMaintenanceJobV1 {
    version: u16,
    job_id: Uuid => job_id,
    resource: VectorMaintenanceResourceV1 => resource,
    kind: VectorMaintenanceKind => kind,
    origin: VectorMaintenanceOrigin,
    request_id: Uuid,
    spec_sha256: String,
    revision: Uuid => revision,
    last_update_id: Uuid,
    last_update_sha256: String,
    state: VectorMaintenanceJobState => state,
    input: VectorMaintenanceInputV1 => input,
    execution_identity: VectorMaintenanceExecutionIdentityV1 => execution_identity,
    initiator_ref: Option<String> => initiator_ref,
    policy: VectorMaintenancePolicyV1 => policy,
    attempt: u16 => attempt,
    lease: Option<VectorMaintenanceLeaseV1> => lease,
    created_at: String,
    updated_at: String,
    next_retry_at: Option<String> => next_retry_at,
    finished_at: Option<String>,
    failure: Option<VectorMaintenanceFailureCode>,
    result_ref: Option<CatalogMetadataRefRecordV1> => result_ref,
} }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum VectorMaintenanceResultEffect {
    NoWork,
    Published,
    Recovered,
    Retired,
    PartialRecovered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum VectorMaintenanceNoWorkReason {
    NotDue,
    NoCandidates,
    AlreadyRecovered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum VectorMaintenanceStageOutcome {
    NotPrepared,
    Unpublished,
    Committed,
    Superseded,
    Unknown,
}

maintenance_record! { VectorMaintenanceStageResultV1 {
    stage: String,
    commit_id: String,
    expected_head: Option<VectorMaintenanceHeadV1>,
    candidate: Option<CatalogMetadataRefRecordV1>,
    outcome: VectorMaintenanceStageOutcome,
    commit_ref: Option<CatalogMetadataRefRecordV1> => commit_ref,
} }

maintenance_record! { VectorMaintenanceResultV1 {
    version: u16,
    job_id: Uuid,
    recorded_by_job_id: Uuid,
    attempt: u16,
    resource: VectorMaintenanceResourceV1,
    finished_at: String,
    effect: VectorMaintenanceResultEffect,
    stages: Vec<VectorMaintenanceStageResultV1>,
    commit_refs: Vec<CatalogMetadataRefRecordV1>,
    retirement_refs: Vec<CatalogMetadataRefRecordV1>,
    output_refs: Vec<CatalogMetadataRefRecordV1>,
    reason: Option<VectorMaintenanceNoWorkReason>,
} }

maintenance_record! { VectorObjectTimeV1 {
    unix_seconds: i64,
    nanosecond: u32,
} }

maintenance_record! { VectorRetirementTargetV1 {
    object_key: String => object_key,
    body_revision: Uuid => body_revision,
    mod_time: Option<VectorObjectTimeV1> => mod_time,
    content_length: u64 => content_length,
    etag: Option<String>,
    version_id: Option<Uuid> => version_id,
    delete_marker: bool => delete_marker,
} }

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub(crate) enum VectorHistoryRetentionV1 {
    All,
    Last { count: u32, min_age_seconds: u64 },
}

maintenance_record! { VectorRetirementEvidenceV1 {
    version: u16,
    resource: VectorMaintenanceResourceV1,
    base_head: VectorMaintenanceHeadV1,
    delete_intent_ref: Option<CatalogMetadataRefRecordV1>,
    history_refs: Vec<CatalogMetadataRefRecordV1> => history_refs,
    protected_refs: Vec<CatalogMetadataRefRecordV1>,
    unresolved_refs: Vec<CatalogMetadataRefRecordV1> => unresolved_refs,
    retention: VectorHistoryRetentionV1 => retention,
} }

maintenance_record! { VectorRetirementPlanV1 {
    version: u16,
    resource: VectorMaintenanceResourceV1 => resource,
    evidence_ref: CatalogMetadataRefRecordV1,
    targets: Vec<VectorRetirementTargetV1>,
} }

maintenance_record! { VectorRetirementAuthorizationV1 {
    version: u16,
    retirement_id: Uuid,
    retirement_revision: Uuid,
    created_by_job_id: Uuid,
    created_by_attempt: u16,
    resource: VectorMaintenanceResourceV1 => resource,
    execution_identity: VectorMaintenanceExecutionIdentityV1,
    created_at: String,
    previous_ref: Option<CatalogMetadataRefRecordV1>,
    base_head: VectorMaintenanceHeadV1,
    delete_intent_ref: Option<CatalogMetadataRefRecordV1>,
    plan_ref: CatalogMetadataRefRecordV1,
    reference_evidence_ref: CatalogMetadataRefRecordV1,
    targets: Vec<VectorRetirementTargetV1>,
} }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum VectorRetirementProgressState {
    Authorized,
    Deleting,
    Completed,
    ManualReview,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum VectorRetirementTargetState {
    Pending,
    InFlight,
    DeleteCommitted,
    AbsentAfterRecovery,
    Unknown,
}

maintenance_record! { VectorRetirementDispatchV1 {
    dispatch_id: Uuid => dispatch_id,
    job_id: Uuid => job_id,
    job_revision: Uuid,
    attempt: u16 => attempt,
    lease_id: Uuid,
    worker_id: String,
    worker_boot_id: Uuid,
    execution_identity: VectorMaintenanceExecutionIdentityV1,
    admitted_at: String,
} }

maintenance_record! { VectorDeleteAccountingV1 {
    size: Option<u64>,
    version_id: Option<Uuid>,
    removed_current_object: bool,
} }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum VectorRetirementReceiptOutcome {
    DeleteCommitted,
    AbsentAfterRecovery,
}

maintenance_record! { VectorRetirementReceiptV1 {
    outcome: VectorRetirementReceiptOutcome,
    completed_at: String,
    accounting: Option<VectorDeleteAccountingV1>,
    recovery_pending: u64,
    recovery_blocked: u64,
    recovery_failed: u64,
} }

maintenance_record! { VectorRetirementTargetProgressV1 {
    ordinal: u16 => ordinal,
    state: VectorRetirementTargetState => state,
    dispatch: Option<VectorRetirementDispatchV1> => dispatch,
    receipt: Option<VectorRetirementReceiptV1>,
    failure: Option<VectorMaintenanceFailureCode>,
} }

#[cfg(test)]
impl VectorMaintenanceJobV1 {
    pub(crate) fn request_id(&self) -> &Uuid {
        &self.request_id
    }
}
#[cfg(test)]
impl VectorRetirementAuthorizationV1 {
    pub(crate) fn retirement_id(&self) -> &Uuid {
        &self.retirement_id
    }
}
#[cfg(test)]
impl VectorRetirementTargetProgressV1 {
    pub(crate) fn receipt(&self) -> &Option<VectorRetirementReceiptV1> {
        &self.receipt
    }
}
#[cfg(test)]
impl VectorRetirementReceiptV1 {
    pub(crate) fn accounting(&self) -> &Option<VectorDeleteAccountingV1> {
        &self.accounting
    }
}

maintenance_record! { VectorRetirementProgressV1 {
    version: u16,
    resource: VectorMaintenanceResourceV1,
    retirement_id: Uuid,
    authorization_ref: CatalogMetadataRefRecordV1,
    revision: Uuid => revision,
    last_update_id: Uuid,
    last_update_sha256: String,
    state: VectorRetirementProgressState => state,
    created_at: String,
    updated_at: String,
    targets: Vec<VectorRetirementTargetProgressV1> => targets,
} }

fn reference(value: &CatalogMetadataRefRecordV1) -> TableCatalogStoreResult<()> {
    CatalogMetadataRef::try_from(value)?;
    ensure(
        value.body_revision.is_some_and(|id| !id.is_nil()) && value.content_length > 0,
        "maintenance references require exact nonempty object identity",
    )?;
    text(&value.object_key, 4096)?;
    if let Some(etag) = &value.etag {
        text(etag, 4096)?;
    }
    hash(&value.content_sha256)
}
fn references(values: &[CatalogMetadataRefRecordV1], max: usize) -> TableCatalogStoreResult<()> {
    ensure(values.len() <= max, "maintenance reference count exceeds limit")?;
    let mut seen = BTreeSet::new();
    for value in values {
        reference(value)?;
        ensure(seen.insert((&value.object_key, value.body_revision)), "duplicate maintenance reference")?;
    }
    Ok(())
}
impl VectorMaintenanceRecord for VectorMaintenanceResourceV1 {
    fn validate(&self) -> TableCatalogStoreResult<()> {
        self.identity()?;
        ensure(
            self.namespace == "indexes" && self.bucket.account_namespace_id.is_empty(),
            "invalid Vector maintenance namespace",
        )?;
        CatalogName::parse(self.name.clone()).map_err(|e| invalid(e.to_string()))?;
        ensure(
            self.name.len() <= 64 && self.bucket.storage_bucket_id.len() <= 63 && self.bucket.public_name.len() <= 63,
            "maintenance resource name exceeds limit",
        )
    }
}
impl VectorMaintenanceResourceV1 {
    pub(crate) fn identity(&self) -> TableCatalogStoreResult<CatalogResourceIdentity> {
        let id = Uuid::parse_str(&self.table_id).map_err(|_| invalid("invalid maintenance table ID"))?;
        ensure(
            id.to_string() == self.table_id && !id.is_nil(),
            "maintenance table ID must be canonical and nonnil",
        )?;
        CatalogResourceIdentity::try_new(
            self.bucket.to_handle()?,
            ManagedTableId::try_new(self.table_id.clone()).map_err(|e| invalid(e.to_string()))?,
            CatalogFormat::Lance,
        )
    }
    pub(crate) fn try_new(
        resource: &CatalogResourceIdentity,
        namespace: &Namespace,
        name: &CatalogName,
    ) -> TableCatalogStoreResult<Self> {
        let bucket = resource
            .vector_bucket()
            .ok_or_else(|| invalid("maintenance requires Vector identity"))?;
        let value = Self {
            bucket: VectorBucketBindingV1::from_handle(bucket)?,
            namespace: namespace.public_name(),
            name: name.as_str().to_string(),
            table_id: resource.table_id().as_str().to_string(),
        };
        value.validate()?;
        Ok(value)
    }
}
impl VectorMaintenanceRecord for VectorMaintenanceExecutionIdentityV1 {
    fn validate(&self) -> TableCatalogStoreResult<()> {
        text(&self.account, VECTOR_MAINTENANCE_JOB_MAX_BYTES)?;
        ensure(
            self.account.len() >= 3 && !matches!(self.account.as_str(), "." | "..") && !self.account.contains(['/', '\\']),
            "invalid maintenance execution account",
        )
    }
}
impl VectorMaintenanceExecutionIdentityV1 {
    pub(crate) fn try_new(account: String, user_generation: [u8; 16]) -> TableCatalogStoreResult<Self> {
        let value = Self {
            account,
            user_generation,
        };
        value.validate()?;
        Ok(value)
    }
}
impl Default for VectorMaintenancePolicyV1 {
    fn default() -> Self {
        Self {
            lease_timeout_seconds: 900,
            heartbeat_interval_seconds: 60,
            max_attempts: 3,
            retry_initial_backoff_seconds: 5,
            retry_max_backoff_seconds: 300,
        }
    }
}
impl VectorMaintenanceRecord for VectorMaintenancePolicyV1 {
    fn validate(&self) -> TableCatalogStoreResult<()> {
        ensure(
            (120..=86400).contains(&self.lease_timeout_seconds)
                && (1..=28800).contains(&self.heartbeat_interval_seconds)
                && self.heartbeat_interval_seconds * 3 <= self.lease_timeout_seconds
                && (1..=10).contains(&self.max_attempts)
                && self.retry_initial_backoff_seconds >= 1
                && self.retry_initial_backoff_seconds <= self.retry_max_backoff_seconds
                && self.retry_max_backoff_seconds <= 86400,
            "invalid maintenance policy",
        )
    }
}
impl VectorMaintenancePolicyV1 {
    pub(crate) fn try_new(
        lease_timeout_seconds: u64,
        heartbeat_interval_seconds: u64,
        max_attempts: u16,
        retry_initial_backoff_seconds: u64,
        retry_max_backoff_seconds: u64,
    ) -> TableCatalogStoreResult<Self> {
        let value = Self {
            lease_timeout_seconds,
            heartbeat_interval_seconds,
            max_attempts,
            retry_initial_backoff_seconds,
            retry_max_backoff_seconds,
        };
        value.validate()?;
        Ok(value)
    }
}
impl VectorMaintenanceRecord for VectorMaintenanceBindingV1 {
    fn validate(&self) -> TableCatalogStoreResult<()> {
        ensure(
            self.version == 1 && self.retirement_count <= 4096 && (self.retirement_count == 0) == self.retirement_tip.is_none(),
            "invalid maintenance binding",
        )?;
        if let Some(value) = &self.retirement_tip {
            reference(value)?;
        }
        Ok(())
    }
}
impl VectorMaintenanceBindingV1 {
    pub(crate) fn empty() -> Self {
        Self {
            version: 1,
            retirement_count: 0,
            retirement_tip: None,
        }
    }
}
impl VectorMaintenanceRecord for VectorMaintenanceHeadV1 {
    fn validate(&self) -> TableCatalogStoreResult<()> {
        ensure(
            matches!(self.entry_version, 2..=4)
                && matches!(self.state, TableCatalogEntryState::Active | TableCatalogEntryState::Deleted),
            "invalid maintenance head state",
        )?;
        text(&self.version_token, 4096)?;
        self.binding.validate()?;
        reference(&self.binding.metadata)?;
        ensure(
            (self.entry_version == 4) == self.maintenance.is_some(),
            "maintenance binding does not match entry version",
        )?;
        if let Some(value) = &self.maintenance {
            value.validate()?;
        }
        Ok(())
    }
}
impl VectorMaintenanceRecord for VectorMaintenanceSourceV1 {
    fn validate(&self) -> TableCatalogStoreResult<()> {
        reference(&self.reference)
    }
}
impl VectorMaintenanceRecord for VectorMaintenanceInputV1 {
    fn validate(&self) -> TableCatalogStoreResult<()> {
        if let Some(value) = &self.base_head {
            value.validate()?;
        }
        if let Some(value) = &self.source {
            value.validate()?;
        }
        if let Some(value) = &self.plan_ref {
            reference(value)?;
        }
        Ok(())
    }
}
impl VectorMaintenanceInputV1 {
    pub(crate) fn try_new(
        base_head: Option<VectorMaintenanceHeadV1>,
        source: Option<VectorMaintenanceSourceV1>,
        plan_ref: Option<CatalogMetadataRefRecordV1>,
    ) -> TableCatalogStoreResult<Self> {
        let value = Self {
            base_head,
            source,
            plan_ref,
        };
        value.validate()?;
        Ok(value)
    }
}
impl VectorMaintenanceSourceV1 {
    pub(crate) fn try_new(
        kind: VectorMaintenanceSourceKind,
        reference: CatalogMetadataRefRecordV1,
    ) -> TableCatalogStoreResult<Self> {
        let value = Self { kind, reference };
        value.validate()?;
        Ok(value)
    }
}
impl VectorMaintenanceRecord for VectorMaintenanceLeaseV1 {
    fn validate(&self) -> TableCatalogStoreResult<()> {
        ensure(self.version == 1, "unsupported maintenance lease version")?;
        nonnil(self.lease_id)?;
        nonnil(self.worker_boot_id)?;
        text(&self.worker_id, 4096)?;
        hash(&self.claim_digest_sha256)?;
        let claimed = vector_maintenance_timestamp(&self.claimed_at)?;
        let heartbeat = vector_maintenance_timestamp(&self.heartbeat_at)?;
        let expires = vector_maintenance_timestamp(&self.expires_at)?;
        ensure(claimed <= heartbeat && heartbeat < expires, "invalid maintenance lease times")
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct VectorMaintenanceSemanticSpec {
    pub(super) resource: VectorMaintenanceResourceV1,
    pub(super) kind: VectorMaintenanceKind,
    pub(super) origin: VectorMaintenanceOrigin,
    pub(super) input: VectorMaintenanceInputV1,
    pub(super) execution_identity: VectorMaintenanceExecutionIdentityV1,
    pub(super) initiator_ref: Option<String>,
    pub(super) policy: VectorMaintenancePolicyV1,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct VectorMaintenanceJobSpec {
    pub(super) job_id: Uuid,
    pub(super) resource: VectorMaintenanceResourceV1,
    pub(super) kind: VectorMaintenanceKind,
    pub(super) origin: VectorMaintenanceOrigin,
    pub(super) request_id: Uuid,
    pub(super) input: VectorMaintenanceInputV1,
    pub(super) execution_identity: VectorMaintenanceExecutionIdentityV1,
    pub(super) initiator_ref: Option<String>,
    pub(super) policy: VectorMaintenancePolicyV1,
}
impl VectorMaintenanceSemanticSpec {
    pub(crate) fn resource(&self) -> &VectorMaintenanceResourceV1 {
        &self.resource
    }
    pub(crate) fn try_new(
        resource: VectorMaintenanceResourceV1,
        kind: VectorMaintenanceKind,
        origin: VectorMaintenanceOrigin,
        input: VectorMaintenanceInputV1,
        execution_identity: VectorMaintenanceExecutionIdentityV1,
        initiator_ref: Option<String>,
        policy: VectorMaintenancePolicyV1,
    ) -> TableCatalogStoreResult<Self> {
        let value = Self {
            resource,
            kind,
            origin,
            input,
            execution_identity,
            initiator_ref,
            policy,
        };
        value.validate()?;
        Ok(value)
    }
    pub(in crate::table_catalog) fn validate(&self) -> TableCatalogStoreResult<()> {
        self.resource.validate()?;
        self.input.validate()?;
        self.execution_identity.validate()?;
        self.policy.validate()?;
        if let Some(value) = &self.initiator_ref {
            text(value, 4096)?;
        }
        let source = self.input.source.as_ref().map(|s| s.kind);
        let head = self.input.base_head.as_ref();
        if let Some(head) = head {
            ensure(
                head.binding.bucket == self.resource.bucket && head.binding.table_id == self.resource.table_id,
                "maintenance input belongs to another resource",
            )?;
        }
        use VectorMaintenanceKind as K;
        use VectorMaintenanceSourceKind as S;
        let valid = match self.kind {
            K::AnnBuild | K::Compaction => {
                head.is_some_and(|h| h.entry_version == 4 && h.state == TableCatalogEntryState::Active) && source.is_none()
            }
            K::CommitRecovery => matches!(source, Some(S::CommitLog | S::MaintenanceJob)) && self.input.plan_ref.is_none(),
            K::CreateRecovery => source == Some(S::CreateIntent) && head.is_none() && self.input.plan_ref.is_none(),
            K::DeleteRecovery => {
                matches!(source, Some(S::DeleteIntent | S::RetirementAuthorization)) && self.input.plan_ref.is_none()
            }
            K::HistoryGc | K::OrphanGc => {
                head.is_some_and(|h| h.entry_version == 4 || h.state == TableCatalogEntryState::Deleted)
                    && self.input.plan_ref.is_some()
                    && matches!(source, None | Some(S::DeleteIntent))
            }
        };
        ensure(valid, "maintenance kind and input do not match")
    }
}
pub(in crate::table_catalog) fn maintenance_uuid(domain: &str, bytes: &[u8]) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(domain.as_bytes());
    hasher.update([0]);
    hasher.update(bytes);
    maintenance_uuid_from_digest(hasher.finalize().into())
}
pub(in crate::table_catalog) fn maintenance_uuid_from_digest(digest: [u8; 32]) -> Uuid {
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 15) | 128;
    bytes[8] = (bytes[8] & 63) | 128;
    Uuid::from_bytes(bytes)
}
pub(crate) fn vector_maintenance_job_ids(spec: &VectorMaintenanceSemanticSpec) -> TableCatalogStoreResult<(Uuid, Uuid)> {
    spec.validate()?;
    let digest = Sha256::digest(maintenance_canonical_bytes(spec)?);
    Ok((
        maintenance_uuid("rustfs-vector-maintenance-job-v1", &digest),
        maintenance_uuid("rustfs-vector-maintenance-request-v1", &digest),
    ))
}
pub(crate) fn vector_maintenance_stage_commit_id(
    job_id: Uuid,
    attempt: u16,
    stage: &str,
) -> TableCatalogStoreResult<CatalogCommitId> {
    nonnil(job_id)?;
    ensure(
        (1..=10).contains(&attempt) && matches!(stage, "ann-build" | "reserve-fragments" | "rewrite"),
        "invalid maintenance stage identity",
    )?;
    let bytes = maintenance_canonical_bytes(&serde_json::json!({"job_id":job_id,"attempt":attempt,"stage":stage}))?;
    CatalogCommitId::try_new(maintenance_uuid("rustfs-vector-maintenance-stage-v1", &bytes).to_string())
}
impl VectorMaintenanceJobSpec {
    pub(crate) fn from_semantic(spec: VectorMaintenanceSemanticSpec) -> TableCatalogStoreResult<Self> {
        let (job_id, request_id) = vector_maintenance_job_ids(&spec)?;
        Ok(Self {
            job_id,
            request_id,
            resource: spec.resource,
            kind: spec.kind,
            origin: spec.origin,
            input: spec.input,
            execution_identity: spec.execution_identity,
            initiator_ref: spec.initiator_ref,
            policy: spec.policy,
        })
    }
    pub(crate) fn job_id(&self) -> Uuid {
        self.job_id
    }
    pub(crate) fn resource(&self) -> &VectorMaintenanceResourceV1 {
        &self.resource
    }
}
impl VectorMaintenanceJobV1 {
    pub(crate) fn semantic_spec(&self) -> VectorMaintenanceSemanticSpec {
        VectorMaintenanceSemanticSpec {
            resource: self.resource.clone(),
            kind: self.kind,
            origin: self.origin,
            input: self.input.clone(),
            execution_identity: self.execution_identity.clone(),
            initiator_ref: self.initiator_ref.clone(),
            policy: self.policy.clone(),
        }
    }
    pub(in crate::table_catalog) fn spec(&self) -> VectorMaintenanceJobSpec {
        VectorMaintenanceJobSpec {
            job_id: self.job_id,
            resource: self.resource.clone(),
            kind: self.kind,
            origin: self.origin,
            request_id: self.request_id,
            input: self.input.clone(),
            execution_identity: self.execution_identity.clone(),
            initiator_ref: self.initiator_ref.clone(),
            policy: self.policy.clone(),
        }
    }
}
impl VectorMaintenanceRecord for VectorMaintenanceJobV1 {
    const MAX_BYTES: usize = VECTOR_MAINTENANCE_JOB_MAX_BYTES;
    fn validate(&self) -> TableCatalogStoreResult<()> {
        ensure(self.version == 1, "unsupported maintenance job version")?;
        self.semantic_spec().validate()?;
        nonnil(self.job_id)?;
        nonnil(self.request_id)?;
        nonnil(self.revision)?;
        nonnil(self.last_update_id)?;
        hash(&self.spec_sha256)?;
        hash(&self.last_update_sha256)?;
        ensure(
            maintenance_uuid_from_digest(decode_catalog_digest(&self.last_update_sha256)?) == self.last_update_id,
            "maintenance latest update ID does not match its digest",
        )?;
        ensure(
            vector_maintenance_job_ids(&self.semantic_spec())? == (self.job_id, self.request_id),
            "maintenance job IDs do not match immutable spec",
        )?;
        let digest =
            hex_simd::encode_to_string(Sha256::digest(maintenance_canonical_bytes(&self.spec())?), hex_simd::AsciiCase::Lower);
        ensure(digest == self.spec_sha256, "maintenance spec digest mismatch")?;
        let created = vector_maintenance_timestamp(&self.created_at)?;
        let updated = vector_maintenance_timestamp(&self.updated_at)?;
        ensure(
            created <= updated && self.attempt <= self.policy.max_attempts,
            "invalid maintenance job time or attempt",
        )?;
        if let Some(value) = &self.next_retry_at {
            vector_maintenance_timestamp(value)?;
        }
        if let Some(value) = &self.finished_at {
            ensure(vector_maintenance_timestamp(value)? >= created, "invalid maintenance finish time")?;
        }
        if let Some(value) = &self.result_ref {
            reference(value)?;
        }
        use VectorMaintenanceJobState as S;
        if self.state == S::Queued {
            return ensure(
                self.attempt == 0
                    && self.lease.is_none()
                    && self.next_retry_at.is_none()
                    && self.finished_at.is_none()
                    && self.failure.is_none()
                    && self.result_ref.is_none()
                    && self.created_at == self.updated_at,
                "invalid queued maintenance state",
            );
        }
        let lease = self
            .lease
            .as_ref()
            .ok_or_else(|| invalid("maintenance execution requires retained lease"))?;
        lease.validate()?;
        ensure(
            self.attempt >= 1
                && vector_maintenance_timestamp(&lease.expires_at)? - vector_maintenance_timestamp(&lease.heartbeat_at)?
                    == Duration::seconds(self.policy.lease_timeout_seconds as i64),
            "maintenance lease duration mismatch",
        )?;
        let valid = match self.state {
            S::Running => {
                self.failure.is_none() && self.result_ref.is_none() && self.finished_at.is_none() && self.next_retry_at.is_none()
            }
            S::RetryScheduled => {
                self.failure.is_some()
                    && self.next_retry_at.is_some()
                    && self.finished_at.is_none()
                    && self.result_ref.is_none()
                    && self.attempt < self.policy.max_attempts
            }
            S::RecoveryRequired => {
                self.failure.is_some() && self.next_retry_at.is_none() && self.finished_at.is_none() && self.result_ref.is_none()
            }
            S::ManualReview | S::Failed => {
                self.failure.is_some() && self.next_retry_at.is_none() && self.finished_at.is_some() && self.result_ref.is_none()
            }
            S::Succeeded => {
                self.failure.is_none() && self.next_retry_at.is_none() && self.finished_at.is_some() && self.result_ref.is_some()
            }
            S::PartialRecovered => {
                self.kind == VectorMaintenanceKind::Compaction
                    && self.failure == Some(VectorMaintenanceFailureCode::PartialPublication)
                    && self.next_retry_at.is_none()
                    && self.finished_at.is_some()
                    && self.result_ref.is_some()
            }
            S::Queued => false,
        };
        ensure(valid, "contradictory maintenance job state")
    }
}
impl VectorMaintenanceRecord for VectorMaintenanceStageResultV1 {
    fn validate(&self) -> TableCatalogStoreResult<()> {
        ensure(
            matches!(self.stage.as_str(), "ann-build" | "reserve-fragments" | "rewrite"),
            "invalid maintenance stage name",
        )?;
        let id = Uuid::parse_str(&self.commit_id).map_err(|_| invalid("invalid stage commit ID"))?;
        ensure(!id.is_nil() && id.to_string() == self.commit_id, "invalid stage commit ID")?;
        if let Some(value) = &self.expected_head {
            value.validate()?;
        }
        if let Some(value) = &self.candidate {
            reference(value)?;
        }
        if let Some(value) = &self.commit_ref {
            reference(value)?;
        }
        use VectorMaintenanceStageOutcome as O;
        ensure(
            match self.outcome {
                O::NotPrepared => self.candidate.is_none() && self.commit_ref.is_none(),
                O::Committed | O::Superseded => {
                    self.expected_head.is_some() && self.candidate.is_some() && self.commit_ref.is_some()
                }
                O::Unpublished => self.expected_head.is_some() && self.candidate.is_some(),
                O::Unknown => true,
            },
            "contradictory maintenance stage result",
        )
    }
}
impl VectorMaintenanceRecord for VectorMaintenanceResultV1 {
    const MAX_BYTES: usize = VECTOR_MAINTENANCE_JOB_MAX_BYTES;
    fn validate(&self) -> TableCatalogStoreResult<()> {
        ensure(
            self.version == 1 && (1..=10).contains(&self.attempt),
            "invalid maintenance result version or attempt",
        )?;
        nonnil(self.job_id)?;
        nonnil(self.recorded_by_job_id)?;
        self.resource.validate()?;
        vector_maintenance_timestamp(&self.finished_at)?;
        ensure(
            self.stages.len() <= 2 && self.commit_refs.len() + self.retirement_refs.len() + self.output_refs.len() <= 256,
            "maintenance result exceeds limit",
        )?;
        for values in [&self.commit_refs, &self.retirement_refs, &self.output_refs] {
            references(values, 256)?;
        }
        for stage in &self.stages {
            stage.validate()?;
            ensure(
                stage.commit_id == vector_maintenance_stage_commit_id(self.job_id, self.attempt, &stage.stage)?.as_str()
                    && stage.outcome != VectorMaintenanceStageOutcome::Unknown,
                "maintenance result has unknown or unrelated stage",
            )?;
        }
        if self.stages.len() == 2 {
            ensure(
                self.stages[0].stage == "reserve-fragments" && self.stages[1].stage == "rewrite",
                "invalid compaction stage order",
            )?;
        }
        if self.stages.len() == 1 {
            ensure(self.stages[0].stage == "ann-build", "incomplete maintenance stage collection")?;
        }
        use VectorMaintenanceResultEffect as E;
        match self.effect {
            E::NoWork => ensure(
                self.recorded_by_job_id == self.job_id
                    && self.reason.is_some()
                    && self.stages.is_empty()
                    && self.commit_refs.is_empty()
                    && self.retirement_refs.is_empty()
                    && self.output_refs.is_empty(),
                "invalid NoWork result",
            ),
            E::Published => ensure(
                self.reason.is_none()
                    && !self.stages.is_empty()
                    && !self.commit_refs.is_empty()
                    && self.stages.iter().all(|s| {
                        matches!(
                            s.outcome,
                            VectorMaintenanceStageOutcome::Committed | VectorMaintenanceStageOutcome::Superseded
                        )
                    }),
                "unproven publication result",
            ),
            E::PartialRecovered => ensure(
                self.reason.is_none()
                    && self.stages.len() == 2
                    && matches!(
                        self.stages[0].outcome,
                        VectorMaintenanceStageOutcome::Committed | VectorMaintenanceStageOutcome::Superseded
                    )
                    && matches!(
                        self.stages[1].outcome,
                        VectorMaintenanceStageOutcome::NotPrepared | VectorMaintenanceStageOutcome::Unpublished
                    )
                    && !self.commit_refs.is_empty(),
                "invalid partial compaction result",
            ),
            E::Recovered => ensure(
                self.reason.is_none()
                    && (!self.commit_refs.is_empty() || !self.output_refs.is_empty() || !self.retirement_refs.is_empty()),
                "recovery result requires evidence",
            ),
            E::Retired => ensure(
                self.reason.is_none()
                    && self.stages.is_empty()
                    && !self.retirement_refs.is_empty()
                    && !self.output_refs.is_empty(),
                "retirement result requires final progress",
            ),
        }
    }
}
impl VectorMaintenanceRecord for VectorObjectTimeV1 {
    fn validate(&self) -> TableCatalogStoreResult<()> {
        self.to_time().map(|_| ())
    }
}
impl VectorObjectTimeV1 {
    pub(crate) fn to_time(&self) -> TableCatalogStoreResult<OffsetDateTime> {
        ensure(self.nanosecond < 1_000_000_000, "invalid object time nanoseconds")?;
        OffsetDateTime::from_unix_timestamp(self.unix_seconds)
            .and_then(|time| time.replace_nanosecond(self.nanosecond))
            .map_err(|e| invalid(e.to_string()))
    }
    pub(crate) fn from_time(value: OffsetDateTime) -> Self {
        Self {
            unix_seconds: value.unix_timestamp(),
            nanosecond: value.nanosecond(),
        }
    }
}
impl VectorMaintenanceRecord for VectorRetirementTargetV1 {
    fn validate(&self) -> TableCatalogStoreResult<()> {
        CatalogObjectKey::try_new(self.object_key.clone())?;
        nonnil(self.body_revision)?;
        text(&self.object_key, 4096)?;
        ensure(
            self.version_id.is_none() && !self.delete_marker,
            "retirement requires an unversioned data object",
        )?;
        if let Some(value) = &self.mod_time {
            value.validate()?;
        }
        if let Some(value) = &self.etag {
            text(value, 4096)?;
        }
        Ok(())
    }
}
fn targets(resource: &VectorMaintenanceResourceV1, values: &[VectorRetirementTargetV1]) -> TableCatalogStoreResult<()> {
    ensure(
        !values.is_empty() && values.len() <= VECTOR_RETIREMENT_MAX_TARGETS,
        "invalid retirement target count",
    )?;
    let prefix = resource.identity()?.dataset_prefix();
    let mut previous: Option<&str> = None;
    for value in values {
        value.validate()?;
        ensure(
            value.object_key.starts_with(&prefix) && previous.is_none_or(|p| p < value.object_key.as_str()),
            "retirement target scope or order mismatch",
        )?;
        previous = Some(&value.object_key);
    }
    Ok(())
}
impl VectorMaintenanceRecord for VectorRetirementEvidenceV1 {
    fn validate(&self) -> TableCatalogStoreResult<()> {
        ensure(self.version == 1, "unsupported retirement evidence version")?;
        self.resource.validate()?;
        self.base_head.validate()?;
        ensure(
            self.base_head.binding.bucket == self.resource.bucket && self.base_head.binding.table_id == self.resource.table_id,
            "retirement evidence resource mismatch",
        )?;
        if let Some(value) = &self.delete_intent_ref {
            reference(value)?;
        }
        ensure(
            self.history_refs.len() + self.protected_refs.len() + self.unresolved_refs.len() <= VECTOR_RETIREMENT_MAX_REFERENCES,
            "retirement evidence exceeds reference limit",
        )?;
        for values in [&self.history_refs, &self.protected_refs, &self.unresolved_refs] {
            references(values, VECTOR_RETIREMENT_MAX_REFERENCES)?;
        }
        if let VectorHistoryRetentionV1::Last { count, min_age_seconds } = self.retention {
            ensure(
                (1..=4096).contains(&count) && (900..=31536000).contains(&min_age_seconds),
                "invalid Vector history retention",
            )?;
        }
        Ok(())
    }
}
impl VectorMaintenanceRecord for VectorRetirementPlanV1 {
    fn validate(&self) -> TableCatalogStoreResult<()> {
        ensure(self.version == 1, "unsupported retirement plan version")?;
        self.resource.validate()?;
        reference(&self.evidence_ref)?;
        targets(&self.resource, &self.targets)
    }
}
impl VectorMaintenanceRecord for VectorRetirementAuthorizationV1 {
    fn validate(&self) -> TableCatalogStoreResult<()> {
        ensure(
            self.version == 1 && (1..=10).contains(&self.created_by_attempt),
            "invalid retirement authorization version or attempt",
        )?;
        nonnil(self.retirement_id)?;
        nonnil(self.retirement_revision)?;
        nonnil(self.created_by_job_id)?;
        self.resource.validate()?;
        self.execution_identity.validate()?;
        vector_maintenance_timestamp(&self.created_at)?;
        self.base_head.validate()?;
        ensure(
            self.base_head.binding.bucket == self.resource.bucket && self.base_head.binding.table_id == self.resource.table_id,
            "authorization resource mismatch",
        )?;
        for value in [&self.previous_ref, &self.delete_intent_ref].into_iter().flatten() {
            reference(value)?;
        }
        reference(&self.plan_ref)?;
        reference(&self.reference_evidence_ref)?;
        targets(&self.resource, &self.targets)
    }
}
impl VectorMaintenanceRecord for VectorRetirementDispatchV1 {
    fn validate(&self) -> TableCatalogStoreResult<()> {
        for id in [
            self.dispatch_id,
            self.job_id,
            self.job_revision,
            self.lease_id,
            self.worker_boot_id,
        ] {
            nonnil(id)?;
        }
        ensure((1..=10).contains(&self.attempt), "invalid retirement attempt")?;
        text(&self.worker_id, 4096)?;
        self.execution_identity.validate()?;
        vector_maintenance_timestamp(&self.admitted_at)?;
        Ok(())
    }
}
impl VectorMaintenanceRecord for VectorDeleteAccountingV1 {
    fn validate(&self) -> TableCatalogStoreResult<()> {
        ensure(self.version_id.is_none(), "managed deletion accounting must be unversioned")
    }
}
impl VectorMaintenanceRecord for VectorRetirementReceiptV1 {
    fn validate(&self) -> TableCatalogStoreResult<()> {
        vector_maintenance_timestamp(&self.completed_at)?;
        ensure(
            self.recovery_pending == 0 && self.recovery_blocked == 0 && self.recovery_failed == 0,
            "retirement receipt has unresolved recovery",
        )?;
        match self.outcome {
            VectorRetirementReceiptOutcome::DeleteCommitted => self
                .accounting
                .as_ref()
                .ok_or_else(|| invalid("committed deletion requires accounting"))?
                .validate(),
            VectorRetirementReceiptOutcome::AbsentAfterRecovery => {
                ensure(self.accounting.is_none(), "absence receipt cannot invent accounting")
            }
        }
    }
}
impl VectorMaintenanceRecord for VectorRetirementTargetProgressV1 {
    fn validate(&self) -> TableCatalogStoreResult<()> {
        ensure(self.ordinal < 256, "retirement ordinal exceeds limit")?;
        if let Some(value) = &self.dispatch {
            value.validate()?;
        }
        if let Some(value) = &self.receipt {
            value.validate()?;
        }
        use VectorRetirementTargetState as S;
        ensure(
            match self.state {
                S::Pending => self.dispatch.is_none() && self.receipt.is_none() && self.failure.is_none(),
                S::InFlight => self.dispatch.is_some() && self.receipt.is_none() && self.failure.is_none(),
                S::Unknown => self.dispatch.is_some() && self.receipt.is_none() && self.failure.is_some(),
                S::DeleteCommitted => {
                    self.dispatch.is_some()
                        && self.failure.is_none()
                        && self
                            .receipt
                            .as_ref()
                            .is_some_and(|r| r.outcome == VectorRetirementReceiptOutcome::DeleteCommitted)
                }
                S::AbsentAfterRecovery => {
                    self.dispatch.is_some()
                        && self.failure.is_none()
                        && self
                            .receipt
                            .as_ref()
                            .is_some_and(|r| r.outcome == VectorRetirementReceiptOutcome::AbsentAfterRecovery)
                }
            },
            "contradictory retirement target progress",
        )
    }
}
impl VectorMaintenanceRecord for VectorRetirementProgressV1 {
    const MAX_BYTES: usize = VECTOR_RETIREMENT_PROGRESS_MAX_BYTES;
    fn validate(&self) -> TableCatalogStoreResult<()> {
        ensure(
            self.version == 1 && !self.targets.is_empty() && self.targets.len() <= 256,
            "invalid retirement progress version or count",
        )?;
        self.resource.validate()?;
        reference(&self.authorization_ref)?;
        for id in [self.retirement_id, self.revision, self.last_update_id] {
            nonnil(id)?;
        }
        hash(&self.last_update_sha256)?;
        ensure(
            maintenance_uuid_from_digest(decode_catalog_digest(&self.last_update_sha256)?) == self.last_update_id,
            "maintenance latest update ID does not match its digest",
        )?;
        ensure(
            vector_maintenance_timestamp(&self.created_at)? <= vector_maintenance_timestamp(&self.updated_at)?,
            "invalid retirement progress times",
        )?;
        for (ordinal, value) in self.targets.iter().enumerate() {
            value.validate()?;
            ensure(usize::from(value.ordinal) == ordinal, "retirement progress ordinal mismatch")?;
        }
        let all_pending = self.targets.iter().all(|t| t.state == VectorRetirementTargetState::Pending);
        let all_completed = self.targets.iter().all(|t| {
            matches!(
                t.state,
                VectorRetirementTargetState::DeleteCommitted | VectorRetirementTargetState::AbsentAfterRecovery
            )
        });
        ensure(
            match self.state {
                VectorRetirementProgressState::Authorized => all_pending,
                VectorRetirementProgressState::Deleting => !all_pending && !all_completed,
                VectorRetirementProgressState::Completed => all_completed,
                VectorRetirementProgressState::ManualReview => !all_completed,
            },
            "contradictory retirement progress state",
        )
    }
}

#[cfg(test)]
#[path = "vector_maintenance_model_tests.rs"]
mod tests;

pub(crate) type VectorResourceMutationKindV2 = VectorResourceMutationKind;
maintenance_record! { VectorResourceMutationBindingV2 {
    version: u16,
    kind: VectorResourceMutationKindV2,
    expected_definition: VectorIndexDefinitionV1,
    next_definition: VectorIndexDefinitionV1,
    expected_state: TableCatalogEntryState,
    next_state: TableCatalogEntryState,
    expected_maintenance: Option<VectorMaintenanceBindingV1>,
    next_maintenance: VectorMaintenanceBindingV1,
} }
impl VectorMaintenanceRecord for VectorResourceMutationBindingV2 {
    fn validate(&self) -> TableCatalogStoreResult<()> {
        ensure(
            self.version == 2 && self.expected_state == TableCatalogEntryState::Active,
            "invalid Vector v2 mutation version or state",
        )?;
        self.expected_definition.validate()?;
        self.next_definition.validate()?;
        self.next_maintenance.validate()?;
        if let Some(binding) = &self.expected_maintenance {
            binding.validate()?;
        }
        let same_definition = self.expected_definition == self.next_definition;
        let same_maintenance = self.expected_maintenance.as_ref() == Some(&self.next_maintenance);
        let active = self.next_state == TableCatalogEntryState::Active;
        use VectorResourceMutationKind as K;
        let valid = match self.kind {
            K::Enroll => {
                active
                    && same_definition
                    && self.expected_maintenance.is_none()
                    && self.next_maintenance == VectorMaintenanceBindingV1::empty()
            }
            K::Retire => {
                active
                    && same_definition
                    && self.expected_maintenance.as_ref().is_some_and(|old| {
                        old.retirement_count.checked_add(1) == Some(self.next_maintenance.retirement_count)
                            && self.next_maintenance.retirement_tip.is_some()
                    })
            }
            K::Dataset => active && same_definition && same_maintenance,
            K::Tags => {
                active
                    && same_maintenance
                    && self.expected_definition.effective_encryption == self.next_definition.effective_encryption
            }
            K::Delete => self.next_state == TableCatalogEntryState::Deleted && same_definition && same_maintenance,
        };
        ensure(valid, "invalid Vector v2 resource mutation")
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub(crate) enum VectorResourceMutationBinding {
    V1(VectorResourceMutationBindingV1),
    V2(Box<VectorResourceMutationBindingV2>),
}
impl<'de> Deserialize<'de> for VectorResourceMutationBinding {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let StrictMaintenanceJson(value) = StrictMaintenanceJson::deserialize(deserializer)?;
        match value.get("version").and_then(Value::as_u64) {
            Some(1) => serde_json::from_value(value).map(Self::V1).map_err(D::Error::custom),
            Some(2) => serde_json::from_value(value).map(Self::V2).map_err(D::Error::custom),
            _ => Err(D::Error::custom("unsupported Vector resource mutation version")),
        }
    }
}
impl From<VectorResourceMutationBindingV1> for VectorResourceMutationBinding {
    fn from(value: VectorResourceMutationBindingV1) -> Self {
        Self::V1(value)
    }
}
impl From<VectorResourceMutationBindingV2> for VectorResourceMutationBinding {
    fn from(value: VectorResourceMutationBindingV2) -> Self {
        Self::V2(Box::new(value))
    }
}
impl VectorResourceMutationBinding {
    pub(in crate::table_catalog) fn validate(&self) -> TableCatalogStoreResult<()> {
        match self {
            Self::V1(value) => value.validate(),
            Self::V2(value) => value.validate(),
        }
    }
    pub(in crate::table_catalog) fn kind(&self) -> VectorResourceMutationKind {
        match self {
            Self::V1(value) => value.kind,
            Self::V2(value) => value.kind,
        }
    }
    pub(in crate::table_catalog) fn expected_definition(&self) -> &VectorIndexDefinitionV1 {
        match self {
            Self::V1(value) => &value.expected_definition,
            Self::V2(value) => &value.expected_definition,
        }
    }
    pub(in crate::table_catalog) fn next_definition(&self) -> &VectorIndexDefinitionV1 {
        match self {
            Self::V1(value) => &value.next_definition,
            Self::V2(value) => &value.next_definition,
        }
    }
    pub(in crate::table_catalog) fn expected_state(&self) -> &TableCatalogEntryState {
        match self {
            Self::V1(value) => &value.expected_state,
            Self::V2(value) => &value.expected_state,
        }
    }
    pub(in crate::table_catalog) fn next_state(&self) -> &TableCatalogEntryState {
        match self {
            Self::V1(value) => &value.next_state,
            Self::V2(value) => &value.next_state,
        }
    }
    pub(in crate::table_catalog) fn expected_maintenance(&self) -> Option<&VectorMaintenanceBindingV1> {
        match self {
            Self::V1(_) => None,
            Self::V2(value) => value.expected_maintenance.as_ref(),
        }
    }
    pub(in crate::table_catalog) fn next_maintenance(&self) -> Option<&VectorMaintenanceBindingV1> {
        match self {
            Self::V1(_) => None,
            Self::V2(value) => Some(&value.next_maintenance),
        }
    }
    pub(in crate::table_catalog) fn entry_version(&self) -> u16 {
        match self {
            Self::V1(_) => 3,
            Self::V2(_) => 4,
        }
    }
    pub(in crate::table_catalog) fn validate_metadata(
        &self,
        expected: &CatalogMetadataRef,
        expected_descriptor: &CatalogFormatDescriptor,
        candidate: &CatalogMetadataRef,
        descriptor: &CatalogFormatDescriptor,
    ) -> TableCatalogStoreResult<()> {
        if let Self::V1(value) = self {
            return value.validate_metadata(expected, expected_descriptor, candidate, descriptor);
        }
        self.validate()?;
        ensure(
            self.kind() == VectorResourceMutationKind::Dataset || (expected == candidate && expected_descriptor == descriptor),
            "resource mutation changed Dataset metadata",
        )
    }
    pub(in crate::table_catalog) fn request_digest(
        &self,
        prepared: &PreparedCatalogCommit,
    ) -> TableCatalogStoreResult<CatalogRequestDigest> {
        match self {
            Self::V1(value) => value.request_digest(prepared),
            Self::V2(value) => {
                value.validate()?;
                let bytes = maintenance_canonical_bytes(
                    &serde_json::json!({"domain":"rustfs.vector.catalog-mutation.v2","prepared_sha256":prepared.request_digest().to_hex(),"resource_mutation":value}),
                )?;
                Ok(CatalogRequestDigest::from_bytes(Sha256::digest(bytes).into()))
            }
        }
    }
}

impl VectorMaintenanceStageResultV1 {
    pub(crate) fn try_new(
        stage: String,
        commit_id: CatalogCommitId,
        expected_head: Option<VectorMaintenanceHeadV1>,
        candidate: Option<CatalogMetadataRefRecordV1>,
        outcome: VectorMaintenanceStageOutcome,
        commit_ref: Option<CatalogMetadataRefRecordV1>,
    ) -> TableCatalogStoreResult<Self> {
        let value = Self {
            stage,
            commit_id: commit_id.as_str().to_string(),
            expected_head,
            candidate,
            outcome,
            commit_ref,
        };
        value.validate()?;
        Ok(value)
    }
}
impl VectorRetirementTargetV1 {
    pub(crate) fn from_object_metadata(
        resource: &CatalogResourceIdentity,
        object_key: CatalogObjectKey,
        metadata: &TableCatalogObjectMetadata,
    ) -> TableCatalogStoreResult<Self> {
        if !object_key.as_str().starts_with(&resource.dataset_prefix()) {
            return Err(invalid("retirement object is outside the resource"));
        }
        let value = Self {
            object_key: object_key.as_str().to_string(),
            body_revision: metadata
                .body_revision
                .ok_or_else(|| invalid("retirement target requires body revision"))?,
            mod_time: metadata.mod_time.map(VectorObjectTimeV1::from_time),
            content_length: metadata.size,
            etag: metadata.etag.clone(),
            version_id: None,
            delete_marker: false,
        };
        value.validate()?;
        Ok(value)
    }
}
impl VectorRetirementEvidenceV1 {
    pub(crate) fn try_new(
        resource: VectorMaintenanceResourceV1,
        base_head: VectorMaintenanceHeadV1,
        delete_intent_ref: Option<CatalogMetadataRefRecordV1>,
        history_refs: Vec<CatalogMetadataRefRecordV1>,
        protected_refs: Vec<CatalogMetadataRefRecordV1>,
        unresolved_refs: Vec<CatalogMetadataRefRecordV1>,
        retention: VectorHistoryRetentionV1,
    ) -> TableCatalogStoreResult<Self> {
        let value = Self {
            version: 1,
            resource,
            base_head,
            delete_intent_ref,
            history_refs,
            protected_refs,
            unresolved_refs,
            retention,
        };
        value.validate()?;
        Ok(value)
    }
}
impl VectorDeleteAccountingV1 {
    pub(crate) fn try_new(
        size: Option<u64>,
        version_id: Option<Uuid>,
        removed_current_object: bool,
    ) -> TableCatalogStoreResult<Self> {
        let value = Self {
            size,
            version_id,
            removed_current_object,
        };
        value.validate()?;
        Ok(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VectorIndexDeleteIntentV2 {
    pub(super) deletion: VectorIndexDeleteIntentV1,
    pub(super) maintenance_binding: VectorMaintenanceBindingV1,
    pub(super) maintenance_revision: Uuid,
    pub(super) last_maintenance_update_id: Uuid,
    pub(super) last_maintenance_update_sha256: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub(crate) enum VectorIndexDeleteIntent {
    V1(VectorIndexDeleteIntentV1),
    V2(VectorIndexDeleteIntentV2),
}
impl std::ops::Deref for VectorIndexDeleteIntent {
    type Target = VectorIndexDeleteIntentV1;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::V1(value) => value,
            Self::V2(value) => &value.deletion,
        }
    }
}
impl std::ops::DerefMut for VectorIndexDeleteIntent {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::V1(value) => value,
            Self::V2(value) => &mut value.deletion,
        }
    }
}
impl Serialize for VectorIndexDeleteIntentV2 {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::Error as _;
        let mut value = serde_json::to_value(&self.deletion).map_err(S::Error::custom)?;
        let fields = value
            .as_object_mut()
            .ok_or_else(|| S::Error::custom("delete intent requires an object"))?;
        fields.insert(
            "maintenance_binding".into(),
            serde_json::to_value(&self.maintenance_binding).map_err(S::Error::custom)?,
        );
        fields.insert("maintenance_revision".into(), self.maintenance_revision.to_string().into());
        fields.insert("last_maintenance_update_id".into(), self.last_maintenance_update_id.to_string().into());
        fields.insert(
            "last_maintenance_update_sha256".into(),
            self.last_maintenance_update_sha256.clone().into(),
        );
        value.serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for VectorIndexDeleteIntent {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let StrictMaintenanceJson(mut value) = StrictMaintenanceJson::deserialize(deserializer)?;
        let version = value
            .get("version")
            .and_then(Value::as_u64)
            .ok_or_else(|| D::Error::custom("missing delete intent version"))?;
        let record = match version {
            1 => Self::V1(serde_json::from_value(value).map_err(D::Error::custom)?),
            2 => {
                validate_json(&value, 0).map_err(D::Error::custom)?;
                let fields = value
                    .as_object_mut()
                    .ok_or_else(|| D::Error::custom("delete intent requires an object"))?;
                let maintenance_binding = decode_field(
                    fields
                        .remove("maintenance_binding")
                        .ok_or_else(|| D::Error::custom("missing maintenance_binding"))?,
                )
                .map_err(D::Error::custom)?;
                let maintenance_revision = decode_field(
                    fields
                        .remove("maintenance_revision")
                        .ok_or_else(|| D::Error::custom("missing maintenance_revision"))?,
                )
                .map_err(D::Error::custom)?;
                let last_maintenance_update_id = decode_field(
                    fields
                        .remove("last_maintenance_update_id")
                        .ok_or_else(|| D::Error::custom("missing last_maintenance_update_id"))?,
                )
                .map_err(D::Error::custom)?;
                let last_maintenance_update_sha256 = decode_field(
                    fields
                        .remove("last_maintenance_update_sha256")
                        .ok_or_else(|| D::Error::custom("missing last_maintenance_update_sha256"))?,
                )
                .map_err(D::Error::custom)?;
                Self::V2(VectorIndexDeleteIntentV2 {
                    deletion: serde_json::from_value(value).map_err(D::Error::custom)?,
                    maintenance_binding,
                    maintenance_revision,
                    last_maintenance_update_id,
                    last_maintenance_update_sha256,
                })
            }
            _ => return Err(D::Error::custom("unsupported Vector delete intent version")),
        };
        record.validate().map_err(D::Error::custom)?;
        Ok(record)
    }
}
impl VectorIndexDeleteIntentV2 {
    fn initial_fields(
        deletion: &VectorIndexDeleteIntentV1,
    ) -> TableCatalogStoreResult<(VectorMaintenanceBindingV1, Uuid, Uuid, String)> {
        let binding = deletion
            .expected_entry
            .vector_maintenance_binding
            .clone()
            .unwrap_or_else(VectorMaintenanceBindingV1::empty);
        let value = serde_json::json!({"delete_id":deletion.delete_id,"expected_entry":deletion.expected_entry,"maintenance_binding":binding});
        let bytes = maintenance_canonical_bytes(&value)?;
        let revision = maintenance_uuid("rustfs-vector-delete-maintenance-initial-v2", &bytes);
        let digest = CatalogRequestDigest::from_bytes(Sha256::digest(&bytes).into()).to_hex();
        let update_id = maintenance_uuid_from_digest(decode_catalog_digest(&digest)?);
        Ok((binding, revision, update_id, digest))
    }
    fn initial_request_digest(deletion: &VectorIndexDeleteIntentV1) -> TableCatalogStoreResult<CatalogRequestDigest> {
        if deletion.expected_entry.version == 3 {
            return deletion.canonical_request_digest();
        }
        let (binding, revision, update_id, digest) = Self::initial_fields(deletion)?;
        let value = serde_json::json!({"delete_commit_request_sha256":deletion.canonical_request_digest()?.to_hex(),"maintenance_binding":binding,"maintenance_revision":revision,"last_maintenance_update_id":update_id,"last_maintenance_update_sha256":digest});
        Ok(CatalogRequestDigest::from_bytes(
            Sha256::digest(maintenance_canonical_bytes(&value)?).into(),
        ))
    }
    pub(in crate::table_catalog) fn from_deletion(mut deletion: VectorIndexDeleteIntentV1) -> TableCatalogStoreResult<Self> {
        let (maintenance_binding, maintenance_revision, last_maintenance_update_id, last_maintenance_update_sha256) =
            Self::initial_fields(&deletion)?;
        deletion.version = 2;
        deletion.request_sha256 = Self::initial_request_digest(&deletion)?.to_hex();
        let value = Self {
            deletion,
            maintenance_binding,
            maintenance_revision,
            last_maintenance_update_id,
            last_maintenance_update_sha256,
        };
        value.validate()?;
        Ok(value)
    }
    pub(in crate::table_catalog) fn validate(&self) -> TableCatalogStoreResult<()> {
        ensure(self.deletion.version == 2, "unsupported Vector delete intent v2")?;
        self.maintenance_binding.validate()?;
        nonnil(self.maintenance_revision)?;
        nonnil(self.last_maintenance_update_id)?;
        hash(&self.last_maintenance_update_sha256)?;
        ensure(
            maintenance_uuid_from_digest(decode_catalog_digest(&self.last_maintenance_update_sha256)?)
                == self.last_maintenance_update_id,
            "delete maintenance update digest mismatch",
        )?;
        self.deletion
            .validate_transition(Self::initial_request_digest(&self.deletion)?)?;
        if self.deletion.phase == VectorIndexDeletePhase::Prepared {
            let (binding, revision, update_id, digest) = Self::initial_fields(&self.deletion)?;
            ensure(
                self.deletion.expected_entry.version == 4
                    && self.maintenance_binding == binding
                    && self.maintenance_revision == revision
                    && self.last_maintenance_update_id == update_id
                    && self.last_maintenance_update_sha256 == digest,
                "prepared deletion maintenance fields differ",
            )?;
        }
        Ok(())
    }
}
impl VectorIndexDeleteIntent {
    pub(in crate::table_catalog) fn validate(&self) -> TableCatalogStoreResult<()> {
        match self {
            Self::V1(value) => value.validate(),
            Self::V2(value) => value.validate(),
        }
    }
    pub(in crate::table_catalog) fn maintenance_binding(&self) -> Option<&VectorMaintenanceBindingV1> {
        match self {
            Self::V1(_) => None,
            Self::V2(value) => Some(&value.maintenance_binding),
        }
    }
}

impl VectorMaintenanceHeadV1 {
    /// Reconstruct a candidate successor; publication still requires complete Catalog history proof.
    pub(crate) fn from_stage_commit(expected: &Self, log: &CommitLogEntry) -> TableCatalogStoreResult<Self> {
        expected.validate()?;
        let binding = log
            .vector_commit_binding
            .as_ref()
            .ok_or_else(|| invalid("stage commit lacks Vector binding"))?;
        binding.validate_log(log, &expected.binding.bucket.storage_bucket_id, &expected.binding.table_id)?;
        let mutation = log
            .vector_resource_mutation
            .as_ref()
            .ok_or_else(|| invalid("stage commit lacks resource mutation"))?;
        ensure(
            expected.entry_version == log.version
                && expected.state == TableCatalogEntryState::Active
                && log.status != CommitLogStatus::Failed
                && log.expected_version_token == expected.version_token
                && binding.expected_generation == expected.generation
                && binding.expected_metadata.as_ref() == Some(&expected.binding.metadata)
                && binding.expected_descriptor.as_ref() == Some(&expected.binding.descriptor)
                && mutation.kind() == VectorResourceMutationKind::Dataset
                && mutation.expected_maintenance() == expected.maintenance.as_ref()
                && mutation.next_maintenance() == expected.maintenance.as_ref(),
            "stage commit differs from exact predecessor",
        )?;
        let mut successor = expected.clone();
        successor.version_token = log.new_version_token.clone();
        successor.generation = expected
            .generation
            .checked_add(1)
            .ok_or_else(|| invalid("stage generation overflow"))?;
        successor.binding.metadata = binding.candidate_metadata.clone();
        successor.binding.descriptor = binding.descriptor.clone();
        successor.validate()?;
        Ok(successor)
    }
}
