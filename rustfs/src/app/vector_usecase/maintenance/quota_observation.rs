// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Bounded, read-only quota evidence for an original SDK candidate receipt.

use super::*;
use crate::app::storage_api::test::bucket::quota::BucketQuota;
use crate::app::storage_api::test::contract::{namespace::NamespaceLocking, object::ObjectIO};
use crate::app::storage_api::test::quota_observation::{self as storage, IdentityReadGuard};
use crate::app::storage_api::test::{RUSTFS_META_BUCKET, StorageObjectOptions};
use crate::vector::fault_test::SdkCandidateReceipt;
use serde::{Deserialize, Serializer};
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;

const MAX_RAW_BYTES: usize = 1024 * 1024;
const MAX_RECORD_BYTES: usize = 64 * 1024;
const SHARD_COUNT: u16 = 16;

#[derive(Debug, Serialize)]
pub(crate) struct CandidateQuotaSnapshot {
    pub(crate) observation_token: Uuid,
    pub(crate) instance_id: Uuid,
    pub(crate) bucket: VectorBucketBindingV1,
    pub(crate) table_id: String,
    pub(crate) observed_at: String,
    pub(crate) quota_config_base64: String,
    pub(crate) quota_config_sha256: String,
    pub(crate) quota_revision_unix_nanos: String,
    pub(crate) protocol: Option<u32>,
    pub(crate) quota_limit: Option<u64>,
    /// Structural and identity validation only; this does not declare D10 passed.
    pub(crate) validation: bool,
    pub(crate) issues: Vec<QuotaIssue>,
    pub(crate) records: Vec<QuotaRecordObservation>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum QuotaIssueCode {
    QuotaMissing,
    UnsupportedProtocol,
    MalformedRecord,
    MissingRecord,
    UnsupportedVersion,
    IdentityMismatch,
    InvalidAccounting,
    InvalidGrant,
    ReadFailed,
}

#[derive(Debug, Serialize)]
pub(crate) struct QuotaIssue {
    object_key: Option<String>,
    code: QuotaIssueCode,
    #[serde(serialize_with = "serialize_optional_error")]
    error: Option<VectorMaintenanceExecutionError>,
}

#[derive(Debug, Serialize)]
pub(crate) struct QuotaRecordObservation {
    pub(crate) object_key: String,
    pub(crate) absent: bool,
    pub(crate) body_revision: Option<Uuid>,
    pub(crate) etag: Option<String>,
    pub(crate) mod_time: Option<String>,
    pub(crate) declared_length: Option<i64>,
    pub(crate) observed_length: usize,
    pub(crate) complete_body: bool,
    pub(crate) content_sha256: Option<String>,
    pub(crate) content_base64: Option<String>,
    #[serde(serialize_with = "serialize_optional_error")]
    error: Option<VectorMaintenanceExecutionError>,
}

impl QuotaRecordObservation {
    fn retain_read_error(&mut self, error: storage::StorageError) {
        self.absent = storage::is_err_object_not_found(&error);
        self.error = Some(maintenance_execution_error(error));
    }
}

fn serialize_optional_error<S: Serializer>(
    error: &Option<VectorMaintenanceExecutionError>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let value = error.as_ref().map(|error| {
        let mut sources = Vec::new();
        let mut current: Option<&(dyn std::error::Error + 'static)> = Some(error);
        while let Some(source) = current {
            sources.push(source.to_string());
            current = source.source();
        }
        serde_json::json!({"category":error.category(),"sources":sources})
    });
    value.serialize(serializer)
}

impl CandidateQuotaSnapshot {
    fn issue(&mut self, object_key: Option<&str>, code: QuotaIssueCode, error: Option<VectorMaintenanceExecutionError>) {
        self.validation = false;
        self.issues.push(QuotaIssue {
            object_key: object_key.map(str::to_owned),
            code,
            error,
        });
    }
}

fn sha256(bytes: &[u8]) -> String {
    hex_simd::encode_to_string(Sha256::digest(bytes), hex_simd::AsciiCase::Lower)
}

fn check_guards(
    identity: &IdentityReadGuard,
    guards: &[rustfs_lock::NamespaceLockGuard],
) -> Result<(), VectorMaintenanceExecutionError> {
    identity.checked_bucket_incarnation().map_err(maintenance_execution_error)?;
    if guards.iter().any(|guard| guard.is_lock_lost() || guard.is_released()) {
        return Err(conflict("quota observation lost its namespace protection"));
    }
    Ok(())
}

async fn read_record(store: &Arc<storage::ECStore>, key: &str, remaining: &mut usize) -> QuotaRecordObservation {
    let mut record = QuotaRecordObservation {
        object_key: key.to_owned(),
        absent: false,
        body_revision: None,
        etag: None,
        mod_time: None,
        declared_length: None,
        observed_length: 0,
        complete_body: false,
        content_sha256: None,
        content_base64: None,
        error: None,
    };
    let options = StorageObjectOptions {
        no_lock: true,
        suppress_read_repair: true,
        ..Default::default()
    };
    let mut reader = match store
        .get_object_reader(RUSTFS_META_BUCKET, key, None, http::HeaderMap::new(), &options)
        .await
    {
        Ok(reader) => reader,
        Err(error) => {
            record.retain_read_error(error);
            return record;
        }
    };
    record.body_revision = reader.object_info.data_dir;
    record.etag = reader.object_info.etag.clone();
    record.mod_time = reader.object_info.mod_time.map(|time| time.to_string());
    record.declared_length = Some(reader.object_info.size);
    let limit = (*remaining).min(MAX_RECORD_BYTES);
    if usize::try_from(reader.object_info.size)
        .ok()
        .is_none_or(|length| length > limit)
    {
        record.error = Some(invalid("quota record exceeds the observation byte budget"));
        return record;
    }
    let mut bytes = Vec::new();
    let read_limit = u64::try_from(limit).expect("the 64 KiB observation limit fits u64") + 1;
    let read = (&mut reader).take(read_limit).read_to_end(&mut bytes).await;
    if bytes.len() > limit {
        bytes.truncate(limit);
        record.error = Some(invalid("quota record grew beyond the observation byte budget"));
    } else if let Err(error) = read {
        record.error = Some(maintenance_execution_error(error));
    } else if i64::try_from(bytes.len()).ok() != record.declared_length {
        record.error = Some(invalid("quota record length differs from its storage metadata"));
    } else {
        record.complete_body = true;
    }
    *remaining -= bytes.len();
    record.observed_length = bytes.len();
    record.content_sha256 = Some(sha256(&bytes));
    record.content_base64 = Some(base64_simd::STANDARD.encode_to_string(&bytes));
    record
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Reservation {
    object: String,
    old_size: u64,
    new_size: u64,
    created_at: i64,
    #[serde(default)]
    pool_index: Option<usize>,
    #[serde(default)]
    set_index: Option<usize>,
    #[serde(default)]
    commit_started: bool,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SingleLedger {
    version: u8,
    bucket_incarnation: Uuid,
    quota_revision_unix_nanos: i128,
    #[serde(default)]
    writer_revision: Uuid,
    accounted_usage: u64,
    reservations: BTreeMap<Uuid, Reservation>,
    #[serde(default)]
    reconcile_required: bool,
    #[serde(default)]
    reap_cursor: Option<Uuid>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Grant {
    shard_index: u16,
    amount: u64,
    #[serde(default)]
    initial_usage: u64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Allocator {
    version: u8,
    bucket_incarnation: Uuid,
    quota_revision_unix_nanos: i128,
    quota_limit: u64,
    generation: u64,
    grants: BTreeMap<Uuid, Grant>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Shard {
    version: u8,
    bucket_incarnation: Uuid,
    quota_revision_unix_nanos: i128,
    shard_index: u16,
    allocator_generation: u64,
    accounted_usage: u64,
    grants: BTreeMap<Uuid, u64>,
    reservations: BTreeMap<Uuid, Reservation>,
    #[serde(default)]
    reconcile_required: bool,
    #[serde(default)]
    reap_cursor: Option<Uuid>,
}

fn decode_record<T: serde::de::DeserializeOwned>(snapshot: &mut CandidateQuotaSnapshot, ordinal: usize) -> Option<T> {
    let record = &snapshot.records[ordinal];
    let key = record.object_key.clone();
    if record.absent {
        snapshot.issue(Some(&key), QuotaIssueCode::MissingRecord, None);
        return None;
    }
    if !record.complete_body || record.error.is_some() {
        snapshot.issue(Some(&key), QuotaIssueCode::ReadFailed, None);
        return None;
    }
    let decoded = record
        .content_base64
        .as_ref()
        .and_then(|body| base64_simd::STANDARD.decode_to_vec(body.as_bytes()).ok());
    let Some(bytes) = decoded else {
        snapshot.issue(Some(&key), QuotaIssueCode::MalformedRecord, None);
        return None;
    };
    match serde_json::from_slice(&bytes) {
        Ok(value) => Some(value),
        Err(error) => {
            snapshot.issue(
                Some(&key),
                QuotaIssueCode::MalformedRecord,
                Some(VectorMaintenanceExecutionError::Executor {
                    category: VectorMaintenanceFailureCode::CorruptRecord,
                    source: Box::new(error),
                }),
            );
            None
        }
    }
}

fn valid_identity(
    snapshot: &mut CandidateQuotaSnapshot,
    key: &str,
    version: u8,
    incarnation: Uuid,
    revision: i128,
    expected_revision: i128,
) {
    if version != 1 {
        snapshot.issue(Some(key), QuotaIssueCode::UnsupportedVersion, None);
    }
    if incarnation != snapshot.bucket.bucket_incarnation || revision != expected_revision {
        snapshot.issue(Some(key), QuotaIssueCode::IdentityMismatch, None);
    }
}

fn reservations_fit(reservations: &BTreeMap<Uuid, Reservation>, usage: u64) -> bool {
    reservations
        .iter()
        .try_fold(0_u64, |sum, (id, reservation)| {
            if id.is_nil() || reservation.object.is_empty() || reservation.pool_index.is_some() != reservation.set_index.is_some()
            {
                return None;
            }
            sum.checked_add(reservation.new_size.saturating_sub(reservation.old_size))
        })
        .is_some_and(|reserved| reserved <= usage)
}

fn validate_records(snapshot: &mut CandidateQuotaSnapshot, revision: i128) {
    if snapshot.protocol == Some(1) {
        let Some(ledger) = decode_record::<SingleLedger>(snapshot, 0) else {
            return;
        };
        let key = snapshot.records[0].object_key.clone();
        valid_identity(
            snapshot,
            &key,
            ledger.version,
            ledger.bucket_incarnation,
            ledger.quota_revision_unix_nanos,
            revision,
        );
        if !reservations_fit(&ledger.reservations, ledger.accounted_usage) {
            snapshot.issue(Some(&key), QuotaIssueCode::InvalidAccounting, None);
        }
        return;
    }
    let Some(allocator) = decode_record::<Allocator>(snapshot, 0) else {
        return;
    };
    let key = snapshot.records[0].object_key.clone();
    valid_identity(
        snapshot,
        &key,
        allocator.version,
        allocator.bucket_incarnation,
        allocator.quota_revision_unix_nanos,
        revision,
    );
    if Some(allocator.quota_limit) != snapshot.quota_limit {
        snapshot.issue(Some(&key), QuotaIssueCode::IdentityMismatch, None);
    }
    let issued = allocator.grants.iter().try_fold(0_u64, |sum, (id, grant)| {
        if id.is_nil() || grant.shard_index >= SHARD_COUNT || grant.initial_usage > grant.amount {
            return None;
        }
        sum.checked_add(grant.amount)
    });
    if issued.is_none_or(|issued| issued > allocator.quota_limit) {
        snapshot.issue(Some(&key), QuotaIssueCode::InvalidGrant, None);
    }
    for index in 0..SHARD_COUNT {
        let ordinal = usize::from(index) + 1;
        let key = snapshot.records[ordinal].object_key.clone();
        if snapshot.records[ordinal].absent {
            // Bootstrap can retain positive initial usage before a shard is materialized.
            // Preserve the absence; never turn its initial grants into a fabricated ledger.
            if allocator.grants.values().any(|grant| grant.shard_index == index) {
                snapshot.issue(Some(&key), QuotaIssueCode::MissingRecord, None);
            }
            continue;
        }
        let Some(shard) = decode_record::<Shard>(snapshot, ordinal) else {
            continue;
        };
        valid_identity(
            snapshot,
            &key,
            shard.version,
            shard.bucket_incarnation,
            shard.quota_revision_unix_nanos,
            revision,
        );
        if shard.shard_index != index || shard.allocator_generation > allocator.generation {
            snapshot.issue(Some(&key), QuotaIssueCode::IdentityMismatch, None);
        }
        let credit = shard.grants.iter().try_fold(0_u64, |sum, (id, amount)| {
            let grant = allocator.grants.get(id)?;
            if grant.shard_index != index || grant.amount != *amount {
                return None;
            }
            sum.checked_add(*amount)
        });
        let unobserved_initial_usage = allocator
            .grants
            .iter()
            .any(|(id, grant)| grant.shard_index == index && grant.initial_usage > 0 && !shard.grants.contains_key(id));
        if credit.is_none_or(|credit| shard.accounted_usage > credit)
            || unobserved_initial_usage
            || !reservations_fit(&shard.reservations, shard.accounted_usage)
        {
            snapshot.issue(Some(&key), QuotaIssueCode::InvalidAccounting, None);
        }
    }
}

/// The caller performs current IAM/resource authorization before and after this observation.
/// No Catalog, candidate object, or operation guard may be retained across this call.
pub(super) async fn read_quota_snapshot(
    app: &Arc<AppContext>,
    receipt: &SdkCandidateReceipt,
) -> Result<CandidateQuotaSnapshot, VectorMaintenanceExecutionError> {
    let store = app.object_store();
    if store.id != receipt.instance_id() {
        return Err(conflict("quota receipt belongs to another storage instance"));
    }
    let bucket = receipt
        .resource()
        .vector_bucket()
        .ok_or_else(|| invalid("quota observation requires a Vector bucket"))?;
    let name = bucket.storage_bucket_id().as_str();
    let incarnation = bucket.incarnation().as_uuid();
    // Lock order: lifecycle/read -> metadata/read -> allocator/write -> shard/read.
    // V1 replaces allocator/shards with one ledger/read. No operation/object locks follow.
    let identity = storage::identity_read_guard(name, incarnation, store.id)
        .await
        .map_err(maintenance_execution_error)?;
    let mut guards = Vec::new();
    check_guards(&identity, &guards)?;
    let metadata = storage::persisted_bucket_metadata(&store, name)
        .await
        .map_err(maintenance_execution_error)?;
    metadata.validate_bucket_handle(bucket).map_err(maintenance_execution_error)?;
    check_guards(&identity, &guards)?;
    if metadata.quota_config_json.len() > MAX_RECORD_BYTES {
        return Err(invalid("quota configuration exceeds the observation byte budget"));
    }
    let revision = metadata.quota_config_updated_at.unix_timestamp_nanos();
    let mut snapshot = CandidateQuotaSnapshot {
        observation_token: receipt.observation().observation_token,
        instance_id: store.id,
        bucket: VectorBucketBindingV1::from_handle(bucket)?,
        table_id: receipt.resource().table_id().as_str().to_owned(),
        observed_at: OffsetDateTime::now_utc().to_string(),
        quota_config_base64: base64_simd::STANDARD.encode_to_string(&metadata.quota_config_json),
        quota_config_sha256: sha256(&metadata.quota_config_json),
        quota_revision_unix_nanos: revision.to_string(),
        protocol: None,
        quota_limit: None,
        validation: true,
        issues: Vec::new(),
        records: Vec::new(),
    };
    if metadata.quota_config_json.is_empty() {
        snapshot.issue(None, QuotaIssueCode::QuotaMissing, None);
    } else {
        match serde_json::from_slice::<BucketQuota>(&metadata.quota_config_json) {
            Ok(quota) => {
                snapshot.protocol = quota.reservation_protocol;
                snapshot.quota_limit = quota.quota;
                if !matches!(snapshot.protocol, Some(1 | 2)) || snapshot.quota_limit.is_none() {
                    snapshot.issue(None, QuotaIssueCode::UnsupportedProtocol, None);
                }
            }
            Err(error) => snapshot.issue(
                None,
                QuotaIssueCode::MalformedRecord,
                Some(VectorMaintenanceExecutionError::Executor {
                    category: VectorMaintenanceFailureCode::CorruptRecord,
                    source: Box::new(error),
                }),
            ),
        }
    }
    if snapshot.validation {
        let root = format!("config/quota-ledger/{name}.json");
        let mut keys = Vec::new();
        if snapshot.protocol == Some(1) {
            let lock = store
                .new_ns_lock(RUSTFS_META_BUCKET, &root)
                .await
                .map_err(maintenance_execution_error)?;
            guards.push(
                lock.get_read_lock(storage::lock_timeout())
                    .await
                    .map_err(maintenance_execution_error)?,
            );
            keys.push(root);
        } else {
            let allocator = format!("{root}.allocator.{incarnation}.{revision}.json");
            let lock = store
                .new_ns_lock(RUSTFS_META_BUCKET, &allocator)
                .await
                .map_err(maintenance_execution_error)?;
            guards.push(
                lock.get_write_lock(storage::lock_timeout())
                    .await
                    .map_err(maintenance_execution_error)?,
            );
            keys.push(allocator);
            for index in 0..SHARD_COUNT {
                check_guards(&identity, &guards)?;
                let key = format!("{root}.shards/{incarnation}/{revision}.{index:02}.json");
                let lock = store
                    .new_ns_lock(RUSTFS_META_BUCKET, &key)
                    .await
                    .map_err(maintenance_execution_error)?;
                guards.push(
                    lock.get_read_lock(storage::lock_timeout())
                        .await
                        .map_err(maintenance_execution_error)?,
                );
                keys.push(key);
            }
        }
        let mut remaining = MAX_RAW_BYTES - metadata.quota_config_json.len();
        for key in keys {
            check_guards(&identity, &guards)?;
            snapshot.records.push(read_record(&store, &key, &mut remaining).await);
            check_guards(&identity, &guards)?;
        }
        validate_records(&mut snapshot, revision);
    }
    check_guards(&identity, &guards)?;
    drop(guards);
    drop(identity);
    Ok(snapshot)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn observed(key: &str, bytes: &[u8]) -> QuotaRecordObservation {
        QuotaRecordObservation {
            object_key: key.into(),
            absent: false,
            body_revision: Some(Uuid::new_v4()),
            etag: Some("etag".into()),
            mod_time: None,
            declared_length: Some(i64::try_from(bytes.len()).expect("fixture length")),
            observed_length: bytes.len(),
            complete_body: true,
            content_sha256: Some(sha256(bytes)),
            content_base64: Some(base64_simd::STANDARD.encode_to_string(bytes)),
            error: None,
        }
    }

    fn fixture(protocol: u32) -> CandidateQuotaSnapshot {
        CandidateQuotaSnapshot {
            observation_token: Uuid::new_v4(),
            instance_id: Uuid::new_v4(),
            bucket: VectorBucketBindingV1 {
                version: 1,
                storage_bucket_id: "fixture".into(),
                bucket_incarnation: Uuid::new_v4(),
                account_namespace_id: String::new(),
                region: "us-east-1".into(),
                public_name: "fixture".into(),
            },
            table_id: Uuid::new_v4().to_string(),
            observed_at: String::new(),
            quota_config_base64: String::new(),
            quota_config_sha256: String::new(),
            quota_revision_unix_nanos: "42".into(),
            protocol: Some(protocol),
            quota_limit: Some(128),
            validation: true,
            issues: Vec::new(),
            records: Vec::new(),
        }
    }

    fn single(snapshot: &CandidateQuotaSnapshot) -> serde_json::Value {
        json!({"version":1,"bucket_incarnation":snapshot.bucket.bucket_incarnation,"quota_revision_unix_nanos":42,
            "writer_revision":Uuid::new_v4(),"accounted_usage":8,"reservations":{}})
    }

    fn sharded(snapshot: &mut CandidateQuotaSnapshot, grants: serde_json::Value) {
        let allocator = json!({"version":1,"bucket_incarnation":snapshot.bucket.bucket_incarnation,
            "quota_revision_unix_nanos":42,"quota_limit":128,"generation":1,"grants":grants});
        snapshot
            .records
            .push(observed("allocator", &serde_json::to_vec(&allocator).expect("allocator fixture")));
        for index in 0..SHARD_COUNT {
            let mut record = observed(&format!("shard-{index}"), b"");
            record.absent = true;
            record.complete_body = false;
            record.content_base64 = None;
            snapshot.records.push(record);
        }
    }

    #[test]
    fn empty_and_malformed_ledger_preserve_the_original_body() {
        for bytes in [b"".as_slice(), b"{damaged".as_slice()] {
            let mut snapshot = fixture(1);
            snapshot.records.push(observed("ledger", bytes));
            validate_records(&mut snapshot, 42);
            assert!(!snapshot.validation);
            assert!(matches!(snapshot.issues[0].code, QuotaIssueCode::MalformedRecord));
            assert!(snapshot.issues[0].error.is_some());
            assert!(!snapshot.records[0].absent);
            assert_eq!(snapshot.records[0].content_sha256, Some(sha256(bytes)));
            assert_eq!(snapshot.records[0].content_base64, Some(base64_simd::STANDARD.encode_to_string(bytes)));
        }
    }

    #[test]
    fn reservations_are_already_included_in_accounted_usage() {
        let mut snapshot = fixture(1);
        let mut ledger = single(&snapshot);
        ledger["reservations"] =
            json!({(Uuid::new_v4().to_string()):{"object":"candidate","old_size":0,"new_size":8,"created_at":1}});
        snapshot
            .records
            .push(observed("ledger", &serde_json::to_vec(&ledger).expect("ledger fixture")));
        validate_records(&mut snapshot, 42);
        assert!(snapshot.validation, "eight reserved bytes are already part of the eight-byte ledger");
        ledger["accounted_usage"] = 7.into();
        snapshot.records[0] = observed("ledger", &serde_json::to_vec(&ledger).expect("corrupt ledger fixture"));
        validate_records(&mut snapshot, 42);
        assert!(!snapshot.validation);
    }

    #[test]
    fn version_incarnation_and_revision_mismatches_retain_record_evidence() {
        for field in ["version", "bucket_incarnation", "quota_revision_unix_nanos"] {
            let mut snapshot = fixture(1);
            let mut ledger = single(&snapshot);
            ledger[field] = if field == "bucket_incarnation" {
                json!(Uuid::new_v4())
            } else {
                json!(2)
            };
            let bytes = serde_json::to_vec(&ledger).expect("mismatching ledger fixture");
            snapshot.records.push(observed("ledger", &bytes));
            validate_records(&mut snapshot, 42);
            assert!(!snapshot.validation, "{field} mismatch must fail validation");
            assert_eq!(snapshot.records[0].content_sha256, Some(sha256(&bytes)));
        }
    }

    #[test]
    fn missing_bootstrap_shard_does_not_become_zero_usage() {
        let mut snapshot = fixture(2);
        sharded(
            &mut snapshot,
            json!({(Uuid::new_v4().to_string()):{"shard_index":0,"amount":32,"initial_usage":32}}),
        );
        validate_records(&mut snapshot, 42);
        assert!(!snapshot.validation);
        assert!(snapshot.records[1].absent);
        assert!(snapshot.records[1].content_base64.is_none());
        assert!(matches!(snapshot.issues[0].code, QuotaIssueCode::MissingRecord));
    }

    #[test]
    fn unallocated_absent_shards_remain_explicit() {
        let mut snapshot = fixture(2);
        sharded(&mut snapshot, json!({}));
        validate_records(&mut snapshot, 42);
        assert!(snapshot.validation);
        assert_eq!(snapshot.records.len(), 17);
        assert!(snapshot.records[1..].iter().all(|record| record.absent));
    }

    #[test]
    fn shard_grant_must_match_the_same_allocator_and_shard() {
        let mut snapshot = fixture(2);
        let grant_id = Uuid::new_v4();
        sharded(&mut snapshot, json!({(grant_id.to_string()):{"shard_index":0,"amount":32}}));
        let shard = json!({"version":1,"bucket_incarnation":snapshot.bucket.bucket_incarnation,
            "quota_revision_unix_nanos":42,"shard_index":0,"allocator_generation":1,"accounted_usage":8,
            "grants":{(grant_id.to_string()):64},"reservations":{}});
        snapshot.records[1] = observed("shard-0", &serde_json::to_vec(&shard).expect("shard fixture"));
        validate_records(&mut snapshot, 42);
        assert!(!snapshot.validation);
        assert!(matches!(snapshot.issues[0].code, QuotaIssueCode::InvalidAccounting));
    }

    #[test]
    fn an_empty_shard_cannot_hide_unadopted_initial_usage() {
        let mut snapshot = fixture(2);
        sharded(
            &mut snapshot,
            json!({(Uuid::new_v4().to_string()):{"shard_index":0,"amount":32,"initial_usage":32}}),
        );
        let shard = json!({"version":1,"bucket_incarnation":snapshot.bucket.bucket_incarnation,
            "quota_revision_unix_nanos":42,"shard_index":0,"allocator_generation":0,"accounted_usage":0,
            "grants":{},"reservations":{}});
        snapshot.records[1] = observed("shard-0", &serde_json::to_vec(&shard).expect("shard fixture"));
        validate_records(&mut snapshot, 42);
        assert!(!snapshot.validation);
        assert!(matches!(snapshot.issues[0].code, QuotaIssueCode::InvalidAccounting));
    }

    #[test]
    fn absent_records_retain_the_original_not_found_without_invalidating_unallocated_shards() {
        for error in [
            storage::StorageError::FileNotFound,
            storage::StorageError::ObjectNotFound("original-bucket".into(), "original-object".into()),
        ] {
            let expected = error.to_string();
            let mut snapshot = fixture(2);
            sharded(&mut snapshot, json!({}));
            snapshot.records[1].absent = false;
            snapshot.records[1].retain_read_error(error);
            validate_records(&mut snapshot, 42);
            assert!(snapshot.validation);
            assert!(snapshot.records[1].absent);
            let error = snapshot.records[1].error.as_ref().expect("retained absence error");
            let source = std::error::Error::source(error).expect("native absence source");
            match source
                .downcast_ref::<storage::StorageError>()
                .expect("original storage error")
            {
                storage::StorageError::FileNotFound => {}
                storage::StorageError::ObjectNotFound(bucket, object) => {
                    assert_eq!(bucket, "original-bucket");
                    assert_eq!(object, "original-object");
                }
                error => panic!("unexpected absence variant: {error:?}"),
            }
            assert_eq!(source.to_string(), expected);
            let encoded = serde_json::to_value(&snapshot).expect("absence evidence serialization");
            assert_eq!(encoded["records"][1]["error"]["sources"][1], expected);
            let absent = snapshot.records.remove(1);
            let mut required = fixture(1);
            required.records.push(absent);
            validate_records(&mut required, 42);
            assert!(!required.validation);
            assert!(required.records[0].error.is_some());
            assert!(matches!(required.issues[0].code, QuotaIssueCode::MissingRecord));
        }
    }

    #[test]
    fn native_error_keeps_its_typed_source_and_prevents_validation() {
        let mut snapshot = fixture(1);
        let mut record = observed("ledger", b"partial");
        record.complete_body = false;
        record.error = Some(maintenance_execution_error(storage::StorageError::ErasureReadQuorum));
        snapshot.records.push(record);
        validate_records(&mut snapshot, 42);
        assert!(!snapshot.validation);
        let error = snapshot.records[0].error.as_ref().expect("original error");
        let source = std::error::Error::source(error).expect("native source");
        assert!(matches!(
            source.downcast_ref::<storage::StorageError>(),
            Some(storage::StorageError::ErasureReadQuorum)
        ));
        let encoded = serde_json::to_value(&snapshot).expect("evidence serialization");
        assert_eq!(encoded["records"][0]["error"]["category"], "STORAGE_UNAVAILABLE");
        assert_eq!(
            snapshot.records[0].content_base64,
            Some(base64_simd::STANDARD.encode_to_string(b"partial"))
        );
    }
}
