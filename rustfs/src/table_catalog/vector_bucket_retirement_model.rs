// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use serde::de::Error as _;

pub(in crate::table_catalog) const VECTOR_BUCKET_RETIREMENT_AUTHORITY_MAX_BYTES: usize = 65_536;
pub(in crate::table_catalog) const VECTOR_BUCKET_RETIREMENT_PAGE_MAX_BYTES: usize = VECTOR_MAINTENANCE_RECORD_MAX_BYTES;

fn invalid(message: impl Into<String>) -> TableCatalogStoreError {
    TableCatalogStoreError::Invalid(message.into())
}
fn ensure(condition: bool, message: &str) -> TableCatalogStoreResult<()> {
    if condition { Ok(()) } else { Err(invalid(message)) }
}
fn nonnil(id: Uuid) -> TableCatalogStoreResult<()> {
    ensure(!id.is_nil(), "nil Catalog retirement identity")
}
fn required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}
fn canonical_uuid<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Uuid, D::Error> {
    let text = String::deserialize(deserializer)?;
    let id = Uuid::parse_str(&text).map_err(D::Error::custom)?;
    if id.is_nil() || id.to_string() != text {
        return Err(D::Error::custom("noncanonical Catalog retirement UUID"));
    }
    Ok(id)
}
fn optional_uuid<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Option<Uuid>, D::Error> {
    Option::<String>::deserialize(deserializer)?
        .map(|text| {
            let id = Uuid::parse_str(&text).map_err(D::Error::custom)?;
            if id.is_nil() || id.to_string() != text {
                return Err(D::Error::custom("noncanonical Catalog retirement UUID"));
            }
            Ok(id)
        })
        .transpose()
}
fn reference(value: &CatalogMetadataRefRecordV1) -> TableCatalogStoreResult<()> {
    CatalogMetadataRef::try_from(value)?;
    ensure(
        value.content_length > 0 && value.etag.as_ref().is_some_and(|e| !e.is_empty()),
        "Catalog retirement reference lacks exact bytes or ETag",
    )?;
    if let Some(id) = value.body_revision {
        nonnil(id)?;
    }
    Ok(())
}
fn version_bucket(version: u16, bucket: &VectorBucketBindingV1, retirement_id: Uuid) -> TableCatalogStoreResult<()> {
    ensure(version == 1, "unsupported Catalog retirement version")?;
    bucket.to_handle()?;
    nonnil(retirement_id)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VectorBucketCatalogRetirementV1 {
    pub(super) version: u16,
    pub(super) bucket: VectorBucketBindingV1,
    #[serde(deserialize_with = "canonical_uuid")]
    pub(super) retirement_id: Uuid,
    pub(super) created_at: String,
    pub(super) plan_root: CatalogMetadataRefRecordV1,
    pub(super) plan_page_count: u32,
    pub(super) target_count: u64,
    pub(super) resource_count: u64,
    pub(super) inventory_sha256: String,
    pub(super) closure_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VectorBucketCatalogRetirementPlanPageV1 {
    pub(super) version: u16,
    pub(super) bucket: VectorBucketBindingV1,
    #[serde(deserialize_with = "canonical_uuid")]
    pub(super) retirement_id: Uuid,
    #[serde(deserialize_with = "canonical_uuid")]
    pub(super) page_id: Uuid,
    #[serde(deserialize_with = "required_option")]
    pub(super) next_page: Option<CatalogMetadataRefRecordV1>,
    pub(super) resources: Vec<VectorCatalogRetiredResourceProofV1>,
    pub(super) targets: Vec<VectorCatalogRetirementTargetV1>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VectorCatalogRetiredResourceProofV1 {
    pub(super) resource: VectorMaintenanceResourceV1,
    #[serde(deserialize_with = "canonical_uuid")]
    pub(super) proof_id: Uuid,
    pub(super) proof_root: CatalogMetadataRefRecordV1,
    pub(super) proof_page_count: u32,
    pub(super) fact_count: u64,
    pub(super) proof_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VectorCatalogRetiredResourceProofPageV1 {
    pub(super) version: u16,
    pub(super) bucket: VectorBucketBindingV1,
    #[serde(deserialize_with = "canonical_uuid")]
    pub(super) retirement_id: Uuid,
    pub(super) resource: VectorMaintenanceResourceV1,
    #[serde(deserialize_with = "canonical_uuid")]
    pub(super) proof_id: Uuid,
    #[serde(deserialize_with = "canonical_uuid")]
    pub(super) page_id: Uuid,
    pub(super) page_ordinal: u32,
    pub(super) page_count: u32,
    pub(super) first_fact_ordinal: u64,
    pub(super) total_fact_count: u64,
    pub(super) proof_sha256: String,
    #[serde(deserialize_with = "required_option")]
    pub(super) next_page: Option<CatalogMetadataRefRecordV1>,
    pub(super) facts: Vec<VectorCatalogRetirementProofFactV1>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VectorCatalogRetirementTargetV1 {
    pub(super) kind: VectorCatalogRetirementTargetKind,
    #[serde(deserialize_with = "required_option")]
    pub(super) resource: Option<VectorMaintenanceResourceV1>,
    pub(super) object_key: String,
    pub(super) etag: String,
    #[serde(deserialize_with = "optional_uuid")]
    pub(super) body_revision: Option<Uuid>,
    pub(super) content_sha256: String,
    pub(super) content_length: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VectorBucketCatalogRetirementProgressPageV1 {
    pub(super) version: u16,
    pub(super) bucket: VectorBucketBindingV1,
    #[serde(deserialize_with = "canonical_uuid")]
    pub(super) retirement_id: Uuid,
    pub(super) plan_page: CatalogMetadataRefRecordV1,
    #[serde(deserialize_with = "canonical_uuid")]
    pub(super) revision: Uuid,
    #[serde(deserialize_with = "optional_uuid")]
    pub(super) previous_revision: Option<Uuid>,
    pub(super) targets: Vec<VectorCatalogRetirementTargetProgressV1>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VectorCatalogRetirementTargetProgressV1 {
    pub(super) ordinal: u16,
    pub(super) state: VectorCatalogRetirementTargetState,
    #[serde(deserialize_with = "optional_uuid")]
    pub(super) dispatch_id: Option<Uuid>,
    #[serde(deserialize_with = "optional_uuid")]
    pub(super) worker_instance_id: Option<Uuid>,
    #[serde(deserialize_with = "optional_uuid")]
    pub(super) worker_boot_id: Option<Uuid>,
    #[serde(deserialize_with = "required_option")]
    pub(super) started_at: Option<String>,
    #[serde(deserialize_with = "required_option")]
    pub(super) acknowledged_at: Option<String>,
    #[serde(deserialize_with = "required_option")]
    pub(super) failure: Option<VectorMaintenanceFailureCode>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub(crate) enum VectorCatalogRetirementProofFactV1 {
    ResourceAnchor {
        delete_intent: CatalogMetadataRefRecordV1,
        deleted_head: Box<VectorMaintenanceHeadV1>,
    },
    SourceRecord {
        reference: CatalogMetadataRefRecordV1,
    },
    Commit {
        commit_id: String,
        request_sha256: String,
        log_ref: CatalogMetadataRefRecordV1,
        committed_head: Box<VectorMaintenanceHeadV1>,
        history_ref_count: u64,
    },
    CommitHistoryReference {
        commit_id: String,
        ordinal: u64,
        reference: CatalogMetadataRefRecordV1,
    },
    Job {
        job_ref: CatalogMetadataRefRecordV1,
        #[serde(deserialize_with = "canonical_uuid")]
        job_id: Uuid,
        #[serde(deserialize_with = "canonical_uuid")]
        revision: Uuid,
        attempt: u16,
        #[serde(deserialize_with = "optional_uuid")]
        lease_id: Option<Uuid>,
        result_ref: CatalogMetadataRefRecordV1,
        source_ref_count: u64,
    },
    JobSourceReference {
        #[serde(deserialize_with = "canonical_uuid")]
        job_id: Uuid,
        #[serde(deserialize_with = "canonical_uuid")]
        revision: Uuid,
        ordinal: u64,
        reference: CatalogMetadataRefRecordV1,
    },
    Retirement {
        authorization_ref: CatalogMetadataRefRecordV1,
        progress_ref: CatalogMetadataRefRecordV1,
        #[serde(deserialize_with = "canonical_uuid")]
        retirement_id: Uuid,
        #[serde(deserialize_with = "canonical_uuid")]
        revision: Uuid,
        receipt_count: u64,
    },
    RetirementReceipt {
        #[serde(deserialize_with = "canonical_uuid")]
        retirement_id: Uuid,
        #[serde(deserialize_with = "canonical_uuid")]
        revision: Uuid,
        ordinal: u64,
        receipt: VectorRetirementReceiptV1,
    },
    UnstartedJob {
        reference: CatalogMetadataRefRecordV1,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum VectorCatalogRetirementTargetKind {
    BucketEntry,
    NamespaceEntry,
    TableEntry,
    CreateIntent,
    DeleteIntent,
    CommitLog,
    CommitIdempotency,
    MaintenanceJob,
    MaintenanceSource,
    MaintenanceResult,
    RetirementEvidence,
    RetirementPlan,
    RetirementAuthorization,
    RetirementProgress,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum VectorCatalogRetirementTargetState {
    Pending,
    InFlight,
    DeleteAcknowledged,
    Unknown,
}

pub(in crate::table_catalog) fn encode_vector_bucket_retirement_record<T: VectorMaintenanceRecord>(
    record: &T,
) -> TableCatalogStoreResult<Vec<u8>> {
    record.validate()?;
    let bytes = serde_json::to_vec(record).map_err(|e| invalid(format!("encode Catalog retirement: {e}")))?;
    ensure(bytes.len() <= T::MAX_BYTES, "Catalog retirement record exceeds byte limit")?;
    // Reuse the existing strict nested-field, integer and duplicate-key decoder.
    decode_vector_maintenance_record::<T>(&bytes)?;
    Ok(bytes)
}

impl VectorMaintenanceRecord for VectorBucketCatalogRetirementV1 {
    const MAX_BYTES: usize = VECTOR_BUCKET_RETIREMENT_AUTHORITY_MAX_BYTES;
    fn validate(&self) -> TableCatalogStoreResult<()> {
        version_bucket(self.version, &self.bucket, self.retirement_id)?;
        vector_maintenance_timestamp(&self.created_at)?;
        reference(&self.plan_root)?;
        ensure(self.plan_page_count > 0 && self.target_count > 0, "empty Catalog retirement authority")?;
        decode_catalog_digest(&self.inventory_sha256)?;
        decode_catalog_digest(&self.closure_sha256)?;
        Ok(())
    }
}
impl VectorMaintenanceRecord for VectorCatalogRetirementTargetV1 {
    fn validate(&self) -> TableCatalogStoreResult<()> {
        CatalogObjectKey::try_new(self.object_key.clone())?;
        ensure(
            !self.etag.is_empty() && self.etag.len() <= 4096 && !self.etag.chars().any(char::is_control),
            "invalid Catalog retirement target ETag",
        )?;
        ensure(self.content_length > 0, "empty Catalog retirement target")?;
        decode_catalog_digest(&self.content_sha256)?;
        if let Some(id) = self.body_revision {
            nonnil(id)?;
        }
        let bucket_level = matches!(
            self.kind,
            VectorCatalogRetirementTargetKind::BucketEntry | VectorCatalogRetirementTargetKind::NamespaceEntry
        );
        ensure(
            bucket_level == self.resource.is_none(),
            "Catalog retirement target resource scope mismatch",
        )?;
        if let Some(resource) = &self.resource {
            resource.validate()?;
        }
        Ok(())
    }
}
impl VectorMaintenanceRecord for VectorCatalogRetiredResourceProofV1 {
    fn validate(&self) -> TableCatalogStoreResult<()> {
        self.resource.validate()?;
        nonnil(self.proof_id)?;
        reference(&self.proof_root)?;
        decode_catalog_digest(&self.proof_sha256)?;
        ensure(self.proof_page_count > 0 && self.fact_count > 0, "empty Catalog resource proof")
    }
}
impl VectorMaintenanceRecord for VectorBucketCatalogRetirementPlanPageV1 {
    fn validate(&self) -> TableCatalogStoreResult<()> {
        version_bucket(self.version, &self.bucket, self.retirement_id)?;
        nonnil(self.page_id)?;
        if let Some(next) = &self.next_page {
            reference(next)?;
        }
        ensure(
            !self.targets.is_empty() && self.targets.len() <= VECTOR_RETIREMENT_MAX_TARGETS,
            "invalid Catalog retirement target page size",
        )?;
        let mut keys = BTreeSet::new();
        for target in &self.targets {
            target.validate()?;
            ensure(
                target.resource.as_ref().is_none_or(|r| r.bucket == self.bucket),
                "foreign Catalog retirement target resource",
            )?;
            ensure(keys.insert(&target.object_key), "duplicate Catalog retirement target")?;
        }
        let mut resources = BTreeSet::new();
        let mut proofs = BTreeSet::new();
        for proof in &self.resources {
            proof.validate()?;
            ensure(proof.resource.bucket == self.bucket, "foreign Catalog retirement proof resource")?;
            ensure(
                resources.insert(&proof.resource.table_id) && proofs.insert(proof.proof_id),
                "duplicate Catalog retirement resource proof",
            )?;
        }
        Ok(())
    }
}
impl VectorMaintenanceRecord for VectorCatalogRetiredResourceProofPageV1 {
    fn validate(&self) -> TableCatalogStoreResult<()> {
        version_bucket(self.version, &self.bucket, self.retirement_id)?;
        self.resource.validate()?;
        ensure(self.resource.bucket == self.bucket, "foreign Catalog proof resource")?;
        nonnil(self.proof_id)?;
        nonnil(self.page_id)?;
        decode_catalog_digest(&self.proof_sha256)?;
        ensure(
            self.page_count > 0 && self.page_ordinal < self.page_count && !self.facts.is_empty(),
            "invalid Catalog proof page ordinal or count",
        )?;
        let end = self
            .first_fact_ordinal
            .checked_add(u64::try_from(self.facts.len()).map_err(|_| invalid("Catalog proof count overflow"))?)
            .ok_or_else(|| invalid("Catalog proof count overflow"))?;
        ensure(
            end <= self.total_fact_count && (self.page_ordinal == 0) == (self.first_fact_ordinal == 0),
            "Catalog proof fact range mismatch",
        )?;
        let final_page = self.page_ordinal.checked_add(1) == Some(self.page_count);
        ensure(
            final_page == self.next_page.is_none() && (!final_page || end == self.total_fact_count),
            "Catalog proof final page mismatch",
        )?;
        if let Some(next) = &self.next_page {
            reference(next)?;
        }
        let mut previous = None;
        for fact in &self.facts {
            fact.validate()?;
            fact.validate_resource(&self.resource)?;
            let order = fact.order();
            ensure(
                previous.as_ref().is_none_or(|last| last < &order),
                "Catalog proof facts are repeated or out of order",
            )?;
            previous = Some(order);
        }
        Ok(())
    }
}
impl VectorMaintenanceRecord for VectorCatalogRetirementTargetProgressV1 {
    fn validate(&self) -> TableCatalogStoreResult<()> {
        ensure(
            usize::from(self.ordinal) < VECTOR_RETIREMENT_MAX_TARGETS,
            "Catalog retirement target ordinal exceeds limit",
        )?;
        for id in [self.dispatch_id, self.worker_instance_id, self.worker_boot_id]
            .into_iter()
            .flatten()
        {
            nonnil(id)?;
        }
        if let Some(start) = &self.started_at {
            vector_maintenance_timestamp(start)?;
        }
        if let Some(ack) = &self.acknowledged_at {
            let ack = vector_maintenance_timestamp(ack)?;
            let start = self
                .started_at
                .as_ref()
                .ok_or_else(|| invalid("Catalog acknowledgment has no start"))?;
            ensure(ack >= vector_maintenance_timestamp(start)?, "Catalog acknowledgment precedes dispatch")?;
        }
        let has_dispatch = self.dispatch_id.is_some()
            && self.worker_instance_id.is_some()
            && self.worker_boot_id.is_some()
            && self.started_at.is_some();
        let no_dispatch = self.dispatch_id.is_none()
            && self.worker_instance_id.is_none()
            && self.worker_boot_id.is_none()
            && self.started_at.is_none();
        let valid = match self.state {
            VectorCatalogRetirementTargetState::Pending => {
                no_dispatch && self.acknowledged_at.is_none() && self.failure.is_none()
            }
            VectorCatalogRetirementTargetState::InFlight => {
                has_dispatch && self.acknowledged_at.is_none() && self.failure.is_none()
            }
            VectorCatalogRetirementTargetState::DeleteAcknowledged => {
                has_dispatch && self.acknowledged_at.is_some() && self.failure.is_none()
            }
            VectorCatalogRetirementTargetState::Unknown => {
                has_dispatch && self.acknowledged_at.is_none() && self.failure.is_some()
            }
        };
        ensure(valid, "contradictory Catalog retirement target progress")
    }
}
impl VectorMaintenanceRecord for VectorBucketCatalogRetirementProgressPageV1 {
    const MAX_BYTES: usize = VECTOR_RETIREMENT_PROGRESS_MAX_BYTES;
    fn validate(&self) -> TableCatalogStoreResult<()> {
        version_bucket(self.version, &self.bucket, self.retirement_id)?;
        reference(&self.plan_page)?;
        nonnil(self.revision)?;
        if let Some(previous) = self.previous_revision {
            nonnil(previous)?;
            ensure(previous != self.revision, "unchanged Catalog progress revision")?;
        }
        ensure(
            !self.targets.is_empty() && self.targets.len() <= VECTOR_RETIREMENT_MAX_TARGETS,
            "invalid Catalog progress page size",
        )?;
        let mut dispatches = BTreeSet::new();
        for (index, target) in self.targets.iter().enumerate() {
            target.validate()?;
            ensure(usize::from(target.ordinal) == index, "Catalog progress ordinals are not contiguous")?;
            if let Some(id) = target.dispatch_id {
                ensure(dispatches.insert(id), "duplicate Catalog retirement dispatch")?;
            }
        }
        Ok(())
    }
}

#[path = "vector_bucket_retirement_model/digest.rs"]
mod digest;
#[path = "vector_bucket_retirement_model/facts.rs"]
mod facts;
pub(in crate::table_catalog) use digest::*;
#[cfg(test)]
#[path = "vector_bucket_retirement_model/tests.rs"]
mod tests;
