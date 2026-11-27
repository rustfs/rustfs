// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;

type FactOrder = (u8, Vec<u8>, Vec<u8>, u64);

impl VectorCatalogRetirementProofFactV1 {
    #[cfg(test)]
    pub(in crate::table_catalog) fn compare_order(&self, other: &Self) -> std::cmp::Ordering {
        self.order().cmp(&other.order())
    }

    pub(in crate::table_catalog) fn order(&self) -> FactOrder {
        let empty = Vec::new();
        match self {
            Self::ResourceAnchor { .. } => (0, empty.clone(), empty, 0),
            Self::SourceRecord { reference } => (1, reference.object_key.as_bytes().to_vec(), empty, 0),
            Self::Commit { commit_id, .. } => (2, commit_id.as_bytes().to_vec(), empty, 0),
            Self::CommitHistoryReference { commit_id, ordinal, .. } => (3, commit_id.as_bytes().to_vec(), empty, *ordinal),
            Self::Job { job_id, revision, .. } => (4, job_id.as_bytes().to_vec(), revision.as_bytes().to_vec(), 0),
            Self::JobSourceReference {
                job_id,
                revision,
                ordinal,
                ..
            } => (5, job_id.as_bytes().to_vec(), revision.as_bytes().to_vec(), *ordinal),
            Self::Retirement {
                retirement_id, revision, ..
            } => (6, retirement_id.as_bytes().to_vec(), revision.as_bytes().to_vec(), 0),
            Self::RetirementReceipt {
                retirement_id,
                revision,
                ordinal,
                ..
            } => (7, retirement_id.as_bytes().to_vec(), revision.as_bytes().to_vec(), *ordinal),
            Self::UnstartedJob { reference } => (8, reference.object_key.as_bytes().to_vec(), empty, 0),
        }
    }

    pub(super) fn validate_resource(&self, resource: &VectorMaintenanceResourceV1) -> TableCatalogStoreResult<()> {
        let head = match self {
            Self::ResourceAnchor { deleted_head, .. } => Some(deleted_head),
            Self::Commit { committed_head, .. } => Some(committed_head),
            _ => None,
        };
        if let Some(head) = head {
            ensure(
                head.binding.bucket == resource.bucket && head.binding.table_id == resource.table_id,
                "Catalog proof head belongs to another resource",
            )?;
        }
        Ok(())
    }
}

impl VectorMaintenanceRecord for VectorCatalogRetirementProofFactV1 {
    fn validate(&self) -> TableCatalogStoreResult<()> {
        match self {
            Self::ResourceAnchor {
                delete_intent,
                deleted_head,
            } => {
                reference(delete_intent)?;
                deleted_head.validate()?;
                ensure(
                    deleted_head.state == TableCatalogEntryState::Deleted,
                    "Catalog retirement anchor is not deleted",
                )
            }
            Self::SourceRecord { reference: value } | Self::UnstartedJob { reference: value } => reference(value),
            Self::Commit {
                commit_id,
                request_sha256,
                log_ref,
                committed_head,
                ..
            } => {
                ensure(
                    !commit_id.is_empty() && commit_id.len() <= 4096 && !commit_id.chars().any(char::is_control),
                    "invalid Catalog proof commit ID",
                )?;
                decode_catalog_digest(request_sha256)?;
                reference(log_ref)?;
                committed_head.validate()
            }
            Self::CommitHistoryReference {
                commit_id,
                reference: value,
                ..
            } => {
                ensure(
                    !commit_id.is_empty() && commit_id.len() <= 4096 && !commit_id.chars().any(char::is_control),
                    "invalid Catalog history owner",
                )?;
                CatalogMetadataRef::try_from(value)?;
                Ok(())
            }
            Self::Job {
                job_ref,
                job_id,
                revision,
                attempt,
                lease_id,
                result_ref,
                ..
            } => {
                reference(job_ref)?;
                nonnil(*job_id)?;
                nonnil(*revision)?;
                if let Some(id) = lease_id {
                    nonnil(*id)?;
                }
                ensure(*attempt > 0, "Catalog job proof has no attempt")?;
                reference(result_ref)
            }
            Self::JobSourceReference {
                job_id,
                revision,
                reference: value,
                ..
            } => {
                nonnil(*job_id)?;
                nonnil(*revision)?;
                let original = CatalogMetadataRef::try_from(value)?;
                CatalogMetadataRef::try_from_identity(
                    original.object_key().clone(),
                    original.content_length(),
                    original.content_sha256(),
                    original
                        .body_revision()
                        .ok_or_else(|| invalid("Catalog job source has no original body revision"))?,
                    value.etag.clone(),
                )?;
                Ok(())
            }
            Self::Retirement {
                authorization_ref,
                progress_ref,
                retirement_id,
                revision,
                receipt_count,
            } => {
                reference(authorization_ref)?;
                reference(progress_ref)?;
                nonnil(*retirement_id)?;
                nonnil(*revision)?;
                ensure(*receipt_count > 0, "Catalog retirement proof has no receipts")
            }
            Self::RetirementReceipt {
                retirement_id,
                revision,
                receipt,
                ..
            } => {
                nonnil(*retirement_id)?;
                nonnil(*revision)?;
                receipt.validate()
            }
        }
    }
}
