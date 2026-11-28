// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Test-only snapshots from the original cleanup owner; these grant no deletion authority.

use super::*;
use std::sync::{LazyLock, Weak};

const MAX_OBSERVERS: usize = 32;
type Callback = dyn Fn(CatalogRetirementObservation) -> BoxFuture<'static, TableCatalogStoreResult<()>> + Send + Sync;
static OBSERVERS: LazyLock<parking_lot::Mutex<Vec<Weak<Registration>>>> = LazyLock::new(Default::default);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CatalogRetirementObservedPhase {
    AuthorityPublished,
    BeforeTargetDispatch,
    TargetNativeReturned,
    TargetProgressPublished,
    CatalogPreparationCompleted,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) enum CatalogRetirementObservedOutcome {
    Succeeded,
    Failed { category: &'static str, message: String },
}
impl CatalogRetirementObservedOutcome {
    pub(super) fn from_result<T>(result: &TableCatalogStoreResult<T>) -> Self {
        match result {
            Ok(_) => Self::Succeeded,
            Err(error) => {
                let category = match error {
                    TableCatalogStoreError::NotFound(_) => "NotFound",
                    TableCatalogStoreError::NamespaceNotFound(_) => "NamespaceNotFound",
                    TableCatalogStoreError::TableNotFound(_) => "TableNotFound",
                    TableCatalogStoreError::AlreadyExists(_) => "AlreadyExists",
                    TableCatalogStoreError::Conflict(_) => "Conflict",
                    TableCatalogStoreError::Invalid(_) => "Invalid",
                    TableCatalogStoreError::Unsupported(_) => "Unsupported",
                    TableCatalogStoreError::Unavailable(_) => "Unavailable",
                    TableCatalogStoreError::LockUnavailable { .. } => "LockUnavailable",
                    TableCatalogStoreError::Internal(_) => "Internal",
                    TableCatalogStoreError::Io { .. } => "Io",
                };
                Self::Failed {
                    category,
                    message: error.to_string(),
                }
            }
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct CatalogRetirementObservedTarget {
    pub(crate) target: VectorCatalogRetirementTargetV1,
    pub(crate) plan_page: CatalogMetadataRefRecordV1,
    pub(crate) ordinal: u16,
    pub(crate) previous_progress_ref: Option<CatalogMetadataRefRecordV1>,
    pub(crate) progress_ref: CatalogMetadataRefRecordV1,
    pub(crate) progress: Box<VectorBucketCatalogRetirementProgressPageV1>,
    pub(crate) attempted_progress: Option<Box<VectorBucketCatalogRetirementProgressPageV1>>,
    /// The existing backend result includes its internal final fence check.
    pub(crate) fenced_delete: Option<CatalogRetirementObservedOutcome>,
    pub(crate) subsequent_fence: Option<CatalogRetirementObservedOutcome>,
    pub(crate) progress_publish: Option<CatalogRetirementObservedOutcome>,
}
impl CatalogRetirementObservedTarget {
    pub(super) fn new(
        planned: &CatalogRetirementPlannedTarget,
        reference: &CatalogMetadataRef,
        progress: &VectorBucketCatalogRetirementProgressPageV1,
    ) -> Self {
        Self {
            target: planned.target.clone(),
            plan_page: planned.plan_page.clone(),
            ordinal: planned.ordinal,
            previous_progress_ref: None,
            progress_ref: reference.into(),
            progress: Box::new(progress.clone()),
            attempted_progress: None,
            fenced_delete: None,
            subsequent_fence: None,
            progress_publish: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct CatalogRetirementObservation {
    pub(crate) phase: CatalogRetirementObservedPhase,
    pub(crate) authority_ref: CatalogMetadataRefRecordV1,
    pub(crate) authority: Box<VectorBucketCatalogRetirementV1>,
    pub(crate) target: Option<Box<CatalogRetirementObservedTarget>>,
}

struct Registration {
    bucket: VectorBucketBindingV1,
    callback: Arc<Callback>,
    managed_diagnostics: parking_lot::Mutex<managed::Diagnostics>,
}

pub(crate) struct CatalogRetirementObservationHandle {
    _registration: Arc<Registration>,
}

/// The test owner selects a complete existing identity, never arbitrary system keys.
pub(crate) fn observe_catalog_retirement(
    bucket: VectorBucketBindingV1,
    callback: Arc<Callback>,
) -> TableCatalogStoreResult<CatalogRetirementObservationHandle> {
    bucket.to_handle()?;
    let mut observers = OBSERVERS.lock();
    observers.retain(|observer| observer.strong_count() > 0);
    if observers.len() >= MAX_OBSERVERS {
        return Err(TableCatalogStoreError::Unavailable(
            "CapacityExceeded: Catalog retirement observers".into(),
        ));
    }
    if observers
        .iter()
        .filter_map(Weak::upgrade)
        .any(|observer| observer.bucket == bucket)
    {
        return Err(TableCatalogStoreError::Conflict(
            "Catalog retirement identity already has an observer".into(),
        ));
    }
    let registration = Arc::new(Registration {
        bucket,
        callback,
        managed_diagnostics: parking_lot::Mutex::new(managed::Diagnostics::default()),
    });
    observers.push(Arc::downgrade(&registration));
    Ok(CatalogRetirementObservationHandle {
        _registration: registration,
    })
}

pub(super) async fn checkpoint(
    phase: CatalogRetirementObservedPhase,
    authority: &VectorBucketCatalogRetirementV1,
    reference: &CatalogMetadataRef,
    target: Option<CatalogRetirementObservedTarget>,
) -> TableCatalogStoreResult<()> {
    let observer = {
        let mut observers = OBSERVERS.lock();
        observers.retain(|observer| observer.strong_count() > 0);
        observers
            .iter()
            .filter_map(Weak::upgrade)
            .find(|observer| observer.bucket == authority.bucket)
    };
    if let Some(observer) = observer {
        (observer.callback)(CatalogRetirementObservation {
            phase,
            authority_ref: reference.into(),
            authority: Box::new(authority.clone()),
            target: target.map(Box::new),
        })
        .await?;
    }
    Ok(())
}

#[path = "observation_reader.rs"]
mod reader;
pub(crate) use reader::{CatalogRetirementEvidenceCursor, CatalogRetirementEvidenceReader};

#[path = "managed_observation.rs"]
mod managed;
pub(crate) use managed::{CatalogRetirementDeleteProbe, with_catalog_retirement_request};
