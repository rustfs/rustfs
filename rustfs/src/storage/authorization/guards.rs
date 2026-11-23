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

//! The bucket-generation fences an authorized request carries to its use case.
//!
//! Responsible for: the guards and snapshots the access phase captures before a decision
//! (bucket incarnation, pending-delete, copy-source, on-demand-migration source, bucket-config
//! mutation), loading them from the object store, and applying them to `ObjectOptions` so a
//! bucket recreated after authorization is refused by the conditional write.
//! Not responsible for: the policy decision itself (`decision`) or the table publication locks
//! the fence also binds (`table_data_plane`).
//! Upstream: the access hooks of both edges and `storage::access_owned`. Downstream: the object
//! store's bucket metadata and the use cases' `ObjectOptions`.

use super::AuthorizationRequest;
use super::TableDataPlanePublicationGuards;
use crate::error::ApiError;
use crate::storage::storage_api::contract::bucket::BUCKET_LIFECYCLE_LOCK_OBJECT;
use crate::storage::storage_api::contract::namespace::NamespaceLocking as _;
use crate::storage::storage_api::runtime_sources_consumer::ServerContextSlot;
use crate::storage::{ObjectOptions, StorageError, get_lock_acquire_timeout};
use rustfs_s3_types::{S3Error, S3ErrorCode, S3Result, s3_error};
#[cfg(test)]
use std::sync::OnceLock;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BucketGenerationGuard {
    pub(crate) bucket: String,
    pub(crate) incarnation_id: uuid::Uuid,
}

#[derive(Clone, Debug)]
pub(crate) struct BucketConfigMutationSnapshot {
    pub(crate) bucket: String,
    pub(crate) incarnation_id: uuid::Uuid,
}

pub(crate) fn bucket_config_mutation_incarnation<R: AuthorizationRequest + ?Sized>(
    req: &R,
    bucket: &str,
) -> S3Result<Option<uuid::Uuid>> {
    let Some(snapshot) = req.bucket_config_mutation() else {
        if req.server_context().is_some() {
            return Err(s3_error!(InternalError, "bucket config mutation snapshot is missing"));
        }
        return Ok(None);
    };
    if snapshot.bucket != bucket {
        return Err(s3_error!(InternalError, "bucket config mutation snapshot does not match request bucket"));
    }
    Ok(Some(snapshot.incarnation_id))
}

pub(crate) async fn load_bucket_config_mutation_snapshot<R: AuthorizationRequest + ?Sized>(
    server_ctx: &ServerContextSlot,
    req: &R,
    bucket: &str,
) -> S3Result<BucketConfigMutationSnapshot> {
    let store = server_ctx
        .object_store()
        .ok_or_else(|| s3_error!(InternalError, "object store is not initialized"))?;
    let lock = store
        .new_ns_lock(bucket, BUCKET_LIFECYCLE_LOCK_OBJECT)
        .await
        .map_err(ApiError::from)?;
    let _lifecycle_guard = lock.get_read_lock(get_lock_acquire_timeout()).await.map_err(|err| {
        ApiError::from(match err {
            rustfs_lock::LockError::QuorumNotReached { required, achieved } => StorageError::NamespaceLockQuorumUnavailable {
                mode: "bucket_config_mutation",
                bucket: bucket.to_string(),
                object: BUCKET_LIFECYCLE_LOCK_OBJECT.to_string(),
                required,
                achieved,
            },
            other => StorageError::Lock(other),
        })
    })?;
    let incarnation_id = load_bucket_generation_from_store(store.as_ref(), req, bucket)
        .await?
        .incarnation_id;
    Ok(BucketConfigMutationSnapshot {
        bucket: bucket.to_string(),
        incarnation_id,
    })
}

#[derive(Clone, Debug)]
pub(crate) struct CopySourceBucketGenerationGuard {
    pub(crate) bucket: String,
    pub(crate) incarnation_id: uuid::Uuid,
}

#[cfg(test)]
type ObjectGenerationTestHook = (String, tokio::sync::oneshot::Sender<()>, tokio::sync::oneshot::Receiver<()>);

#[cfg(test)]
static OBJECT_GENERATION_TEST_HOOK: OnceLock<std::sync::Mutex<Option<ObjectGenerationTestHook>>> = OnceLock::new();

#[cfg(test)]
tokio::task_local! {
    pub(crate) static PUT_GENERATION_READ_QUORUM_FAILURE: ();
}

#[cfg(test)]
pub(crate) fn install_object_generation_test_hook(
    bucket: String,
    loaded: tokio::sync::oneshot::Sender<()>,
    resume: tokio::sync::oneshot::Receiver<()>,
) {
    *OBJECT_GENERATION_TEST_HOOK
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("object generation test hook lock should not be poisoned") = Some((bucket, loaded, resume));
}

#[cfg(test)]
pub(crate) async fn wait_for_object_generation_test_hook(bucket: &str) {
    let hook = {
        let mut slot = OBJECT_GENERATION_TEST_HOOK
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("object generation test hook lock should not be poisoned");
        if slot.as_ref().is_some_and(|(expected_bucket, _, _)| expected_bucket == bucket) {
            slot.take()
        } else {
            None
        }
    };
    if let Some((_bucket, loaded, resume)) = hook {
        let _ = loaded.send(());
        let _ = resume.await;
    }
}

#[derive(Clone, Debug)]
pub(crate) enum PendingDeleteBucketGenerationGuard {
    Ready(BucketGenerationGuard),
    Failed { code: S3ErrorCode, message: String },
}

impl PendingDeleteBucketGenerationGuard {
    /// Resolves the identity storage will validate for a mutation. A deferred
    /// delete capture takes precedence, including a failure to capture it.
    pub(crate) fn resolve_generation<'a>(
        ordinary: Option<&'a BucketGenerationGuard>,
        pending_delete: Option<&'a Self>,
    ) -> S3Result<Option<&'a BucketGenerationGuard>> {
        match pending_delete {
            Some(Self::Ready(guard)) => Ok(Some(guard)),
            Some(Self::Failed { code, message }) => Err(S3Error::with_message(code.clone(), message.clone())),
            None => Ok(ordinary),
        }
    }

    pub(crate) fn from_result(result: S3Result<BucketGenerationGuard>) -> Self {
        match result {
            Ok(guard) => Self::Ready(guard),
            Err(err) => Self::Failed {
                code: err.code().clone(),
                message: err.message().unwrap_or_else(|| err.code().as_str()).to_string(),
            },
        }
    }
}

pub(crate) async fn load_bucket_generation_from_store<R: AuthorizationRequest + ?Sized>(
    store: &crate::storage::storage_api::ECStore,
    req: &R,
    bucket: &str,
) -> S3Result<BucketGenerationGuard> {
    if let Some(existing) = req.bucket_generation() {
        return if existing.bucket == bucket {
            Ok(existing.clone())
        } else {
            Err(s3_error!(InternalError, "bucket generation guard does not match request bucket"))
        };
    }

    let incarnation_id = store.bucket_incarnation_id(bucket).await.map_err(ApiError::from)?;
    Ok(BucketGenerationGuard {
        bucket: bucket.to_string(),
        incarnation_id,
    })
}

/// Loads the bucket incarnation through `server_ctx`'s object store, or returns the guard the
/// request already carries. The legacy hooks pass `FS::server_ctx`; the gateway edge passes the
/// envelope's server context.
pub(crate) async fn load_bucket_generation<R: AuthorizationRequest + ?Sized>(
    server_ctx: &ServerContextSlot,
    req: &R,
    bucket: &str,
) -> S3Result<BucketGenerationGuard> {
    let store = server_ctx
        .object_store()
        .ok_or_else(|| s3_error!(InternalError, "object store is not initialized"))?;
    load_bucket_generation_from_store(store.as_ref(), req, bucket).await
}

/// A read may consult only the source admitted before its authorization.
/// Capture failures are deferred until a local miss actually needs that source.
#[derive(Clone, Debug)]
pub(crate) enum OdmReadGenerationGuard {
    Unavailable,
    Ready(BucketGenerationGuard),
    Failed { code: S3ErrorCode, message: String },
}

impl OdmReadGenerationGuard {
    fn from_result(result: S3Result<BucketGenerationGuard>) -> Self {
        match result {
            Ok(guard) => Self::Ready(guard),
            Err(err) => Self::Failed {
                code: err.code().clone(),
                message: err.message().unwrap_or_else(|| err.code().as_str()).to_string(),
            },
        }
    }
}

fn odm_read_source_configured(bucket: &str) -> bool {
    let sys = crate::on_demand_migration::OnDemandMigrationSys::get();
    sys.is_module_enabled() && sys.state(bucket).is_some()
}

/// The on-demand-migration source generation a read may consult, captured before its
/// authorization. A capture failure is kept in the guard and surfaces only on a source miss.
pub(crate) async fn capture_odm_read_generation<R: AuthorizationRequest + ?Sized>(
    server_ctx: &ServerContextSlot,
    req: &R,
    bucket: &str,
) -> OdmReadGenerationGuard {
    if !odm_read_source_configured(bucket) {
        return OdmReadGenerationGuard::Unavailable;
    }
    OdmReadGenerationGuard::from_result(load_bucket_generation(server_ctx, req, bucket).await)
}

/// Direct usecase callers have no access middleware; capture at their entry.
/// A server request without the access marker must never bind to a later source.
pub(crate) async fn prepare_odm_read_generation<R: AuthorizationRequest + ?Sized>(
    store: &crate::storage::storage_api::ECStore,
    req: &mut R,
    bucket: &str,
) {
    if req.odm_read_generation().is_some() {
        return;
    }
    let guard = if req.server_context().is_some() || !odm_read_source_configured(bucket) {
        OdmReadGenerationGuard::Unavailable
    } else {
        OdmReadGenerationGuard::from_result(load_bucket_generation_from_store(store, req, bucket).await)
    };
    req.install_odm_read_generation(guard);
}

pub(crate) fn odm_read_generation<R: AuthorizationRequest + ?Sized>(req: &R, bucket: &str) -> S3Result<Option<uuid::Uuid>> {
    match req.odm_read_generation() {
        Some(OdmReadGenerationGuard::Ready(guard)) if guard.bucket == bucket => Ok(Some(guard.incarnation_id)),
        Some(OdmReadGenerationGuard::Ready(_)) => {
            Err(s3_error!(InternalError, "source generation guard does not match request bucket"))
        }
        Some(OdmReadGenerationGuard::Failed { code, message }) => Err(S3Error::with_message(code.clone(), message.clone())),
        Some(OdmReadGenerationGuard::Unavailable) | None => Ok(None),
    }
}

pub(crate) fn apply_bucket_generation_guard<R: AuthorizationRequest + ?Sized>(
    req: &R,
    bucket: &str,
    opts: &mut ObjectOptions,
) -> S3Result<()> {
    let guard =
        PendingDeleteBucketGenerationGuard::resolve_generation(req.bucket_generation(), req.pending_delete_bucket_generation())?;
    apply_generation_fence(
        req,
        guard.map(|guard| (guard.bucket.as_str(), guard.incarnation_id)),
        bucket,
        opts,
        "bucket generation guard",
    )
}

pub(crate) fn retained_table_data_plane_publication_guards<R: AuthorizationRequest + ?Sized>(
    req: &R,
) -> Option<TableDataPlanePublicationGuards> {
    req.publication_guards().cloned()
}

pub(crate) fn apply_copy_source_bucket_generation_guard<R: AuthorizationRequest + ?Sized>(
    req: &R,
    bucket: &str,
    opts: &mut ObjectOptions,
) -> S3Result<()> {
    let guard = req.copy_source_bucket_generation();
    apply_generation_fence(
        req,
        guard.map(|guard| (guard.bucket.as_str(), guard.incarnation_id)),
        bucket,
        opts,
        "copy source bucket generation guard",
    )
}

/// Binds `opts` to the captured incarnation of `bucket` and to the retained table publication
/// locks. A server request without a captured guard is refused: only direct use-case callers,
/// which carry no server context, run unfenced.
fn apply_generation_fence<R: AuthorizationRequest + ?Sized>(
    req: &R,
    guard: Option<(&str, uuid::Uuid)>,
    bucket: &str,
    opts: &mut ObjectOptions,
    guard_name: &str,
) -> S3Result<()> {
    match guard {
        Some((guarded, _)) if guarded != bucket => {
            return Err(S3Error::with_message(
                S3ErrorCode::InternalError,
                format!("{guard_name} does not match request bucket"),
            ));
        }
        Some((_, incarnation_id)) => opts.expected_bucket_incarnation_id = Some(incarnation_id),
        None if req.server_context().is_some() => {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, format!("{guard_name} is missing")));
        }
        None => {}
    }
    if let Some(publication_guards) = req.publication_guards() {
        publication_guards.add_lock_loss_fences(opts);
    }
    Ok(())
}
