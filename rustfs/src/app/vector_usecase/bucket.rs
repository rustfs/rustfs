// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::resource_list_pagination::ResourceListPagination;
use super::*;
use crate::app::storage_api::managed::{ManagedBucketConfigMutation, ManagedBucketPrecheckError};
use rustfs_gateway_types::cors_tagging::{PersistedTag, PersistedTagging};
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

const LIST_INDEXES_PREFIX_PAGE_FLOOR: usize = 128;
const LIST_INDEXES_MAX_SCANNED_PAGES: usize = 64;
const LIST_INDEXES_SCAN_DEADLINE: Duration = Duration::from_secs(15);

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum VectorBucketPausePoint {
    BeforeIndexCatalogParent,
    BeforeBucketControlMutationLock,
    AfterListIndexesPageFetched,
}

#[cfg(test)]
type VectorBucketPauseHook = Arc<
    dyn Fn(&BucketHandle, VectorBucketPausePoint) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
        + Send
        + Sync,
>;

#[cfg(test)]
static VECTOR_BUCKET_PAUSE_HOOK: std::sync::Mutex<Option<VectorBucketPauseHook>> = std::sync::Mutex::new(None);

#[cfg(test)]
pub(super) struct VectorBucketPauseGuard;

#[cfg(test)]
impl VectorBucketPauseGuard {
    pub(super) fn install<F, Fut>(hook: F) -> Self
    where
        F: Fn(BucketHandle, VectorBucketPausePoint) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let mut slot = VECTOR_BUCKET_PAUSE_HOOK.lock().expect("vector bucket pause hook");
        assert!(slot.is_none(), "vector bucket pause hook already installed");
        *slot = Some(Arc::new(move |bucket, stage| Box::pin(hook(bucket.clone(), stage))));
        Self
    }
}

#[cfg(test)]
impl Drop for VectorBucketPauseGuard {
    fn drop(&mut self) {
        if let Ok(mut slot) = VECTOR_BUCKET_PAUSE_HOOK.lock() {
            *slot = None;
        }
    }
}

#[cfg(test)]
async fn maybe_pause_vector_bucket(bucket: &BucketHandle, stage: VectorBucketPausePoint) {
    let hook = VECTOR_BUCKET_PAUSE_HOOK.lock().expect("vector bucket pause hook").clone();
    if let Some(hook) = hook {
        hook(bucket, stage).await;
    }
}

#[cfg(test)]
static LIST_INDEXES_SCAN_BUDGET_OVERRIDE: std::sync::Mutex<Option<(BucketHandle, usize, usize)>> = std::sync::Mutex::new(None);

#[cfg(test)]
struct ListIndexesScanBudgetGuard;

#[cfg(test)]
impl ListIndexesScanBudgetGuard {
    fn install(bucket: BucketHandle, prefix_page_floor: usize, max_scanned_pages: usize) -> Self {
        let mut slot = LIST_INDEXES_SCAN_BUDGET_OVERRIDE
            .lock()
            .expect("list indexes scan budget override");
        assert!(slot.is_none(), "list indexes scan budget override already installed");
        *slot = Some((bucket, prefix_page_floor, max_scanned_pages));
        Self
    }
}

#[cfg(test)]
impl Drop for ListIndexesScanBudgetGuard {
    fn drop(&mut self) {
        if let Ok(mut slot) = LIST_INDEXES_SCAN_BUDGET_OVERRIDE.lock() {
            *slot = None;
        }
    }
}

#[cfg(test)]
pub(super) async fn with_list_indexes_scan_budget_for_test<F, T>(
    bucket: &BucketHandle,
    prefix_page_floor: usize,
    max_scanned_pages: usize,
    future: F,
) -> T
where
    F: std::future::Future<Output = T>,
{
    let _guard = ListIndexesScanBudgetGuard::install(bucket.clone(), prefix_page_floor, max_scanned_pages);
    future.await
}

#[cfg_attr(not(test), allow(unused_variables))]
fn list_indexes_scan_limits(bucket: &BucketHandle) -> (usize, usize) {
    #[cfg(test)]
    {
        let guard = LIST_INDEXES_SCAN_BUDGET_OVERRIDE
            .lock()
            .expect("list indexes scan budget override");
        if let Some((target, prefix_page_floor, max_scanned_pages)) = guard.as_ref()
            && target == bucket
        {
            return (*prefix_page_floor, *max_scanned_pages);
        }
    }
    (LIST_INDEXES_PREFIX_PAGE_FLOOR, LIST_INDEXES_MAX_SCANNED_PAGES)
}

pub(super) fn authorization_facts(
    handle: &BucketHandle,
    metadata: &BucketMetadata,
) -> Result<BucketAuthorizationFacts, ProtocolError> {
    metadata.validate_bucket_handle(handle).map_err(|_| denied())?;
    let mut tags = BTreeMap::new();
    if let Some(config) = &metadata.tagging_config {
        for tag in &config.tag_set {
            let key = tag.key.as_ref().ok_or_else(unavailable)?;
            let value = tag.value.as_ref().ok_or_else(unavailable)?;
            if tags.insert(key.clone(), value.clone()).is_some() {
                return Err(unavailable());
            }
        }
    }
    Ok(BucketAuthorizationFacts {
        handle: handle.clone(),
        tags,
        policy: metadata.policy_config.clone(),
    })
}

async fn authorize_bucket_metadata(
    authorization: &AuthorizedVectorRequest,
    request: &Request,
    bucket: &BucketHandle,
    metadata: &BucketMetadata,
) -> Result<BucketAuthorizationFacts, ProtocolError> {
    let facts = authorization_facts(bucket, metadata)?;
    authorization
        .verified
        .authorize(request, &VectorTarget::Bucket(&facts))
        .await?;
    Ok(facts)
}

fn arn(handle: &BucketHandle) -> String {
    format!("arn:aws:s3vectors:{}::bucket/{}", handle.region(), handle.public_name().as_str())
}

/// Normalize residual `409 Conflict` errors (such as storage `PreconditionFailed`
/// from lock loss) on `ListVectorBuckets` / `ListIndexes`, whose botocore models
/// do not declare `ConflictException`.
pub(super) fn normalize_resource_list_error(error: ProtocolError) -> ProtocolError {
    if error.code() == ErrorCode::Conflict {
        unavailable()
    } else {
        error
    }
}

pub(super) fn map_list_indexes_non_race_conflict(message: &str) -> ProtocolError {
    if message == "Vector bucket Catalog retirement prevents ordinary admission" {
        unavailable()
    } else {
        ProtocolError::new(ErrorCode::InternalServer, "Vector Catalog operation failed")
    }
}

fn is_per_index_lifetime_conflict(message: &str) -> bool {
    message == "Vector Index lifetime changed"
}

enum AuthorizedBucketMutationPlan {
    DeleteBucket,
    MutateConfig {
        mutation: ManagedBucketConfigMutation,
        response: Response,
    },
}

fn bucket_mutation_plan(request: &Request) -> Option<AuthorizedBucketMutationPlan> {
    match request {
        Request::DeleteVectorBucket(_) => Some(AuthorizedBucketMutationPlan::DeleteBucket),
        Request::PutVectorBucketPolicy(input) => Some(AuthorizedBucketMutationPlan::MutateConfig {
            mutation: ManagedBucketConfigMutation::PutPolicy {
                policy_json: input.policy.as_bytes().to_vec(),
            },
            response: Response::PutVectorBucketPolicy(dto::PutVectorBucketPolicyOutput {}),
        }),
        Request::DeleteVectorBucketPolicy(_) => Some(AuthorizedBucketMutationPlan::MutateConfig {
            mutation: ManagedBucketConfigMutation::DeletePolicy,
            response: Response::DeleteVectorBucketPolicy(dto::DeleteVectorBucketPolicyOutput {}),
        }),
        Request::TagResource(input) => Some(AuthorizedBucketMutationPlan::MutateConfig {
            mutation: ManagedBucketConfigMutation::Tag {
                tags: input.tags.clone(),
            },
            response: Response::TagResource(dto::TagResourceOutput {}),
        }),
        Request::UntagResource(input) => Some(AuthorizedBucketMutationPlan::MutateConfig {
            mutation: ManagedBucketConfigMutation::Untag {
                keys: input.tag_keys.iter().cloned().collect(),
            },
            response: Response::UntagResource(dto::UntagResourceOutput {}),
        }),
        _ => None,
    }
}

fn validate_put_bucket_policy_input(request: &Request) -> Result<(), ProtocolError> {
    if let Request::PutVectorBucketPolicy(input) = request {
        let policy =
            serde_json::from_str(&input.policy).map_err(|_| ProtocolError::validation("policy", "invalid Vector policy"))?;
        crate::vector::access_core::validate_private_bucket_policy(&policy).map_err(access_error)?;
    }
    Ok(())
}

impl DefaultVectorUsecase {
    async fn authorize_bucket_with_fence(
        &self,
        authorization: &AuthorizedVectorRequest,
        request: &Request,
        bucket: &BucketHandle,
    ) -> Result<(BucketIncarnationFenceGuard, Arc<BucketMetadata>, BucketAuthorizationFacts), ProtocolError> {
        let guard = self
            .buckets()
            .acquire_bucket_handle_guard(bucket)
            .await
            .map_err(bucket_error)?;
        let metadata = self
            .buckets()
            .read_managed_bucket_metadata(bucket, &guard)
            .await
            .map_err(bucket_error)?;
        let facts = authorize_bucket_metadata(authorization, request, bucket, &metadata).await?;
        if guard.is_lock_lost() {
            return Err(unavailable());
        }
        Ok((guard, metadata, facts))
    }

    async fn map_delete_storage_error(
        &self,
        identity: &BucketHandle,
        error: crate::app::bucket_usecase::BucketResolveError,
    ) -> ProtocolError {
        use crate::app::bucket_usecase::BucketResolveError as E;
        use crate::app::storage_api::vector_usecase::StorageError;

        if matches!(&error, E::Storage(StorageError::PreconditionFailed) | E::Storage(StorageError::Io(_)))
            && let Err(recheck) = self.buckets().acquire_bucket_handle_guard(identity).await
            && matches!(
                recheck,
                E::MissingIdentity
                    | E::Storage(
                        StorageError::BucketNotFound(_)
                            | StorageError::ConfigNotFound
                            | StorageError::VolumeNotFound
                            | StorageError::ObjectNotFound(_, _)
                    )
            )
        {
            return ProtocolError::new(ErrorCode::NotFound, "Vector bucket does not exist");
        }
        bucket_error(error)
    }

    pub(super) async fn delete_vector_bucket_with_precheck<P, PFut, E>(
        &self,
        identity: &BucketHandle,
        precheck: P,
    ) -> Result<(), ManagedBucketPrecheckError<E, crate::app::bucket_usecase::BucketResolveError>>
    where
        P: FnOnce(&BucketMetadata) -> PFut + Send,
        PFut: std::future::Future<Output = Result<(), E>> + Send,
    {
        use crate::app::storage_api::vector_usecase::StorageError;

        let buckets = self.buckets();
        let store = buckets
            .checked_vector_store(identity)
            .map_err(ManagedBucketPrecheckError::Storage)?;
        #[cfg(test)]
        let mut diagnostic = crate::table_catalog::CatalogRetirementDeleteProbe::begin(identity, store.id);
        let catalog = crate::table_catalog::ObjectTableCatalogStore::for_vector_scope(
            crate::table_catalog::EcStoreTableCatalogObjectBackend::new(store.clone()),
            identity.clone(),
        )
        .await
        .map_err(StorageError::other);
        #[cfg(test)]
        diagnostic.catalog_scope_returned(&catalog);
        let catalog = catalog
            .map_err(crate::app::bucket_usecase::BucketResolveError::Storage)
            .map_err(ManagedBucketPrecheckError::Storage)?;
        let prepared_identity = identity.clone();
        let cleanup_instance_id = store.id;
        #[cfg(test)]
        diagnostic.managed_started();
        #[cfg(test)]
        let mut callback_diagnostic = diagnostic.callback();
        let result = crate::app::storage_api::managed::delete_managed_bucket_with_precheck_and_preparation(
            &store,
            identity,
            precheck,
            move |guard| async move {
                #[cfg(test)]
                callback_diagnostic.entered();
                let execution =
                    crate::table_catalog::CatalogCleanupExecution::new(cleanup_instance_id).map_err(StorageError::other);
                #[cfg(test)]
                if execution.is_err() {
                    callback_diagnostic.returned(&execution);
                }
                let execution = execution?;
                let result = catalog
                    .prepare_vector_bucket_delete(&prepared_identity, guard, execution)
                    .await
                    .map_err(StorageError::other);
                #[cfg(test)]
                callback_diagnostic.returned(&result);
                result
            },
        )
        .await;
        match result {
            Ok(()) => {
                #[cfg(test)]
                diagnostic.managed_returned(&Ok::<(), StorageError>(()));
                Ok(())
            }
            Err(ManagedBucketPrecheckError::Storage(error)) => {
                let probe_result = Err::<(), StorageError>(error);
                #[cfg(test)]
                diagnostic.managed_returned(&probe_result);
                let Err(error) = probe_result else { unreachable!() };
                Err(ManagedBucketPrecheckError::Storage(error.into()))
            }
            Err(ManagedBucketPrecheckError::Precheck(error)) => {
                #[cfg(test)]
                diagnostic.managed_returned(&Err::<(), StorageError>(StorageError::PrefixAccessDenied(
                    identity.storage_bucket_id().as_str().to_owned(),
                    String::new(),
                )));
                Err(ManagedBucketPrecheckError::Precheck(error))
            }
        }
    }

    #[cfg(test)]
    pub(super) async fn delete_vector_bucket_for_test(
        &self,
        identity: &BucketHandle,
    ) -> Result<(), crate::app::bucket_usecase::BucketResolveError> {
        match self
            .delete_vector_bucket_with_precheck(identity, |_| async { Ok::<(), std::convert::Infallible>(()) })
            .await
        {
            Ok(()) => Ok(()),
            Err(ManagedBucketPrecheckError::Precheck(never)) => match never {},
            Err(ManagedBucketPrecheckError::Storage(error)) => Err(error),
        }
    }

    async fn execute_authorized_bucket_mutation(
        &self,
        authorization: &AuthorizedVectorRequest,
        request: &Request,
        bucket: &BucketHandle,
        plan: AuthorizedBucketMutationPlan,
    ) -> Result<Response, ProtocolError> {
        #[cfg(test)]
        maybe_pause_vector_bucket(bucket, VectorBucketPausePoint::BeforeBucketControlMutationLock).await;
        match plan {
            AuthorizedBucketMutationPlan::DeleteBucket => {
                let deletion = self.delete_vector_bucket_with_precheck(bucket, |metadata| {
                    let facts = authorization_facts(bucket, metadata);
                    async move {
                        let facts = facts?;
                        authorization
                            .verified
                            .authorize(request, &VectorTarget::Bucket(&facts))
                            .await?;
                        Ok::<(), ProtocolError>(())
                    }
                });
                #[cfg(test)]
                let deletion = crate::table_catalog::with_catalog_retirement_request(
                    authorization.request_id.as_uuid(),
                    authorization.verified.fault_http_request_id(),
                    deletion,
                );
                match deletion.await {
                    Ok(()) => Ok(Response::DeleteVectorBucket(dto::DeleteVectorBucketOutput {})),
                    Err(ManagedBucketPrecheckError::Precheck(error)) => Err(error),
                    Err(ManagedBucketPrecheckError::Storage(error)) => Err(self.map_delete_storage_error(bucket, error).await),
                }
            }
            AuthorizedBucketMutationPlan::MutateConfig { mutation, response } => {
                match self
                    .buckets()
                    .mutate_managed_bucket_config_with_precheck(bucket, mutation, |metadata| {
                        let facts = authorization_facts(bucket, metadata);
                        async move {
                            let facts = facts?;
                            authorization
                                .verified
                                .authorize(request, &VectorTarget::Bucket(&facts))
                                .await?;
                            validate_put_bucket_policy_input(request)
                        }
                    })
                    .await
                {
                    Ok(_) => Ok(response),
                    Err(ManagedBucketPrecheckError::Precheck(error)) => Err(error),
                    Err(ManagedBucketPrecheckError::Storage(error)) => Err(bucket_error(error)),
                }
            }
        }
    }

    pub(super) async fn initialize_index_catalog(
        &self,
        authorization: &AuthorizedVectorRequest,
        index: &CatalogName,
    ) -> Result<(), ProtocolError> {
        let bucket = authorization.bucket.as_ref().ok_or_else(unavailable)?;
        let catalog = authorization.catalog.as_ref().ok_or_else(unavailable)?;
        let lifecycle = Arc::new(
            self.buckets()
                .acquire_bucket_handle_guard(bucket)
                .await
                .map_err(bucket_error)?,
        );
        let metadata = self
            .buckets()
            .read_managed_bucket_metadata(bucket, &lifecycle)
            .await
            .map_err(bucket_error)?;
        let facts = authorization_facts(bucket, &metadata)?;
        authorization
            .verified
            .authorize(
                authorization.verified.request(),
                &VectorTarget::NewIndex {
                    bucket: &facts,
                    index_name: index,
                },
            )
            .await?;
        #[cfg(test)]
        maybe_pause_vector_bucket(bucket, VectorBucketPausePoint::BeforeIndexCatalogParent).await;
        catalog
            .ensure_vector_catalog_parent(bucket, lifecycle.clone())
            .await
            .map_err(catalog_error)?;
        if lifecycle.is_lock_lost() {
            return Err(unavailable());
        }
        Ok(())
    }

    pub(super) async fn execute_bucket(
        &self,
        authorization: AuthorizedVectorRequest,
        request: &Request,
    ) -> Result<Response, ProtocolError> {
        if let Request::CreateVectorBucket(input) = request {
            let envelope = authorization
                .verified
                .authorize(
                    request,
                    &VectorTarget::NewBucket {
                        public_name: &input.vector_bucket_name,
                    },
                )
                .await?;
            let effective = encryption::requested(input.encryption_configuration.as_ref(), None)?;
            encryption::preflight(&self.server, &envelope, &effective, true).await?;
            let account = AccountNamespaceId::try_new(String::new()).map_err(|_| unavailable())?;
            let public_name = BucketPublicName::try_new(input.vector_bucket_name.clone()).map_err(|_| denied())?;
            let usecase = self.buckets();
            let storage_id = usecase
                .checked_storage_bucket_id(BucketKind::Vector, &account, &self.region, &public_name)
                .map_err(bucket_error)?;
            let handle = BucketHandle::try_from_parts(
                BucketKind::Vector,
                account,
                self.region.clone(),
                public_name,
                storage_id,
                BucketIncarnation::try_new(Uuid::new_v4()).map_err(|_| unavailable())?,
            )
            .map_err(|_| denied())?;
            let tagging = PersistedTagging {
                tag_set: input
                    .tags
                    .iter()
                    .flat_map(|tags| tags.iter())
                    .map(|(key, value)| PersistedTag {
                        key: Some(key.clone()),
                        value: Some(value.clone()),
                    })
                    .collect(),
            };
            let tags = if tagging.tag_set.is_empty() {
                Vec::new()
            } else {
                crate::app::storage_api::vector_usecase::serialize(&tagging).map_err(|_| unavailable())?
            };
            let handle = usecase
                .create_managed_bucket(handle, encryption::configuration_xml(&effective)?, tags)
                .await
                .map_err(bucket_error)?;
            return Ok(Response::CreateVectorBucket(dto::CreateVectorBucketOutput {
                vector_bucket_arn: arn(&handle),
            }));
        }
        if let Request::ListVectorBuckets(input) = request {
            authorization
                .verified
                .authorize(request, &VectorTarget::Service)
                .await
                .map_err(normalize_resource_list_error)?;
            let pagination = ResourceListPagination::for_request(self, &authorization, request, None)
                .map_err(normalize_resource_list_error)?;
            let cursor = pagination
                .decode(input.next_token.as_deref())
                .map_err(normalize_resource_list_error)?;
            let snapshots = self
                .buckets()
                .list_managed_buckets(&AccountNamespaceId::try_new(String::new()).map_err(|_| unavailable())?, &self.region)
                .await
                .map_err(|error| normalize_resource_list_error(bucket_error(error)))?;
            let max = usize::try_from(input.max_results.unwrap_or(500)).map_err(|_| denied())?;
            if let Some(cursor) = &cursor
                && let Some((handle, _)) = snapshots
                    .iter()
                    .find(|(handle, _)| handle.public_name().as_str() == cursor.after)
            {
                pagination
                    .check_identity(cursor, handle, None)
                    .map_err(normalize_resource_list_error)?;
            }
            let mut buckets: Vec<_> = snapshots
                .into_iter()
                .filter(|(handle, _)| {
                    cursor
                        .as_ref()
                        .is_none_or(|cursor| handle.public_name().as_str() > cursor.after.as_str())
                        && input
                            .prefix
                            .as_ref()
                            .is_none_or(|prefix| handle.public_name().as_str().starts_with(prefix))
                })
                .take(max.checked_add(1).ok_or_else(unavailable)?)
                .collect();
            let next_token = if buckets.len() > max {
                buckets.pop();
                let (last, _) = buckets.last().ok_or_else(unavailable)?;
                Some(
                    pagination
                        .encode(last.public_name().as_str(), last, None)
                        .map_err(normalize_resource_list_error)?,
                )
            } else {
                None
            };
            authorization
                .verified
                .authorize(request, &VectorTarget::Service)
                .await
                .map_err(normalize_resource_list_error)?;
            return Ok(Response::ListVectorBuckets(dto::ListVectorBucketsOutput {
                vector_buckets: buckets
                    .into_iter()
                    .map(|(handle, snapshot)| dto::VectorBucketSummary {
                        vector_bucket_name: handle.public_name().as_str().into(),
                        vector_bucket_arn: arn(&handle),
                        creation_time: snapshot.created.unix_timestamp_nanos() as f64 / 1_000_000_000.0,
                    })
                    .collect(),
                next_token,
            }));
        }
        let bucket = authorization.bucket.as_ref().ok_or_else(unavailable)?;
        if let Some(plan) = bucket_mutation_plan(request) {
            return self
                .execute_authorized_bucket_mutation(&authorization, request, bucket, plan)
                .await;
        }
        if let Request::ListIndexes(input) = request {
            return self
                .list_indexes(&authorization, request, bucket, input)
                .await
                .map_err(normalize_resource_list_error);
        }
        let (guard, metadata, facts) = self.authorize_bucket_with_fence(&authorization, request, bucket).await?;
        let response = match request {
            Request::GetVectorBucket(_) => Response::GetVectorBucket(dto::GetVectorBucketOutput {
                vector_bucket: dto::VectorBucket {
                    vector_bucket_name: bucket.public_name().as_str().into(),
                    vector_bucket_arn: arn(bucket),
                    creation_time: metadata.created.unix_timestamp_nanos() as f64 / 1_000_000_000.0,
                    encryption_configuration: encryption::public_configuration(&encryption::stored(&metadata)?),
                    default_index_mode: Some(dto::IndexMode::Enhanced),
                },
            }),
            Request::GetVectorBucketPolicy(_) => Response::GetVectorBucketPolicy(dto::GetVectorBucketPolicyOutput {
                policy: if metadata.policy_config_json.is_empty() {
                    None
                } else {
                    Some(String::from_utf8(metadata.policy_config_json.clone()).map_err(|_| unavailable())?)
                },
            }),
            Request::ListTagsForResource(_) => Response::ListTagsForResource(dto::ListTagsForResourceOutput { tags: facts.tags }),
            Request::PutVectorBucketDefaultIndexMode(input) => {
                if input.default_index_mode != dto::IndexMode::Enhanced {
                    return Err(ProtocolError::validation("defaultIndexMode", "only ENHANCED is supported"));
                }
                Response::PutVectorBucketDefaultIndexMode(dto::PutVectorBucketDefaultIndexModeOutput {})
            }
            _ => {
                return Err(ProtocolError::new(
                    ErrorCode::InternalServer,
                    "request does not match its admitted target",
                ));
            }
        };
        if guard.is_lock_lost() {
            return Err(unavailable());
        }
        Ok(response)
    }

    async fn verify_list_indexes_parent_bucket(&self, bucket: &BucketHandle) -> Result<(), ProtocolError> {
        let guard = self
            .buckets()
            .acquire_bucket_handle_guard(bucket)
            .await
            .map_err(|error| normalize_resource_list_error(bucket_error(error)))?;
        if guard.is_lock_lost() {
            return Err(unavailable());
        }
        Ok(())
    }

    async fn open_listed_vector_index(
        &self,
        catalog: &Catalog,
        bucket: &BucketHandle,
        namespace: &crate::table_catalog::Namespace,
        name: &CatalogName,
    ) -> Result<
        Option<(
            crate::table_catalog::VectorCatalogOperationGuard,
            crate::table_catalog::VectorCatalogEntry,
        )>,
        ProtocolError,
    > {
        for _attempt in 0..2 {
            match catalog.begin_vector_operation(bucket, namespace, name).await {
                Ok((operation, current)) => {
                    if operation.resource_identity().vector_bucket() != Some(bucket) {
                        self.verify_list_indexes_parent_bucket(bucket).await?;
                        return Err(unavailable());
                    }
                    operation
                        .check_held()
                        .map_err(|error| normalize_resource_list_error(catalog_error(error)))?;
                    return Ok(Some((operation, current)));
                }
                Err(
                    TableCatalogStoreError::NotFound(_)
                    | TableCatalogStoreError::NamespaceNotFound(_)
                    | TableCatalogStoreError::TableNotFound(_),
                ) => {
                    self.verify_list_indexes_parent_bucket(bucket).await?;
                    return Ok(None);
                }
                Err(TableCatalogStoreError::Conflict(message)) if is_per_index_lifetime_conflict(&message) => {
                    self.verify_list_indexes_parent_bucket(bucket).await?;
                }
                Err(TableCatalogStoreError::Conflict(message)) => {
                    self.verify_list_indexes_parent_bucket(bucket).await?;
                    return Err(map_list_indexes_non_race_conflict(&message));
                }
                Err(TableCatalogStoreError::Internal(message)) => {
                    self.verify_list_indexes_parent_bucket(bucket).await?;
                    return Err(catalog_error(TableCatalogStoreError::Internal(message)));
                }
                Err(error) => return Err(normalize_resource_list_error(catalog_error(error))),
            }
        }
        self.verify_list_indexes_parent_bucket(bucket).await?;
        Err(unavailable())
    }

    async fn list_indexes(
        &self,
        authorization: &AuthorizedVectorRequest,
        request: &Request,
        bucket: &BucketHandle,
        input: &dto::ListIndexesInput,
    ) -> Result<Response, ProtocolError> {
        use crate::table_catalog::TableCatalogStore;

        // Authorize against the authoritative bucket state before touching the Catalog,
        // then drop the fence so Catalog list/read methods do not nest lifecycle locks.
        let (pre_guard, _, _) = self
            .authorize_bucket_with_fence(authorization, request, bucket)
            .await
            .map_err(normalize_resource_list_error)?;
        drop(pre_guard);

        let pagination = ResourceListPagination::for_request(self, authorization, request, Some(bucket))
            .map_err(normalize_resource_list_error)?;
        let position = pagination
            .decode(input.next_token.as_deref())
            .map_err(normalize_resource_list_error)?;
        let max = usize::try_from(input.max_results.unwrap_or(500)).map_err(|_| denied())?;
        let (prefix_page_floor, max_scanned_pages) = list_indexes_scan_limits(bucket);
        let raw_page_size = max.checked_add(1).ok_or_else(unavailable)?;
        let bounded_page_size = if input.prefix.is_some() {
            raw_page_size.max(prefix_page_floor)
        } else {
            raw_page_size
        };
        let page_size = NonZeroUsize::new(bounded_page_size).ok_or_else(unavailable)?;
        let catalog = match self.catalog(bucket, authorization.reader.clone()).await {
            Ok(catalog) => catalog,
            Err(error) => {
                self.verify_list_indexes_parent_bucket(bucket).await?;
                return Err(normalize_resource_list_error(error));
            }
        };
        let namespace = index_namespace()?;
        let mut cursor = if let Some(position) = &position {
            let name = CatalogName::parse(&position.after).map_err(|_| unavailable())?;
            let mut anchor_verified = false;
            for _attempt in 0..2 {
                match catalog.begin_vector_operation(bucket, &namespace, &name).await {
                    Ok((operation, _)) => {
                        let resource = operation.resource_identity();
                        if resource.vector_bucket() != Some(bucket) {
                            self.verify_list_indexes_parent_bucket(bucket).await?;
                            return Err(unavailable());
                        }
                        pagination.check_identity(position, bucket, Some(resource.table_id().as_str()))?;
                        operation
                            .check_held()
                            .map_err(|error| normalize_resource_list_error(catalog_error(error)))?;
                        anchor_verified = true;
                        break;
                    }
                    Err(
                        TableCatalogStoreError::NotFound(_)
                        | TableCatalogStoreError::NamespaceNotFound(_)
                        | TableCatalogStoreError::TableNotFound(_),
                    ) => {
                        self.verify_list_indexes_parent_bucket(bucket).await?;
                        anchor_verified = true;
                        break;
                    }
                    Err(TableCatalogStoreError::Conflict(message)) if is_per_index_lifetime_conflict(&message) => {
                        self.verify_list_indexes_parent_bucket(bucket).await?;
                    }
                    Err(TableCatalogStoreError::Conflict(message)) => {
                        self.verify_list_indexes_parent_bucket(bucket).await?;
                        return Err(map_list_indexes_non_race_conflict(&message));
                    }
                    Err(TableCatalogStoreError::Internal(message)) => {
                        self.verify_list_indexes_parent_bucket(bucket).await?;
                        return Err(catalog_error(TableCatalogStoreError::Internal(message)));
                    }
                    Err(error) => return Err(normalize_resource_list_error(catalog_error(error))),
                }
            }
            if !anchor_verified {
                // Anchor index lifetime kept changing across retries (`Vector Index lifetime changed`)
                // while the parent bucket remained the same, so its table_id cannot match the token.
                return Err(ProtocolError::validation("nextToken", "Invalid continuation token"));
            }
            Some(
                catalog
                    .vector_index_list_cursor_after(bucket, &namespace, &name)
                    .map_err(|error| normalize_resource_list_error(catalog_error(error)))?,
            )
        } else {
            None
        };

        let mut indexes = Vec::with_capacity(max.min(64));
        let mut last_returned: Option<(String, String)> = None;
        let mut next_token = None;
        let mut scanned_pages = 0usize;
        let scan_started = Instant::now();

        'pages: loop {
            if scanned_pages >= max_scanned_pages || scan_started.elapsed() >= LIST_INDEXES_SCAN_DEADLINE {
                return Err(ProtocolError::new(
                    ErrorCode::ServiceUnavailable,
                    "Vector Index prefix scan budget exceeded",
                ));
            }
            scanned_pages += 1;

            let page = match catalog
                .list_tables_page(
                    bucket.storage_bucket_id().as_str(),
                    &namespace.public_name(),
                    cursor.as_deref(),
                    page_size,
                )
                .await
            {
                Ok(page) => page,
                Err(TableCatalogStoreError::Conflict(message)) => {
                    self.verify_list_indexes_parent_bucket(bucket).await?;
                    return Err(map_list_indexes_non_race_conflict(&message));
                }
                Err(error) => {
                    if matches!(error, TableCatalogStoreError::NotFound(_) | TableCatalogStoreError::Internal(_)) {
                        self.verify_list_indexes_parent_bucket(bucket).await?;
                    }
                    return Err(normalize_resource_list_error(catalog_error(error)));
                }
            };
            #[cfg(test)]
            maybe_pause_vector_bucket(bucket, VectorBucketPausePoint::AfterListIndexesPageFetched).await;

            for listed in page.entries {
                if input.prefix.as_ref().is_some_and(|prefix| !listed.table.starts_with(prefix)) {
                    continue;
                }
                if indexes.len() == max {
                    let (name, table_id) = last_returned.as_ref().ok_or_else(unavailable)?;
                    next_token = Some(pagination.encode(name, bucket, Some(table_id))?);
                    break 'pages;
                }
                let name = CatalogName::parse(&listed.table).map_err(|_| unavailable())?;
                let Some((operation, current)) = self.open_listed_vector_index(&catalog, bucket, &namespace, &name).await? else {
                    continue;
                };
                let table_id = operation.resource_identity().table_id().as_str().to_owned();
                last_returned = Some((listed.table.clone(), table_id));
                indexes.push(dto::IndexSummary {
                    vector_bucket_name: bucket.public_name().as_str().into(),
                    index_name: listed.table,
                    index_arn: format!("{}/index/{}", arn(bucket), name.as_str()),
                    creation_time: current.created_at().map_err(catalog_error)?.unix_timestamp_nanos() as f64 / 1_000_000_000.0,
                });
            }
            match page.next_cursor {
                Some(next) if cursor.as_ref() != Some(&next) => cursor = Some(next),
                Some(_) => return Err(unavailable()),
                None => break,
            }
        }

        let (_post_guard, _, _) = self.authorize_bucket_with_fence(authorization, request, bucket).await?;
        Ok(Response::ListIndexes(dto::ListIndexesOutput { next_token, indexes }))
    }
}
