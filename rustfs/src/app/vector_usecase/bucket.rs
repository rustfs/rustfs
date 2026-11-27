// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::resource_list_pagination::ResourceListPagination;
use super::*;
use rustfs_gateway_types::cors_tagging::{PersistedTag, PersistedTagging};
use std::collections::BTreeMap;

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

fn arn(handle: &BucketHandle) -> String {
    format!("arn:aws:s3vectors:{}::bucket/{}", handle.region(), handle.public_name().as_str())
}

impl DefaultVectorUsecase {
    pub(super) async fn initialize_index_catalog(
        &self,
        authorization: &AuthorizedVectorRequest,
        index: &CatalogName,
    ) -> Result<(), ProtocolError> {
        use crate::table_catalog::{
            NamespaceEntry, TableBucketEntry, TableCatalogEntryState, TableCatalogStore, VectorBucketBindingV1,
        };
        let bucket = authorization.bucket.as_ref().ok_or_else(unavailable)?;
        let catalog = authorization.catalog.as_ref().ok_or_else(unavailable)?;
        let lifecycle = self
            .buckets()
            .acquire_bucket_handle_guard(bucket)
            .await
            .map_err(bucket_error)?;
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
        let storage_name = bucket.storage_bucket_id().as_str();
        let binding = VectorBucketBindingV1::from_handle(bucket).map_err(catalog_error)?;
        catalog
            .ensure_table_bucket(TableBucketEntry {
                vector_bucket_binding: Some(binding.clone()),
                version: 2,
                table_bucket: storage_name.into(),
                catalog_type: "s3vectors".into(),
                warehouse_root: format!("s3://{storage_name}/"),
                state: TableCatalogEntryState::Active,
                properties: Default::default(),
                active_rename_id: None,
                created_at: None,
                updated_at: None,
            })
            .await
            .map_err(catalog_error)?;
        let parent = catalog
            .get_table_bucket(storage_name)
            .await
            .map_err(catalog_error)?
            .ok_or_else(unavailable)?;
        if parent.vector_bucket_binding.as_ref() != Some(&binding)
            || parent.state != TableCatalogEntryState::Active
            || parent.version != 2
            || parent.catalog_type != "s3vectors"
            || parent.table_bucket != storage_name
            || parent.warehouse_root != format!("s3://{storage_name}/")
            || parent.active_rename_id.is_some()
        {
            return Err(denied());
        }
        let namespace = index_namespace()?;
        if catalog
            .get_namespace(storage_name, &namespace.public_name())
            .await
            .map_err(catalog_error)?
            .is_none()
        {
            let result = catalog
                .create_namespace(NamespaceEntry {
                    version: 1,
                    table_bucket: storage_name.into(),
                    namespace: namespace.public_name(),
                    namespace_id: namespace.storage_id(),
                    state: TableCatalogEntryState::Active,
                    properties: Default::default(),
                    created_at: None,
                    updated_at: None,
                })
                .await;
            if let Err(error) = result
                && !matches!(error, TableCatalogStoreError::Conflict(_))
            {
                return Err(catalog_error(error));
            }
        }
        let current = catalog
            .get_namespace(storage_name, &namespace.public_name())
            .await
            .map_err(catalog_error)?
            .ok_or_else(unavailable)?;
        if current.version != 1
            || current.table_bucket != storage_name
            || current.namespace != namespace.public_name()
            || current.namespace_id != namespace.storage_id()
            || current.state != TableCatalogEntryState::Active
            || !current.properties.is_empty()
        {
            return Err(denied());
        }
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
            authorization.verified.authorize(request, &VectorTarget::Service).await?;
            let pagination = ResourceListPagination::for_request(self, &authorization, request, None)?;
            let cursor = pagination.decode(input.next_token.as_deref())?;
            let snapshots = self
                .buckets()
                .list_managed_buckets(&AccountNamespaceId::try_new(String::new()).map_err(|_| unavailable())?, &self.region)
                .await
                .map_err(bucket_error)?;
            let max = usize::try_from(input.max_results.unwrap_or(500)).map_err(|_| denied())?;
            if let Some(cursor) = &cursor
                && let Some((handle, _)) = snapshots
                    .iter()
                    .find(|(handle, _)| handle.public_name().as_str() == cursor.after)
            {
                pagination.check_identity(cursor, handle, None)?;
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
                Some(pagination.encode(last.public_name().as_str(), last, None)?)
            } else {
                None
            };
            authorization.verified.authorize(request, &VectorTarget::Service).await?;
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
        let facts = authorization_facts(bucket, &metadata)?;
        authorization
            .verified
            .authorize(request, &VectorTarget::Bucket(&facts))
            .await?;
        if matches!(request, Request::DeleteVectorBucket(_)) {
            if guard.is_lock_lost() {
                return Err(unavailable());
            }
            drop(guard);
            let buckets = self.buckets();
            let deletion = buckets.delete_managed_bucket(bucket);
            #[cfg(test)]
            let deletion = crate::table_catalog::with_catalog_retirement_request(
                authorization.request_id.as_uuid(),
                authorization.verified.fault_http_request_id(),
                deletion,
            );
            deletion.await.map_err(bucket_error)?;
            return Ok(Response::DeleteVectorBucket(dto::DeleteVectorBucketOutput {}));
        }
        if let Request::ListIndexes(input) = request {
            drop(guard);
            return self.list_indexes(&authorization, bucket, input).await;
        }
        use crate::app::storage_api::managed::ManagedBucketConfigMutation;
        let mutation = match request {
            Request::PutVectorBucketPolicy(input) => {
                let policy = serde_json::from_str(&input.policy)
                    .map_err(|_| ProtocolError::validation("policy", "invalid Vector policy"))?;
                crate::vector::access_core::validate_private_bucket_policy(&policy).map_err(access_error)?;
                Some((
                    ManagedBucketConfigMutation::PutPolicy {
                        policy_json: input.policy.as_bytes().to_vec(),
                    },
                    Response::PutVectorBucketPolicy(dto::PutVectorBucketPolicyOutput {}),
                ))
            }
            Request::DeleteVectorBucketPolicy(_) => Some((
                ManagedBucketConfigMutation::DeletePolicy,
                Response::DeleteVectorBucketPolicy(dto::DeleteVectorBucketPolicyOutput {}),
            )),
            Request::TagResource(input) => Some((
                ManagedBucketConfigMutation::Tag {
                    tags: input.tags.clone(),
                },
                Response::TagResource(dto::TagResourceOutput {}),
            )),
            Request::UntagResource(input) => Some((
                ManagedBucketConfigMutation::Untag {
                    keys: input.tag_keys.iter().cloned().collect(),
                },
                Response::UntagResource(dto::UntagResourceOutput {}),
            )),
            _ => None,
        };
        if let Some((mutation, response)) = mutation {
            if guard.is_lock_lost() {
                return Err(unavailable());
            }
            // The storage mutation owns its lifecycle guard and revalidates the complete identity.
            drop(guard);
            self.buckets()
                .mutate_managed_bucket_config(bucket, mutation)
                .await
                .map_err(bucket_error)?;
            return Ok(response);
        }
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

    async fn list_indexes(
        &self,
        authorization: &AuthorizedVectorRequest,
        bucket: &BucketHandle,
        input: &dto::ListIndexesInput,
    ) -> Result<Response, ProtocolError> {
        use crate::table_catalog::TableCatalogStore;
        let pagination =
            ResourceListPagination::for_request(self, authorization, authorization.verified.request(), Some(bucket))?;
        let position = pagination.decode(input.next_token.as_deref())?;
        let max = usize::try_from(input.max_results.unwrap_or(500)).map_err(|_| denied())?;
        let page_size = NonZeroUsize::new(max.checked_add(1).ok_or_else(unavailable)?).ok_or_else(unavailable)?;
        let catalog = self.catalog(bucket, authorization.reader.clone()).await?;
        let namespace = index_namespace()?;
        let mut cursor = if let Some(position) = &position {
            if let Some(listed) = catalog
                .load_table(bucket.storage_bucket_id().as_str(), &namespace.public_name(), &position.after)
                .await
                .map_err(catalog_error)?
            {
                let name = CatalogName::parse(&position.after).map_err(|_| unavailable())?;
                let (operation, _) = catalog
                    .begin_vector_operation(bucket, &namespace, &name)
                    .await
                    .map_err(catalog_error)?;
                let resource = operation.resource_identity();
                if resource.vector_bucket() != Some(bucket)
                    || resource.table_id().as_str() != listed.table_id
                    || listed.table_uuid != listed.table_id
                {
                    return Err(ProtocolError::new(ErrorCode::Conflict, "Vector Index changed during listing"));
                }
                pagination.check_identity(position, bucket, Some(resource.table_id().as_str()))?;
                operation.check_held().map_err(catalog_error)?;
            }
            Some(
                catalog
                    .vector_index_list_cursor_after(
                        bucket,
                        &namespace,
                        &CatalogName::parse(&position.after).map_err(|_| unavailable())?,
                    )
                    .map_err(catalog_error)?,
            )
        } else {
            None
        };
        let mut indexes = Vec::new();
        let mut last_returned: Option<(String, String)> = None;
        let mut next_token = None;
        'pages: loop {
            let page = catalog
                .list_tables_page(
                    bucket.storage_bucket_id().as_str(),
                    &namespace.public_name(),
                    cursor.as_deref(),
                    page_size,
                )
                .await
                .map_err(catalog_error)?;
            for listed in page.entries {
                if input.prefix.as_ref().is_some_and(|prefix| !listed.table.starts_with(prefix)) {
                    continue;
                }
                let name = CatalogName::parse(&listed.table).map_err(|_| unavailable())?;
                let (operation, current) = catalog
                    .begin_vector_operation(bucket, &namespace, &name)
                    .await
                    .map_err(catalog_error)?;
                let resource = operation.resource_identity();
                if resource.vector_bucket() != Some(bucket)
                    || resource.table_id().as_str() != listed.table_id
                    || listed.table_uuid != listed.table_id
                {
                    return Err(ProtocolError::new(ErrorCode::Conflict, "Vector Index changed during listing"));
                }
                let lifecycle = operation.lifecycle_guard().map_err(catalog_error)?;
                let metadata = self
                    .buckets()
                    .read_managed_bucket_metadata(bucket, &lifecycle)
                    .await
                    .map_err(bucket_error)?;
                let facts = authorization_facts(bucket, &metadata)?;
                authorization
                    .verified
                    .authorize(authorization.verified.request(), &VectorTarget::Bucket(&facts))
                    .await?;
                operation.check_held().map_err(catalog_error)?;
                if indexes.len() == max {
                    let (name, table_id) = last_returned.as_ref().ok_or_else(unavailable)?;
                    next_token = Some(pagination.encode(name, bucket, Some(table_id))?);
                    break 'pages;
                }
                last_returned = Some((listed.table.clone(), resource.table_id().as_str().to_owned()));
                indexes.push(dto::IndexSummary {
                    vector_bucket_name: bucket.public_name().as_str().into(),
                    index_name: listed.table.clone(),
                    index_arn: format!("{}/index/{}", arn(bucket), listed.table),
                    creation_time: current.created_at().map_err(catalog_error)?.unix_timestamp_nanos() as f64 / 1_000_000_000.0,
                });
            }
            match page.next_cursor {
                Some(next) if cursor.as_ref() != Some(&next) => cursor = Some(next),
                Some(_) => return Err(unavailable()),
                None => break,
            }
        }
        let lifecycle = self
            .buckets()
            .acquire_bucket_handle_guard(bucket)
            .await
            .map_err(bucket_error)?;
        let metadata = self
            .buckets()
            .read_managed_bucket_metadata(bucket, &lifecycle)
            .await
            .map_err(bucket_error)?;
        authorization
            .verified
            .authorize(
                authorization.verified.request(),
                &VectorTarget::Bucket(&authorization_facts(bucket, &metadata)?),
            )
            .await?;
        Ok(Response::ListIndexes(dto::ListIndexesOutput { next_token, indexes }))
    }
}
