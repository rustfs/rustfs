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

pub(crate) const SITE_REPLICATION_PEER_BUCKET_OPS_PATH: &str = "/rustfs/admin/v3/site-replication/peer/bucket-ops";

pub(crate) const SITE_REPLICATION_BUCKET_OP_MAKE_WITH_VERSIONING: &str = "make-with-versioning";

pub(crate) const SITE_REPLICATION_BUCKET_OP_CONFIGURE_REPLICATION: &str = "configure-replication";

pub(crate) static SITE_REPLICATION_BUCKET_OP_LOCK: LazyLock<RwLock<()>> = LazyLock::new(|| RwLock::new(()));

const SITE_REPLICATION_BUCKET_MUTATION_LOCK_PREFIX: &str = "config/site-replication/bucket-mutation";
pub(crate) const SITE_REPLICATION_BUCKET_MUTATION_ADMISSION_LOCK_PATH: &str =
    "config/site-replication/bucket-mutation-admission.lock";

pub(crate) fn site_replication_bucket_mutation_lock_path(bucket: &str) -> String {
    format!("{SITE_REPLICATION_BUCKET_MUTATION_LOCK_PREFIX}/{bucket}.lock")
}

pub(crate) async fn with_site_replication_bucket_mutation_lock<F, Fut, T>(
    store: Arc<ECStore>,
    bucket: &str,
    operation: F,
) -> S3Result<T>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let mutation_store = store.clone();
    let mutation_path = site_replication_bucket_mutation_lock_path(bucket);
    with_config_object_read_lock(
        store,
        SITE_REPLICATION_BUCKET_MUTATION_ADMISSION_LOCK_PATH.to_string(),
        move || async move {
            with_config_object_write_lock(mutation_store, mutation_path, operation)
                .await
                .map_err(|err| S3Error::from(ApiError::from(err)))
        },
    )
    .await
    .map_err(|err| S3Error::from(ApiError::from(err)))?
}

/// Exclude every local bucket namespace mutation from an add's local preflight
/// snapshot until its topology commit. Peer bootstrap callbacks do not enter
/// this public-mutation admission path, so they can finish while the writer is
/// held; post-commit fan-out and backfill must run after it is released.
pub(crate) async fn with_site_replication_bucket_mutation_admission_lock<F, Fut, T>(
    store: Arc<ECStore>,
    operation: F,
) -> S3Result<T>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = S3Result<T>> + Send + 'static,
    T: Send + 'static,
{
    with_config_object_write_lock(store, SITE_REPLICATION_BUCKET_MUTATION_ADMISSION_LOCK_PATH.to_string(), operation)
        .await
        .map_err(|err| S3Error::from(ApiError::from(err)))?
}

#[derive(Debug, Default)]
pub(crate) struct SiteReplicationBootstrapPlan {
    pub(crate) iam_items: Vec<SRIAMItem>,
    pub(crate) bucket_make_ops: Vec<String>,
    pub(crate) bucket_items: Vec<SRBucketMeta>,
    pub(crate) bucket_configure_ops: Vec<String>,
}

pub(crate) fn bootstrap_bucket_op_path(bucket: &str, operation: &str) -> String {
    format!(
        "/rustfs/admin/v3/site-replication/peer/bucket-ops?{}",
        form_urlencoded::Serializer::new(String::new())
            .append_pair("bucket", bucket)
            .append_pair("operation", operation)
            .finish()
    )
}

pub(crate) fn with_site_replication_bootstrap_token(path: &str, token: &str) -> String {
    let separator = if path.contains('?') { '&' } else { '?' };
    let query = form_urlencoded::Serializer::new(String::new())
        .append_pair("bootstrapToken", token)
        .finish();
    format!("{path}{separator}{query}")
}

/// Query for a peer `make-with-versioning` bucket op. `versioningEnabled`
/// always travels so the outbound query matches MinIO's site-replication
/// make-bucket wire contract: MinIO's own create-bucket hook sends
/// `versioningEnabled=true` on this op. RustFS's inbound handler
/// force-enables versioning either way.
pub(crate) fn make_with_versioning_bucket_op_path(bucket: &str, created_at: Option<&str>, lock_enabled: bool) -> String {
    let mut query = form_urlencoded::Serializer::new(String::new());
    query.append_pair("bucket", bucket);
    query.append_pair("operation", SITE_REPLICATION_BUCKET_OP_MAKE_WITH_VERSIONING);
    query.append_pair("versioningEnabled", "true");
    if let Some(created_at) = created_at {
        query.append_pair("createdAt", created_at);
    }
    if lock_enabled {
        query.append_pair("lockEnabled", "true");
    }
    format!("{SITE_REPLICATION_PEER_BUCKET_OPS_PATH}?{}", query.finish())
}

pub(crate) fn bootstrap_bucket_make_op_path(bucket: &SRBucketInfo) -> String {
    let created_at = bucket
        .created_at
        .and_then(|value| value.format(&time::format_description::well_known::Rfc3339).ok());
    make_with_versioning_bucket_op_path(&bucket.bucket, created_at.as_deref(), bucket.object_lock_config.is_some())
}

pub(crate) fn bootstrap_bucket_meta_item(
    bucket: &SRBucketInfo,
    item_type: &str,
    updated_at: Option<OffsetDateTime>,
) -> SRBucketMeta {
    SRBucketMeta {
        bucket: bucket.bucket.clone(),
        r#type: item_type.to_string(),
        updated_at,
        api_version: Some(SITE_REPL_API_VERSION.to_string()),
        derived_rule_contract: true,
        ..Default::default()
    }
}

pub(crate) fn bootstrap_bucket_quota_value(bucket: &str, raw: &str) -> S3Result<Value> {
    serde_json::from_slice(&decode_bucket_meta_wire_value(raw))
        .map_err(|e| s3_error!(InvalidRequest, "invalid quota metadata for bootstrap bucket `{bucket}`: {e}"))
}

pub(crate) fn append_bootstrap_bucket_item(
    items: &mut Vec<SRBucketMeta>,
    bucket: &SRBucketInfo,
    item_type: &str,
    value: Option<String>,
    updated_at: Option<OffsetDateTime>,
    apply: impl FnOnce(&mut SRBucketMeta, String) -> S3Result<()>,
) -> S3Result<()> {
    if let Some(value) = value {
        let mut item = bootstrap_bucket_meta_item(bucket, item_type, updated_at);
        apply(&mut item, value)?;
        items.push(item);
    }
    Ok(())
}

pub(crate) fn append_bootstrap_bucket_items(
    plan: &mut SiteReplicationBootstrapPlan,
    bucket: &SRBucketInfo,
    replicate_ilm_expiry: bool,
) -> S3Result<()> {
    append_bootstrap_bucket_item(
        &mut plan.bucket_items,
        bucket,
        "policy",
        bucket.policy.clone().map(|value| value.to_string()),
        bucket.policy_updated_at,
        |item, value| {
            item.policy =
                Some(serde_json::from_str(&value).map_err(|e| {
                    s3_error!(InvalidRequest, "invalid bucket policy for bootstrap bucket `{}`: {e}", item.bucket)
                })?);
            Ok(())
        },
    )?;
    append_bootstrap_bucket_item(
        &mut plan.bucket_items,
        bucket,
        "version-config",
        bucket.versioning.clone(),
        bucket.versioning_config_updated_at,
        |item, value| {
            item.versioning = Some(value);
            Ok(())
        },
    )?;
    append_bootstrap_bucket_item(
        &mut plan.bucket_items,
        bucket,
        "tags",
        bucket.tags.clone(),
        bucket.tag_config_updated_at,
        |item, value| {
            item.tags = Some(value);
            Ok(())
        },
    )?;
    append_bootstrap_bucket_item(
        &mut plan.bucket_items,
        bucket,
        "object-lock-config",
        bucket.object_lock_config.clone(),
        bucket.object_lock_config_updated_at,
        |item, value| {
            item.object_lock_config = Some(value);
            Ok(())
        },
    )?;
    append_bootstrap_bucket_item(
        &mut plan.bucket_items,
        bucket,
        "sse-config",
        bucket.sse_config.clone(),
        bucket.sse_config_updated_at,
        |item, value| {
            item.sse_config = Some(value);
            Ok(())
        },
    )?;
    append_bootstrap_bucket_item(
        &mut plan.bucket_items,
        bucket,
        "replication-config",
        bucket.replication_config.clone(),
        bucket.replication_config_updated_at,
        |item, value| {
            item.replication_config = Some(value);
            Ok(())
        },
    )?;
    append_bootstrap_bucket_item(
        &mut plan.bucket_items,
        bucket,
        "quota-config",
        bucket.quota_config.clone(),
        bucket.quota_config_updated_at,
        |item, value| {
            item.quota = Some(bootstrap_bucket_quota_value(&item.bucket, &value)?);
            Ok(())
        },
    )?;
    if replicate_ilm_expiry {
        if bucket.expiry_lc_config.is_some() {
            append_bootstrap_bucket_item(
                &mut plan.bucket_items,
                bucket,
                "lc-config",
                bucket.expiry_lc_config.clone(),
                bucket.expiry_lc_config_updated_at,
                |item, value| {
                    item.expiry_lc_config = Some(value);
                    // `updated_at` here is the entry's expiry axis (see the
                    // SRBucketInfo construction), not the wall clock.
                    item.expiry_updated_at = item.updated_at;
                    Ok(())
                },
            )?;
        } else if bucket.expiry_lc_config_updated_at.is_some() {
            // Expiry rules were removed at this axis (lifecycle_expiry_statement):
            // an explicit timestamped delete item, so a peer that missed the
            // live delete converges on bootstrap/repair instead of keeping
            // stale expiry rules. The receiver's staleness guard protects a
            // peer whose expiry state is newer.
            let mut item = bootstrap_bucket_meta_item(bucket, "lc-config", bucket.expiry_lc_config_updated_at);
            item.expiry_updated_at = item.updated_at;
            plan.bucket_items.push(item);
        }
    }
    append_bootstrap_bucket_item(
        &mut plan.bucket_items,
        bucket,
        "cors-config",
        bucket.cors_config.clone(),
        bucket.cors_config_updated_at,
        |item, value| {
            item.cors = Some(value);
            Ok(())
        },
    )
}

pub(crate) fn group_status_from_desc(status: &str) -> GroupStatus {
    if status.eq_ignore_ascii_case("disabled") {
        GroupStatus::Disabled
    } else {
        GroupStatus::Enabled
    }
}

pub(crate) fn site_replication_info_replicates_ilm_expiry(info: &SRInfo) -> bool {
    info.state.peers.values().any(|peer| peer.replicate_ilm_expiry)
}

pub(crate) fn site_replication_state_replicates_ilm_expiry(state: &SiteReplicationState) -> bool {
    state.peers.values().any(|peer| peer.replicate_ilm_expiry)
}

pub(crate) fn site_replication_bootstrap_plan(info: &SRInfo) -> S3Result<SiteReplicationBootstrapPlan> {
    let mut plan = SiteReplicationBootstrapPlan::default();
    let replicate_ilm_expiry = site_replication_info_replicates_ilm_expiry(info);

    for (name, policy) in &info.policies {
        plan.iam_items.push(SRIAMItem {
            r#type: "policy".to_string(),
            name: name.clone(),
            policy: policy.policy.clone(),
            updated_at: policy.updated_at,
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
            ..Default::default()
        });
    }

    for (access_key, user) in &info.user_info_map {
        if let Some(secret_key) = &user.secret_key {
            plan.iam_items.push(SRIAMItem {
                r#type: "iam-user".to_string(),
                iam_user: Some(rustfs_madmin::SRIAMUser {
                    access_key: access_key.clone(),
                    is_delete_req: false,
                    user_req: Some(AddOrUpdateUserReq {
                        secret_key: secret_key.clone(),
                        policy: user.policy_name.clone(),
                        status: user.status.clone(),
                    }),
                    api_version: Some(SITE_REPL_API_VERSION.to_string()),
                }),
                updated_at: user.updated_at,
                api_version: Some(SITE_REPL_API_VERSION.to_string()),
                ..Default::default()
            });
        }
    }

    for (name, desc) in &info.group_desc_map {
        plan.iam_items.push(SRIAMItem {
            r#type: "group-info".to_string(),
            group_info: Some(SRGroupInfo {
                update_req: GroupAddRemove {
                    group: if desc.name.is_empty() {
                        name.clone()
                    } else {
                        desc.name.clone()
                    },
                    members: desc.members.clone(),
                    status: group_status_from_desc(&desc.status),
                    is_remove: false,
                },
                api_version: Some(SITE_REPL_API_VERSION.to_string()),
            }),
            updated_at: desc.updated_at,
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
            ..Default::default()
        });
    }

    for mapping in info.user_policies.values().chain(info.group_policies.values()) {
        plan.iam_items.push(SRIAMItem {
            r#type: "policy-mapping".to_string(),
            policy_mapping: Some(mapping.clone()),
            updated_at: mapping.updated_at,
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
            ..Default::default()
        });
    }

    for bucket in info.buckets.values() {
        plan.bucket_make_ops.push(bootstrap_bucket_make_op_path(bucket));
        append_bootstrap_bucket_items(&mut plan, bucket, replicate_ilm_expiry)?;
        plan.bucket_configure_ops
            .push(bootstrap_bucket_op_path(&bucket.bucket, "configure-replication"));
    }

    Ok(plan)
}

/// Build only the two bucket operations needed by the lightweight retry
/// drain. The full bootstrap plan scans every bucket and IAM record; doing
/// that on a 30-second recovery cadence would make lifecycle admission scale
/// with the whole site instead of the one queued bucket.
pub(crate) fn site_replication_bucket_retry_plan_for(
    bucket: &str,
    created_at: Option<OffsetDateTime>,
    lock_enabled: bool,
) -> SiteReplicationBootstrapPlan {
    let bucket = SRBucketInfo {
        bucket: bucket.to_string(),
        created_at,
        object_lock_config: lock_enabled.then(String::new),
        ..Default::default()
    };
    SiteReplicationBootstrapPlan {
        bucket_make_ops: vec![bootstrap_bucket_make_op_path(&bucket)],
        bucket_configure_ops: vec![bootstrap_bucket_op_path(
            &bucket.bucket,
            SITE_REPLICATION_BUCKET_OP_CONFIGURE_REPLICATION,
        )],
        ..Default::default()
    }
}

pub(crate) fn site_replication_bucket_retry_plan_from_info(
    bucket: &SRBucketInfo,
    replicate_ilm_expiry: bool,
) -> S3Result<SiteReplicationBootstrapPlan> {
    let mut plan = site_replication_bucket_retry_plan_for(&bucket.bucket, bucket.created_at, bucket.object_lock_config.is_some());
    append_bootstrap_bucket_items(&mut plan, bucket, replicate_ilm_expiry)?;
    // Omit only metadata the make/configure operations can reproduce exactly.
    // Non-default versioning fields and operator-authored replication rules
    // remain in the plan; their extra request cost intentionally defers the
    // event to the complete drain when the lightweight budget is too small.
    plan.bucket_items.retain(|item| !retry_bucket_metadata_is_redundant(item));
    Ok(plan)
}

fn retry_bucket_metadata_is_redundant(item: &SRBucketMeta) -> bool {
    match item.r#type.as_str() {
        "version-config" => item.versioning.as_deref().is_some_and(|raw| {
            deserialize::<VersioningConfiguration>(&decode_bucket_meta_wire_value(raw)).is_ok_and(|config| {
                config
                    == VersioningConfiguration {
                        status: Some(BucketVersioningStatus::from_static(BucketVersioningStatus::ENABLED)),
                        ..Default::default()
                    }
            })
        }),
        "replication-config" => item.replication_config.as_deref().is_some_and(|raw| {
            deserialize::<ReplicationConfiguration>(&decode_bucket_meta_wire_value(raw))
                .is_ok_and(|config| config.role.trim().is_empty() && config.rules.iter().all(is_derived_site_replication_rule))
        }),
        // `Some("")` is the in-memory sentinel used when the bucket is lock
        // enabled but has no object-lock configuration body. The make query
        // carries lockEnabled=true; sending an empty metadata body is neither
        // useful nor parseable.
        "object-lock-config" => item.object_lock_config.as_deref() == Some(""),
        _ => false,
    }
}

pub(crate) async fn site_replication_bucket_retry_plan(bucket: &str) -> S3Result<SiteReplicationBootstrapPlan> {
    let Some(store) = current_object_store_handle() else {
        return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
    };
    let bucket_info = match store.get_bucket_info(bucket, &BucketOptions::default()).await {
        Ok(bucket_info) => bucket_info,
        Err(err) if is_err_bucket_not_found(&err) => return Ok(SiteReplicationBootstrapPlan::default()),
        Err(err) => return Err(ApiError::from(err).into()),
    };
    let lock_enabled = bucket_info.object_locking;
    let metadata = metadata_sys::get(bucket).await.map_err(ApiError::from)?;
    let mut bucket_info = SRBucketInfo {
        bucket: bucket.to_string(),
        created_at: bucket_info.created,
        location: current_region().map(|region| region.to_string()).unwrap_or_default(),
        api_version: Some(SITE_REPL_API_VERSION.to_string()),
        ..Default::default()
    };
    populate_sr_bucket_info_from_metadata(&mut bucket_info, &metadata).await;
    if lock_enabled && bucket_info.object_lock_config.is_none() {
        bucket_info.object_lock_config = Some(String::new());
    }
    let state = load_site_replication_state().await?;
    site_replication_bucket_retry_plan_from_info(&bucket_info, site_replication_state_replicates_ilm_expiry(&state))
}

pub async fn site_replication_make_bucket_hook(bucket: &str, lock_enabled: bool) -> S3Result<()> {
    let _bucket_op_guard = SITE_REPLICATION_BUCKET_OP_LOCK.read().await;
    let runtime = {
        // The bucket-op lock is what orders this against add/remove. The
        // state is only read here (through the runtime snapshot), and the
        // bucket setup below writes bucket metadata, never the state object —
        // holding the state transaction across it would put local metadata
        // IO inside a distributed lock for nothing.
        let Some(runtime) = runtime_site_replication_targets().await? else {
            return Ok(());
        };

        ensure_site_replication_bucket_versioning(bucket).await?;
        ensure_site_replication_bucket_setup_with_runtime(bucket, &runtime).await?;
        runtime
    };

    broadcast_site_replication_make_bucket(bucket, lock_enabled, Some(&runtime), None).await
}

pub(crate) async fn broadcast_site_replication_json_using_runtime<T: Serialize>(
    runtime: Option<&SiteReplicationRuntime>,
    path: &str,
    body: &T,
) -> S3Result<()> {
    match runtime {
        Some(runtime) => broadcast_site_replication_json_with_runtime(runtime, path, body).await,
        None => broadcast_site_replication_json(path, body).await,
    }
}

pub(crate) async fn broadcast_site_replication_make_bucket(
    bucket: &str,
    lock_enabled: bool,
    runtime: Option<&SiteReplicationRuntime>,
    bootstrap_token: Option<&str>,
) -> S3Result<()> {
    let created_at = current_object_store_handle()
        .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()))?
        .get_bucket_info(bucket, &BucketOptions::default())
        .await
        .map_err(ApiError::from)?
        .created
        .unwrap_or_else(OffsetDateTime::now_utc)
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default();

    let path = make_with_versioning_bucket_op_path(bucket, Some(&created_at), lock_enabled);
    let path = if let Some(token) = bootstrap_token {
        with_site_replication_bootstrap_token(&path, token)
    } else {
        path
    };
    broadcast_site_replication_json_using_runtime(runtime, &path, &serde_json::json!({})).await?;

    let configure_path = bootstrap_bucket_op_path(bucket, "configure-replication");
    let configure_path = if let Some(token) = bootstrap_token {
        with_site_replication_bootstrap_token(&configure_path, token)
    } else {
        configure_path
    };
    broadcast_site_replication_json_using_runtime(runtime, &configure_path, &serde_json::json!({})).await
}

const SITE_REPLICATION_DELETE_INTENT_PENDING: &str =
    "bucket deletion reserved; local completion and peer delivery are not yet known";

#[derive(Clone)]
struct SiteReplicationDeleteBucketReservation {
    peer: PeerInfo,
    previous: Option<SiteReplicationRetryEvent>,
    observed: SiteReplicationRetryEvent,
}

pub(crate) struct SiteReplicationDeleteBucketIntent {
    path: String,
    reservations: Vec<SiteReplicationDeleteBucketReservation>,
    displaced: Vec<SiteReplicationRetryEvent>,
}

fn site_replication_delete_bucket_path(bucket: &str, force_delete: bool) -> String {
    let operation = if force_delete {
        "force-delete-bucket"
    } else {
        "delete-bucket"
    };
    format!(
        "/rustfs/admin/v3/site-replication/peer/bucket-ops?{}",
        form_urlencoded::Serializer::new(String::new())
            .append_pair("bucket", bucket)
            .append_pair("operation", operation)
            .finish()
    )
}

/// Reserve every destructive peer delivery before the local namespace is
/// changed. The state transaction either persists the complete set or writes
/// nothing, so a full/unreadable queue fails the S3 delete closed.
pub(crate) async fn prepare_site_replication_delete_bucket(
    bucket: &str,
    force_delete: bool,
) -> S3Result<Option<SiteReplicationDeleteBucketIntent>> {
    let path = site_replication_delete_bucket_path(bucket, force_delete);
    let reservation_path = path.clone();
    update_site_replication_state_when_changed(move |state| {
        if !state.enabled() {
            return Ok(StateCommit::Unchanged(None));
        }
        let local_peer = current_local_runtime_peer(state);
        let peers = state
            .peers
            .values()
            .filter(|peer| {
                peer.deployment_id != local_peer.deployment_id && !same_identity_endpoint(&peer.endpoint, &local_peer.endpoint)
            })
            .cloned()
            .collect::<Vec<_>>();
        if peers.is_empty() {
            return Ok(StateCommit::Unchanged(None));
        }

        let mut reservations = Vec::with_capacity(peers.len());
        let mut displaced = Vec::new();
        for peer in peers {
            let previous = state
                .retry_queue
                .iter()
                .find(|event| retry_event_matches(event, &peer, &reservation_path))
                .cloned();
            displaced.extend(upsert_site_replication_retry_event(
                &mut state.retry_queue,
                &peer,
                &reservation_path,
                SITE_REPLICATION_DELETE_INTENT_PENDING,
                None,
            )?);
            let observed = state
                .retry_queue
                .iter()
                .find(|event| retry_event_matches(event, &peer, &reservation_path))
                .cloned()
                .ok_or_else(|| {
                    S3Error::with_message(
                        S3ErrorCode::InternalError,
                        "site replication delete reservation disappeared before commit".to_string(),
                    )
                })?;
            reservations.push(SiteReplicationDeleteBucketReservation {
                peer,
                previous,
                observed,
            });
        }
        Ok(StateCommit::Changed(Some(SiteReplicationDeleteBucketIntent {
            path: reservation_path,
            reservations,
            displaced,
        })))
    })
    .await
}

/// Roll back a reservation when the local storage delete definitively failed.
/// A concurrently revised reservation is preserved; it belongs to a newer
/// observation and this operation has no authority to settle it.
pub(crate) async fn cancel_site_replication_delete_bucket(intent: SiteReplicationDeleteBucketIntent) {
    let path = intent.path.clone();
    let result = update_site_replication_state_when_changed(move |state| {
        let mut changed = false;
        for reservation in intent.reservations {
            let Some(index) = state.retry_queue.iter().position(|event| {
                retry_event_matches(event, &reservation.peer, &reservation.observed.path)
                    && event.id == reservation.observed.id
                    && event.updated_at == reservation.observed.updated_at
            }) else {
                continue;
            };
            if let Some(previous) = reservation.previous {
                state.retry_queue[index] = previous;
            } else {
                state.retry_queue.remove(index);
            }
            changed = true;
        }

        let mut restored_all = true;
        for displaced in intent.displaced {
            let duplicate = state.retry_queue.iter().any(|event| {
                event.id == displaced.id
                    || (event.peer_deployment_id == displaced.peer_deployment_id && event.path == displaced.path)
            });
            if duplicate {
                continue;
            }
            if state.retry_queue.len() >= SITE_REPLICATION_RETRY_QUEUE_LIMIT {
                restored_all = false;
                continue;
            }
            state.retry_queue.push(displaced);
            changed = true;
        }
        Ok(if changed {
            StateCommit::Changed(restored_all)
        } else {
            StateCommit::Unchanged(restored_all)
        })
    })
    .await;

    match result {
        Ok(true) => {}
        Ok(false) => warn!(
            event = EVENT_ADMIN_SITE_REPLICATION_STATE,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
            path,
            result = "delete_intent_cancel_incomplete",
            "admin site replication state"
        ),
        Err(err) => warn!(
            event = EVENT_ADMIN_SITE_REPLICATION_STATE,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
            path,
            result = "delete_intent_cancel_failed",
            error = ?err,
            "admin site replication state"
        ),
    }
}

async fn broadcast_site_replication_delete_bucket(intent: &SiteReplicationDeleteBucketIntent) -> S3Result<()> {
    let sends = intent.reservations.iter().cloned().map(|reservation| {
        let request_path = intent.path.clone();
        async move {
            let fallback_peer = reservation.peer.clone();
            let observed = reservation.observed.clone();
            let delivery_path = request_path.clone();
            let delivery = with_site_replication_state_read_lock(move |state| async move {
                let Some(current_peer) = state.peers.get(&fallback_peer.deployment_id).cloned() else {
                    return Ok(None);
                };
                let service_account_secret_key =
                    match site_replicator_service_account_secret(&state.service_account_access_key).await {
                        Ok(secret) => secret,
                        Err(err) => {
                            let Some(secret) = legacy_site_replicator_state_secret(&state) else {
                                return Err(err);
                            };
                            warn!(
                                event = EVENT_ADMIN_SITE_REPLICATION_STATE,
                                component = LOG_COMPONENT_ADMIN,
                                subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
                                result = "legacy_state_service_account_secret_fallback",
                                error = ?err,
                                "admin site replication state"
                            );
                            secret
                        }
                    };
                let result = async {
                    let transport = PeerTransport::for_runtime_peer(&current_peer).await?;
                    PeerAdminRequest::put(&transport.connection, &delivery_path, &state.service_account_access_key)
                        .with_client(&transport.client)
                        .send(&service_account_secret_key, &serde_json::json!({}))
                        .await
                }
                .await;
                Ok(Some((current_peer, result)))
            })
            .await;
            match delivery {
                Ok(Some((current_peer, Ok(_)))) => {
                    dequeue_observed_site_replication_retry_event(&current_peer, &observed).await;
                    None
                }
                Ok(Some((current_peer, Err(err)))) => {
                    // Keep the failed deletion operator-visible, but never
                    // replay it automatically: without a bucket-incarnation
                    // fence, a delayed delete could erase a recreated bucket.
                    enqueue_site_replication_retry_event(&current_peer, &request_path, &err).await;
                    Some(err)
                }
                Ok(None) => {
                    dequeue_observed_site_replication_retry_event(&reservation.peer, &observed).await;
                    None
                }
                Err(err) => {
                    enqueue_site_replication_retry_event(&reservation.peer, &request_path, &err).await;
                    Some(err)
                }
            }
        }
    });
    futures::future::join_all(sends)
        .await
        .into_iter()
        .flatten()
        .next()
        .map_or(Ok(()), Err)
}

pub(crate) async fn commit_site_replication_delete_bucket(intent: &SiteReplicationDeleteBucketIntent) -> S3Result<()> {
    let _bucket_op_guard = SITE_REPLICATION_BUCKET_OP_LOCK.read().await;
    let store =
        current_object_store_handle().ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()))?;
    let retry_peers = intent
        .reservations
        .iter()
        .map(|reservation| reservation.peer.clone())
        .collect::<Vec<_>>();
    let retry_path = intent.path.clone();
    let delivery_intent = SiteReplicationDeleteBucketIntent {
        path: intent.path.clone(),
        reservations: intent.reservations.clone(),
        displaced: Vec::new(),
    };
    match with_config_object_write_lock(store, SITE_REPLICATION_REPAIR_EXECUTION_LOCK_PATH.to_string(), move || async move {
        broadcast_site_replication_delete_bucket(&delivery_intent).await
    })
    .await
    {
        Ok(result) => result,
        Err(err) => {
            let err: S3Error = ApiError::from(err).into();
            for peer in &retry_peers {
                enqueue_site_replication_retry_event(peer, &retry_path, &err).await;
            }
            Err(err)
        }
    }
}

pub async fn site_replication_bucket_meta_hook(mut item: SRBucketMeta) -> S3Result<()> {
    let Some(runtime) = runtime_site_replication_targets().await? else {
        return Ok(());
    };
    if item.r#type == "lc-config" && !site_replication_state_replicates_ilm_expiry(&runtime.state) {
        return Ok(());
    }
    if item.r#type == "lc-config" {
        // Only the expiry subset travels (MinIO peers install incoming rules
        // verbatim, so transition rules must never leave this site). An empty
        // subset becomes a delete, which the receiver merges with the empty
        // set — local transition rules there survive.
        item.expiry_lc_config = item
            .expiry_lc_config
            .and_then(|raw| lifecycle_expiry_subset_xml(raw.as_bytes()))
            .map(|data| String::from_utf8_lossy(&data).into_owned());
    }
    broadcast_site_replication_json_with_runtime(
        &runtime,
        "/rustfs/admin/v3/site-replication/peer/bucket-meta",
        &encode_bucket_meta_wire_item(item),
    )
    .await
}

/// Broadcast one IAM change to every peer. Unlike the generic JSON broadcast
/// this attempts ALL peers instead of failing fast, and books every failure —
/// transport construction included — through the deletion-aware recorder: a
/// deletion body that failed to reach a peer must be persisted for the retry
/// drain, or the peer keeps the deleted entity forever (backlog#2071).
pub async fn site_replication_iam_change_hook(item: SRIAMItem) -> S3Result<()> {
    let Some(runtime) = runtime_site_replication_targets().await? else {
        return Ok(());
    };
    let mut first_error: Option<S3Error> = None;
    for peer in runtime.state.peers.values() {
        if peer.deployment_id == runtime.local_peer.deployment_id
            || same_identity_endpoint(&peer.endpoint, &runtime.local_peer.endpoint)
        {
            continue;
        }
        let sent = async {
            let transport = PeerTransport::for_runtime_peer(peer).await?;
            PeerAdminRequest::put(
                &transport.connection,
                SITE_REPLICATION_PEER_IAM_ITEM_WIRE_PATH,
                &runtime.state.service_account_access_key,
            )
            .with_client(&transport.client)
            .send(&runtime.service_account_secret_key, &item)
            .await
        }
        .await;
        match sent {
            Ok(_) => dequeue_site_replication_retry_event(peer, SITE_REPLICATION_PEER_IAM_ITEM_WIRE_PATH).await,
            Err(err) => {
                record_failed_site_replication_iam_delivery(peer, &item, &err).await;
                first_error.get_or_insert(err);
            }
        }
    }
    first_error.map_or(Ok(()), Err)
}

pub(crate) fn raw_config_to_string(raw: &[u8]) -> Option<String> {
    if raw.is_empty() {
        return None;
    }
    String::from_utf8(raw.to_vec()).ok()
}

pub(crate) fn raw_config_to_base64(raw: &[u8]) -> Option<String> {
    (!raw.is_empty()).then(|| BASE64_STANDARD.encode_to_string(raw))
}

pub(crate) fn encode_bucket_meta_wire_value(value: Option<String>) -> Option<String> {
    value.map(|raw| BASE64_STANDARD.encode_to_string(raw.as_bytes()))
}

pub(crate) fn encode_bucket_meta_wire_item(mut item: SRBucketMeta) -> SRBucketMeta {
    item.versioning = encode_bucket_meta_wire_value(item.versioning);
    item.tags = encode_bucket_meta_wire_value(item.tags);
    item.object_lock_config = encode_bucket_meta_wire_value(item.object_lock_config);
    item.sse_config = encode_bucket_meta_wire_value(item.sse_config);
    item.replication_config = encode_bucket_meta_wire_value(item.replication_config);
    item.expiry_lc_config = encode_bucket_meta_wire_value(item.expiry_lc_config);
    item.cors = encode_bucket_meta_wire_value(item.cors);
    item
}

pub(crate) fn decode_bucket_meta_wire_value(raw: &str) -> Vec<u8> {
    BASE64_STANDARD
        .decode_to_vec(raw.as_bytes())
        .ok()
        .filter(|decoded| std::str::from_utf8(decoded).is_ok())
        .unwrap_or_else(|| raw.as_bytes().to_vec())
}

pub(crate) fn decode_bucket_meta_wire_option(value: Option<String>) -> Option<Vec<u8>> {
    value.map(|raw| decode_bucket_meta_wire_value(&raw))
}

pub(crate) fn maybe_time(value: OffsetDateTime) -> Option<OffsetDateTime> {
    (value != OffsetDateTime::UNIX_EPOCH).then_some(value)
}

async fn populate_sr_bucket_info_from_metadata(entry: &mut SRBucketInfo, metadata: &BucketMetadata) {
    entry.policy = raw_config_to_string(&metadata.policy_config_json).and_then(|raw| serde_json::from_str(&raw).ok());
    entry.versioning = raw_config_to_base64(&metadata.versioning_config_xml);
    entry.tags = raw_config_to_base64(&metadata.tagging_config_xml);
    entry.object_lock_config = raw_config_to_base64(&metadata.object_lock_config_xml);
    entry.sse_config = raw_config_to_base64(&metadata.encryption_config_xml);
    entry.replication_config = raw_config_to_base64(&metadata.replication_config_xml);
    entry.quota_config = raw_config_to_base64(&metadata.quota_config_json);
    // Expiry subset only: this entry feeds both the bootstrap/repair plan
    // (peers must not receive transition rules) and cross-site consistency
    // views (transition rules are site-local and would read as false
    // mismatches). A deleted expiry state is a `None` value with the
    // deletion's axis so repair can converge peers that missed the live
    // delete.
    let expiry_statement = lifecycle_expiry_statement(metadata);
    entry.expiry_lc_config = expiry_statement.as_ref().and_then(|(subset, _)| subset.clone());
    entry.cors_config = raw_config_to_base64(&metadata.cors_config_xml);
    entry.policy_updated_at = maybe_time(metadata.policy_config_updated_at);
    entry.tag_config_updated_at = maybe_time(metadata.tagging_config_updated_at);
    entry.object_lock_config_updated_at = maybe_time(metadata.object_lock_config_updated_at);
    entry.sse_config_updated_at = maybe_time(metadata.encryption_config_updated_at);
    entry.versioning_config_updated_at = maybe_time(metadata.versioning_config_updated_at);
    entry.replication_config_updated_at = maybe_time(metadata.replication_config_updated_at);
    entry.quota_config_updated_at = maybe_time(metadata.quota_config_updated_at);
    // The expiry axis, not the whole-config write time: local transition-only
    // edits inflate the latter, and a repair item stamped with it could
    // out-rank a newer real expiry edit on a third site.
    entry.expiry_lc_config_updated_at = expiry_statement.map(|(_, axis)| axis);
    entry.cors_config_updated_at = maybe_time(metadata.cors_config_updated_at);
    entry.replication_targets_online =
        Some(site_replication_targets_online(&entry.bucket, &metadata.replication_config_xml).await);
}

pub(crate) async fn build_sr_info(state: &SiteReplicationState, local_peer: &PeerInfo) -> S3Result<SRInfo> {
    let Some(store) = current_object_store_handle() else {
        return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
    };

    let mut info = SRInfo {
        enabled: state.enabled(),
        name: local_peer.name.clone(),
        deployment_id: local_peer.deployment_id.clone(),
        state: SRStateInfo {
            name: local_peer.name.clone(),
            peers: state.peers.clone(),
            updated_at: state.updated_at,
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
        },
        api_version: Some(SITE_REPL_API_VERSION.to_string()),
        ..Default::default()
    };

    let buckets = store.list_bucket(&BucketOptions::default()).await.map_err(ApiError::from)?;
    for bucket in buckets {
        let metadata = metadata_sys::get(&bucket.name).await.ok();
        let mut entry = SRBucketInfo {
            bucket: bucket.name.clone(),
            created_at: bucket.created,
            location: current_region().map(|region| region.to_string()).unwrap_or_default(),
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
            ..Default::default()
        };

        if let Some(metadata) = metadata {
            populate_sr_bucket_info_from_metadata(&mut entry, &metadata).await;
        }

        info.buckets.insert(bucket.name, entry);
    }

    if let Some(iam_sys) = current_iam_handle() {
        for (name, policy_doc) in iam_sys.list_policy_docs("").await.map_err(ApiError::from)? {
            info.policies.insert(
                name,
                SRIAMPolicy {
                    policy: serde_json::to_value(policy_doc.policy).ok(),
                    updated_at: policy_doc.update_date,
                    api_version: Some(SITE_REPL_API_VERSION.to_string()),
                },
            );
        }

        let users = iam_sys.list_users().await.map_err(ApiError::from)?;
        for (name, user) in users {
            info.user_info_map.insert(name, user);
        }

        let groups = iam_sys.list_groups_load().await.map_err(ApiError::from)?;
        for group in groups {
            let desc = iam_sys.get_group_description(&group).await.map_err(ApiError::from)?;
            info.group_desc_map.insert(group.clone(), desc);
        }

        let mut user_policies = HashMap::<String, MappedPolicy>::new();
        iam_sys
            .load_mapped_policies(UserType::Reg, false, &mut user_policies)
            .await
            .map_err(ApiError::from)?;
        for (name, mapping) in user_policies {
            info.user_policies
                .insert(name.clone(), mapped_policy_to_sr_mapping(name, false, UserType::Reg, mapping));
        }

        let mut group_policies = HashMap::<String, MappedPolicy>::new();
        iam_sys
            .load_mapped_policies(UserType::None, true, &mut group_policies)
            .await
            .map_err(ApiError::from)?;
        for (name, mapping) in group_policies {
            info.group_policies
                .insert(name.clone(), mapped_policy_to_sr_mapping(name, true, UserType::None, mapping));
        }
    }

    for (name, bucket_info) in &info.buckets {
        if let Some(raw) = bucket_info
            .replication_config
            .as_ref()
            .and_then(|value| serde_json::from_str::<Value>(value).ok())
        {
            info.replication_cfg.insert(name.clone(), raw);
        }
    }

    Ok(info)
}

pub(crate) fn mapped_policy_to_sr_mapping(
    name: String,
    is_group: bool,
    user_type: UserType,
    mapping: MappedPolicy,
) -> SRPolicyMapping {
    SRPolicyMapping {
        user_or_group: name,
        user_type: sr_wire_user_type(user_type, is_group),
        is_group,
        policy: mapping.policies,
        updated_at: Some(mapping.update_at),
        api_version: Some(SITE_REPL_API_VERSION.to_string()),
        ..Default::default()
    }
}

pub(crate) fn bucket_target_endpoint(target: &BucketTarget) -> String {
    let scheme = if target.secure { "https" } else { "http" };
    canonical_endpoint(&format!("{scheme}://{}", target.endpoint))
}

pub(crate) fn bucket_target_matches_peer(target: &BucketTarget, peer: &PeerInfo) -> bool {
    if !target.deployment_id.is_empty() {
        return target.deployment_id == peer.deployment_id;
    }
    bucket_target_endpoint(target) == canonical_endpoint(&peer.endpoint)
}

pub(crate) fn site_replication_target_arns_by_peer(config: Option<&ReplicationConfiguration>) -> HashMap<String, String> {
    let mut arns_by_peer = HashMap::new();
    let Some(config) = config else {
        return arns_by_peer;
    };

    let mut configured_arns = Vec::new();
    if !config.role.trim().is_empty() {
        configured_arns.push(config.role.clone());
    }
    for rule in &config.rules {
        let arn = rule.destination.bucket.trim();
        if !arn.is_empty() {
            configured_arns.push(arn.to_string());
        }
    }

    for arn in configured_arns {
        if let Some(deployment_id) = replication_target_arn_deployment_id(&arn) {
            arns_by_peer.entry(deployment_id).or_insert(arn);
        }
    }

    arns_by_peer
}

pub(crate) fn site_replication_bucket_target_for_peer(
    bucket: &str,
    state: &SiteReplicationState,
    peer: &PeerInfo,
    service_account_secret_key: &str,
    arn_override: Option<String>,
) -> S3Result<Option<BucketTarget>> {
    if state.service_account_access_key.is_empty() || service_account_secret_key.is_empty() {
        return Ok(None);
    }

    let parsed = Url::parse(&peer.endpoint)
        .ok()
        .or_else(|| Url::parse(&format!("http://{}", peer.endpoint.trim())).ok())
        .ok_or_else(|| S3Error::with_message(S3ErrorCode::InvalidRequest, format!("invalid peer endpoint: {}", peer.endpoint)))?;
    let host = parsed.host_str().ok_or_else(|| {
        S3Error::with_message(S3ErrorCode::InvalidRequest, format!("peer endpoint missing host: {}", peer.endpoint))
    })?;
    let port = parsed.port_or_known_default().ok_or_else(|| {
        S3Error::with_message(S3ErrorCode::InvalidRequest, format!("peer endpoint missing port: {}", peer.endpoint))
    })?;
    let region = current_region()
        .map(|region| region.to_string())
        .filter(|region| !region.is_empty())
        .unwrap_or_else(|| "us-east-1".to_string());
    let arn = arn_override.unwrap_or_else(|| {
        ARN::new(
            BucketTargetType::ReplicationService,
            peer.deployment_id.clone(),
            String::new(),
            bucket.to_string(),
        )
        .to_string()
    });

    Ok(Some(BucketTarget {
        source_bucket: bucket.to_string(),
        endpoint: format!("{host}:{port}"),
        credentials: Some(Credentials {
            access_key: state.service_account_access_key.clone(),
            secret_key: service_account_secret_key.to_string(),
            session_token: None,
            expiration: None,
        }),
        target_bucket: bucket.to_string(),
        secure: parsed.scheme().eq_ignore_ascii_case("https"),
        arn,
        region,
        target_type: BucketTargetType::ReplicationService,
        deployment_id: peer.deployment_id.clone(),
        skip_tls_verify: peer.skip_tls_verify,
        ca_cert_pem: peer.ca_cert_pem.clone(),
        ..Default::default()
    }))
}

pub(crate) fn reconcile_site_replication_bucket_targets(
    existing: BucketTargets,
    bucket: &str,
    state: &SiteReplicationState,
    local_peer: &PeerInfo,
    config: Option<&ReplicationConfiguration>,
    service_account_secret_key: &str,
) -> S3Result<BucketTargets> {
    if !state.enabled() || state.service_account_access_key.is_empty() || service_account_secret_key.is_empty() {
        return Ok(existing);
    }

    let configured_arns = site_replication_target_arns_by_peer(config);
    let mut targets = existing.targets;

    for peer in state.peers.values() {
        if peer.deployment_id == local_peer.deployment_id || same_identity_endpoint(&peer.endpoint, &local_peer.endpoint) {
            continue;
        }

        let Some(mut target) = site_replication_bucket_target_for_peer(
            bucket,
            state,
            peer,
            service_account_secret_key,
            configured_arns.get(&peer.deployment_id).cloned(),
        )?
        else {
            continue;
        };

        if let Some(index) = targets.iter().position(|existing| {
            existing.target_type == BucketTargetType::ReplicationService
                && (bucket_target_matches_peer(existing, peer) || existing.arn == target.arn)
        }) {
            let existing = targets[index].clone();
            target.path = existing.path;
            target.region = existing.region;
            target.bandwidth_limit = existing.bandwidth_limit;
            target.replication_sync = existing.replication_sync;
            target.storage_class = existing.storage_class;
            target.health_check_duration = existing.health_check_duration;
            target.disable_proxy = existing.disable_proxy;
            target.reset_before_date = existing.reset_before_date;
            target.reset_id = existing.reset_id;
            target.total_downtime = existing.total_downtime;
            target.last_online = existing.last_online;
            target.online = existing.online;
            target.latency = existing.latency;
            target.edge = existing.edge;
            target.edge_sync_before_expiry = existing.edge_sync_before_expiry;
            target.offline_count = existing.offline_count;
            targets[index] = target;
        } else {
            targets.push(target);
        }
    }

    Ok(BucketTargets { targets })
}

/// Whether every `site-repl-*` rule on this bucket resolves to a live remote target.
///
/// The rule set alone cannot answer this: a rule can be perfectly formed while the endpoint
/// recorded for its peer is one this site cannot reach, so `update_all_targets` never built
/// a client for it and `replicate_object` drops every object against that ARN. Reads the
/// already-resolved client map rather than rebuilding clients, so it stays cheap enough for
/// the status path.
pub(crate) async fn site_replication_targets_online(bucket: &str, replication_config_xml: &[u8]) -> bool {
    let Ok(config) = deserialize::<ReplicationConfiguration>(replication_config_xml) else {
        return true;
    };

    for rule in config.rules.iter().filter(|rule| is_derived_site_replication_rule(rule)) {
        if BucketTargetSys::get()
            .get_remote_target_client_by_arn(bucket, &rule.destination.bucket)
            .await
            .is_none()
        {
            return false;
        }
    }

    true
}

/// True when the rule carries the expiry semantics that `replicateILMExpiry`
/// propagates. Del-marker expiration and abort-multipart are deliberately
/// excluded: MinIO's sender never emits them (`CloneNonTransition` drops
/// both), so treating them as traveling state would let a MinIO peer's
/// broadcast delete this site's del-marker-only rules.
pub(crate) fn lifecycle_rule_has_expiry(rule: &LifecycleRule) -> bool {
    rule.expiration.is_some() || rule.noncurrent_version_expiration.is_some()
}

/// Remove the fields that never travel between sites (MinIO
/// `CloneNonTransition` parity).
pub(crate) fn strip_site_local_lifecycle_fields(rule: &mut LifecycleRule) {
    rule.transitions = None;
    rule.noncurrent_version_transitions = None;
    rule.abort_incomplete_multipart_upload = None;
    rule.del_marker_expiration = None;
}

/// Reduce a lifecycle XML document to the expiry subset that is allowed to
/// travel between sites (what MinIO's sender emits): transition fields are
/// stripped and rules left with no expiry semantics are dropped. Returns
/// `None` when nothing remains — the receiver then merges with the empty set,
/// which is exactly the "no expiry rules here" statement. A document that
/// fails to parse is forwarded unfiltered (`Some(original)`): the receiver
/// merge strips it anyway, and turning a local parse error into a `None`
/// would delete the peers' replicated expiry rules.
pub(crate) fn lifecycle_expiry_subset_xml(raw: &[u8]) -> Option<Vec<u8>> {
    if raw.is_empty() {
        return None;
    }
    let config: BucketLifecycleConfiguration = match deserialize(raw) {
        Ok(config) => config,
        Err(err) => {
            warn!("failed to parse local lifecycle config for expiry replication; forwarding unfiltered: {err}");
            return Some(raw.to_vec());
        }
    };
    let expiry_updated_at = config.expiry_updated_at.clone();
    let rules: Vec<LifecycleRule> = config
        .rules
        .into_iter()
        .filter_map(|mut rule| {
            strip_site_local_lifecycle_fields(&mut rule);
            lifecycle_rule_has_expiry(&rule).then_some(rule)
        })
        .collect();
    if rules.is_empty() {
        return None;
    }
    let subset = BucketLifecycleConfiguration {
        rules,
        expiry_updated_at,
    };
    match serialize(&subset) {
        Ok(data) => Some(data),
        Err(err) => {
            warn!("failed to serialize lifecycle expiry subset; forwarding unfiltered: {err}");
            Some(raw.to_vec())
        }
    }
}

/// The expiry replication axis persisted in a lifecycle XML document, if any.
/// Used for the SRInfo bucket entry so bootstrap/repair items carry the
/// expiry axis instead of the whole-config write time (which local
/// transition-only edits inflate).
pub(crate) fn lifecycle_expiry_updated_at(raw: &[u8]) -> Option<OffsetDateTime> {
    if raw.is_empty() {
        return None;
    }
    deserialize::<BucketLifecycleConfiguration>(raw)
        .ok()
        .and_then(|config| config.expiry_updated_at)
        .map(OffsetDateTime::from)
}

/// The ILM expiry statement this site contributes to its SRInfo bucket entry
/// (feeding bootstrap/repair and consistency views), if any.
/// `Some((subset_b64, axis))` — a `None` subset means "expiry rules were
/// removed at `axis`" and travels as an explicit timestamped delete item, so
/// a peer that missed the live delete still converges on repair.
pub(crate) fn lifecycle_expiry_statement(
    metadata: &crate::storage_api::site_replication::BucketMetadata,
) -> Option<(Option<String>, OffsetDateTime)> {
    if metadata.lifecycle_config_xml.is_empty() {
        // Deleted vs never configured: the whole-config write time survives
        // deletion in bucket metadata and strictly exceeds the created-time
        // backfill only after a real write.
        return (metadata.lifecycle_config_updated_at > metadata.created).then_some((None, metadata.lifecycle_config_updated_at));
    }
    let axis = lifecycle_expiry_updated_at(&metadata.lifecycle_config_xml);
    match lifecycle_expiry_subset_xml(&metadata.lifecycle_config_xml) {
        Some(subset) => {
            // Legacy documents predate the axis field; their whole-config
            // write time bounds the last expiry edit.
            let axis = axis.unwrap_or(metadata.lifecycle_config_updated_at);
            Some((raw_config_to_base64(&subset), axis))
        }
        // Transition-only config: with an expiry axis the site once had
        // expiry rules and properly removed them — the delete travels at
        // that axis. Without one there is nothing to say (a delete stamped
        // off the whole-config time would let a local transition edit erase
        // newer peer expiry state).
        None => axis.map(|axis| (None, axis)),
    }
}

/// Whether `rule` is in the shape the reconciler derives (`site-repl-<id>`
/// naming the deployment its ARN targets). The reconciler rebuilds every such
/// rule from the current peer set — current peer or not, so a leftover from a
/// removed peer or a self-pointing rule is rebuilt away — while the merges
/// keep only the current peers' rules and treat a leftover as operator state
/// the edit replaces. An operator-authored `site-repl-*` id on an operator
/// ARN is outside the shape and survives every pass.
pub(crate) fn is_derived_site_replication_rule(rule: &ReplicationRule) -> bool {
    site_replication_rule_deployment_id(rule).is_some()
}

pub(crate) fn build_site_replication_rule(arn: &str, priority: i32, rule_id: &str) -> ReplicationRule {
    ReplicationRule {
        delete_marker_replication: Some(DeleteMarkerReplication {
            status: Some(DeleteMarkerReplicationStatus::from_static(DeleteMarkerReplicationStatus::ENABLED)),
        }),
        delete_replication: Some(DeleteReplication {
            status: DeleteReplicationStatus::from_static(DeleteReplicationStatus::ENABLED),
        }),
        destination: Destination {
            bucket: arn.to_string(),
            ..Default::default()
        },
        existing_object_replication: Some(ExistingObjectReplication {
            status: ExistingObjectReplicationStatus::from_static(ExistingObjectReplicationStatus::ENABLED),
        }),
        filter: None,
        id: Some(rule_id.to_string()),
        prefix: None,
        priority: Some(priority),
        source_selection_criteria: Some(SourceSelectionCriteria {
            replica_modifications: Some(ReplicaModifications {
                status: ReplicaModificationsStatus::from_static(ReplicaModificationsStatus::ENABLED),
            }),
            sse_kms_encrypted_objects: None,
        }),
        status: ReplicationRuleStatus::from_static(ReplicationRuleStatus::ENABLED),
    }
}

pub(crate) fn build_site_replication_config(
    bucket: &str,
    state: &SiteReplicationState,
    local_peer: &PeerInfo,
    service_account_secret_key: &str,
    existing: Option<&ReplicationConfiguration>,
) -> S3Result<Option<ReplicationConfiguration>> {
    // Reuse the ARN already recorded for a peer so the rule keeps pointing at the same
    // bucket target `reconcile_site_replication_bucket_targets` keys off (a MinIO-era
    // `arn:minio:...` target would otherwise be orphaned by a freshly minted ARN).
    let configured_arns = site_replication_target_arns_by_peer(existing);
    let mut rules = Vec::new();
    for peer in state.peers.values() {
        if peer.deployment_id == local_peer.deployment_id || same_identity_endpoint(&peer.endpoint, &local_peer.endpoint) {
            continue;
        }

        let Some(target) = site_replication_bucket_target_for_peer(
            bucket,
            state,
            peer,
            service_account_secret_key,
            configured_arns.get(&peer.deployment_id).cloned(),
        )?
        else {
            continue;
        };
        rules.push(build_site_replication_rule(
            &target.arn,
            (rules.len() + 1) as i32,
            &format!("site-repl-{}", peer.deployment_id),
        ));
    }

    if rules.is_empty() {
        Ok(None)
    } else {
        Ok(Some(ReplicationConfiguration {
            role: String::new(),
            rules,
        }))
    }
}

pub(crate) async fn ensure_site_replication_bucket_targets_with_runtime(
    bucket: &str,
    state: &SiteReplicationState,
    local_peer: &PeerInfo,
    config: Option<&ReplicationConfiguration>,
    service_account_secret_key: &str,
    expected_incarnation_id: Uuid,
) -> S3Result<()> {
    let existing = match metadata_sys::list_bucket_targets(bucket).await {
        Ok(targets) => targets,
        Err(StorageError::ConfigNotFound) => BucketTargets::default(),
        Err(err) => return Err(ApiError::from(err).into()),
    };
    let existing_json = serde_json::to_vec(&existing)
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("serialize bucket targets failed: {e}")))?;

    let updated =
        reconcile_site_replication_bucket_targets(existing, bucket, state, local_peer, config, service_account_secret_key)?;
    if updated.targets.is_empty() {
        return Ok(());
    }

    let json_targets = serde_json::to_vec(&updated)
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("serialize bucket targets failed: {e}")))?;
    // Rewriting identical targets would churn bucket metadata and rebuild every remote S3
    // client — noticeable now that startup reconciles all buckets, not just the one bucket
    // an operation touched.
    if json_targets == existing_json {
        return Ok(());
    }
    metadata_sys::update_if_incarnation(bucket, BUCKET_TARGETS_FILE, json_targets, expected_incarnation_id)
        .await
        .map_err(ApiError::from)?;
    Ok(())
}

pub(crate) async fn bucket_replication_config_for_target_refresh(bucket: &str) -> S3Result<Option<ReplicationConfiguration>> {
    match metadata_sys::get_replication_config(bucket).await {
        Ok((config, _)) => Ok(Some(config)),
        Err(StorageError::ConfigNotFound) => Ok(None),
        Err(err) => Err(ApiError::from(err).into()),
    }
}

pub(crate) async fn ensure_site_replication_bucket_replication_config_with_runtime(
    bucket: &str,
    state: &SiteReplicationState,
    local_peer: &PeerInfo,
    service_account_secret_key: &str,
    expected_incarnation_id: Uuid,
) -> S3Result<()> {
    let existing = match metadata_sys::get_replication_config(bucket).await {
        Ok((existing, _)) => Some(existing),
        Err(StorageError::ConfigNotFound) => None,
        Err(err) => return Err(ApiError::from(err).into()),
    };

    let Some(desired) = build_site_replication_config(bucket, state, local_peer, service_account_secret_key, existing.as_ref())?
    else {
        return Ok(());
    };

    // Derived rules are state owned by this site: rebuild them from the current peer
    // set on every pass instead of preserving whatever is on disk. A rule left over
    // from a removed peer — or one whose destination ARN names this very deployment,
    // which no bucket target can ever satisfy — must not survive, otherwise objects
    // are queued against an ARN that resolves to nothing.
    let (existing_role, existing_rules) = existing
        .map(|config| (config.role, config.rules))
        .unwrap_or_else(|| (String::new(), Vec::new()));
    let mut rules: Vec<ReplicationRule> = existing_rules
        .iter()
        .filter(|rule| !is_derived_site_replication_rule(rule))
        .cloned()
        .collect();
    rules.extend(desired.rules);
    // Operator priorities are the operator's policy; only the derived rules
    // take free slots, by the same function as the config merges so a merged
    // write and this pass agree byte for byte.
    assign_site_replication_rule_priorities(&mut rules, is_derived_site_replication_rule);

    // Only a `role` naming a current peer is ours to drop — an operator-authored role is
    // part of the bucket's S3-visible configuration, and repairing a reverse rule must not
    // quietly rewrite it. Same rule as `merge_incoming_replication_config`.
    let role = if is_site_replication_role(&existing_role, &remote_peer_deployment_ids(state, local_peer)) {
        String::new()
    } else {
        existing_role.clone()
    };

    if rules == existing_rules && role == existing_role {
        return Ok(());
    }

    let data = serialize(&ReplicationConfiguration { role, rules })
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("serialize replication failed: {e}")))?;
    metadata_sys::update_if_incarnation(bucket, BUCKET_REPLICATION_CONFIG, data, expected_incarnation_id)
        .await
        .map_err(ApiError::from)?;

    Ok(())
}

pub(crate) async fn ensure_site_replication_bucket_setup_with_runtime(
    bucket: &str,
    runtime: &SiteReplicationRuntime,
) -> S3Result<()> {
    let expected_incarnation_id = metadata_sys::capture_bucket_metadata_incarnation(bucket)
        .await
        .map_err(ApiError::from)?;
    ensure_site_replication_bucket_setup_with_runtime_for_incarnation(bucket, runtime, expected_incarnation_id).await
}

pub(crate) async fn ensure_site_replication_bucket_setup_with_runtime_for_incarnation(
    bucket: &str,
    runtime: &SiteReplicationRuntime,
    expected_incarnation_id: Uuid,
) -> S3Result<()> {
    let _targets_guard = lock_bucket_targets_metadata(bucket).await;
    let config = bucket_replication_config_for_target_refresh(bucket).await?;
    ensure_site_replication_bucket_targets_with_runtime(
        bucket,
        &runtime.state,
        &runtime.local_peer,
        config.as_ref(),
        &runtime.service_account_secret_key,
        expected_incarnation_id,
    )
    .await?;
    ensure_site_replication_bucket_replication_config_with_runtime(
        bucket,
        &runtime.state,
        &runtime.local_peer,
        &runtime.service_account_secret_key,
        expected_incarnation_id,
    )
    .await?;
    Ok(())
}

pub(crate) fn bucket_versioning_xml() -> S3Result<Vec<u8>> {
    let config = VersioningConfiguration {
        status: Some(BucketVersioningStatus::from_static(BucketVersioningStatus::ENABLED)),
        ..Default::default()
    };
    serialize(&config).map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("serialize versioning failed: {e}")))
}

pub(crate) async fn ensure_site_replication_bucket_versioning(bucket: &str) -> S3Result<()> {
    let expected_incarnation_id = metadata_sys::capture_bucket_metadata_incarnation(bucket)
        .await
        .map_err(ApiError::from)?;
    match metadata_sys::get_versioning_config(bucket).await {
        Ok((config, _)) if config.enabled() => return Ok(()),
        Ok(_) | Err(StorageError::ConfigNotFound) => {}
        Err(err) => return Err(ApiError::from(err).into()),
    }

    metadata_sys::update_if_incarnation(bucket, BUCKET_VERSIONING_CONFIG, bucket_versioning_xml()?, expected_incarnation_id)
        .await
        .map_err(ApiError::from)?;

    Ok(())
}
