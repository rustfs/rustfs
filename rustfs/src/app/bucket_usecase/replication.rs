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

//! Bucket replication configuration use-case operations.

use super::*;
use rustfs_gateway_types::persistence::parse_replication_dto;
use rustfs_s3_types::{S3Error, S3ErrorCode, S3Result, s3_error};

fn validate_replication_config_targets(targets: &BucketTargets, config: &PersistedReplicationConfiguration) -> S3Result<()> {
    let configured_arns = targets
        .targets
        .iter()
        .filter(|target| target.target_type == BucketTargetType::ReplicationService)
        .map(|target| target.arn.as_str());

    match validate_replication_config_target_arns(configured_arns, config) {
        Ok(()) => Ok(()),
        Err(err) => {
            let message = match err {
                ReplicationTargetValidationError::RoleWithMultipleDestinations => {
                    "replication config with Role cannot define multiple destination targets"
                }
                ReplicationTargetValidationError::StaleTarget => "replication config has a stale target",
            };
            Err(S3Error::with_message(S3ErrorCode::InvalidRequest, message))
        }
    }
}

fn validate_replication_config_capabilities(config: &PersistedReplicationConfiguration) -> S3Result<()> {
    if let Err(err) = validate_replication_config_structure(config) {
        return Err(S3Error::with_message(S3ErrorCode::InvalidRequest, err.message()));
    }
    if let Some(field) = invalid_replication_config_status_field(config) {
        return Err(S3Error::with_message(
            S3ErrorCode::InvalidRequest,
            format!("replication field {field} has an invalid status"),
        ));
    }
    if let Some(field) = unsupported_replication_config_field(config) {
        return Err(S3Error::with_message(
            S3ErrorCode::InvalidRequest,
            format!("replication field {field} is not supported by this RustFS version"),
        ));
    }
    Ok(())
}

async fn validate_bucket_replication_update(bucket: &str, config: &PersistedReplicationConfiguration) -> S3Result<()> {
    if !BucketVersioningSys::enabled(bucket).await {
        return Err(s3_error!(
            InvalidRequest,
            "bucket versioning must be enabled before replication can be configured"
        ));
    }

    let targets = metadata_sys::get_bucket_targets_config(bucket)
        .await
        .map_err(|err| match err {
            StorageError::ConfigNotFound => {
                S3Error::with_message(S3ErrorCode::InvalidRequest, "replication target configuration not found".to_string())
            }
            other => ApiError::from(other).into(),
        })?;

    validate_replication_config_targets(&targets, config)
}

/// Defense in depth for site-replication-managed buckets (issue #1948): an S3
/// PutBucketReplication replaces the operator-authored rules but must not wipe
/// the rules the reconciler derived for the current remote peers
/// (`site_peer_deployment_ids`) — until its next pass (600s period) every
/// peer link on this bucket would be silently dead. The same merge also drops
/// incoming impostors of those rules. An empty peer set (site replication
/// disabled) keeps the verbatim overwrite semantics: rule ids are not
/// reserved, so an operator's own `site-repl-*` rule is ordinary state there.
/// `contract` is what the peers were probed to support; the merged config is
/// what gets broadcast, so it is built the way every peer will merge it.
fn merge_user_replication_config_update(
    incoming: PersistedReplicationConfiguration,
    existing: Option<PersistedReplicationConfiguration>,
    site_peer_deployment_ids: &HashSet<String>,
    contract: OperatorRuleContract,
) -> PersistedReplicationConfiguration {
    if site_peer_deployment_ids.is_empty() {
        return incoming;
    }
    // `incoming` passed structure validation, so it holds at least one rule;
    // `None` is only reachable when every incoming rule impersonates a
    // reconciler rule, and then the stored reconciler rules are what remains.
    merge_user_replication_config(Some(incoming.clone()), existing, site_peer_deployment_ids, contract).unwrap_or(incoming)
}

/// Split of an S3 DeleteBucketReplication on the stored config (issue #1948):
/// the operator-authored rules are removed, the rules the reconciler derived
/// for the current remote peers survive (`None` means nothing survives and
/// the config is deleted), and the returned ARNs are the ones whose bucket
/// targets may be garbage-collected — never an ARN a surviving reconciler
/// rule still points at.
fn split_replication_config_for_user_delete(
    config: PersistedReplicationConfiguration,
    site_peer_deployment_ids: &HashSet<String>,
    contract: OperatorRuleContract,
) -> (Option<PersistedReplicationConfiguration>, HashSet<String>) {
    let mut removable_arns = replication_target_arns(&config);
    let remaining = merge_user_replication_config(None, Some(config), site_peer_deployment_ids, contract);
    if let Some(remaining) = remaining.as_ref() {
        for rule in &remaining.rules {
            removable_arns.remove(rule.destination.bucket.trim());
        }
    }
    (remaining, removable_arns)
}

async fn replication_targets_without_arns(
    bucket: &str,
    target_arns: &HashSet<String>,
) -> S3Result<Option<(BucketTargets, usize)>> {
    if target_arns.is_empty() {
        return Ok(None);
    }

    let mut targets = match metadata_sys::get_bucket_targets_config(bucket).await {
        Ok(targets) => targets,
        Err(StorageError::ConfigNotFound) => return Ok(None),
        Err(err) => return Err(ApiError::from(err).into()),
    };

    let removed = remove_replication_targets_from_config_targets(&mut targets, target_arns);
    if removed == 0 {
        return Ok(None);
    }

    Ok(Some((targets, removed)))
}

fn remove_replication_targets_from_config_targets(targets: &mut BucketTargets, target_arns: &HashSet<String>) -> usize {
    let original_len = targets.targets.len();
    targets.targets.retain(|target| {
        !should_remove_replication_target(
            target.arn.as_str(),
            target.target_type == BucketTargetType::ReplicationService,
            target_arns,
        )
    });

    original_len - targets.targets.len()
}

async fn write_replication_targets_after_config_delete(
    bucket: &str,
    targets: &BucketTargets,
    removed: usize,
    expected_incarnation_id: Option<uuid::Uuid>,
) -> S3Result<()> {
    #[cfg(test)]
    if let Some(fail) = tests::TARGET_CLEANUP_FAILURE
        .try_with(std::cell::RefCell::take)
        .ok()
        .flatten()
    {
        fail.await?;
    }

    let json_targets = serde_json::to_vec(&targets).map_err(to_internal_error)?;
    update_bucket_config_for_incarnation(bucket, BUCKET_TARGETS_FILE, json_targets, expected_incarnation_id)
        .await
        .map_err(ApiError::from)?;
    info!(bucket = %bucket, removed, "removed replication remote targets referenced by deleted bucket replication config");

    Ok(())
}

async fn restore_replication_config_after_target_cleanup_failure(
    bucket: &str,
    config: &PersistedReplicationConfiguration,
    cleanup_err: S3Error,
    expected_incarnation_id: Option<uuid::Uuid>,
) -> S3Error {
    match serialize(config) {
        Ok(data) => {
            if let Err(restore_err) =
                update_bucket_config_for_incarnation(bucket, BUCKET_REPLICATION_CONFIG, data, expected_incarnation_id).await
            {
                error!(
                    bucket = %bucket,
                    error = ?restore_err,
                    cleanup_error = ?cleanup_err,
                    "failed to restore bucket replication config after target cleanup failure"
                );
            }
        }
        Err(restore_err) => {
            error!(
                bucket = %bucket,
                error = ?restore_err,
                cleanup_error = ?cleanup_err,
                "failed to serialize bucket replication config for restore after target cleanup failure"
            );
        }
    }

    cleanup_err
}

impl DefaultBucketUsecase {
    /// Removes operator rules and targets unique to those rules.
    ///
    /// Preparation runs inside the audit scope and resolves the peer contract
    /// before any bucket lookup or mutation.
    pub(crate) async fn execute_delete_bucket_replication(
        &self,
        envelope: RequestEnvelope,
        bucket: &str,
        prepare: impl Future<Output = S3Result<(HashSet<String>, OperatorRuleContract)>> + Send,
    ) -> S3Result<()> {
        // Preserve the success status recorded by the legacy audit for an empty response.
        RequestEnvelope::audit_only_operation(
            envelope,
            S3Operation::DeleteBucketReplication,
            StatusCode::OK,
            |envelope| async move {
                let (site_peers, contract) = prepare.await?;
                self.delete_bucket_replication(envelope, bucket, site_peers, contract).await
            },
        )
        .await
    }

    async fn delete_bucket_replication(
        &self,
        envelope: RequestEnvelope,
        bucket: &str,
        site_peers: HashSet<String>,
        contract: OperatorRuleContract,
    ) -> S3Result<()> {
        let expected_incarnation_id = bucket_config_mutation_incarnation(&envelope, bucket)?;
        let request_context = envelope.request_context().cloned();

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init"));
        };

        store
            .get_bucket_info(bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;
        let targets_guard = lock_bucket_targets_metadata(bucket).await;
        let replication_config = match metadata_sys::get_replication_config(bucket).await {
            Ok((config, _)) => Some(config),
            Err(StorageError::ConfigNotFound) => None,
            Err(err) => return Err(ApiError::from(err).into()),
        };
        let (remaining_config, updated_targets) = if let Some(config) = replication_config.as_ref() {
            let (remaining, removable_arns) = split_replication_config_for_user_delete(config.clone(), &site_peers, contract);
            let targets = replication_targets_without_arns(bucket, &removable_arns).await?;
            (remaining, targets)
        } else {
            (None, None)
        };

        match remaining_config {
            // Site-replication rules and the targets backing them survive the
            // S3 delete (issue #1948); only the operator-authored rules go.
            Some(remaining) => {
                let data = serialize_config(&remaining)?;
                update_bucket_config_for_incarnation(bucket, BUCKET_REPLICATION_CONFIG, data, expected_incarnation_id)
                    .await
                    .map_err(ApiError::from)?;
            }
            None => {
                delete_bucket_config_for_incarnation(bucket, BUCKET_REPLICATION_CONFIG, expected_incarnation_id)
                    .await
                    .map_err(ApiError::from)?;
            }
        }
        if let Some((targets, removed)) = updated_targets
            && let Err(err) =
                write_replication_targets_after_config_delete(bucket, &targets, removed, expected_incarnation_id).await
        {
            if let Some(config) = replication_config.as_ref() {
                return Err(restore_replication_config_after_target_cleanup_failure(
                    bucket,
                    config,
                    err,
                    expected_incarnation_id,
                )
                .await);
            }
            return Err(err);
        }
        drop(targets_guard);

        notify_bucket_metadata_reload(bucket.to_owned(), "delete bucket replication", request_context, true).await;

        let item = sr_bucket_meta_item(bucket.to_owned(), "replication-config");
        if let Err(err) = site_replication_bucket_meta_hook(item).await {
            warn!(bucket = %bucket, error = ?err, "site replication bucket replication-config delete hook failed");
        }

        info!(bucket = %bucket, "deleted bucket replication config");

        Ok(())
    }

    /// The stored replication configuration as the gateway's answer holds it.
    ///
    /// # Errors
    ///
    /// `InternalError` when the store is not initialized or the stored document cannot be
    /// represented; the lookup's storage error, `NoSuchBucket` among them;
    /// `ReplicationConfigurationNotFoundError` when none is stored.
    pub async fn execute_get_bucket_replication(
        &self,
        input: gateway_dto::GetBucketReplicationInput,
    ) -> rustfs_s3_types::S3Result<gateway_dto::GetBucketReplicationOutput> {
        let bucket = input.bucket.as_str();
        let Some(store) = self.object_store() else {
            return Err(rustfs_s3_types::s3_error!(InternalError, "Not init"));
        };

        store
            .get_bucket_info(bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let replication_configuration = match metadata_sys::get_replication_config(bucket).await {
            Ok((cfg, _created)) => cfg,
            Err(StorageError::ConfigNotFound) => {
                return Err(rustfs_s3_types::s3_error!(ReplicationConfigurationNotFoundError, "replication not found"));
            }
            Err(err) => {
                warn!(
                    component = LOG_COMPONENT_APP,
                    subsystem = LOG_SUBSYSTEM_BUCKET,
                    event = "bucket_replication_config_load_failed",
                    bucket = %bucket,
                    error = ?err,
                    "Failed to load bucket replication configuration"
                );
                return Err(ApiError::from(err).into());
            }
        };

        Ok(gateway_dto::GetBucketReplicationOutput {
            replication_configuration: Some(dto_values::stored_config(&replication_configuration, parse_replication_dto)?),
        })
    }

    /// Replaces operator rules after the interface resolves the peer contract.
    pub(crate) async fn execute_put_bucket_replication(
        &self,
        envelope: RequestEnvelope,
        bucket: &str,
        prepare: impl Future<Output = S3Result<(PersistedReplicationConfiguration, HashSet<String>, OperatorRuleContract)>> + Send,
    ) -> S3Result<()> {
        RequestEnvelope::audit_only_operation(
            envelope,
            S3Operation::PutBucketReplication,
            StatusCode::OK,
            |envelope| async move {
                let (replication_configuration, site_peers, contract) = prepare.await?;
                self.put_bucket_replication(envelope, bucket, replication_configuration, site_peers, contract)
                    .await
            },
        )
        .await
    }

    async fn put_bucket_replication(
        &self,
        envelope: RequestEnvelope,
        bucket: &str,
        replication_configuration: PersistedReplicationConfiguration,
        site_peers: HashSet<String>,
        contract: OperatorRuleContract,
    ) -> S3Result<()> {
        let expected_incarnation_id = bucket_config_mutation_incarnation(&envelope, bucket)?;
        let request_context = envelope.request_context().cloned();
        info!(bucket = %bucket, "updating bucket replication config");

        validate_replication_config_capabilities(&replication_configuration)?;

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init"));
        };

        store
            .get_bucket_info(bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let targets_guard = lock_bucket_targets_metadata(bucket).await;
        validate_bucket_replication_update(bucket, &replication_configuration).await?;
        let existing_config = match metadata_sys::get_replication_config(bucket).await {
            Ok((config, _)) => Some(config),
            Err(StorageError::ConfigNotFound) => None,
            Err(err) => return Err(ApiError::from(err).into()),
        };
        let replication_configuration =
            merge_user_replication_config_update(replication_configuration, existing_config, &site_peers, contract);
        let data = serialize_config(&replication_configuration)?;
        update_bucket_config_for_incarnation(bucket, BUCKET_REPLICATION_CONFIG, data, expected_incarnation_id)
            .await
            .map_err(ApiError::from)?;
        drop(targets_guard);

        notify_bucket_metadata_reload(bucket.to_owned(), "put bucket replication", request_context, true).await;

        let mut item = sr_bucket_meta_item(bucket.to_owned(), "replication-config");
        item.replication_config = Some(
            serialize_config(&replication_configuration).and_then(|bytes| String::from_utf8(bytes).map_err(to_internal_error))?,
        );
        if let Err(err) = site_replication_bucket_meta_hook(item).await {
            warn!(bucket = %bucket, error = ?err, "site replication bucket replication-config hook failed");
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::bucket_usecase::test_support::{build_request, owned_envelope};
    use crate::app::gating_test_env::{run_large_stack_test, shared_gating_ambient};
    use crate::app::runtime_sources::ServerContextSlot;
    use crate::app::storage_api::owned_parity::{ReqInfo, authorize_bucket_config_mutation};
    use crate::site_replication::hooks::{BUCKET_META_HOOK_CALLS, BucketMetaHookCall};
    use futures::{FutureExt, future::BoxFuture};
    use http::Method;
    use rustfs_credentials::Credentials;
    use rustfs_gateway_types::dto::Status;
    use rustfs_gateway_types::persistence::{
        PersistedEncryptionConfiguration, PersistedReplicationDestination, PersistedReplicationRule,
    };
    use rustfs_policy::policy::action::{Action, S3Action};
    use serial_test::serial;
    use std::cell::RefCell;
    use std::sync::Mutex;

    fn replication_rule_for_target(arn: &str) -> PersistedReplicationRule {
        PersistedReplicationRule {
            delete_marker_replication: None,
            delete_replication: None,
            destination: PersistedReplicationDestination {
                bucket: arn.to_string(),
                ..Default::default()
            },
            existing_object_replication: None,
            filter: None,
            id: Some("rule-1".to_string()),
            prefix: None,
            priority: Some(1),
            source_selection_criteria: None,
            status: Status::ENABLED.to_string(),
        }
    }

    #[test]
    fn replication_target_arns_use_role_when_present() {
        let role = "arn:rustfs:replication:us-east-1:source:bucket";
        let destination = "arn:rustfs:replication:us-east-1:target:bucket";
        let config = PersistedReplicationConfiguration {
            role: format!(" {role} "),
            rules: vec![replication_rule_for_target(destination)],
        };

        let arns = replication_target_arns(&config);

        assert!(arns.contains(role));
        assert!(!arns.contains(destination));
    }

    #[test]
    fn replication_target_arns_use_rule_destinations_without_role() {
        let destination = "arn:rustfs:replication:us-east-1:target:bucket";
        let config = PersistedReplicationConfiguration {
            role: String::new(),
            rules: vec![replication_rule_for_target(destination)],
        };

        let arns = replication_target_arns(&config);

        assert!(arns.contains(destination));
    }

    fn replication_rule_with_id(arn: &str, id: &str, priority: i32) -> PersistedReplicationRule {
        let mut rule = replication_rule_for_target(arn);
        rule.id = Some(id.to_string());
        rule.priority = Some(priority);
        rule
    }

    fn site_peers(deployment_ids: &[&str]) -> HashSet<String> {
        deployment_ids.iter().map(|id| id.to_string()).collect()
    }

    #[test]
    fn put_replication_merge_preserves_site_replication_rules() {
        let existing = PersistedReplicationConfiguration {
            role: String::new(),
            rules: vec![
                replication_rule_with_id("arn:rustfs:replication::peer-dep:bucket", "site-repl-peer-dep", 1),
                replication_rule_with_id("arn:rustfs:replication:us-east-1:old:bucket", "old-user-rule", 2),
            ],
        };
        let incoming = PersistedReplicationConfiguration {
            role: String::new(),
            rules: vec![
                replication_rule_with_id("arn:rustfs:replication:us-east-1:new:bucket", "new-user-rule", 1),
                replication_rule_with_id("arn:rustfs:replication::forged-dep:bucket", "site-repl-peer-dep", 2),
                replication_rule_with_id("arn:rustfs:replication::other-dep:bucket", "site-repl-other", 3),
            ],
        };

        let merged = merge_user_replication_config_update(
            incoming,
            Some(existing),
            &site_peers(&["peer-dep"]),
            OperatorRuleContract::Derived,
        );

        let rules: Vec<_> = merged
            .rules
            .iter()
            .map(|rule| (rule.id.as_deref().unwrap_or_default(), rule.destination.bucket.as_str()))
            .collect();
        assert_eq!(
            rules,
            vec![
                ("new-user-rule", "arn:rustfs:replication:us-east-1:new:bucket"),
                ("site-repl-other", "arn:rustfs:replication::other-dep:bucket"),
                ("site-repl-peer-dep", "arn:rustfs:replication::peer-dep:bucket"),
            ],
            "user rules replaced, the reconciler rule for the current peer kept over the incoming impostor, \
             a site-repl-* id that names no current peer is ordinary operator state"
        );
    }

    // Rule ids do not reserve `site-repl-*`: outside site replication an
    // owner's `site-repl-user` rule is ordinary state, so PUT stores it
    // verbatim and DELETE removes it and garbage-collects its target.
    #[test]
    fn put_then_delete_replication_without_site_replication_treats_site_repl_id_as_user_rule() {
        let user_arn = "arn:minio:replication:us-east-1:2f1c-remote:bucket";
        let incoming = PersistedReplicationConfiguration {
            role: String::new(),
            rules: vec![replication_rule_with_id(user_arn, "site-repl-user", 1)],
        };

        let stored = merge_user_replication_config_update(incoming.clone(), None, &HashSet::new(), OperatorRuleContract::Derived);
        assert_eq!(stored, incoming, "PUT on a non-site-replication bucket is verbatim");

        let (remaining, removable) =
            split_replication_config_for_user_delete(stored, &HashSet::new(), OperatorRuleContract::Derived);
        assert!(remaining.is_none(), "DELETE must remove the operator's site-repl-* rule");
        assert_eq!(removable, HashSet::from([user_arn.to_string()]));
    }

    // Under site replication only a rule the reconciler would derive — id
    // `site-repl-<peer>` for a current peer, destination ARN naming the same
    // peer — is reconciler-owned. Everything else is operator state.
    #[test]
    fn delete_replication_split_keeps_only_reconciler_derived_rules() {
        let peer_arn = "arn:rustfs:replication::peer-dep:bucket";
        let user_arn = "arn:minio:replication:us-east-1:2f1c-remote:bucket";
        let config = PersistedReplicationConfiguration {
            role: String::new(),
            rules: vec![
                replication_rule_with_id(user_arn, "site-repl-user", 1),
                replication_rule_with_id(user_arn, "site-repl-peer-dep", 2),
                replication_rule_with_id("arn:rustfs:replication::gone-dep:bucket", "site-repl-gone-dep", 3),
                replication_rule_with_id(peer_arn, "site-repl-peer-dep", 4),
            ],
        };

        let (remaining, removable) =
            split_replication_config_for_user_delete(config, &site_peers(&["peer-dep"]), OperatorRuleContract::Derived);

        let remaining = remaining.expect("the reconciler-derived rule must survive");
        assert_eq!(remaining.rules.len(), 1);
        assert_eq!(remaining.rules[0].destination.bucket, peer_arn);
        assert_eq!(
            removable,
            HashSet::from([user_arn.to_string(), "arn:rustfs:replication::gone-dep:bucket".to_string()]),
            "targets of operator rules and of a removed peer are garbage-collected"
        );
    }

    #[test]
    fn put_replication_merge_returns_incoming_verbatim_without_site_rules() {
        let existing = PersistedReplicationConfiguration {
            role: String::new(),
            rules: vec![replication_rule_with_id(
                "arn:rustfs:replication:us-east-1:old:bucket",
                "old-user-rule",
                7,
            )],
        };
        let incoming = PersistedReplicationConfiguration {
            role: String::new(),
            rules: vec![replication_rule_with_id(
                "arn:rustfs:replication:us-east-1:new:bucket",
                "new-user-rule",
                5,
            )],
        };

        let merged = merge_user_replication_config_update(
            incoming.clone(),
            Some(existing),
            &HashSet::new(),
            OperatorRuleContract::Derived,
        );

        assert_eq!(merged.role, incoming.role);
        assert_eq!(merged.rules, incoming.rules, "non-SR buckets keep the verbatim overwrite semantics");
    }

    #[test]
    fn delete_replication_split_keeps_site_rules_and_their_targets() {
        let sr_arn = "arn:rustfs:replication::peer-dep:bucket";
        let user_arn = "arn:rustfs:replication:us-east-1:user:bucket";
        let config = PersistedReplicationConfiguration {
            role: String::new(),
            rules: vec![
                replication_rule_with_id(user_arn, "user-rule", 1),
                replication_rule_with_id(sr_arn, "site-repl-peer-dep", 2),
            ],
        };

        let (remaining, removable) =
            split_replication_config_for_user_delete(config, &site_peers(&["peer-dep"]), OperatorRuleContract::Derived);

        let remaining = remaining.expect("site-replication rules must survive a user delete");
        let ids: Vec<_> = remaining
            .rules
            .iter()
            .map(|rule| rule.id.as_deref().unwrap_or_default())
            .collect();
        assert_eq!(ids, vec!["site-repl-peer-dep"]);
        assert_eq!(removable, HashSet::from([user_arn.to_string()]));
    }

    #[test]
    fn delete_replication_split_protects_targets_shared_with_site_rules() {
        let sr_arn = "arn:rustfs:replication::peer-dep:bucket";
        let config = PersistedReplicationConfiguration {
            role: String::new(),
            rules: vec![
                replication_rule_with_id(sr_arn, "user-rule-on-sr-target", 1),
                replication_rule_with_id(sr_arn, "site-repl-peer-dep", 2),
            ],
        };

        let (remaining, removable) =
            split_replication_config_for_user_delete(config, &site_peers(&["peer-dep"]), OperatorRuleContract::Derived);

        assert!(remaining.is_some());
        assert!(
            removable.is_empty(),
            "a target still referenced by a surviving site-replication rule must not be removed"
        );
    }

    #[test]
    fn delete_replication_split_removes_everything_without_site_rules() {
        let user_arn = "arn:rustfs:replication:us-east-1:user:bucket";
        let config = PersistedReplicationConfiguration {
            role: String::new(),
            rules: vec![replication_rule_with_id(user_arn, "user-rule", 1)],
        };

        let (remaining, removable) =
            split_replication_config_for_user_delete(config, &site_peers(&["peer-dep"]), OperatorRuleContract::Derived);

        assert!(remaining.is_none(), "without site-replication rules the whole config is deleted");
        assert_eq!(removable, HashSet::from([user_arn.to_string()]));
    }

    fn replication_targets_with_arn(arns: &[&str]) -> BucketTargets {
        BucketTargets {
            targets: arns
                .iter()
                .map(|arn| BucketTarget {
                    arn: (*arn).to_string(),
                    target_type: BucketTargetType::ReplicationService,
                    ..Default::default()
                })
                .collect(),
        }
    }

    #[test]
    fn validate_replication_config_targets_accepts_matching_destination_arns() {
        let arn = "arn:rustfs:replication:us-east-1:target:bucket";
        let targets = replication_targets_with_arn(&[arn]);
        let config = PersistedReplicationConfiguration {
            role: String::new(),
            rules: vec![replication_rule_for_target(arn)],
        };

        validate_replication_config_targets(&targets, &config).expect("matching target should pass validation");
    }

    #[test]
    fn validate_replication_config_targets_rejects_stale_destination_arns() {
        let targets = replication_targets_with_arn(&["arn:rustfs:replication:us-east-1:target:bucket-a"]);
        let config = PersistedReplicationConfiguration {
            role: String::new(),
            rules: vec![replication_rule_for_target(
                "arn:rustfs:replication:us-east-1:target:bucket-b",
            )],
        };

        let err = validate_replication_config_targets(&targets, &config).expect_err("stale target should fail validation");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn validate_replication_config_targets_accepts_matching_role_arn() {
        let arn = "arn:rustfs:replication:us-east-1:role-target:bucket";
        let targets = replication_targets_with_arn(&[arn]);
        let config = PersistedReplicationConfiguration {
            role: format!(" {arn} "),
            rules: vec![replication_rule_for_target("arn:rustfs:replication:us-east-1:ignored:bucket")],
        };

        validate_replication_config_targets(&targets, &config).expect("matching role ARN should pass validation");
    }

    #[test]
    fn validate_replication_config_targets_rejects_role_with_multiple_destinations() {
        let role = "arn:rustfs:replication:us-east-1:role-target:bucket";
        let targets = replication_targets_with_arn(&[role]);
        let config = PersistedReplicationConfiguration {
            role: role.to_string(),
            rules: vec![
                replication_rule_for_target("arn:rustfs:replication:us-east-1:target-a:bucket"),
                replication_rule_for_target("arn:rustfs:replication:us-east-1:target-b:bucket"),
            ],
        };

        let err = validate_replication_config_targets(&targets, &config)
            .expect_err("role plus multiple destinations should be rejected");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn validate_replication_config_targets_trims_destination_arns() {
        let arn = "arn:rustfs:replication:us-east-1:target:bucket";
        let targets = replication_targets_with_arn(&[arn]);
        let config = PersistedReplicationConfiguration {
            role: String::new(),
            rules: vec![replication_rule_for_target(
                " arn:rustfs:replication:us-east-1:target:bucket ",
            )],
        };

        validate_replication_config_targets(&targets, &config).expect("trimmed destination ARN should match configured target");
    }

    #[test]
    fn validate_replication_config_targets_ignores_disabled_rules() {
        let targets = replication_targets_with_arn(&[]);
        let mut rule = replication_rule_for_target("arn:rustfs:replication:us-east-1:stale:bucket");
        rule.status = Status::DISABLED.to_string();
        let config = PersistedReplicationConfiguration {
            role: String::new(),
            rules: vec![rule],
        };

        validate_replication_config_targets(&targets, &config).expect("disabled rules should not require live targets");
    }

    #[test]
    fn validate_replication_config_capabilities_names_unsupported_field() {
        let mut rule = replication_rule_for_target("arn:rustfs:replication:us-east-1:target:bucket");
        let destination_key_id = "arn:aws:kms:us-east-1:123456789012:key/opaque-key-id";
        rule.destination.encryption_configuration = Some(PersistedEncryptionConfiguration {
            replica_kms_key_id: Some(destination_key_id.to_string()),
        });
        let config = PersistedReplicationConfiguration {
            role: String::new(),
            rules: vec![rule],
        };

        let err = validate_replication_config_capabilities(&config)
            .expect_err("destination encryption must be rejected until the execution path supports it");

        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert!(
            err.to_string()
                .contains("Destination.EncryptionConfiguration is not supported")
        );
        assert!(!err.to_string().contains(destination_key_id));
    }

    #[test]
    fn validate_replication_config_capabilities_rejects_structural_defects_before_write() {
        let mut first = replication_rule_for_target("arn:rustfs:replication:us-east-1:target:bucket");
        first.priority = Some(1);
        let mut second = replication_rule_for_target("arn:rustfs:replication:us-east-1:target:bucket");
        second.priority = Some(1);
        let config = PersistedReplicationConfiguration {
            role: String::new(),
            rules: vec![first, second],
        };

        let err = validate_replication_config_capabilities(&config)
            .expect_err("duplicate rule priorities must be rejected before persistence");

        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert!(err.to_string().contains("Priority must be unique"));
    }

    #[test]
    fn validate_replication_config_capabilities_rejects_invalid_status_before_write() {
        let mut rule = replication_rule_for_target("arn:rustfs:replication:us-east-1:target:bucket");
        rule.status = "Invalid".to_owned();
        let config = PersistedReplicationConfiguration {
            role: String::new(),
            rules: vec![rule],
        };

        let err = validate_replication_config_capabilities(&config)
            .expect_err("an invalid string-backed replication status must be rejected before persistence");

        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert!(err.to_string().contains("Rule.Status has an invalid status"));
    }

    #[test]
    fn remove_replication_targets_from_config_targets_only_removes_referenced_replication_targets() {
        let removed_arn = "arn:rustfs:replication:us-east-1:removed:bucket";
        let kept_replication_arn = "arn:rustfs:replication:us-east-1:kept:bucket";
        let kept_ilm_arn = "arn:rustfs:ilm:us-east-1:kept:bucket";
        let mut targets = BucketTargets {
            targets: vec![
                BucketTarget {
                    arn: removed_arn.to_string(),
                    target_type: BucketTargetType::ReplicationService,
                    ..Default::default()
                },
                BucketTarget {
                    arn: kept_replication_arn.to_string(),
                    target_type: BucketTargetType::ReplicationService,
                    ..Default::default()
                },
                BucketTarget {
                    arn: kept_ilm_arn.to_string(),
                    target_type: BucketTargetType::IlmService,
                    ..Default::default()
                },
            ],
        };
        let target_arns = HashSet::from([removed_arn.to_string(), kept_ilm_arn.to_string()]);

        let removed = remove_replication_targets_from_config_targets(&mut targets, &target_arns);

        assert_eq!(removed, 1);
        let remaining_arns = targets
            .targets
            .iter()
            .map(|target| target.arn.as_str())
            .collect::<HashSet<_>>();
        assert!(!remaining_arns.contains(removed_arn));
        assert!(remaining_arns.contains(kept_replication_arn));
        assert!(remaining_arns.contains(kept_ilm_arn));
    }

    #[tokio::test]
    async fn execute_delete_bucket_replication_returns_internal_error_when_store_uninitialized() {
        let usecase = DefaultBucketUsecase::without_context();
        let err = usecase
            .execute_delete_bucket_replication(owned_envelope(Method::DELETE), "test-bucket", async {
                Ok((HashSet::new(), OperatorRuleContract::Derived))
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_get_bucket_replication_returns_internal_error_when_store_uninitialized() {
        let input = gateway_dto::GetBucketReplicationInput {
            bucket: rustfs_gateway_types::BucketName::new("test-bucket").expect("valid bucket name"),
            ..Default::default()
        };
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_get_bucket_replication(input).await.unwrap_err();
        assert_eq!(err.code(), &rustfs_s3_types::S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_put_bucket_replication_returns_internal_error_when_store_uninitialized() {
        // The config must clear the structural/capability validators so the
        // request actually reaches the store lookup this test pins.
        let configuration = PersistedReplicationConfiguration {
            role: "arn:aws:iam::123456789012:role/test".to_string(),
            rules: vec![replication_rule_for_target("arn:rustfs:replication:us-east-1:target:bucket")],
        };
        let usecase = DefaultBucketUsecase::without_context();
        let err = usecase
            .execute_put_bucket_replication(owned_envelope(Method::PUT), "test-bucket", async {
                Ok((configuration, HashSet::new(), OperatorRuleContract::Derived))
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_put_bucket_replication_rejects_unsupported_fields_before_store_or_metadata_write() {
        let mut rule = replication_rule_for_target("arn:rustfs:replication:us-east-1:target:bucket");
        rule.destination.account = Some("123456789012".to_string());
        let configuration = PersistedReplicationConfiguration {
            role: String::new(),
            rules: vec![rule],
        };
        let err = DefaultBucketUsecase::without_context()
            .execute_put_bucket_replication(owned_envelope(Method::PUT), "test-bucket", async {
                Ok((configuration, HashSet::new(), OperatorRuleContract::Derived))
            })
            .await
            .expect_err("unsupported fields must be rejected before store access");

        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert_eq!(
            err.message(),
            Some("replication field Destination.Account is not supported by this RustFS version")
        );
    }

    const PEER_ARN: &str = "arn:rustfs:replication::peer-dep:bucket";
    const USER_ARN: &str = "arn:rustfs:replication:us-east-1:user:bucket";
    const SITE_RULE: &str = "site-repl-peer-dep";

    tokio::task_local! {
        pub(super) static TARGET_CLEANUP_FAILURE: RefCell<Option<BoxFuture<'static, S3Result<()>>>>;
    }

    fn put_configuration() -> PersistedReplicationConfiguration {
        PersistedReplicationConfiguration {
            role: String::new(),
            rules: vec![
                replication_rule_with_id(USER_ARN, "new-operator", 3),
                replication_rule_with_id(PEER_ARN, "shared-peer-target", 4),
            ],
        }
    }

    async fn seed_replication(store: &ECStore, bucket: &str, operator: &str) -> (Vec<u8>, Vec<u8>) {
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("create bucket for replication");
        metadata_sys::update(
            bucket,
            BUCKET_VERSIONING_CONFIG,
            b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>".to_vec(),
        )
        .await
        .expect("enable versioning");
        let config = PersistedReplicationConfiguration {
            role: String::new(),
            rules: vec![
                replication_rule_with_id(USER_ARN, operator, 1),
                replication_rule_with_id(PEER_ARN, SITE_RULE, 2),
            ],
        };
        let targets = BucketTargets {
            targets: [PEER_ARN, USER_ARN]
                .into_iter()
                .map(|arn| BucketTarget {
                    source_bucket: bucket.to_owned(),
                    arn: arn.to_owned(),
                    target_type: BucketTargetType::ReplicationService,
                    ..Default::default()
                })
                .collect(),
        };
        let bytes = serialize(&config).expect("seed configuration bytes");
        let targets = serde_json::to_vec(&targets).expect("seed target bytes");
        metadata_sys::update(bucket, BUCKET_TARGETS_FILE, targets.clone())
            .await
            .expect("seed target metadata");
        metadata_sys::update(bucket, BUCKET_REPLICATION_CONFIG, bytes.clone())
            .await
            .expect("seed replication configuration");
        (bytes, targets)
    }

    async fn persisted_replication(bucket: &str) -> (Vec<u8>, Vec<u8>) {
        let stored = metadata_sys::get_global_bucket_metadata_sys()
            .expect("initialized metadata system")
            .read()
            .await
            .get_config_from_disk(bucket)
            .await
            .expect("disk metadata");
        (stored.replication_config_xml, stored.bucket_targets_config_json)
    }

    async fn authorize_replication(context: &Arc<AppContext>, bucket: &str, method: Method) -> RequestEnvelope {
        let server = ServerContextSlot::new();
        assert!(server.install(Arc::clone(context)));
        let mut request = build_request((), method);
        request.uri = format!("/{bucket}?replication").parse().expect("replication URI");
        request.extensions.insert(Arc::clone(&server));
        request.extensions.insert(ReqInfo {
            cred: Some(Credentials {
                access_key: "replication-owner".to_owned(),
                ..Default::default()
            }),
            is_owner: true,
            ..Default::default()
        });
        let mut envelope = RequestEnvelope::from_s3s(&request);
        authorize_bucket_config_mutation(
            &server,
            &mut envelope,
            bucket,
            Action::S3Action(S3Action::PutReplicationConfigurationAction),
        )
        .await
        .expect("snapshot authorized bucket");
        envelope
    }

    async fn edit_replication(
        usecase: &DefaultBucketUsecase,
        envelope: RequestEnvelope,
        bucket: &str,
        put: bool,
    ) -> S3Result<()> {
        let peers = HashSet::from(["peer-dep".to_owned()]);
        if put {
            usecase
                .execute_put_bucket_replication(envelope, bucket, async {
                    Ok((put_configuration(), peers, OperatorRuleContract::Derived))
                })
                .await
        } else {
            usecase
                .execute_delete_bucket_replication(envelope, bucket, async { Ok((peers, OperatorRuleContract::Derived)) })
                .await
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    struct Reload {
        completed: bool,
        maintenance: bool,
        target_lock_available: bool,
        hook_calls: usize,
    }

    async fn observe_replication_edit(
        bucket: &str,
        edit: impl Future<Output = S3Result<()>>,
    ) -> (S3Result<()>, Vec<Reload>, Vec<BucketMetaHookCall>) {
        let reloads = Arc::new(Mutex::new(Vec::new()));
        let record = Arc::clone(&reloads);
        let target_bucket = bucket.to_owned();
        let (result, hooks) = BUCKET_META_HOOK_CALLS
            .scope(
                RefCell::default(),
                BUCKET_METADATA_RELOAD_PROBE.scope(
                    (
                        bucket.to_owned(),
                        Box::new(move |completed, _, maintenance| {
                            record.lock().expect("reloads").push(Reload {
                                completed,
                                maintenance,
                                target_lock_available: lock_bucket_targets_metadata(&target_bucket).now_or_never().is_some(),
                                hook_calls: BUCKET_META_HOOK_CALLS.with(|calls| calls.borrow().len()),
                            });
                        }),
                    ),
                    async {
                        let result = edit.await;
                        (result, BUCKET_META_HOOK_CALLS.with(RefCell::take))
                    },
                ),
            )
            .await;
        let reloads = std::mem::take(&mut *reloads.lock().expect("reloads"));
        (result, reloads, hooks)
    }

    #[test]
    #[serial]
    fn owned_replication_preserves_peer_rules_and_finishes_reload_before_site_hook() {
        run_large_stack_test("owned-replication-peer-rules", || async {
            let context = shared_gating_ambient().await;
            let bucket = format!("replication-peer-{}", uuid::Uuid::new_v4().simple());
            let (original, _) = seed_replication(&context.object_store(), &bucket, "old-operator").await;
            let usecase = DefaultBucketUsecase::with_context(Some(Arc::clone(&context)));
            for (put, method) in [(true, Method::PUT), (false, Method::DELETE)] {
                let envelope = authorize_replication(&context, &bucket, method).await;
                let (result, reloads, hooks) =
                    observe_replication_edit(&bucket, edit_replication(&usecase, envelope, &bucket, put)).await;
                result.expect("replication edit");
                assert_eq!(
                    reloads,
                    [
                        Reload {
                            completed: false,
                            maintenance: true,
                            target_lock_available: true,
                            hook_calls: 0
                        },
                        Reload {
                            completed: true,
                            maintenance: true,
                            target_lock_available: true,
                            hook_calls: 0
                        },
                    ]
                );

                let (xml, target_bytes) = persisted_replication(&bucket).await;
                assert_ne!(xml, original);
                let config = rustfs_gateway_types::persistence::parse_replication(&xml).expect("stored XML");
                let ids: HashSet<_> = config.rules.iter().filter_map(|rule| rule.id.as_deref()).collect();
                let targets: BucketTargets = serde_json::from_slice(&target_bytes).expect("stored targets");
                let arns: HashSet<_> = targets.targets.iter().map(|target| target.arn.as_str()).collect();
                if put {
                    assert_eq!(ids, HashSet::from([SITE_RULE, "new-operator", "shared-peer-target"]));
                    assert_eq!(arns, HashSet::from([PEER_ARN, USER_ARN]));
                } else {
                    assert_eq!(ids, HashSet::from([SITE_RULE]));
                    assert_eq!(arns, HashSet::from([PEER_ARN]));
                }
                assert_eq!(hooks.len(), 1);
                assert_eq!(
                    (hooks[0].bucket.as_str(), hooks[0].item_type.as_str()),
                    (bucket.as_str(), "replication-config")
                );
                assert_eq!(hooks[0].replication_config.as_deref().map(str::as_bytes), put.then_some(xml.as_slice()));
            }
        });
    }

    #[test]
    #[serial]
    fn owned_replication_stale_edits_leave_replacement_bytes_and_skip_callbacks() {
        run_large_stack_test("owned-replication-stale", || async {
            let context = shared_gating_ambient().await;
            let store = context.object_store();
            let usecase = DefaultBucketUsecase::with_context(Some(Arc::clone(&context)));
            for (method, preserve_peer) in [(Method::PUT, true), (Method::DELETE, true), (Method::DELETE, false)] {
                let bucket = format!("replication-stale-{}", uuid::Uuid::new_v4().simple());
                seed_replication(&store, &bucket, "old-operator").await;
                let put = method == Method::PUT;
                let envelope = authorize_replication(&context, &bucket, method).await;
                store
                    .delete_bucket(&bucket, &DeleteBucketOptions::default())
                    .await
                    .expect("delete authorized bucket");
                let replacement = seed_replication(&store, &bucket, "replacement").await;
                let edit = async {
                    if preserve_peer {
                        edit_replication(&usecase, envelope, &bucket, put).await
                    } else {
                        usecase
                            .execute_delete_bucket_replication(envelope, &bucket, async {
                                Ok((HashSet::new(), OperatorRuleContract::Derived))
                            })
                            .await
                    }
                };
                let (result, reloads, hooks) = observe_replication_edit(&bucket, edit).await;
                assert_eq!(
                    (
                        result.err().map(|err| err.code().clone()),
                        persisted_replication(&bucket).await,
                        reloads.len(),
                        hooks.len()
                    ),
                    (Some(S3ErrorCode::NoSuchBucket), replacement, 0, 0),
                    "stale {}",
                    if put { "Put" } else { "Delete" }
                );
            }
        });
    }

    #[test]
    #[serial]
    fn owned_replication_cleanup_error_restores_original_and_never_overwrites_a_replacement() {
        run_large_stack_test("owned-replication-cleanup", || async {
            let context = shared_gating_ambient().await;
            let store = context.object_store();
            let usecase = DefaultBucketUsecase::with_context(Some(Arc::clone(&context)));
            for (replace, fail_target_write) in [(false, true), (true, true), (true, false)] {
                let bucket = format!("replication-cleanup-{}", uuid::Uuid::new_v4().simple());
                let original = seed_replication(&store, &bucket, "old-operator").await;
                let envelope = authorize_replication(&context, &bucket, Method::DELETE).await;
                let expected_storage = Arc::new(Mutex::new(original));
                let output = Arc::clone(&expected_storage);
                let replacement_bucket = bucket.clone();
                let old_store = Arc::clone(&store);
                let fail = Box::pin(async move {
                    let (after_delete, _) = persisted_replication(&replacement_bucket).await;
                    assert!(!String::from_utf8_lossy(&after_delete).contains("old-operator"));
                    assert!(lock_bucket_targets_metadata(&replacement_bucket).now_or_never().is_none());
                    if replace {
                        old_store
                            .delete_bucket(&replacement_bucket, &DeleteBucketOptions::default())
                            .await
                            .expect("delete original before restoration");
                        let replacement = seed_replication(&old_store, &replacement_bucket, "replacement").await;
                        *output.lock().expect("replacement bytes") = replacement;
                    }
                    if fail_target_write {
                        Err(s3_error!(InternalError, "replication target cleanup failed"))
                    } else {
                        Ok(())
                    }
                });
                let (result, reloads, hooks) = TARGET_CLEANUP_FAILURE
                    .scope(
                        RefCell::new(Some(fail)),
                        observe_replication_edit(&bucket, edit_replication(&usecase, envelope, &bucket, false)),
                    )
                    .await;
                let expected = if fail_target_write {
                    S3ErrorCode::InternalError
                } else {
                    S3ErrorCode::NoSuchBucket
                };
                let stored = persisted_replication(&bucket).await;
                assert_eq!(
                    (result.as_ref().err().map(|error| error.code().clone()), stored),
                    (Some(expected), expected_storage.lock().expect("expected bytes").clone())
                );
                if fail_target_write {
                    assert_eq!(result.unwrap_err().message(), Some("replication target cleanup failed"));
                }
                assert!(reloads.is_empty() && hooks.is_empty());
            }
        });
    }
}
