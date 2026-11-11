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
    /// `site_peers` is the set of remote site-replication peer deployment ids
    /// (empty when site replication is disabled). The interface layer reads it
    /// from the persisted state and fails closed on a read error, so this
    /// usecase stays a pure function of its inputs (layer rule: app never
    /// imports interface).
    pub async fn execute_delete_bucket_replication(
        &self,
        req: S3Request<DeleteBucketReplicationInput>,
        site_peers: HashSet<String>,
        contract: OperatorRuleContract,
    ) -> S3Result<S3Response<DeleteBucketReplicationOutput>> {
        let expected_incarnation_id = bucket_config_mutation_incarnation(&req, &req.input.bucket)?;
        let request_context = req.extensions.get::<request_context::RequestContext>().cloned();
        let DeleteBucketReplicationInput { bucket, .. } = req.input;

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;
        let targets_guard = lock_bucket_targets_metadata(&bucket).await;
        let replication_config = match metadata_sys::get_replication_config(&bucket).await {
            Ok((config, _)) => Some(config),
            Err(StorageError::ConfigNotFound) => None,
            Err(err) => return Err(ApiError::from(err).into()),
        };
        let (remaining_config, updated_targets) = if let Some(config) = replication_config.as_ref() {
            let (remaining, removable_arns) = split_replication_config_for_user_delete(config.clone(), &site_peers, contract);
            let targets = replication_targets_without_arns(&bucket, &removable_arns).await?;
            (remaining, targets)
        } else {
            (None, None)
        };

        match remaining_config {
            // Site-replication rules and the targets backing them survive the
            // S3 delete (issue #1948); only the operator-authored rules go.
            Some(remaining) => {
                let data = serialize_config(&remaining)?;
                update_bucket_config_for_incarnation(&bucket, BUCKET_REPLICATION_CONFIG, data, expected_incarnation_id)
                    .await
                    .map_err(ApiError::from)?;
            }
            None => {
                delete_bucket_config_for_incarnation(&bucket, BUCKET_REPLICATION_CONFIG, expected_incarnation_id)
                    .await
                    .map_err(ApiError::from)?;
            }
        }
        if let Some((targets, removed)) = updated_targets
            && let Err(err) =
                write_replication_targets_after_config_delete(&bucket, &targets, removed, expected_incarnation_id).await
        {
            if let Some(config) = replication_config.as_ref() {
                return Err(restore_replication_config_after_target_cleanup_failure(
                    &bucket,
                    config,
                    err,
                    expected_incarnation_id,
                )
                .await);
            }
            return Err(err);
        }
        drop(targets_guard);

        notify_bucket_metadata_reload(bucket.clone(), "delete bucket replication", request_context, true).await;

        let item = sr_bucket_meta_item(bucket.clone(), "replication-config");
        if let Err(err) = site_replication_bucket_meta_hook(item).await {
            warn!(bucket = %bucket, error = ?err, "site replication bucket replication-config delete hook failed");
        }

        info!(bucket = %bucket, "deleted bucket replication config");

        Ok(S3Response::new(DeleteBucketReplicationOutput::default()))
    }

    /// Answers the stored configuration in the engine's shape; the s3s edge
    /// (`storage::ecfs`) converts it into the `GetBucketReplication` output.
    pub async fn execute_get_bucket_replication(
        &self,
        req: S3Request<GetBucketReplicationInput>,
    ) -> S3Result<PersistedReplicationConfiguration> {
        let GetBucketReplicationInput { bucket, .. } = req.input;

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let replication_configuration = match metadata_sys::get_replication_config(&bucket).await {
            Ok((cfg, _created)) => cfg,
            Err(StorageError::ConfigNotFound) => {
                return Err(S3Error::with_message(
                    S3ErrorCode::ReplicationConfigurationNotFoundError,
                    "replication not found".to_string(),
                ));
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

        Ok(replication_configuration)
    }

    /// See [`Self::execute_delete_bucket_replication`] for `site_peers` and
    /// `contract`. `replication_configuration` is the request body in the
    /// engine's shape: the s3s edge (`storage::ecfs`) converts it, and this use
    /// case never reads `req.input.replication_configuration`.
    pub async fn execute_put_bucket_replication(
        &self,
        req: S3Request<PutBucketReplicationInput>,
        replication_configuration: PersistedReplicationConfiguration,
        site_peers: HashSet<String>,
        contract: OperatorRuleContract,
    ) -> S3Result<S3Response<PutBucketReplicationOutput>> {
        let expected_incarnation_id = bucket_config_mutation_incarnation(&req, &req.input.bucket)?;
        let request_context = req.extensions.get::<request_context::RequestContext>().cloned();
        let PutBucketReplicationInput { bucket, .. } = req.input;
        info!(bucket = %bucket, "updating bucket replication config");

        validate_replication_config_capabilities(&replication_configuration)?;

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let targets_guard = lock_bucket_targets_metadata(&bucket).await;
        validate_bucket_replication_update(&bucket, &replication_configuration).await?;
        let existing_config = match metadata_sys::get_replication_config(&bucket).await {
            Ok((config, _)) => Some(config),
            Err(StorageError::ConfigNotFound) => None,
            Err(err) => return Err(ApiError::from(err).into()),
        };
        let replication_configuration =
            merge_user_replication_config_update(replication_configuration, existing_config, &site_peers, contract);
        let data = serialize_config(&replication_configuration)?;
        update_bucket_config_for_incarnation(&bucket, BUCKET_REPLICATION_CONFIG, data, expected_incarnation_id)
            .await
            .map_err(ApiError::from)?;
        drop(targets_guard);

        notify_bucket_metadata_reload(bucket.clone(), "put bucket replication", request_context, true).await;

        let mut item = sr_bucket_meta_item(bucket.clone(), "replication-config");
        item.replication_config = Some(
            serialize_config(&replication_configuration).and_then(|bytes| String::from_utf8(bytes).map_err(to_internal_error))?,
        );
        if let Err(err) = site_replication_bucket_meta_hook(item).await {
            warn!(bucket = %bucket, error = ?err, "site replication bucket replication-config hook failed");
        }

        Ok(S3Response::new(PutBucketReplicationOutput::default()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::bucket_usecase::test_support::build_request;
    use http::Method;
    use rustfs_gateway_types::dto::Status;
    use rustfs_gateway_types::persistence::{
        PersistedEncryptionConfiguration, PersistedReplicationDestination, PersistedReplicationRule,
    };

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
        let input = DeleteBucketReplicationInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::DELETE);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase
            .execute_delete_bucket_replication(req, HashSet::new(), OperatorRuleContract::Derived)
            .await
            .unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_get_bucket_replication_returns_internal_error_when_store_uninitialized() {
        let input = GetBucketReplicationInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_get_bucket_replication(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_put_bucket_replication_returns_internal_error_when_store_uninitialized() {
        // The config must clear the structural/capability validators so the
        // request actually reaches the store lookup this test pins.
        let configuration = PersistedReplicationConfiguration {
            role: "arn:aws:iam::123456789012:role/test".to_string(),
            rules: vec![replication_rule_for_target("arn:rustfs:replication:us-east-1:target:bucket")],
        };
        let input = PutBucketReplicationInput::builder()
            .bucket("test-bucket".to_string())
            .replication_configuration(Default::default())
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase
            .execute_put_bucket_replication(req, configuration, HashSet::new(), OperatorRuleContract::Derived)
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
        let input = PutBucketReplicationInput::builder()
            .bucket("test-bucket".to_string())
            .replication_configuration(Default::default())
            .build()
            .unwrap();

        let err = DefaultBucketUsecase::without_context()
            .execute_put_bucket_replication(
                build_request(input, Method::PUT),
                configuration,
                HashSet::new(),
                OperatorRuleContract::Derived,
            )
            .await
            .expect_err("unsupported fields must be rejected before store access");

        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert_eq!(
            err.message(),
            Some("replication field Destination.Account is not supported by this RustFS version")
        );
    }
}
