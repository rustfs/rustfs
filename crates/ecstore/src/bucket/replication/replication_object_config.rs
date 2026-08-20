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

use std::{collections::HashMap, fmt, sync::Arc};

use crate::bucket::metadata::BucketMetadata;
use rustfs_utils::http::AMZ_BUCKET_REPLICATION_STATUS;
use s3s::dto::{BucketVersioningStatus, ReplicationConfiguration, ReplicationRuleStatus, VersioningConfiguration};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tracing::error;

use super::replication_config_boundary::{
    ObjectOpts, ReplicationConfigurationExt as _, ReplicationRuleExt as _, invalid_replication_config_status_field,
};
use super::replication_error_boundary::Result;
use super::replication_filemeta_boundary::{
    ReplicateDecision, ReplicateTargetDecision, ReplicationStatusType, ReplicationType, ResyncDecision,
};
use super::replication_logging::{EVENT_RESYNC_CONFIG_LOOKUP_SKIPPED, LOG_COMPONENT_ECSTORE, LOG_SUBSYSTEM_REPLICATION_RESYNC};
use super::replication_metadata_boundary::{ReplicationInstanceContext, ReplicationMetadataStore};
use super::replication_object_decision_boundary::{
    MustReplicateOptions, ReplicationDeleteSource, ReplicationResyncTargetObject, delete_replication_missing_source_decision,
    delete_replication_object_opts, heal_uses_delete_replication_path, resync_target_for_object,
};
use super::replication_storage_boundary::{ObjectInfo, ObjectOptions, ObjectToDelete, object_to_delete_for_replication};
use super::replication_target_boundary::{BucketTargets, ReplicationTargetStore};
use super::replication_versioning_boundary::ReplicationVersioningStore;
use super::runtime_boundary as runtime_sources;

pub(crate) async fn get_replication_config(bucket: &str) -> Result<Option<ReplicationConfiguration>> {
    let config = ReplicationMetadataStore::optional_replication_config(bucket).await?;
    validate_delete_replication_config(&VersioningConfiguration::default(), config.as_ref())?;
    Ok(config)
}

#[derive(Default)]
pub struct DeleteReplicationConfigSnapshot {
    metadata: Option<Arc<BucketMetadata>>,
    versioning: VersioningConfiguration,
}

impl fmt::Debug for DeleteReplicationConfigSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeleteReplicationConfigSnapshot")
            .field("has_replication_config", &self.replication_config().is_some())
            .field("versioning_status", &self.versioning.status)
            .finish()
    }
}

impl DeleteReplicationConfigSnapshot {
    #[cfg(test)]
    pub(crate) fn from_configs_for_test(
        versioning: VersioningConfiguration,
        replication: Option<ReplicationConfiguration>,
    ) -> Self {
        let metadata = replication.map(|config| {
            let mut metadata = BucketMetadata::new("test-bucket");
            metadata.replication_config = Some(config);
            Arc::new(metadata)
        });
        Self { metadata, versioning }
    }

    pub fn versioning_config(&self) -> &VersioningConfiguration {
        &self.versioning
    }

    pub fn replication_config(&self) -> Option<&ReplicationConfiguration> {
        self.metadata
            .as_ref()
            .and_then(|metadata| metadata.replication_config.as_ref())
    }

    pub(crate) fn force_delete_target_set(&self, prefix: &str) -> Option<(Vec<String>, OffsetDateTime)> {
        self.metadata.as_ref().and_then(|metadata| {
            metadata
                .replication_config
                .as_ref()
                .map(|config| (config.filter_force_delete_target_arns(prefix), metadata.replication_config_updated_at))
        })
    }

    pub(crate) fn has_active_rule(&self, object: &str) -> bool {
        self.replication_config()
            .is_some_and(|config| config.has_active_rules(object, true))
    }

    pub(crate) fn active_delete_marker_rules_require_tags(&self, object: &str) -> bool {
        self.replication_config().is_some_and(|config| {
            config.rules.iter().any(|rule| {
                if rule.status == ReplicationRuleStatus::from_static(ReplicationRuleStatus::DISABLED) {
                    return false;
                }
                if !object.starts_with(rule.prefix()) {
                    return false;
                }
                rule.filter.as_ref().is_some_and(|filter| {
                    filter.tag.is_some()
                        || filter
                            .and
                            .as_ref()
                            .and_then(|and| and.tags.as_ref())
                            .is_some_and(|tags| !tags.is_empty())
                })
            })
        })
    }
}

fn validate_delete_replication_config(
    versioning: &VersioningConfiguration,
    config: Option<&ReplicationConfiguration>,
) -> Result<()> {
    if versioning
        .status
        .as_ref()
        .is_some_and(|status| !matches!(status.as_str(), BucketVersioningStatus::ENABLED | BucketVersioningStatus::SUSPENDED))
    {
        return Err(super::replication_error_boundary::Error::other(
            "bucket versioning configuration has an invalid status",
        ));
    }

    if let Some(config) = config {
        if let Some(field) = invalid_replication_config_status_field(config) {
            return Err(super::replication_error_boundary::Error::other(format!(
                "replication field {field} has an invalid status"
            )));
        }

        let role = config.role.trim();
        let mut role_destination = None;
        for rule in &config.rules {
            if rule.status.as_str() == ReplicationRuleStatus::ENABLED {
                let destination = rule.destination.bucket.trim();
                if role.is_empty() && destination.is_empty() {
                    return Err(super::replication_error_boundary::Error::other(
                        "enabled replication rule has no destination ARN",
                    ));
                }
                if !role.is_empty() && !destination.is_empty() {
                    match role_destination {
                        Some(existing) if existing != destination => {
                            return Err(super::replication_error_boundary::Error::other(
                                "replication role cannot address multiple active destinations",
                            ));
                        }
                        None => role_destination = Some(destination),
                        _ => {}
                    }
                }
            }
        }
    }

    Ok(())
}

fn replication_config_from_metadata(metadata: &BucketMetadata) -> Result<Option<&ReplicationConfiguration>> {
    if !metadata.replication_config_xml.is_empty() && metadata.replication_config.is_none() {
        return Err(super::replication_error_boundary::Error::other(
            "persisted bucket replication configuration is invalid",
        ));
    }
    Ok(metadata.replication_config.as_ref())
}

fn delete_request_snapshot_from_metadata(metadata: Arc<BucketMetadata>) -> Result<DeleteReplicationConfigSnapshot> {
    if !metadata.versioning_config_xml.is_empty() && metadata.versioning_config.is_none() {
        return Err(super::replication_error_boundary::Error::other(
            "persisted bucket versioning configuration is invalid",
        ));
    }

    let versioning = metadata.versioning_config.clone().unwrap_or_default();
    let has_config = {
        let config = replication_config_from_metadata(&metadata)?;
        if versioning.status.is_none() && config.is_some() {
            return Err(super::replication_error_boundary::Error::other(
                "bucket replication configuration requires versioning",
            ));
        }
        validate_delete_replication_config(&versioning, config)?;
        config.is_some()
    };
    Ok(DeleteReplicationConfigSnapshot {
        metadata: has_config.then_some(metadata),
        versioning,
    })
}

fn delete_snapshot_from_metadata(metadata: Arc<BucketMetadata>) -> Result<DeleteReplicationConfigSnapshot> {
    let has_config = {
        let config = replication_config_from_metadata(&metadata)?;
        validate_delete_replication_config(&VersioningConfiguration::default(), config)?;
        config.is_some()
    };
    Ok(DeleteReplicationConfigSnapshot {
        metadata: has_config.then_some(metadata),
        versioning: VersioningConfiguration::default(),
    })
}

pub(crate) async fn load_delete_request_config_in(
    ctx: &ReplicationInstanceContext,
    bucket: &str,
) -> Result<DeleteReplicationConfigSnapshot> {
    delete_request_snapshot_from_metadata(ReplicationMetadataStore::delete_metadata_in(ctx, bucket).await?)
}

pub(crate) async fn load_delete_replication_config(
    bucket: &str,
    opts: &ObjectOptions,
) -> Result<DeleteReplicationConfigSnapshot> {
    if opts.replication_request || (!opts.versioned && !opts.version_suspended) {
        return Ok(DeleteReplicationConfigSnapshot::default());
    }
    delete_snapshot_from_metadata(ReplicationMetadataStore::delete_metadata(bucket).await?)
}

#[allow(
    dead_code,
    reason = "MinIO-parity replication surface with no caller in this port (backlog#1823)"
)]
pub(crate) async fn load_delete_replication_config_in(
    ctx: &ReplicationInstanceContext,
    bucket: &str,
    opts: &ObjectOptions,
) -> Result<DeleteReplicationConfigSnapshot> {
    if opts.replication_request || (!opts.versioned && !opts.version_suspended) {
        return Ok(DeleteReplicationConfigSnapshot::default());
    }
    delete_snapshot_from_metadata(ReplicationMetadataStore::delete_metadata_in(ctx, bucket).await?)
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReplicationConfig {
    pub config: Option<ReplicationConfiguration>,
    pub remotes: Option<BucketTargets>,
}

impl ReplicationConfig {
    pub fn new(config: Option<ReplicationConfiguration>, remotes: Option<BucketTargets>) -> Self {
        Self { config, remotes }
    }

    pub fn is_empty(&self) -> bool {
        self.config.is_none()
    }

    pub(crate) fn validate(&self) -> Result<()> {
        validate_delete_replication_config(&VersioningConfiguration::default(), self.config.as_ref())
    }

    pub fn replicate(&self, obj: &ObjectOpts) -> bool {
        self.config.as_ref().is_some_and(|config| config.replicate(obj))
    }

    pub(crate) fn check_delete_for_heal(
        &self,
        object: &ObjectToDelete,
        source: &ObjectInfo,
        opts: &ObjectOptions,
    ) -> ReplicateDecision {
        check_replicate_delete_with_config(
            object,
            source,
            opts,
            false,
            self.config.as_ref(),
            source.delete_marker && source.version_purge_status.is_empty(),
            true,
        )
    }

    pub async fn resync(
        &self,
        oi: ObjectInfo,
        dsc: ReplicateDecision,
        status: &HashMap<String, ReplicationStatusType>,
    ) -> ResyncDecision {
        if self.is_empty() {
            return ResyncDecision::default();
        }

        let mut dsc = dsc;

        if heal_uses_delete_replication_path(oi.delete_marker, &oi.version_purge_status) {
            if !dsc.targets_map.is_empty() {
                return self.resync_internal(oi, dsc, status);
            }
            let opts = ObjectOpts {
                name: oi.name.clone(),
                version_id: if oi.version_purge_status.is_empty() {
                    None
                } else {
                    oi.version_id
                },
                delete_marker: oi.delete_marker,
                op_type: ReplicationType::Delete,
                existing_object: true,
                ..Default::default()
            };
            let targets = self
                .config
                .as_ref()
                .map(|config| config.filter_target_replication_decisions(&opts))
                .unwrap_or_default();

            if targets.is_empty() {
                return ResyncDecision::default();
            }

            for (arn, replicate) in targets {
                dsc.set(ReplicateTargetDecision::new(arn, replicate, false));
            }

            return self.resync_internal(oi, dsc, status);
        }

        let mut user_defined = (*oi.user_defined).clone();
        user_defined.remove(AMZ_BUCKET_REPLICATION_STATUS);

        let dsc = must_replicate(
            oi.bucket.as_str(),
            &oi.name,
            MustReplicateOptions::new(&user_defined, (*oi.user_tags).clone(), ReplicationType::ExistingObject, false),
        )
        .await;

        self.resync_internal(oi, dsc, status)
    }

    fn resync_internal(
        &self,
        oi: ObjectInfo,
        dsc: ReplicateDecision,
        status: &HashMap<String, ReplicationStatusType>,
    ) -> ResyncDecision {
        let Some(remotes) = self.remotes.as_ref() else {
            return ResyncDecision::default();
        };

        if remotes.is_empty() {
            return ResyncDecision::default();
        }

        let mut resync_decision = ResyncDecision::default();

        for target in remotes.targets.iter() {
            if let Some(decision) = dsc.targets_map.get(&target.arn)
                && decision.replicate
            {
                resync_decision.targets.insert(
                    decision.arn.clone(),
                    resync_target_for_object(
                        &ReplicationResyncTargetObject {
                            mod_time: oi.mod_time,
                            user_defined: oi.user_defined.as_ref(),
                        },
                        &target.arn,
                        &target.reset_id,
                        target.reset_before_date,
                        status.get(&decision.arn).unwrap_or(&ReplicationStatusType::Empty).clone(),
                    ),
                );
            }
        }

        resync_decision
    }
}

pub(crate) fn get_must_replicate_options(
    user_defined: &HashMap<String, String>,
    user_tags: String,
    status: ReplicationStatusType,
    op_type: ReplicationType,
    opts: ObjectOptions,
) -> MustReplicateOptions {
    MustReplicateOptions::new(user_defined, user_tags, op_type, opts.replication_request).with_replication_status(status)
}

pub(crate) async fn check_replicate_delete(
    bucket: &str,
    dobj: &ObjectToDelete,
    oi: &ObjectInfo,
    del_opts: &ObjectOptions,
    gerr: Option<String>,
) -> ReplicateDecision {
    match load_delete_replication_config(bucket, del_opts).await {
        Ok(snapshot) => {
            check_replicate_delete_with_config(dobj, oi, del_opts, gerr.is_some(), snapshot.replication_config(), false, false)
        }
        Err(err) => {
            error!(
                event = EVENT_RESYNC_CONFIG_LOOKUP_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = %bucket,
                reason = "replication_config_lookup_failed",
                error = %err,
                "Failed to look up replication config for delete replication"
            );
            ReplicateDecision::default()
        }
    }
}

pub(crate) async fn check_replicate_delete_strict(
    bucket: &str,
    dobj: &ObjectToDelete,
    oi: &ObjectInfo,
    del_opts: &ObjectOptions,
    gerr: Option<String>,
) -> Result<ReplicateDecision> {
    let Some(config) = get_replication_config(bucket).await? else {
        return Ok(ReplicateDecision::default());
    };
    let mut decision = check_replicate_delete_with_config(dobj, oi, del_opts, gerr.is_some(), Some(&config), false, false);
    if gerr.is_some() {
        return Ok(decision);
    }

    for target in decision.targets_map.values_mut() {
        if let Some(client) = ReplicationTargetStore::remote_target_client(bucket, &target.arn).await {
            target.synchronous = client.replicate_sync;
        } else {
            target.replicate = false;
            target.synchronous = false;
        }
    }
    Ok(decision)
}

pub(crate) fn check_replicate_delete_with_snapshot(
    dobj: &ObjectToDelete,
    oi: &ObjectInfo,
    del_opts: &ObjectOptions,
    source_error: bool,
    snapshot: &DeleteReplicationConfigSnapshot,
) -> ReplicateDecision {
    check_replicate_delete_with_config(dobj, oi, del_opts, source_error, snapshot.replication_config(), false, false)
}

fn check_replicate_delete_with_config(
    dobj: &ObjectToDelete,
    oi: &ObjectInfo,
    del_opts: &ObjectOptions,
    source_error: bool,
    config: Option<&ReplicationConfiguration>,
    existing_delete_marker: bool,
    trust_persisted_replica_status: bool,
) -> ReplicateDecision {
    if del_opts.replication_request {
        return ReplicateDecision::default();
    }

    if !del_opts.versioned && !del_opts.version_suspended {
        return ReplicateDecision::default();
    }

    let Some(rcfg) = config else {
        return ReplicateDecision::default();
    };

    let replication_delete = object_to_delete_for_replication(dobj);
    let missing_source_marker = source_error && dobj.version_id.is_none();
    let mut opts = delete_replication_object_opts(
        &replication_delete,
        &ReplicationDeleteSource {
            user_defined: oi.user_defined.as_ref(),
            user_tags: oi.user_tags.as_str(),
            delete_marker: oi.delete_marker || missing_source_marker,
            replication_status: if trust_persisted_replica_status {
                oi.replication_status.clone()
            } else {
                ReplicationStatusType::Empty
            },
        },
    );
    if existing_delete_marker {
        opts.version_id = None;
    }

    let target_decisions = rcfg.filter_target_replication_decisions(&opts);
    let mut dsc = ReplicateDecision::new();

    if target_decisions.is_empty() {
        return dsc;
    }

    for (tgt_arn, replicate) in target_decisions {
        let effective_replicate = if source_error {
            delete_replication_missing_source_decision(
                oi.delete_marker || missing_source_marker,
                oi.target_replication_status(&tgt_arn),
                replicate,
                &oi.version_purge_status,
            )
        } else {
            Some(replicate)
        };
        let Some(effective_replicate) = effective_replicate else {
            continue;
        };

        dsc.set(ReplicateTargetDecision::new(tgt_arn, effective_replicate, false));
    }

    dsc
}

pub(crate) async fn must_replicate(bucket: &str, object: &str, mopts: MustReplicateOptions) -> ReplicateDecision {
    if runtime_sources::object_store_handle().is_none() {
        return ReplicateDecision::default();
    }

    if !ReplicationVersioningStore::prefix_enabled(bucket, object).await {
        return ReplicateDecision::default();
    }

    let replication_status = mopts.replication_status();

    if replication_status == ReplicationStatusType::Replica && !mopts.is_metadata_replication() {
        return ReplicateDecision::default();
    }

    if mopts.is_replication_request() {
        return ReplicateDecision::default();
    }

    let cfg = match get_replication_config(bucket).await {
        Ok(Some(cfg)) => cfg,
        Ok(None) | Err(_) => return ReplicateDecision::default(),
    };

    let opts = ObjectOpts {
        name: object.to_string(),
        replica: replication_status == ReplicationStatusType::Replica,
        existing_object: mopts.is_existing_object_replication(),
        user_tags: mopts.user_tags().to_string(),
        ..Default::default()
    };

    let arns = cfg.filter_target_arns(&opts);

    if arns.is_empty() {
        return ReplicateDecision::default();
    }

    let mut dsc = ReplicateDecision::default();

    for arn in arns {
        let cli = ReplicationTargetStore::remote_target_client(bucket, &arn).await;

        let mut sopts = opts.clone();
        sopts.target_arn = arn.clone();

        let replicate = cfg.replicate(&sopts) && mopts.metadata_target_is_eligible(&arn);
        let synchronous = if let Some(cli) = cli { cli.replicate_sync } else { false };

        dsc.set(ReplicateTargetDecision::new(arn, replicate, synchronous));
    }

    dsc
}

#[cfg(test)]
mod tests {
    use s3s::dto::{
        DeleteMarkerReplication, DeleteMarkerReplicationStatus, DeleteReplication, DeleteReplicationStatus, Destination,
        ReplicaModifications, ReplicationRule, ReplicationRuleFilter, ReplicationRuleStatus, SourceSelectionCriteria, Tag,
    };

    use super::super::replication_filemeta_boundary::VersionPurgeStatusType;
    use super::super::replication_target_boundary::BucketTarget;
    use super::*;

    fn replication_rule() -> ReplicationRule {
        ReplicationRule {
            delete_marker_replication: None,
            delete_replication: None,
            destination: Destination {
                bucket: "arn:aws:s3:::target-bucket".to_string(),
                ..Default::default()
            },
            existing_object_replication: None,
            filter: None,
            id: Some("rule".to_string()),
            prefix: Some(String::new()),
            priority: Some(1),
            source_selection_criteria: None,
            status: ReplicationRuleStatus::from_static(ReplicationRuleStatus::ENABLED),
        }
    }

    #[test]
    fn replication_config_empty_and_replicate_follow_config() {
        let empty = ReplicationConfig::default();
        assert!(empty.is_empty());
        assert!(!empty.replicate(&ObjectOpts::default()));

        let config = ReplicationConfig::new(
            Some(ReplicationConfiguration {
                role: String::new(),
                rules: vec![replication_rule()],
            }),
            None,
        );

        assert!(!config.is_empty());
        assert!(config.replicate(&ObjectOpts {
            name: "object".to_string(),
            ..Default::default()
        }));
    }

    #[test]
    fn must_replicate_options_preserve_request_flag() {
        let user_defined = HashMap::new();
        let options = get_must_replicate_options(
            &user_defined,
            "env=prod".to_string(),
            ReplicationStatusType::Empty,
            ReplicationType::Metadata,
            ObjectOptions {
                replication_request: true,
                ..Default::default()
            },
        );

        assert!(options.is_metadata_replication());
        assert!(options.is_replication_request());
        assert_eq!(options.user_tags(), "env=prod");
    }

    #[test]
    fn delete_snapshot_rejects_enabled_rules_without_a_destination() {
        let mut rule = replication_rule();
        rule.destination.bucket.clear();
        let config = ReplicationConfiguration {
            role: String::new(),
            rules: vec![rule],
        };

        let err = validate_delete_replication_config(&VersioningConfiguration::default(), Some(&config))
            .expect_err("an enabled rule without a destination must fail closed");

        assert!(err.to_string().contains("destination ARN"));
    }

    #[test]
    fn delete_snapshot_rejects_role_with_multiple_destinations() {
        let first = replication_rule();
        let mut second = replication_rule();
        second.destination.bucket = "arn:aws:s3:::other-target".to_string();
        let config = ReplicationConfiguration {
            role: "arn:aws:s3:::role-target".to_string(),
            rules: vec![first, second],
        };

        assert!(validate_delete_replication_config(&VersioningConfiguration::default(), Some(&config)).is_err());
    }

    #[test]
    fn delete_snapshot_rejects_unknown_status_values() {
        let invalid_versioning = VersioningConfiguration {
            status: Some("Enabld".to_string().into()),
            ..Default::default()
        };
        assert!(validate_delete_replication_config(&invalid_versioning, None).is_err());

        let mut invalid_rule = replication_rule();
        invalid_rule.status = "Enabld".to_string().into();
        let config = ReplicationConfiguration {
            role: String::new(),
            rules: vec![invalid_rule],
        };
        assert!(validate_delete_replication_config(&VersioningConfiguration::default(), Some(&config)).is_err());

        let mut invalid_delete = replication_rule();
        invalid_delete.delete_replication = Some(DeleteReplication {
            status: "Enabld".to_string().into(),
        });
        let config = ReplicationConfiguration {
            role: String::new(),
            rules: vec![invalid_delete],
        };
        assert!(validate_delete_replication_config(&VersioningConfiguration::default(), Some(&config)).is_err());

        let mut invalid_delete_marker = replication_rule();
        invalid_delete_marker.delete_marker_replication = Some(DeleteMarkerReplication {
            status: Some("Enabld".to_string().into()),
        });
        let config = ReplicationConfiguration {
            role: String::new(),
            rules: vec![invalid_delete_marker],
        };
        assert!(validate_delete_replication_config(&VersioningConfiguration::default(), Some(&config)).is_err());

        let mut invalid_replica_modifications = replication_rule();
        invalid_replica_modifications.source_selection_criteria = Some(SourceSelectionCriteria {
            replica_modifications: Some(ReplicaModifications {
                status: "Enabld".to_string().into(),
            }),
            sse_kms_encrypted_objects: None,
        });
        let config = ReplicationConfiguration {
            role: String::new(),
            rules: vec![invalid_replica_modifications],
        };
        assert!(validate_delete_replication_config(&VersioningConfiguration::default(), Some(&config)).is_err());
    }

    #[test]
    fn request_snapshot_borrows_cached_replication_config() {
        let mut metadata = BucketMetadata::new("bucket");
        metadata.versioning_config_xml = b"configured".to_vec();
        metadata.versioning_config = Some(VersioningConfiguration {
            status: Some(BucketVersioningStatus::from_static(BucketVersioningStatus::ENABLED)),
            ..Default::default()
        });
        metadata.replication_config_xml = b"configured".to_vec();
        metadata.replication_config = Some(ReplicationConfiguration {
            role: String::new(),
            rules: vec![replication_rule()],
        });
        let metadata = Arc::new(metadata);
        let cached_config = metadata.replication_config.as_ref().expect("cached config") as *const _;

        let snapshot = delete_request_snapshot_from_metadata(Arc::clone(&metadata)).expect("valid snapshot");

        assert_eq!(snapshot.replication_config().expect("snapshot config") as *const _, cached_config);
    }

    #[test]
    fn delete_snapshot_debug_redacts_bucket_target_credentials() {
        let secret = "snapshot-secret-must-not-be-formatted";
        let mut metadata = BucketMetadata::new("bucket");
        metadata.bucket_targets_config_json = format!(r#"{{"secretKey":"{secret}"}}"#).into_bytes();
        let opts = ObjectOptions {
            delete_replication_config_snapshot: Some(Arc::new(DeleteReplicationConfigSnapshot {
                metadata: Some(Arc::new(metadata)),
                versioning: VersioningConfiguration::default(),
            })),
            ..Default::default()
        };

        let debug = format!("{opts:?}");
        assert!(!debug.contains(secret), "delete tracing must not expose replication target credentials");
        assert!(debug.contains("has_replication_config"));
    }

    #[test]
    fn request_snapshot_rejects_replication_without_versioning_status() {
        let mut malformed = BucketMetadata::new("bucket");
        malformed.replication_config_xml = b"<ReplicationConfiguration>".to_vec();
        assert!(
            delete_request_snapshot_from_metadata(Arc::new(malformed)).is_err(),
            "malformed replication metadata must fail closed even when versioning has no status"
        );

        let mut inconsistent = BucketMetadata::new("bucket");
        inconsistent.replication_config = Some(ReplicationConfiguration {
            role: String::new(),
            rules: vec![replication_rule()],
        });
        assert!(
            delete_request_snapshot_from_metadata(Arc::new(inconsistent)).is_err(),
            "replication metadata without an enabled or suspended versioning state must fail closed"
        );
    }

    #[test]
    fn missing_source_marker_creation_is_still_admitted() {
        let arn = "arn:rustfs:replication:us-east-1:target:bucket";
        let mut rule = replication_rule();
        rule.destination.bucket = arn.to_string();
        rule.delete_marker_replication = Some(DeleteMarkerReplication {
            status: Some(DeleteMarkerReplicationStatus::from_static(DeleteMarkerReplicationStatus::ENABLED)),
        });
        let mut metadata = BucketMetadata::new("bucket");
        metadata.replication_config = Some(ReplicationConfiguration {
            role: String::new(),
            rules: vec![rule],
        });
        let snapshot = DeleteReplicationConfigSnapshot {
            metadata: Some(Arc::new(metadata)),
            ..Default::default()
        };

        let decision = check_replicate_delete_with_snapshot(
            &ObjectToDelete {
                object_name: "object".to_string(),
                ..Default::default()
            },
            &ObjectInfo::default(),
            &ObjectOptions {
                versioned: true,
                ..Default::default()
            },
            true,
            &snapshot,
        );

        assert!(decision.replicate_any());
        assert!(decision.targets_map.get(arn).is_some_and(|target| target.replicate));
    }

    #[test]
    fn delete_marker_source_read_is_required_only_for_tag_filtered_rules() {
        let mut prefix_rule = replication_rule();
        prefix_rule.prefix = Some("logs/".to_string());
        prefix_rule.delete_marker_replication = Some(DeleteMarkerReplication {
            status: Some(DeleteMarkerReplicationStatus::from_static(DeleteMarkerReplicationStatus::ENABLED)),
        });

        let prefix_snapshot = DeleteReplicationConfigSnapshot::from_configs_for_test(
            VersioningConfiguration {
                status: Some(BucketVersioningStatus::from_static(BucketVersioningStatus::ENABLED)),
                ..Default::default()
            },
            Some(ReplicationConfiguration {
                role: String::new(),
                rules: vec![prefix_rule],
            }),
        );

        let mut tag_rule = replication_rule();
        tag_rule.delete_marker_replication = Some(DeleteMarkerReplication {
            status: Some(DeleteMarkerReplicationStatus::from_static(DeleteMarkerReplicationStatus::ENABLED)),
        });
        tag_rule.filter = Some(ReplicationRuleFilter {
            tag: Some(Tag {
                key: Some("class".to_string()),
                value: Some("audit".to_string()),
            }),
            ..Default::default()
        });
        let tag_snapshot = DeleteReplicationConfigSnapshot::from_configs_for_test(
            VersioningConfiguration {
                status: Some(BucketVersioningStatus::from_static(BucketVersioningStatus::ENABLED)),
                ..Default::default()
            },
            Some(ReplicationConfiguration {
                role: String::new(),
                rules: vec![tag_rule],
            }),
        );

        assert!(!prefix_snapshot.active_delete_marker_rules_require_tags("logs/2026/app.log"));
        assert!(tag_snapshot.active_delete_marker_rules_require_tags("logs/2026/app.log"));
    }

    #[test]
    fn heal_uses_delete_switch_for_pending_purges_and_marker_switch_for_stored_markers() {
        let arn = "arn:rustfs:replication:us-east-1:target:bucket";
        let mut rule = replication_rule();
        rule.destination.bucket = arn.to_string();
        rule.delete_replication = Some(DeleteReplication {
            status: DeleteReplicationStatus::from_static(DeleteReplicationStatus::ENABLED),
        });
        rule.delete_marker_replication = Some(DeleteMarkerReplication {
            status: Some(DeleteMarkerReplicationStatus::from_static(DeleteMarkerReplicationStatus::DISABLED)),
        });
        let config = ReplicationConfig::new(
            Some(ReplicationConfiguration {
                role: String::new(),
                rules: vec![rule],
            }),
            None,
        );
        let object = ObjectToDelete {
            object_name: "object".to_string(),
            version_id: Some(uuid::Uuid::new_v4()),
            ..Default::default()
        };
        let opts = ObjectOptions {
            versioned: true,
            ..Default::default()
        };

        let purge = ObjectInfo {
            version_id: object.version_id,
            version_purge_status: VersionPurgeStatusType::Pending,
            ..Default::default()
        };
        assert!(config.check_delete_for_heal(&object, &purge, &opts).replicate_any());

        let marker = ObjectInfo {
            delete_marker: true,
            version_id: object.version_id,
            ..Default::default()
        };
        assert!(!config.check_delete_for_heal(&object, &marker, &opts).replicate_any());
    }

    #[test]
    fn live_delete_does_not_trust_persisted_replica_status() {
        let mut rule = replication_rule();
        rule.delete_replication = Some(DeleteReplication {
            status: DeleteReplicationStatus::from_static(DeleteReplicationStatus::ENABLED),
        });
        rule.source_selection_criteria = Some(SourceSelectionCriteria {
            replica_modifications: Some(ReplicaModifications {
                status: s3s::dto::ReplicaModificationsStatus::from_static(s3s::dto::ReplicaModificationsStatus::DISABLED),
            }),
            sse_kms_encrypted_objects: None,
        });
        let replication = ReplicationConfiguration {
            role: String::new(),
            rules: vec![rule],
        };
        let snapshot = DeleteReplicationConfigSnapshot::from_configs_for_test(
            VersioningConfiguration {
                status: Some(BucketVersioningStatus::from_static(BucketVersioningStatus::ENABLED)),
                ..Default::default()
            },
            Some(replication.clone()),
        );
        let object = ObjectToDelete {
            object_name: "object".to_string(),
            version_id: Some(uuid::Uuid::new_v4()),
            ..Default::default()
        };
        let source = ObjectInfo {
            replication_status: ReplicationStatusType::Replica,
            ..Default::default()
        };
        let opts = ObjectOptions {
            versioned: true,
            ..Default::default()
        };

        assert!(
            check_replicate_delete_with_snapshot(&object, &source, &opts, false, &snapshot).replicate_any(),
            "an ordinary authenticated delete must not inherit replica identity from object metadata"
        );
        assert!(
            !ReplicationConfig::new(Some(replication), None)
                .check_delete_for_heal(&object, &source, &opts)
                .replicate_any(),
            "heal must still honor the persisted replica identity"
        );
    }

    #[tokio::test]
    async fn resync_keeps_marker_version_purges_separate_from_marker_creation() {
        let arn = "arn:rustfs:replication:us-east-1:target:bucket";
        let mut rule = replication_rule();
        rule.destination.bucket = arn.to_string();
        rule.delete_replication = Some(DeleteReplication {
            status: DeleteReplicationStatus::from_static(DeleteReplicationStatus::ENABLED),
        });
        rule.delete_marker_replication = Some(DeleteMarkerReplication {
            status: Some(DeleteMarkerReplicationStatus::from_static(DeleteMarkerReplicationStatus::DISABLED)),
        });
        let config = ReplicationConfig::new(
            Some(ReplicationConfiguration {
                role: String::new(),
                rules: vec![rule],
            }),
            Some(BucketTargets {
                targets: vec![BucketTarget {
                    arn: arn.to_string(),
                    ..Default::default()
                }],
            }),
        );
        let marker = ObjectInfo {
            name: "object".to_string(),
            delete_marker: true,
            version_id: Some(uuid::Uuid::new_v4()),
            ..Default::default()
        };

        let purge = config
            .resync(
                ObjectInfo {
                    version_purge_status: VersionPurgeStatusType::Pending,
                    ..marker.clone()
                },
                ReplicateDecision::default(),
                &HashMap::new(),
            )
            .await;
        assert!(purge.targets.get(arn).is_some_and(|target| target.replicate));

        let mut object_purge_decision = ReplicateDecision::default();
        object_purge_decision.set(ReplicateTargetDecision::new(arn.to_string(), true, false));
        let object_purge = config
            .resync(
                ObjectInfo {
                    name: "object".to_string(),
                    version_id: Some(uuid::Uuid::new_v4()),
                    version_purge_status: VersionPurgeStatusType::Pending,
                    ..Default::default()
                },
                object_purge_decision,
                &HashMap::new(),
            )
            .await;
        assert!(
            object_purge.targets.get(arn).is_some_and(|target| target.replicate),
            "non-marker PENDING purges must stay on the delete resync path"
        );

        let stored_marker = config.resync(marker, ReplicateDecision::default(), &HashMap::new()).await;
        assert!(!stored_marker.targets.contains_key(arn));
    }
}
