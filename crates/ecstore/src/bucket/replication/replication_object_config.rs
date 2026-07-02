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

use std::collections::HashMap;

use rustfs_replication::{
    MustReplicateOptions, ReplicationDeleteSource, ReplicationResyncTargetObject, delete_replication_missing_source_decision,
    delete_replication_object_opts, resync_target_for_object,
};
use rustfs_utils::http::AMZ_BUCKET_REPLICATION_STATUS;
use s3s::dto::ReplicationConfiguration;
use serde::{Deserialize, Serialize};
use tracing::error;

use super::config::{ObjectOpts, ReplicationConfigurationExt as _};
use super::replication_error_boundary::Result;
use super::replication_filemeta_boundary::{
    ReplicateDecision, ReplicateTargetDecision, ReplicationStatusType, ReplicationType, ResyncDecision,
};
use super::replication_metadata_boundary::ReplicationMetadataStore;
use super::replication_storage_boundary::{ObjectInfo, ObjectOptions, ObjectToDelete};
use super::replication_target_boundary::{BucketTargets, ReplicationTargetStore};
use super::replication_versioning_boundary::ReplicationVersioningStore;
use super::runtime_boundary as runtime_sources;

const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_REPLICATION_RESYNC: &str = "replication_resync";
const EVENT_RESYNC_CONFIG_LOOKUP_SKIPPED: &str = "replication_resync_config_lookup_skipped";

pub(crate) async fn get_replication_config(bucket: &str) -> Result<Option<ReplicationConfiguration>> {
    ReplicationMetadataStore::optional_replication_config(bucket).await
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

    pub fn replicate(&self, obj: &ObjectOpts) -> bool {
        self.config.as_ref().is_some_and(|config| config.replicate(obj))
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

        if oi.delete_marker {
            let opts = ObjectOpts {
                name: oi.name.clone(),
                version_id: oi.version_id,
                delete_marker: true,
                op_type: ReplicationType::Delete,
                existing_object: true,
                ..Default::default()
            };
            let arns = self
                .config
                .as_ref()
                .map(|config| config.filter_target_arns(&opts))
                .unwrap_or_default();

            if arns.is_empty() {
                return ResyncDecision::default();
            }

            for arn in arns {
                let mut opts = opts.clone();
                opts.target_arn = arn;

                dsc.set(ReplicateTargetDecision::new(opts.target_arn.clone(), self.replicate(&opts), false));
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
    _status: ReplicationStatusType,
    op_type: ReplicationType,
    opts: ObjectOptions,
) -> MustReplicateOptions {
    MustReplicateOptions::new(user_defined, user_tags, op_type, opts.replication_request)
}

pub(crate) async fn check_replicate_delete(
    bucket: &str,
    dobj: &ObjectToDelete,
    oi: &ObjectInfo,
    del_opts: &ObjectOptions,
    gerr: Option<String>,
) -> ReplicateDecision {
    let rcfg = match get_replication_config(bucket).await {
        Ok(Some(config)) => config,
        Ok(None) => return ReplicateDecision::default(),
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
            return ReplicateDecision::default();
        }
    };

    if del_opts.replication_request {
        return ReplicateDecision::default();
    }

    if !del_opts.versioned {
        return ReplicateDecision::default();
    }

    let opts = delete_replication_object_opts(
        dobj,
        &ReplicationDeleteSource {
            user_defined: oi.user_defined.as_ref(),
            user_tags: oi.user_tags.as_str(),
            delete_marker: oi.delete_marker,
            replication_status: oi.replication_status.clone(),
        },
    );

    let tgt_arns = rcfg.filter_target_arns(&opts);
    let mut dsc = ReplicateDecision::new();

    if tgt_arns.is_empty() {
        return dsc;
    }

    for tgt_arn in tgt_arns {
        let mut opts = opts.clone();
        opts.target_arn = tgt_arn.clone();
        let replicate = rcfg.replicate(&opts);
        let sync = false;

        if gerr.is_some() {
            if let Some(replicate) = delete_replication_missing_source_decision(
                oi.delete_marker,
                oi.target_replication_status(&tgt_arn),
                replicate,
                &oi.version_purge_status,
            ) {
                dsc.set(ReplicateTargetDecision::new(tgt_arn, replicate, sync));
            }
            continue;
        }

        let tgt = ReplicationTargetStore::remote_target_client(bucket, &tgt_arn).await;
        let tgt_dsc = if let Some(tgt) = tgt {
            ReplicateTargetDecision::new(tgt_arn, replicate, tgt.replicate_sync)
        } else {
            ReplicateTargetDecision::new(tgt_arn, false, false)
        };
        dsc.set(tgt_dsc);
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

        let replicate = cfg.replicate(&sopts);
        let synchronous = if let Some(cli) = cli { cli.replicate_sync } else { false };

        dsc.set(ReplicateTargetDecision::new(arn, replicate, synchronous));
    }

    dsc
}

#[cfg(test)]
mod tests {
    use s3s::dto::{Destination, ReplicationRule, ReplicationRuleStatus};

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
}
