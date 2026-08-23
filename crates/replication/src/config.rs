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

use crate::ReplicationTagFilter;
use crate::ReplicationType;
use crate::rule::ReplicationRuleExt as _;
use s3s::dto::DeleteMarkerReplicationStatus;
use s3s::dto::DeleteReplicationStatus;
use s3s::dto::Destination;
use s3s::dto::{
    ExistingObjectReplicationStatus, ReplicaModificationsStatus, ReplicationConfiguration, ReplicationRule,
    ReplicationRuleStatus, ReplicationRules, StorageClass,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use uuid::Uuid;

pub const REPLICATION_CAPABILITY_CONTRACT_VERSION: u32 = 1;

// `Rule.Destination.StorageClass` is deliberately absent from both lists below:
// clients should keep omitting it, but the validator tolerates an explicit
// `STANDARD` as a no-op (see `unsupported_replication_config_field`) because the
// console's rule form always sends it.
//
// Contract note: rule-level `Destination.StorageClass` is never consumed by the
// replication engine (MinIO's engine likewise reads only the target-level
// storage class). To control the storage class of replicated objects, set the
// remote target's `storage_class` field (set-remote-target API), which RustFS
// does apply on replication PUTs.
pub const REPLICATION_WRITABLE_FIELDS: &[&str] = &[
    "Role",
    "Rule.ID",
    "Rule.Status",
    "Rule.Priority",
    "Rule.Filter.Prefix",
    "Rule.Filter.Tag",
    "Rule.Filter.And",
    "Rule.Destination.Bucket",
    "Rule.ExistingObjectReplication.Status",
    "Rule.DeleteMarkerReplication.Status",
    "Rule.DeleteReplication.Status",
    "Rule.SourceSelectionCriteria.ReplicaModifications.Status",
];

pub const REPLICATION_READ_ONLY_HISTORICAL_FIELDS: &[&str] = &[
    "SourceSelectionCriteria.SseKmsEncryptedObjects",
    "Destination.EncryptionConfiguration",
    "Destination.Metrics",
    "Destination.ReplicationTime",
];

// v2: disableProxy moved from unsupported to writable (per-target read-proxy
// opt-out is accepted by set-remote-target and the `proxy` update op).
pub const REMOTE_TARGET_CAPABILITY_CONTRACT_VERSION: u32 = 2;

pub const REMOTE_TARGET_WRITABLE_FIELDS: &[&str] = &[
    "sourcebucket",
    "endpoint",
    "credentials.accessKey",
    "credentials.secretKey",
    "targetbucket",
    "secure",
    "path",
    "api",
    "arn",
    "type",
    "region",
    "bandwidth",
    "replicationSync",
    "storage_class",
    "skipTlsVerify",
    "caCertPem",
    // Accepted for mc compatibility (mc `replicate add` always sends the
    // madmin default of 60s); the per-target health-check interval is not
    // yet applied — the heartbeat keeps its global env-configured interval.
    "healthCheckDuration",
    // Per-target read-proxy opt-out, consumed by the proxy-target selector
    // (contract v2; previously only importable via MinIO bucket-targets.json).
    "disableProxy",
];

pub const REMOTE_TARGET_UNSUPPORTED_FIELDS: &[&str] = &["edge", "edgeSyncBeforeExpiry"];

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ObjectOpts {
    pub name: String,
    pub user_tags: String,
    pub version_id: Option<Uuid>,
    pub delete_marker: bool,
    pub ssec: bool,
    pub op_type: ReplicationType,
    pub replica: bool,
    pub existing_object: bool,
    pub target_arn: String,
}

pub trait ReplicationConfigurationExt {
    fn replicate(&self, opts: &ObjectOpts) -> bool;
    fn has_existing_object_replication(&self, arn: &str) -> (bool, bool);
    fn filter_actionable_rules(&self, obj: &ObjectOpts) -> ReplicationRules;
    fn get_destination(&self) -> Destination;
    fn has_active_rules(&self, prefix: &str, recursive: bool) -> bool;
    fn filter_target_arns(&self, obj: &ObjectOpts) -> Vec<String>;
    fn filter_force_delete_target_arns(&self, prefix: &str) -> Vec<String>;
    fn filter_target_replication_decisions(&self, obj: &ObjectOpts) -> Vec<(String, bool)> {
        self.filter_target_arns(obj)
            .into_iter()
            .map(|arn| {
                let mut target = obj.clone();
                target.target_arn = arn.clone();
                (arn, self.replicate(&target))
            })
            .collect()
    }
}

fn rule_replicates(rule: &ReplicationRule, obj: &ObjectOpts) -> bool {
    if let Some(status) = &rule.existing_object_replication
        && obj.existing_object
        && status.status == ExistingObjectReplicationStatus::from_static(ExistingObjectReplicationStatus::DISABLED)
    {
        return false;
    }

    if obj.op_type != ReplicationType::Delete {
        return rule.metadata_replicate(obj);
    }

    if !rule.metadata_replicate(obj) {
        return false;
    }

    let version_purge = obj.version_id.is_some();
    if version_purge {
        rule.delete_replication
            .as_ref()
            .is_some_and(|delete| delete.status == DeleteReplicationStatus::from_static(DeleteReplicationStatus::ENABLED))
    } else {
        rule.delete_marker_replication.as_ref().is_some_and(|delete_marker| {
            delete_marker.status == Some(DeleteMarkerReplicationStatus::from_static(DeleteMarkerReplicationStatus::ENABLED))
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationTargetValidationError {
    RoleWithMultipleDestinations,
    StaleTarget,
}

pub fn unsupported_replication_config_field(config: &ReplicationConfiguration) -> Option<&'static str> {
    for rule in &config.rules {
        if rule
            .source_selection_criteria
            .as_ref()
            .is_some_and(|criteria| criteria.sse_kms_encrypted_objects.is_some())
        {
            return Some("SourceSelectionCriteria.SseKmsEncryptedObjects");
        }
        if rule.destination.encryption_configuration.is_some() {
            return Some("Destination.EncryptionConfiguration");
        }
        if rule.destination.access_control_translation.is_some() {
            return Some("Destination.AccessControlTranslation");
        }
        if rule.destination.account.is_some() {
            return Some("Destination.Account");
        }
        if rule.destination.metrics.is_some() {
            return Some("Destination.Metrics");
        }
        if rule.destination.replication_time.is_some() {
            return Some("Destination.ReplicationTime");
        }
        // The replication engine never reads this field (replica placement comes from
        // the bucket-target config or the source object), so an explicit STANDARD —
        // which the console's rule form always sends — is indistinguishable from
        // omitting it and is tolerated as a no-op. Any other value would be silently
        // ignored rather than honored, so it stays rejected. Exact match: S3 storage
        // class enums are case-sensitive.
        if rule
            .destination
            .storage_class
            .as_ref()
            .is_some_and(|class| class.as_str() != StorageClass::STANDARD)
        {
            return Some("Destination.StorageClass");
        }
    }
    None
}

pub fn invalid_replication_config_status_field(config: &ReplicationConfiguration) -> Option<&'static str> {
    for rule in &config.rules {
        if !matches!(rule.status.as_str(), ReplicationRuleStatus::ENABLED | ReplicationRuleStatus::DISABLED) {
            return Some("Rule.Status");
        }
        if rule.existing_object_replication.as_ref().is_some_and(|existing| {
            !matches!(
                existing.status.as_str(),
                ExistingObjectReplicationStatus::ENABLED | ExistingObjectReplicationStatus::DISABLED
            )
        }) {
            return Some("Rule.ExistingObjectReplication.Status");
        }
        if rule.delete_replication.as_ref().is_some_and(|delete| {
            !matches!(
                delete.status.as_str(),
                DeleteReplicationStatus::ENABLED | DeleteReplicationStatus::DISABLED
            )
        }) {
            return Some("Rule.DeleteReplication.Status");
        }
        if rule
            .delete_marker_replication
            .as_ref()
            .and_then(|delete| delete.status.as_ref())
            .is_some_and(|status| {
                !matches!(
                    status.as_str(),
                    DeleteMarkerReplicationStatus::ENABLED | DeleteMarkerReplicationStatus::DISABLED
                )
            })
        {
            return Some("Rule.DeleteMarkerReplication.Status");
        }
        if rule
            .source_selection_criteria
            .as_ref()
            .and_then(|criteria| criteria.replica_modifications.as_ref())
            .is_some_and(|modifications| {
                !matches!(
                    modifications.status.as_str(),
                    ReplicaModificationsStatus::ENABLED | ReplicaModificationsStatus::DISABLED
                )
            })
        {
            return Some("Rule.SourceSelectionCriteria.ReplicaModifications.Status");
        }
    }

    None
}

pub fn active_replication_rule_destination_arns(config: &ReplicationConfiguration) -> HashSet<String> {
    let mut arns = HashSet::new();

    for rule in &config.rules {
        if rule.status == ReplicationRuleStatus::from_static(ReplicationRuleStatus::DISABLED) {
            continue;
        }

        let arn = rule.destination.bucket.trim();
        if !arn.is_empty() {
            arns.insert(arn.to_string());
        }
    }

    arns
}

/// Deployment id extracted from a site-replication target ARN
/// (`arn:{rustfs|minio}:replication::<deployment-id>:<bucket>`), or `None`
/// for an operator-authored ARN.
pub fn replication_target_arn_deployment_id(arn: &str) -> Option<String> {
    let parts: Vec<_> = arn.split(':').collect();
    if parts.len() == 6
        && parts[0] == "arn"
        && matches!(parts[1], "rustfs" | "minio")
        && parts[2] == "replication"
        && !parts[4].is_empty()
    {
        return Some(parts[4].to_string());
    }

    None
}

/// Rule id prefix the site-replication reconciler stamps on the rules it
/// derives (`site-repl-<peer deployment id>`).
pub const SITE_REPLICATION_RULE_ID_PREFIX: &str = "site-repl-";

/// Whether `rule` carries a site-replication rule id (`site-repl-*`). Rule
/// ids are not reserved, so this is only the classification of
/// [`OperatorRuleContract::Legacy`]; every other path classifies by
/// [`site_replication_rule_deployment_id`].
pub fn is_site_replication_rule(rule: &ReplicationRule) -> bool {
    rule.id
        .as_deref()
        .is_some_and(|id| id.starts_with(SITE_REPLICATION_RULE_ID_PREFIX))
}

/// Deployment id of the peer a reconciler-derived rule replicates to, or
/// `None` for any other rule. The reconciler builds each rule from one peer:
/// the id is `site-repl-<deployment id>` and the destination ARN names that
/// same deployment id — an operator-authored `site-repl-user` rule, or a
/// `site-repl-<peer>` id pasted onto a foreign ARN, fails the agreement check.
/// Callers that know the current peer set must also confirm the id is one of
/// those peers before treating the rule as reconciler-owned.
pub fn site_replication_rule_deployment_id(rule: &ReplicationRule) -> Option<&str> {
    let deployment_id = rule.id.as_deref()?.strip_prefix(SITE_REPLICATION_RULE_ID_PREFIX)?;
    (!deployment_id.is_empty()
        && replication_target_arn_deployment_id(&rule.destination.bucket).as_deref() == Some(deployment_id))
    .then_some(deployment_id)
}

/// Whether `rule` is one the local reconciler derived for a current remote
/// site-replication peer in `peer_deployment_ids`. With an empty peer set
/// (site replication disabled) nothing qualifies, so a bucket outside site
/// replication keeps the verbatim S3 put/delete semantics.
pub fn is_reconciler_owned_site_replication_rule(rule: &ReplicationRule, peer_deployment_ids: &HashSet<String>) -> bool {
    site_replication_rule_deployment_id(rule).is_some_and(|deployment_id| peer_deployment_ids.contains(deployment_id))
}

/// Whether a config's `Role` is a site-replication ARN naming a site in
/// `deployment_ids`. Such a role is the holder's identity, not policy: the
/// reconciler's per-peer target lookup reads it, so carrying it across sites
/// would pin the receiver's targets to the sender's. Any other role — an IAM
/// role, or an operator remote target whose ARN happens to carry an empty
/// region — passed target validation and drives target selection.
pub fn is_site_replication_role(role: &str, deployment_ids: &HashSet<String>) -> bool {
    replication_target_arn_deployment_id(role).is_some_and(|deployment_id| deployment_ids.contains(&deployment_id))
}

/// How the sites of a cluster treat the operator rules of a replication
/// config merge. Every site must apply the same contract to the same
/// payload or the sites persist different configs, so the S3 edit path
/// probes the peers before merging and a peer payload carries the contract
/// its sender applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorRuleContract {
    /// Site rules are the derived id/ARN shape; operator rule priorities are
    /// kept verbatim.
    Derived,
    /// Some site still runs the pre-contract code: every `site-repl-*` id is
    /// a site rule, a site-replication-shaped `Role` is dropped, and every
    /// rule is renumbered 1..n in list order on ingest and on each reconciler
    /// pass. Merging the same way keeps a mixed cluster on one config; the
    /// operator's priority values are lost for that edit but their order —
    /// what decides the winning rule per target — is not, because the S3
    /// merge lists the operator rules in priority order first.
    Legacy,
}

/// Merge a peer's replication config into the local one.
///
/// Reconciler-derived rules encode the *holder's* outbound direction — their
/// destination ARN names another site — so applying an external rule set
/// verbatim replaces the local reverse rule with one this site can never
/// satisfy (no bucket target backs it) and replication silently stops. Only
/// operator-authored rules travel: the sender's derived rules are dropped
/// and the local site's survive. `site_deployment_ids` is every site of the
/// cluster, the receiver included — the sender's rule towards the receiver
/// names the receiver's own id. Rules are classified by the derived id/ARN
/// contract ([`is_reconciler_owned_site_replication_rule`]), the same one
/// the S3 edit merge applies, so an operator-authored `site-repl-*` id
/// persists on every site. `incoming == None` models a delete of the
/// operator-authored rules.
pub fn merge_incoming_replication_config(
    incoming: Option<ReplicationConfiguration>,
    local: Option<ReplicationConfiguration>,
    site_deployment_ids: &HashSet<String>,
    contract: OperatorRuleContract,
) -> Option<ReplicationConfiguration> {
    merge_replication_config_keeping_site_rules(incoming, local, site_deployment_ids, contract)
}

/// [`merge_incoming_replication_config`] for the S3 put/delete-bucket-replication
/// path (issue #1948): only rules the local reconciler derived for a current
/// peer in `peer_deployment_ids` survive as site rules; every other stored
/// rule — including an operator-authored `site-repl-*` id — is operator state
/// that the request replaces or deletes. An incoming rule whose id is a
/// current peer's `site-repl-<id>` is dropped whatever its ARN: accepting it
/// would duplicate the reconciler rule's id. Under
/// [`OperatorRuleContract::Legacy`] the merge instead reproduces what the
/// pre-contract peers will do with the broadcast, listing the operator rules
/// in priority order so their relative order survives the renumbering.
pub fn merge_user_replication_config(
    incoming: Option<ReplicationConfiguration>,
    local: Option<ReplicationConfiguration>,
    peer_deployment_ids: &HashSet<String>,
    contract: OperatorRuleContract,
) -> Option<ReplicationConfiguration> {
    let incoming = incoming.map(|mut config| {
        match contract {
            OperatorRuleContract::Derived => config.rules.retain(|rule| {
                !rule
                    .id
                    .as_deref()
                    .and_then(|id| id.strip_prefix(SITE_REPLICATION_RULE_ID_PREFIX))
                    .is_some_and(|deployment_id| peer_deployment_ids.contains(deployment_id))
            }),
            // Pre-contract peers renumber in list order, so listing the
            // operator rules in priority order keeps their relative order —
            // and the replication decision — through that renumbering.
            OperatorRuleContract::Legacy => config.rules.sort_by_key(|rule| rule.priority.unwrap_or(0)),
        }
        config
    });
    merge_replication_config_keeping_site_rules(incoming, local, peer_deployment_ids, contract)
}

fn merge_replication_config_keeping_site_rules(
    incoming: Option<ReplicationConfiguration>,
    local: Option<ReplicationConfiguration>,
    deployment_ids: &HashSet<String>,
    contract: OperatorRuleContract,
) -> Option<ReplicationConfiguration> {
    let is_site_rule = |rule: &ReplicationRule| match contract {
        OperatorRuleContract::Derived => is_reconciler_owned_site_replication_rule(rule, deployment_ids),
        OperatorRuleContract::Legacy => is_site_replication_rule(rule),
    };
    let incoming_role = incoming.as_ref().map(|config| config.role.clone()).unwrap_or_default();
    // Operator rules first, then the local site rules — the same order the
    // site-replication reconciler produces, so its no-op check matches and
    // the bucket metadata is written once per broadcast, not twice.
    let mut rules: Vec<ReplicationRule> = incoming
        .into_iter()
        .flat_map(|config| config.rules)
        .filter(|rule| !is_site_rule(rule))
        .collect();
    rules.extend(
        local
            .into_iter()
            .flat_map(|config| config.rules)
            .filter(|rule| is_site_rule(rule)),
    );

    if rules.is_empty() {
        return None;
    }

    let drop_role = match contract {
        OperatorRuleContract::Derived => {
            assign_site_replication_rule_priorities(&mut rules, is_site_rule);
            is_site_replication_role(&incoming_role, deployment_ids)
        }
        OperatorRuleContract::Legacy => {
            for (index, rule) in rules.iter_mut().enumerate() {
                rule.priority = Some(i32::try_from(index + 1).unwrap_or(i32::MAX));
            }
            replication_target_arn_deployment_id(&incoming_role).is_some()
        }
    };
    let role = if drop_role { String::new() } else { incoming_role };

    Some(ReplicationConfiguration { role, rules })
}

/// Give the site rules in `rules` the lowest priorities no operator rule uses,
/// in rule order, leaving every operator rule's priority untouched. Operator
/// priorities decide which rule wins per target, so they are part of the
/// submitted policy; site rules are derived state and only need to be unique
/// (`validate_replication_config_structure` rejects duplicates). The result
/// is a pure function of the rule list, so the site-replication reconciler,
/// the peer ingestion merge and the S3 edit merge all converge on the same
/// bytes and the reconciler's no-op check holds.
pub fn assign_site_replication_rule_priorities(rules: &mut [ReplicationRule], is_site_rule: impl Fn(&ReplicationRule) -> bool) {
    let taken: HashSet<i32> = rules
        .iter()
        .filter(|rule| !is_site_rule(rule))
        .map(|rule| rule.priority.unwrap_or(0))
        .collect();
    let mut next = 1;
    for rule in rules.iter_mut().filter(|rule| is_site_rule(rule)) {
        while taken.contains(&next) {
            next += 1;
        }
        rule.priority = Some(next);
        next = next.saturating_add(1);
    }
}

pub fn replication_target_arns(config: &ReplicationConfiguration) -> HashSet<String> {
    let role = config.role.trim();
    if !role.is_empty() {
        return HashSet::from([role.to_string()]);
    }

    active_replication_rule_destination_arns(config)
}

pub fn validate_replication_config_target_arns<'a>(
    configured_arns: impl IntoIterator<Item = &'a str>,
    config: &ReplicationConfiguration,
) -> std::result::Result<(), ReplicationTargetValidationError> {
    let configured_arns = configured_arns.into_iter().collect::<HashSet<_>>();

    let role = config.role.trim();
    let destination_arns = active_replication_rule_destination_arns(config);
    if !role.is_empty() && destination_arns.len() > 1 {
        return Err(ReplicationTargetValidationError::RoleWithMultipleDestinations);
    }

    for configured_arn in replication_target_arns(config) {
        if !configured_arns.contains(configured_arn.as_str()) {
            return Err(ReplicationTargetValidationError::StaleTarget);
        }
    }

    Ok(())
}

pub fn should_remove_replication_target(
    target_arn: &str,
    is_replication_target: bool,
    config_target_arns: &HashSet<String>,
) -> bool {
    is_replication_target && config_target_arns.contains(target_arn)
}

/// Maximum number of rules accepted in one replication configuration,
/// matching MinIO's `replication.Config.Validate` limit.
pub const REPLICATION_CONFIG_MAX_RULES: usize = 1000;

/// Maximum length of a replication rule ID, matching the S3 schema.
pub const REPLICATION_CONFIG_MAX_RULE_ID_LEN: usize = 255;

/// A structural defect in a replication configuration, detected before the
/// configuration is persisted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationConfigStructureError {
    NoRules,
    TooManyRules,
    NegativeRulePriority,
    DuplicateRulePriority,
    RuleIdTooLong,
    AmbiguousRuleFilter,
    TagFilterWithDeleteMarkerReplication,
}

impl ReplicationConfigStructureError {
    pub fn message(self) -> &'static str {
        match self {
            Self::NoRules => "replication configuration must contain at least one rule",
            Self::TooManyRules => "replication configuration cannot contain more than 1000 rules",
            Self::NegativeRulePriority => "replication rule Priority must be zero or a positive integer",
            Self::DuplicateRulePriority => "replication rule Priority must be unique across rules",
            Self::RuleIdTooLong => "replication rule ID cannot be longer than 255 characters",
            Self::AmbiguousRuleFilter => "replication rule Filter must specify only one of Prefix, Tag or And",
            Self::TagFilterWithDeleteMarkerReplication => {
                "delete marker replication cannot be enabled on a rule with a Tag filter"
            }
        }
    }
}

fn filter_and_operator_is_set(and: &s3s::dto::ReplicationRuleAndOperator) -> bool {
    and.prefix.as_ref().is_some_and(|prefix| !prefix.is_empty()) || and.tags.as_ref().is_some_and(|tags| !tags.is_empty())
}

/// Structural validation of a replication configuration, mirroring the checks
/// MinIO's `replication.Config.Validate` performs before persisting: at least
/// one rule, at most [`REPLICATION_CONFIG_MAX_RULES`], non-negative and unique
/// per-rule priorities (a missing Priority counts as 0, like Go's zero value),
/// rule IDs within [`REPLICATION_CONFIG_MAX_RULE_ID_LEN`] bytes, a Filter
/// carrying only one of Prefix/Tag/And, and delete marker replication
/// disabled on rules with a direct `Filter.Tag`. Tags inside `Filter.And` do
/// NOT trigger the delete-marker check — MinIO only inspects the direct tag,
/// and `mc replicate add --tags "k1=v1&k2=v2"` (delete-marker replication on
/// by default) puts multiple tags into `And.Tags`, so rejecting that shape
/// would break mc-generated configs that MinIO accepts.
///
/// This is shape-only validation: capability gating lives in
/// [`unsupported_replication_config_field`]/[`invalid_replication_config_status_field`],
/// and the self-target ("same target") rejection is enforced when the remote
/// target itself is created, so a config can never reference a self-pointing
/// ARN.
pub fn validate_replication_config_structure(
    config: &ReplicationConfiguration,
) -> std::result::Result<(), ReplicationConfigStructureError> {
    if config.rules.is_empty() {
        return Err(ReplicationConfigStructureError::NoRules);
    }
    if config.rules.len() > REPLICATION_CONFIG_MAX_RULES {
        return Err(ReplicationConfigStructureError::TooManyRules);
    }

    let mut priorities = HashSet::new();
    for rule in &config.rules {
        let priority = rule.priority.unwrap_or(0);
        if priority < 0 {
            return Err(ReplicationConfigStructureError::NegativeRulePriority);
        }
        if !priorities.insert(priority) {
            return Err(ReplicationConfigStructureError::DuplicateRulePriority);
        }

        // Byte length, matching Go's `len(r.ID) > 255` in MinIO.
        if rule
            .id
            .as_ref()
            .is_some_and(|id| id.len() > REPLICATION_CONFIG_MAX_RULE_ID_LEN)
        {
            return Err(ReplicationConfigStructureError::RuleIdTooLong);
        }

        if let Some(filter) = &rule.filter {
            let has_and = filter.and.as_ref().is_some_and(filter_and_operator_is_set);
            let has_prefix = filter.prefix.as_ref().is_some_and(|prefix| !prefix.is_empty());
            // An empty <Tag/> element (no key) counts as absent, matching
            // MinIO's Tag.IsEmpty(); console form serializers emit empty tags.
            let has_tag = filter
                .tag
                .as_ref()
                .is_some_and(|tag| tag.key.as_ref().is_some_and(|key| !key.is_empty()));
            if usize::from(has_and) + usize::from(has_prefix) + usize::from(has_tag) > 1 {
                return Err(ReplicationConfigStructureError::AmbiguousRuleFilter);
            }

            let delete_marker_replication_enabled = rule
                .delete_marker_replication
                .as_ref()
                .and_then(|delete_marker| delete_marker.status.as_ref())
                .is_some_and(|status| status.as_str() == DeleteMarkerReplicationStatus::ENABLED);
            if delete_marker_replication_enabled && has_tag {
                return Err(ReplicationConfigStructureError::TagFilterWithDeleteMarkerReplication);
            }
        }
    }

    Ok(())
}

impl ReplicationConfigurationExt for ReplicationConfiguration {
    /// Check whether any object-replication rules exist
    fn has_existing_object_replication(&self, arn: &str) -> (bool, bool) {
        let mut has_arn = false;
        let arn = arn.trim();

        for rule in &self.rules {
            if rule.destination.bucket.trim() == arn || self.role.trim() == arn {
                if !has_arn {
                    has_arn = true;
                }
                if let Some(status) = &rule.existing_object_replication
                    && status.status == ExistingObjectReplicationStatus::from_static(ExistingObjectReplicationStatus::ENABLED)
                {
                    return (true, true);
                }
            }
        }
        (has_arn, false)
    }

    fn filter_actionable_rules(&self, obj: &ObjectOpts) -> ReplicationRules {
        if obj.name.is_empty() && obj.op_type != ReplicationType::Resync && obj.op_type != ReplicationType::All {
            return vec![];
        }

        let mut rules = ReplicationRules::default();

        for rule in &self.rules {
            if rule.status == ReplicationRuleStatus::from_static(ReplicationRuleStatus::DISABLED) {
                continue;
            }

            if !obj.target_arn.is_empty()
                && rule.destination.bucket.trim() != obj.target_arn.trim()
                && self.role.trim() != obj.target_arn.trim()
            {
                continue;
            }

            if obj.op_type == ReplicationType::Resync || obj.op_type == ReplicationType::All {
                rules.push(rule.clone());
                continue;
            }

            if let Some(status) = &rule.existing_object_replication
                && obj.existing_object
                && status.status == ExistingObjectReplicationStatus::from_static(ExistingObjectReplicationStatus::DISABLED)
            {
                continue;
            }

            if !obj.name.starts_with(rule.prefix()) {
                continue;
            }

            if let Some(filter) = &rule.filter {
                let object_tags = ReplicationTagFilter::decode_tags_to_map(&obj.user_tags);
                if filter.test_tags(&object_tags) {
                    rules.push(rule.clone());
                }
            } else {
                rules.push(rule.clone());
            }
        }

        rules.sort_by(|a, b| {
            if a.destination == b.destination {
                b.priority.cmp(&a.priority)
            } else {
                std::cmp::Ordering::Equal
            }
        });

        rules
    }

    /// Retrieve the destination configuration
    fn get_destination(&self) -> Destination {
        if !self.rules.is_empty() {
            self.rules[0].destination.clone()
        } else {
            Destination {
                bucket: String::new(),
                ..Default::default()
            }
        }
    }

    /// Determine whether an object should be replicated
    fn replicate(&self, obj: &ObjectOpts) -> bool {
        let rules = self.filter_actionable_rules(obj);
        rules.first().is_some_and(|rule| rule_replicates(rule, obj))
    }

    /// Check for an active rule
    /// Optionally accept a prefix
    /// When recursive is true, return true if any level under the prefix has an active rule
    /// Without a prefix, recursive behaves as true
    fn has_active_rules(&self, prefix: &str, recursive: bool) -> bool {
        if self.rules.is_empty() {
            return false;
        }

        for rule in &self.rules {
            if rule.status == ReplicationRuleStatus::from_static(ReplicationRuleStatus::DISABLED) {
                continue;
            }

            if let Some(filter) = &rule.filter
                && let Some(filter_prefix) = &filter.prefix
            {
                if !prefix.is_empty() && !filter_prefix.is_empty() {
                    // The provided prefix must fall within the rule prefix
                    if !recursive && !prefix.starts_with(filter_prefix) {
                        continue;
                    }
                }

                // When recursive, skip this rule if it does not match the test prefix or hierarchy
                if recursive && !rule.prefix().starts_with(prefix) && !prefix.starts_with(rule.prefix()) {
                    continue;
                }
            }
            return true;
        }
        false
    }

    /// Filter target ARNs and return a slice of the distinct values in the config
    fn filter_target_arns(&self, obj: &ObjectOpts) -> Vec<String> {
        let role = self.role.trim();
        if !role.is_empty() {
            return vec![role.to_string()];
        }

        let mut arns = Vec::new();
        let mut targets_map: HashSet<String> = HashSet::new();
        let rules = self.filter_actionable_rules(obj);

        for rule in rules {
            if rule.status == ReplicationRuleStatus::from_static(ReplicationRuleStatus::DISABLED) {
                continue;
            }

            let arn = rule.destination.bucket.trim();
            if !arn.is_empty() && !targets_map.contains(arn) {
                targets_map.insert(arn.to_string());
            }
        }

        for arn in targets_map {
            arns.push(arn);
        }
        arns
    }

    fn filter_force_delete_target_arns(&self, prefix: &str) -> Vec<String> {
        let role = self.role.trim();
        let mut selected = BTreeMap::<String, (&ReplicationRule, bool)>::new();

        for rule in &self.rules {
            if rule.status == ReplicationRuleStatus::from_static(ReplicationRuleStatus::DISABLED) {
                continue;
            }

            let rule_prefix = rule.prefix();
            if !prefix.starts_with(rule_prefix) && !rule_prefix.starts_with(prefix) {
                continue;
            }

            let target = if role.is_empty() {
                rule.destination.bucket.trim()
            } else {
                role
            };
            if target.is_empty() {
                continue;
            }

            let delete_enabled =
                rule.delete_replication.as_ref().is_some_and(|delete| {
                    delete.status == DeleteReplicationStatus::from_static(DeleteReplicationStatus::ENABLED)
                }) || rule.delete_marker_replication.as_ref().is_some_and(|delete_marker| {
                    delete_marker.status
                        == Some(DeleteMarkerReplicationStatus::from_static(DeleteMarkerReplicationStatus::ENABLED))
                });

            if selected
                .get(target)
                .is_none_or(|(current, _)| rule.priority > current.priority)
            {
                selected.insert(target.to_string(), (rule, delete_enabled));
            }
        }

        selected
            .into_iter()
            .filter_map(|(target, (_, enabled))| enabled.then_some(target))
            .collect()
    }

    fn filter_target_replication_decisions(&self, obj: &ObjectOpts) -> Vec<(String, bool)> {
        let rules = self.filter_actionable_rules(obj);
        let role = self.role.trim();
        if !role.is_empty() {
            let mut selected = None;
            for rule in &rules {
                if selected.is_none_or(|current: &ReplicationRule| rule.priority > current.priority) {
                    selected = Some(rule);
                }
            }
            return vec![(role.to_string(), selected.is_some_and(|rule| rule_replicates(rule, obj)))];
        }

        let mut target_indexes: HashMap<&str, usize> = HashMap::new();
        let mut selected_rules: Vec<(&str, &ReplicationRule)> = Vec::new();
        for rule in &rules {
            let arn = rule.destination.bucket.trim();
            if arn.is_empty() {
                continue;
            }
            if let Some(index) = target_indexes.get(arn).copied() {
                if rule.priority > selected_rules[index].1.priority {
                    selected_rules[index].1 = rule;
                }
            } else {
                target_indexes.insert(arn, selected_rules.len());
                selected_rules.push((arn, rule));
            }
        }
        selected_rules
            .into_iter()
            .map(|(arn, rule)| (arn.to_string(), rule_replicates(rule, obj)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use s3s::dto::{
        DeleteMarkerReplication, DeleteReplication, Destination, EncryptionConfiguration, ExistingObjectReplication, Metrics,
        MetricsStatus, ReplicaModifications, ReplicationRule, ReplicationTime, ReplicationTimeStatus, ReplicationTimeValue,
        SourceSelectionCriteria, SseKmsEncryptedObjects, SseKmsEncryptedObjectsStatus,
    };
    use s3s::xml::{Deserializer, Serializer};

    fn replication_rule(id: &str, arn: &str) -> ReplicationRule {
        ReplicationRule {
            delete_marker_replication: Some(DeleteMarkerReplication::default()),
            delete_replication: None,
            destination: Destination {
                bucket: arn.to_string(),
                ..Default::default()
            },
            existing_object_replication: Some(ExistingObjectReplication {
                status: ExistingObjectReplicationStatus::from_static(ExistingObjectReplicationStatus::ENABLED),
            }),
            filter: None,
            id: Some(id.to_string()),
            prefix: Some(String::new()),
            priority: Some(1),
            source_selection_criteria: None,
            status: ReplicationRuleStatus::from_static(ReplicationRuleStatus::ENABLED),
        }
    }

    fn structure_config(rules: Vec<ReplicationRule>) -> ReplicationConfiguration {
        ReplicationConfiguration {
            role: String::new(),
            rules,
        }
    }

    fn tag_filter() -> s3s::dto::ReplicationRuleFilter {
        s3s::dto::ReplicationRuleFilter {
            tag: Some(s3s::dto::Tag {
                key: Some("k".to_string()),
                value: Some("v".to_string()),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn structure_validation_accepts_multi_rule_config_with_unique_priorities() {
        let mut second = replication_rule("rule-2", "arn:target:a");
        second.priority = Some(2);
        second.filter = Some(s3s::dto::ReplicationRuleFilter {
            and: Some(s3s::dto::ReplicationRuleAndOperator {
                prefix: Some("photos/".to_string()),
                tags: Some(vec![s3s::dto::Tag {
                    key: Some("k".to_string()),
                    value: Some("v".to_string()),
                }]),
            }),
            ..Default::default()
        });
        let config = structure_config(vec![replication_rule("rule-1", "arn:target:a"), second]);

        assert_eq!(validate_replication_config_structure(&config), Ok(()));
    }

    #[test]
    fn structure_validation_rejects_empty_rule_list() {
        let config = structure_config(Vec::new());

        assert_eq!(
            validate_replication_config_structure(&config),
            Err(ReplicationConfigStructureError::NoRules)
        );
    }

    #[test]
    fn structure_validation_rejects_more_than_max_rules() {
        let rules = (0..=REPLICATION_CONFIG_MAX_RULES as i32)
            .map(|priority| {
                let mut rule = replication_rule(&format!("rule-{priority}"), "arn:target:a");
                rule.priority = Some(priority);
                rule
            })
            .collect();

        assert_eq!(
            validate_replication_config_structure(&structure_config(rules)),
            Err(ReplicationConfigStructureError::TooManyRules)
        );
    }

    #[test]
    fn structure_validation_rejects_duplicate_priorities() {
        let config = structure_config(vec![
            replication_rule("rule-1", "arn:target:a"),
            replication_rule("rule-2", "arn:target:a"),
        ]);

        assert_eq!(
            validate_replication_config_structure(&config),
            Err(ReplicationConfigStructureError::DuplicateRulePriority)
        );
    }

    #[test]
    fn structure_validation_treats_missing_priority_as_zero_for_uniqueness() {
        let mut first = replication_rule("rule-1", "arn:target:a");
        first.priority = None;
        let mut second = replication_rule("rule-2", "arn:target:a");
        second.priority = None;

        assert_eq!(
            validate_replication_config_structure(&structure_config(vec![first, second])),
            Err(ReplicationConfigStructureError::DuplicateRulePriority)
        );
    }

    #[test]
    fn structure_validation_rejects_negative_priority() {
        let mut rule = replication_rule("rule-1", "arn:target:a");
        rule.priority = Some(-1);

        assert_eq!(
            validate_replication_config_structure(&structure_config(vec![rule])),
            Err(ReplicationConfigStructureError::NegativeRulePriority)
        );
    }

    #[test]
    fn structure_validation_rejects_rule_id_longer_than_255_chars() {
        let mut rule = replication_rule(&"x".repeat(REPLICATION_CONFIG_MAX_RULE_ID_LEN + 1), "arn:target:a");
        rule.priority = Some(1);

        assert_eq!(
            validate_replication_config_structure(&structure_config(vec![rule])),
            Err(ReplicationConfigStructureError::RuleIdTooLong)
        );
    }

    #[test]
    fn structure_validation_counts_rule_id_limit_in_bytes() {
        let mut within_byte_limit = replication_rule(&"\u{00e9}".repeat(127), "arn:target:a");
        within_byte_limit.priority = Some(1);
        assert_eq!(
            within_byte_limit.id.as_ref().expect("rule id should be present").len(),
            REPLICATION_CONFIG_MAX_RULE_ID_LEN - 1
        );
        assert_eq!(validate_replication_config_structure(&structure_config(vec![within_byte_limit])), Ok(()));

        let mut over_byte_limit = replication_rule(&"\u{00e9}".repeat(128), "arn:target:a");
        over_byte_limit.priority = Some(1);
        assert!(
            over_byte_limit
                .id
                .as_ref()
                .expect("rule id should be present")
                .chars()
                .count()
                < REPLICATION_CONFIG_MAX_RULE_ID_LEN
        );
        assert_eq!(
            validate_replication_config_structure(&structure_config(vec![over_byte_limit])),
            Err(ReplicationConfigStructureError::RuleIdTooLong)
        );
    }

    #[test]
    fn structure_validation_rejects_filter_with_both_prefix_and_tag() {
        let mut rule = replication_rule("rule-1", "arn:target:a");
        let mut filter = tag_filter();
        filter.prefix = Some("photos/".to_string());
        rule.filter = Some(filter);

        assert_eq!(
            validate_replication_config_structure(&structure_config(vec![rule])),
            Err(ReplicationConfigStructureError::AmbiguousRuleFilter)
        );
    }

    #[test]
    fn structure_validation_rejects_delete_marker_replication_on_tag_filtered_rule() {
        let mut rule = replication_rule("rule-1", "arn:target:a");
        rule.delete_marker_replication = Some(DeleteMarkerReplication {
            status: Some(DeleteMarkerReplicationStatus::from_static(DeleteMarkerReplicationStatus::ENABLED)),
        });
        rule.filter = Some(tag_filter());

        assert_eq!(
            validate_replication_config_structure(&structure_config(vec![rule])),
            Err(ReplicationConfigStructureError::TagFilterWithDeleteMarkerReplication)
        );
    }

    #[test]
    fn structure_validation_treats_empty_tag_element_as_absent() {
        // MinIO's Tag.IsEmpty() ignores an empty <Tag/> element; the console's
        // form serializer emits them, so prefix + empty tag must stay valid
        // and an empty tag must not trip the delete-marker check.
        let mut rule = replication_rule("rule-1", "arn:target:a");
        rule.delete_marker_replication = Some(DeleteMarkerReplication {
            status: Some(DeleteMarkerReplicationStatus::from_static(DeleteMarkerReplicationStatus::ENABLED)),
        });
        rule.filter = Some(s3s::dto::ReplicationRuleFilter {
            prefix: Some("photos/".to_string()),
            tag: Some(s3s::dto::Tag { key: None, value: None }),
            ..Default::default()
        });

        assert_eq!(validate_replication_config_structure(&structure_config(vec![rule])), Ok(()));
    }

    #[test]
    fn structure_validation_allows_delete_marker_replication_with_and_tags() {
        // mc `replicate add --tags "k1=v1&k2=v2"` puts multiple tags into
        // Filter.And.Tags and enables delete-marker replication by default;
        // MinIO's validator only inspects the direct Filter.Tag, so this
        // shape must stay accepted for mc interop.
        let mut rule = replication_rule("rule-1", "arn:target:a");
        rule.delete_marker_replication = Some(DeleteMarkerReplication {
            status: Some(DeleteMarkerReplicationStatus::from_static(DeleteMarkerReplicationStatus::ENABLED)),
        });
        rule.filter = Some(s3s::dto::ReplicationRuleFilter {
            and: Some(s3s::dto::ReplicationRuleAndOperator {
                prefix: None,
                tags: Some(vec![
                    s3s::dto::Tag {
                        key: Some("k1".to_string()),
                        value: Some("v1".to_string()),
                    },
                    s3s::dto::Tag {
                        key: Some("k2".to_string()),
                        value: Some("v2".to_string()),
                    },
                ]),
            }),
            ..Default::default()
        });

        assert_eq!(validate_replication_config_structure(&structure_config(vec![rule])), Ok(()));
    }

    #[test]
    fn structure_validation_allows_tag_filter_when_delete_marker_replication_disabled() {
        let mut rule = replication_rule("rule-1", "arn:target:a");
        rule.delete_marker_replication = Some(DeleteMarkerReplication {
            status: Some(DeleteMarkerReplicationStatus::from_static(DeleteMarkerReplicationStatus::DISABLED)),
        });
        rule.filter = Some(tag_filter());

        assert_eq!(validate_replication_config_structure(&structure_config(vec![rule])), Ok(()));
    }

    #[test]
    fn filter_target_arns_uses_role_when_role_is_present() {
        let config = ReplicationConfiguration {
            role: " arn:legacy:target ".to_string(),
            rules: vec![
                replication_rule("rule-1", "arn:target:a"),
                replication_rule("rule-2", "arn:target:b"),
            ],
        };

        let arns = config.filter_target_arns(&ObjectOpts {
            name: "object".to_string(),
            op_type: ReplicationType::Object,
            ..Default::default()
        });

        assert_eq!(arns, vec!["arn:legacy:target".to_string()]);
    }

    #[test]
    fn filter_target_arns_falls_back_to_role_when_destination_is_empty() {
        let config = ReplicationConfiguration {
            role: "arn:legacy:target".to_string(),
            rules: vec![ReplicationRule {
                delete_marker_replication: Some(DeleteMarkerReplication::default()),
                delete_replication: None,
                destination: Destination {
                    bucket: String::new(),
                    ..Default::default()
                },
                existing_object_replication: Some(ExistingObjectReplication {
                    status: ExistingObjectReplicationStatus::from_static(ExistingObjectReplicationStatus::ENABLED),
                }),
                filter: None,
                id: Some("rule-1".to_string()),
                prefix: Some(String::new()),
                priority: Some(1),
                source_selection_criteria: None,
                status: ReplicationRuleStatus::from_static(ReplicationRuleStatus::ENABLED),
            }],
        };

        let arns = config.filter_target_arns(&ObjectOpts {
            name: "object".to_string(),
            op_type: ReplicationType::Object,
            ..Default::default()
        });

        assert_eq!(arns, vec!["arn:legacy:target".to_string()]);
    }

    fn replication_rule_existing_object_disabled(id: &str, arn: &str) -> ReplicationRule {
        ReplicationRule {
            delete_marker_replication: Some(DeleteMarkerReplication::default()),
            delete_replication: None,
            destination: Destination {
                bucket: arn.to_string(),
                ..Default::default()
            },
            existing_object_replication: Some(ExistingObjectReplication {
                status: ExistingObjectReplicationStatus::from_static(ExistingObjectReplicationStatus::DISABLED),
            }),
            filter: None,
            id: Some(id.to_string()),
            prefix: Some(String::new()),
            priority: Some(1),
            source_selection_criteria: None,
            status: ReplicationRuleStatus::from_static(ReplicationRuleStatus::ENABLED),
        }
    }

    // Regression test for BUG-3: replicate_object was calling filter_target_arns with
    // existing_object:false regardless of op_type, letting ExistingObject resync operations
    // fan out to targets whose rule has ExistingObjectReplicationStatus::DISABLED.
    #[test]
    fn filter_target_arns_excludes_disabled_existing_object_target_for_existing_object_op() {
        let config = ReplicationConfiguration {
            role: String::new(),
            rules: vec![
                replication_rule("rule-enabled", "arn:target:enabled"),
                replication_rule_existing_object_disabled("rule-disabled", "arn:target:disabled"),
            ],
        };

        let arns = config.filter_target_arns(&ObjectOpts {
            name: "object".to_string(),
            op_type: ReplicationType::ExistingObject,
            existing_object: true,
            ..Default::default()
        });

        assert_eq!(arns.len(), 1, "only the ENABLED target should be returned for ExistingObject ops");
        assert!(arns.contains(&"arn:target:enabled".to_string()));
        assert!(!arns.contains(&"arn:target:disabled".to_string()));
    }

    // Heal operations intentionally bypass ExistingObjectReplicationStatus — healing a past
    // failure is not subject to the existing-object opt-out.
    #[test]
    fn filter_target_arns_includes_disabled_existing_object_target_for_heal_op() {
        let config = ReplicationConfiguration {
            role: String::new(),
            rules: vec![
                replication_rule("rule-enabled", "arn:target:enabled"),
                replication_rule_existing_object_disabled("rule-disabled", "arn:target:disabled"),
            ],
        };

        let arns = config.filter_target_arns(&ObjectOpts {
            name: "object".to_string(),
            op_type: ReplicationType::Heal,
            existing_object: false,
            ..Default::default()
        });

        assert_eq!(
            arns.len(),
            2,
            "Heal ops must reach all targets regardless of existing_object_replication setting"
        );
        assert!(arns.contains(&"arn:target:enabled".to_string()));
        assert!(arns.contains(&"arn:target:disabled".to_string()));
    }

    #[test]
    fn replication_target_arns_use_role_when_present() {
        let role = "arn:rustfs:replication:us-east-1:source:bucket";
        let destination = "arn:rustfs:replication:us-east-1:target:bucket";
        let config = ReplicationConfiguration {
            role: format!(" {role} "),
            rules: vec![replication_rule("rule-1", destination)],
        };

        let arns = replication_target_arns(&config);

        assert!(arns.contains(role));
        assert!(!arns.contains(destination));
    }

    #[test]
    fn replication_target_arns_use_rule_destinations_without_role() {
        let destination = "arn:rustfs:replication:us-east-1:target:bucket";
        let config = ReplicationConfiguration {
            role: String::new(),
            rules: vec![replication_rule("rule-1", destination)],
        };

        let arns = replication_target_arns(&config);

        assert!(arns.contains(destination));
    }

    #[test]
    fn validate_replication_config_target_arns_accepts_matching_destination_arns() {
        let arn = "arn:rustfs:replication:us-east-1:target:bucket";
        let config = ReplicationConfiguration {
            role: String::new(),
            rules: vec![replication_rule("rule-1", arn)],
        };

        validate_replication_config_target_arns([arn], &config).expect("matching target should pass validation");
    }

    #[test]
    fn validate_replication_config_target_arns_rejects_stale_destination_arns() {
        let config = ReplicationConfiguration {
            role: String::new(),
            rules: vec![replication_rule("rule-1", "arn:rustfs:replication:us-east-1:target-b:bucket")],
        };

        let err = validate_replication_config_target_arns(["arn:rustfs:replication:us-east-1:target-a:bucket"], &config)
            .expect_err("stale target should fail validation");
        assert_eq!(err, ReplicationTargetValidationError::StaleTarget);
    }

    #[test]
    fn validate_replication_config_target_arns_rejects_role_with_multiple_destinations() {
        let role = "arn:rustfs:replication:us-east-1:role-target:bucket";
        let config = ReplicationConfiguration {
            role: role.to_string(),
            rules: vec![
                replication_rule("rule-a", "arn:rustfs:replication:us-east-1:target-a:bucket"),
                replication_rule("rule-b", "arn:rustfs:replication:us-east-1:target-b:bucket"),
            ],
        };

        let err = validate_replication_config_target_arns([role], &config)
            .expect_err("role plus multiple destinations should be rejected");
        assert_eq!(err, ReplicationTargetValidationError::RoleWithMultipleDestinations);
    }

    #[test]
    fn validate_replication_config_target_arns_ignores_disabled_rules() {
        let mut rule = replication_rule("rule-1", "arn:rustfs:replication:us-east-1:stale:bucket");
        rule.status = ReplicationRuleStatus::from_static(ReplicationRuleStatus::DISABLED);
        let config = ReplicationConfiguration {
            role: String::new(),
            rules: vec![rule],
        };

        validate_replication_config_target_arns(std::iter::empty::<&str>(), &config)
            .expect("disabled rules should not require live targets");
    }

    #[test]
    fn should_remove_replication_target_only_matches_replication_target_arns() {
        let target_arns = HashSet::from(["arn:rustfs:replication:us-east-1:removed:bucket".to_string()]);

        assert!(should_remove_replication_target(
            "arn:rustfs:replication:us-east-1:removed:bucket",
            true,
            &target_arns
        ));
        assert!(!should_remove_replication_target(
            "arn:rustfs:replication:us-east-1:kept:bucket",
            true,
            &target_arns
        ));
        assert!(!should_remove_replication_target(
            "arn:rustfs:replication:us-east-1:removed:bucket",
            false,
            &target_arns
        ));
    }

    fn delete_marker_rule(id: &str, arn: &str, prefix: &str, priority: i32, delete_marker_enabled: bool) -> ReplicationRule {
        let status = if delete_marker_enabled {
            DeleteMarkerReplicationStatus::from_static(DeleteMarkerReplicationStatus::ENABLED)
        } else {
            DeleteMarkerReplicationStatus::from_static(DeleteMarkerReplicationStatus::DISABLED)
        };
        ReplicationRule {
            delete_marker_replication: Some(DeleteMarkerReplication { status: Some(status) }),
            delete_replication: None,
            destination: Destination {
                bucket: arn.to_string(),
                ..Default::default()
            },
            existing_object_replication: Some(ExistingObjectReplication {
                status: ExistingObjectReplicationStatus::from_static(ExistingObjectReplicationStatus::ENABLED),
            }),
            filter: None,
            id: Some(id.to_string()),
            prefix: Some(prefix.to_string()),
            priority: Some(priority),
            source_selection_criteria: None,
            status: ReplicationRuleStatus::from_static(ReplicationRuleStatus::ENABLED),
        }
    }

    // Regression test for backlog#1029: two ENABLED rules to the same destination with
    // overlapping prefixes must be evaluated highest-priority-first (AWS S3 / MinIO precedence).
    // The higher-priority rule disables delete-marker replication, so a delete-marker op must
    // NOT replicate; if the sort were ascending the lower-priority ENABLED rule would win.
    #[test]
    fn replicate_delete_marker_follows_highest_priority_rule() {
        let arn = "arn:rustfs:replication:us-east-1:target:bucket";
        let config = ReplicationConfiguration {
            role: String::new(),
            rules: vec![
                delete_marker_rule("low-priority-enabled", arn, "logs/", 1, true),
                delete_marker_rule("high-priority-disabled", arn, "logs/2026/", 5, false),
            ],
        };

        let opts = ObjectOpts {
            name: "logs/2026/app.log".to_string(),
            op_type: ReplicationType::Delete,
            delete_marker: true,
            version_id: None,
            ..Default::default()
        };

        assert!(
            !config.replicate(&opts),
            "highest-priority rule disables delete-marker replication, so the delete marker must not replicate"
        );
    }

    #[test]
    fn role_delete_decision_follows_highest_priority_rule() {
        let role = "arn:rustfs:replication:us-east-1:role-target:bucket";
        let destination = "arn:rustfs:replication:us-east-1:target:bucket";
        let config = ReplicationConfiguration {
            role: role.to_string(),
            rules: vec![
                delete_marker_rule("low-priority-enabled", destination, "logs/", 1, true),
                delete_marker_rule("high-priority-disabled", destination, "logs/2026/", 5, false),
            ],
        };
        let opts = ObjectOpts {
            name: "logs/2026/app.log".to_string(),
            op_type: ReplicationType::Delete,
            delete_marker: true,
            ..Default::default()
        };

        assert_eq!(
            config.filter_target_replication_decisions(&opts),
            vec![(role.to_string(), false)],
            "the role target must use the highest-priority matching rule"
        );
    }

    #[test]
    fn version_purge_uses_delete_replication_for_object_and_marker_versions() {
        let arn = "arn:rustfs:replication:us-east-1:target:bucket";
        let mut rule = delete_marker_rule("delete-switches", arn, "", 1, true);
        rule.delete_replication = Some(DeleteReplication {
            status: DeleteReplicationStatus::from_static(DeleteReplicationStatus::DISABLED),
        });
        let mut config = ReplicationConfiguration {
            role: String::new(),
            rules: vec![rule],
        };

        for version_id in [Some(Uuid::new_v4()), Some(Uuid::nil())] {
            for delete_marker in [false, true] {
                assert!(!config.replicate(&ObjectOpts {
                    name: "object".to_string(),
                    op_type: ReplicationType::Delete,
                    version_id,
                    delete_marker,
                    ..Default::default()
                }));
            }
        }

        let stored_marker = ObjectOpts {
            name: "object".to_string(),
            op_type: ReplicationType::Delete,
            delete_marker: true,
            ..Default::default()
        };
        assert!(config.replicate(&stored_marker), "stored markers must use DeleteMarkerReplication");
        assert_eq!(config.filter_target_replication_decisions(&stored_marker), vec![(arn.to_string(), true)]);

        config.rules[0].delete_replication = Some(DeleteReplication {
            status: DeleteReplicationStatus::from_static(DeleteReplicationStatus::ENABLED),
        });
        assert!(config.replicate(&ObjectOpts {
            name: "object".to_string(),
            op_type: ReplicationType::Delete,
            version_id: Some(Uuid::nil()),
            delete_marker: true,
            ..Default::default()
        }));
        assert!(config.replicate(&stored_marker));
    }

    #[test]
    fn unsupported_replication_fields_are_reported_before_persistence() {
        let arn = "arn:rustfs:replication:us-east-1:target:bucket";
        let mut config = ReplicationConfiguration {
            role: String::new(),
            rules: vec![replication_rule("unsupported", arn)],
        };

        config.rules[0].source_selection_criteria = Some(SourceSelectionCriteria {
            replica_modifications: None,
            sse_kms_encrypted_objects: Some(SseKmsEncryptedObjects {
                status: SseKmsEncryptedObjectsStatus::from_static(SseKmsEncryptedObjectsStatus::ENABLED),
            }),
        });
        assert_eq!(
            unsupported_replication_config_field(&config),
            Some("SourceSelectionCriteria.SseKmsEncryptedObjects")
        );

        config.rules[0].source_selection_criteria = None;
        config.rules[0].destination.encryption_configuration = Some(EncryptionConfiguration {
            replica_kms_key_id: Some("arn:aws:kms:us-east-1:123456789012:key/opaque-key-id".to_string()),
        });
        assert_eq!(unsupported_replication_config_field(&config), Some("Destination.EncryptionConfiguration"));

        config.rules[0].destination.encryption_configuration = None;
        config.rules[0].destination.access_control_translation = Some(s3s::dto::AccessControlTranslation {
            owner: s3s::dto::OwnerOverride::from_static(s3s::dto::OwnerOverride::DESTINATION),
        });
        assert_eq!(
            unsupported_replication_config_field(&config),
            Some("Destination.AccessControlTranslation")
        );

        config.rules[0].destination.access_control_translation = None;
        config.rules[0].destination.account = Some("123456789012".to_string());
        assert_eq!(unsupported_replication_config_field(&config), Some("Destination.Account"));

        config.rules[0].destination.account = None;
        config.rules[0].destination.metrics = Some(Metrics {
            event_threshold: None,
            status: MetricsStatus::from_static(MetricsStatus::ENABLED),
        });
        assert_eq!(unsupported_replication_config_field(&config), Some("Destination.Metrics"));

        config.rules[0].destination.metrics = None;
        config.rules[0].destination.replication_time = Some(ReplicationTime {
            status: ReplicationTimeStatus::from_static(ReplicationTimeStatus::ENABLED),
            time: ReplicationTimeValue { minutes: Some(15) },
        });
        assert_eq!(unsupported_replication_config_field(&config), Some("Destination.ReplicationTime"));

        config.rules[0].destination.replication_time = None;
        config.rules[0].destination.storage_class =
            Some(s3s::dto::StorageClass::from_static(s3s::dto::StorageClass::STANDARD_IA));
        assert_eq!(unsupported_replication_config_field(&config), Some("Destination.StorageClass"));

        // The exact-match contract is deliberate: S3 storage class enums are
        // case-sensitive, so a lowercase variant must stay rejected.
        config.rules[0].destination.storage_class = Some(StorageClass::from("standard".to_string()));
        assert_eq!(unsupported_replication_config_field(&config), Some("Destination.StorageClass"));

        // Explicit STANDARD is a no-op (the engine never reads the field) and must
        // pass: the console's rule form always sends it, and rejecting it makes the
        // form unusable.
        config.rules[0].destination.storage_class = Some(StorageClass::from_static(StorageClass::STANDARD));
        assert_eq!(unsupported_replication_config_field(&config), None);
    }

    #[test]
    fn explicit_standard_storage_class_is_accepted_from_wire_xml() {
        // The exact request shape the console's add-replication-rule form sends:
        // a rule whose Destination carries <StorageClass>STANDARD</StorageClass>.
        let xml = br#"
            <ReplicationConfiguration>
              <Role></Role>
              <Rule>
                <ID>console-rule</ID>
                <Status>Enabled</Status>
                <Priority>1</Priority>
                <DeleteMarkerReplication><Status>Enabled</Status></DeleteMarkerReplication>
                <Destination>
                  <Bucket>arn:aws:s3:::destination</Bucket>
                  <StorageClass>STANDARD</StorageClass>
                </Destination>
              </Rule>
            </ReplicationConfiguration>
        "#;
        let mut deserializer = Deserializer::new(xml);
        let config = <ReplicationConfiguration as s3s::xml::Deserialize>::deserialize(&mut deserializer)
            .expect("console-shaped config should parse");
        deserializer
            .expect_eof()
            .expect("console-shaped config should consume the whole body");

        assert_eq!(config.rules[0].destination.storage_class.as_ref().map(|c| c.as_str()), Some("STANDARD"));
        assert_eq!(unsupported_replication_config_field(&config), None);
    }

    #[test]
    fn historical_destination_fields_survive_the_s3_xml_round_trip() {
        let xml = br#"
            <ReplicationConfiguration>
              <Role></Role>
              <Rule>
                <ID>historical</ID>
                <Status>Enabled</Status>
                <Destination>
                  <Bucket>arn:aws:s3:::destination</Bucket>
                  <Account>123456789012</Account>
                  <AccessControlTranslation><Owner>Destination</Owner></AccessControlTranslation>
                  <StorageClass>STANDARD_IA</StorageClass>
                </Destination>
              </Rule>
            </ReplicationConfiguration>
        "#;
        let mut deserializer = Deserializer::new(xml);
        let config = <ReplicationConfiguration as s3s::xml::Deserialize>::deserialize(&mut deserializer)
            .expect("historical config should parse");
        deserializer
            .expect_eof()
            .expect("historical config should consume the whole body");

        let mut encoded = Vec::new();
        <ReplicationConfiguration as s3s::xml::Serialize>::serialize(&config, &mut Serializer::new(&mut encoded))
            .expect("historical config should serialize");
        let encoded = String::from_utf8(encoded).expect("serialized XML should be UTF-8");
        for field in [
            "<Account>123456789012</Account>",
            "<Owner>Destination</Owner>",
            "<StorageClass>STANDARD_IA</StorageClass>",
        ] {
            assert!(encoded.contains(field), "historical field {field} was lost: {encoded}");
        }
    }

    #[test]
    fn s3_xml_parser_discards_unknown_replication_elements_before_validation() {
        let xml = br#"
            <ReplicationConfiguration>
              <Role></Role>
              <FutureTopLevel>future</FutureTopLevel>
              <Rule>
                <ID>unknown</ID>
                <Status>Enabled</Status>
                <Destination>
                  <Bucket>arn:aws:s3:::destination</Bucket>
                </Destination>
              </Rule>
            </ReplicationConfiguration>
        "#;
        let mut deserializer = Deserializer::new(xml);
        let config = <ReplicationConfiguration as s3s::xml::Deserialize>::deserialize(&mut deserializer)
            .expect("s3s should accept unknown elements");
        deserializer.expect_eof().expect("unknown elements should still be consumed");

        assert!(config.rules[0].destination.encryption_configuration.is_none());
        assert_eq!(unsupported_replication_config_field(&config), None);
    }

    #[test]
    fn capability_fields_match_validator_rejections() {
        let rejected_fields = [
            "SourceSelectionCriteria.SseKmsEncryptedObjects",
            "Destination.EncryptionConfiguration",
            "Destination.Metrics",
            "Destination.ReplicationTime",
        ];

        for field in rejected_fields {
            assert!(
                REPLICATION_READ_ONLY_HISTORICAL_FIELDS.contains(&field),
                "rejected field {field} must be advertised as readable historical data"
            );
            assert!(
                !REPLICATION_WRITABLE_FIELDS.contains(&field),
                "rejected field {field} must not be advertised as writable"
            );
        }

        for field in REPLICATION_WRITABLE_FIELDS {
            assert!(
                !REPLICATION_READ_ONLY_HISTORICAL_FIELDS.contains(field),
                "field {field} cannot be both writable and historical-only"
            );
        }
    }

    #[test]
    fn invalid_replication_status_fields_are_reported_before_persistence() {
        let arn = "arn:rustfs:replication:us-east-1:target:bucket";
        let mut config = ReplicationConfiguration {
            role: String::new(),
            rules: vec![replication_rule("invalid-status", arn)],
        };

        config.rules[0].status = ReplicationRuleStatus::from_static("Invalid");
        assert_eq!(invalid_replication_config_status_field(&config), Some("Rule.Status"));

        config.rules[0] = replication_rule("invalid-status", arn);
        config.rules[0].existing_object_replication = Some(ExistingObjectReplication {
            status: ExistingObjectReplicationStatus::from_static("Invalid"),
        });
        assert_eq!(
            invalid_replication_config_status_field(&config),
            Some("Rule.ExistingObjectReplication.Status")
        );

        config.rules[0] = replication_rule("invalid-status", arn);
        config.rules[0].delete_replication = Some(DeleteReplication {
            status: DeleteReplicationStatus::from_static("Invalid"),
        });
        assert_eq!(invalid_replication_config_status_field(&config), Some("Rule.DeleteReplication.Status"));

        config.rules[0] = replication_rule("invalid-status", arn);
        config.rules[0].delete_marker_replication = Some(DeleteMarkerReplication {
            status: Some(DeleteMarkerReplicationStatus::from_static("Invalid")),
        });
        assert_eq!(
            invalid_replication_config_status_field(&config),
            Some("Rule.DeleteMarkerReplication.Status")
        );

        config.rules[0] = replication_rule("invalid-status", arn);
        config.rules[0].source_selection_criteria = Some(SourceSelectionCriteria {
            replica_modifications: Some(ReplicaModifications {
                status: ReplicaModificationsStatus::from_static("Invalid"),
            }),
            sse_kms_encrypted_objects: None,
        });
        assert_eq!(
            invalid_replication_config_status_field(&config),
            Some("Rule.SourceSelectionCriteria.ReplicaModifications.Status")
        );
    }

    #[test]
    fn target_decisions_choose_highest_priority_rule_per_destination() {
        let target_a = "arn:rustfs:replication:us-east-1:target:a";
        let target_b = "arn:rustfs:replication:us-east-1:target:b";
        let mut a_low = delete_marker_rule("a-low", target_a, "logs/", 1, true);
        let b = delete_marker_rule("b", target_b, "logs/", 2, true);
        let a_high = delete_marker_rule("a-high", target_a, "logs/2026/", 5, false);
        a_low.delete_replication = Some(DeleteReplication {
            status: DeleteReplicationStatus::from_static(DeleteReplicationStatus::ENABLED),
        });
        let config = ReplicationConfiguration {
            role: String::new(),
            rules: vec![a_low, b, a_high],
        };

        let decisions = config.filter_target_replication_decisions(&ObjectOpts {
            name: "logs/2026/app.log".to_string(),
            op_type: ReplicationType::Delete,
            delete_marker: true,
            ..Default::default()
        });

        assert_eq!(decisions, vec![(target_a.to_string(), false), (target_b.to_string(), true)]);
    }

    #[test]
    fn force_delete_targets_use_overlapping_rules_and_highest_priority_switch() {
        let target_a = "arn:target:a";
        let target_b = "arn:target:b";
        let mut a_parent = delete_marker_rule("a-parent", target_a, "logs/", 1, true);
        a_parent.delete_replication = Some(DeleteReplication {
            status: DeleteReplicationStatus::from_static(DeleteReplicationStatus::ENABLED),
        });
        let a_child_disabled = delete_marker_rule("a-child", target_a, "logs/2026/", 5, false);
        let b_child = delete_marker_rule("b-child", target_b, "logs/2026/", 2, true);
        let config = ReplicationConfiguration {
            role: String::new(),
            rules: vec![a_parent, a_child_disabled, b_child],
        };

        assert_eq!(
            config.filter_force_delete_target_arns("logs/2026/app.log"),
            vec![target_b.to_string()],
            "the child rule must win for target A while the overlapping child target B remains eligible"
        );
    }

    #[test]
    fn site_replication_rule_deployment_id_requires_id_and_arn_agreement() {
        let reconciler_rule = replication_rule("site-repl-peer-dep", "arn:rustfs:replication::peer-dep:bucket");
        assert_eq!(site_replication_rule_deployment_id(&reconciler_rule), Some("peer-dep"));

        // A remote-target ARN carries the remote's deployment id (or a random
        // uuid), never the operator's rule id.
        let operator_named_rule = replication_rule("site-repl-user", "arn:minio:replication:us-east-1:2f1c-remote:bucket");
        assert_eq!(site_replication_rule_deployment_id(&operator_named_rule), None);

        let foreign_arn = replication_rule("site-repl-peer-dep", "arn:rustfs:replication::other-dep:bucket");
        assert_eq!(site_replication_rule_deployment_id(&foreign_arn), None);

        let empty_id = replication_rule("site-repl-", "arn:rustfs:replication::peer-dep:bucket");
        assert_eq!(site_replication_rule_deployment_id(&empty_id), None);

        let peers = HashSet::from(["peer-dep".to_string()]);
        assert!(is_reconciler_owned_site_replication_rule(&reconciler_rule, &peers));
        assert!(!is_reconciler_owned_site_replication_rule(&reconciler_rule, &HashSet::new()));
        let removed_peer = replication_rule("site-repl-gone-dep", "arn:rustfs:replication::gone-dep:bucket");
        assert!(!is_reconciler_owned_site_replication_rule(&removed_peer, &peers));
    }

    // The merge must not rewrite the operator's priorities: with the
    // priority-5 rule listed first and renumbered 1 then 2, the priority-1
    // delete-marker-disabled rule would win the replication decision.
    #[test]
    fn merge_keeps_operator_priorities_and_replication_decision() {
        let user_arn = "arn:minio:replication:us-east-1:2f1c-remote:bucket";
        let peer_arn = "arn:rustfs:replication::peer-dep:bucket";
        let incoming = ReplicationConfiguration {
            role: String::new(),
            rules: vec![
                delete_marker_rule("dm-enabled", user_arn, "logs/", 5, true),
                delete_marker_rule("dm-disabled", user_arn, "logs/2026/", 1, false),
            ],
        };
        let mut site_rule = delete_marker_rule("site-repl-peer-dep", peer_arn, "", 7, true);
        site_rule.prefix = None;
        let local = structure_config(vec![site_rule]);
        let opts = ObjectOpts {
            name: "logs/2026/app.log".to_string(),
            op_type: ReplicationType::Delete,
            delete_marker: true,
            version_id: None,
            ..Default::default()
        };
        let submitted: Vec<_> = incoming.filter_target_replication_decisions(&opts);

        let peers = HashSet::from(["peer-dep".to_string()]);
        let merged =
            merge_user_replication_config(Some(incoming.clone()), Some(local.clone()), &peers, OperatorRuleContract::Derived)
                .expect("rules");

        let priorities: Vec<_> = merged
            .rules
            .iter()
            .map(|rule| (rule.id.as_deref().unwrap(), rule.priority))
            .collect();
        assert_eq!(
            priorities,
            vec![
                ("dm-enabled", Some(5)),
                ("dm-disabled", Some(1)),
                ("site-repl-peer-dep", Some(2))
            ],
            "operator priorities are kept verbatim; the site rule takes the lowest free slot"
        );
        assert!(validate_replication_config_structure(&merged).is_ok());
        let mut decisions = merged.filter_target_replication_decisions(&opts);
        decisions.retain(|(arn, _)| arn == user_arn);
        assert_eq!(decisions, submitted, "the merged config must replicate exactly as the operator submitted");
        assert_eq!(decisions, vec![(user_arn.to_string(), true)]);

        // The peer ingestion merge follows the same rule.
        let merged =
            merge_incoming_replication_config(Some(incoming), Some(local), &peers, OperatorRuleContract::Derived).expect("rules");
        let priorities: Vec<_> = merged.rules.iter().map(|rule| rule.priority).collect();
        assert_eq!(priorities, vec![Some(5), Some(1), Some(2)]);
    }

    #[test]
    fn site_rule_priorities_skip_every_operator_priority() {
        let mut rules = vec![
            delete_marker_rule("a", "arn:a", "", 2, true),
            delete_marker_rule("site-repl-x", "arn:rustfs:replication::x:b", "", 9, true),
            delete_marker_rule("b", "arn:a", "", 1, true),
            delete_marker_rule("site-repl-y", "arn:rustfs:replication::y:b", "", 9, true),
            delete_marker_rule("c", "arn:a", "", 4, true),
        ];
        assign_site_replication_rule_priorities(&mut rules, is_site_replication_rule);
        let priorities: Vec<_> = rules.iter().map(|rule| rule.priority).collect();
        assert_eq!(priorities, vec![Some(2), Some(3), Some(1), Some(5), Some(4)]);
        assert!(validate_replication_config_structure(&structure_config(rules.clone())).is_ok());

        // Idempotent, so the reconciler's pass over an already-merged config
        // is a byte-stable no-op rather than a rewrite every period.
        let settled = rules.clone();
        assign_site_replication_rule_priorities(&mut rules, is_site_replication_rule);
        assert_eq!(rules, settled);
    }

    fn operator_rule_ids(config: &ReplicationConfiguration) -> Vec<(&str, Option<i32>)> {
        config
            .rules
            .iter()
            .filter(|rule| site_replication_rule_deployment_id(rule).is_none())
            .map(|rule| (rule.id.as_deref().unwrap(), rule.priority))
            .collect()
    }

    // Issue #1948 review: an owner-authored `site-repl-user` rule is operator
    // state. Site A's S3 merge keeps it; the broadcast payload must survive
    // site B's peer ingestion too, or the sites persist different configs.
    #[test]
    fn peer_ingestion_keeps_owner_site_repl_user_rule_and_sites_agree() {
        let user_arn = "arn:minio:replication:us-east-1:2f1c-remote:bucket";
        let put = structure_config(vec![
            delete_marker_rule("site-repl-user", user_arn, "logs/", 3, true),
            delete_marker_rule("nightly", user_arn, "", 1, true),
        ]);
        let a_local = structure_config(vec![replication_rule("site-repl-b-dep", "arn:rustfs:replication::b-dep:bucket")]);
        let a_peers = HashSet::from(["b-dep".to_string()]);
        let a_merged =
            merge_user_replication_config(Some(put), Some(a_local), &a_peers, OperatorRuleContract::Derived).expect("rules");
        assert_eq!(operator_rule_ids(&a_merged), vec![("site-repl-user", Some(3)), ("nightly", Some(1))]);

        // Site B ingests A's broadcast; its own reverse rule names A.
        let b_local = structure_config(vec![replication_rule("site-repl-a-dep", "arn:rustfs:replication::a-dep:bucket")]);
        let b_sites = HashSet::from(["a-dep".to_string(), "b-dep".to_string()]);
        let b_merged =
            merge_incoming_replication_config(Some(a_merged.clone()), Some(b_local), &b_sites, OperatorRuleContract::Derived)
                .expect("rules");

        let ids: Vec<_> = b_merged.rules.iter().map(|rule| rule.id.as_deref().unwrap()).collect();
        assert_eq!(ids, vec!["site-repl-user", "nightly", "site-repl-a-dep"]);
        assert_eq!(
            operator_rule_ids(&b_merged),
            operator_rule_ids(&a_merged),
            "both sites must persist the same operator rules"
        );
    }

    // Issue #1948 review: `Role` is only the sender's when it names a
    // current site-replication peer; an owner-submitted role target has
    // already passed target validation and drives target selection.
    #[test]
    fn merge_keeps_operator_role_target_for_target_selection() {
        let role = "arn:minio:replication::operator-dep:bucket";
        let peers = HashSet::from(["peer-dep".to_string()]);
        let incoming = ReplicationConfiguration {
            role: role.to_string(),
            rules: vec![delete_marker_rule("nightly", role, "", 1, true)],
        };
        let local = structure_config(vec![replication_rule(
            "site-repl-peer-dep",
            "arn:rustfs:replication::peer-dep:bucket",
        )]);
        let opts = ObjectOpts {
            name: "logs/app.log".to_string(),
            ..Default::default()
        };

        let merged =
            merge_user_replication_config(Some(incoming.clone()), Some(local.clone()), &peers, OperatorRuleContract::Derived)
                .expect("rules");
        assert_eq!(merged.role, role);
        assert_eq!(replication_target_arns(&merged), HashSet::from([role.to_string()]));
        assert_eq!(merged.filter_target_arns(&opts), vec![role.to_string()]);

        let ingested =
            merge_incoming_replication_config(Some(incoming.clone()), Some(local.clone()), &peers, OperatorRuleContract::Derived)
                .expect("rules");
        assert_eq!(ingested.role, role);
        assert_eq!(ingested.filter_target_arns(&opts), vec![role.to_string()]);

        // A role naming a current peer is the sender's identity and still goes.
        let mut derived_role = incoming;
        derived_role.role = "arn:rustfs:replication::peer-dep:bucket".to_string();
        let merged =
            merge_user_replication_config(Some(derived_role), Some(local), &peers, OperatorRuleContract::Derived).expect("rules");
        assert!(merged.role.is_empty());
    }

    // Issue #1948 review: while a site still runs the pre-contract code the
    // cluster must stay on one config. A new site broadcasting `5,1` would
    // be renumbered `1,2` by that peer — selecting the other overlapping
    // rule — so the new sites merge the legacy way and list the operator
    // rules in priority order first, which keeps the decision.
    #[test]
    fn legacy_contract_matches_pre_contract_peers_and_keeps_the_decision() {
        let user_arn = "arn:minio:replication:us-east-1:2f1c-remote:bucket";
        let put = ReplicationConfiguration {
            role: "arn:minio:replication::operator-dep:bucket".to_string(),
            rules: vec![
                delete_marker_rule("dm-enabled", user_arn, "logs/", 5, true),
                delete_marker_rule("dm-disabled", user_arn, "logs/2026/", 1, false),
                delete_marker_rule("site-repl-user", user_arn, "tmp/", 2, true),
            ],
        };
        let a_local = structure_config(vec![replication_rule("site-repl-b-dep", "arn:rustfs:replication::b-dep:bucket")]);
        let opts = ObjectOpts {
            name: "logs/2026/app.log".to_string(),
            op_type: ReplicationType::Delete,
            delete_marker: true,
            ..Default::default()
        };
        // Decisions per rule destination: the role is dropped by the legacy
        // merge, so compare against the rules alone.
        let submitted: Vec<_> = structure_config(put.rules.clone()).filter_target_replication_decisions(&opts);

        let a_peers = HashSet::from(["b-dep".to_string()]);
        let a_merged =
            merge_user_replication_config(Some(put.clone()), Some(a_local.clone()), &a_peers, OperatorRuleContract::Legacy)
                .expect("rules");
        let layout: Vec<_> = a_merged
            .rules
            .iter()
            .map(|rule| (rule.id.as_deref().unwrap(), rule.priority))
            .collect();
        assert_eq!(
            layout,
            vec![
                ("dm-disabled", Some(1)),
                ("dm-enabled", Some(2)),
                ("site-repl-b-dep", Some(3))
            ],
            "legacy: operator rules in priority order, every rule renumbered 1..n, `site-repl-*` ids dropped"
        );
        assert!(a_merged.role.is_empty(), "legacy peers drop any site-replication-shaped role");
        let mut decisions = a_merged.filter_target_replication_decisions(&opts);
        decisions.retain(|(arn, _)| arn == user_arn);
        assert_eq!(decisions, submitted, "the renumbering must not flip the winning rule");

        // A pre-contract peer renumbers A's payload in list order: same bytes.
        let mut pre_contract = a_merged
            .rules
            .iter()
            .filter(|rule| !is_site_replication_rule(rule))
            .cloned()
            .collect::<Vec<_>>();
        pre_contract.push(replication_rule("site-repl-a-dep", "arn:rustfs:replication::a-dep:bucket"));
        for (index, rule) in pre_contract.iter_mut().enumerate() {
            rule.priority = Some(index as i32 + 1);
        }
        // A new peer told the payload is legacy produces the same bytes too.
        let b_local = structure_config(vec![replication_rule("site-repl-a-dep", "arn:rustfs:replication::a-dep:bucket")]);
        let b_sites = HashSet::from(["a-dep".to_string(), "b-dep".to_string()]);
        let b_merged = merge_incoming_replication_config(Some(a_merged), Some(b_local), &b_sites, OperatorRuleContract::Legacy)
            .expect("rules");
        assert_eq!(b_merged.rules, pre_contract);

        // Every site on the derived contract: the submitted policy is kept.
        let a_merged =
            merge_user_replication_config(Some(put), Some(a_local), &a_peers, OperatorRuleContract::Derived).expect("rules");
        let layout: Vec<_> = a_merged
            .rules
            .iter()
            .map(|rule| (rule.id.as_deref().unwrap(), rule.priority))
            .collect();
        assert_eq!(
            layout,
            vec![
                ("dm-enabled", Some(5)),
                ("dm-disabled", Some(1)),
                ("site-repl-user", Some(2)),
                ("site-repl-b-dep", Some(3))
            ]
        );
        assert_eq!(a_merged.role, "arn:minio:replication::operator-dep:bucket");
    }
}
