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

use super::loader::collect_merged_target_configs_from_env;
use crate::domain::TargetDomain;
use rustfs_ecstore::config::{Config, KVS};
use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TargetPluginInstanceCompatDescriptor<'a> {
    pub domain: TargetDomain,
    pub plugin_id: &'a str,
    pub target_type: &'a str,
    pub subsystem: &'a str,
    pub route_prefix: &'a str,
    pub valid_fields: &'a [&'a str],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetInstanceSourceClass {
    Config,
    Env,
    Mixed,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TargetInstanceSourceHints {
    pub has_file_default: bool,
    pub has_file_instance: bool,
    pub has_env_default: bool,
    pub has_env_instance: bool,
}

impl TargetInstanceSourceHints {
    #[inline]
    pub fn has_config_source(self) -> bool {
        self.has_file_default || self.has_file_instance
    }

    #[inline]
    pub fn has_env_source(self) -> bool {
        self.has_env_default || self.has_env_instance
    }

    #[inline]
    pub fn classification(self) -> TargetInstanceSourceClass {
        match (self.has_config_source(), self.has_env_source()) {
            (true, true) => TargetInstanceSourceClass::Mixed,
            (true, false) => TargetInstanceSourceClass::Config,
            (false, true) => TargetInstanceSourceClass::Env,
            (false, false) => TargetInstanceSourceClass::Config,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetPluginInstanceRecord {
    pub domain: TargetDomain,
    pub plugin_id: String,
    pub target_type: String,
    pub subsystem: String,
    pub instance_id: String,
    pub enabled: bool,
    pub source_hints: TargetInstanceSourceHints,
    pub effective_config: KVS,
}

pub type LegacyTargetInstanceDescriptor<'a> = TargetPluginInstanceCompatDescriptor<'a>;
pub type TargetPluginInstance = TargetPluginInstanceRecord;

pub fn normalize_target_plugin_instances(
    config: &Config,
    descriptor: &TargetPluginInstanceCompatDescriptor<'_>,
) -> Vec<TargetPluginInstanceRecord> {
    normalize_target_plugin_instances_from_env(config, descriptor, std::env::vars())
}

pub fn normalize_target_plugin_instances_from_env<I>(
    config: &Config,
    descriptor: &TargetPluginInstanceCompatDescriptor<'_>,
    env_vars: I,
) -> Vec<TargetPluginInstanceRecord>
where
    I: IntoIterator<Item = (String, String)>,
{
    let valid_fields = descriptor
        .valid_fields
        .iter()
        .map(|field| (*field).to_string())
        .collect::<HashSet<_>>();

    collect_merged_target_configs_from_env(
        config,
        descriptor.subsystem,
        descriptor.route_prefix,
        descriptor.target_type,
        &valid_fields,
        env_vars,
    )
    .into_iter()
    .map(|record| TargetPluginInstanceRecord {
        domain: descriptor.domain,
        plugin_id: descriptor.plugin_id.to_string(),
        target_type: descriptor.target_type.to_string(),
        subsystem: descriptor.subsystem.to_string(),
        instance_id: record.instance_id,
        enabled: record.enabled,
        source_hints: TargetInstanceSourceHints {
            has_file_default: record.has_file_default,
            has_file_instance: record.has_file_instance,
            has_env_default: record.has_env_default,
            has_env_instance: record.has_env_instance,
        },
        effective_config: record.effective_config,
    })
    .collect()
}

pub fn normalize_legacy_target_instances(
    config: &Config,
    descriptor: &LegacyTargetInstanceDescriptor<'_>,
) -> Vec<TargetPluginInstance> {
    normalize_target_plugin_instances(config, descriptor)
}

pub fn normalize_legacy_target_instances_from_env<I>(
    config: &Config,
    descriptor: &LegacyTargetInstanceDescriptor<'_>,
    env_vars: I,
) -> Vec<TargetPluginInstance>
where
    I: IntoIterator<Item = (String, String)>,
{
    normalize_target_plugin_instances_from_env(config, descriptor, env_vars)
}

#[cfg(test)]
mod tests {
    use super::{
        TargetInstanceSourceClass, TargetPluginInstanceCompatDescriptor, normalize_legacy_target_instances_from_env,
        normalize_target_plugin_instances_from_env,
    };
    use crate::domain::TargetDomain;
    use crate::manifest::builtin_target_manifest;
    use rustfs_config::audit::{AUDIT_ROUTE_PREFIX, AUDIT_WEBHOOK_KEYS, AUDIT_WEBHOOK_SUB_SYS};
    use rustfs_config::notify::{NOTIFY_ROUTE_PREFIX, NOTIFY_WEBHOOK_KEYS, NOTIFY_WEBHOOK_SUB_SYS};
    use rustfs_config::{ENABLE_KEY, WEBHOOK_ENDPOINT, WEBHOOK_QUEUE_LIMIT};
    use rustfs_ecstore::config::{Config, KVS};
    use std::collections::HashMap;

    fn notify_webhook_descriptor() -> TargetPluginInstanceCompatDescriptor<'static> {
        TargetPluginInstanceCompatDescriptor {
            domain: TargetDomain::Notify,
            plugin_id: builtin_target_manifest("webhook").plugin_id,
            target_type: "webhook",
            subsystem: NOTIFY_WEBHOOK_SUB_SYS,
            route_prefix: NOTIFY_ROUTE_PREFIX,
            valid_fields: NOTIFY_WEBHOOK_KEYS,
        }
    }

    fn audit_webhook_descriptor() -> TargetPluginInstanceCompatDescriptor<'static> {
        TargetPluginInstanceCompatDescriptor {
            domain: TargetDomain::Audit,
            plugin_id: builtin_target_manifest("webhook").plugin_id,
            target_type: "webhook",
            subsystem: AUDIT_WEBHOOK_SUB_SYS,
            route_prefix: AUDIT_ROUTE_PREFIX,
            valid_fields: AUDIT_WEBHOOK_KEYS,
        }
    }

    #[test]
    fn normalize_notify_instances_merges_file_and_env_sources() {
        let mut cfg = Config(HashMap::new());
        let mut subsystem = HashMap::new();

        let mut default_kvs = KVS::new();
        default_kvs.insert(ENABLE_KEY.to_string(), "on".to_string());
        default_kvs.insert(WEBHOOK_QUEUE_LIMIT.to_string(), "10".to_string());
        subsystem.insert("_".to_string(), default_kvs);

        let mut primary = KVS::new();
        primary.insert(WEBHOOK_ENDPOINT.to_string(), "https://example.com/primary".to_string());
        subsystem.insert("primary".to_string(), primary);

        cfg.0.insert(NOTIFY_WEBHOOK_SUB_SYS.to_string(), subsystem);

        let instances = normalize_legacy_target_instances_from_env(
            &cfg,
            &notify_webhook_descriptor(),
            vec![
                ("RUSTFS_NOTIFY_WEBHOOK_QUEUE_LIMIT".to_string(), "42".to_string()),
                ("RUSTFS_NOTIFY_WEBHOOK_ENABLE_SECONDARY".to_string(), "on".to_string()),
                (
                    "RUSTFS_NOTIFY_WEBHOOK_ENDPOINT_SECONDARY".to_string(),
                    "https://example.com/secondary".to_string(),
                ),
            ],
        );

        assert_eq!(instances.len(), 2);

        let primary = instances
            .iter()
            .find(|instance| instance.instance_id == "primary")
            .expect("primary notify instance should be normalized");
        assert_eq!(primary.domain, TargetDomain::Notify);
        assert_eq!(primary.plugin_id, "builtin:webhook");
        assert!(primary.enabled);
        assert_eq!(primary.effective_config.lookup(WEBHOOK_QUEUE_LIMIT).as_deref(), Some("42"));
        assert_eq!(
            primary.effective_config.lookup(WEBHOOK_ENDPOINT).as_deref(),
            Some("https://example.com/primary")
        );
        assert_eq!(primary.source_hints.classification(), TargetInstanceSourceClass::Mixed);
        assert!(primary.source_hints.has_file_default);
        assert!(primary.source_hints.has_file_instance);
        assert!(primary.source_hints.has_env_default);
        assert!(!primary.source_hints.has_env_instance);

        let secondary = instances
            .iter()
            .find(|instance| instance.instance_id == "secondary")
            .expect("secondary env notify instance should be normalized");
        assert!(secondary.enabled);
        assert_eq!(
            secondary.effective_config.lookup(WEBHOOK_ENDPOINT).as_deref(),
            Some("https://example.com/secondary")
        );
        assert_eq!(secondary.effective_config.lookup(WEBHOOK_QUEUE_LIMIT).as_deref(), Some("42"));
        assert_eq!(secondary.source_hints.classification(), TargetInstanceSourceClass::Mixed);
        assert!(secondary.source_hints.has_file_default);
        assert!(!secondary.source_hints.has_file_instance);
        assert!(secondary.source_hints.has_env_default);
        assert!(secondary.source_hints.has_env_instance);
    }

    #[test]
    fn normalize_audit_instances_preserves_domain_and_subsystem() {
        let mut cfg = Config(HashMap::new());
        let mut subsystem = HashMap::new();

        let mut default_kvs = KVS::new();
        default_kvs.insert(ENABLE_KEY.to_string(), "off".to_string());
        subsystem.insert("_".to_string(), default_kvs);

        let mut primary = KVS::new();
        primary.insert(ENABLE_KEY.to_string(), "on".to_string());
        primary.insert(WEBHOOK_ENDPOINT.to_string(), "https://example.com/audit".to_string());
        subsystem.insert("primary".to_string(), primary);

        cfg.0.insert(AUDIT_WEBHOOK_SUB_SYS.to_string(), subsystem);

        let instances = normalize_legacy_target_instances_from_env(&cfg, &audit_webhook_descriptor(), Vec::new());

        assert_eq!(instances.len(), 1);
        let primary = &instances[0];
        assert_eq!(primary.domain, TargetDomain::Audit);
        assert_eq!(primary.target_type, "webhook");
        assert_eq!(primary.subsystem, AUDIT_WEBHOOK_SUB_SYS);
        assert_eq!(primary.instance_id, "primary");
        assert!(primary.enabled);
        assert_eq!(
            primary.effective_config.lookup(WEBHOOK_ENDPOINT).as_deref(),
            Some("https://example.com/audit")
        );
        assert_eq!(primary.source_hints.classification(), TargetInstanceSourceClass::Config);
    }

    #[test]
    fn normalize_instances_keeps_disabled_records() {
        let cfg = Config(HashMap::new());

        let instances = normalize_legacy_target_instances_from_env(
            &cfg,
            &notify_webhook_descriptor(),
            vec![
                ("RUSTFS_NOTIFY_WEBHOOK_ENABLE_DISABLED".to_string(), "off".to_string()),
                (
                    "RUSTFS_NOTIFY_WEBHOOK_ENDPOINT_DISABLED".to_string(),
                    "https://example.com/disabled".to_string(),
                ),
            ],
        );

        assert_eq!(instances.len(), 1);
        let disabled = &instances[0];
        assert_eq!(disabled.instance_id, "disabled");
        assert!(!disabled.enabled);
        assert_eq!(disabled.source_hints.classification(), TargetInstanceSourceClass::Env);
        assert!(disabled.source_hints.has_env_instance);
    }

    #[test]
    fn normalize_instances_excludes_default_only_entries() {
        let mut cfg = Config(HashMap::new());
        let mut subsystem = HashMap::new();

        let mut default_kvs = KVS::new();
        default_kvs.insert(ENABLE_KEY.to_string(), "on".to_string());
        default_kvs.insert(WEBHOOK_QUEUE_LIMIT.to_string(), "99".to_string());
        subsystem.insert("_".to_string(), default_kvs);

        cfg.0.insert(NOTIFY_WEBHOOK_SUB_SYS.to_string(), subsystem);

        let instances = normalize_legacy_target_instances_from_env(
            &cfg,
            &notify_webhook_descriptor(),
            vec![("RUSTFS_NOTIFY_WEBHOOK_QUEUE_LIMIT".to_string(), "100".to_string())],
        );

        assert!(instances.is_empty());
    }

    #[test]
    fn compatibility_wrapper_matches_canonical_instance_model() {
        let mut cfg = Config(HashMap::new());
        let mut subsystem = HashMap::new();

        let mut primary = KVS::new();
        primary.insert(ENABLE_KEY.to_string(), "on".to_string());
        primary.insert(WEBHOOK_ENDPOINT.to_string(), "https://example.com/primary".to_string());
        subsystem.insert("primary".to_string(), primary);
        cfg.0.insert(NOTIFY_WEBHOOK_SUB_SYS.to_string(), subsystem);

        let descriptor = notify_webhook_descriptor();
        let env = vec![("RUSTFS_NOTIFY_WEBHOOK_QUEUE_LIMIT".to_string(), "7".to_string())];

        let canonical = normalize_target_plugin_instances_from_env(&cfg, &descriptor, env.clone());
        let compatibility = normalize_legacy_target_instances_from_env(&cfg, &descriptor, env);

        assert_eq!(canonical, compatibility);
    }
}
