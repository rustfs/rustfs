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

use crate::AuditEntry;
use rustfs_targets::catalog::builtin::builtin_audit_target_descriptors;
use rustfs_targets::{BuiltinTargetDescriptor, TargetPluginDescriptor};

pub fn builtin_target_descriptors() -> Vec<BuiltinTargetDescriptor<AuditEntry>> {
    builtin_audit_target_descriptors::<AuditEntry>()
}

pub fn builtin_target_plugins() -> Vec<TargetPluginDescriptor<AuditEntry>> {
    builtin_target_descriptors()
        .into_iter()
        .map(|descriptor| descriptor.plugin().clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::builtin_target_descriptors;
    use rustfs_config::audit::AUDIT_AMQP_KEYS;
    use rustfs_config::{AMQP_EXCHANGE, AMQP_QUEUE_DIR, AMQP_ROUTING_KEY, AMQP_URL};
    use rustfs_ecstore::config::KVS;
    use rustfs_targets::target::ChannelTargetType;

    fn amqp_base_config() -> KVS {
        let mut config = KVS::new();
        config.insert(AMQP_URL.to_string(), "amqp://127.0.0.1:5672/%2f".to_string());
        config.insert(AMQP_EXCHANGE.to_string(), "rustfs.audit".to_string());
        config.insert(AMQP_ROUTING_KEY.to_string(), "audit".to_string());
        config.insert(AMQP_QUEUE_DIR.to_string(), String::new());
        config
    }

    #[test]
    fn builtin_plugins_include_amqp_descriptor() {
        let plugin = builtin_target_descriptors()
            .into_iter()
            .find(|plugin| plugin.plugin().target_type() == ChannelTargetType::Amqp.as_str())
            .expect("amqp plugin should exist");

        assert!(plugin.plugin().valid_fields().contains(&AMQP_URL));
        assert!(plugin.plugin().valid_fields().contains(&AMQP_EXCHANGE));
        assert!(plugin.plugin().valid_fields().contains(&AMQP_ROUTING_KEY));
        assert_eq!(plugin.plugin().valid_fields().len(), AUDIT_AMQP_KEYS.len());
    }

    #[test]
    fn builtin_plugins_create_audit_amqp_target() {
        let plugin = builtin_target_descriptors()
            .into_iter()
            .find(|plugin| plugin.plugin().target_type() == ChannelTargetType::Amqp.as_str())
            .expect("amqp plugin should exist");

        let target = plugin
            .plugin()
            .create_target("primary".to_string(), &amqp_base_config())
            .expect("AMQP audit target should be created");

        let target_id = target.id();
        assert_eq!(target_id.id, "primary");
        assert_eq!(target_id.name, "amqp");
        assert!(target.store().is_none());
    }
}
