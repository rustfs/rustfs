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

use s3s::dto::ReplicationConfiguration;

use super::replication_config_boundary::{ObjectOpts, ReplicationConfigurationExt};
use super::replication_filemeta_boundary::ReplicationType;

pub(crate) struct ReplicationTargetConfigBridge;

impl ReplicationTargetConfigBridge {
    pub(crate) fn target_is_used_by_rules(config: &ReplicationConfiguration, arn: &str) -> bool {
        config
            .filter_target_arns(&ObjectOpts {
                op_type: ReplicationType::All,
                ..Default::default()
            })
            .into_iter()
            .any(|rule| rule == arn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_config_bridge_matches_role_target() {
        let config = ReplicationConfiguration {
            role: " arn:target ".to_string(),
            rules: Vec::new(),
        };

        assert!(ReplicationTargetConfigBridge::target_is_used_by_rules(&config, "arn:target"));
        assert!(!ReplicationTargetConfigBridge::target_is_used_by_rules(&config, "arn:other"));
    }
}
