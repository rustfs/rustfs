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

pub use rustfs_replication::{
    ObjectOpts, OperatorRuleContract, REMOTE_TARGET_CAPABILITY_CONTRACT_VERSION, REMOTE_TARGET_UNSUPPORTED_FIELDS,
    REMOTE_TARGET_WRITABLE_FIELDS, REPLICATION_CAPABILITY_CONTRACT_VERSION, REPLICATION_READ_ONLY_HISTORICAL_FIELDS,
    REPLICATION_WRITABLE_FIELDS, ReplicationConfigStructureError, ReplicationConfigurationExt, ReplicationRuleExt,
    ReplicationTargetValidationError, assign_site_replication_rule_priorities, invalid_replication_config_status_field,
    is_site_replication_role, is_site_replication_rule, merge_incoming_replication_config, merge_user_replication_config,
    replication_target_arn_deployment_id, replication_target_arns, should_remove_replication_target,
    site_replication_rule_deployment_id, unsupported_replication_config_field, validate_replication_config_structure,
    validate_replication_config_target_arns,
};
