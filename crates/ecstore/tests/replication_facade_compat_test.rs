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

mod storage_api;

use s3s::dto::ReplicationConfiguration;
use storage_api::replication_compat::{
    BucketStats, DeletedObjectReplicationInfo, DynReplicationPool, ObjectOpts, REPLICATE_INCOMING_DELETE, ReplicateDecision,
    ReplicationConfigurationExt, ReplicationDeleteScheduleInput, ReplicationDeleteStateSource, ReplicationObjectBridge,
    ReplicationState, ReplicationStats, ReplicationTargetValidationError, ResyncStatusType, delete_replication_state_from_config,
    delete_replication_version_id, replication_statuses_map, replication_target_arns, should_remove_replication_target,
    should_schedule_delete_replication, should_use_existing_delete_replication_info,
    should_use_existing_delete_replication_source, validate_replication_config_target_arns,
};

fn type_name<T>() -> &'static str {
    std::any::type_name::<T>()
}

fn type_name_unsized<T: ?Sized>() -> &'static str {
    std::any::type_name::<T>()
}

fn assert_replication_config_ext<T: ReplicationConfigurationExt>() {}

#[test]
fn replication_facade_exports_config_extension_contract() {
    assert_replication_config_ext::<ReplicationConfiguration>();
}

#[test]
fn replication_facade_exports_runtime_and_dto_types() {
    assert_eq!(ResyncStatusType::default(), ResyncStatusType::NoResync);
    assert!(!ResyncStatusType::default().is_valid());

    let object_opts = ObjectOpts::default();
    assert!(object_opts.name.is_empty());

    let deleted = DeletedObjectReplicationInfo::default();
    assert!(deleted.bucket.is_empty());

    let _stats = ReplicationStats::new();
    let _bucket_stats = BucketStats::default();

    assert!(type_name::<ReplicationStats>().contains("ReplicationStats"));
    assert!(type_name::<BucketStats>().contains("BucketStats"));
    assert!(type_name::<DeletedObjectReplicationInfo>().contains("DeletedObjectReplicationInfo"));
    assert!(type_name::<ReplicationObjectBridge>().contains("ReplicationObjectBridge"));
    assert!(type_name_unsized::<DynReplicationPool>().contains("ReplicationPoolTrait"));
}

#[test]
fn replication_facade_exports_app_storage_helper_contracts() {
    let config = ReplicationConfiguration::default();
    let target_arns = replication_target_arns(&config);
    assert!(target_arns.is_empty());

    let source = ReplicationDeleteStateSource {
        name: String::new(),
        user_tags: String::new(),
        version_id: None,
        delete_marker: false,
        replica: false,
    };
    assert!(delete_replication_state_from_config(&config, &source).is_none());
    assert!(replication_statuses_map("").is_empty());

    let source_replication_status = Default::default();
    let source_version_purge_status = Default::default();
    let input = ReplicationDeleteScheduleInput {
        replication_request: false,
        version_id_requested: false,
        source_delete_marker: false,
        source_replication_status: &source_replication_status,
        source_version_purge_status: &source_version_purge_status,
        deleted_delete_marker_version: false,
    };
    assert!(!should_schedule_delete_replication(input));

    assert!(delete_replication_version_id(false, None, false).is_none());
    assert!(!should_remove_replication_target("arn", false, &target_arns));
    assert!(!should_use_existing_delete_replication_info(false, false));
    assert!(!should_use_existing_delete_replication_source(false, false, false));
    assert!(validate_replication_config_target_arns(std::iter::empty(), &config).is_ok());

    assert_eq!(REPLICATE_INCOMING_DELETE, "replicate:incoming:delete");
    assert!(type_name::<ReplicateDecision>().contains("ReplicateDecision"));
    assert!(type_name::<ReplicationState>().contains("ReplicationState"));
    assert!(type_name::<ReplicationTargetValidationError>().contains("ReplicationTargetValidationError"));
}
