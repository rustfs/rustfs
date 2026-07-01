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
    BucketStats, DeletedObjectReplicationInfo, DynReplicationPool, ObjectOpts, ReplicationConfigurationExt,
    ReplicationObjectBridge, ReplicationStats, ResyncStatusType,
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
