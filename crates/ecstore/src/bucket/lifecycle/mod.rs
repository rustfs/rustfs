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

pub mod bucket_lifecycle_audit;
pub mod bucket_lifecycle_ops;
mod config_boundary;
pub mod core;
mod durable_namespace;
pub mod evaluator;
pub mod manual_transition_job;
mod metadata_boundary;
pub(crate) use metadata_boundary::{LifecycleExpiryConfigs, get_expiry_configs, get_lifecycle_config};
mod object_handlers_common;
mod object_lock_boundary;
pub use self::core as lifecycle;
pub mod recovery_control;
pub mod recovery_disposition;
pub(crate) mod recovery_disposition_runtime;
pub mod recovery_export;
mod replication_sink;
pub mod rule;
mod runtime_boundary;
pub mod tier_delete_journal;
pub mod tier_free_version_recovery;
pub mod tier_last_day_stats;
pub mod tier_sweeper;
pub mod transition_transaction;

pub(crate) use durable_namespace::{
    DurableIlmRecordCheckpoint, ILM_META_PREFIX, TIER_DELETE_DISPATCH_MANIFEST_NAMESPACE, ValidatedDurableIlmRecord,
    classify_durable_ilm_record, validate_durable_ilm_record,
};
