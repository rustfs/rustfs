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

pub mod arn;
pub mod catalog;
mod check;
pub mod config;
pub mod control_plane;
pub mod domain;
pub mod error;
pub mod manifest;
mod net;
pub mod plugin;
pub mod runtime;
pub mod store;
pub mod sys;
pub mod target;

pub use catalog::extension::{
    OPS_DIAGNOSTICS_EXTENSION_API_VERSION, S3_HOOK_EXTENSION_API_VERSION, TARGET_AUDIT_CAPABILITY, TARGET_NOTIFY_CAPABILITY,
    builtin_extension_schemas, builtin_ops_diagnostics_contract, builtin_ops_diagnostics_extension_schema,
    builtin_s3_hook_contract, builtin_s3_hook_extension_schema, builtin_target_extension_schemas,
    target_marketplace_extension_schema, target_runtime_boundary,
};
pub use check::{
    check_amqp_broker_available, check_kafka_broker_available, check_mqtt_broker_available, check_mqtt_broker_available_with_tls,
    check_mysql_server_available, check_nats_server_available, check_postgres_server_available, check_pulsar_broker_available,
    check_redis_server_available,
};
pub use config::{
    LegacyTargetInstanceDescriptor, TargetInstanceSourceClass, TargetInstanceSourceHints, TargetPluginInstance,
    TargetPluginInstanceCompatDescriptor, TargetPluginInstanceRecord, normalize_legacy_target_instances,
    normalize_legacy_target_instances_from_env, normalize_target_plugin_instances, normalize_target_plugin_instances_from_env,
};
pub use control_plane::{
    TargetPluginEnableState, TargetPluginExternalAction, TargetPluginExternalActionDecision, TargetPluginExternalActionError,
    TargetPluginExternalFlowGate, TargetPluginExternalFlowGateStatus, TargetPluginInstallState, TargetPluginInstallation,
    TargetPluginOperationalState, TargetPluginRevision, TargetPluginRuntimeState, builtin_target_plugin_installation,
    builtin_target_plugin_operational_state, external_target_plugin_installation, plan_external_target_plugin_action,
    rollback_target_plugin_installation, runtime_state_from_status_label,
};
pub use domain::TargetDomain;
pub use error::{StoreError, TargetError};
pub use manifest::{
    TargetPluginArtifactManifest, TargetPluginDistributionManifest, TargetPluginEntrypointKind,
    TargetPluginExternalRuntimeContract, TargetPluginManifest, TargetPluginMarketplaceManifest, TargetPluginPackaging,
    TargetPluginRuntimeTransport, builtin_target_marketplace_manifest, installable_target_marketplace_manifest,
};
pub use net::*;
pub use plugin::{
    BuiltinTargetAdminDescriptor, BuiltinTargetDescriptor, TargetAdminMetadata, TargetPluginDescriptor, TargetPluginRegistry,
    TargetRequestValidator, boxed_target,
};
pub use runtime::{
    ReplayEvent, ReplayWorkerManager, RuntimeActivation, RuntimeStatusSnapshot, RuntimeTargetHealthSnapshot,
    RuntimeTargetHealthState, RuntimeTargetSnapshot, SharedTarget, TargetRuntimeManager, activate_targets_with_replay,
    adapter::{BuiltinPluginRuntimeAdapter, PluginRuntimeAdapter},
    init_target_and_optionally_start_replay,
    ops_diagnostics::{
        OpsDiagnosticsAccessDecision, OpsDiagnosticsReadRequest, OpsDiagnosticsRegistration, OpsDiagnosticsRegistry,
        OpsDiagnosticsRegistryError,
    },
    s3_hooks::{S3HookContext, S3HookDecision, S3HookRegistration, S3HookRegistry, S3HookRegistryError},
    sidecar::{SidecarPluginRuntime, SidecarRuntimePolicy, SidecarRuntimePolicyError, SidecarRuntimeSafetyChecks},
    sidecar_protocol::{SIDECAR_RUNTIME_PROTOCOL_VERSION, SidecarHandshake, SidecarPluginCapability},
    start_replay_worker,
};
pub use rustfs_s3_types::EventName;
use serde::{Deserialize, Serialize};
pub use sys::user_agent::*;
pub use target::{Target, TargetDeliverySnapshot};

/// Represents a log of events for sending to targets
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct TargetLog<E> {
    /// The event name
    pub event_name: EventName,
    /// The object key
    pub key: String,
    /// The list of events
    pub records: Vec<E>,
}
