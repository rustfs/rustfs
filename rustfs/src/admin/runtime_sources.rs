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

pub(crate) use crate::app::context::{
    AppContext, get_global_app_context, publish_server_config, publish_storage_class_config, resolve_action_credentials,
    resolve_boot_time, resolve_bucket_metadata_handle, resolve_bucket_monitor_handle, resolve_deployment_id,
    resolve_endpoints_handle, resolve_iam_handle, resolve_kms_runtime_service_manager, resolve_notification_system,
    resolve_object_store_handle, resolve_object_store_handle_for_context, resolve_oidc_handle,
    resolve_or_init_kms_runtime_service_manager, resolve_outbound_tls_generation, resolve_outbound_tls_state,
    resolve_ready_iam_handle, resolve_region, resolve_replication_pool_handle, resolve_replication_stats_handle,
    resolve_runtime_port, resolve_scanner_metrics_report, resolve_server_config, resolve_token_signing_key,
};

#[cfg(test)]
pub(crate) use crate::app::context::set_test_outbound_tls_generation;
