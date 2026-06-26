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

use crate::app::context;

pub(crate) use context::{
    AppContext, NotifyInterface, publish_oidc_handle, resolve_action_credentials, resolve_buffer_config,
    resolve_endpoints_handle, resolve_iam_ready, resolve_kms_runtime_service_manager, resolve_lock_clients_handle,
    resolve_notify_interface, resolve_object_store_handle, resolve_outbound_tls_generation, resolve_ready_iam_handle,
    resolve_region, resolve_replication_pool_handle, resolve_server_config,
};
