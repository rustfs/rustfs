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

use crate::app::context::get_global_app_context;
pub(crate) use crate::app::context::{
    AppContext, resolve_encryption_service, resolve_endpoints_handle, resolve_expiry_state_handle, resolve_notification_system,
    resolve_notify_interface_for_context, resolve_object_store_handle_for_context, resolve_s3select_db,
};
use std::sync::Arc;

#[cfg(test)]
pub(crate) use crate::app::context::resolve_tier_config_handle;

pub(crate) fn current_app_context() -> Option<Arc<AppContext>> {
    get_global_app_context()
}
