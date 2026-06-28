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

use std::sync::Arc;

use crate::init::{init_auto_tuner, init_update_check, print_server_info};
use crate::startup_runtime_sources;
use crate::storage::storage_api::ECStore;
use crate::storage::storage_api::ecstore_data_usage::init_compression_total_memory_from_backend;
use tokio_util::sync::CancellationToken;

pub(crate) async fn init_observability_runtime(store: Arc<ECStore>, ctx: CancellationToken) {
    print_server_info();
    init_update_check();
    crate::allocator_reclaim::init_allocator_reclaim(ctx.clone());

    if startup_runtime_sources::observability_metric_enabled() {
        // Load persisted compression stats into memory early, before any PUTs can occur.
        init_compression_total_memory_from_backend(store).await;
        startup_runtime_sources::set_put_stage_metrics_enabled(true);
        startup_runtime_sources::set_get_stage_metrics_enabled(true);
        startup_runtime_sources::init_metrics_runtime(ctx.clone());
        crate::memory_observability::init_memory_observability(ctx.clone());
        init_auto_tuner(ctx).await;
    }
}
