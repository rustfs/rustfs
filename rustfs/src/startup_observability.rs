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
use crate::storage_api::startup::storage::{ECStore, init_compression_total_memory_from_backend};
use tokio_util::sync::CancellationToken;

pub(crate) async fn init_observability_runtime(store: Arc<ECStore>, ctx: CancellationToken) {
    print_server_info();
    init_update_check();
    crate::allocator_reclaim::init_allocator_reclaim(ctx.clone());

    let metrics_enabled = startup_runtime_sources::observability_metric_enabled();
    configure_metric_gates(metrics_enabled);

    if metrics_enabled {
        // Load persisted compression stats into memory early, before any PUTs can occur.
        init_compression_total_memory_from_backend(store).await;
        startup_runtime_sources::init_metrics_runtime(ctx.clone());
        crate::memory_observability::init_memory_observability(ctx.clone());
        init_auto_tuner(ctx).await;
    }
}

fn configure_metric_gates(metrics_enabled: bool) {
    let put_stage_metrics_enabled = metrics_enabled
        && rustfs_utils::get_env_bool(
            rustfs_config::observability::ENV_OBS_PUT_STAGE_METRICS_ENABLED,
            rustfs_config::DEFAULT_OBS_PUT_STAGE_METRICS_ENABLED,
        );
    startup_runtime_sources::set_put_stage_metrics_enabled(put_stage_metrics_enabled);
    startup_runtime_sources::set_get_stage_metrics_enabled(metrics_enabled);
    startup_runtime_sources::set_metrics_enabled(metrics_enabled);
}

#[cfg(test)]
mod tests {
    use super::*;

    const PUT_STAGE_ENV: &str = rustfs_config::observability::ENV_OBS_PUT_STAGE_METRICS_ENABLED;

    #[test]
    #[serial_test::serial]
    fn put_stage_metrics_require_explicit_opt_in() {
        let previous_metrics = rustfs_io_metrics::metrics_enabled();
        let previous_get_stages = rustfs_io_metrics::get_stage_metrics_enabled();
        let previous_put_stages = rustfs_io_metrics::put_stage_metrics_enabled();

        temp_env::with_var(PUT_STAGE_ENV, None::<&str>, || {
            configure_metric_gates(true);
            assert!(rustfs_io_metrics::metrics_enabled());
            assert!(rustfs_io_metrics::get_stage_metrics_enabled());
            assert!(!rustfs_io_metrics::put_stage_metrics_enabled());
        });

        temp_env::with_var(PUT_STAGE_ENV, Some("true"), || {
            configure_metric_gates(true);
            assert!(rustfs_io_metrics::metrics_enabled());
            assert!(rustfs_io_metrics::get_stage_metrics_enabled());
            assert!(rustfs_io_metrics::put_stage_metrics_enabled());

            configure_metric_gates(false);
            assert!(!rustfs_io_metrics::metrics_enabled());
            assert!(!rustfs_io_metrics::get_stage_metrics_enabled());
            assert!(!rustfs_io_metrics::put_stage_metrics_enabled());
        });

        startup_runtime_sources::set_metrics_enabled(previous_metrics);
        startup_runtime_sources::set_get_stage_metrics_enabled(previous_get_stages);
        startup_runtime_sources::set_put_stage_metrics_enabled(previous_put_stages);
    }
}
