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
use std::time::Duration;

use crate::init::{init_auto_tuner, init_update_check, print_server_info};
use crate::startup_runtime_sources;
use crate::storage_api::startup::storage::{ECStore, init_compression_total_memory_from_backend};
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

const GET_STAGE_LOCAL_SUMMARY_FILE: &str = "/var/log/rustfs/get-stage-summary.log";

pub(crate) async fn init_observability_runtime(store: Arc<ECStore>, ctx: CancellationToken) {
    print_server_info();
    init_update_check();
    crate::allocator_reclaim::init_allocator_reclaim(ctx.clone());

    let metrics_enabled = startup_runtime_sources::observability_metric_enabled();
    configure_metric_gates(metrics_enabled);

    if rustfs_io_metrics::get_stage_local_summary_enabled() {
        init_get_stage_local_summary_runtime(ctx.clone());
    }

    if metrics_enabled {
        // Load persisted compression stats into memory early, before any PUTs can occur.
        init_compression_total_memory_from_backend(store).await;
        startup_runtime_sources::init_metrics_runtime(ctx.clone());
        crate::memory_observability::init_memory_observability(ctx.clone());
        init_auto_tuner(ctx).await;
    }
}

fn configure_metric_gates(metrics_enabled: bool) {
    let local_summary_enabled = !metrics_enabled
        && rustfs_utils::get_env_bool(rustfs_config::observability::ENV_OBS_GET_STAGE_LOCAL_SUMMARY_ENABLED, false);
    let sample_rate =
        rustfs_utils::get_env_u64(rustfs_config::observability::ENV_OBS_GET_STAGE_LOCAL_SUMMARY_SAMPLE_RATE, 64).max(1);
    startup_runtime_sources::set_get_stage_local_summary_enabled(local_summary_enabled);
    startup_runtime_sources::set_get_stage_local_summary_sample_rate(sample_rate);
    let put_stage_metrics_enabled = metrics_enabled
        && rustfs_utils::get_env_bool(
            rustfs_config::observability::ENV_OBS_PUT_STAGE_METRICS_ENABLED,
            rustfs_config::DEFAULT_OBS_PUT_STAGE_METRICS_ENABLED,
        );
    startup_runtime_sources::set_put_stage_metrics_enabled(put_stage_metrics_enabled);
    startup_runtime_sources::set_get_stage_metrics_enabled(metrics_enabled || local_summary_enabled);
    startup_runtime_sources::set_metrics_enabled(metrics_enabled);
}

fn init_get_stage_local_summary_runtime(ctx: CancellationToken) {
    let interval_secs =
        rustfs_utils::get_env_u64(rustfs_config::observability::ENV_OBS_GET_STAGE_LOCAL_SUMMARY_INTERVAL_SECS, 15).max(1);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
        interval.tick().await;
        loop {
            tokio::select! {
                _ = ctx.cancelled() => break,
                _ = interval.tick() => {
                    let rows = rustfs_io_metrics::take_get_stage_local_summary();
                    if rows.is_empty() {
                        continue;
                    }
                    let summary = rows.iter().map(|row| {
                        format!("{}|{}|{}|{}|{}|{}", row.path, row.stage, row.object_class, row.size_bucket, row.sampled_count, row.duration_nanoseconds)
                    }).collect::<Vec<_>>().join(";");
                    let timestamp_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis();
                    let line = format!(
                        "timestamp_ms={timestamp_ms}\tsample_rate={}\tstage_count={}\tstages={summary}\n",
                        rustfs_io_metrics::get_stage_local_summary_sample_rate(),
                        rows.len(),
                    );
                    let Ok(mut file) = tokio::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(GET_STAGE_LOCAL_SUMMARY_FILE)
                        .await
                    else {
                        break;
                    };
                    if file.write_all(line.as_bytes()).await.is_err() {
                        break;
                    }
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    const PUT_STAGE_ENV: &str = rustfs_config::observability::ENV_OBS_PUT_STAGE_METRICS_ENABLED;
    const LOCAL_SUMMARY_ENV: &str = rustfs_config::observability::ENV_OBS_GET_STAGE_LOCAL_SUMMARY_ENABLED;
    const LOCAL_SUMMARY_RATE_ENV: &str = rustfs_config::observability::ENV_OBS_GET_STAGE_LOCAL_SUMMARY_SAMPLE_RATE;

    #[test]
    #[serial_test::serial]
    fn get_stage_local_summary_can_run_without_metrics_export() {
        let previous_metrics = rustfs_io_metrics::metrics_enabled();
        let previous_get_stages = rustfs_io_metrics::get_stage_metrics_enabled();
        let previous_local_summary = rustfs_io_metrics::get_stage_local_summary_enabled();
        let previous_sample_rate = rustfs_io_metrics::get_stage_local_summary_sample_rate();

        temp_env::with_var(LOCAL_SUMMARY_ENV, Some("true"), || {
            temp_env::with_var(LOCAL_SUMMARY_RATE_ENV, Some("1"), || {
                configure_metric_gates(false);
                assert!(!rustfs_io_metrics::metrics_enabled());
                assert!(rustfs_io_metrics::get_stage_metrics_enabled());
                assert!(rustfs_io_metrics::get_stage_local_summary_enabled());
                rustfs_io_metrics::take_get_stage_local_summary();

                rustfs_io_metrics::record_get_object_stage_duration("legacy_duplex", "metadata", 0.001);
                rustfs_io_metrics::record_get_object_stage_duration("legacy_duplex", "metadata", 0.003);
                rustfs_io_metrics::record_get_object_stage_duration_by_size(
                    "legacy_duplex",
                    "inline_direct.decode",
                    "plain_single_part",
                    "le_4kib",
                    0.002,
                );

                let rows = rustfs_io_metrics::take_get_stage_local_summary();
                let metadata = rows
                    .iter()
                    .find(|row| row.path == "legacy_duplex" && row.stage == "metadata")
                    .expect("local summary should retain sampled metadata observations");
                assert_eq!(metadata.sampled_count, 2);
                assert_eq!(metadata.duration_nanoseconds, 4_000_000);
                assert!(rows.iter().any(|row| row.size_bucket == "le_4kib"));
            });
        });

        startup_runtime_sources::set_metrics_enabled(previous_metrics);
        startup_runtime_sources::set_get_stage_metrics_enabled(previous_get_stages);
        startup_runtime_sources::set_get_stage_local_summary_enabled(previous_local_summary);
        startup_runtime_sources::set_get_stage_local_summary_sample_rate(previous_sample_rate);
    }

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
