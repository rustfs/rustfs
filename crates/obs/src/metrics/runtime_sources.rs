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

use crate::metrics::{
    ObsBucketBandwidthMonitor, obs_expiry_state_handle, obs_get_global_bucket_monitor, obs_transition_state_handle,
};
use rustfs_iam::{get_global_iam_sys, oidc::oidc_plugin_authn_metrics_snapshot};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ObsIlmRuntimeSnapshot {
    pub(crate) expiry_pending_tasks: u64,
    pub(crate) transition_active_tasks: u64,
    pub(crate) transition_pending_tasks: u64,
    pub(crate) transition_missed_immediate_tasks: u64,
    pub(crate) transition_queue_full_tasks: u64,
    pub(crate) transition_queue_send_timeout_tasks: u64,
    pub(crate) transition_compensation_scheduled_tasks: u64,
    pub(crate) transition_compensation_running_tasks: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ObsIamMetricsSnapshot {
    pub(crate) last_sync_duration_millis: u64,
    pub(crate) plugin_authn_service_failed_requests_minute: u64,
    pub(crate) plugin_authn_service_last_fail_seconds: u64,
    pub(crate) plugin_authn_service_last_succ_seconds: u64,
    pub(crate) plugin_authn_service_succ_avg_rtt_ms_minute: u64,
    pub(crate) plugin_authn_service_succ_max_rtt_ms_minute: u64,
    pub(crate) plugin_authn_service_total_requests_minute: u64,
    pub(crate) since_last_sync_millis: u64,
    pub(crate) sync_failures: u64,
    pub(crate) sync_successes: u64,
}

fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn i64_to_u64_floor_zero(value: i64) -> u64 {
    u64::try_from(value.max(0)).unwrap_or_default()
}

pub(crate) fn bucket_monitor_handle() -> Option<Arc<ObsBucketBandwidthMonitor>> {
    obs_get_global_bucket_monitor()
}

pub(crate) fn bucket_monitor_available() -> bool {
    bucket_monitor_handle().is_some()
}

pub(crate) async fn ilm_runtime_snapshot() -> ObsIlmRuntimeSnapshot {
    let expiry_state = obs_expiry_state_handle();
    let transition_state = obs_transition_state_handle();

    ObsIlmRuntimeSnapshot {
        expiry_pending_tasks: usize_to_u64_saturating(expiry_state.read().await.pending_tasks()),
        transition_active_tasks: i64_to_u64_floor_zero(transition_state.active_tasks()),
        transition_pending_tasks: usize_to_u64_saturating(transition_state.pending_tasks()),
        transition_missed_immediate_tasks: i64_to_u64_floor_zero(transition_state.missed_immediate_tasks()),
        transition_queue_full_tasks: i64_to_u64_floor_zero(transition_state.queue_full_tasks()),
        transition_queue_send_timeout_tasks: i64_to_u64_floor_zero(transition_state.queue_send_timeout_tasks()),
        transition_compensation_scheduled_tasks: i64_to_u64_floor_zero(transition_state.compensation_scheduled_tasks()),
        transition_compensation_running_tasks: i64_to_u64_floor_zero(transition_state.compensation_running_tasks()),
    }
}

pub(crate) fn iam_metrics_snapshot() -> Option<ObsIamMetricsSnapshot> {
    let iam_sys = get_global_iam_sys()?;
    let sync = iam_sys.sync_metrics_snapshot();
    let oidc = oidc_plugin_authn_metrics_snapshot();

    Some(ObsIamMetricsSnapshot {
        last_sync_duration_millis: sync.last_sync_duration_millis,
        plugin_authn_service_failed_requests_minute: oidc.failed_requests_minute,
        plugin_authn_service_last_fail_seconds: oidc.last_fail_seconds,
        plugin_authn_service_last_succ_seconds: oidc.last_succ_seconds,
        plugin_authn_service_succ_avg_rtt_ms_minute: oidc.succ_avg_rtt_ms_minute,
        plugin_authn_service_succ_max_rtt_ms_minute: oidc.succ_max_rtt_ms_minute,
        plugin_authn_service_total_requests_minute: oidc.total_requests_minute,
        since_last_sync_millis: sync.since_last_sync_millis,
        sync_failures: sync.sync_failures,
        sync_successes: sync.sync_successes,
    })
}
