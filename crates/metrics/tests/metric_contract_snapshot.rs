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

use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::path::PathBuf;

const SNAPSHOT_FIXTURE: &str = "tests/fixtures/metric_contract_snapshot.json";
const UPDATE_ENV_KEY: &str = "RUSTFS_UPDATE_METRIC_CONTRACT_SNAPSHOT";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct MetricContractEntry {
    name: String,
    metric_type: String,
    help: String,
    labels: Vec<String>,
}

#[test]
fn metric_contract_snapshot_matches_fixture() {
    let actual = build_metric_contract_snapshot();
    let fixture_path = fixture_path();

    if should_update_snapshot() {
        let payload = serde_json::to_string_pretty(&actual).expect("serialize metric snapshot");
        fs::write(&fixture_path, format!("{payload}\n")).expect("write metric contract snapshot fixture");
    }

    let expected_raw = fs::read_to_string(&fixture_path).unwrap_or_else(|err| {
        panic!(
            "failed to read fixture {}: {err}. Set {UPDATE_ENV_KEY}=1 and rerun to create it.",
            fixture_path.display()
        )
    });
    let expected: Vec<MetricContractEntry> = serde_json::from_str(&expected_raw).expect("parse metric contract snapshot fixture");

    assert_eq!(
        actual, expected,
        "metric descriptor contract changed; update fixture only with explicit review"
    );
}

fn build_metric_contract_snapshot() -> Vec<MetricContractEntry> {
    let mut entries = all_metric_descriptors()
        .into_iter()
        .map(|descriptor| MetricContractEntry {
            name: descriptor.get_full_metric_name(),
            metric_type: descriptor.metric_type.as_str().to_string(),
            help: descriptor.help.clone(),
            labels: descriptor.variable_labels.clone(),
        })
        .collect::<Vec<_>>();

    entries.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| left.metric_type.cmp(&right.metric_type))
            .then_with(|| left.help.cmp(&right.help))
            .then_with(|| left.labels.cmp(&right.labels))
    });

    entries
}

fn all_metric_descriptors() -> Vec<&'static rustfs_metrics::MetricDescriptor> {
    vec![
        &rustfs_metrics::audit::AUDIT_FAILED_MESSAGES_MD,
        &rustfs_metrics::audit::AUDIT_TARGET_QUEUE_LENGTH_MD,
        &rustfs_metrics::audit::AUDIT_TOTAL_MESSAGES_MD,
        &rustfs_metrics::bucket::BUCKET_API_REQUESTS_4XX_ERRORS_MD,
        &rustfs_metrics::bucket::BUCKET_API_REQUESTS_5XX_ERRORS_MD,
        &rustfs_metrics::bucket::BUCKET_API_REQUESTS_CANCELED_MD,
        &rustfs_metrics::bucket::BUCKET_API_REQUESTS_IN_FLIGHT_MD,
        &rustfs_metrics::bucket::BUCKET_API_REQUESTS_TOTAL_MD,
        &rustfs_metrics::bucket::BUCKET_API_REQUESTS_TTFB_SECONDS_DISTRIBUTION_MD,
        &rustfs_metrics::bucket::BUCKET_API_TRAFFIC_RECV_BYTES_MD,
        &rustfs_metrics::bucket::BUCKET_API_TRAFFIC_SENT_BYTES_MD,
        &rustfs_metrics::bucket_replication::BUCKET_REPL_BANDWIDTH_CURRENT_MD,
        &rustfs_metrics::bucket_replication::BUCKET_REPL_BANDWIDTH_LIMIT_MD,
        &rustfs_metrics::bucket_replication::BUCKET_REPL_LAST_HR_FAILED_BYTES_MD,
        &rustfs_metrics::bucket_replication::BUCKET_REPL_LAST_HR_FAILED_COUNT_MD,
        &rustfs_metrics::bucket_replication::BUCKET_REPL_LAST_MIN_FAILED_BYTES_MD,
        &rustfs_metrics::bucket_replication::BUCKET_REPL_LAST_MIN_FAILED_COUNT_MD,
        &rustfs_metrics::bucket_replication::BUCKET_REPL_LATENCY_MS_MD,
        &rustfs_metrics::bucket_replication::BUCKET_REPL_PROXIED_DELETE_TAGGING_REQUESTS_FAILURES_MD,
        &rustfs_metrics::bucket_replication::BUCKET_REPL_PROXIED_DELETE_TAGGING_REQUESTS_TOTAL_MD,
        &rustfs_metrics::bucket_replication::BUCKET_REPL_PROXIED_GET_REQUESTS_FAILURES_MD,
        &rustfs_metrics::bucket_replication::BUCKET_REPL_PROXIED_GET_REQUESTS_TOTAL_MD,
        &rustfs_metrics::bucket_replication::BUCKET_REPL_PROXIED_GET_TAGGING_REQUESTS_FAILURES_MD,
        &rustfs_metrics::bucket_replication::BUCKET_REPL_PROXIED_GET_TAGGING_REQUESTS_TOTAL_MD,
        &rustfs_metrics::bucket_replication::BUCKET_REPL_PROXIED_HEAD_REQUESTS_FAILURES_MD,
        &rustfs_metrics::bucket_replication::BUCKET_REPL_PROXIED_HEAD_REQUESTS_TOTAL_MD,
        &rustfs_metrics::bucket_replication::BUCKET_REPL_PROXIED_PUT_TAGGING_REQUESTS_FAILURES_MD,
        &rustfs_metrics::bucket_replication::BUCKET_REPL_PROXIED_PUT_TAGGING_REQUESTS_TOTAL_MD,
        &rustfs_metrics::bucket_replication::BUCKET_REPL_SENT_BYTES_MD,
        &rustfs_metrics::bucket_replication::BUCKET_REPL_SENT_COUNT_MD,
        &rustfs_metrics::bucket_replication::BUCKET_REPL_TOTAL_FAILED_BYTES_MD,
        &rustfs_metrics::bucket_replication::BUCKET_REPL_TOTAL_FAILED_COUNT_MD,
        &rustfs_metrics::cluster::CLUSTER_BUCKETS_TOTAL_MD,
        &rustfs_metrics::cluster::CLUSTER_CAPACITY_FREE_BYTES_MD,
        &rustfs_metrics::cluster::CLUSTER_CAPACITY_RAW_TOTAL_BYTES_MD,
        &rustfs_metrics::cluster::CLUSTER_CAPACITY_USABLE_TOTAL_BYTES_MD,
        &rustfs_metrics::cluster::CLUSTER_CAPACITY_USED_BYTES_MD,
        &rustfs_metrics::cluster::CLUSTER_OBJECTS_TOTAL_MD,
        &rustfs_metrics::cluster_config::CONFIG_RRS_PARITY_MD,
        &rustfs_metrics::cluster_config::CONFIG_STANDARD_PARITY_MD,
        &rustfs_metrics::cluster_erasure_set::ERASURE_SET_DATA_SHARDS_MD,
        &rustfs_metrics::cluster_erasure_set::ERASURE_SET_HEALING_DRIVES_COUNT_MD,
        &rustfs_metrics::cluster_erasure_set::ERASURE_SET_HEALTH_MD,
        &rustfs_metrics::cluster_erasure_set::ERASURE_SET_ONLINE_DRIVES_COUNT_MD,
        &rustfs_metrics::cluster_erasure_set::ERASURE_SET_OVERALL_HEALTH_MD,
        &rustfs_metrics::cluster_erasure_set::ERASURE_SET_OVERALL_WRITE_QUORUM_MD,
        &rustfs_metrics::cluster_erasure_set::ERASURE_SET_PARITY_MD,
        &rustfs_metrics::cluster_erasure_set::ERASURE_SET_READ_HEALTH_MD,
        &rustfs_metrics::cluster_erasure_set::ERASURE_SET_READ_QUORUM_MD,
        &rustfs_metrics::cluster_erasure_set::ERASURE_SET_READ_TOLERANCE_MD,
        &rustfs_metrics::cluster_erasure_set::ERASURE_SET_SIZE_MD,
        &rustfs_metrics::cluster_erasure_set::ERASURE_SET_WRITE_HEALTH_MD,
        &rustfs_metrics::cluster_erasure_set::ERASURE_SET_WRITE_QUORUM_MD,
        &rustfs_metrics::cluster_erasure_set::ERASURE_SET_WRITE_TOLERANCE_MD,
        &rustfs_metrics::cluster_health::HEALTH_DRIVES_COUNT_MD,
        &rustfs_metrics::cluster_health::HEALTH_DRIVES_OFFLINE_COUNT_MD,
        &rustfs_metrics::cluster_health::HEALTH_DRIVES_ONLINE_COUNT_MD,
        &rustfs_metrics::cluster_iam::LAST_SYNC_DURATION_MILLIS_MD,
        &rustfs_metrics::cluster_iam::PLUGIN_AUTHN_SERVICE_FAILED_REQUESTS_MINUTE_MD,
        &rustfs_metrics::cluster_iam::PLUGIN_AUTHN_SERVICE_LAST_FAIL_SECONDS_MD,
        &rustfs_metrics::cluster_iam::PLUGIN_AUTHN_SERVICE_LAST_SUCC_SECONDS_MD,
        &rustfs_metrics::cluster_iam::PLUGIN_AUTHN_SERVICE_SUCC_AVG_RTT_MS_MINUTE_MD,
        &rustfs_metrics::cluster_iam::PLUGIN_AUTHN_SERVICE_SUCC_MAX_RTT_MS_MINUTE_MD,
        &rustfs_metrics::cluster_iam::PLUGIN_AUTHN_SERVICE_TOTAL_REQUESTS_MINUTE_MD,
        &rustfs_metrics::cluster_iam::SINCE_LAST_SYNC_MILLIS_MD,
        &rustfs_metrics::cluster_iam::SYNC_FAILURES_MD,
        &rustfs_metrics::cluster_iam::SYNC_SUCCESSES_MD,
        &rustfs_metrics::cluster_notification::NOTIFICATION_CURRENT_SEND_IN_PROGRESS_MD,
        &rustfs_metrics::cluster_notification::NOTIFICATION_EVENTS_ERRORS_TOTAL_MD,
        &rustfs_metrics::cluster_notification::NOTIFICATION_EVENTS_SENT_TOTAL_MD,
        &rustfs_metrics::cluster_notification::NOTIFICATION_EVENTS_SKIPPED_TOTAL_MD,
        &rustfs_metrics::cluster_usage::USAGE_BUCKETS_COUNT_MD,
        &rustfs_metrics::cluster_usage::USAGE_BUCKET_DELETE_MARKERS_COUNT_MD,
        &rustfs_metrics::cluster_usage::USAGE_BUCKET_OBJECTS_TOTAL_MD,
        &rustfs_metrics::cluster_usage::USAGE_BUCKET_OBJECT_SIZE_DISTRIBUTION_MD,
        &rustfs_metrics::cluster_usage::USAGE_BUCKET_OBJECT_VERSION_COUNT_DISTRIBUTION_MD,
        &rustfs_metrics::cluster_usage::USAGE_BUCKET_QUOTA_TOTAL_BYTES_MD,
        &rustfs_metrics::cluster_usage::USAGE_BUCKET_TOTAL_BYTES_MD,
        &rustfs_metrics::cluster_usage::USAGE_BUCKET_VERSIONS_COUNT_MD,
        &rustfs_metrics::cluster_usage::USAGE_DELETE_MARKERS_COUNT_MD,
        &rustfs_metrics::cluster_usage::USAGE_OBJECTS_COUNT_MD,
        &rustfs_metrics::cluster_usage::USAGE_OBJECTS_DISTRIBUTION_MD,
        &rustfs_metrics::cluster_usage::USAGE_SINCE_LAST_UPDATE_SECONDS_MD,
        &rustfs_metrics::cluster_usage::USAGE_TOTAL_BYTES_MD,
        &rustfs_metrics::cluster_usage::USAGE_VERSIONS_COUNT_MD,
        &rustfs_metrics::cluster_usage::USAGE_VERSIONS_DISTRIBUTION_MD,
        &rustfs_metrics::ilm::ILM_EXPIRY_PENDING_TASKS_MD,
        &rustfs_metrics::ilm::ILM_TRANSITION_ACTIVE_TASKS_MD,
        &rustfs_metrics::ilm::ILM_TRANSITION_MISSED_IMMEDIATE_TASKS_MD,
        &rustfs_metrics::ilm::ILM_TRANSITION_PENDING_TASKS_MD,
        &rustfs_metrics::ilm::ILM_VERSIONS_SCANNED_MD,
        &rustfs_metrics::node_bucket::BUCKET_OBJECTS_TOTAL_MD,
        &rustfs_metrics::node_bucket::BUCKET_QUOTA_BYTES_MD,
        &rustfs_metrics::node_bucket::BUCKET_USAGE_BYTES_MD,
        &rustfs_metrics::node_disk::NODE_DISK_FREE_BYTES_MD,
        &rustfs_metrics::node_disk::NODE_DISK_TOTAL_BYTES_MD,
        &rustfs_metrics::node_disk::NODE_DISK_USED_BYTES_MD,
        &rustfs_metrics::notification_target::NOTIFICATION_TARGET_FAILED_MESSAGES_MD,
        &rustfs_metrics::notification_target::NOTIFICATION_TARGET_QUEUE_LENGTH_MD,
        &rustfs_metrics::notification_target::NOTIFICATION_TARGET_TOTAL_MESSAGES_MD,
        &rustfs_metrics::process_resource::PROCESS_CPU_PERCENT_MD,
        &rustfs_metrics::process_resource::PROCESS_MEMORY_BYTES_MD,
        &rustfs_metrics::process_resource::PROCESS_UPTIME_SECONDS_MD,
        &rustfs_metrics::replication::REPLICATION_AVERAGE_ACTIVE_WORKERS_MD,
        &rustfs_metrics::replication::REPLICATION_AVERAGE_DATA_TRANSFER_RATE_MD,
        &rustfs_metrics::replication::REPLICATION_AVERAGE_QUEUED_BYTES_MD,
        &rustfs_metrics::replication::REPLICATION_AVERAGE_QUEUED_COUNT_MD,
        &rustfs_metrics::replication::REPLICATION_CURRENT_ACTIVE_WORKERS_MD,
        &rustfs_metrics::replication::REPLICATION_CURRENT_DATA_TRANSFER_RATE_MD,
        &rustfs_metrics::replication::REPLICATION_LAST_MINUTE_QUEUED_BYTES_MD,
        &rustfs_metrics::replication::REPLICATION_LAST_MINUTE_QUEUED_COUNT_MD,
        &rustfs_metrics::replication::REPLICATION_MAX_ACTIVE_WORKERS_MD,
        &rustfs_metrics::replication::REPLICATION_MAX_DATA_TRANSFER_RATE_MD,
        &rustfs_metrics::replication::REPLICATION_MAX_QUEUED_BYTES_MD,
        &rustfs_metrics::replication::REPLICATION_MAX_QUEUED_COUNT_MD,
        &rustfs_metrics::replication::REPLICATION_RECENT_BACKLOG_COUNT_MD,
        &rustfs_metrics::request::API_REJECTED_AUTH_TOTAL_MD,
        &rustfs_metrics::request::API_REJECTED_HEADER_TOTAL_MD,
        &rustfs_metrics::request::API_REJECTED_INVALID_TOTAL_MD,
        &rustfs_metrics::request::API_REJECTED_TIMESTAMP_TOTAL_MD,
        &rustfs_metrics::request::API_REQUESTS_4XX_ERRORS_TOTAL_MD,
        &rustfs_metrics::request::API_REQUESTS_5XX_ERRORS_TOTAL_MD,
        &rustfs_metrics::request::API_REQUESTS_CANCELED_TOTAL_MD,
        &rustfs_metrics::request::API_REQUESTS_ERRORS_TOTAL_MD,
        &rustfs_metrics::request::API_REQUESTS_INCOMING_TOTAL_MD,
        &rustfs_metrics::request::API_REQUESTS_IN_FLIGHT_TOTAL_MD,
        &rustfs_metrics::request::API_REQUESTS_TOTAL_MD,
        &rustfs_metrics::request::API_REQUESTS_TTFB_SECONDS_DISTRIBUTION_MD,
        &rustfs_metrics::request::API_REQUESTS_WAITING_TOTAL_MD,
        &rustfs_metrics::request::API_TRAFFIC_RECV_BYTES_MD,
        &rustfs_metrics::request::API_TRAFFIC_SENT_BYTES_MD,
        &rustfs_metrics::scanner::SCANNER_BUCKET_SCANS_FINISHED_MD,
        &rustfs_metrics::scanner::SCANNER_BUCKET_SCANS_STARTED_MD,
        &rustfs_metrics::scanner::SCANNER_DIRECTORIES_SCANNED_MD,
        &rustfs_metrics::scanner::SCANNER_LAST_ACTIVITY_SECONDS_MD,
        &rustfs_metrics::scanner::SCANNER_OBJECTS_SCANNED_MD,
        &rustfs_metrics::scanner::SCANNER_VERSIONS_SCANNED_MD,
        &rustfs_metrics::system_cpu::SYS_CPU_AVG_IDLE_MD,
        &rustfs_metrics::system_cpu::SYS_CPU_AVG_IOWAIT_MD,
        &rustfs_metrics::system_cpu::SYS_CPU_LOAD_MD,
        &rustfs_metrics::system_cpu::SYS_CPU_LOAD_PERC_MD,
        &rustfs_metrics::system_cpu::SYS_CPU_NICE_MD,
        &rustfs_metrics::system_cpu::SYS_CPU_STEAL_MD,
        &rustfs_metrics::system_cpu::SYS_CPU_SYSTEM_MD,
        &rustfs_metrics::system_cpu::SYS_CPU_USER_MD,
        &rustfs_metrics::system_drive::DRIVE_API_LATENCY_MD,
        &rustfs_metrics::system_drive::DRIVE_AVAILABILITY_ERRORS_MD,
        &rustfs_metrics::system_drive::DRIVE_COUNT_MD,
        &rustfs_metrics::system_drive::DRIVE_FREE_BYTES_MD,
        &rustfs_metrics::system_drive::DRIVE_FREE_INODES_MD,
        &rustfs_metrics::system_drive::DRIVE_HEALTH_MD,
        &rustfs_metrics::system_drive::DRIVE_IO_ERRORS_MD,
        &rustfs_metrics::system_drive::DRIVE_OFFLINE_COUNT_MD,
        &rustfs_metrics::system_drive::DRIVE_ONLINE_COUNT_MD,
        &rustfs_metrics::system_drive::DRIVE_PERC_UTIL_MD,
        &rustfs_metrics::system_drive::DRIVE_READS_AWAIT_MD,
        &rustfs_metrics::system_drive::DRIVE_READS_KB_PER_SEC_MD,
        &rustfs_metrics::system_drive::DRIVE_READS_PER_SEC_MD,
        &rustfs_metrics::system_drive::DRIVE_TIMEOUT_ERRORS_MD,
        &rustfs_metrics::system_drive::DRIVE_TOTAL_BYTES_MD,
        &rustfs_metrics::system_drive::DRIVE_TOTAL_INODES_MD,
        &rustfs_metrics::system_drive::DRIVE_USED_BYTES_MD,
        &rustfs_metrics::system_drive::DRIVE_USED_INODES_MD,
        &rustfs_metrics::system_drive::DRIVE_WAITING_IO_MD,
        &rustfs_metrics::system_drive::DRIVE_WRITES_AWAIT_MD,
        &rustfs_metrics::system_drive::DRIVE_WRITES_KB_PER_SEC_MD,
        &rustfs_metrics::system_drive::DRIVE_WRITES_PER_SEC_MD,
        &rustfs_metrics::system_gpu::PROCESS_GPU_MEMORY_USAGE_MD,
        &rustfs_metrics::system_memory::MEM_AVAILABLE_MD,
        &rustfs_metrics::system_memory::MEM_BUFFERS_MD,
        &rustfs_metrics::system_memory::MEM_CACHE_MD,
        &rustfs_metrics::system_memory::MEM_FREE_MD,
        &rustfs_metrics::system_memory::MEM_SHARED_MD,
        &rustfs_metrics::system_memory::MEM_TOTAL_MD,
        &rustfs_metrics::system_memory::MEM_USED_MD,
        &rustfs_metrics::system_memory::MEM_USED_PERC_MD,
        &rustfs_metrics::system_network::INTERNODE_DIAL_AVG_TIME_NANOS_MD,
        &rustfs_metrics::system_network::INTERNODE_DIAL_ERRORS_TOTAL_MD,
        &rustfs_metrics::system_network::INTERNODE_ERRORS_TOTAL_MD,
        &rustfs_metrics::system_network::INTERNODE_RECV_BYTES_TOTAL_MD,
        &rustfs_metrics::system_network::INTERNODE_SENT_BYTES_TOTAL_MD,
        &rustfs_metrics::system_process::PROCESS_CPU_TOTAL_SECONDS_MD,
        &rustfs_metrics::system_process::PROCESS_CPU_USAGE_MD,
        &rustfs_metrics::system_process::PROCESS_CPU_UTILIZATION_MD,
        &rustfs_metrics::system_process::PROCESS_DISK_IO_MD,
        &rustfs_metrics::system_process::PROCESS_FILE_DESCRIPTOR_LIMIT_TOTAL_MD,
        &rustfs_metrics::system_process::PROCESS_FILE_DESCRIPTOR_OPEN_TOTAL_MD,
        &rustfs_metrics::system_process::PROCESS_GO_ROUTINE_TOTAL_MD,
        &rustfs_metrics::system_process::PROCESS_IO_RCHAR_BYTES_MD,
        &rustfs_metrics::system_process::PROCESS_IO_READ_BYTES_MD,
        &rustfs_metrics::system_process::PROCESS_IO_WCHAR_BYTES_MD,
        &rustfs_metrics::system_process::PROCESS_IO_WRITE_BYTES_MD,
        &rustfs_metrics::system_process::PROCESS_LOCKS_READ_TOTAL_MD,
        &rustfs_metrics::system_process::PROCESS_LOCKS_WRITE_TOTAL_MD,
        &rustfs_metrics::system_process::PROCESS_NETWORK_IO_MD,
        &rustfs_metrics::system_process::PROCESS_NETWORK_IO_PER_INTERFACE_MD,
        &rustfs_metrics::system_process::PROCESS_RESIDENT_MEMORY_BYTES_MD,
        &rustfs_metrics::system_process::PROCESS_START_TIME_SECONDS_MD,
        &rustfs_metrics::system_process::PROCESS_STATUS_MD,
        &rustfs_metrics::system_process::PROCESS_SYSCALL_READ_TOTAL_MD,
        &rustfs_metrics::system_process::PROCESS_SYSCALL_WRITE_TOTAL_MD,
        &rustfs_metrics::system_process::PROCESS_UPTIME_SECONDS_MD,
        &rustfs_metrics::system_process::PROCESS_VIRTUAL_MEMORY_BYTES_MD,
        &rustfs_metrics::system_process::PROCESS_VIRTUAL_MEMORY_MAX_BYTES_MD,
    ]
}

fn should_update_snapshot() -> bool {
    matches!(env::var(UPDATE_ENV_KEY).ok().as_deref(), Some("1") | Some("true") | Some("yes"))
}

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(SNAPSHOT_FIXTURE)
}
