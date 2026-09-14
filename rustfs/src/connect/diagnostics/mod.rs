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

mod inspect;
mod job;
pub(crate) mod job_delivery;
mod logs;
mod perf_client;
mod perf_drive;
mod perf_network;
mod perf_object;
mod perf_site_replication;
mod profile_cpu;
mod profile_memory;
mod profile_threads;
mod receipt_delivery;
mod schedule;
mod top_api;
mod top_disk;
mod top_locks;
mod top_net;
mod top_rpc;
mod trace_analysis;
mod trace_otlp;
mod trace_record;
mod trace_replay;
#[cfg(unix)]
mod trace_runtime;

/// Signed diagnostic producers available through the CLI or authenticated service jobs.
pub const CONNECT_DIAGNOSTIC_CAPABILITIES: &[&str] = &[
    perf_client::CLIENT_CAPABILITY,
    perf_drive::DRIVE_CAPABILITY,
    perf_network::NETWORK_CAPABILITY,
    perf_object::OBJECT_CAPABILITY,
    perf_site_replication::SITE_REPLICATION_CAPABILITY,
    logs::LOGS_CAPABILITY,
    profile_cpu::CPU_PROFILE_CAPABILITY,
    profile_cpu::MEMORY_PROFILE_CAPABILITY,
    profile_cpu::THREAD_PROFILE_CAPABILITY,
    trace_record::TELEMETRY_RECORD_CAPABILITY,
    trace_record::TELEMETRY_OTLP_CAPABILITY,
    trace_record::TELEMETRY_REPLAY_CAPABILITY,
    top_api::TOP_API_CAPABILITY,
    top_disk::TOP_DISK_CAPABILITY,
    top_locks::TOP_LOCKS_CAPABILITY,
    top_net::TOP_NET_CAPABILITY,
    top_rpc::TOP_RPC_CAPABILITY,
    inspect::INSPECT_CAPABILITY,
];
#[cfg(not(unix))]
#[path = "trace_runtime_unsupported.rs"]
mod trace_runtime;

pub use inspect::{
    INSPECT_CAPABILITY, INSPECT_SCHEMA_VERSION, InspectArtifactConsent, InspectDiagnosticResult, InspectError, InspectFinding,
    InspectOutcome, InspectProvenance, InspectReason, InspectReasonCode, InspectRequest, InspectRule, InspectRuleOutcome,
    InspectRun, Reconstruction, SavedInspectExport, SignedInspectExport, export_inspect_summary, save_signed_inspect_export,
};
pub use job::{
    DIAGNOSTIC_JOB_SIGNATURE_DOMAIN, DiagnosticJobEnvelope, DiagnosticJobError, DiagnosticJobExecution, DiagnosticJobLimits,
    DiagnosticJobParameters, DiagnosticJobTarget, TrustedDiagnosticJobSigner, VerifiedDiagnosticJob, execute_diagnostic_job,
};
pub use logs::{
    CaptureMode, LOGS_CAPABILITY, LOGS_SCHEMA_VERSION, LocalLogConsent, LogCaptureError, LogCaptureRequest, LogProvenance,
    SavedLogExport, SignedLogExport, export_logs, save_signed_log_export,
};
pub use perf_client::{
    CLIENT_CAPABILITY, CLIENT_SCHEMA_VERSION, ClientDiagnosticResult, ClientMeasurement, ClientOperation, ClientOutcome,
    ClientPerformanceData, ClientPerformanceError, ClientPerformanceRequest, ClientProbe, ClientProbeError, ClientProbeFuture,
    ClientProbeMeasurement, ClientProvenance, ClientReasonCode, ClientTargetParameters, ClientTargetReasonCode,
    ClientTargetResult, ClientTargetUnits, HttpClientProbe, LocalClientConsent, SavedClientExport, SignedClientExport,
    measure_client, read_protected_client_credential, save_signed_client_export, sign_client_export, validate_client_limits,
};
pub use perf_drive::{
    DRIVE_CAPABILITY, DRIVE_SCHEMA_VERSION, DriveDiagnosticResult, DriveMeasurement, DriveOutcome, DrivePerformanceData,
    DrivePerformanceError, DrivePerformanceRequest, DriveProvenance, DriveReadMode, DriveReasonCode, DriveTargetParameters,
    DriveTargetReasonCode, DriveTargetResult, DriveTargetUnits, LocalDriveConsent, SavedDriveExport, SignedDriveExport,
    measure_drive, save_signed_drive_export, sign_drive_export, validate_drive_limits,
};
pub(crate) use perf_network::runtime_network_peer_aliases;
pub use perf_network::{
    LocalNetworkConsent, MAX_ARCHIVE_BYTES as MAX_NETWORK_ARCHIVE_BYTES,
    MAX_BANDWIDTH_BYTES_PER_SECOND as MAX_NETWORK_BANDWIDTH_BYTES_PER_SECOND,
    MAX_DECOMPRESSED_BYTES as MAX_NETWORK_DECOMPRESSED_BYTES, MAX_ENVELOPE_BYTES as MAX_NETWORK_ENVELOPE_BYTES,
    MAX_NETWORK_DURATION, MAX_OPERATIONS as MAX_NETWORK_OPERATIONS, MAX_PEERS as MAX_NETWORK_PEERS,
    MAX_RESULT_BYTES as MAX_NETWORK_RESULT_BYTES, MAX_TRAFFIC_BYTES as MAX_NETWORK_TRAFFIC_BYTES, NETWORK_CAPABILITY,
    NETWORK_SCHEMA_VERSION, NETWORK_TOOL_ID, NetworkCoverage, NetworkDiagnosticResult, NetworkMeasurement, NetworkOutcome,
    NetworkPeerHarness, NetworkPeerResult, NetworkPerformanceData, NetworkPerformanceError, NetworkPerformanceRequest,
    NetworkProvenance, NetworkReasonCode, PeerProbeError, PeerProbeFuture, PeerProbeMeasurement, PeerReasonCode,
    SavedNetworkExport, SignedNetworkExport, measure_network, measure_network_with_harness, save_signed_network_export,
    sign_network_export,
};
pub use perf_object::{
    LocalObjectConsent, MAX_OBJECT_BANDWIDTH_BYTES_PER_SECOND, MAX_OBJECT_DURATION, MAX_OBJECT_RESULT_BYTES,
    MAX_OBJECT_TRAFFIC_BYTES, OBJECT_CAPABILITY, OBJECT_SCHEMA_VERSION, OBJECT_TOOL_ID, ObjectDiagnosticResult,
    ObjectMeasurement, ObjectOperation, ObjectOutcome, ObjectPerformanceData, ObjectPerformanceError, ObjectPerformanceRequest,
    ObjectProbe, ObjectProbeError, ObjectProbeFuture, ObjectProbeMeasurement, ObjectProvenance, ObjectReasonCode,
    ObjectTargetParameters, ObjectTargetReasonCode, ObjectTargetResult, ObjectTargetUnits, S3ObjectProbe, SavedObjectExport,
    SignedObjectExport, measure_object, read_protected_object_credential, save_signed_object_export, sign_object_export,
    validate_object_limits,
};
pub use perf_site_replication::{
    LocalSiteReplicationConsent, MAX_SITE_REPLICATION_DURATION, MAX_SITE_REPLICATION_TRAFFIC_BYTES, S3SiteReplicationProbe,
    SITE_REPLICATION_CAPABILITY, SITE_REPLICATION_SCHEMA_VERSION, SITE_REPLICATION_TOOL_ID, SavedSiteReplicationExport,
    SignedSiteReplicationExport, SiteReplicationCredentials, SiteReplicationDiagnosticResult, SiteReplicationEndpoint,
    SiteReplicationMeasurement, SiteReplicationOutcome, SiteReplicationPerformanceData, SiteReplicationPerformanceError,
    SiteReplicationPerformanceRequest, SiteReplicationProbe, SiteReplicationProbeError, SiteReplicationProbeFuture,
    SiteReplicationProbeMeasurement, SiteReplicationProvenance, SiteReplicationReasonCode, SiteReplicationTargetReasonCode,
    SiteReplicationTargetResult, measure_site_replication, read_protected_site_replication_credential,
    save_signed_site_replication_export, sign_site_replication_export, validate_site_replication_limits,
};
pub use profile_cpu::{
    CPU_PROFILE_CAPABILITY, LocalProfileConsent, MAX_PROFILE_DURATION, MEMORY_PROFILE_CAPABILITY, PROFILE_SCHEMA_VERSION,
    ProfileCaptureRequest, ProfileData, ProfileError, ProfileOutcome, ProfileProvenance, ProfileReasonCode, ProfileResult,
    ProfileTool, SavedProfileExport, SignedProfileExport, THREAD_PROFILE_CAPABILITY, ThreadProfileData, ThreadProfileScope,
    ThreadState, ThreadStateCount, capture_cpu_profile, encode_signed_profile_export, export_cpu_profile,
    save_signed_profile_export,
};
pub use profile_memory::export_memory_profile;
pub use profile_threads::{capture_thread_profile, export_thread_profile};
pub(crate) use receipt_delivery::{DiagnosticReceiptDelivery, DiagnosticReceiptSender};
pub use schedule::{
    DiagnosticCollectionPolicy, DiagnosticReceipt, DiagnosticScheduleError, DiagnosticScheduleRuntime, DiagnosticScheduleStatus,
    ReceiptOutcome, run_local_environment_once, spawn_environment_schedule,
};
pub(crate) use top_api::sign_top_export_with_nonce;
pub use top_api::{
    LocalTopConsent, MAX_TOP_DURATION, MAX_TOP_EXPORT_VALIDITY, SavedTopExport, SignedTopExport, TOP_API_CAPABILITY,
    TOP_CLASSIFICATION, TOP_SCHEMA_VERSION, TopApiData, TopApiOperation, TopCaptureError, TopCaptureLimits, TopCaptureRequest,
    TopCaptureScope, TopCoverage, TopOutcome, TopProvenance, TopReasonCode, TopResult, capture_top_api, save_signed_top_export,
    sign_top_export,
};
pub use top_disk::{DiskCounterSnapshot, TOP_DISK_CAPABILITY, TopDiskData, capture_top_disk, evaluate_disk_window};
pub use top_locks::{TOP_LOCKS_CAPABILITY, TopLocksData, capture_top_locks, evaluate_lock_snapshot};
pub use top_net::{NetworkCounterSnapshot, TOP_NET_CAPABILITY, TopNetData, capture_top_net, evaluate_network_window};
pub use top_rpc::{TOP_RPC_CAPABILITY, TopRpcData, capture_top_rpc};
pub use trace_analysis::{OperationSummary, TraceAnalysis, TraceAnalysisError, analyze_trace};
pub use trace_otlp::{
    LocalOtlpHeaders, MAX_OTLP_BODY_BYTES, OtlpBatch, OtlpForwardError, OtlpReceipt, export_trace_otlp, export_trace_otlp_result,
};
pub use trace_record::{
    LocalTelemetryConsent, MAX_SAFE_INTEGER, MAX_TELEMETRY_DURATION, MAX_TELEMETRY_RESULT_BYTES, MAX_TELEMETRY_SPANS,
    ObservedTelemetrySpan, RecordedTrace, SavedTelemetryExport, SignedTelemetryExport, TELEMETRY_OTLP_CAPABILITY,
    TELEMETRY_RECORD_CAPABILITY, TELEMETRY_REPLAY_CAPABILITY, TELEMETRY_SCHEMA_VERSION, TelemetryArtifactConsent,
    TelemetryArtifactError, TelemetryArtifactRequest, TelemetryCoverage, TelemetryDiagnosticResult, TelemetryOperation,
    TelemetryOutcome, TelemetryProducerError, TelemetryProvenance, TelemetryReasonCode, TelemetrySpan, TelemetrySpanStatus,
    TelemetryTool, TraceRecordCapture, TraceRecordCompletion, TraceRecordLimits, encode_signed_telemetry_export,
    record_diagnostic_result, record_trace, record_trace_bus, save_signed_telemetry_export,
};
pub use trace_replay::{LocallyReviewedTraceArtifact, ReplayedTrace, TraceReplayError, replay_trace, replay_trace_result};
pub(crate) use trace_runtime::{
    LocalTraceCaptureError, LocalTraceCaptureRuntime, request_local_trace_capture, spawn_local_trace_capture_runtime,
};
