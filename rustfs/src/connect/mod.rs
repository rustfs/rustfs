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

//! RustFS Connect device identity.
//!
//! A cluster device proves possession of its own key when it exchanges a
//! one-time registration token for a durable certificate. This module owns the
//! device-side half of that exchange: the P-256 key, the PKCS#10 certificate
//! request built from it, and the proof-of-possession signature over the
//! canonical transcript frozen by
//! `protocol/agent/v1/registration-proof.md`.
//!
//! Enrolled deployments may start the optional outbound heartbeat runtime.
//! An unconfigured server starts no Connect task, generates no key, and holds
//! no Connect identity.

pub mod client;
pub mod config;
pub mod credential_store;
pub mod diagnostics;
pub mod environment;
pub mod heartbeat;
pub mod identity;
pub mod identity_store;
pub mod inventory;
pub mod license;
pub mod offline;
pub mod registration;
pub mod registration_bootstrap;
pub mod runtime;
mod telemetry;

pub use client::{ClientError, ConnectClient, ConnectConfig};
pub use config::{
    ENV_CONNECT_PROXY_BYPASS, ENV_CONNECT_PROXY_PASSWORD_FILE, ENV_CONNECT_PROXY_URL, ENV_CONNECT_PROXY_USERNAME_FILE,
    HeartbeatConfig, HeartbeatConfigError, HeartbeatSchedule, ProxyConfig, ProxyConfigError,
};
pub use credential_store::{CredentialStore, DeviceCredential};
pub use diagnostics::{
    CLIENT_CAPABILITY, CLIENT_SCHEMA_VERSION, CPU_PROFILE_CAPABILITY, CaptureMode, ClientDiagnosticResult, ClientMeasurement,
    ClientOperation, ClientOutcome, ClientPerformanceData, ClientPerformanceError, ClientPerformanceRequest, ClientProbe,
    ClientProbeError, ClientProbeFuture, ClientProbeMeasurement, ClientProvenance, ClientReasonCode, ClientTargetParameters,
    ClientTargetReasonCode, ClientTargetResult, ClientTargetUnits, DRIVE_CAPABILITY, DRIVE_SCHEMA_VERSION,
    DiagnosticCollectionPolicy, DiagnosticReceipt, DiagnosticScheduleError, DiagnosticScheduleRuntime, DiagnosticScheduleStatus,
    DriveDiagnosticResult, DriveMeasurement, DriveOutcome, DrivePerformanceData, DrivePerformanceError, DrivePerformanceRequest,
    DriveProvenance, DriveReadMode, DriveReasonCode, DriveTargetParameters, DriveTargetReasonCode, DriveTargetResult,
    DriveTargetUnits, HttpClientProbe, LOGS_CAPABILITY, LOGS_SCHEMA_VERSION, LocalClientConsent, LocalDriveConsent,
    LocalLogConsent, LocalOtlpHeaders, LocalProfileConsent, LocalTelemetryConsent, LocallyReviewedTraceArtifact, LogCaptureError,
    LogCaptureRequest, LogProvenance, MAX_OTLP_BODY_BYTES, MAX_PROFILE_DURATION, MAX_SAFE_INTEGER, MAX_TELEMETRY_DURATION,
    MAX_TELEMETRY_RESULT_BYTES, MAX_TELEMETRY_SPANS, MEMORY_PROFILE_CAPABILITY, ObservedTelemetrySpan, OperationSummary,
    OtlpBatch, OtlpForwardError, OtlpReceipt, PROFILE_SCHEMA_VERSION, ProfileCaptureRequest, ProfileData, ProfileError,
    ProfileOutcome, ProfileProvenance, ProfileReasonCode, ProfileResult, ProfileTool, ReceiptOutcome, RecordedTrace,
    ReplayedTrace, SavedClientExport, SavedDriveExport, SavedLogExport, SavedProfileExport, SavedTelemetryExport,
    SignedClientExport, SignedDriveExport, SignedLogExport, SignedProfileExport, SignedTelemetryExport,
    TELEMETRY_OTLP_CAPABILITY, TELEMETRY_RECORD_CAPABILITY, TELEMETRY_REPLAY_CAPABILITY, TELEMETRY_SCHEMA_VERSION,
    THREAD_PROFILE_CAPABILITY, TelemetryArtifactConsent, TelemetryArtifactError, TelemetryArtifactRequest, TelemetryCoverage,
    TelemetryDiagnosticResult, TelemetryOperation, TelemetryOutcome, TelemetryProducerError, TelemetryProvenance,
    TelemetryReasonCode, TelemetrySpan, TelemetrySpanStatus, TelemetryTool, ThreadProfileScope, TraceAnalysis,
    TraceAnalysisError, TraceRecordCapture, TraceRecordCompletion, TraceRecordLimits, TraceReplayError, analyze_trace,
    capture_cpu_profile, capture_thread_profile, encode_signed_profile_export, encode_signed_telemetry_export,
    export_cpu_profile, export_logs, export_memory_profile, export_thread_profile, export_trace_otlp, export_trace_otlp_result,
    measure_client, measure_drive, read_protected_client_credential, record_diagnostic_result, record_trace, record_trace_bus,
    replay_trace, replay_trace_result, run_local_environment_once, save_signed_client_export, save_signed_drive_export,
    save_signed_log_export, save_signed_profile_export, save_signed_telemetry_export, sign_client_export, sign_drive_export,
    spawn_environment_schedule, validate_client_limits, validate_drive_limits,
};
pub use diagnostics::{
    LocalNetworkConsent, MAX_NETWORK_ARCHIVE_BYTES, MAX_NETWORK_BANDWIDTH_BYTES_PER_SECOND, MAX_NETWORK_DECOMPRESSED_BYTES,
    MAX_NETWORK_DURATION, MAX_NETWORK_ENVELOPE_BYTES, MAX_NETWORK_OPERATIONS, MAX_NETWORK_PEERS, MAX_NETWORK_RESULT_BYTES,
    MAX_NETWORK_TRAFFIC_BYTES, NETWORK_CAPABILITY, NETWORK_SCHEMA_VERSION, NETWORK_TOOL_ID, NetworkCoverage,
    NetworkDiagnosticResult, NetworkMeasurement, NetworkOutcome, NetworkPeerHarness, NetworkPeerResult, NetworkPerformanceData,
    NetworkPerformanceError, NetworkPerformanceRequest, NetworkProvenance, NetworkReasonCode, PeerProbeError, PeerProbeFuture,
    PeerProbeMeasurement, PeerReasonCode, SavedNetworkExport, SignedNetworkExport, measure_network, measure_network_with_harness,
    save_signed_network_export, sign_network_export,
};
pub use diagnostics::{
    LocalObjectConsent, MAX_OBJECT_BANDWIDTH_BYTES_PER_SECOND, MAX_OBJECT_DURATION, MAX_OBJECT_RESULT_BYTES,
    MAX_OBJECT_TRAFFIC_BYTES, OBJECT_CAPABILITY, OBJECT_SCHEMA_VERSION, OBJECT_TOOL_ID, ObjectDiagnosticResult,
    ObjectMeasurement, ObjectOperation, ObjectOutcome, ObjectPerformanceData, ObjectPerformanceError, ObjectPerformanceRequest,
    ObjectProbe, ObjectProbeError, ObjectProbeFuture, ObjectProbeMeasurement, ObjectProvenance, ObjectReasonCode,
    ObjectTargetParameters, ObjectTargetReasonCode, ObjectTargetResult, ObjectTargetUnits, S3ObjectProbe, SavedObjectExport,
    SignedObjectExport, measure_object, read_protected_object_credential, save_signed_object_export, sign_object_export,
    validate_object_limits,
};
pub use diagnostics::{
    LocalTopConsent, MAX_TOP_DURATION, MAX_TOP_EXPORT_VALIDITY, NetworkCounterSnapshot, SavedTopExport, SignedTopExport,
    TOP_CLASSIFICATION, TOP_SCHEMA_VERSION, TopApiData, TopApiOperation, TopCaptureError, TopCaptureLimits, TopCaptureRequest,
    TopCaptureScope, TopCoverage, TopDiskData, TopLocksData, TopNetData, TopOutcome, TopProvenance, TopReasonCode, TopResult,
    TopRpcData, capture_top_api, capture_top_disk, capture_top_locks, capture_top_net, capture_top_rpc, evaluate_disk_window,
    evaluate_network_window, save_signed_top_export, sign_top_export,
};
pub use environment::{
    ENVIRONMENT_CAPABILITY, ENVIRONMENT_SCHEMA_VERSION, EnvironmentCollectionRequest, EnvironmentError,
    EnvironmentFilesystemType, EnvironmentInventory, EnvironmentOsFamily, MAX_ENVIRONMENT_DURATION, collect_environment,
};
pub use heartbeat::{CoarseNodeSummary, HeartbeatError, HeartbeatStatus};
pub use identity::{DeviceIdentity, IdentityError, RegistrationProof, RegistrationTranscript};
pub use identity_store::{IdentityStore, StoreError};
pub use inventory::{
    InventoryError, InventoryFlag, InventoryOsVersion, InventorySchedule, InventorySnapshot, InventoryStatus,
    OperatingSystemFamily,
};
pub use license::{
    LICENSE_DOMAIN_SEPARATION_TAG, LicenseArtifactError, LicenseArtifactStatus, LicenseClaims, LicenseReport,
    LicenseVerificationContext, apply_license_artifact, inspect_installed_license, verify_license_artifact,
};
pub use offline::{EnrollmentError, OfflineEnrollment, OfflineKeyStore, VerifiedChallenge};
pub use registration::{RegistrationToken, TokenError};
pub use registration_bootstrap::{RegistrationBootstrapError, RegistrationBootstrapResult, register_from_protected_input};
pub use runtime::{HeartbeatRuntime, InventoryRuntime, spawn_heartbeat_runtime, spawn_inventory_runtime};
