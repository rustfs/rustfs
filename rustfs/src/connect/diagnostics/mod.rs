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

mod logs;
mod profile_cpu;
mod profile_memory;
mod profile_threads;
mod schedule;
mod trace_analysis;
mod trace_otlp;
mod trace_record;
mod trace_replay;

pub use logs::{
    CaptureMode, LOGS_CAPABILITY, LOGS_SCHEMA_VERSION, LocalLogConsent, LogCaptureError, LogCaptureRequest, LogProvenance,
    SavedLogExport, SignedLogExport, export_logs, save_signed_log_export,
};
pub use profile_cpu::{
    CPU_PROFILE_CAPABILITY, LocalProfileConsent, MAX_PROFILE_DURATION, MEMORY_PROFILE_CAPABILITY, PROFILE_SCHEMA_VERSION,
    ProfileCaptureRequest, ProfileData, ProfileError, ProfileOutcome, ProfileProvenance, ProfileReasonCode, ProfileResult,
    ProfileTool, SavedProfileExport, SignedProfileExport, THREAD_PROFILE_CAPABILITY, ThreadProfileScope, capture_cpu_profile,
    encode_signed_profile_export, export_cpu_profile, save_signed_profile_export,
};
pub use profile_memory::export_memory_profile;
pub use profile_threads::{capture_thread_profile, export_thread_profile};
pub use schedule::{
    DiagnosticCollectionPolicy, DiagnosticReceipt, DiagnosticScheduleError, DiagnosticScheduleRuntime, DiagnosticScheduleStatus,
    ReceiptOutcome, run_local_environment_once, spawn_environment_schedule,
};
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
