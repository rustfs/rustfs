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

mod connect {
    pub use rustfs::connect::DeviceIdentity;
}

#[allow(dead_code)]
#[path = "../src/connect/diagnostics/profile_cpu.rs"]
mod profile_cpu;
#[path = "../src/connect/diagnostics/profile_threads.rs"]
mod profile_threads;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use profile_cpu::{
    LocalProfileConsent, ProfileCaptureRequest, ProfileError, ProfileOutcome, ProfileProvenance, ProfileReasonCode,
    THREAD_PROFILE_CAPABILITY, ThreadProfileScope,
};
use profile_threads::{capture_thread_profile, export_thread_profile};
#[cfg(target_os = "linux")]
use std::io::{Cursor, Read as _};
use tokio_util::sync::CancellationToken;
#[cfg(target_os = "linux")]
use zip::ZipArchive;

static CAPTURE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn request() -> ProfileCaptureRequest {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).expect("current time").as_secs() as i64;
    let organization = "organizations/019e3ae0-0000-7000-8000-000000000021";
    let cluster = format!("{organization}/clusters/019e3ae0-0000-7000-8000-000000000022");
    ProfileCaptureRequest {
        organization_name: organization.to_string(),
        cluster_name: cluster.clone(),
        device_name: format!("{cluster}/clusterDevices/019e3ae0-0000-7000-8000-000000000023"),
        run_uid: "019e3ae0-0000-7000-8000-000000000024".to_string(),
        artifact_uid: "019e3ae0-0000-7000-8000-000000000025".to_string(),
        schema_version: 1,
        capability: THREAD_PROFILE_CAPABILITY.to_string(),
        consent: LocalProfileConsent {
            consent_uid: "019e3ae0-0000-7000-8000-000000000026".to_string(),
            policy_revision: 1,
            expires_at_unix: now + 120,
            confirmed: true,
        },
        produced_at_unix: now,
        expires_at_unix: now + 60,
        nonce: [0x7c; 32],
        duration: Duration::from_secs(1),
        sample_period: Duration::from_millis(10),
        provenance: ProfileProvenance::new("e".repeat(40), "f".repeat(64), "1.0.0-rc.6", vec!["default".to_string()]),
    }
}

#[tokio::test]
async fn tokio_thread_scope_remains_explicitly_unsupported() {
    let key = connect::DeviceIdentity::generate();
    let result = capture_thread_profile(&request(), ThreadProfileScope::TokioRuntime, &CancellationToken::new())
        .expect("unsupported result");
    assert_eq!(result.outcome(), ProfileOutcome::Unsupported);
    assert_eq!(result.reason_code(), ProfileReasonCode::UnsupportedTool);
    assert!(result.data().is_none(), "unsupported scope must not publish zero state counts");
    let json = serde_json::to_value(result).expect("result JSON");
    assert_eq!(json["toolId"], "profile.threads");
    assert_eq!(json["capability"], "profile.threads@1");
    assert!(json["data"].is_null());

    let export = export_thread_profile(&request(), ThreadProfileScope::TokioRuntime, &key, &CancellationToken::new())
        .await
        .expect("unsupported export");
    assert_eq!(export.tool.id(), "profile.threads");
    assert_eq!(export.outcome, ProfileOutcome::Unsupported);
    assert_eq!(export.reason_code, ProfileReasonCode::UnsupportedTool);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn native_thread_scope_exports_bounded_redacted_state_counts() {
    let _capture = CAPTURE_LOCK.lock().await;
    let key = connect::DeviceIdentity::generate();
    let result = capture_thread_profile(&request(), ThreadProfileScope::NativeThreads, &CancellationToken::new())
        .expect("native thread result");
    assert_eq!(result.outcome(), ProfileOutcome::Succeeded);
    assert_eq!(result.reason_code(), ProfileReasonCode::Complete);
    let json = serde_json::to_value(result).expect("result JSON");
    assert_eq!(json["data"]["scope"], "NATIVE_THREADS");
    let states = json["data"]["states"].as_array().expect("thread states");
    assert_eq!(states.len(), 4);
    assert!(states.iter().all(|state| state["threadCount"].as_u64().is_some()));
    assert!(states.iter().map(|state| state["threadCount"].as_u64().unwrap()).sum::<u64>() > 0);
    let encoded = serde_json::to_string(&json).expect("encoded result");
    for forbidden in ["/proc/", "task/", "worker", "secret", "stack", "address", "threadId"] {
        assert!(!encoded.contains(forbidden), "result leaked forbidden material: {forbidden}");
    }

    let export = export_thread_profile(&request(), ThreadProfileScope::NativeThreads, &key, &CancellationToken::new())
        .await
        .expect("native thread export");
    assert_eq!(export.outcome, ProfileOutcome::Succeeded);
    assert!(export.archive_bytes.len() <= profile_cpu::MAX_ARCHIVE_BYTES);
    let mut archive = ZipArchive::new(Cursor::new(export.archive_bytes)).expect("profile archive");
    let mut result = String::new();
    archive
        .by_name("result.json")
        .expect("profile result")
        .read_to_string(&mut result)
        .expect("read profile result");
    assert!(result.contains("\"scope\":\"NATIVE_THREADS\""));
    for forbidden in ["/proc/", "task/", "worker", "secret", "stack", "address", "threadId"] {
        assert!(!result.contains(forbidden), "archive leaked forbidden material: {forbidden}");
    }
}

#[cfg(target_os = "linux")]
#[test]
fn native_thread_scope_honors_the_monotonic_deadline() {
    let _capture = CAPTURE_LOCK.blocking_lock();
    let mut expired = request();
    expired.duration = Duration::from_nanos(1);
    expired.sample_period = Duration::from_nanos(1);
    assert!(matches!(
        capture_thread_profile(&expired, ThreadProfileScope::NativeThreads, &CancellationToken::new()),
        Err(ProfileError::TimedOut)
    ));
}

#[cfg(not(target_os = "linux"))]
#[test]
fn native_thread_scope_is_explicitly_unsupported_off_linux() {
    let result = capture_thread_profile(&request(), ThreadProfileScope::NativeThreads, &CancellationToken::new())
        .expect("unsupported result");
    assert_eq!(result.outcome(), ProfileOutcome::Unsupported);
    assert_eq!(result.reason_code(), ProfileReasonCode::UnsupportedPlatform);
    assert!(result.data().is_none());
}

#[test]
fn thread_profile_rejects_wrong_negotiation_and_cancellation() {
    let mut invalid = request();
    invalid.capability = "profile.threads@2".to_string();
    assert!(matches!(
        capture_thread_profile(&invalid, ThreadProfileScope::TokioRuntime, &CancellationToken::new()),
        Err(ProfileError::UnsupportedCapability)
    ));

    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(matches!(
        capture_thread_profile(&request(), ThreadProfileScope::NativeThreads, &cancel),
        Err(ProfileError::Cancelled)
    ));
}

#[cfg(target_has_atomic = "64")]
#[test]
fn service_runtime_profile_observes_the_supplied_two_worker_runtime() {
    let _capture = CAPTURE_LOCK.blocking_lock();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let metrics = runtime.metrics();
    let task = runtime.spawn(std::future::pending::<()>());
    let mut request = request();
    request.duration = Duration::from_millis(30);
    assert!(matches!(
        profile_threads::capture_runtime_profile(&request, &metrics, Duration::from_millis(1), &CancellationToken::new()),
        Err(ProfileError::LimitExceeded)
    ));
    let result =
        profile_threads::capture_runtime_profile(&request, &metrics, Duration::from_secs(30), &CancellationToken::new()).unwrap();
    let json = serde_json::to_value(result).unwrap();
    assert_eq!(json["data"]["workerCount"], 2);
    assert_eq!(json["data"]["scope"], "TOKIO_RUNTIME");
    let samples = json["data"]["samples"].as_array().unwrap();
    assert_eq!(samples.len(), 2);
    assert_eq!(samples[0]["elapsedMicros"], 0);
    assert!(samples[1]["elapsedMicros"].as_u64().unwrap() >= 30_000);
    assert!(samples[1]["elapsedMicros"].as_u64().unwrap() <= json["durationMillis"].as_u64().unwrap() * 1000 + 999);
    for sample in samples {
        assert!(sample["aliveTaskCount"].as_u64().unwrap() >= 1);
        let workers = sample["workers"].as_array().unwrap();
        assert_eq!(workers.len(), 2);
        for (index, worker) in workers.iter().enumerate() {
            assert_eq!(worker["workerIndex"], index);
        }
        assert!(sample.get("states").is_none());
    }
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(matches!(
        profile_threads::capture_runtime_profile(&request, &metrics, Duration::from_secs(30), &cancel),
        Err(ProfileError::Cancelled)
    ));
    request.consent.confirmed = false;
    assert!(matches!(
        profile_threads::capture_runtime_profile(&request, &metrics, Duration::from_secs(30), &CancellationToken::new()),
        Err(ProfileError::ConsentRequired)
    ));
    request.consent.confirmed = true;
    request.duration = Duration::from_secs(1);
    let cancel = CancellationToken::new();
    let canceller = cancel.clone();
    let thread = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(25));
        canceller.cancel();
    });
    assert!(matches!(
        profile_threads::capture_runtime_profile(&request, &metrics, Duration::from_secs(30), &cancel),
        Err(ProfileError::Cancelled)
    ));
    thread.join().unwrap();
    task.abort();
}

#[test]
fn runtime_result_rejects_submillisecond_deadline_overrun() {
    use profile_cpu::{RuntimeProfileData, RuntimeProfileSample, RuntimeWorkerSample};
    let sample = || RuntimeProfileSample {
        elapsed_micros: 0,
        alive_task_count: 0,
        global_queue_depth: 0,
        workers: vec![RuntimeWorkerSample {
            worker_index: 0,
            busy_duration_micros: 0,
            park_count: 0,
        }],
    };
    let data = RuntimeProfileData {
        scope: ThreadProfileScope::TokioRuntime,
        worker_count: 1,
        samples: [sample(), sample()],
    };
    assert!(matches!(
        profile_cpu::ProfileResult::runtime_succeeded(&request(), Duration::from_secs(30) + Duration::from_nanos(1), data),
        Err(ProfileError::TimedOut)
    ));
}
