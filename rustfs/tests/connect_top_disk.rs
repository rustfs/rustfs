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

use std::time::Duration;

use rustfs::connect::diagnostics::{
    DiskCounterSnapshot, LocalTopConsent, MAX_SAFE_INTEGER, TopCaptureLimits, TopCaptureRequest, TopCaptureScope, TopOutcome,
    TopReasonCode, evaluate_disk_window,
};
use time::OffsetDateTime;

fn request() -> TopCaptureRequest {
    let now = OffsetDateTime::now_utc().unix_timestamp();
    TopCaptureRequest {
        scope: TopCaptureScope {
            organization_name: "organizations/019e3ae0-0000-7000-8000-000000000010".to_owned(),
            cluster_name: "organizations/019e3ae0-0000-7000-8000-000000000010/clusters/019e3ae0-0000-7000-8000-000000000011".to_owned(),
            device_name: "organizations/019e3ae0-0000-7000-8000-000000000010/clusters/019e3ae0-0000-7000-8000-000000000011/clusterDevices/019e3ae0-0000-7000-8000-000000000012".to_owned(),
            run_uid: "019e3ae0-0000-7000-8000-000000000001".to_owned(),
            artifact_uid: "019e3ae0-0000-7000-8000-000000000013".to_owned(),
            policy_revision: 1,
            run_expires_at_unix: now + 3_600,
            executable_sha256: "b".repeat(64),
            build_features: Vec::new(),
            consent: LocalTopConsent {
                uid: "019e3ae0-0000-7000-8000-000000000014".to_owned(),
                tool_id: "top.disk".to_owned(),
                classification: "L3".to_owned(),
                active: true,
                expires_at_unix: now + 3_600,
            },
        },
        limits: TopCaptureLimits::default(),
        window: Duration::from_millis(1),
        export_validity: Duration::from_secs(300),
    }
}

#[test]
fn top_disk_uses_exact_process_counter_deltas_and_redacts_the_source() {
    let result = evaluate_disk_window(
        &request(),
        DiskCounterSnapshot {
            read_bytes: 100,
            write_bytes: 200,
            io_count: 10,
        },
        DiskCounterSnapshot {
            read_bytes: 4_196,
            write_bytes: 200,
            io_count: 11,
        },
        1_000,
    )
    .expect("disk result");

    assert_eq!(result.outcome, TopOutcome::Succeeded);
    let value = serde_json::to_value(result).expect("disk json");
    assert_eq!(value["data"]["resourceAlias"], "resource-1");
    assert_eq!(value["data"]["readBytes"], 4_096);
    assert_eq!(value["data"]["writeBytes"], 0);
    assert_eq!(value["data"]["ioCount"], 1);
    assert!(!value["data"].to_string().contains('/'));
}

#[test]
fn top_disk_counter_reset_and_safe_integer_overflow_fail_without_data() {
    for after in [
        DiskCounterSnapshot {
            read_bytes: 99,
            write_bytes: 200,
            io_count: 10,
        },
        DiskCounterSnapshot {
            read_bytes: MAX_SAFE_INTEGER + 101,
            write_bytes: 200,
            io_count: 10,
        },
    ] {
        let result = evaluate_disk_window(
            &request(),
            DiskCounterSnapshot {
                read_bytes: 100,
                write_bytes: 200,
                io_count: 10,
            },
            after,
            1_000,
        )
        .expect("structured failed result");
        assert_eq!(result.outcome, TopOutcome::Failed);
        assert_eq!(result.reason_code, TopReasonCode::CollectionFailed);
        assert!(result.data.is_none());
    }
}
