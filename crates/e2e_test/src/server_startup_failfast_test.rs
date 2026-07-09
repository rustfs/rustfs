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

//! Regression tests for e2e harness server-startup behavior.
//!
//! The harness spawns servers with the embedded console disabled so they never
//! contend for its fixed default port :9001 (often held by unrelated local
//! services such as Docker Desktop), and `wait_for_server_ready` must report a
//! server that dies during startup immediately instead of polling out the full
//! readiness timeout.

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestEnvironment, init_logging, rustfs_binary_path};
    use serial_test::serial;
    use std::net::TcpListener;
    use std::time::{Duration, Instant};

    /// Force the console back on (extra_env overrides the harness default)
    /// while :9001 is occupied: the server exits at startup, and the harness
    /// must surface that promptly rather than waiting out the 60s timeout.
    #[tokio::test]
    #[serial]
    async fn test_start_fails_fast_when_server_exits_during_startup() {
        init_logging();

        // Occupy :9001 on both stacks; a failed bind means another local
        // process already holds the port, which serves equally well.
        let _guard_v6 = TcpListener::bind(("::", 9001)).ok();
        let _guard_v4 = TcpListener::bind(("0.0.0.0", 9001)).ok();

        // Resolve (and possibly build) the binary before starting the timer.
        let _ = rustfs_binary_path();

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        let started = Instant::now();
        let result = env
            .start_rustfs_server_with_env(vec![], &[("RUSTFS_CONSOLE_ENABLE", "true")])
            .await;
        let elapsed = started.elapsed();

        let err = result.expect_err("server start should fail while :9001 is occupied");
        assert!(err.to_string().contains("exited before becoming ready"), "unexpected start error: {err}");
        assert!(
            elapsed < Duration::from_secs(30),
            "startup failure should be detected fast, took {elapsed:?}"
        );
    }
}
