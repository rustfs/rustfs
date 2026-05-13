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

use crate::TargetDomain;
use crate::runtime::sidecar_protocol::SidecarHandshake;
use serde::{Deserialize, Serialize};
use std::time::Duration;

const DEFAULT_FAILURE_THRESHOLD: usize = 3;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SidecarPluginRuntime {
    pub endpoint: String,
    pub handshake: SidecarHandshake,
    pub healthy: bool,
    pub failure_count: usize,
    pub degraded_to_builtin: bool,
    pub last_error: Option<String>,
}

impl SidecarPluginRuntime {
    pub fn new(endpoint: impl Into<String>, handshake: SidecarHandshake) -> Self {
        Self {
            endpoint: endpoint.into(),
            handshake,
            healthy: false,
            failure_count: 0,
            degraded_to_builtin: false,
            last_error: None,
        }
    }

    pub fn enable(&mut self, expected_plugin_id: &str, required_domain: TargetDomain) -> Result<(), String> {
        self.handshake.validate(expected_plugin_id)?;
        if !self.handshake.supported_domains.contains(&required_domain) {
            return Err(format!(
                "sidecar plugin {} does not support required domain {:?}",
                self.handshake.plugin_id, required_domain
            ));
        }

        self.healthy = true;
        self.degraded_to_builtin = false;
        self.last_error = None;
        self.failure_count = 0;
        Ok(())
    }

    pub fn mark_unhealthy(&mut self) {
        self.healthy = false;
    }

    pub fn record_failure(&mut self, error: impl Into<String>) {
        self.failure_count = self.failure_count.saturating_add(1);
        self.healthy = false;
        self.last_error = Some(error.into());
        if self.failure_count >= DEFAULT_FAILURE_THRESHOLD {
            self.degraded_to_builtin = true;
        }
    }

    pub fn send_with_timeout(&mut self, operation_timeout: Duration, simulated_latency: Duration) -> Result<(), String> {
        if simulated_latency > operation_timeout {
            self.record_failure(format!(
                "sidecar send timeout after {:?} (budget {:?})",
                simulated_latency, operation_timeout
            ));
            return Err(self
                .last_error
                .clone()
                .unwrap_or_else(|| "sidecar timeout without recorded error".to_string()));
        }
        self.healthy = true;
        self.last_error = None;
        Ok(())
    }

    pub fn shutdown(&mut self) {
        self.healthy = false;
    }
}

#[cfg(test)]
mod tests {
    use super::SidecarPluginRuntime;
    use crate::TargetDomain;
    use crate::runtime::sidecar_protocol::{SIDECAR_RUNTIME_PROTOCOL_VERSION, SidecarHandshake, SidecarPluginCapability};
    use std::time::Duration;

    fn notify_sidecar_handshake() -> SidecarHandshake {
        SidecarHandshake {
            protocol_version: SIDECAR_RUNTIME_PROTOCOL_VERSION.to_string(),
            plugin_id: "external:webhook".to_string(),
            plugin_version: "1.2.3".to_string(),
            supported_domains: vec![TargetDomain::Notify],
            capabilities: vec![
                SidecarPluginCapability::HealthCheck,
                SidecarPluginCapability::SendEvent,
                SidecarPluginCapability::Shutdown,
            ],
        }
    }

    #[test]
    fn sidecar_runtime_enable_marks_runtime_healthy() {
        let mut runtime = SidecarPluginRuntime::new("grpc://127.0.0.1:50051", notify_sidecar_handshake());

        runtime
            .enable("external:webhook", TargetDomain::Notify)
            .expect("sidecar runtime should enable");

        assert!(runtime.healthy);
    }

    #[test]
    fn sidecar_runtime_enable_rejects_domain_mismatch() {
        let mut runtime = SidecarPluginRuntime::new("grpc://127.0.0.1:50051", notify_sidecar_handshake());

        let result = runtime.enable("external:webhook", TargetDomain::Audit);

        assert!(result.is_err());
        assert!(!runtime.healthy);
    }

    #[test]
    fn sidecar_runtime_shutdown_marks_runtime_unhealthy() {
        let mut runtime = SidecarPluginRuntime::new("grpc://127.0.0.1:50051", notify_sidecar_handshake());
        runtime
            .enable("external:webhook", TargetDomain::Notify)
            .expect("sidecar runtime should enable");

        runtime.shutdown();

        assert!(!runtime.healthy);
    }

    #[test]
    fn sidecar_runtime_degrades_to_builtin_after_failure_threshold() {
        let mut runtime = SidecarPluginRuntime::new("grpc://127.0.0.1:50051", notify_sidecar_handshake());

        runtime.record_failure("send failed");
        runtime.record_failure("send failed again");
        runtime.record_failure("send failed third time");

        assert!(runtime.degraded_to_builtin);
        assert!(!runtime.healthy);
        assert_eq!(runtime.failure_count, 3);
    }

    #[test]
    fn sidecar_runtime_send_timeout_records_last_error() {
        let mut runtime = SidecarPluginRuntime::new("grpc://127.0.0.1:50051", notify_sidecar_handshake());

        let result = runtime.send_with_timeout(Duration::from_millis(50), Duration::from_millis(75));

        assert!(result.is_err());
        assert_eq!(runtime.last_error.as_deref(), Some("sidecar send timeout after 75ms (budget 50ms)"));
    }
}
