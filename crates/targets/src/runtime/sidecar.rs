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
use thiserror::Error;

const DEFAULT_FAILURE_THRESHOLD: usize = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SidecarRuntimePolicy {
    pub allow_external_sidecars: bool,
    pub require_sandbox: bool,
    pub require_provenance: bool,
    pub max_queue_depth: usize,
    pub operation_timeout: Duration,
    pub failure_threshold: usize,
    pub redact_error_details: bool,
}

impl Default for SidecarRuntimePolicy {
    fn default() -> Self {
        Self {
            allow_external_sidecars: false,
            require_sandbox: true,
            require_provenance: true,
            max_queue_depth: 0,
            operation_timeout: Duration::from_secs(5),
            failure_threshold: DEFAULT_FAILURE_THRESHOLD,
            redact_error_details: true,
        }
    }
}

impl SidecarRuntimePolicy {
    pub fn verified_external(max_queue_depth: usize, operation_timeout: Duration, failure_threshold: usize) -> Self {
        Self {
            allow_external_sidecars: true,
            require_sandbox: true,
            require_provenance: true,
            max_queue_depth,
            operation_timeout,
            failure_threshold: failure_threshold.max(1),
            redact_error_details: true,
        }
    }

    pub fn failure_threshold(&self) -> usize {
        self.failure_threshold.max(1)
    }

    pub fn validate_activation(&self, safety_checks: &SidecarRuntimeSafetyChecks) -> Result<(), SidecarRuntimePolicyError> {
        validate_runtime_policy(self, safety_checks)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SidecarRuntimeSafetyChecks {
    pub sandboxed: bool,
    pub provenance_verified: bool,
    pub queue_depth: usize,
}

impl SidecarRuntimeSafetyChecks {
    pub fn verified(queue_depth: usize) -> Self {
        Self {
            sandboxed: true,
            provenance_verified: true,
            queue_depth,
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SidecarRuntimePolicyError {
    #[error("external sidecar runtime is disabled by policy")]
    ExternalSidecarDisabled,

    #[error("sidecar runtime requires sandbox isolation")]
    SandboxRequired,

    #[error("sidecar runtime requires verified provenance")]
    ProvenanceRequired,

    #[error("sidecar runtime queue depth {queue_depth} exceeds policy bound {max_queue_depth}")]
    QueueDepthExceeded { queue_depth: usize, max_queue_depth: usize },
}

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

    pub fn enable_with_policy(
        &mut self,
        expected_plugin_id: &str,
        required_domain: TargetDomain,
        policy: &SidecarRuntimePolicy,
        safety_checks: &SidecarRuntimeSafetyChecks,
    ) -> Result<(), String> {
        self.handshake.validate(expected_plugin_id)?;
        if !self.handshake.supported_domains.contains(&required_domain) {
            return Err(format!(
                "sidecar plugin {} does not support required domain {:?}",
                self.handshake.plugin_id, required_domain
            ));
        }
        policy.validate_activation(safety_checks).map_err(|err| err.to_string())?;

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

    pub fn record_failure_with_policy(&mut self, policy: &SidecarRuntimePolicy, error: impl Into<String>) {
        self.failure_count = self.failure_count.saturating_add(1);
        self.healthy = false;
        self.last_error = Some(if policy.redact_error_details {
            "sidecar operation failed".to_string()
        } else {
            error.into()
        });
        if self.failure_count >= policy.failure_threshold() {
            self.degraded_to_builtin = true;
        }
    }

    /// Simulates a send bounded by the policy's operation timeout. On timeout the
    /// failure is recorded through the policy (so error details are redacted when
    /// `redact_error_details` is set and the configurable failure threshold —
    /// not a hardcoded constant — drives circuit breaking). A successful send
    /// clears the failure count so transient blips never accumulate toward the
    /// breaker.
    pub fn send_with_timeout(&mut self, policy: &SidecarRuntimePolicy, simulated_latency: Duration) -> Result<(), String> {
        if simulated_latency > policy.operation_timeout {
            self.record_failure_with_policy(
                policy,
                format!(
                    "sidecar send timeout after {:?} (budget {:?})",
                    simulated_latency, policy.operation_timeout
                ),
            );
            return Err(self
                .last_error
                .clone()
                .unwrap_or_else(|| "sidecar operation failed".to_string()));
        }
        self.healthy = true;
        self.last_error = None;
        // Success resets the circuit-breaker accounting: a healthy send must not
        // leave stale failures that could trip the breaker on the next blip.
        self.failure_count = 0;
        self.degraded_to_builtin = false;
        Ok(())
    }

    pub fn shutdown(&mut self) {
        self.healthy = false;
    }
}

fn validate_runtime_policy(
    policy: &SidecarRuntimePolicy,
    safety_checks: &SidecarRuntimeSafetyChecks,
) -> Result<(), SidecarRuntimePolicyError> {
    if !policy.allow_external_sidecars {
        return Err(SidecarRuntimePolicyError::ExternalSidecarDisabled);
    }
    if policy.require_sandbox && !safety_checks.sandboxed {
        return Err(SidecarRuntimePolicyError::SandboxRequired);
    }
    if policy.require_provenance && !safety_checks.provenance_verified {
        return Err(SidecarRuntimePolicyError::ProvenanceRequired);
    }
    if safety_checks.queue_depth > policy.max_queue_depth {
        return Err(SidecarRuntimePolicyError::QueueDepthExceeded {
            queue_depth: safety_checks.queue_depth,
            max_queue_depth: policy.max_queue_depth,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{SidecarPluginRuntime, SidecarRuntimePolicy, SidecarRuntimeSafetyChecks};
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
    fn sidecar_runtime_policy_rejects_external_activation_by_default() {
        let mut runtime = SidecarPluginRuntime::new("grpc://127.0.0.1:50051", notify_sidecar_handshake());

        let result = runtime.enable_with_policy(
            "external:webhook",
            TargetDomain::Notify,
            &SidecarRuntimePolicy::default(),
            &SidecarRuntimeSafetyChecks::verified(0),
        );

        assert_eq!(
            result.as_ref().map_err(String::as_str),
            Err("external sidecar runtime is disabled by policy")
        );
        assert!(!runtime.healthy);
    }

    #[test]
    fn sidecar_runtime_policy_requires_sandbox_and_provenance() {
        let mut runtime = SidecarPluginRuntime::new("grpc://127.0.0.1:50051", notify_sidecar_handshake());
        let policy = SidecarRuntimePolicy::verified_external(16, Duration::from_secs(5), 3);

        let missing_sandbox = runtime.enable_with_policy(
            "external:webhook",
            TargetDomain::Notify,
            &policy,
            &SidecarRuntimeSafetyChecks {
                sandboxed: false,
                provenance_verified: true,
                queue_depth: 0,
            },
        );

        assert_eq!(
            missing_sandbox.as_ref().map_err(String::as_str),
            Err("sidecar runtime requires sandbox isolation")
        );

        let missing_provenance = runtime.enable_with_policy(
            "external:webhook",
            TargetDomain::Notify,
            &policy,
            &SidecarRuntimeSafetyChecks {
                sandboxed: true,
                provenance_verified: false,
                queue_depth: 0,
            },
        );

        assert_eq!(
            missing_provenance.as_ref().map_err(String::as_str),
            Err("sidecar runtime requires verified provenance")
        );
        assert!(!runtime.healthy);
    }

    #[test]
    fn sidecar_runtime_policy_enforces_queue_bound() {
        let mut runtime = SidecarPluginRuntime::new("grpc://127.0.0.1:50051", notify_sidecar_handshake());
        let policy = SidecarRuntimePolicy::verified_external(2, Duration::from_secs(5), 3);

        let result = runtime.enable_with_policy(
            "external:webhook",
            TargetDomain::Notify,
            &policy,
            &SidecarRuntimeSafetyChecks::verified(3),
        );

        assert_eq!(
            result.as_ref().map_err(String::as_str),
            Err("sidecar runtime queue depth 3 exceeds policy bound 2")
        );
        assert!(!runtime.healthy);
    }

    #[test]
    fn sidecar_runtime_policy_allows_verified_external_activation() {
        let mut runtime = SidecarPluginRuntime::new("grpc://127.0.0.1:50051", notify_sidecar_handshake());
        let policy = SidecarRuntimePolicy::verified_external(16, Duration::from_secs(5), 3);

        runtime
            .enable_with_policy(
                "external:webhook",
                TargetDomain::Notify,
                &policy,
                &SidecarRuntimeSafetyChecks::verified(1),
            )
            .expect("verified sidecar runtime should enable");

        assert!(runtime.healthy);
        assert_eq!(policy.failure_threshold(), 3);
    }

    #[test]
    fn sidecar_runtime_policy_redacts_failure_details() {
        let mut runtime = SidecarPluginRuntime::new("grpc://127.0.0.1:50051", notify_sidecar_handshake());
        let policy = SidecarRuntimePolicy::verified_external(16, Duration::from_secs(5), 2);

        runtime.record_failure_with_policy(&policy, "secret token leaked in transport error");
        runtime.record_failure_with_policy(&policy, "another secret error");

        assert_eq!(runtime.last_error.as_deref(), Some("sidecar operation failed"));
        assert!(runtime.degraded_to_builtin);
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
    fn sidecar_runtime_send_timeout_redacts_error_and_uses_policy_threshold() {
        let mut runtime = SidecarPluginRuntime::new("grpc://127.0.0.1:50051", notify_sidecar_handshake());
        // redact_error_details defaults to true; threshold 2 is honored, not the
        // hardcoded DEFAULT_FAILURE_THRESHOLD.
        let policy = SidecarRuntimePolicy::verified_external(16, Duration::from_millis(50), 2);

        let result = runtime.send_with_timeout(&policy, Duration::from_millis(75));

        assert!(result.is_err());
        // The raw budget/latency detail must not leak; it is redacted.
        assert_eq!(runtime.last_error.as_deref(), Some("sidecar operation failed"));
        assert_eq!(runtime.failure_count, 1);
        assert!(!runtime.degraded_to_builtin);

        // Second timeout hits the configured threshold and degrades.
        let _ = runtime.send_with_timeout(&policy, Duration::from_millis(75));
        assert_eq!(runtime.failure_count, 2);
        assert!(runtime.degraded_to_builtin);
    }

    #[test]
    fn sidecar_runtime_successful_send_clears_failure_count() {
        let mut runtime = SidecarPluginRuntime::new("grpc://127.0.0.1:50051", notify_sidecar_handshake());
        let policy = SidecarRuntimePolicy::verified_external(16, Duration::from_millis(50), 3);

        // Accumulate a failure, then a successful send must reset the breaker
        // accounting so a later single failure does not immediately degrade.
        let _ = runtime.send_with_timeout(&policy, Duration::from_millis(75));
        assert_eq!(runtime.failure_count, 1);

        runtime
            .send_with_timeout(&policy, Duration::from_millis(10))
            .expect("a within-budget send should succeed");
        assert_eq!(runtime.failure_count, 0);
        assert!(!runtime.degraded_to_builtin);
        assert!(runtime.healthy);
        assert!(runtime.last_error.is_none());
    }
}
