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

use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityState {
    Supported,
    Unsupported,
    Disabled,
    #[default]
    Unknown,
}

impl CapabilityState {
    pub const fn is_supported(self) -> bool {
        matches!(self, Self::Supported)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityStatus {
    pub state: CapabilityState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl CapabilityStatus {
    pub const fn new(state: CapabilityState) -> Self {
        Self { state, reason: None }
    }

    pub const fn supported() -> Self {
        Self::new(CapabilityState::Supported)
    }

    pub const fn unsupported() -> Self {
        Self::new(CapabilityState::Unsupported)
    }

    pub const fn disabled() -> Self {
        Self::new(CapabilityState::Disabled)
    }

    pub const fn unknown() -> Self {
        Self::new(CapabilityState::Unknown)
    }

    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }
}

impl Default for CapabilityStatus {
    fn default() -> Self {
        Self::unknown()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapabilitySnapshotError {
    Unavailable,
    Unsupported,
    InvalidSnapshot(String),
}

impl fmt::Display for CapabilitySnapshotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable => f.write_str("capability snapshot unavailable"),
            Self::Unsupported => f.write_str("capability snapshot unsupported"),
            Self::InvalidSnapshot(reason) => write!(f, "invalid capability snapshot: {reason}"),
        }
    }
}

impl std::error::Error for CapabilitySnapshotError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capability_status_serializes_unknown_and_unsupported_states() {
        let unknown = CapabilityStatus::unknown();
        let unsupported = CapabilityStatus::unsupported().with_reason("target does not expose profiler");

        let encoded = serde_json::to_string(&(unknown, unsupported)).expect("serialize capability statuses");
        let decoded: (CapabilityStatus, CapabilityStatus) =
            serde_json::from_str(&encoded).expect("deserialize capability statuses");

        assert_eq!(decoded.0.state, CapabilityState::Unknown);
        assert_eq!(decoded.1.state, CapabilityState::Unsupported);
        assert_eq!(decoded.1.reason.as_deref(), Some("target does not expose profiler"));
        assert!(!decoded.1.state.is_supported());
    }
}
