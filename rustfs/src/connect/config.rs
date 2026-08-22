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

use std::env;
use std::ffi::OsString;
use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use super::{CredentialStore, IdentityStore};

pub const ENV_CONNECT_ENDPOINT: &str = "RUSTFS_CONNECT_ENDPOINT";
pub const ENV_CONNECT_ROOT_CA_FILE: &str = "RUSTFS_CONNECT_ROOT_CA_FILE";
pub const ENV_CONNECT_STATE_DIR: &str = "RUSTFS_CONNECT_STATE_DIR";

#[derive(Clone, Copy, Debug)]
pub struct HeartbeatSchedule {
    pub cadence: Duration,
    pub jitter: Duration,
    pub timeout: Duration,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for HeartbeatSchedule {
    fn default() -> Self {
        Self {
            cadence: Duration::from_secs(30),
            jitter: Duration::from_secs(3),
            timeout: Duration::from_secs(5),
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(5 * 60),
        }
    }
}

#[derive(Clone, Debug)]
pub struct HeartbeatConfig {
    pub endpoint: String,
    pub root_ca_pem: Vec<u8>,
    pub identity_store: IdentityStore,
    pub credential_store: CredentialStore,
    pub state_path: PathBuf,
    pub schedule: HeartbeatSchedule,
}

impl HeartbeatConfig {
    pub fn new(
        endpoint: impl Into<String>,
        root_ca_pem: impl Into<Vec<u8>>,
        identity_store: IdentityStore,
        credential_store: CredentialStore,
        state_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            root_ca_pem: root_ca_pem.into(),
            identity_store,
            credential_store,
            state_path: state_path.into(),
            schedule: HeartbeatSchedule::default(),
        }
    }

    pub fn from_env() -> Result<Option<Self>, HeartbeatConfigError> {
        Self::from_env_values(
            env::var_os(ENV_CONNECT_ENDPOINT),
            env::var_os(ENV_CONNECT_ROOT_CA_FILE),
            env::var_os(ENV_CONNECT_STATE_DIR),
        )
    }

    fn from_env_values(
        endpoint: Option<OsString>,
        root_ca_file: Option<OsString>,
        state_dir: Option<OsString>,
    ) -> Result<Option<Self>, HeartbeatConfigError> {
        let configured = endpoint.is_some() || root_ca_file.is_some() || state_dir.is_some();
        if !configured {
            return Ok(None);
        }
        let (Some(endpoint), Some(root_ca_file), Some(state_dir)) = (endpoint, root_ca_file, state_dir) else {
            return Err(HeartbeatConfigError::Partial);
        };
        let endpoint = endpoint.into_string().map_err(|_| HeartbeatConfigError::EndpointEncoding)?;
        let root_ca_file = PathBuf::from(root_ca_file);
        let state_dir = PathBuf::from(state_dir);
        if endpoint.is_empty() || root_ca_file.as_os_str().is_empty() || state_dir.as_os_str().is_empty() {
            return Err(HeartbeatConfigError::Partial);
        }
        let root_ca_pem = fs::read(&root_ca_file).map_err(|source| HeartbeatConfigError::RootCertificate {
            path: root_ca_file,
            source,
        })?;
        Ok(Some(Self::new(
            endpoint,
            root_ca_pem,
            IdentityStore::new(state_dir.join("identity")),
            CredentialStore::new(state_dir.join("credential")),
            state_dir.join("heartbeat/state.json"),
        )))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HeartbeatConfigError {
    #[error(
        "Connect heartbeat configuration requires RUSTFS_CONNECT_ENDPOINT, RUSTFS_CONNECT_ROOT_CA_FILE, and RUSTFS_CONNECT_STATE_DIR"
    )]
    Partial,
    #[error("RUSTFS_CONNECT_ENDPOINT is not valid UTF-8")]
    EndpointEncoding,
    #[error("failed to read the Connect root CA at {path}: {source}")]
    RootCertificate {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

#[cfg(test)]
mod tests {
    use super::{HeartbeatConfig, HeartbeatConfigError};
    use std::ffi::OsString;

    #[test]
    fn absent_environment_is_disabled_without_side_effects() {
        assert!(
            HeartbeatConfig::from_env_values(None, None, None)
                .expect("absent config")
                .is_none()
        );
    }

    #[test]
    fn partial_environment_is_rejected() {
        assert!(matches!(
            HeartbeatConfig::from_env_values(Some(OsString::from("https://connect.example/agent/")), None, None),
            Err(HeartbeatConfigError::Partial)
        ));
    }

    #[test]
    fn complete_environment_builds_the_durable_paths() {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path().join("root.pem");
        std::fs::write(&root, b"root certificate").expect("root CA");
        let state = temp.path().join("state");
        let config = HeartbeatConfig::from_env_values(
            Some(OsString::from("https://connect.example/agent/")),
            Some(root.into_os_string()),
            Some(state.clone().into_os_string()),
        )
        .expect("complete config")
        .expect("enabled config");

        assert_eq!(config.endpoint, "https://connect.example/agent/");
        assert_eq!(config.root_ca_pem, b"root certificate");
        assert_eq!(config.state_path, state.join("heartbeat/state.json"));
        assert!(!state.exists(), "parsing configuration must not create state");
    }
}
