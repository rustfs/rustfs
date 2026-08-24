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
#[cfg(target_os = "linux")]
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
        let state_path = state_path.into();
        Self {
            endpoint: endpoint.into(),
            root_ca_pem: root_ca_pem.into(),
            identity_store,
            credential_store,
            state_path,
            schedule: HeartbeatSchedule::default(),
        }
    }

    #[cfg(any(target_os = "linux", test))]
    pub(crate) fn state_only(state_root: PathBuf) -> Self {
        Self {
            endpoint: String::new(),
            root_ca_pem: Vec::new(),
            identity_store: IdentityStore::new(state_root.join("identity")),
            credential_store: CredentialStore::new(state_root.join("credential")),
            state_path: state_root.join("heartbeat/state.json"),
            schedule: HeartbeatSchedule::default(),
        }
    }

    pub(crate) fn transport_enabled(&self) -> bool {
        !self.endpoint.is_empty()
    }

    pub(crate) fn state_root(&self) -> Option<&std::path::Path> {
        self.state_path.parent().and_then(std::path::Path::parent)
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
        let Some(state_dir) = state_dir else {
            return Err(HeartbeatConfigError::Partial);
        };
        let state_dir = PathBuf::from(state_dir);
        if state_dir.as_os_str().is_empty() || endpoint.is_some() != root_ca_file.is_some() {
            return Err(HeartbeatConfigError::Partial);
        }
        #[cfg(not(target_os = "linux"))]
        return Err(HeartbeatConfigError::PlatformSecurity);
        #[cfg(target_os = "linux")]
        let (Some(endpoint), Some(root_ca_file)) = (endpoint, root_ca_file) else {
            return Ok(Some(Self::state_only(state_dir)));
        };
        #[cfg(target_os = "linux")]
        let endpoint = endpoint.into_string().map_err(|_| HeartbeatConfigError::EndpointEncoding)?;
        #[cfg(target_os = "linux")]
        let root_ca_file = PathBuf::from(root_ca_file);
        #[cfg(target_os = "linux")]
        if endpoint.is_empty() || root_ca_file.as_os_str().is_empty() {
            return Err(HeartbeatConfigError::Partial);
        }
        #[cfg(target_os = "linux")]
        let root_ca_pem = fs::read(&root_ca_file).map_err(|source| HeartbeatConfigError::RootCertificate {
            path: root_ca_file,
            source,
        })?;
        #[cfg(target_os = "linux")]
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
        "Connect requires RUSTFS_CONNECT_STATE_DIR and either both or neither of RUSTFS_CONNECT_ENDPOINT and RUSTFS_CONNECT_ROOT_CA_FILE"
    )]
    Partial,
    #[error("RUSTFS_CONNECT_ENDPOINT is not valid UTF-8")]
    EndpointEncoding,
    #[error("Connect root CA could not be read")]
    RootCertificate {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("Connect inventory persistence requires Linux filesystem security guarantees")]
    PlatformSecurity,
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
        assert!(matches!(
            HeartbeatConfig::from_env_values(
                Some(OsString::from("https://connect.example/agent/")),
                Some(OsString::from("root.pem")),
                None,
            ),
            Err(HeartbeatConfigError::Partial)
        ));
        assert!(matches!(
            HeartbeatConfig::from_env_values(None, Some(OsString::from("root.pem")), Some(OsString::from("state"))),
            Err(HeartbeatConfigError::Partial)
        ));
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn state_directory_alone_enables_local_inventory_without_transport() {
        let state = tempfile::tempdir().expect("tempdir").keep();
        let config = HeartbeatConfig::from_env_values(None, None, Some(state.clone().into_os_string()))
            .expect("state-only config")
            .expect("enabled config");

        assert_eq!(config.state_root(), Some(state.as_path()));
        assert!(!config.transport_enabled());
    }

    #[test]
    #[cfg(target_os = "linux")]
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
        assert_eq!(config.state_root(), Some(state.as_path()));
        assert!(!state.exists(), "parsing configuration must not create state");
    }

    #[test]
    #[cfg(not(target_os = "linux"))]
    fn configured_inventory_fails_without_linux_filesystem_guarantees() {
        assert!(matches!(
            HeartbeatConfig::from_env_values(None, None, Some(OsString::from("state"))),
            Err(HeartbeatConfigError::PlatformSecurity)
        ));
        assert!(matches!(
            HeartbeatConfig::from_env_values(
                Some(OsString::from("https://connect.example/agent/")),
                Some(OsString::from("missing-root.pem")),
                Some(OsString::from("state")),
            ),
            Err(HeartbeatConfigError::PlatformSecurity)
        ));
    }
}
