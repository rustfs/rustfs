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
use std::fmt;
#[cfg(unix)]
use std::fs::{self, OpenOptions};
#[cfg(unix)]
use std::io::Read as _;
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _};
use std::path::PathBuf;
use std::time::Duration;

use reqwest::{ClientBuilder, NoProxy, Proxy, Url};
use zeroize::Zeroizing;

use super::{CredentialStore, IdentityStore};

pub const ENV_CONNECT_ENDPOINT: &str = "RUSTFS_CONNECT_ENDPOINT";
pub const ENV_CONNECT_ROOT_CA_FILE: &str = "RUSTFS_CONNECT_ROOT_CA_FILE";
pub const ENV_CONNECT_STATE_DIR: &str = "RUSTFS_CONNECT_STATE_DIR";
pub const ENV_CONNECT_PROXY_URL: &str = "RUSTFS_CONNECT_PROXY_URL";
pub const ENV_CONNECT_PROXY_BYPASS: &str = "RUSTFS_CONNECT_PROXY_BYPASS";
pub const ENV_CONNECT_PROXY_USERNAME_FILE: &str = "RUSTFS_CONNECT_PROXY_USERNAME_FILE";
pub const ENV_CONNECT_PROXY_PASSWORD_FILE: &str = "RUSTFS_CONNECT_PROXY_PASSWORD_FILE";

const MAX_PROXY_BYPASS_BYTES: usize = 2048;
const MAX_PROXY_USERNAME_BYTES: usize = 256;
const MAX_PROXY_PASSWORD_BYTES: usize = 4096;

/// Explicit HTTP CONNECT proxy configuration for RustFS Connect traffic.
#[derive(Clone)]
pub struct ProxyConfig {
    url: Url,
    bypass: Option<String>,
    username: Option<Zeroizing<String>>,
    password: Option<Zeroizing<String>>,
}

impl ProxyConfig {
    /// Creates an unauthenticated proxy configuration.
    pub fn new(url: &str, bypass: Option<&str>) -> Result<Self, ProxyConfigError> {
        let url = proxy_url(url)?;
        let bypass = proxy_bypass(bypass)?;
        Ok(Self {
            url,
            bypass,
            username: None,
            password: None,
        })
    }

    /// Adds HTTP Basic authentication without placing credentials in the proxy URL.
    pub fn with_basic_auth(mut self, username: &str, password: &str) -> Result<Self, ProxyConfigError> {
        validate_proxy_secret(username, MAX_PROXY_USERNAME_BYTES)?;
        validate_proxy_secret(password, MAX_PROXY_PASSWORD_BYTES)?;
        self.username = Some(Zeroizing::new(username.to_owned()));
        self.password = Some(Zeroizing::new(password.to_owned()));
        Ok(self)
    }

    /// Loads an explicit proxy and optional protected Basic-auth files from RustFS-specific environment variables.
    pub fn from_env() -> Result<Option<Self>, ProxyConfigError> {
        Self::from_env_values(
            env::var_os(ENV_CONNECT_PROXY_URL),
            env::var_os(ENV_CONNECT_PROXY_BYPASS),
            env::var_os(ENV_CONNECT_PROXY_USERNAME_FILE),
            env::var_os(ENV_CONNECT_PROXY_PASSWORD_FILE),
        )
    }

    pub(crate) fn apply(&self, builder: ClientBuilder) -> Result<ClientBuilder, ProxyConfigError> {
        let mut proxy = Proxy::https(self.url.clone()).map_err(|_| ProxyConfigError::Url)?;
        if let Some(bypass) = self.bypass.as_deref() {
            proxy = proxy.no_proxy(NoProxy::from_string(bypass));
        }
        if let (Some(username), Some(password)) = (&self.username, &self.password) {
            proxy = proxy.basic_auth(username, password);
        }
        Ok(builder.proxy(proxy))
    }

    #[cfg(unix)]
    fn from_env_values(
        url: Option<OsString>,
        bypass: Option<OsString>,
        username_file: Option<OsString>,
        password_file: Option<OsString>,
    ) -> Result<Option<Self>, ProxyConfigError> {
        let configured = url.is_some() || bypass.is_some() || username_file.is_some() || password_file.is_some();
        if !configured {
            return Ok(None);
        }
        let Some(url) = url else {
            return Err(ProxyConfigError::Partial);
        };
        if username_file.is_some() != password_file.is_some() {
            return Err(ProxyConfigError::Partial);
        }
        let url = url.into_string().map_err(|_| ProxyConfigError::Encoding)?;
        let bypass = bypass
            .map(|value| value.into_string().map_err(|_| ProxyConfigError::Encoding))
            .transpose()?;
        let mut config = Self::new(&url, bypass.as_deref())?;
        if let (Some(username_file), Some(password_file)) = (username_file, password_file) {
            let username = read_proxy_secret(PathBuf::from(username_file), MAX_PROXY_USERNAME_BYTES)?;
            let password = read_proxy_secret(PathBuf::from(password_file), MAX_PROXY_PASSWORD_BYTES)?;
            config = config.with_basic_auth(&username, &password)?;
        }
        Ok(Some(config))
    }

    #[cfg(not(unix))]
    fn from_env_values(
        url: Option<OsString>,
        bypass: Option<OsString>,
        username_file: Option<OsString>,
        password_file: Option<OsString>,
    ) -> Result<Option<Self>, ProxyConfigError> {
        if url.is_none() && bypass.is_none() && username_file.is_none() && password_file.is_none() {
            Ok(None)
        } else {
            Err(ProxyConfigError::PlatformSecurity)
        }
    }
}

impl fmt::Debug for ProxyConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProxyConfig")
            .field("configured", &true)
            .field("bypass_configured", &self.bypass.is_some())
            .field("authentication_configured", &self.username.is_some())
            .finish()
    }
}

fn proxy_url(value: &str) -> Result<Url, ProxyConfigError> {
    let url = Url::parse(value).map_err(|_| ProxyConfigError::Url)?;
    if url.scheme() != "http"
        || url.host_str().is_none()
        || url.cannot_be_a_base()
        || !url.username().is_empty()
        || url.password().is_some()
        || !matches!(url.path(), "" | "/")
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(ProxyConfigError::Url);
    }
    Ok(url)
}

fn proxy_bypass(value: Option<&str>) -> Result<Option<String>, ProxyConfigError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_empty()
        || value.len() > MAX_PROXY_BYPASS_BYTES
        || !value.is_ascii()
        || value.split(',').any(|entry| !valid_bypass_entry(entry.trim()))
    {
        return Err(ProxyConfigError::Bypass);
    }
    Ok(Some(value.to_owned()))
}

fn valid_bypass_entry(value: &str) -> bool {
    if value == "*" || value.parse::<std::net::IpAddr>().is_ok() {
        return true;
    }
    if let Some((network, prefix)) = value.split_once('/') {
        let Ok(address) = network.parse::<std::net::IpAddr>() else {
            return false;
        };
        let Ok(prefix) = prefix.parse::<u8>() else {
            return false;
        };
        return prefix <= if address.is_ipv4() { 32 } else { 128 };
    }
    let domain = value.strip_prefix('.').unwrap_or(value);
    !domain.is_empty()
        && domain.len() <= 253
        && domain.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
}

fn validate_proxy_secret(value: &str, maximum: usize) -> Result<(), ProxyConfigError> {
    if value.is_empty() || value.len() > maximum || value.chars().any(char::is_control) {
        return Err(ProxyConfigError::Authentication);
    }
    Ok(())
}

#[cfg(unix)]
fn read_proxy_secret(path: PathBuf, maximum: usize) -> Result<Zeroizing<String>, ProxyConfigError> {
    let initial = fs::symlink_metadata(&path).map_err(|source| ProxyConfigError::SecretFile {
        path: path.clone(),
        source,
    })?;
    if !initial.file_type().is_file() || initial.permissions().mode() & 0o077 != 0 {
        return Err(ProxyConfigError::SecretFileSecurity { path });
    }
    let mut options = OpenOptions::new();
    options.read(true).custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let mut file = options.open(&path).map_err(|source| ProxyConfigError::SecretFile {
        path: path.clone(),
        source,
    })?;
    let opened = file.metadata().map_err(|source| ProxyConfigError::SecretFile {
        path: path.clone(),
        source,
    })?;
    if !opened.is_file()
        || opened.uid() != process_uid()
        || opened.dev() != initial.dev()
        || opened.ino() != initial.ino()
        || opened.len() > maximum as u64 + 2
    {
        return Err(ProxyConfigError::SecretFileSecurity { path });
    }
    let mut bytes = Zeroizing::new(Vec::with_capacity(opened.len() as usize));
    file.read_to_end(&mut bytes).map_err(|source| ProxyConfigError::SecretFile {
        path: path.clone(),
        source,
    })?;
    while matches!(bytes.last(), Some(b'\n' | b'\r')) {
        bytes.pop();
    }
    let value = Zeroizing::new(String::from_utf8(std::mem::take(&mut *bytes)).map_err(|_| ProxyConfigError::Authentication)?);
    validate_proxy_secret(&value, maximum)?;
    Ok(value)
}

#[cfg(unix)]
// SAFETY: geteuid has no pointer arguments or caller preconditions.
#[allow(unsafe_code)]
fn process_uid() -> u32 {
    unsafe { libc::geteuid() }
}

#[derive(Debug, thiserror::Error)]
pub enum ProxyConfigError {
    #[error("Connect proxy configuration requires RUSTFS_CONNECT_PROXY_URL and both or neither authentication files")]
    Partial,
    #[error("Connect proxy configuration is not valid UTF-8")]
    Encoding,
    #[error("Connect proxy must be an HTTP base URL without credentials, path, query, or fragment")]
    Url,
    #[error("Connect proxy bypass rules are invalid")]
    Bypass,
    #[error("Connect proxy credentials are invalid")]
    Authentication,
    #[error("Connect proxy credential file must be an owner-only regular file: {path}")]
    SecretFileSecurity { path: PathBuf },
    #[error("Connect proxy credential file could not be read: {path}")]
    SecretFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("Connect proxy credential files require Unix filesystem security guarantees")]
    PlatformSecurity,
}

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
    pub proxy: Option<ProxyConfig>,
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
            proxy: None,
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
            proxy: None,
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
            env::var_os(ENV_CONNECT_PROXY_URL),
            env::var_os(ENV_CONNECT_PROXY_BYPASS),
            env::var_os(ENV_CONNECT_PROXY_USERNAME_FILE),
            env::var_os(ENV_CONNECT_PROXY_PASSWORD_FILE),
        )
    }

    fn from_env_values(
        endpoint: Option<OsString>,
        root_ca_file: Option<OsString>,
        state_dir: Option<OsString>,
        proxy_url: Option<OsString>,
        proxy_bypass: Option<OsString>,
        proxy_username_file: Option<OsString>,
        proxy_password_file: Option<OsString>,
    ) -> Result<Option<Self>, HeartbeatConfigError> {
        let proxy_configured =
            proxy_url.is_some() || proxy_bypass.is_some() || proxy_username_file.is_some() || proxy_password_file.is_some();
        let configured = endpoint.is_some()
            || root_ca_file.is_some()
            || state_dir.is_some()
            || proxy_url.is_some()
            || proxy_bypass.is_some()
            || proxy_username_file.is_some()
            || proxy_password_file.is_some();
        if !configured {
            return Ok(None);
        }
        let Some(state_dir) = state_dir else {
            return Err(HeartbeatConfigError::Partial);
        };
        let state_dir = PathBuf::from(state_dir);
        if state_dir.as_os_str().is_empty()
            || endpoint.is_some() != root_ca_file.is_some()
            || (proxy_configured && endpoint.is_none())
        {
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
        let proxy = ProxyConfig::from_env_values(proxy_url, proxy_bypass, proxy_username_file, proxy_password_file)?;
        #[cfg(target_os = "linux")]
        {
            let mut config = Self::new(
                endpoint,
                root_ca_pem,
                IdentityStore::new(state_dir.join("identity")),
                CredentialStore::new(state_dir.join("credential")),
                state_dir.join("heartbeat/state.json"),
            );
            config.proxy = proxy;
            Ok(Some(config))
        }
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
    #[error(transparent)]
    Proxy(#[from] ProxyConfigError),
}

#[cfg(test)]
mod tests {
    use super::{HeartbeatConfig, HeartbeatConfigError, ProxyConfig, ProxyConfigError};
    use std::ffi::OsString;

    #[test]
    fn proxy_rejects_implicit_credentials_and_non_http_transport() {
        assert!(matches!(
            ProxyConfig::new("http://user:secret@proxy.example:8080", None),
            Err(ProxyConfigError::Url)
        ));
        assert!(matches!(ProxyConfig::new("https://proxy.example:8443", None), Err(ProxyConfigError::Url)));
        assert!(matches!(
            ProxyConfig::new("http://proxy.example:8080/tunnel", None),
            Err(ProxyConfigError::Url)
        ));
    }

    #[test]
    fn proxy_debug_output_contains_no_endpoint_or_credentials() {
        let proxy = ProxyConfig::new("http://sensitive-proxy.example:8080", Some("private.example"))
            .expect("proxy")
            .with_basic_auth("sensitive-user", "sensitive-password")
            .expect("authentication");
        let debug = format!("{proxy:?}");

        for secret in ["sensitive-proxy", "private.example", "sensitive-user", "sensitive-password"] {
            assert!(!debug.contains(secret));
        }
        assert!(debug.contains("authentication_configured: true"));
    }

    #[test]
    #[cfg(unix)]
    fn proxy_authentication_requires_owner_only_regular_files() {
        use std::os::unix::fs::PermissionsExt as _;

        let temp = tempfile::tempdir().expect("tempdir");
        let username = temp.path().join("username");
        let password = temp.path().join("password");
        std::fs::write(&username, b"proxy-user\n").expect("username");
        std::fs::write(&password, b"proxy-password\n").expect("password");
        std::fs::set_permissions(&username, std::fs::Permissions::from_mode(0o600)).expect("username mode");
        std::fs::set_permissions(&password, std::fs::Permissions::from_mode(0o644)).expect("password mode");

        assert!(matches!(
            ProxyConfig::from_env_values(
                Some(OsString::from("http://proxy.example:8080")),
                None,
                Some(username.clone().into_os_string()),
                Some(password.clone().into_os_string()),
            ),
            Err(ProxyConfigError::SecretFileSecurity { .. })
        ));
        std::fs::set_permissions(&password, std::fs::Permissions::from_mode(0o600)).expect("private password mode");
        let proxy = ProxyConfig::from_env_values(
            Some(OsString::from("http://proxy.example:8080")),
            Some(OsString::from("localhost,127.0.0.1")),
            Some(username.into_os_string()),
            Some(password.into_os_string()),
        )
        .expect("valid proxy")
        .expect("configured proxy");
        assert!(!format!("{proxy:?}").contains("proxy-password"));
    }

    #[test]
    fn absent_environment_is_disabled_without_side_effects() {
        assert!(
            HeartbeatConfig::from_env_values(None, None, None, None, None, None, None)
                .expect("absent config")
                .is_none()
        );
    }

    #[test]
    fn partial_environment_is_rejected() {
        assert!(matches!(
            HeartbeatConfig::from_env_values(
                Some(OsString::from("https://connect.example/agent/")),
                None,
                None,
                None,
                None,
                None,
                None,
            ),
            Err(HeartbeatConfigError::Partial)
        ));
        assert!(matches!(
            HeartbeatConfig::from_env_values(
                Some(OsString::from("https://connect.example/agent/")),
                Some(OsString::from("root.pem")),
                None,
                None,
                None,
                None,
                None,
            ),
            Err(HeartbeatConfigError::Partial)
        ));
        assert!(matches!(
            HeartbeatConfig::from_env_values(
                None,
                Some(OsString::from("root.pem")),
                Some(OsString::from("state")),
                None,
                None,
                None,
                None,
            ),
            Err(HeartbeatConfigError::Partial)
        ));
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn state_directory_alone_enables_local_inventory_without_transport() {
        let state = tempfile::tempdir().expect("tempdir").keep();
        let config = HeartbeatConfig::from_env_values(None, None, Some(state.clone().into_os_string()), None, None, None, None)
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
            None,
            None,
            None,
            None,
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
            HeartbeatConfig::from_env_values(None, None, Some(OsString::from("state")), None, None, None, None),
            Err(HeartbeatConfigError::PlatformSecurity)
        ));
        assert!(matches!(
            HeartbeatConfig::from_env_values(
                Some(OsString::from("https://connect.example/agent/")),
                Some(OsString::from("missing-root.pem")),
                Some(OsString::from("state")),
                None,
                None,
                None,
                None,
            ),
            Err(HeartbeatConfigError::PlatformSecurity)
        ));
    }
}
