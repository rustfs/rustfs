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

use crate::policy::Args as PArgs;
use rustfs_config::{ENV_PREFIX, opa::*};
use serde::Deserialize;
use serde_json::json;
use std::{collections::HashMap, env, time::Duration};
use tracing::{error, info};

#[derive(Debug, Clone, Default)]
pub struct Args {
    pub url: String,
    pub auth_token: String,
}
impl Args {
    pub fn enable(&self) -> bool {
        !self.url.is_empty()
    }
}

pub fn is_configured() -> bool {
    env::var_os(ENV_POLICY_PLUGIN_OPA_URL).is_some_and(|url| !url.is_empty())
}

#[derive(Debug, Clone)]
pub struct AuthZPlugin {
    client: reqwest::Client,
    args: Args,
}

#[derive(Debug, thiserror::Error)]
pub enum OpaConfigError {
    #[error("Missing required env var: {0}")]
    MissingRequiredEnv(&'static str),
    #[error("Invalid env vars: {0:?}")]
    InvalidEnvVars(HashMap<String, String>),
    #[error("Error getting env var {name}: {source:?}")]
    EnvRead {
        name: &'static str,
        #[source]
        source: env::VarError,
    },
    #[error("OPA returned an error: {0}")]
    InvalidStatus(reqwest::StatusCode),
    #[error("Error connecting to OPA: {0}")]
    Connection(reqwest::Error),
}

impl OpaConfigError {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::MissingRequiredEnv(_) => "missing_required_env",
            Self::InvalidEnvVars(_) => "invalid_env_vars",
            Self::EnvRead { .. } => "env_read_failed",
            Self::InvalidStatus(_) => "invalid_status",
            Self::Connection(_) => "connection_failed",
        }
    }
}

fn redact_opa_connection_error(error: reqwest::Error) -> OpaConfigError {
    OpaConfigError::Connection(error.without_url())
}

fn opa_request_error_kind(error: &reqwest::Error) -> &'static str {
    if error.is_timeout() {
        "timeout"
    } else if error.is_connect() {
        "connect"
    } else if error.is_request() {
        "request"
    } else if error.is_body() {
        "body"
    } else if error.is_decode() {
        "decode"
    } else if error.is_status() {
        "status"
    } else if error.is_builder() {
        "builder"
    } else if error.is_redirect() {
        "redirect"
    } else {
        "other"
    }
}

fn check() -> Result<(), OpaConfigError> {
    let env_list = env::vars();
    let mut candidate = HashMap::new();
    let prefix = format!("{ENV_PREFIX}{POLICY_PLUGIN_SUB_SYS}").to_uppercase();
    for (key, value) in env_list {
        if key.starts_with(&prefix) {
            candidate.insert(key.to_string(), value);
        }
    }

    //check required env vars
    if candidate.remove(ENV_POLICY_PLUGIN_OPA_URL).is_none() {
        return Err(OpaConfigError::MissingRequiredEnv(ENV_POLICY_PLUGIN_OPA_URL));
    }

    // check optional env vars
    candidate.remove(ENV_POLICY_PLUGIN_AUTH_TOKEN);
    if !candidate.is_empty() {
        return Err(OpaConfigError::InvalidEnvVars(candidate));
    }
    Ok(())
}
async fn validate(config: &Args) -> Result<(), OpaConfigError> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .connect_timeout(Duration::from_secs(1))
        .build()
        .map_err(redact_opa_connection_error)?;

    let mut request = client.post(&config.url);
    if !config.auth_token.is_empty() {
        request = request.header("Authorization", format!("Bearer {}", config.auth_token));
    }

    match request.send().await {
        Ok(resp) => {
            match resp.status() {
                reqwest::StatusCode::OK => {
                    info!("OPA is ready to accept requests.");
                }
                _ => {
                    return Err(OpaConfigError::InvalidStatus(resp.status()));
                }
            };
        }
        Err(err) => {
            return Err(redact_opa_connection_error(err));
        }
    };
    Ok(())
}

pub async fn lookup_config() -> Result<Args, OpaConfigError> {
    let args = Args::default();

    let get_cfg = |cfg: &'static str| -> Result<String, OpaConfigError> {
        env::var(cfg).map_err(|source| OpaConfigError::EnvRead { name: cfg, source })
    };

    let url = match get_cfg(ENV_POLICY_PLUGIN_OPA_URL) {
        Ok(url) => url,
        Err(_) => {
            info!("OPA is not enabled.");
            return Ok(args);
        }
    };
    check()?;
    let args = Args {
        url,
        auth_token: get_cfg(ENV_POLICY_PLUGIN_AUTH_TOKEN).unwrap_or_default(),
    };
    validate(&args).await?;
    Ok(args)
}

impl AuthZPlugin {
    pub fn new(config: Args) -> Self {
        let builder = || {
            reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .connect_timeout(Duration::from_secs(1))
                .pool_max_idle_per_host(10)
                .pool_idle_timeout(Some(Duration::from_secs(60)))
                .tcp_keepalive(Some(Duration::from_secs(30)))
                .tcp_nodelay(true)
                .http2_keep_alive_interval(Some(Duration::from_secs(30)))
                .http2_keep_alive_timeout(Duration::from_secs(15))
        };
        // Never fall back to `reqwest::Client::new()`: it panics for the same
        // reason the first build failed (e.g. no system CA bundle, issue
        // #6734). Retry with an explicit empty trust store instead — an HTTP
        // OPA endpoint keeps working, an HTTPS one fails closed per request.
        let client = builder().build().unwrap_or_else(|err| {
            error!("failed to build OPA HTTP client ({err}); continuing with an empty trust store");
            builder()
                .tls_certs_only(std::iter::empty::<reqwest::Certificate>())
                .build()
                .expect("HTTP client construction must succeed with an explicit empty trust store")
        });

        Self { client, args: config }
    }

    pub async fn is_allowed(&self, args: &PArgs<'_>) -> bool {
        let payload = self.build_opa_input(args);

        let mut request = self.client.post(self.args.url.clone()).json(&payload);
        if !self.args.auth_token.is_empty() {
            request = request.header("Authorization", format!("Bearer {}", self.args.auth_token));
        }

        match request.send().await {
            Ok(resp) => {
                let status = resp.status();
                if !status.is_success() {
                    error!(
                        component = "policy",
                        subsystem = "opa",
                        result = "request_rejected",
                        status = %status,
                        "OPA request rejected"
                    );
                    return false;
                }

                match resp.json::<OpaResponseEnum>().await {
                    Ok(response_enum) => match response_enum {
                        OpaResponseEnum::SimpleResult(result) => result.result,
                        OpaResponseEnum::AllowResult(response) => response.result.allow,
                    },
                    Err(err) => {
                        error!(
                            component = "policy",
                            subsystem = "opa",
                            result = "response_decode_failed",
                            error_kind = opa_request_error_kind(&err),
                            "OPA response decoding failed"
                        );
                        false
                    }
                }
            }
            Err(err) => {
                error!(
                    component = "policy",
                    subsystem = "opa",
                    result = "request_failed",
                    error_kind = opa_request_error_kind(&err),
                    "OPA request failed"
                );
                false
            }
        }
    }

    fn build_opa_input(&self, args: &PArgs<'_>) -> serde_json::Value {
        let groups = match args.groups {
            Some(g) => g.clone(),
            None => vec![],
        };
        let action_str: &str = (&args.action).into();
        json!({
                // Core authorization parameters for OPA policy evaluation
                "input":{
                    "identity": {
                        "account": args.account,
                        "groups": groups,
                        "is_owner": args.is_owner,
                        "claims": args.claims
                    },

                    "resource": {
                        "bucket": args.bucket,
                        "object": args.object,
                        "arn": if args.object.is_empty() {
                            format!("arn:aws:s3:::{}", args.bucket)
                        } else {
                            format!("arn:aws:s3:::{}/{}", args.bucket, args.object)
                        }
                    },

                    "action": action_str,

                    "context": {
                        "conditions": args.conditions,
                        "deny_only": args.deny_only,
                        "timestamp": jiff::Timestamp::now()
                            .display_with_offset(jiff::tz::Offset::UTC)
                            .to_string()
                    }
            }
        })
    }
}

#[derive(Deserialize, Default)]
struct OpaResultAllow {
    allow: bool,
}

#[derive(Deserialize, Default)]
struct OpaResult {
    result: bool,
}

#[derive(Deserialize, Default)]
struct OpaResponse {
    result: OpaResultAllow,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum OpaResponseEnum {
    SimpleResult(OpaResult),
    AllowResult(OpaResponse),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};
    use temp_env;

    #[derive(Clone)]
    struct TestWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for TestWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("lock test log buffer").extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for TestWriter {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    fn assert_reqwest_client(_: &reqwest::Client) {}

    #[test]
    fn test_check_valid_config() {
        // Use temp_env to temporarily set environment variables
        temp_env::with_vars(
            [
                ("RUSTFS_POLICY_PLUGIN_URL", Some("http://localhost:8181/v1/data/rustfs/authz/allow")),
                ("RUSTFS_POLICY_PLUGIN_AUTH_TOKEN", Some("test-token")),
            ],
            || {
                assert!(check().is_ok());
            },
        );
    }

    #[test]
    fn test_check_missing_required_env() {
        temp_env::with_var_unset("RUSTFS_POLICY_PLUGIN_URL", || {
            temp_env::with_var("RUSTFS_POLICY_PLUGIN_AUTH_TOKEN", Some("test-token"), || {
                let result = check();
                assert!(result.is_err());
                assert!(matches!(result.unwrap_err(), OpaConfigError::MissingRequiredEnv(_)));
            });
        });
    }

    #[test]
    fn test_check_invalid_env_vars() {
        temp_env::with_vars(
            [
                ("RUSTFS_POLICY_PLUGIN_URL", Some("http://localhost:8181/v1/data/rustfs/authz/allow")),
                ("RUSTFS_POLICY_PLUGIN_INVALID", Some("invalid-value")),
            ],
            || {
                let result = check();
                assert!(result.is_err());
                assert!(matches!(result.unwrap_err(), OpaConfigError::InvalidEnvVars(_)));
            },
        );
    }

    #[test]
    fn test_lookup_config_not_enabled() {
        temp_env::with_var_unset("RUSTFS_POLICY_PLUGIN_URL", || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let result = rt.block_on(async { lookup_config().await });

            // Should return the default empty Args
            assert!(result.is_ok());
            let args = result.unwrap();
            assert!(!args.enable());
            assert_eq!(args.url, "");
            assert_eq!(args.auth_token, "");
        });
    }

    #[test]
    fn test_lookup_config_uses_env_url_without_unwrap_path() {
        temp_env::with_vars(
            [
                ("RUSTFS_POLICY_PLUGIN_URL", Some("http://localhost:8181")),
                ("RUSTFS_POLICY_PLUGIN_AUTH_TOKEN", Some("token")),
            ],
            || {
                let rt = tokio::runtime::Runtime::new().unwrap();
                let result = rt.block_on(async { lookup_config().await });
                assert!(result.is_err(), "lookup should fail validation without panicking when OPA is unreachable");
            },
        );
    }

    #[test]
    fn test_args_enable() {
        // Test Args enable method
        let args_enabled = Args {
            url: "http://localhost:8181".to_string(),
            auth_token: "token".to_string(),
        };
        assert!(args_enabled.enable());

        let args_disabled = Args {
            url: "".to_string(),
            auth_token: "".to_string(),
        };
        assert!(!args_disabled.enable());
    }

    #[test]
    fn test_sensitive_opa_endpoints_never_use_hotpath_http_wrapper() {
        let endpoints = [
            "https://profile-user:profile-token@10.24.0.5/private/opa?access_token=query-secret",
            "https://service.internal.example/private/path?token=another-secret",
        ];

        for endpoint in endpoints {
            let plugin = AuthZPlugin::new(Args {
                url: endpoint.to_string(),
                auth_token: "opa-auth-token".to_string(),
            });

            assert_reqwest_client(&plugin.client);
            assert_eq!(plugin.args.url, endpoint);
        }
    }

    #[test]
    fn test_build_opa_input_timestamp_serializes_as_rfc3339_utc() {
        let plugin = AuthZPlugin::new(Args {
            url: "http://127.0.0.1:8181/v1/data/rustfs/authz/allow".to_string(),
            auth_token: String::new(),
        });
        let groups = Some(vec!["developers".to_string()]);
        let conditions = HashMap::new();
        let claims = HashMap::new();
        let args = PArgs {
            account: "account",
            groups: &groups,
            action: crate::policy::action::Action::None,
            bucket: "bucket",
            conditions: &conditions,
            is_owner: false,
            object: "object",
            claims: &claims,
            deny_only: false,
        };

        let payload = plugin.build_opa_input(&args);
        let timestamp = payload
            .pointer("/input/context/timestamp")
            .and_then(|value| value.as_str())
            .expect("OPA input should include a timestamp string");

        timestamp
            .parse::<jiff::Timestamp>()
            .expect("OPA timestamp should remain RFC3339-compatible");
        assert!(timestamp.contains('T'), "OPA timestamp should use RFC3339 date-time form: {timestamp}");
        assert!(
            timestamp.ends_with("+00:00"),
            "OPA timestamp should preserve chrono::DateTime::to_rfc3339 UTC offset form: {timestamp}"
        );
    }

    #[test]
    fn test_opa_connection_error_removes_sensitive_endpoint() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind local test listener");
        let port = listener.local_addr().expect("read local test listener address").port();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept local test request");
            let mut request = [0_u8; 1024];
            let read = stream.read(&mut request).expect("read local test request");
            assert!(read > 0, "local test client must send an HTTP request");
            stream
                .write_all(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .expect("write local test response");
        });
        let endpoint = format!("http://profile-user:profile-token@127.0.0.1:{port}/private/opa?access_token=query-secret");
        let runtime = tokio::runtime::Runtime::new().expect("build tokio test runtime");
        let error = runtime.block_on(async {
            reqwest::Client::builder()
                .no_proxy()
                .build()
                .expect("build local test client")
                .get(&endpoint)
                .send()
                .await
                .expect("send local test request")
                .error_for_status()
                .expect_err("test response should be an HTTP error")
        });
        server.join().expect("join local test server");
        assert!(error.url().is_some(), "fixture must carry the endpoint before redaction");

        let OpaConfigError::Connection(error) = redact_opa_connection_error(error) else {
            panic!("expected a redacted OPA connection error");
        };
        let rendered = error.to_string();
        assert!(error.url().is_none());
        for leaked in ["profile-user", "profile-token", "127.0.0.1", "private/opa", "query-secret"] {
            assert!(!rendered.contains(leaked), "redacted error leaked {leaked}: {rendered}");
        }
    }

    #[test]
    fn test_is_allowed_rejection_log_redacts_sensitive_endpoint() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind local test listener");
        let port = listener.local_addr().expect("read local test listener address").port();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept local test request");
            let mut request = [0_u8; 1024];
            let read = stream.read(&mut request).expect("read local test request");
            assert!(read > 0, "local test client must send an HTTP request");
            stream
                .write_all(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .expect("write local test response");
        });
        let endpoint = format!("http://opa-user:opa-token@127.0.0.1:{port}/private/allow?access_token=query-secret");
        let plugin = AuthZPlugin::new(Args {
            url: endpoint,
            auth_token: "authorization-secret".to_string(),
        });
        let groups = None;
        let conditions = HashMap::new();
        let claims = HashMap::new();
        let args = PArgs {
            account: "account",
            groups: &groups,
            action: crate::policy::action::Action::None,
            bucket: "bucket",
            conditions: &conditions,
            is_owner: false,
            object: "object",
            claims: &claims,
            deny_only: false,
        };
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let writer = TestWriter(Arc::clone(&buffer));
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_writer(writer)
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build tokio test runtime");
            assert!(!runtime.block_on(plugin.is_allowed(&args)));
        });
        server.join().expect("join local test server");

        let rendered = String::from_utf8(buffer.lock().expect("lock test log buffer").clone()).expect("decode test log");
        assert!(!rendered.is_empty(), "OPA request failure did not reach the test log sink");
        assert!(
            rendered.contains("request_rejected"),
            "OPA request failure did not retain its stable result category"
        );
        assert!(rendered.contains("500 Internal Server Error"));
        for leaked in [
            "opa-user",
            "opa-token",
            "127.0.0.1",
            "private/allow",
            "query-secret",
            "authorization-secret",
        ] {
            assert!(!rendered.contains(leaked), "OPA request log leaked {leaked}: {rendered}");
        }
    }
}
