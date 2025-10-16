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

#[derive(Debug, Clone)]
pub struct AuthZPlugin {
    client: reqwest::Client,
    args: Args,
}

fn check() -> Result<(), String> {
    let env_list = env::vars();
    let mut candidate = HashMap::new();
    let prefix = format!("{}{}", ENV_PREFIX, POLICY_PLUGIN_SUB_SYS).to_uppercase();
    for (key, value) in env_list {
        if key.starts_with(&prefix) {
            candidate.insert(key.to_string(), value);
        }
    }

    //check required env vars
    if candidate.remove(ENV_POLICY_PLUGIN_OPA_URL).is_none() {
        return Err(format!("Missing required env var: {}", ENV_POLICY_PLUGIN_OPA_URL));
    }

    // check optional env vars
    candidate.remove(ENV_POLICY_PLUGIN_AUTH_TOKEN);
    if !candidate.is_empty() {
        return Err(format!("Invalid env vars: {:?}", candidate));
    }
    Ok(())
}
async fn validate(config: &Args) -> Result<(), String> {
    let client = reqwest::Client::new();

    match client.post(&config.url).send().await {
        Ok(resp) => {
            match resp.status() {
                reqwest::StatusCode::OK => {
                    info!("OPA is ready to accept requests.");
                }
                _ => {
                    return Err(format!("OPA returned an error: {}", resp.status()));
                }
            };
        }
        Err(err) => {
            return Err(format!("Error connecting to OPA: {}", err));
        }
    };
    Ok(())
}

pub async fn lookup_config() -> Result<Args, String> {
    let args = Args::default();

    let get_cfg =
        |cfg: &str| -> Result<String, String> { env::var(cfg).map_err(|e| format!("Error getting env var {}: {:?}", cfg, e)) };

    let url = get_cfg(ENV_POLICY_PLUGIN_OPA_URL);
    if url.is_err() {
        info!("OPA is not enabled.");
        return Ok(args);
    }
    check()?;
    let args = Args {
        url: url.ok().unwrap(),
        auth_token: get_cfg(ENV_POLICY_PLUGIN_AUTH_TOKEN).unwrap_or_default(),
    };
    validate(&args).await?;
    Ok(args)
}

impl AuthZPlugin {
    pub fn new(config: Args) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .connect_timeout(Duration::from_secs(1))
            .pool_max_idle_per_host(10)
            .pool_idle_timeout(Some(Duration::from_secs(60)))
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .tcp_nodelay(true)
            .http2_keep_alive_interval(Some(Duration::from_secs(30)))
            .http2_keep_alive_timeout(Duration::from_secs(15))
            .build()
            .unwrap();

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
                    error!("OPA returned non-success status: {}", status);
                    return false;
                }

                match resp.json::<OpaResponseEnum>().await {
                    Ok(response_enum) => match response_enum {
                        OpaResponseEnum::SimpleResult(result) => result.result,
                        OpaResponseEnum::AllowResult(response) => response.result.allow,
                    },
                    Err(err) => {
                        error!("Error parsing OPA response: {:?}", err);
                        false
                    }
                }
            }
            Err(err) => {
                error!("Error sending request to OPA: {:?}", err);
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
                        "timestamp": chrono::Utc::now().to_rfc3339()
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
    use temp_env;

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
                assert!(result.unwrap_err().contains("Missing required env var"));
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
                assert!(result.unwrap_err().contains("Invalid env vars"));
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
}
