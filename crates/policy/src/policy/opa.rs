use crate::policy::Args as PArgs;
use reqwest;
use rustfs_config::{ENV_PREFIX, opa::*};
use serde::Deserialize;
use serde_json::json;
use std::{collections::HashMap, time::Duration};
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
    let env_list = std::env::vars();
    let mut candidate = HashMap::new();
    let prefix = format!("{}{}", ENV_PREFIX, POLICY_PLUGIN_SUB_SYS).to_uppercase();
    for (key, value) in env_list {
        if key.starts_with(&prefix) {
            candidate.insert(key.to_string(), value);
        }
    }

    for key in ENV_POLICY_PLUGIN_KEYS {
        if candidate.remove(*key).is_none() {
            return Err(format!("Missing required env var: {}", key));
        }
    }
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

    let get_cfg = |cfg: &str| -> Result<String, String> {
        std::env::var(cfg).map_err(|e| format!("Error getting env var {}: {:?}", cfg, e))
    };

    let url = get_cfg(ENV_POLICY_PLUGIN_OPA_URL);
    if url.is_err() {
        info!("OPA is not enabled.");
        return Ok(args);
    }
    check()?;
    let args = Args {
        url: url.ok().unwrap(),
        auth_token: get_cfg(ENV_POLICY_PLUGIN_AUTH_TOKEN)?,
    };
    validate(&args).await?;
    Ok(args)
}

impl AuthZPlugin {
    pub fn new(config: Args) -> Self {
        let client = reqwest::Client::builder()
            // 超时设置
            .timeout(Duration::from_secs(5)) // 总超时
            .connect_timeout(Duration::from_secs(1)) // 连接超时
            // 连接池设置
            .pool_max_idle_per_host(10) // 每主机最大空闲连接
            .pool_idle_timeout(Some(Duration::from_secs(60))) // 空闲连接超时
            // TCP 设置
            .tcp_keepalive(Some(Duration::from_secs(30))) // TCP keepalive
            .tcp_nodelay(true) // 禁用 Nagle 算法
            // HTTP/2 设置
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
                // 核心权限检查参数
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
