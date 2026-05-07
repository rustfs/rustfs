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

use crate::TargetError;
use crate::target::TargetType;
use rustfs_config::{AMQP_TLS_CA, AMQP_TLS_CLIENT_CERT, AMQP_TLS_CLIENT_KEY};
use std::path::Path;
use url::Url;

#[derive(Debug, Clone)]
pub struct AMQPArgs {
    pub enable: bool,
    pub url: Url,
    pub exchange: String,
    pub routing_key: String,
    pub mandatory: bool,
    pub persistent: bool,
    pub username: String,
    pub password: String,
    pub tls_ca: String,
    pub tls_client_cert: String,
    pub tls_client_key: String,
    pub queue_dir: String,
    pub queue_limit: u64,
    pub target_type: TargetType,
}

impl AMQPArgs {
    pub fn validate(&self) -> Result<(), TargetError> {
        if !self.enable {
            return Ok(());
        }

        validate_amqp_url(&self.url)?;

        if self.exchange.trim().is_empty() {
            return Err(TargetError::Configuration("AMQP exchange cannot be empty".to_string()));
        }
        if self.routing_key.trim().is_empty() {
            return Err(TargetError::Configuration("AMQP routing_key cannot be empty".to_string()));
        }

        let url_has_credentials = !self.url.username().is_empty() || self.url.password().is_some();
        let config_has_credentials = !self.username.is_empty() || !self.password.is_empty();
        if self.username.is_empty() != self.password.is_empty() {
            return Err(TargetError::Configuration(
                "AMQP username and password must be specified together".to_string(),
            ));
        }
        if url_has_credentials && config_has_credentials {
            return Err(TargetError::Configuration(
                "AMQP credentials must be specified either in url or username/password, not both".to_string(),
            ));
        }

        validate_amqp_tls_paths(self)?;

        if !self.queue_dir.is_empty() && !Path::new(&self.queue_dir).is_absolute() {
            return Err(TargetError::Configuration("AMQP queue directory must be an absolute path".to_string()));
        }

        Ok(())
    }
}

pub fn validate_amqp_url(url: &Url) -> Result<(), TargetError> {
    match url.scheme() {
        "amqp" | "amqps" => {
            if url.host_str().is_none() {
                return Err(TargetError::Configuration("AMQP URL is missing host".to_string()));
            }
            Ok(())
        }
        scheme => Err(TargetError::Configuration(format!(
            "Unsupported AMQP URL scheme: {scheme} (only amqp and amqps are allowed)"
        ))),
    }
}

fn validate_amqp_tls_paths(args: &AMQPArgs) -> Result<(), TargetError> {
    let has_tls_settings = !args.tls_ca.is_empty() || !args.tls_client_cert.is_empty() || !args.tls_client_key.is_empty();
    if has_tls_settings && args.url.scheme() != "amqps" {
        return Err(TargetError::Configuration(
            "AMQP TLS settings are only allowed with amqps URLs".to_string(),
        ));
    }

    if args.tls_client_cert.is_empty() != args.tls_client_key.is_empty() {
        return Err(TargetError::Configuration(
            "AMQP tls_client_cert and tls_client_key must be specified together".to_string(),
        ));
    }

    if !args.tls_ca.is_empty() && !Path::new(&args.tls_ca).is_absolute() {
        return Err(TargetError::Configuration(format!("{AMQP_TLS_CA} must be an absolute path")));
    }
    if !args.tls_client_cert.is_empty() && !Path::new(&args.tls_client_cert).is_absolute() {
        return Err(TargetError::Configuration(format!("{AMQP_TLS_CLIENT_CERT} must be an absolute path")));
    }
    if !args.tls_client_key.is_empty() && !Path::new(&args.tls_client_key).is_absolute() {
        return Err(TargetError::Configuration(format!("{AMQP_TLS_CLIENT_KEY} must be an absolute path")));
    }

    Ok(())
}
