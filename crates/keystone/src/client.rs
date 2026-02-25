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

use crate::{EC2Credential, KeystoneError, KeystoneToken, KeystoneVersion, Result};
use reqwest::{Client, StatusCode};
use serde_json::json;
use std::sync::Arc;
use time::OffsetDateTime;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Keystone client for API interactions
#[derive(Clone)]
pub struct KeystoneClient {
    client: Client,
    auth_url: String,
    version: KeystoneVersion,
    admin_token: Arc<RwLock<Option<AdminToken>>>,
    admin_user: Option<String>,
    admin_password: Option<String>,
    admin_project: Option<String>,
    admin_domain: String,
    #[allow(dead_code)]
    verify_ssl: bool,
}

#[derive(Clone)]
struct AdminToken {
    token: String,
    expires_at: OffsetDateTime,
}

impl AdminToken {
    fn is_expired(&self) -> bool {
        OffsetDateTime::now_utc() >= self.expires_at
    }
}

impl KeystoneClient {
    /// Create new Keystone client
    pub fn new(
        auth_url: String,
        version: KeystoneVersion,
        admin_user: Option<String>,
        admin_password: Option<String>,
        admin_project: Option<String>,
        verify_ssl: bool,
    ) -> Self {
        let client = Client::builder()
            .danger_accept_invalid_certs(!verify_ssl)
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap();

        Self {
            client,
            auth_url,
            version,
            admin_token: Arc::new(RwLock::new(None)),
            admin_user,
            admin_password,
            admin_project,
            admin_domain: "Default".to_string(),
            verify_ssl,
        }
    }

    /// Validate a Keystone token
    pub async fn validate_token(&self, token: &str) -> Result<KeystoneToken> {
        match self.version {
            KeystoneVersion::V3 => self.validate_token_v3(token).await,
            KeystoneVersion::V2_0 => self.validate_token_v2(token).await,
        }
    }

    /// Validate token using Keystone v3 API
    async fn validate_token_v3(&self, token: &str) -> Result<KeystoneToken> {
        let url = format!("{}/v3/auth/tokens", self.auth_url);

        debug!("Validating token with Keystone v3: {}", url);

        let response = self
            .client
            .get(&url)
            .header("X-Auth-Token", token)
            .header("X-Subject-Token", token)
            .send()
            .await
            .map_err(|e| {
                error!("Failed to send token validation request: {}", e);
                KeystoneError::HttpError(e.to_string())
            })?;

        let status = response.status();
        debug!("Token validation response status: {}", status);

        if status == StatusCode::NOT_FOUND || status == StatusCode::UNAUTHORIZED {
            return Err(KeystoneError::InvalidToken);
        }

        if !status.is_success() {
            return Err(KeystoneError::AuthenticationFailed(format!(
                "Token validation failed with status: {}",
                status
            )));
        }

        let body: serde_json::Value = response.json().await.map_err(|e| KeystoneError::ParseError(e.to_string()))?;

        self.parse_token_v3(&body)
    }

    fn parse_token_v3(&self, body: &serde_json::Value) -> Result<KeystoneToken> {
        let token_data = body
            .get("token")
            .ok_or_else(|| KeystoneError::ParseError("Missing token field".to_string()))?;

        let user = token_data
            .get("user")
            .ok_or_else(|| KeystoneError::ParseError("Missing user field".to_string()))?;

        let user_id = user
            .get("id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| KeystoneError::ParseError("Missing user id".to_string()))?
            .to_string();

        let username = user.get("name").and_then(|v| v.as_str()).unwrap_or("unknown").to_string();

        let project = token_data.get("project");
        let (project_id, project_name) = if let Some(proj) = project {
            (
                proj.get("id").and_then(|v| v.as_str()).map(String::from),
                proj.get("name").and_then(|v| v.as_str()).map(String::from),
            )
        } else {
            (None, None)
        };

        let domain = user.get("domain");
        let (domain_id, domain_name) = if let Some(dom) = domain {
            (
                dom.get("id").and_then(|v| v.as_str()).map(String::from),
                dom.get("name").and_then(|v| v.as_str()).map(String::from),
            )
        } else {
            (None, None)
        };

        let roles = token_data
            .get("roles")
            .and_then(|v| v.as_array())
            .map(|roles| {
                roles
                    .iter()
                    .filter_map(|r| r.get("name").and_then(|n| n.as_str()).map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let expires_at = token_data
            .get("expires_at")
            .and_then(|v| v.as_str())
            .and_then(|s| OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339).ok())
            .ok_or_else(|| KeystoneError::ParseError("Invalid expires_at".to_string()))?;

        let issued_at = token_data
            .get("issued_at")
            .and_then(|v| v.as_str())
            .and_then(|s| OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339).ok())
            .unwrap_or_else(OffsetDateTime::now_utc);

        Ok(KeystoneToken {
            token: String::new(),
            user_id,
            username,
            project_id,
            project_name,
            domain_id,
            domain_name,
            roles,
            expires_at,
            issued_at,
        })
    }

    async fn validate_token_v2(&self, _token: &str) -> Result<KeystoneToken> {
        warn!("Keystone v2.0 support is deprecated");
        Err(KeystoneError::UnsupportedVersion)
    }

    /// Validate EC2 credentials
    pub async fn validate_ec2_credentials(
        &self,
        access_key: &str,
        signature: &str,
        string_to_sign: &str,
    ) -> Result<EC2Credential> {
        let url = format!("{}/v3/ec2tokens", self.auth_url);

        debug!("Validating EC2 credentials: access_key={}", access_key);

        let payload = json!({
            "auth": {
                "identity": {
                    "methods": ["ec2"],
                    "ec2": {
                        "access": access_key,
                        "signature": signature,
                        "data": string_to_sign
                    }
                }
            }
        });

        let response = self
            .client
            .post(&url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| KeystoneError::HttpError(e.to_string()))?;

        if !response.status().is_success() {
            return Err(KeystoneError::InvalidCredentials);
        }

        let _body: serde_json::Value = response.json().await.map_err(|e| KeystoneError::ParseError(e.to_string()))?;

        // Parse access key to extract user_id and project_id
        let (user_id, project_id) = EC2Credential::parse_access_key(access_key).unwrap_or((access_key.to_string(), None));

        Ok(EC2Credential {
            access: access_key.to_string(),
            secret: String::new(), // Secret not returned in validation
            user_id,
            project_id,
            trust_id: None,
        })
    }

    /// Get EC2 credentials for a user
    pub async fn get_ec2_credentials(&self, user_id: &str, project_id: Option<&str>) -> Result<Vec<EC2Credential>> {
        let admin_token = self.get_admin_token().await?;

        let url = if let Some(proj_id) = project_id {
            format!("{}/v3/users/{}/credentials/OS-EC2?project_id={}", self.auth_url, user_id, proj_id)
        } else {
            format!("{}/v3/users/{}/credentials/OS-EC2", self.auth_url, user_id)
        };

        debug!("Fetching EC2 credentials for user: {}", user_id);

        let response = self
            .client
            .get(&url)
            .header("X-Auth-Token", admin_token)
            .send()
            .await
            .map_err(|e| KeystoneError::HttpError(e.to_string()))?;

        if !response.status().is_success() {
            return Ok(vec![]);
        }

        let body: serde_json::Value = response.json().await.map_err(|e| KeystoneError::ParseError(e.to_string()))?;

        let credentials = body
            .get("credentials")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|cred| self.parse_ec2_credential(cred).ok()).collect())
            .unwrap_or_default();

        Ok(credentials)
    }

    fn parse_ec2_credential(&self, cred: &serde_json::Value) -> Result<EC2Credential> {
        let access = cred
            .get("access")
            .and_then(|v| v.as_str())
            .ok_or_else(|| KeystoneError::ParseError("Missing access key".to_string()))?
            .to_string();

        let secret = cred
            .get("secret")
            .and_then(|v| v.as_str())
            .ok_or_else(|| KeystoneError::ParseError("Missing secret key".to_string()))?
            .to_string();

        let user_id = cred
            .get("user_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| KeystoneError::ParseError("Missing user_id".to_string()))?
            .to_string();

        let project_id = cred.get("project_id").and_then(|v| v.as_str()).map(String::from);

        let trust_id = cred.get("trust_id").and_then(|v| v.as_str()).map(String::from);

        Ok(EC2Credential {
            access,
            secret,
            user_id,
            project_id,
            trust_id,
        })
    }

    /// Get admin token for privileged operations
    async fn get_admin_token(&self) -> Result<String> {
        // Check if we have a valid cached token
        {
            let guard = self.admin_token.read().await;
            if let Some(token) = guard.as_ref()
                && !token.is_expired()
            {
                return Ok(token.token.clone());
            }
        }

        // Need to authenticate as admin
        let admin_user = self
            .admin_user
            .as_ref()
            .ok_or_else(|| KeystoneError::ConfigError("Missing admin user".to_string()))?;
        let admin_password = self
            .admin_password
            .as_ref()
            .ok_or_else(|| KeystoneError::ConfigError("Missing admin password".to_string()))?;

        let url = format!("{}/v3/auth/tokens", self.auth_url);

        debug!("Authenticating as admin user: {}", admin_user);

        let mut auth_payload = json!({
            "auth": {
                "identity": {
                    "methods": ["password"],
                    "password": {
                        "user": {
                            "name": admin_user,
                            "password": admin_password,
                            "domain": {"name": self.admin_domain}
                        }
                    }
                }
            }
        });

        if let Some(proj) = &self.admin_project {
            auth_payload["auth"]["scope"] = json!({
                "project": {
                    "name": proj,
                    "domain": {"name": self.admin_domain}
                }
            });
        }

        let response = self
            .client
            .post(&url)
            .json(&auth_payload)
            .send()
            .await
            .map_err(|e| KeystoneError::HttpError(e.to_string()))?;

        if !response.status().is_success() {
            return Err(KeystoneError::AuthenticationFailed("Admin authentication failed".to_string()));
        }

        let token = response
            .headers()
            .get("X-Subject-Token")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| KeystoneError::ParseError("Missing X-Subject-Token header".to_string()))?
            .to_string();

        // Parse expiration from response body
        let body: serde_json::Value = response.json().await.map_err(|e| KeystoneError::ParseError(e.to_string()))?;

        let expires_at = body
            .get("token")
            .and_then(|t| t.get("expires_at"))
            .and_then(|v| v.as_str())
            .and_then(|s| OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339).ok())
            .unwrap_or_else(|| OffsetDateTime::now_utc() + time::Duration::hours(1));

        // Cache the token
        let mut guard = self.admin_token.write().await;
        *guard = Some(AdminToken {
            token: token.clone(),
            expires_at,
        });

        info!("Admin token obtained successfully");
        Ok(token)
    }

    /// Clear cached admin token
    pub async fn clear_admin_token(&self) {
        let mut guard = self.admin_token.write().await;
        *guard = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_creation() {
        let client = KeystoneClient::new(
            "http://keystone:5000".to_string(),
            KeystoneVersion::V3,
            Some("admin".to_string()),
            Some("secret".to_string()),
            Some("admin".to_string()),
            true,
        );

        assert_eq!(client.auth_url, "http://keystone:5000");
        assert_eq!(client.version, KeystoneVersion::V3);
    }
}
