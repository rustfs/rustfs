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

//! Configuration error types

use std::net::AddrParseError;

/// 配置错误类型
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// 环境变量缺失
    #[error("Missing environment variable: {0}")]
    MissingEnvVar(String),

    /// 环境变量解析失败
    #[error("Failed to parse environment variable {0}: {1}")]
    EnvParseError(String, String),

    /// 无效的配置值
    #[error("Invalid configuration value for {0}: {1}")]
    InvalidValue(String, String),

    /// 无效的 IP 地址或网络
    #[error("Invalid IP address or network: {0}")]
    InvalidIp(String),

    /// 配置验证失败
    #[error("Configuration validation failed: {0}")]
    ValidationFailed(String),

    /// 配置冲突
    #[error("Configuration conflict: {0}")]
    Conflict(String),

    /// 配置文件错误
    #[error("Config file error: {0}")]
    FileError(String),

    /// 无效的配置
    #[error("Invalid config: {0}")]
    InvalidConfig(String),
}

impl From<AddrParseError> for ConfigError {
    fn from(err: AddrParseError) -> Self {
        Self::InvalidIp(err.to_string())
    }
}

impl From<ipnetwork::IpNetworkError> for ConfigError {
    fn from(err: ipnetwork::IpNetworkError) -> Self {
        Self::InvalidIp(err.to_string())
    }
}

impl ConfigError {
    /// 创建环境变量缺失错误
    pub fn missing_env_var(key: &str) -> Self {
        Self::MissingEnvVar(key.to_string())
    }

    /// 创建环境变量解析错误
    pub fn env_parse(key: &str, value: &str) -> Self {
        Self::EnvParseError(key.to_string(), value.to_string())
    }

    /// 创建无效配置值错误
    pub fn invalid_value(field: &str, value: &str) -> Self {
        Self::InvalidValue(field.to_string(), value.to_string())
    }
}
