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

//! Proxy validation error types

use std::net::AddrParseError;

/// 代理验证错误类型
#[derive(Debug, thiserror::Error)]
pub enum ProxyError {
    /// 无效的 X-Forwarded-For 头部
    #[error("Invalid X-Forwarded-For header: {0}")]
    InvalidXForwardedFor(String),

    /// 无效的 Forwarded 头部（RFC 7239）
    #[error("Invalid Forwarded header (RFC 7239): {0}")]
    InvalidForwardedHeader(String),

    /// 代理链验证失败
    #[error("Proxy chain validation failed: {0}")]
    ChainValidationFailed(String),

    /// 代理链过长
    #[error("Proxy chain too long: {0} hops (max: {1})")]
    ChainTooLong(usize, usize),

    /// 来自不可信代理
    #[error("Request from untrusted proxy: {0}")]
    UntrustedProxy(String),

    /// 代理链不连续
    #[error("Proxy chain is not continuous")]
    ChainNotContinuous,

    /// IP 地址解析失败
    #[error("Failed to parse IP address: {0}")]
    IpParseError(String),

    /// 头部解析失败
    #[error("Failed to parse header: {0}")]
    HeaderParseError(String),

    /// 验证超时
    #[error("Validation timeout")]
    Timeout,

    /// 内部验证错误
    #[error("Internal validation error: {0}")]
    Internal(String),
}

impl From<AddrParseError> for ProxyError {
    fn from(err: AddrParseError) -> Self {
        Self::IpParseError(err.to_string())
    }
}

impl ProxyError {
    /// 创建无效 X-Forwarded-For 头部错误
    pub fn invalid_xff(msg: impl Into<String>) -> Self {
        Self::InvalidXForwardedFor(msg.into())
    }

    /// 创建无效 Forwarded 头部错误
    pub fn invalid_forwarded(msg: impl Into<String>) -> Self {
        Self::InvalidForwardedHeader(msg.into())
    }

    /// 创建代理链验证失败错误
    pub fn chain_failed(msg: impl Into<String>) -> Self {
        Self::ChainValidationFailed(msg.into())
    }

    /// 创建来自不可信代理错误
    pub fn untrusted(proxy: impl Into<String>) -> Self {
        Self::UntrustedProxy(proxy.into())
    }

    /// 创建内部验证错误
    pub fn internal(msg: impl Into<String>) -> Self {
        Self::Internal(msg.into())
    }

    /// 判断错误是否可恢复（是否应该继续处理请求）
    pub fn is_recoverable(&self) -> bool {
        match self {
            // 这些错误通常意味着我们应该拒绝请求或使用备用 IP
            Self::UntrustedProxy(_) => true,
            Self::ChainTooLong(_, _) => true,
            Self::ChainNotContinuous => true,

            // 这些错误可能意味着配置问题或恶意请求
            Self::InvalidXForwardedFor(_) => false,
            Self::InvalidForwardedHeader(_) => false,
            Self::ChainValidationFailed(_) => false,
            Self::IpParseError(_) => false,
            Self::HeaderParseError(_) => false,
            Self::Timeout => true,
            Self::Internal(_) => false,
        }
    }
}
