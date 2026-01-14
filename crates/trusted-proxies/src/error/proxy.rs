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

//! Proxy validation error types for the trusted proxy system.

use std::net::AddrParseError;

/// Errors that can occur during proxy chain validation.
#[derive(Debug, thiserror::Error)]
pub enum ProxyError {
    /// The X-Forwarded-For header is malformed or contains invalid data.
    #[error("Invalid X-Forwarded-For header: {0}")]
    InvalidXForwardedFor(String),

    /// The RFC 7239 Forwarded header is malformed.
    #[error("Invalid Forwarded header (RFC 7239): {0}")]
    InvalidForwardedHeader(String),

    /// General failure during proxy chain validation.
    #[error("Proxy chain validation failed: {0}")]
    ChainValidationFailed(String),

    /// The number of proxy hops exceeds the configured limit.
    #[error("Proxy chain too long: {0} hops (max: {1})")]
    ChainTooLong(usize, usize),

    /// The request originated from a proxy that is not in the trusted list.
    #[error("Request from untrusted proxy: {0}")]
    UntrustedProxy(String),

    /// The proxy chain is not continuous (e.g., an untrusted IP is between trusted ones).
    #[error("Proxy chain is not continuous")]
    ChainNotContinuous,

    /// An IP address in the chain could not be parsed.
    #[error("Failed to parse IP address: {0}")]
    IpParseError(String),

    /// A header value could not be parsed as a string.
    #[error("Failed to parse header: {0}")]
    HeaderParseError(String),

    /// Validation took too long and timed out.
    #[error("Validation timeout")]
    Timeout,

    /// An unexpected internal error occurred during validation.
    #[error("Internal validation error: {0}")]
    Internal(String),
}

impl From<AddrParseError> for ProxyError {
    fn from(err: AddrParseError) -> Self {
        Self::IpParseError(err.to_string())
    }
}

impl ProxyError {
    /// Creates an `InvalidXForwardedFor` error.
    pub fn invalid_xff(msg: impl Into<String>) -> Self {
        Self::InvalidXForwardedFor(msg.into())
    }

    /// Creates an `InvalidForwardedHeader` error.
    pub fn invalid_forwarded(msg: impl Into<String>) -> Self {
        Self::InvalidForwardedHeader(msg.into())
    }

    /// Creates a `ChainValidationFailed` error.
    pub fn chain_failed(msg: impl Into<String>) -> Self {
        Self::ChainValidationFailed(msg.into())
    }

    /// Creates an `UntrustedProxy` error.
    pub fn untrusted(proxy: impl Into<String>) -> Self {
        Self::UntrustedProxy(proxy.into())
    }

    /// Creates an `Internal` validation error.
    pub fn internal(msg: impl Into<String>) -> Self {
        Self::Internal(msg.into())
    }

    /// Determines if the error is recoverable, meaning the request can still be processed
    /// (perhaps by falling back to the direct peer IP).
    pub fn is_recoverable(&self) -> bool {
        match self {
            // These errors typically mean we should use the direct peer IP as a fallback.
            Self::UntrustedProxy(_) => true,
            Self::ChainTooLong(_, _) => true,
            Self::ChainNotContinuous => true,

            // These errors suggest malformed requests or severe configuration issues.
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
