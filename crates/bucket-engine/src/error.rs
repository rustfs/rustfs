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

use crate::name::{EngineName, OperationName};
use thiserror::Error;

pub type EngineResult<T> = Result<T, EngineError>;

/// Errors an engine or the binding contract reports. The dispatch layer maps
/// them to S3 errors; `Unavailable` and `CacheFull` are retryable or skippable
/// by clients, the rest are not.
#[derive(Debug, Error)]
pub enum EngineError {
    #[error("engine {engine} does not serve {operation}")]
    Unsupported { engine: EngineName, operation: String },
    #[error("engine {engine} has no custom operation {operation}")]
    UnknownOperation { engine: EngineName, operation: OperationName },
    #[error("invalid bucket engine binding: {0}")]
    InvalidBinding(String),
    #[error("invalid engine config: {0}")]
    InvalidConfig(String),
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("not found")]
    NotFound,
    #[error("bucket still holds data")]
    BucketNotEmpty,
    #[error("cache full")]
    CacheFull,
    #[error("stale placement view {client}, current is {current}")]
    StalePlacement { client: u64, current: u64 },
    #[error("temporarily unavailable: {0}")]
    Unavailable(String),
    #[error("engine I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("internal engine error: {0}")]
    Internal(String),
}

impl EngineError {
    /// Whether a client may retry the same request.
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Unavailable(_) | Self::StalePlacement { .. })
    }

    /// Stable name of the error kind, carried across nodes with
    /// [`EngineError::wire_message`] and rebuilt by [`EngineError::from_wire`].
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Unsupported { .. } => "unsupported",
            Self::UnknownOperation { .. } => "unknown-operation",
            Self::InvalidBinding(_) => "invalid-binding",
            Self::InvalidConfig(_) => "invalid-config",
            Self::InvalidRequest(_) => "invalid-request",
            Self::NotFound => "not-found",
            Self::BucketNotEmpty => "bucket-not-empty",
            Self::CacheFull => "cache-full",
            Self::StalePlacement { .. } => "stale-placement",
            Self::Unavailable(_) => "unavailable",
            Self::Io(_) => "io",
            Self::Internal(_) => "internal",
        }
    }

    /// The payload sent with [`EngineError::kind`]: the inner message, or for
    /// a stale placement the two view versions.
    pub fn wire_message(&self) -> String {
        match self {
            Self::InvalidBinding(message)
            | Self::InvalidConfig(message)
            | Self::InvalidRequest(message)
            | Self::Unavailable(message)
            | Self::Internal(message) => message.clone(),
            Self::StalePlacement { client, current } => format!("{client} {current}"),
            other => other.to_string(),
        }
    }

    /// Rebuilds an error a peer reported. Kinds whose fields do not cross the
    /// wire (unsupported and unknown operations) come back as invalid
    /// requests; unknown kinds from newer peers come back as internal errors.
    pub fn from_wire(kind: &str, message: String) -> Self {
        match kind {
            "unsupported" | "unknown-operation" | "invalid-request" => Self::InvalidRequest(message),
            "invalid-binding" => Self::InvalidBinding(message),
            "invalid-config" => Self::InvalidConfig(message),
            "not-found" => Self::NotFound,
            "bucket-not-empty" => Self::BucketNotEmpty,
            "cache-full" => Self::CacheFull,
            "stale-placement" => {
                let mut versions = message.split(' ').map(str::parse::<u64>);
                match (versions.next(), versions.next()) {
                    (Some(Ok(client)), Some(Ok(current))) => Self::StalePlacement { client, current },
                    _ => Self::Internal(format!("malformed stale placement report: {message}")),
                }
            }
            "unavailable" => Self::Unavailable(message),
            "io" => Self::Io(std::io::Error::other(message)),
            "internal" => Self::Internal(message),
            _ => Self::Internal(format!("{kind}: {message}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn errors_round_trip_through_the_wire_form() {
        let engine = EngineName::new("kv-cache").expect("valid engine name");
        let cases = [
            EngineError::InvalidConfig("bad".into()),
            EngineError::InvalidRequest("bad".into()),
            EngineError::InvalidBinding("bad".into()),
            EngineError::NotFound,
            EngineError::BucketNotEmpty,
            EngineError::CacheFull,
            EngineError::StalePlacement { client: 3, current: 5 },
            EngineError::Unavailable("busy".into()),
            EngineError::Internal("boom".into()),
        ];
        for error in cases {
            let rebuilt = EngineError::from_wire(error.kind(), error.wire_message());
            assert_eq!(rebuilt.kind(), error.kind());
            assert_eq!(rebuilt.to_string(), error.to_string());
        }

        let io = EngineError::from_wire("io", "disk gone".into());
        assert_eq!(io.kind(), "io");
        assert!(io.to_string().contains("disk gone"));

        let unsupported = EngineError::Unsupported {
            engine,
            operation: "CopyObject".into(),
        };
        let rebuilt = EngineError::from_wire(unsupported.kind(), unsupported.wire_message());
        assert_eq!(rebuilt.kind(), "invalid-request");
        assert!(rebuilt.to_string().contains("CopyObject"));

        assert_eq!(EngineError::from_wire("stale-placement", "x".into()).kind(), "internal");
        assert_eq!(EngineError::from_wire("from-the-future", "m".into()).kind(), "internal");
    }
}
