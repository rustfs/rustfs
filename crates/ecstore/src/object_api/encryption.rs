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

use async_trait::async_trait;
use http::{HeaderMap, HeaderValue};
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadEncryptionMode {
    Direct { base_nonce: [u8; 12] },
    Object,
}

pub struct ReadEncryptionMaterial {
    pub key_bytes: [u8; 32],
    pub mode: ReadEncryptionMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncryptionResolutionErrorKind {
    InvalidRequest,
    InvalidMetadata,
    ServiceUnavailable,
    DecryptionFailed,
}

#[derive(Debug)]
pub struct EncryptionResolutionError {
    kind: EncryptionResolutionErrorKind,
    message: String,
}

impl EncryptionResolutionError {
    pub fn new(kind: EncryptionResolutionErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn kind(&self) -> EncryptionResolutionErrorKind {
        self.kind
    }
}

impl Display for EncryptionResolutionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for EncryptionResolutionError {}

pub struct ReadEncryptionRequest<'a> {
    pub bucket: &'a str,
    pub object: &'a str,
    pub metadata: &'a HashMap<String, String>,
    pub headers: &'a HeaderMap<HeaderValue>,
}

#[async_trait]
pub trait ObjectEncryptionResolver: Send + Sync {
    async fn resolve_read_material(
        &self,
        request: ReadEncryptionRequest<'_>,
    ) -> Result<Option<ReadEncryptionMaterial>, EncryptionResolutionError>;
}
