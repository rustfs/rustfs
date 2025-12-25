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

//! Session context for protocol implementations
//!
//! This module provides session context for all protocol implementations.
//! It ensures that all protocols use the same session context and security boundaries.
//!
//! MINIO CONSTRAINT: Session context MUST be protocol-agnostic and
//! MUST NOT provide capabilities beyond what external S3 clients can do.

use crate::protocols::session::principal::ProtocolPrincipal;
use std::net::IpAddr;

/// Protocol types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Protocol {
    Ftp,
    Ftps,
    Sftp,
}

/// Session context for protocol operations
///
/// MINIO CONSTRAINT: This context MUST contain all necessary information
/// for authorization and auditing, but MUST NOT make security decisions
#[derive(Debug, Clone)]
pub struct SessionContext {
    /// The protocol principal (authenticated user)
    pub principal: ProtocolPrincipal,

    /// The protocol type
    pub protocol: Protocol,

    /// The source IP address
    pub source_ip: IpAddr,

    /// The current working directory (if applicable)
    pub working_dir: Option<String>,
}

impl SessionContext {
    /// Create a new session context
    ///
    /// MINIO CONSTRAINT: Must use the same authentication path as external clients
    pub fn new(principal: ProtocolPrincipal, protocol: Protocol, source_ip: IpAddr) -> Self {
        Self {
            principal,
            protocol,
            source_ip,
            working_dir: None,
        }
    }

    /// Set the working directory
    pub fn set_working_dir(&mut self, dir: String) {
        self.working_dir = Some(dir);
    }

    /// Get the current working directory
    pub fn working_dir(&self) -> Option<&str> {
        self.working_dir.as_deref()
    }

    /// Get the access key for this session
    pub fn access_key(&self) -> &str {
        self.principal.access_key()
    }

    /// Resolve a path relative to the working directory
    pub fn resolve_path(&self, path: &str) -> String {
        if path.starts_with('/') {
            // Absolute path
            path.to_string()
        } else {
            // Relative path
            match self.working_dir() {
                Some(cwd) => {
                    if cwd.ends_with('/') {
                        format!("{}{}", cwd, path)
                    } else {
                        format!("{}/{}", cwd, path)
                    }
                }
                None => path.to_string(),
            }
        }
    }
}