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

//! Session context for RustFS
//!
//! This module provides session context management functionality,
//! integrating with the existing auth and storage modules.

use std::sync::Arc;
use crate::auth::IAMAuth;
use crate::storage::ecfs::FS;

/// Session context for protocol adapters
#[derive(Debug, Clone)]
pub struct SessionContext {
    /// Authentication information
    pub auth: Arc<IAMAuth>,
    /// Storage filesystem access
    pub fs: Arc<FS>,
    /// Session identifier
    pub session_id: String,
}

impl SessionContext {
    /// Create a new session context
    pub fn new(auth: Arc<IAMAuth>, fs: Arc<FS>, session_id: String) -> Self {
        Self {
            auth,
            fs,
            session_id,
        }
    }

    /// Get authentication information
    pub fn auth(&self) -> &Arc<IAMAuth> {
        &self.auth
    }

    /// Get filesystem access
    pub fn fs(&self) -> &Arc<FS> {
        &self.fs
    }

    /// Get session identifier
    pub fn session_id(&self) -> &str {
        &self.session_id
    }
}