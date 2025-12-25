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

//! Protocol principal for authentication and authorization
//!
//! This module provides the protocol principal that represents an authenticated
//! user in the protocol implementations. It ensures that all protocols use
//! the same principal representation as the S3 API.

use rustfs_policy::auth::UserIdentity;
use std::sync::Arc;

/// Protocol principal representing an authenticated user
///
/// MINIO CONSTRAINT: This principal MUST contain all necessary information
/// for authorization and auditing, but MUST NOT make security decisions
#[derive(Debug, Clone)]
pub struct ProtocolPrincipal {
    /// User identity from IAM system
    pub user_identity: Arc<UserIdentity>,
}

impl ProtocolPrincipal {
    /// Create a new protocol principal
    ///
    /// MINIO CONSTRAINT: Must use the same authentication path as external clients
    pub fn new(user_identity: Arc<UserIdentity>) -> Self {
        Self { user_identity }
    }

    /// Get the access key for this principal
    pub fn access_key(&self) -> &str {
        &self.user_identity.credentials.access_key
    }

    /// Get the secret key for this principal
    pub fn secret_key(&self) -> &str {
        &self.user_identity.credentials.secret_key
    }
}