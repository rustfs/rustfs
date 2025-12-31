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

use rustfs_policy::auth::UserIdentity;
use std::sync::Arc;

/// Protocol principal representing an authenticated user
#[derive(Debug, Clone)]
pub struct ProtocolPrincipal {
    /// User identity from IAM system
    pub user_identity: Arc<UserIdentity>,
}

impl ProtocolPrincipal {
    /// Create a new protocol principal
    pub fn new(user_identity: Arc<UserIdentity>) -> Self {
        Self { user_identity }
    }

    /// Get the access key for this principal
    pub fn access_key(&self) -> &str {
        &self.user_identity.credentials.access_key
    }
}
