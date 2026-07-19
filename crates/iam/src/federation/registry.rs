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

use super::FederatedIdentityProvider;
use std::sync::Arc;

/// Immutable registry of built-in federation adapters.
pub struct FederatedIdentityRegistry {
    standard_oidc: Arc<dyn FederatedIdentityProvider>,
}

impl FederatedIdentityRegistry {
    pub fn new(standard_oidc: Arc<dyn FederatedIdentityProvider>) -> Self {
        Self { standard_oidc }
    }

    pub(crate) fn standard_oidc(&self) -> &dyn FederatedIdentityProvider {
        self.standard_oidc.as_ref()
    }
}
