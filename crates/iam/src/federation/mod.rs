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

mod binding;
mod error;
mod model;
pub mod oidc;
mod provider;
mod registry;
mod transaction;

pub use binding::FederatedSessionBinding;
pub use error::{FederatedSessionBindingError, FederationError, Result};
pub use model::{
    FederatedAuthorization, FederatedClaims, FederatedCodeExchange, FederatedLoginSession, FederatedSession,
    FederatedSessionTransaction, OIDC_VIRTUAL_PARENT_CLAIM,
};
pub use provider::FederatedIdentityProvider;
pub use registry::FederatedIdentityRegistry;
pub use transaction::FederatedIdentityService;
