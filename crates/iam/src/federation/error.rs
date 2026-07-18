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

pub type Result<T> = core::result::Result<T, FederationError>;

#[derive(Debug, thiserror::Error)]
pub enum FederatedSessionBindingError {
    #[error("{0}")]
    InvalidRequest(String),
    #[error("{0}")]
    Internal(String),
}

#[derive(Debug, thiserror::Error)]
pub enum FederationError {
    #[error("{0}")]
    Authorization(String),
    #[error("{0}")]
    CodeExchange(String),
    #[error("{0}")]
    TokenVerification(String),
    #[error("{0}")]
    Logout(String),
    #[error("no policies are available for this OIDC token")]
    NoAuthorizationContext,
    #[error(transparent)]
    Binding(#[from] FederatedSessionBindingError),
}
