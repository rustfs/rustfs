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

//! RustFS implementations of the `rustfs-gateway` extension traits.
//!
//! This is the gateway's ring 2: it may depend on RustFS business crates, and nothing in the
//! gateway depends on it. Each module adapts one extension point to the RustFS subsystem that
//! owns the answer.

pub mod credentials;
pub mod policy;
pub mod root;

use std::sync::Arc;

pub use credentials::{IamCredentials, IamKeyLookup, STS_SESSION_ISSUER};
pub use policy::{IamPolicySource, IamPrincipalLookup, PrincipalKind, RustfsPrincipal};
pub use root::{MIN_SECRET_KEY_LEN, RootCell, RootCredentials, RootCredentialsError, RootSource};

/// The IAM-backed credential provider and policy source, assembled together.
///
/// This is the only way to build either one, so the two always read the same IAM and the same
/// live [`RootSource`]: a key cannot be root to one and an ordinary user to the other.
pub struct IamAdapters {
    /// The gateway `CredentialProvider`.
    pub credentials: IamCredentials,
    /// The gateway `PolicySource`.
    pub policy: IamPolicySource,
}

impl IamAdapters {
    /// Assembles both adapters over one IAM handle and one root source. The binary supplies a
    /// source that follows its live root credentials, such as a [`RootCell`] it updates on every
    /// root change.
    #[must_use]
    pub fn new(iam: Arc<dyn IamPrincipalLookup>, root: Arc<dyn RootSource>) -> Self {
        Self {
            credentials: IamCredentials::new(iam.clone(), root.clone()),
            policy: IamPolicySource::new(iam, root),
        }
    }
}
