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

pub mod authz_audit;
pub mod bucket_owner;
pub mod credentials;
pub mod host;
pub mod keystone;
pub mod policy;
pub mod root;
pub mod trace;

use std::sync::Arc;

use rustfs_gateway::{Authenticator, CredentialProvider, ServiceBuilder};

pub use authz_audit::{DenialAdmission, DenialAuditSink, GatewayDenialRecord, RustfsAuthzAudit};
pub use bucket_owner::{BucketLookup, OwnerLookupError, RustfsBucketOwners};
pub use credentials::{IamCredentials, IamKeyLookup, STS_SESSION_ISSUER};
pub use host::rustfs_host_resolver;
pub use keystone::{
    KeystoneCaller, KeystoneCallerRefused, KeystoneEstablished, KeystoneNotEstablished, KeystoneVerifier,
    establish_keystone_identity, keystone_header_token_claim,
};
pub use policy::{IamPolicySource, IamPrincipalLookup, PrincipalKind, RustfsPrincipal};
pub use root::{MIN_SECRET_KEY_LEN, RootCredentials, RootCredentialsError, RootSource};
pub use trace::{AttachOutcome, RequestIdCarrier, attach_host_request_id, host_request_id};

/// The IAM-backed credential provider and policy source, assembled together.
///
/// Both halves read the same IAM handle and the same live [`RootSource`], so a key cannot be root
/// to one and an ordinary user to the other. Neither half can be taken out on its own: the fields
/// are private, and [`IamAdapters::install`] consumes the pair and hands both to one builder.
///
/// Mixing the halves of two assemblies does not compile:
///
/// ```compile_fail
/// fn mix(a: rustfs_gateway_adapter::IamAdapters, b: rustfs_gateway_adapter::IamAdapters) {
///     let _mixed = (a.credentials, b.policy);
/// }
/// ```
///
/// Nor does building a half outside the pair:
///
/// ```compile_fail
/// use std::sync::Arc;
/// use rustfs_gateway_adapter::{IamPolicySource, IamPrincipalLookup, RootSource};
/// fn build(iam: Arc<dyn IamPrincipalLookup>, root: Arc<dyn RootSource>) {
///     let _alone = IamPolicySource::new(iam, root);
/// }
/// ```
///
/// Installing the pair is the one supported path:
///
/// ```no_run
/// use std::sync::Arc;
/// use rustfs_gateway::{RegionSet, ServiceBuilder, SigV4Authenticator};
/// use rustfs_gateway_adapter::{IamAdapters, IamPrincipalLookup, RootSource};
/// fn assemble(iam: Arc<dyn IamPrincipalLookup>, root: Arc<dyn RootSource>, regions: RegionSet) -> ServiceBuilder {
///     IamAdapters::new(iam, root).install(ServiceBuilder::new(), |provider| SigV4Authenticator::new(provider, regions))
/// }
/// ```
pub struct IamAdapters {
    credentials: IamCredentials,
    policy: IamPolicySource,
}

impl IamAdapters {
    /// Assembles both adapters over one IAM handle and one root source. The binary passes a
    /// source that reads its live root credentials on every call (see [`RootSource`]).
    #[must_use]
    pub fn new(iam: Arc<dyn IamPrincipalLookup>, root: Arc<dyn RootSource>) -> Self {
        Self {
            credentials: IamCredentials::new(iam.clone(), root.clone()),
            policy: IamPolicySource::new(iam, root),
        }
    }

    /// Installs both halves into `builder`: the credential provider through the authenticator
    /// `authenticator` builds over it (for example `SigV4Authenticator::new(provider, regions)`
    /// with the RustFS profile applied), and the policy source.
    ///
    /// Consumes the pair and gives both halves to this one builder. The `authenticator` closure
    /// receives the provider handle and may keep a clone of it; nothing here prevents that, so
    /// the closure must not hand it to a second builder.
    #[must_use]
    pub fn install<A: Authenticator>(
        self,
        builder: ServiceBuilder,
        authenticator: impl FnOnce(Arc<dyn CredentialProvider>) -> A,
    ) -> ServiceBuilder {
        let authenticator = authenticator(Arc::new(self.credentials));
        builder.authenticator(authenticator).policy_source(self.policy)
    }
}
