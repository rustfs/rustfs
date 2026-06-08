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

pub mod admin_matrix;
pub mod redaction;
pub mod serde_policy;
pub mod supply_chain;

pub use admin_matrix::{
    AdminActionRef, AdminRouteAccess, AdminRouteMatrixError, AdminRouteSpec, HttpMethod, PublicRouteKind, RouteRiskLevel,
    validate_admin_route_specs,
};
pub use redaction::{RedactionLevel, RedactionPolicyError, RedactionRule, validate_redaction_rules};
pub use serde_policy::{SerdePolicy, SerdePolicyError, SerdePolicyKind, UnknownFieldPolicy, validate_serde_policies};
pub use supply_chain::{
    ArtifactIntegrityPolicy, ArtifactSourceKind, SupplyChainPolicyError, validate_artifact_integrity_policies,
};
