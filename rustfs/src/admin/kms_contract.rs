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

//! Reviewable snapshot of the KMS admin route surface.
//!
//! `route_registration_test` already fails when a route appears or disappears,
//! and `route_policy` pins each route's action and risk with individual
//! assertions. Neither records the surface as a whole, so a change to an
//! existing route's action or risk lands as an edited assertion rather than as
//! a visible before/after — and the KMS routes are exactly where that matters,
//! since the action decides who may reach key material and the risk level
//! decides which confirmations the route demands.
//!
//! Every field here is derived from [`ADMIN_ROUTE_POLICY_SPECS`], so this file
//! cannot drift from the routing table: there is nothing to keep in sync by
//! hand. Per-key authorization scoping is deliberately *not* restated here —
//! it is enforced and tested where it is implemented, by
//! `single_key_endpoints_reject_a_key_outside_the_policy_scope` in
//! `handlers::kms_keys`, and a second hand-maintained list would be a claim
//! nothing checks.

use serde::Serialize;

use crate::admin::route_policy::ADMIN_ROUTE_POLICY_SPECS;

/// Path fragment identifying a KMS admin route.
const KMS_PATH_MARKER: &str = "/kms/";

/// One KMS admin route as the authorization layer sees it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
struct KmsRouteContract {
    method: String,
    path: String,
    /// The action a caller's policy must allow, or `null` for a public route.
    action: Option<String>,
    risk: String,
}

/// Every KMS admin route, in a stable order.
fn kms_route_contract() -> Vec<KmsRouteContract> {
    let mut routes: Vec<KmsRouteContract> = ADMIN_ROUTE_POLICY_SPECS
        .iter()
        .filter(|spec| spec.path().contains(KMS_PATH_MARKER))
        .map(|spec| KmsRouteContract {
            method: format!("{:?}", spec.method()).to_uppercase(),
            path: spec.path().to_string(),
            action: spec.access().admin_action().map(|action| action.as_str().to_string()),
            risk: format!("{:?}", spec.risk_level()),
        })
        .collect();
    routes.sort();
    routes
}

#[cfg(test)]
mod tests {
    use super::{KMS_PATH_MARKER, kms_route_contract};

    /// The reviewable record. A route whose action or risk changes shows up as
    /// a snapshot diff instead of as an edited assertion buried in
    /// `route_policy`.
    #[test]
    fn kms_admin_route_contract_is_stable() {
        insta::assert_json_snapshot!("kms_admin_route_contract", kms_route_contract());
    }

    /// The snapshot is only evidence if it actually covers the surface. A KMS
    /// route registered outside the policy table, or a path spelling the filter
    /// does not match, would otherwise leave the snapshot green and the route
    /// unrecorded.
    #[test]
    fn every_kms_route_is_covered_and_authorized() {
        let routes = kms_route_contract();
        assert!(routes.len() > 20, "expected the full KMS surface, got {}", routes.len());

        for route in &routes {
            assert!(route.path.contains(KMS_PATH_MARKER), "unexpected path in the KMS contract: {route:?}");
            let action = route
                .action
                .as_deref()
                .unwrap_or_else(|| panic!("no KMS route may be public: {route:?}"));
            assert!(
                action.starts_with("kms:"),
                "a KMS route must gate on a dedicated kms:* action, not on {action}: {route:?}"
            );
        }
    }
}
