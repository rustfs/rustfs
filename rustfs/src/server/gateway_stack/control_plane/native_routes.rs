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

//! Native admin routes served ahead of the host's control-plane catch-all.
//!
//! All 12 native control-plane admin routes are mounted here; Object Lambda requests route
//! through `GatewayStack::object_lambda_fallback`, preserving header-signed and presigned
//! `?lambdaArn=...` requests (#1577).

use super::{Host, NAME, ROUTER};
use rustfs_gateway::ServiceBuilder;
use rustfs_gateway_core::dialect::{ClaimedRoute, DialectBuilder, OverlayRow};
use rustfs_gateway_core::route::ShadowingDecl;
use rustfs_gateway_dialect_rustfs_admin::{AdminOperation, native::ops};
use std::sync::Arc;

const fn shadows_control_plane(winner: &'static str) -> ShadowingDecl {
    ShadowingDecl {
        winner,
        shadowed: NAME,
        reason: "The registered admin handler precedes the control-plane catch-all.",
        evidence: &[ROUTER],
    }
}

macro_rules! native_routes {
    ($($module:ident::$operation:ident),+ $(,)?) => {
        const ROWS: &[OverlayRow] = &[$(ops::$module::OVERLAY_ROW),+];

        pub(super) fn declare(builder: DialectBuilder) -> DialectBuilder {
            $(
                let builder = builder.declare_claimed::<ops::$module::$operation>(ClaimedRoute {
                    precedence: ops::$module::$operation::PRECEDENCE,
                    rows: ops::$module::$operation::rows(),
                    shadows: const { &[shadows_control_plane(ops::$module::NAME)] },
                    bucket_param: ops::$module::$operation::BUCKET,
                });
            )+
            builder
        }

        pub(super) fn register(builder: ServiceBuilder, host: &Arc<Host>) -> ServiceBuilder {
            builder$(.register::<ops::$module::$operation, _>(Arc::clone(host)))+
        }
    };
}

native_routes!(
    delete_v3_bucket_etag_by_bucket::DeleteV3BucketEtagByBucket,
    delete_v3_target_by_target_type_by_target_name::DeleteV3TargetByTargetTypeByTargetName,
    get_v3_bucket_etag_by_bucket::GetV3BucketEtagByBucket,
    get_v3_site_replication_remove_v2_capabilities::GetV3SiteReplicationRemoveV2Capabilities,
    get_v4_heal_mrf_cluster::GetV4HealMrfCluster,
    get_v4_heal_mrf_cluster_responsibilities::GetV4HealMrfClusterResponsibilities,
    get_v4_heal_mrf_cluster_responsibilities_by_node_id_by_id::GetV4HealMrfClusterResponsibilitiesByNodeIdById,
    post_v4_account_permissions::PostV4AccountPermissions,
    post_v4_heal_mrf_cluster_responsibilities_actions::PostV4HealMrfClusterResponsibilitiesActions,
    put_v3_bucket_etag_by_bucket::PutV3BucketEtagByBucket,
    put_v3_site_replication_peer_remove_v2::PutV3SiteReplicationPeerRemoveV2,
    put_v3_site_replication_remove_v2::PutV3SiteReplicationRemoveV2,
);

pub(super) fn append_rows(rows: &mut Vec<OverlayRow>) {
    rows.extend_from_slice(ROWS);
}

pub(super) fn contains(name: &str) -> bool {
    ROWS.iter().any(|row| row.name == name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_gateway_dialect_rustfs_admin::{ROUTES, native};

    #[test]
    fn all_native_admin_routes_precede_the_control_plane_and_are_absent_from_object_lambda_fallback() {
        let dialect = super::super::dialect().expect("control-plane dialect");
        let object_lambda_dialect = super::super::object_lambda_dialect().expect("object-lambda dialect");
        let fallback = dialect
            .claimed_operations()
            .iter()
            .find(|op| op.name() == NAME)
            .expect("control-plane catch-all");
        let added: Vec<_> = native::ROUTES
            .iter()
            .filter(|record| !ROUTES.iter().any(|old| old.operation == record.operation))
            .collect();
        let deferred: Vec<_> = added.iter().copied().filter(|record| !contains(record.operation)).collect();

        assert_eq!(added.iter().filter(|record| record.method == "GET").count(), 5);
        assert_eq!(added.iter().filter(|record| record.method != "GET").count(), 7);
        assert_eq!(added.len(), 12);
        assert!(deferred.is_empty(), "deferred native routes remain: {deferred:?}");
        assert_eq!(ROWS.len(), added.len());

        for record in added {
            assert!(contains(record.operation), "{}", record.operation);
            assert!(
                !object_lambda_dialect
                    .claimed_operations()
                    .iter()
                    .any(|op| op.name() == record.operation),
                "{} must not be declared on object-lambda fallback dialect",
                record.operation
            );
            let op = dialect
                .claimed_operations()
                .iter()
                .find(|op| op.name() == record.operation)
                .expect("mounted native operation");
            assert!(op.precedence() < fallback.precedence());
            assert!(
                op.shadows()
                    .iter()
                    .any(|shadow| shadow.winner == record.operation && shadow.shadowed == NAME)
            );
            assert_eq!(op.entries().len(), 2);
            for (entry, path) in op.entries().iter().zip([record.path, record.alias.expect("MinIO alias")]) {
                let path = path
                    .split('/')
                    .map(|segment| if segment.starts_with('{') { "example" } else { segment })
                    .collect::<Vec<_>>()
                    .join("/");
                assert!(entry.template().matches(&path), "{path}");
                assert_eq!(entry.bucket_param(), record.bucket);
                assert!(fallback.entries().iter().any(|other| other.template().matches(&path)));
            }
        }
    }
}
