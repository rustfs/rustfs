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

//! Non-GET native admin routes served ahead of the host's control-plane catch-all.
//!
//! GET admin routes stay on [`super::ControlPlane`] until `GatewayStack` carries a dedicated
//! Object Lambda fallback, preserving presigned `?lambdaArn=...` requests (#1577).

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
    fn non_get_native_admin_routes_precede_the_control_plane_and_get_routes_stay_deferred() {
        let dialect = super::super::dialect().expect("control-plane dialect");
        let fallback = dialect
            .claimed_operations()
            .iter()
            .find(|op| op.name() == NAME)
            .expect("control-plane catch-all");
        let added: Vec<_> = native::ROUTES
            .iter()
            .filter(|record| !ROUTES.iter().any(|old| old.operation == record.operation))
            .collect();
        let deferred_reads: Vec<_> = added.iter().copied().filter(|record| record.method == "GET").collect();
        let mounted_writes: Vec<_> = added.iter().copied().filter(|record| record.method != "GET").collect();

        assert_eq!(deferred_reads.len(), 5);
        for record in deferred_reads {
            assert!(!contains(record.operation), "{}", record.operation);
            assert!(
                !dialect.claimed_operations().iter().any(|op| op.name() == record.operation),
                "{}",
                record.operation
            );
        }

        assert_eq!(mounted_writes.len(), 7);
        assert_eq!(ROWS.len(), mounted_writes.len());
        for record in mounted_writes {
            assert!(contains(record.operation), "{}", record.operation);
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
