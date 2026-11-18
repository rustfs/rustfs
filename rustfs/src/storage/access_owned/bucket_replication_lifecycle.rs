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

//! Prepares external S3 replication edits after IAM authorization.
//!
//! The interface resolves owner privileges and the site contract. The bucket
//! use case receives those facts without consulting site membership itself.

use crate::storage::access::{AuthorizationRequest, req_info_ref};
use crate::storage::storage_api::ecstore_bucket::replication::OperatorRuleContract;
use http::StatusCode;
use rustfs_s3_types::{S3Error, S3ErrorCode, S3Result};
use std::collections::HashSet;
use std::future::Future;

#[cfg(test)]
pub(crate) static SITE_REPLICATION_GATE_TEST_OVERRIDE: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);

#[cfg(test)]
pub(crate) const SITE_REPLICATION_GATE_FORCE_DISABLED: u8 = 1;

#[cfg(test)]
pub(crate) const SITE_REPLICATION_GATE_FORCE_ENABLED: u8 = 2;

async fn site_replication_gate_enabled() -> S3Result<bool> {
    #[cfg(test)]
    match SITE_REPLICATION_GATE_TEST_OVERRIDE.load(std::sync::atomic::Ordering::SeqCst) {
        SITE_REPLICATION_GATE_FORCE_DISABLED => return Ok(false),
        SITE_REPLICATION_GATE_FORCE_ENABLED => return Ok(true),
        _ => {}
    }
    crate::admin::handlers::site_replication::site_replication_enabled().await
}

/// Remote site-replication peer deployment ids and the operator-rule contract
/// the peers support, handed to the bucket usecase so an S3 replication-config
/// edit keeps exactly the reconciler-owned rules and merges the way every
/// peer will (issue #1948). Read here, in the interface layer, because the
/// usecase must not import the admin handlers (layer guard); a state-read
/// failure propagates so the edit fails closed.
async fn site_replication_edit_context() -> S3Result<(HashSet<String>, OperatorRuleContract)> {
    // While the gate override is in effect the test exercises the deny/allow
    // branch, not the peer set; there is no persisted state to read.
    #[cfg(test)]
    if SITE_REPLICATION_GATE_TEST_OVERRIDE.load(std::sync::atomic::Ordering::SeqCst) != 0 {
        return Ok((HashSet::new(), OperatorRuleContract::Derived));
    }
    crate::admin::handlers::site_replication::site_replication_edit_context().await
}

/// MinIO `ErrReplicationDenyEditError`.
fn replication_deny_edit_error() -> S3Error {
    let mut err = S3Error::with_message(
        S3ErrorCode::Custom("XMinioReplicationDenyEdit".into()),
        "Sub-User is not allowed to edit Replication configuration",
    );
    err.set_status_code(StatusCode::BAD_REQUEST);
    err
}

/// Site-replication gate for S3 replication-config edits (issue #1948).
///
/// On a site-replication deployment the bucket's replication config carries
/// the operator-managed `site-repl-*` rules that keep every peer in sync, and
/// a successful edit is broadcast to all peers — so a user holding only
/// bucket-scoped `s3:PutReplicationConfiguration` could rewrite or erase
/// replication net-wide. MinIO parity (`ErrReplicationDenyEditError`): only
/// owner credentials (root or root-parented) may edit. Runs after the policy
/// authorization in the access layer and only on the external S3 path — the
/// reconciler and peer bucket-meta ingestion never route through these
/// handlers.
pub(crate) fn prepare_replication_config_edit<R: AuthorizationRequest + ?Sized>(
    request: &R,
) -> impl Future<Output = S3Result<(HashSet<String>, OperatorRuleContract)>> + Send + use<R> {
    let is_owner = req_info_ref(request).map(|info| info.is_owner).map_err(S3Error::from);
    async move {
        if !is_owner? && site_replication_gate_enabled().await? {
            return Err(replication_deny_edit_error());
        }
        site_replication_edit_context().await
    }
}
