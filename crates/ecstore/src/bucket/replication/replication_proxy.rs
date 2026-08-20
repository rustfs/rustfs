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

//! Proxy-target selection for reads of objects not yet replicated locally
//! (MinIO `getProxyTargets`, bucket-replication.go).
//!
//! During the active-active replication lag window a GET/HEAD/Tagging request
//! for an object the local site does not have yet may be served by proxying to
//! a replication target. This module only *selects* the candidate targets; the
//! request-path callers perform the remote calls and response translation.

use std::sync::Arc;

use tracing::debug;

use super::replication_config_boundary::{ObjectOpts, ReplicationConfigurationExt as _};
use super::replication_object_config::get_replication_config;
use super::replication_storage_boundary::ObjectOptions;
use super::replication_target_boundary::{ReplicationTargetStore, TargetClient};

/// Returns the replication-target clients eligible to serve a proxied read of
/// `bucket/object`, in rule order. Mirrors MinIO's `getProxyTargets`:
///
/// - the `source-proxy-request` header family was present at all
///   (`opts.proxy_request` / `opts.proxy_header_set`, MinIO `ProxyRequest` /
///   `ProxyHeaderSet`) -> empty. "true" is the anti-loop marker of an
///   already-proxied client read; "false" is what a peer's replication
///   worker sends on convergence HEADs so the receiver answers locally —
///   proxying that miss back would echo the source object and fake
///   convergence, permanently skipping replication;
/// - the bucket's versioning is suspended for the object -> empty;
/// - no replication configuration / no matching rule -> empty;
/// - otherwise every distinct target ARN whose rules match the object,
///   resolved through the bucket target system, skipping targets that opted
///   out of proxying (`disable_proxy`).
pub async fn get_proxy_targets(bucket: &str, object: &str, opts: &ObjectOptions) -> Vec<Arc<TargetClient>> {
    if opts.proxy_request || opts.proxy_header_set {
        return Vec::new();
    }
    if opts.version_suspended {
        return Vec::new();
    }

    let cfg = match get_replication_config(bucket).await {
        Ok(Some(cfg)) => cfg,
        Ok(None) => return Vec::new(),
        Err(err) => {
            debug!(bucket, object, error = %err, "read proxy: failed to load replication config; not proxying");
            return Vec::new();
        }
    };

    let arns = cfg.filter_target_arns(&ObjectOpts {
        name: object.to_string(),
        ..Default::default()
    });

    let mut targets = Vec::with_capacity(arns.len());
    for arn in arns {
        let Some(client) = ReplicationTargetStore::remote_target_client(bucket, &arn).await else {
            debug!(bucket, object, arn, "read proxy: no client for replication target ARN");
            continue;
        };
        if client.disable_proxy {
            continue;
        }
        targets.push(client);
    }

    targets
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts() -> ObjectOptions {
        ObjectOptions::default()
    }

    /// Anti-loop: a request that was already proxied by a peer must never be
    /// proxied onward, regardless of replication configuration.
    #[tokio::test]
    async fn proxy_request_yields_no_targets() {
        let targets = get_proxy_targets(
            "bucket",
            "object",
            &ObjectOptions {
                proxy_request: true,
                ..opts()
            },
        )
        .await;
        assert!(targets.is_empty());
    }

    /// MinIO `ProxyHeaderSet` parity: the header family being present at all
    /// disables proxying, even with the value "false" — that is what a
    /// peer's replication worker sends on convergence HEADs.
    #[tokio::test]
    async fn proxy_header_set_yields_no_targets() {
        let targets = get_proxy_targets(
            "bucket",
            "object",
            &ObjectOptions {
                proxy_header_set: true,
                proxy_request: false,
                ..opts()
            },
        )
        .await;
        assert!(targets.is_empty());
    }

    /// Suspended versioning disables proxying (MinIO parity): the local null
    /// version is authoritative and a remote read could resurrect data.
    #[tokio::test]
    async fn version_suspended_yields_no_targets() {
        let targets = get_proxy_targets(
            "bucket",
            "object",
            &ObjectOptions {
                version_suspended: true,
                ..opts()
            },
        )
        .await;
        assert!(targets.is_empty());
    }

    /// A bucket without replication configuration has nothing to proxy to.
    /// (No metadata system is running in unit tests, so the config lookup
    /// resolves to "no configuration" — the same empty-result contract.)
    #[tokio::test]
    async fn missing_replication_config_yields_no_targets() {
        let targets = get_proxy_targets("bucket-without-replication", "object", &opts()).await;
        assert!(targets.is_empty());
    }
}
