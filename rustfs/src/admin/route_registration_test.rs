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

use crate::admin::{
    handlers::health,
    router::{AdminOperation, S3Router},
};
use crate::server::{
    ADMIN_PREFIX, HEALTH_PREFIX, HEALTH_READY_PATH, MINIO_ADMIN_PREFIX, PROFILE_CPU_PATH, PROFILE_MEMORY_PATH,
    TABLE_CATALOG_COMPAT_PREFIX, TABLE_CATALOG_PREFIX,
};
use hyper::Method;
use serial_test::serial;
use std::collections::BTreeSet;
use temp_env::with_var;

fn admin_path(path: &str) -> String {
    format!("{}{}", ADMIN_PREFIX, path)
}

fn compat_admin_alias_path(path: &str) -> String {
    format!("{}{}", MINIO_ADMIN_PREFIX, path)
}

fn table_catalog_path(path: &str) -> String {
    format!("{}{}", TABLE_CATALOG_PREFIX, path)
}

fn compat_table_catalog_path(path: &str) -> String {
    format!("{}{}", TABLE_CATALOG_COMPAT_PREFIX, path)
}

#[derive(Debug)]
struct RouteMatrixEntry {
    method: Method,
    pattern: String,
    sample: String,
}

fn route(method: Method, pattern: impl Into<String>) -> RouteMatrixEntry {
    let pattern = pattern.into();
    RouteMatrixEntry {
        method,
        sample: pattern.clone(),
        pattern,
    }
}

fn route_sample(method: Method, pattern: impl Into<String>, sample: impl Into<String>) -> RouteMatrixEntry {
    RouteMatrixEntry {
        method,
        pattern: pattern.into(),
        sample: sample.into(),
    }
}

fn admin_route(method: Method, pattern: &str) -> RouteMatrixEntry {
    route(method, admin_path(pattern))
}

fn admin_route_sample(method: Method, pattern: &str, sample: &str) -> RouteMatrixEntry {
    route_sample(method, admin_path(pattern), admin_path(sample))
}

fn table_route(method: Method, pattern: &str) -> RouteMatrixEntry {
    route(method, table_catalog_path(pattern))
}

fn table_route_sample(method: Method, pattern: &str, sample: &str) -> RouteMatrixEntry {
    route_sample(method, table_catalog_path(pattern), table_catalog_path(sample))
}

fn compat_table_route(method: Method, pattern: &str) -> RouteMatrixEntry {
    route(method, compat_table_catalog_path(pattern))
}

fn compat_table_route_sample(method: Method, pattern: &str, sample: &str) -> RouteMatrixEntry {
    route_sample(method, compat_table_catalog_path(pattern), compat_table_catalog_path(sample))
}

fn route_key(method: &Method, path: &str) -> String {
    format!("{}|{}", method.as_str(), path)
}

fn route_key_set(routes: &[RouteMatrixEntry]) -> BTreeSet<String> {
    routes.iter().map(|route| route_key(&route.method, &route.pattern)).collect()
}

fn assert_route(router: &S3Router<AdminOperation>, method: Method, path: &str) {
    assert!(
        router.contains_route(method.clone(), path),
        "expected route missing: {} {}",
        method.as_str(),
        path
    );
}

fn registered_admin_router() -> S3Router<AdminOperation> {
    with_var(rustfs_config::ENV_HEALTH_ENDPOINT_ENABLE, Some("true"), || {
        let mut router = S3Router::new(false);
        super::register_admin_routes(&mut router).expect("register production admin routes");
        router
    })
}

fn expected_admin_route_matrix() -> Vec<RouteMatrixEntry> {
    vec![
        route(Method::GET, HEALTH_PREFIX),
        route(Method::HEAD, HEALTH_PREFIX),
        route(Method::GET, HEALTH_READY_PATH),
        route(Method::HEAD, HEALTH_READY_PATH),
        route(Method::GET, PROFILE_CPU_PATH),
        route(Method::GET, PROFILE_MEMORY_PATH),
        route(Method::POST, "/"),
        admin_route(Method::GET, "/v3/is-admin"),
        admin_route(Method::GET, "/v3/accountinfo"),
        admin_route(Method::GET, "/v3/list-users"),
        admin_route(Method::GET, "/v3/user-info"),
        admin_route(Method::DELETE, "/v3/remove-user"),
        admin_route(Method::PUT, "/v3/add-user"),
        admin_route(Method::PUT, "/v3/set-user-status"),
        admin_route(Method::GET, "/v3/groups"),
        admin_route(Method::GET, "/v3/group"),
        admin_route_sample(Method::DELETE, "/v3/group/{group}", "/v3/group/test-group"),
        admin_route(Method::PUT, "/v3/set-group-status"),
        admin_route(Method::PUT, "/v3/update-group-members"),
        admin_route(Method::POST, "/v3/update-service-account"),
        admin_route(Method::GET, "/v3/info-service-account"),
        admin_route(Method::GET, "/v3/temporary-account-info"),
        admin_route(Method::GET, "/v3/info-access-key"),
        admin_route(Method::GET, "/v3/list-service-accounts"),
        admin_route(Method::GET, "/v3/list-access-keys-bulk"),
        admin_route(Method::DELETE, "/v3/delete-service-accounts"),
        admin_route(Method::DELETE, "/v3/delete-service-account"),
        admin_route(Method::PUT, "/v3/add-service-accounts"),
        admin_route(Method::PUT, "/v3/add-service-account"),
        admin_route(Method::GET, "/v3/export-iam"),
        admin_route(Method::PUT, "/v3/import-iam"),
        admin_route(Method::GET, "/v3/list-canned-policies"),
        admin_route(Method::GET, "/v3/info-canned-policy"),
        admin_route(Method::PUT, "/v3/add-canned-policy"),
        admin_route(Method::DELETE, "/v3/remove-canned-policy"),
        admin_route(Method::PUT, "/v3/set-user-or-group-policy"),
        admin_route(Method::PUT, "/v3/set-policy"),
        admin_route(Method::POST, "/v3/idp/builtin/policy/attach"),
        admin_route(Method::POST, "/v3/idp/builtin/policy/detach"),
        admin_route(Method::GET, "/v3/idp/builtin/policy-entities"),
        admin_route(Method::GET, "/v3/target/list"),
        admin_route_sample(Method::PUT, "/v3/target/{target_type}/{target_name}", "/v3/target/webhook/test-target"),
        admin_route_sample(
            Method::DELETE,
            "/v3/target/{target_type}/{target_name}/reset",
            "/v3/target/webhook/test-target/reset",
        ),
        admin_route(Method::GET, "/v3/target/arns"),
        admin_route(Method::POST, "/v3/service"),
        admin_route(Method::GET, "/v3/info"),
        admin_route(Method::GET, "/v3/inspect-data"),
        admin_route(Method::POST, "/v3/inspect-data"),
        admin_route(Method::GET, "/v3/storageinfo"),
        admin_route(Method::GET, "/v3/datausageinfo"),
        admin_route(Method::GET, "/v3/metrics"),
        admin_route(Method::GET, "/v3/pools/list"),
        admin_route(Method::GET, "/v3/pools/status"),
        admin_route(Method::POST, "/v3/pools/decommission"),
        admin_route(Method::POST, "/v3/pools/cancel"),
        admin_route(Method::POST, "/v3/rebalance/start"),
        admin_route(Method::GET, "/v3/rebalance/status"),
        admin_route(Method::POST, "/v3/rebalance/stop"),
        admin_route(Method::POST, "/v3/heal/"),
        admin_route_sample(Method::POST, "/v3/heal/{bucket}", "/v3/heal/test-bucket"),
        admin_route_sample(Method::POST, "/v3/heal/{bucket}/{prefix}", "/v3/heal/test-bucket/prefix"),
        admin_route(Method::POST, "/v3/background-heal/status"),
        admin_route(Method::GET, "/v3/tier"),
        admin_route(Method::GET, "/v3/tier-stats"),
        admin_route_sample(Method::GET, "/v3/tier/{tier}", "/v3/tier/HOT"),
        admin_route_sample(Method::DELETE, "/v3/tier/{tiername}", "/v3/tier/HOT"),
        admin_route(Method::PUT, "/v3/tier"),
        admin_route_sample(Method::POST, "/v3/tier/{tiername}", "/v3/tier/HOT"),
        admin_route(Method::POST, "/v3/tier/clear"),
        admin_route(Method::PUT, "/v3/set-bucket-quota"),
        admin_route(Method::GET, "/v3/get-bucket-quota"),
        admin_route_sample(Method::PUT, "/v3/quota/{bucket}", "/v3/quota/test-bucket"),
        admin_route_sample(Method::GET, "/v3/quota/{bucket}", "/v3/quota/test-bucket"),
        admin_route_sample(Method::DELETE, "/v3/quota/{bucket}", "/v3/quota/test-bucket"),
        admin_route_sample(Method::GET, "/v3/quota-stats/{bucket}", "/v3/quota-stats/test-bucket"),
        admin_route_sample(Method::POST, "/v3/quota-check/{bucket}", "/v3/quota-check/test-bucket"),
        admin_route(Method::GET, "/export-bucket-metadata"),
        admin_route(Method::GET, "/v3/export-bucket-metadata"),
        admin_route(Method::PUT, "/import-bucket-metadata"),
        admin_route(Method::PUT, "/v3/import-bucket-metadata"),
        admin_route(Method::GET, "/v3/get-config-kv"),
        admin_route(Method::PUT, "/v3/set-config-kv"),
        admin_route(Method::DELETE, "/v3/del-config-kv"),
        admin_route(Method::GET, "/v3/help-config-kv"),
        admin_route(Method::GET, "/v3/list-config-history-kv"),
        admin_route(Method::DELETE, "/v3/clear-config-history-kv"),
        admin_route(Method::PUT, "/v3/restore-config-history-kv"),
        admin_route(Method::GET, "/v3/config"),
        admin_route(Method::PUT, "/v3/config"),
        admin_route(Method::GET, "/v3/scanner/status"),
        admin_route(Method::GET, "/v3/audit/target/list"),
        admin_route_sample(
            Method::PUT,
            "/v3/audit/target/{target_type}/{target_name}",
            "/v3/audit/target/audit_webhook/test-audit",
        ),
        admin_route_sample(
            Method::DELETE,
            "/v3/audit/target/{target_type}/{target_name}/reset",
            "/v3/audit/target/audit_webhook/test-audit/reset",
        ),
        admin_route(Method::GET, "/v3/module-switches"),
        admin_route(Method::PUT, "/v3/module-switches"),
        admin_route(Method::GET, "/v4/extensions/catalog"),
        admin_route(Method::GET, "/v4/extensions/instances"),
        admin_route(Method::POST, "/v3/object-zip-downloads"),
        admin_route_sample(
            Method::GET,
            "/v3/object-zip-downloads/{id}.zip",
            "/v3/object-zip-downloads/example-id.zip",
        ),
        admin_route(Method::GET, "/v4/plugins/catalog"),
        admin_route(Method::GET, "/v4/plugins/instances"),
        admin_route_sample(Method::GET, "/v4/plugins/instances/{id}", "/v4/plugins/instances/example-id"),
        admin_route_sample(Method::PUT, "/v4/plugins/instances/{id}", "/v4/plugins/instances/example-id"),
        admin_route_sample(Method::DELETE, "/v4/plugins/instances/{id}", "/v4/plugins/instances/example-id"),
        admin_route(Method::GET, "/v3/list-remote-targets"),
        admin_route(Method::GET, "/v3/replicationmetrics"),
        admin_route(Method::PUT, "/v3/set-remote-target"),
        admin_route(Method::DELETE, "/v3/remove-remote-target"),
        admin_route(Method::PUT, "/v3/site-replication/add"),
        admin_route(Method::PUT, "/v3/site-replication/remove"),
        admin_route(Method::GET, "/v3/site-replication/info"),
        admin_route(Method::GET, "/v3/site-replication/metainfo"),
        admin_route(Method::GET, "/v3/site-replication/status"),
        admin_route(Method::POST, "/v3/site-replication/devnull"),
        admin_route(Method::POST, "/v3/site-replication/netperf"),
        admin_route(Method::POST, "/v3/site-replication/rotate-svc-acct"),
        admin_route(Method::PUT, "/v3/site-replication/peer/join"),
        admin_route(Method::PUT, "/v3/site-replication/peer/bucket-ops"),
        admin_route(Method::PUT, "/v3/site-replication/peer/iam-item"),
        admin_route(Method::PUT, "/v3/site-replication/peer/bucket-meta"),
        admin_route(Method::GET, "/v3/site-replication/peer/idp-settings"),
        admin_route(Method::PUT, "/v3/site-replication/edit"),
        admin_route(Method::PUT, "/v3/site-replication/peer/edit"),
        admin_route(Method::PUT, "/v3/site-replication/peer/remove"),
        admin_route(Method::PUT, "/v3/site-replication/resync/op"),
        admin_route(Method::PUT, "/v3/site-replication/state/edit"),
        admin_route(Method::GET, "/debug/pprof/profile"),
        admin_route(Method::GET, "/debug/pprof/status"),
        admin_route(Method::GET, "/debug/tls/status"),
        admin_route(Method::POST, "/v3/kms/create-key"),
        admin_route(Method::POST, "/v3/kms/key/create"),
        admin_route(Method::GET, "/v3/kms/describe-key"),
        admin_route(Method::GET, "/v3/kms/key/status"),
        admin_route(Method::GET, "/v3/kms/list-keys"),
        admin_route(Method::POST, "/v3/kms/generate-data-key"),
        admin_route(Method::GET, "/v3/kms/status"),
        admin_route(Method::POST, "/v3/kms/status"),
        admin_route(Method::GET, "/v3/kms/config"),
        admin_route(Method::POST, "/v3/kms/clear-cache"),
        admin_route(Method::POST, "/v3/kms/configure"),
        admin_route(Method::POST, "/v3/kms/start"),
        admin_route(Method::POST, "/v3/kms/stop"),
        admin_route(Method::GET, "/v3/kms/service-status"),
        admin_route(Method::POST, "/v3/kms/reconfigure"),
        admin_route(Method::POST, "/v3/kms/keys"),
        admin_route(Method::DELETE, "/v3/kms/keys/delete"),
        admin_route(Method::POST, "/v3/kms/keys/cancel-deletion"),
        admin_route(Method::GET, "/v3/kms/keys"),
        admin_route_sample(Method::GET, "/v3/kms/keys/{key_id}", "/v3/kms/keys/test-key"),
        admin_route(Method::GET, "/v3/oidc/providers"),
        admin_route_sample(Method::GET, "/v3/oidc/authorize/{provider_id}", "/v3/oidc/authorize/default"),
        admin_route_sample(Method::GET, "/v3/oidc/callback/{provider_id}", "/v3/oidc/callback/default"),
        admin_route(Method::GET, "/v3/oidc/logout"),
        admin_route(Method::GET, "/v3/oidc/config"),
        admin_route_sample(Method::PUT, "/v3/oidc/config/{provider_id}", "/v3/oidc/config/default"),
        admin_route_sample(Method::DELETE, "/v3/oidc/config/{provider_id}", "/v3/oidc/config/default"),
        admin_route(Method::POST, "/v3/oidc/validate"),
        table_route(Method::GET, "/config"),
        table_route_sample(Method::PUT, "/buckets/{warehouse}", "/buckets/analytics"),
        table_route_sample(Method::GET, "/buckets/{warehouse}", "/buckets/analytics"),
        table_route_sample(Method::GET, "/{warehouse}/namespaces", "/analytics/namespaces"),
        table_route_sample(Method::POST, "/{warehouse}/namespaces", "/analytics/namespaces"),
        table_route_sample(Method::GET, "/{warehouse}/namespaces/{namespace}", "/analytics/namespaces/sales"),
        table_route_sample(Method::HEAD, "/{warehouse}/namespaces/{namespace}", "/analytics/namespaces/sales"),
        table_route_sample(Method::DELETE, "/{warehouse}/namespaces/{namespace}", "/analytics/namespaces/sales"),
        table_route_sample(
            Method::GET,
            "/{warehouse}/namespaces/{namespace}/tables",
            "/analytics/namespaces/sales/tables",
        ),
        table_route_sample(
            Method::POST,
            "/{warehouse}/namespaces/{namespace}/tables",
            "/analytics/namespaces/sales/tables",
        ),
        table_route_sample(
            Method::POST,
            "/{warehouse}/namespaces/{namespace}/register",
            "/analytics/namespaces/sales/register",
        ),
        table_route_sample(
            Method::GET,
            "/{warehouse}/namespaces/{namespace}/views",
            "/analytics/namespaces/sales/views",
        ),
        table_route_sample(
            Method::POST,
            "/{warehouse}/namespaces/{namespace}/views",
            "/analytics/namespaces/sales/views",
        ),
        table_route_sample(
            Method::GET,
            "/{warehouse}/namespaces/{namespace}/tables/{table}",
            "/analytics/namespaces/sales/tables/orders",
        ),
        table_route_sample(
            Method::HEAD,
            "/{warehouse}/namespaces/{namespace}/tables/{table}",
            "/analytics/namespaces/sales/tables/orders",
        ),
        table_route_sample(
            Method::GET,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/credentials",
            "/analytics/namespaces/sales/tables/orders/credentials",
        ),
        table_route_sample(
            Method::POST,
            "/{warehouse}/namespaces/{namespace}/tables/{table}",
            "/analytics/namespaces/sales/tables/orders",
        ),
        table_route_sample(
            Method::DELETE,
            "/{warehouse}/namespaces/{namespace}/tables/{table}",
            "/analytics/namespaces/sales/tables/orders",
        ),
        table_route_sample(
            Method::GET,
            "/{warehouse}/namespaces/{namespace}/views/{view}",
            "/analytics/namespaces/sales/views/recent_orders",
        ),
        table_route_sample(
            Method::HEAD,
            "/{warehouse}/namespaces/{namespace}/views/{view}",
            "/analytics/namespaces/sales/views/recent_orders",
        ),
        table_route_sample(
            Method::POST,
            "/{warehouse}/namespaces/{namespace}/views/{view}",
            "/analytics/namespaces/sales/views/recent_orders",
        ),
        table_route_sample(
            Method::DELETE,
            "/{warehouse}/namespaces/{namespace}/views/{view}",
            "/analytics/namespaces/sales/views/recent_orders",
        ),
        table_route_sample(
            Method::GET,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/refs",
            "/analytics/namespaces/sales/tables/orders/refs",
        ),
        table_route_sample(
            Method::PUT,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/refs/{ref}",
            "/analytics/namespaces/sales/tables/orders/refs/audit",
        ),
        table_route_sample(
            Method::DELETE,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/refs/{ref}",
            "/analytics/namespaces/sales/tables/orders/refs/audit",
        ),
        table_route_sample(
            Method::POST,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/metadata",
            "/analytics/namespaces/sales/tables/orders/maintenance/metadata",
        ),
        table_route_sample(
            Method::GET,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/metadata-location",
            "/analytics/namespaces/sales/tables/orders/metadata-location",
        ),
        table_route_sample(
            Method::PUT,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/metadata-location",
            "/analytics/namespaces/sales/tables/orders/metadata-location",
        ),
        table_route_sample(
            Method::GET,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/config",
            "/analytics/namespaces/sales/tables/orders/maintenance/config",
        ),
        table_route_sample(
            Method::PUT,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/config",
            "/analytics/namespaces/sales/tables/orders/maintenance/config",
        ),
        table_route_sample(
            Method::GET,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/jobs/{job}",
            "/analytics/namespaces/sales/tables/orders/maintenance/jobs/job-1",
        ),
        table_route_sample(
            Method::POST,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/worker/run",
            "/analytics/namespaces/sales/tables/orders/maintenance/worker/run",
        ),
        table_route_sample(
            Method::POST,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/jobs/{job}/heartbeat",
            "/analytics/namespaces/sales/tables/orders/maintenance/jobs/job-1/heartbeat",
        ),
        table_route_sample(
            Method::GET,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/export",
            "/analytics/namespaces/sales/tables/orders/catalog/export",
        ),
        table_route_sample(
            Method::POST,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/import",
            "/analytics/namespaces/sales/tables/orders/catalog/import",
        ),
        table_route_sample(
            Method::GET,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/external",
            "/analytics/namespaces/sales/tables/orders/catalog/external",
        ),
        table_route_sample(
            Method::PUT,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/external",
            "/analytics/namespaces/sales/tables/orders/catalog/external",
        ),
        table_route_sample(
            Method::POST,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/external/sync",
            "/analytics/namespaces/sales/tables/orders/catalog/external/sync",
        ),
        table_route_sample(
            Method::GET,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/diagnostics",
            "/analytics/namespaces/sales/tables/orders/catalog/diagnostics",
        ),
        table_route_sample(
            Method::POST,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/recovery",
            "/analytics/namespaces/sales/tables/orders/catalog/recovery",
        ),
        table_route_sample(
            Method::POST,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/rollback",
            "/analytics/namespaces/sales/tables/orders/catalog/rollback",
        ),
        compat_table_route(Method::GET, "/config"),
        compat_table_route_sample(Method::PUT, "/buckets/{warehouse}", "/buckets/analytics"),
        compat_table_route_sample(Method::GET, "/buckets/{warehouse}", "/buckets/analytics"),
        compat_table_route_sample(Method::GET, "/{warehouse}/namespaces", "/analytics/namespaces"),
        compat_table_route_sample(Method::POST, "/{warehouse}/namespaces", "/analytics/namespaces"),
        compat_table_route_sample(Method::GET, "/{warehouse}/namespaces/{namespace}", "/analytics/namespaces/sales"),
        compat_table_route_sample(Method::HEAD, "/{warehouse}/namespaces/{namespace}", "/analytics/namespaces/sales"),
        compat_table_route_sample(Method::DELETE, "/{warehouse}/namespaces/{namespace}", "/analytics/namespaces/sales"),
        compat_table_route_sample(
            Method::GET,
            "/{warehouse}/namespaces/{namespace}/tables",
            "/analytics/namespaces/sales/tables",
        ),
        compat_table_route_sample(
            Method::POST,
            "/{warehouse}/namespaces/{namespace}/tables",
            "/analytics/namespaces/sales/tables",
        ),
        compat_table_route_sample(
            Method::POST,
            "/{warehouse}/namespaces/{namespace}/register",
            "/analytics/namespaces/sales/register",
        ),
        compat_table_route_sample(
            Method::GET,
            "/{warehouse}/namespaces/{namespace}/views",
            "/analytics/namespaces/sales/views",
        ),
        compat_table_route_sample(
            Method::POST,
            "/{warehouse}/namespaces/{namespace}/views",
            "/analytics/namespaces/sales/views",
        ),
        compat_table_route_sample(
            Method::GET,
            "/{warehouse}/namespaces/{namespace}/tables/{table}",
            "/analytics/namespaces/sales/tables/orders",
        ),
        compat_table_route_sample(
            Method::HEAD,
            "/{warehouse}/namespaces/{namespace}/tables/{table}",
            "/analytics/namespaces/sales/tables/orders",
        ),
        compat_table_route_sample(
            Method::GET,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/credentials",
            "/analytics/namespaces/sales/tables/orders/credentials",
        ),
        compat_table_route_sample(
            Method::POST,
            "/{warehouse}/namespaces/{namespace}/tables/{table}",
            "/analytics/namespaces/sales/tables/orders",
        ),
        compat_table_route_sample(
            Method::DELETE,
            "/{warehouse}/namespaces/{namespace}/tables/{table}",
            "/analytics/namespaces/sales/tables/orders",
        ),
        compat_table_route_sample(
            Method::GET,
            "/{warehouse}/namespaces/{namespace}/views/{view}",
            "/analytics/namespaces/sales/views/recent_orders",
        ),
        compat_table_route_sample(
            Method::HEAD,
            "/{warehouse}/namespaces/{namespace}/views/{view}",
            "/analytics/namespaces/sales/views/recent_orders",
        ),
        compat_table_route_sample(
            Method::POST,
            "/{warehouse}/namespaces/{namespace}/views/{view}",
            "/analytics/namespaces/sales/views/recent_orders",
        ),
        compat_table_route_sample(
            Method::DELETE,
            "/{warehouse}/namespaces/{namespace}/views/{view}",
            "/analytics/namespaces/sales/views/recent_orders",
        ),
        compat_table_route_sample(
            Method::GET,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/refs",
            "/analytics/namespaces/sales/tables/orders/refs",
        ),
        compat_table_route_sample(
            Method::PUT,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/refs/{ref}",
            "/analytics/namespaces/sales/tables/orders/refs/audit",
        ),
        compat_table_route_sample(
            Method::DELETE,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/refs/{ref}",
            "/analytics/namespaces/sales/tables/orders/refs/audit",
        ),
        compat_table_route_sample(
            Method::POST,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/metadata",
            "/analytics/namespaces/sales/tables/orders/maintenance/metadata",
        ),
        compat_table_route_sample(
            Method::GET,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/metadata-location",
            "/analytics/namespaces/sales/tables/orders/metadata-location",
        ),
        compat_table_route_sample(
            Method::PUT,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/metadata-location",
            "/analytics/namespaces/sales/tables/orders/metadata-location",
        ),
        compat_table_route_sample(
            Method::GET,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/config",
            "/analytics/namespaces/sales/tables/orders/maintenance/config",
        ),
        compat_table_route_sample(
            Method::PUT,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/config",
            "/analytics/namespaces/sales/tables/orders/maintenance/config",
        ),
        compat_table_route_sample(
            Method::GET,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/jobs/{job}",
            "/analytics/namespaces/sales/tables/orders/maintenance/jobs/job-1",
        ),
        compat_table_route_sample(
            Method::POST,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/worker/run",
            "/analytics/namespaces/sales/tables/orders/maintenance/worker/run",
        ),
        compat_table_route_sample(
            Method::POST,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/jobs/{job}/heartbeat",
            "/analytics/namespaces/sales/tables/orders/maintenance/jobs/job-1/heartbeat",
        ),
        compat_table_route_sample(
            Method::GET,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/export",
            "/analytics/namespaces/sales/tables/orders/catalog/export",
        ),
        compat_table_route_sample(
            Method::POST,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/import",
            "/analytics/namespaces/sales/tables/orders/catalog/import",
        ),
        compat_table_route_sample(
            Method::GET,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/external",
            "/analytics/namespaces/sales/tables/orders/catalog/external",
        ),
        compat_table_route_sample(
            Method::PUT,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/external",
            "/analytics/namespaces/sales/tables/orders/catalog/external",
        ),
        compat_table_route_sample(
            Method::POST,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/external/sync",
            "/analytics/namespaces/sales/tables/orders/catalog/external/sync",
        ),
        compat_table_route_sample(
            Method::GET,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/diagnostics",
            "/analytics/namespaces/sales/tables/orders/catalog/diagnostics",
        ),
        compat_table_route_sample(
            Method::POST,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/recovery",
            "/analytics/namespaces/sales/tables/orders/catalog/recovery",
        ),
        compat_table_route_sample(
            Method::POST,
            "/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/rollback",
            "/analytics/namespaces/sales/tables/orders/catalog/rollback",
        ),
    ]
}

// registered_admin_router pins ENV_HEALTH_ENDPOINT_ENABLE because the
// production registration helper intentionally honors that environment switch.
#[test]
#[serial]
fn test_admin_route_matrix_matches_registered_routes() {
    let router = registered_admin_router();

    let matrix = expected_admin_route_matrix();
    let actual_routes = router.registered_routes();
    assert_eq!(
        actual_routes.len(),
        matrix.len(),
        "admin route matrix must account for every registered route"
    );

    let actual = actual_routes.iter().cloned().collect::<BTreeSet<_>>();
    let expected = route_key_set(&matrix);
    assert_eq!(actual, expected, "admin route registration changed without updating the matrix");

    for route in matrix {
        assert_route(&router, route.method, &route.sample);
    }
}

#[test]
#[serial]
fn test_admin_route_matrix_preserves_minio_admin_aliases() {
    let router = registered_admin_router();

    for route in expected_admin_route_matrix()
        .into_iter()
        .filter(|route| route.sample.starts_with(ADMIN_PREFIX))
    {
        let suffix = route
            .sample
            .strip_prefix(ADMIN_PREFIX)
            .expect("matrix route is already filtered by admin prefix");
        let alias = compat_admin_alias_path(suffix);
        assert!(
            router.contains_compatible_route(route.method.clone(), &alias),
            "expected MinIO admin alias path to match: {} {}",
            route.method.as_str(),
            alias
        );
    }
}

#[test]
#[serial]
fn test_register_routes_cover_representative_admin_paths() {
    let router = registered_admin_router();
    assert_route(&router, Method::GET, HEALTH_PREFIX);
    assert_route(&router, Method::HEAD, HEALTH_PREFIX);
    assert_route(&router, Method::GET, HEALTH_READY_PATH);
    assert_route(&router, Method::HEAD, HEALTH_READY_PATH);
    assert_route(&router, Method::GET, PROFILE_CPU_PATH);
    assert_route(&router, Method::GET, PROFILE_MEMORY_PATH);

    assert_route(&router, Method::POST, "/");
    assert_route(&router, Method::GET, &admin_path("/v3/is-admin"));

    assert_route(&router, Method::GET, &admin_path("/v3/list-users"));
    assert_route(&router, Method::PUT, &admin_path("/v3/add-user"));
    assert_route(&router, Method::PUT, &admin_path("/v3/set-user-status"));
    assert_route(&router, Method::GET, &admin_path("/v3/groups"));
    assert_route(&router, Method::DELETE, &admin_path("/v3/group/test-group"));
    assert_route(&router, Method::PUT, &admin_path("/v3/update-group-members"));
    assert_route(&router, Method::PUT, &admin_path("/v3/add-service-accounts"));
    assert_route(&router, Method::PUT, &admin_path("/v3/add-service-account"));
    assert_route(&router, Method::GET, &admin_path("/v3/temporary-account-info"));
    assert_route(&router, Method::GET, &admin_path("/v3/info-access-key"));
    assert_route(&router, Method::GET, &admin_path("/v3/list-access-keys-bulk"));
    assert_route(&router, Method::GET, &admin_path("/v3/export-iam"));
    assert_route(&router, Method::PUT, &admin_path("/v3/import-iam"));
    assert_route(&router, Method::GET, &admin_path("/v3/list-canned-policies"));
    assert_route(&router, Method::PUT, &admin_path("/v3/set-policy"));
    assert_route(&router, Method::POST, &admin_path("/v3/idp/builtin/policy/attach"));
    assert_route(&router, Method::POST, &admin_path("/v3/idp/builtin/policy/detach"));
    assert_route(&router, Method::GET, &admin_path("/v3/idp/builtin/policy-entities"));
    assert_route(&router, Method::GET, &admin_path("/v3/target/list"));
    assert_route(&router, Method::GET, &admin_path("/v3/audit/target/list"));
    assert_route(&router, Method::GET, &admin_path("/v3/module-switches"));
    assert_route(&router, Method::PUT, &admin_path("/v3/module-switches"));
    assert_route(&router, Method::GET, &admin_path("/v4/extensions/catalog"));
    assert_route(&router, Method::GET, &admin_path("/v4/extensions/instances"));
    assert_route(&router, Method::POST, &admin_path("/v3/object-zip-downloads"));
    assert_route(&router, Method::GET, &admin_path("/v4/plugins/catalog"));
    assert_route(&router, Method::GET, &admin_path("/v4/plugins/instances"));
    assert_route(&router, Method::GET, &admin_path("/v4/plugins/instances/example-id"));
    assert_route(&router, Method::PUT, &admin_path("/v4/plugins/instances/example-id"));
    assert_route(&router, Method::DELETE, &admin_path("/v4/plugins/instances/example-id"));
    assert_route(&router, Method::PUT, &admin_path("/v3/audit/target/audit_webhook/test-audit"));
    assert_route(&router, Method::DELETE, &admin_path("/v3/audit/target/audit_webhook/test-audit/reset"));
    assert_route(&router, Method::GET, &admin_path("/v3/accountinfo"));

    assert_route(&router, Method::GET, &admin_path("/v3/get-config-kv"));
    assert_route(&router, Method::PUT, &admin_path("/v3/set-config-kv"));
    assert_route(&router, Method::DELETE, &admin_path("/v3/del-config-kv"));
    assert_route(&router, Method::GET, &admin_path("/v3/help-config-kv"));
    assert_route(&router, Method::GET, &admin_path("/v3/list-config-history-kv"));
    assert_route(&router, Method::DELETE, &admin_path("/v3/clear-config-history-kv"));
    assert_route(&router, Method::PUT, &admin_path("/v3/restore-config-history-kv"));
    assert_route(&router, Method::GET, &admin_path("/v3/config"));
    assert_route(&router, Method::PUT, &admin_path("/v3/config"));
    assert_route(&router, Method::GET, &admin_path("/v3/scanner/status"));

    assert_route(&router, Method::GET, &table_catalog_path("/config"));
    assert_route(&router, Method::PUT, &table_catalog_path("/buckets/analytics"));
    assert_route(&router, Method::GET, &table_catalog_path("/buckets/analytics"));
    assert_route(&router, Method::GET, &table_catalog_path("/analytics/namespaces"));
    assert_route(&router, Method::POST, &table_catalog_path("/analytics/namespaces"));
    assert_route(&router, Method::GET, &table_catalog_path("/analytics/namespaces/sales"));
    assert_route(&router, Method::DELETE, &table_catalog_path("/analytics/namespaces/sales"));
    assert_route(&router, Method::GET, &table_catalog_path("/analytics/namespaces/sales/tables"));
    assert_route(&router, Method::POST, &table_catalog_path("/analytics/namespaces/sales/tables"));
    assert_route(&router, Method::POST, &table_catalog_path("/analytics/namespaces/sales/register"));
    assert_route(&router, Method::GET, &table_catalog_path("/analytics/namespaces/sales/views"));
    assert_route(&router, Method::POST, &table_catalog_path("/analytics/namespaces/sales/views"));
    assert_route(&router, Method::GET, &table_catalog_path("/analytics/namespaces/sales/tables/orders"));
    assert_route(
        &router,
        Method::GET,
        &table_catalog_path("/analytics/namespaces/sales/tables/orders/credentials"),
    );
    assert_route(&router, Method::POST, &table_catalog_path("/analytics/namespaces/sales/tables/orders"));
    assert_route(&router, Method::DELETE, &table_catalog_path("/analytics/namespaces/sales/tables/orders"));
    assert_route(
        &router,
        Method::GET,
        &table_catalog_path("/analytics/namespaces/sales/views/recent_orders"),
    );
    assert_route(
        &router,
        Method::HEAD,
        &table_catalog_path("/analytics/namespaces/sales/views/recent_orders"),
    );
    assert_route(
        &router,
        Method::POST,
        &table_catalog_path("/analytics/namespaces/sales/views/recent_orders"),
    );
    assert_route(
        &router,
        Method::DELETE,
        &table_catalog_path("/analytics/namespaces/sales/views/recent_orders"),
    );
    assert_route(
        &router,
        Method::GET,
        &table_catalog_path("/analytics/namespaces/sales/tables/orders/refs"),
    );
    assert_route(
        &router,
        Method::PUT,
        &table_catalog_path("/analytics/namespaces/sales/tables/orders/refs/audit"),
    );
    assert_route(
        &router,
        Method::DELETE,
        &table_catalog_path("/analytics/namespaces/sales/tables/orders/refs/audit"),
    );
    assert_route(
        &router,
        Method::POST,
        &table_catalog_path("/analytics/namespaces/sales/tables/orders/maintenance/metadata"),
    );
    assert_route(
        &router,
        Method::GET,
        &table_catalog_path("/analytics/namespaces/sales/tables/orders/metadata-location"),
    );
    assert_route(
        &router,
        Method::PUT,
        &table_catalog_path("/analytics/namespaces/sales/tables/orders/metadata-location"),
    );
    assert_route(
        &router,
        Method::PUT,
        &table_catalog_path("/analytics/namespaces/sales/tables/orders/maintenance/config"),
    );
    assert_route(
        &router,
        Method::GET,
        &table_catalog_path("/analytics/namespaces/sales/tables/orders/maintenance/config"),
    );
    assert_route(
        &router,
        Method::GET,
        &table_catalog_path("/analytics/namespaces/sales/tables/orders/maintenance/jobs/job-1"),
    );
    assert_route(
        &router,
        Method::POST,
        &table_catalog_path("/analytics/namespaces/sales/tables/orders/maintenance/worker/run"),
    );
    assert_route(
        &router,
        Method::POST,
        &table_catalog_path("/analytics/namespaces/sales/tables/orders/maintenance/jobs/job-1/heartbeat"),
    );
    assert_route(
        &router,
        Method::GET,
        &table_catalog_path("/analytics/namespaces/sales/tables/orders/catalog/export"),
    );
    assert_route(
        &router,
        Method::POST,
        &table_catalog_path("/analytics/namespaces/sales/tables/orders/catalog/import"),
    );
    assert_route(
        &router,
        Method::GET,
        &table_catalog_path("/analytics/namespaces/sales/tables/orders/catalog/external"),
    );
    assert_route(
        &router,
        Method::PUT,
        &table_catalog_path("/analytics/namespaces/sales/tables/orders/catalog/external"),
    );
    assert_route(
        &router,
        Method::POST,
        &table_catalog_path("/analytics/namespaces/sales/tables/orders/catalog/external/sync"),
    );
    assert_route(
        &router,
        Method::GET,
        &table_catalog_path("/analytics/namespaces/sales/tables/orders/catalog/diagnostics"),
    );
    assert_route(
        &router,
        Method::POST,
        &table_catalog_path("/analytics/namespaces/sales/tables/orders/catalog/recovery"),
    );
    assert_route(
        &router,
        Method::POST,
        &table_catalog_path("/analytics/namespaces/sales/tables/orders/catalog/rollback"),
    );
    assert_route(&router, Method::GET, &compat_table_catalog_path("/config"));
    assert_route(&router, Method::PUT, &compat_table_catalog_path("/buckets/analytics"));
    assert_route(&router, Method::GET, &compat_table_catalog_path("/buckets/analytics"));
    assert_route(&router, Method::GET, &compat_table_catalog_path("/analytics/namespaces"));
    assert_route(&router, Method::POST, &compat_table_catalog_path("/analytics/namespaces"));
    assert_route(&router, Method::GET, &compat_table_catalog_path("/analytics/namespaces/sales"));
    assert_route(&router, Method::DELETE, &compat_table_catalog_path("/analytics/namespaces/sales"));
    assert_route(&router, Method::GET, &compat_table_catalog_path("/analytics/namespaces/sales/tables"));
    assert_route(&router, Method::POST, &compat_table_catalog_path("/analytics/namespaces/sales/tables"));
    assert_route(&router, Method::POST, &compat_table_catalog_path("/analytics/namespaces/sales/register"));
    assert_route(&router, Method::GET, &compat_table_catalog_path("/analytics/namespaces/sales/views"));
    assert_route(&router, Method::POST, &compat_table_catalog_path("/analytics/namespaces/sales/views"));
    assert_route(
        &router,
        Method::GET,
        &compat_table_catalog_path("/analytics/namespaces/sales/tables/orders"),
    );
    assert_route(
        &router,
        Method::GET,
        &compat_table_catalog_path("/analytics/namespaces/sales/tables/orders/credentials"),
    );
    assert_route(
        &router,
        Method::POST,
        &compat_table_catalog_path("/analytics/namespaces/sales/tables/orders"),
    );
    assert_route(
        &router,
        Method::DELETE,
        &compat_table_catalog_path("/analytics/namespaces/sales/tables/orders"),
    );
    assert_route(
        &router,
        Method::GET,
        &compat_table_catalog_path("/analytics/namespaces/sales/views/recent_orders"),
    );
    assert_route(
        &router,
        Method::HEAD,
        &compat_table_catalog_path("/analytics/namespaces/sales/views/recent_orders"),
    );
    assert_route(
        &router,
        Method::POST,
        &compat_table_catalog_path("/analytics/namespaces/sales/views/recent_orders"),
    );
    assert_route(
        &router,
        Method::DELETE,
        &compat_table_catalog_path("/analytics/namespaces/sales/views/recent_orders"),
    );
    assert_route(
        &router,
        Method::GET,
        &compat_table_catalog_path("/analytics/namespaces/sales/tables/orders/refs"),
    );
    assert_route(
        &router,
        Method::PUT,
        &compat_table_catalog_path("/analytics/namespaces/sales/tables/orders/refs/audit"),
    );
    assert_route(
        &router,
        Method::DELETE,
        &compat_table_catalog_path("/analytics/namespaces/sales/tables/orders/refs/audit"),
    );
    assert_route(
        &router,
        Method::POST,
        &compat_table_catalog_path("/analytics/namespaces/sales/tables/orders/maintenance/metadata"),
    );
    assert_route(
        &router,
        Method::GET,
        &compat_table_catalog_path("/analytics/namespaces/sales/tables/orders/metadata-location"),
    );
    assert_route(
        &router,
        Method::PUT,
        &compat_table_catalog_path("/analytics/namespaces/sales/tables/orders/metadata-location"),
    );
    assert_route(
        &router,
        Method::PUT,
        &compat_table_catalog_path("/analytics/namespaces/sales/tables/orders/maintenance/config"),
    );
    assert_route(
        &router,
        Method::GET,
        &compat_table_catalog_path("/analytics/namespaces/sales/tables/orders/maintenance/config"),
    );
    assert_route(
        &router,
        Method::GET,
        &compat_table_catalog_path("/analytics/namespaces/sales/tables/orders/maintenance/jobs/job-1"),
    );
    assert_route(
        &router,
        Method::POST,
        &compat_table_catalog_path("/analytics/namespaces/sales/tables/orders/maintenance/worker/run"),
    );
    assert_route(
        &router,
        Method::POST,
        &compat_table_catalog_path("/analytics/namespaces/sales/tables/orders/maintenance/jobs/job-1/heartbeat"),
    );
    assert_route(
        &router,
        Method::GET,
        &compat_table_catalog_path("/analytics/namespaces/sales/tables/orders/catalog/export"),
    );
    assert_route(
        &router,
        Method::POST,
        &compat_table_catalog_path("/analytics/namespaces/sales/tables/orders/catalog/import"),
    );
    assert_route(
        &router,
        Method::GET,
        &compat_table_catalog_path("/analytics/namespaces/sales/tables/orders/catalog/external"),
    );
    assert_route(
        &router,
        Method::PUT,
        &compat_table_catalog_path("/analytics/namespaces/sales/tables/orders/catalog/external"),
    );
    assert_route(
        &router,
        Method::POST,
        &compat_table_catalog_path("/analytics/namespaces/sales/tables/orders/catalog/external/sync"),
    );
    assert_route(
        &router,
        Method::GET,
        &compat_table_catalog_path("/analytics/namespaces/sales/tables/orders/catalog/diagnostics"),
    );
    assert_route(
        &router,
        Method::POST,
        &compat_table_catalog_path("/analytics/namespaces/sales/tables/orders/catalog/recovery"),
    );
    assert_route(
        &router,
        Method::POST,
        &compat_table_catalog_path("/analytics/namespaces/sales/tables/orders/catalog/rollback"),
    );

    assert_route(&router, Method::POST, &admin_path("/v3/service"));
    assert_route(&router, Method::GET, &admin_path("/v3/info"));
    assert_route(&router, Method::GET, &admin_path("/v3/storageinfo"));
    assert_route(&router, Method::GET, &admin_path("/v3/metrics"));

    assert_route(&router, Method::GET, &admin_path("/v3/pools/list"));
    assert_route(&router, Method::POST, &admin_path("/v3/rebalance/start"));
    assert_route(&router, Method::GET, &admin_path("/v3/rebalance/status"));
    assert_route(&router, Method::POST, &admin_path("/v3/heal/"));
    assert_route(&router, Method::POST, &admin_path("/v3/heal/test-bucket"));
    assert_route(&router, Method::POST, &admin_path("/v3/heal/test-bucket/prefix"));
    assert_route(&router, Method::POST, &admin_path("/v3/background-heal/status"));

    assert_route(&router, Method::GET, &admin_path("/v3/tier"));
    assert_route(&router, Method::GET, &admin_path("/v3/tier/HOT"));
    assert_route(&router, Method::POST, &admin_path("/v3/tier/clear"));
    assert_route(&router, Method::PUT, &admin_path("/v3/set-bucket-quota"));
    assert_route(&router, Method::GET, &admin_path("/v3/get-bucket-quota"));
    assert_route(&router, Method::PUT, &admin_path("/v3/quota/test-bucket"));
    assert_route(&router, Method::GET, &admin_path("/v3/quota-stats/test-bucket"));

    assert_route(&router, Method::GET, &admin_path("/export-bucket-metadata"));
    assert_route(&router, Method::GET, &admin_path("/v3/export-bucket-metadata"));
    assert_route(&router, Method::PUT, &admin_path("/import-bucket-metadata"));
    assert_route(&router, Method::PUT, &admin_path("/v3/import-bucket-metadata"));
    assert_route(&router, Method::GET, &admin_path("/v3/list-remote-targets"));
    assert_route(&router, Method::PUT, &admin_path("/v3/set-remote-target"));
    assert_route(&router, Method::PUT, &admin_path("/v3/site-replication/add"));
    assert_route(&router, Method::PUT, &admin_path("/v3/site-replication/remove"));
    assert_route(&router, Method::GET, &admin_path("/v3/site-replication/info"));
    assert_route(&router, Method::GET, &admin_path("/v3/site-replication/metainfo"));
    assert_route(&router, Method::GET, &admin_path("/v3/site-replication/status"));
    assert_route(&router, Method::POST, &admin_path("/v3/site-replication/devnull"));
    assert_route(&router, Method::POST, &admin_path("/v3/site-replication/netperf"));
    assert_route(&router, Method::POST, &admin_path("/v3/site-replication/rotate-svc-acct"));
    assert_route(&router, Method::PUT, &admin_path("/v3/site-replication/peer/join"));
    assert_route(&router, Method::PUT, &admin_path("/v3/site-replication/peer/bucket-ops"));
    assert_route(&router, Method::PUT, &admin_path("/v3/site-replication/peer/iam-item"));
    assert_route(&router, Method::PUT, &admin_path("/v3/site-replication/peer/bucket-meta"));
    assert_route(&router, Method::GET, &admin_path("/v3/site-replication/peer/idp-settings"));
    assert_route(&router, Method::PUT, &admin_path("/v3/site-replication/edit"));
    assert_route(&router, Method::PUT, &admin_path("/v3/site-replication/peer/edit"));
    assert_route(&router, Method::PUT, &admin_path("/v3/site-replication/peer/remove"));
    assert_route(&router, Method::PUT, &admin_path("/v3/site-replication/resync/op"));
    assert_route(&router, Method::PUT, &admin_path("/v3/site-replication/state/edit"));
    assert_route(&router, Method::GET, &admin_path("/debug/pprof/profile"));
    assert_route(&router, Method::GET, &admin_path("/debug/tls/status"));

    assert_route(&router, Method::POST, &admin_path("/v3/kms/create-key"));
    assert_route(&router, Method::POST, &admin_path("/v3/kms/key/create"));
    assert_route(&router, Method::POST, &admin_path("/v3/kms/configure"));
    assert_route(&router, Method::GET, &admin_path("/v3/kms/status"));
    assert_route(&router, Method::POST, &admin_path("/v3/kms/status"));
    assert_route(&router, Method::GET, &admin_path("/v3/kms/key/status"));
    assert_route(&router, Method::POST, &admin_path("/v3/kms/keys"));
    assert_route(&router, Method::GET, &admin_path("/v3/kms/keys"));
    assert_route(&router, Method::GET, &admin_path("/v3/kms/keys/test-key"));
    assert_route(&router, Method::GET, &admin_path("/v3/oidc/providers"));
    assert_route(&router, Method::GET, &admin_path("/v3/oidc/config"));
    assert_route(&router, Method::PUT, &admin_path("/v3/oidc/config/default"));
    assert_route(&router, Method::DELETE, &admin_path("/v3/oidc/config/default"));
    assert_route(&router, Method::POST, &admin_path("/v3/oidc/validate"));
    assert_route(&router, Method::GET, &admin_path("/v3/oidc/authorize/default"));
    assert_route(&router, Method::GET, &admin_path("/v3/oidc/callback/default"));
    assert_route(&router, Method::GET, &admin_path("/v3/oidc/logout"));

    assert!(
        !router.contains_route(Method::GET, "/rustfs/rpc/read_file_stream"),
        "internode rpc routes should no longer be registered inside the admin router"
    );
}

#[test]
#[serial]
fn test_admin_alias_paths_match_existing_admin_routes() {
    let router = registered_admin_router();

    for (method, path) in [
        (Method::GET, compat_admin_alias_path("/v3/is-admin")),
        (Method::GET, compat_admin_alias_path("/v3/info")),
        (Method::GET, compat_admin_alias_path("/v3/storageinfo")),
        (Method::GET, compat_admin_alias_path("/v3/pools/list")),
        (Method::PUT, compat_admin_alias_path("/v3/add-service-account")),
        (Method::GET, compat_admin_alias_path("/v3/temporary-account-info")),
        (Method::GET, compat_admin_alias_path("/v3/info-access-key")),
        (Method::GET, compat_admin_alias_path("/v3/list-access-keys-bulk")),
        (Method::PUT, compat_admin_alias_path("/v3/set-policy")),
        (Method::PUT, compat_admin_alias_path("/v3/set-bucket-quota")),
        (Method::GET, compat_admin_alias_path("/v3/get-bucket-quota")),
        (Method::GET, compat_admin_alias_path("/v3/audit/target/list")),
        (Method::GET, compat_admin_alias_path("/v3/module-switches")),
        (Method::PUT, compat_admin_alias_path("/v3/module-switches")),
        (Method::PUT, compat_admin_alias_path("/v3/audit/target/audit_webhook/test-audit")),
        (Method::DELETE, compat_admin_alias_path("/v3/audit/target/audit_webhook/test-audit/reset")),
        (Method::POST, compat_admin_alias_path("/v3/heal/")),
        (Method::POST, compat_admin_alias_path("/v3/heal/test-bucket")),
        (Method::POST, compat_admin_alias_path("/v3/heal/test-bucket/prefix")),
        (Method::POST, compat_admin_alias_path("/v3/background-heal/status")),
        (Method::GET, compat_admin_alias_path("/v3/tier/HOT")),
        (Method::GET, compat_admin_alias_path("/v3/export-bucket-metadata")),
        (Method::PUT, compat_admin_alias_path("/v3/import-bucket-metadata")),
        (Method::POST, compat_admin_alias_path("/v3/idp/builtin/policy/attach")),
        (Method::POST, compat_admin_alias_path("/v3/idp/builtin/policy/detach")),
        (Method::GET, compat_admin_alias_path("/v3/idp/builtin/policy-entities")),
        (Method::POST, compat_admin_alias_path("/v3/rebalance/start")),
        (Method::GET, compat_admin_alias_path("/v3/oidc/providers")),
        (Method::GET, compat_admin_alias_path("/v3/oidc/authorize/default")),
        (Method::GET, compat_admin_alias_path("/v3/oidc/callback/default")),
        (Method::GET, compat_admin_alias_path("/v3/oidc/logout")),
        (Method::GET, compat_admin_alias_path("/v3/oidc/config")),
        (Method::PUT, compat_admin_alias_path("/v3/oidc/config/default")),
        (Method::PUT, compat_admin_alias_path("/v3/site-replication/add")),
        (Method::GET, compat_admin_alias_path("/v3/site-replication/info")),
        (Method::GET, compat_admin_alias_path("/v3/site-replication/status")),
        (Method::PUT, compat_admin_alias_path("/v3/site-replication/peer/join")),
        (Method::GET, compat_admin_alias_path("/export-bucket-metadata")),
        (Method::GET, compat_admin_alias_path("/v3/export-bucket-metadata")),
        (Method::PUT, compat_admin_alias_path("/import-bucket-metadata")),
        (Method::PUT, compat_admin_alias_path("/v3/import-bucket-metadata")),
        (Method::POST, compat_admin_alias_path("/v3/kms/key/create")),
        (Method::GET, compat_admin_alias_path("/v3/kms/keys/test-key")),
        (Method::GET, compat_admin_alias_path("/v3/kms/status")),
        (Method::POST, compat_admin_alias_path("/v3/kms/status")),
        (Method::GET, compat_admin_alias_path("/v3/kms/key/status")),
        (Method::GET, compat_admin_alias_path("/v3/get-config-kv")),
        (Method::PUT, compat_admin_alias_path("/v3/set-config-kv")),
        (Method::DELETE, compat_admin_alias_path("/v3/del-config-kv")),
        (Method::GET, compat_admin_alias_path("/v3/help-config-kv")),
        (Method::GET, compat_admin_alias_path("/v3/list-config-history-kv")),
        (Method::DELETE, compat_admin_alias_path("/v3/clear-config-history-kv")),
        (Method::PUT, compat_admin_alias_path("/v3/restore-config-history-kv")),
        (Method::GET, compat_admin_alias_path("/v3/config")),
        (Method::PUT, compat_admin_alias_path("/v3/config")),
        (Method::GET, compat_admin_alias_path("/v3/scanner/status")),
    ] {
        assert!(
            router.contains_compatible_route(method.clone(), &path),
            "expected MinIO admin alias path to match: {} {}",
            method.as_str(),
            path
        );
    }
}

#[test]
#[serial]
fn test_health_routes_not_registered_when_disabled_by_env() {
    with_var(rustfs_config::ENV_HEALTH_ENDPOINT_ENABLE, Some("false"), || {
        let mut router: S3Router<AdminOperation> = S3Router::new(false);
        health::register_health_route(&mut router).expect("register health route");

        assert!(
            !router.contains_route(Method::GET, HEALTH_PREFIX),
            "GET /health must not be registered when health endpoint is disabled"
        );
        assert!(
            !router.contains_route(Method::HEAD, HEALTH_PREFIX),
            "HEAD /health must not be registered when health endpoint is disabled"
        );
        assert!(
            !router.contains_route(Method::GET, HEALTH_READY_PATH),
            "GET /health/ready must not be registered when health endpoint is disabled"
        );
        assert!(
            !router.contains_route(Method::HEAD, HEALTH_READY_PATH),
            "HEAD /health/ready must not be registered when health endpoint is disabled"
        );
        assert!(
            router.contains_route(Method::GET, PROFILE_CPU_PATH),
            "GET /profile/cpu must stay registered when health endpoint is disabled"
        );
        assert!(
            router.contains_route(Method::GET, PROFILE_MEMORY_PATH),
            "GET /profile/memory must stay registered when health endpoint is disabled"
        );
    });
}

#[test]
fn test_phase5_admin_info_contract() {
    let system_src = include_str!("handlers/system.rs");

    let server_info_impl_marker = "impl Operation for ServerInfoHandler";
    let server_info_impl_start = system_src
        .find(server_info_impl_marker)
        .expect("Expected impl Operation for ServerInfoHandler in handlers/system.rs");
    let server_info_impl_block = &system_src[server_info_impl_start..];

    assert!(
        server_info_impl_block.contains("DefaultAdminUsecase::from_global()")
            && server_info_impl_block.contains("execute_query_server_info(QueryServerInfoRequest { include_pools: true })"),
        "admin server info path must be served through DefaultAdminUsecase::execute_query_server_info"
    );
}

fn extract_block_between_markers<'a>(src: &'a str, start_marker: &str, end_marker: &str) -> &'a str {
    let start = src
        .find(start_marker)
        .unwrap_or_else(|| panic!("Expected marker `{}` in source", start_marker));
    let after_start = &src[start..];
    let end = after_start
        .find(end_marker)
        .unwrap_or_else(|| panic!("Expected end marker `{}` in source", end_marker));
    &after_start[..end]
}

#[test]
fn test_replication_set_remote_target_compat_contract() {
    let replication_src = include_str!("handlers/replication.rs");
    let handler_block = extract_block_between_markers(
        replication_src,
        "impl Operation for SetRemoteTargetHandler",
        "pub struct ListRemoteTargetHandler",
    );

    assert!(
        handler_block.contains("read_compatible_admin_body("),
        "set-remote-target must decode MinIO-compatible encrypted admin payloads"
    );

    assert!(
        handler_block.contains("Body::from(arn_str)"),
        "set-remote-target must keep ARN success responses as plain JSON string body"
    );

    assert!(
        !handler_block.contains("encode_compatible_admin_payload("),
        "set-remote-target should not re-encrypt ARN success responses"
    );
}
