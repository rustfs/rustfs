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
    handlers::{
        audit, bucket_meta, heal, health, kms, module_switch, oidc, plugins_catalog, pools, profile_admin, quota, rebalance,
        replication, site_replication, sts, system, tier, user,
    },
    router::{AdminOperation, S3Router},
};
use crate::server::{ADMIN_PREFIX, HEALTH_PREFIX, HEALTH_READY_PATH, MINIO_ADMIN_PREFIX, PROFILE_CPU_PATH, PROFILE_MEMORY_PATH};
use hyper::Method;
use serial_test::serial;
use temp_env::with_var;

fn admin_path(path: &str) -> String {
    format!("{}{}", ADMIN_PREFIX, path)
}

fn compat_admin_alias_path(path: &str) -> String {
    format!("{}{}", MINIO_ADMIN_PREFIX, path)
}

fn assert_route(router: &S3Router<AdminOperation>, method: Method, path: &str) {
    assert!(
        router.contains_route(method.clone(), path),
        "expected route missing: {} {}",
        method.as_str(),
        path
    );
}

fn register_admin_routes(router: &mut S3Router<AdminOperation>) {
    health::register_health_route(router).expect("register health route");
    sts::register_admin_auth_route(router).expect("register sts route");
    user::register_user_route(router).expect("register user route");
    system::register_system_route(router).expect("register system route");
    pools::register_pool_route(router).expect("register pool route");
    rebalance::register_rebalance_route(router).expect("register rebalance route");
    heal::register_heal_route(router).expect("register heal route");
    tier::register_tier_route(router).expect("register tier route");
    quota::register_quota_route(router).expect("register quota route");
    bucket_meta::register_bucket_meta_route(router).expect("register bucket meta route");
    audit::register_audit_target_route(router).expect("register audit target route");
    module_switch::register_module_switch_route(router).expect("register module switch route");
    plugins_catalog::register_plugin_catalog_route(router).expect("register plugin catalog route");
    replication::register_replication_route(router).expect("register replication route");
    site_replication::register_site_replication_route(router).expect("register site replication route");
    profile_admin::register_profiling_route(router).expect("register profile route");
    kms::register_kms_route(router).expect("register kms route");
    oidc::register_oidc_route(router).expect("register oidc route");
}

// register_admin_routes reads ENV_HEALTH_ENDPOINT_ENABLE to decide whether
// to register /health; serialise with the env-mutating test below to avoid
// cross-thread leakage of that override.
#[test]
#[serial]
fn test_register_routes_cover_representative_admin_paths() {
    let mut router: S3Router<AdminOperation> = S3Router::new(false);
    register_admin_routes(&mut router);
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
    assert_route(&router, Method::GET, &admin_path("/v4/plugins/catalog"));
    assert_route(&router, Method::PUT, &admin_path("/v3/audit/target/audit_webhook/test-audit"));
    assert_route(&router, Method::DELETE, &admin_path("/v3/audit/target/audit_webhook/test-audit/reset"));
    assert_route(&router, Method::GET, &admin_path("/v3/accountinfo"));

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
    let mut router: S3Router<AdminOperation> = S3Router::new(false);
    register_admin_routes(&mut router);

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
