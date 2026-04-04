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
        bucket_meta, config_admin, heal, health, kms, oidc, pools, profile_admin, quota, rebalance, replication,
        site_replication, sts, system, tier, user,
    },
    router::{AdminOperation, S3Router},
};
use crate::server::{ADMIN_PREFIX, HEALTH_PREFIX, HEALTH_READY_PATH, MINIO_ADMIN_PREFIX, PROFILE_CPU_PATH, PROFILE_MEMORY_PATH};
use hyper::Method;

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
    config_admin::register_config_route(router).expect("register config route");
    replication::register_replication_route(router).expect("register replication route");
    site_replication::register_site_replication_route(router).expect("register site replication route");
    profile_admin::register_profiling_route(router).expect("register profile route");
    kms::register_kms_route(router).expect("register kms route");
    oidc::register_oidc_route(router).expect("register oidc route");
}

#[test]
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

    assert!(
        !router.contains_route(Method::GET, "/rustfs/rpc/read_file_stream"),
        "internode rpc routes should no longer be registered inside the admin router"
    );
}

#[test]
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
        (Method::GET, compat_admin_alias_path("/v3/get-config-kv")),
        (Method::PUT, compat_admin_alias_path("/v3/set-config-kv")),
        (Method::DELETE, compat_admin_alias_path("/v3/del-config-kv")),
        (Method::GET, compat_admin_alias_path("/v3/help-config-kv")),
        (Method::GET, compat_admin_alias_path("/v3/list-config-history-kv")),
        (Method::DELETE, compat_admin_alias_path("/v3/clear-config-history-kv")),
        (Method::PUT, compat_admin_alias_path("/v3/restore-config-history-kv")),
        (Method::GET, compat_admin_alias_path("/v3/config")),
        (Method::PUT, compat_admin_alias_path("/v3/config")),
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

#[test]
fn test_config_admin_contracts_use_auth_and_compat_payloads() {
    let config_admin_src = include_str!("handlers/config_admin.rs");

    for handler in [
        "impl Operation for GetConfigKVHandler",
        "impl Operation for SetConfigKVHandler",
        "impl Operation for DelConfigKVHandler",
        "impl Operation for HelpConfigKVHandler",
        "impl Operation for GetConfigHandler",
        "impl Operation for SetConfigHandler",
    ] {
        let handler_block = &config_admin_src[config_admin_src
            .find(handler)
            .unwrap_or_else(|| panic!("Expected handler block `{handler}` in handlers/config_admin.rs"))..];
        assert!(
            handler_block.contains("validate_config_admin_request(&req).await?"),
            "{handler} must validate admin authorization"
        );
    }

    for handler in [
        "impl Operation for SetConfigKVHandler",
        "impl Operation for DelConfigKVHandler",
        "impl Operation for SetConfigHandler",
    ] {
        let handler_block = &config_admin_src[config_admin_src
            .find(handler)
            .unwrap_or_else(|| panic!("Expected handler block `{handler}` in handlers/config_admin.rs"))..];
        assert!(
            handler_block.contains("read_compatible_admin_body("),
            "{handler} must accept MinIO-compatible encrypted admin payloads"
        );
    }

    let get_config_block = &config_admin_src[config_admin_src
        .find("impl Operation for GetConfigKVHandler")
        .expect("Expected GetConfigKVHandler block in handlers/config_admin.rs")..];
    assert!(
        get_config_block.contains("load_active_server_config()?"),
        "GetConfigKVHandler must read the active runtime config snapshot"
    );

    let help_config_block = &config_admin_src[config_admin_src
        .find("impl Operation for HelpConfigKVHandler")
        .expect("Expected HelpConfigKVHandler block in handlers/config_admin.rs")..];
    assert!(
        help_config_block.contains("build_help_response("),
        "HelpConfigKVHandler must build MinIO-compatible help metadata responses"
    );

    let get_full_config_block = &config_admin_src[config_admin_src
        .find("impl Operation for GetConfigHandler")
        .expect("Expected GetConfigHandler block in handlers/config_admin.rs")..];
    assert!(
        get_full_config_block.contains("load_active_server_config()?"),
        "GetConfigHandler must export the active runtime config snapshot"
    );

    let set_kv_block = &config_admin_src[config_admin_src
        .find("impl Operation for SetConfigKVHandler")
        .expect("Expected SetConfigKVHandler block in handlers/config_admin.rs")..];
    assert!(
        set_kv_block.contains("apply_dynamic_config_for_subsystem(&config, sub_system).await?"),
        "SetConfigKVHandler must dynamically apply supported subsystems"
    );
    assert!(
        set_kv_block.contains("validate_config_directives(&directives)?;"),
        "SetConfigKVHandler must reject unsupported subsystems and keys"
    );
    assert!(
        set_kv_block.contains("validate_server_config(&config, sub_system).await?;"),
        "SetConfigKVHandler must validate config before persisting"
    );
    assert!(
        set_kv_block.contains("signal_dynamic_config_reload(sub_system).await;"),
        "SetConfigKVHandler must propagate dynamic config reloads to peers"
    );
    assert!(
        set_kv_block.contains("signal_config_snapshot_reload().await;"),
        "SetConfigKVHandler must refresh peer config snapshots for non-dynamic updates"
    );

    let del_kv_block = &config_admin_src[config_admin_src
        .find("impl Operation for DelConfigKVHandler")
        .expect("Expected DelConfigKVHandler block in handlers/config_admin.rs")..];
    assert!(
        del_kv_block.contains("apply_dynamic_config_for_subsystem(&config, sub_system).await?"),
        "DelConfigKVHandler must dynamically apply supported subsystems"
    );
    assert!(
        del_kv_block.contains("validate_config_directives(&directives)?;"),
        "DelConfigKVHandler must reject unsupported subsystems and keys"
    );
    assert!(
        del_kv_block.contains("validate_server_config(&config, sub_system).await?;"),
        "DelConfigKVHandler must validate config before persisting"
    );
    assert!(
        del_kv_block.contains("signal_dynamic_config_reload(sub_system).await;"),
        "DelConfigKVHandler must propagate dynamic config reloads to peers"
    );
    assert!(
        del_kv_block.contains("signal_config_snapshot_reload().await;"),
        "DelConfigKVHandler must refresh peer config snapshots for non-dynamic updates"
    );

    let list_history_block = &config_admin_src[config_admin_src
        .find("impl Operation for ListConfigHistoryKVHandler")
        .expect("Expected ListConfigHistoryKVHandler block in handlers/config_admin.rs")..];
    assert!(
        list_history_block.contains("list_server_config_history(true, Some(count)).await?"),
        "ListConfigHistoryKVHandler must read persisted history entries"
    );

    let clear_history_block = &config_admin_src[config_admin_src
        .find("impl Operation for ClearConfigHistoryKVHandler")
        .expect("Expected ClearConfigHistoryKVHandler block in handlers/config_admin.rs")..];
    assert!(
        clear_history_block.contains("delete_server_config_history("),
        "ClearConfigHistoryKVHandler must delete persisted history entries"
    );

    let restore_history_block = &config_admin_src[config_admin_src
        .find("impl Operation for RestoreConfigHistoryKVHandler")
        .expect("Expected RestoreConfigHistoryKVHandler block in handlers/config_admin.rs")..];
    assert!(
        restore_history_block.contains("read_server_config_history(restore_id).await?"),
        "RestoreConfigHistoryKVHandler must read persisted history entries"
    );
    assert!(
        restore_history_block.contains("apply_set_directives(&mut config, &directives);"),
        "RestoreConfigHistoryKVHandler must replay stored config directives"
    );
    assert!(
        restore_history_block.contains("validate_server_config(&config, None).await?;"),
        "RestoreConfigHistoryKVHandler must validate restored config before persisting"
    );
    assert!(
        restore_history_block.contains("signal_config_snapshot_reload().await;"),
        "RestoreConfigHistoryKVHandler must refresh peer config snapshots after restore"
    );
    assert!(
        !restore_history_block.contains("apply_dynamic_config_for_subsystem("),
        "RestoreConfigHistoryKVHandler must not dynamically apply config changes"
    );

    let set_full_config_block = &config_admin_src[config_admin_src
        .find("impl Operation for SetConfigHandler")
        .expect("Expected SetConfigHandler block in handlers/config_admin.rs")..];
    assert!(
        set_full_config_block.contains("validate_server_config(&config, None).await?;"),
        "SetConfigHandler must validate full-config imports before persisting"
    );
    assert!(
        set_full_config_block.contains("signal_config_snapshot_reload().await;"),
        "SetConfigHandler must refresh peer config snapshots after full-config import"
    );
    assert!(
        !set_full_config_block.contains("apply_dynamic_config_for_subsystem("),
        "SetConfigHandler must not dynamically apply full-config imports"
    );
}
