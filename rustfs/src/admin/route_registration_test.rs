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
    handlers::{bucket_meta, heal, health, kms, pools, profile_admin, quota, rebalance, replication, sts, system, tier, user},
    router::{AdminOperation, S3Router},
    rpc,
};
use crate::server::{ADMIN_PREFIX, HEALTH_PREFIX, HEALTH_READY_PATH, PROFILE_CPU_PATH, PROFILE_MEMORY_PATH};
use hyper::Method;

fn admin_path(path: &str) -> String {
    format!("{}{}", ADMIN_PREFIX, path)
}

fn assert_route(router: &S3Router<AdminOperation>, method: Method, path: &str) {
    assert!(
        router.contains_route(method.clone(), path),
        "expected route missing: {} {}",
        method.as_str(),
        path
    );
}

#[test]
fn test_register_routes_cover_representative_admin_paths() {
    let mut router: S3Router<AdminOperation> = S3Router::new(false);

    health::register_health_route(&mut router).expect("register health route");
    sts::register_admin_auth_route(&mut router).expect("register sts route");
    user::register_user_route(&mut router).expect("register user route");
    system::register_system_route(&mut router).expect("register system route");
    pools::register_pool_route(&mut router).expect("register pool route");
    rebalance::register_rebalance_route(&mut router).expect("register rebalance route");
    heal::register_heal_route(&mut router).expect("register heal route");
    tier::register_tier_route(&mut router).expect("register tier route");
    quota::register_quota_route(&mut router).expect("register quota route");
    bucket_meta::register_bucket_meta_route(&mut router).expect("register bucket meta route");
    replication::register_replication_route(&mut router).expect("register replication route");
    profile_admin::register_profiling_route(&mut router).expect("register profile route");
    kms::register_kms_route(&mut router).expect("register kms route");
    rpc::register_rpc_route(&mut router).expect("register rpc route");

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
    assert_route(&router, Method::GET, &admin_path("/v3/export-iam"));
    assert_route(&router, Method::PUT, &admin_path("/v3/import-iam"));
    assert_route(&router, Method::GET, &admin_path("/v3/list-canned-policies"));
    assert_route(&router, Method::GET, &admin_path("/v3/target/list"));
    assert_route(&router, Method::GET, &admin_path("/v3/accountinfo"));

    assert_route(&router, Method::POST, &admin_path("/v3/service"));
    assert_route(&router, Method::GET, &admin_path("/v3/info"));
    assert_route(&router, Method::GET, &admin_path("/v3/storageinfo"));
    assert_route(&router, Method::GET, &admin_path("/v3/metrics"));

    assert_route(&router, Method::GET, &admin_path("/v3/pools/list"));
    assert_route(&router, Method::POST, &admin_path("/v3/rebalance/start"));
    assert_route(&router, Method::GET, &admin_path("/v3/rebalance/status"));
    assert_route(&router, Method::POST, &admin_path("/v3/heal/test-bucket"));
    assert_route(&router, Method::POST, &admin_path("/v3/heal/test-bucket/prefix"));

    assert_route(&router, Method::GET, &admin_path("/v3/tier"));
    assert_route(&router, Method::POST, &admin_path("/v3/tier/clear"));
    assert_route(&router, Method::PUT, &admin_path("/v3/quota/test-bucket"));
    assert_route(&router, Method::GET, &admin_path("/v3/quota-stats/test-bucket"));

    assert_route(&router, Method::GET, &admin_path("/export-bucket-metadata"));
    assert_route(&router, Method::PUT, &admin_path("/import-bucket-metadata"));
    assert_route(&router, Method::GET, &admin_path("/v3/list-remote-targets"));
    assert_route(&router, Method::PUT, &admin_path("/v3/set-remote-target"));
    assert_route(&router, Method::GET, &admin_path("/debug/pprof/profile"));

    assert_route(&router, Method::POST, &admin_path("/v3/kms/create-key"));
    assert_route(&router, Method::POST, &admin_path("/v3/kms/configure"));
    assert_route(&router, Method::POST, &admin_path("/v3/kms/keys"));
    assert_route(&router, Method::GET, &admin_path("/v3/kms/keys"));
    assert_route(&router, Method::GET, &admin_path("/v3/kms/keys/test-key"));
    assert_route(&router, Method::GET, "/rustfs/rpc/read_file_stream");
    assert_route(&router, Method::HEAD, "/rustfs/rpc/read_file_stream");
}

#[test]
fn test_phase5_admin_info_and_rpc_read_file_contract() {
    let system_src = include_str!("handlers/system.rs");
    let rpc_src = include_str!("rpc.rs");

    assert!(
        system_src.contains("DefaultAdminUsecase::from_global()")
            && system_src.contains("execute_query_server_info(QueryServerInfoRequest { include_pools: true })"),
        "admin server info path must be served through DefaultAdminUsecase::execute_query_server_info"
    );

    assert!(
        rpc_src.contains("format!(\"{}{}\", RPC_PREFIX, \"/read_file_stream\")")
            && rpc_src.contains(".read_file_stream(&query.volume, &query.path, query.offset, query.length)"),
        "rpc read_file_stream route must remain wired to disk.read_file_stream"
    );
}
