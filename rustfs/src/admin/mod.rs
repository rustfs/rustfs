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

pub(crate) mod access_key_identity;
mod auth;
pub mod console;
pub mod handlers;
mod plugin_contract;
// Contract inventory is validated by tests before later runtime integration.
#[allow(dead_code)]
pub(crate) mod route_policy;
pub mod router;
pub mod service;
pub mod site_replication_identity;
pub mod utils;

#[cfg(test)]
mod console_test;
#[cfg(test)]
mod route_registration_test;

use handlers::{
    audit, bucket_meta, config_admin, extensions, heal, health, kms, module_switch, object_zip_download, oidc, plugins_catalog,
    plugins_instances, pools, profile_admin, quota, rebalance, replication, scanner, site_replication, sts, system,
    table_catalog, tier, tls_debug, user,
};
use router::{AdminOperation, S3Router};
use s3s::route::S3Route;

/// Create admin router
///
/// # Arguments
/// * `console_enabled` - Whether the console is enabled
///
/// # Returns
/// An instance of S3Route for admin operations
pub fn make_admin_route(console_enabled: bool) -> std::io::Result<impl S3Route> {
    let mut r: S3Router<AdminOperation> = S3Router::new(console_enabled);
    register_admin_routes(&mut r)?;
    Ok(r)
}

fn register_admin_routes(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    health::register_health_route(r)?;
    sts::register_admin_auth_route(r)?;

    user::register_user_route(r)?;
    system::register_system_route(r)?;
    pools::register_pool_route(r)?;
    rebalance::register_rebalance_route(r)?;

    heal::register_heal_route(r)?;

    tier::register_tier_route(r)?;

    quota::register_quota_route(r)?;
    bucket_meta::register_bucket_meta_route(r)?;
    config_admin::register_config_route(r)?;
    scanner::register_scanner_route(r)?;
    audit::register_audit_target_route(r)?;
    module_switch::register_module_switch_route(r)?;
    extensions::register_extension_route(r)?;
    object_zip_download::register_object_zip_download_route(r)?;
    plugins_catalog::register_plugin_catalog_route(r)?;
    plugins_instances::register_plugin_instance_route(r)?;

    replication::register_replication_route(r)?;
    site_replication::register_site_replication_route(r)?;
    profile_admin::register_profiling_route(r)?;
    tls_debug::register_tls_debug_route(r)?;
    kms::register_kms_route(r)?;
    oidc::register_oidc_route(r)?;
    table_catalog::register_table_catalog_route(r)?;

    Ok(())
}
