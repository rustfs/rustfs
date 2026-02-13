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

mod auth;
pub mod console;
pub mod handlers;
pub mod router;
mod rpc;
pub mod utils;

#[cfg(test)]
mod console_test;

use handlers::{bucket_meta, heal, health, kms, pools, profile_admin, quota, rebalance, replication, sts, system, tier, user};
use router::{AdminOperation, S3Router};
use rpc::register_rpc_route;
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

    health::register_health_route(&mut r)?;
    sts::register_admin_auth_route(&mut r)?;

    register_rpc_route(&mut r)?;
    user::register_user_route(&mut r)?;
    system::register_system_route(&mut r)?;
    pools::register_pool_route(&mut r)?;
    rebalance::register_rebalance_route(&mut r)?;

    heal::register_heal_route(&mut r)?;

    tier::register_tier_route(&mut r)?;

    quota::register_quota_route(&mut r)?;
    bucket_meta::register_bucket_meta_route(&mut r)?;

    replication::register_replication_route(&mut r)?;
    profile_admin::register_profiling_route(&mut r)?;
    kms::register_kms_route(&mut r)?;

    Ok(r)
}
