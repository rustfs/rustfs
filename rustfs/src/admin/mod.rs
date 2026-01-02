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

use crate::server::{ADMIN_PREFIX, HEALTH_PREFIX, PROFILE_CPU_PATH, PROFILE_MEMORY_PATH};
use handlers::{
    GetReplicationMetricsHandler, HealthCheckHandler, IsAdminHandler, ListRemoteTargetHandler, RemoveRemoteTargetHandler,
    SetRemoteTargetHandler, bucket_meta,
    event::{ListNotificationTargets, ListTargetsArns, NotificationTarget, RemoveNotificationTarget},
    group, kms, kms_dynamic, kms_keys, policies, pools,
    profile::{TriggerProfileCPU, TriggerProfileMemory},
    rebalance,
    service_account::{AddServiceAccount, DeleteServiceAccount, InfoServiceAccount, ListServiceAccount, UpdateServiceAccount},
    sts, tier, user,
};
use hyper::Method;
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

    // Health check endpoint for monitoring and orchestration
    r.insert(Method::GET, HEALTH_PREFIX, AdminOperation(&HealthCheckHandler {}))?;
    r.insert(Method::HEAD, HEALTH_PREFIX, AdminOperation(&HealthCheckHandler {}))?;
    r.insert(Method::GET, PROFILE_CPU_PATH, AdminOperation(&TriggerProfileCPU {}))?;
    r.insert(Method::GET, PROFILE_MEMORY_PATH, AdminOperation(&TriggerProfileMemory {}))?;

    // 1
    r.insert(Method::POST, "/", AdminOperation(&sts::AssumeRoleHandle {}))?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/is-admin").as_str(),
        AdminOperation(&IsAdminHandler {}),
    )?;

    register_rpc_route(&mut r)?;
    register_user_route(&mut r)?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/service").as_str(),
        AdminOperation(&handlers::ServiceHandle {}),
    )?;
    // 1
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/info").as_str(),
        AdminOperation(&handlers::ServerInfoHandler {}),
    )?;
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/inspect-data").as_str(),
        AdminOperation(&handlers::InspectDataHandler {}),
    )?;
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/inspect-data").as_str(),
        AdminOperation(&handlers::InspectDataHandler {}),
    )?;
    // 1
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/storageinfo").as_str(),
        AdminOperation(&handlers::StorageInfoHandler {}),
    )?;
    // 1
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/datausageinfo").as_str(),
        AdminOperation(&handlers::DataUsageInfoHandler {}),
    )?;
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/metrics").as_str(),
        AdminOperation(&handlers::MetricsHandler {}),
    )?;

    // 1
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/pools/list").as_str(),
        AdminOperation(&pools::ListPools {}),
    )?;
    // 1
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/pools/status").as_str(),
        AdminOperation(&pools::StatusPool {}),
    )?;
    // todo
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/pools/decommission").as_str(),
        AdminOperation(&pools::StartDecommission {}),
    )?;
    // todo
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/pools/cancel").as_str(),
        AdminOperation(&pools::CancelDecommission {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/rebalance/start").as_str(),
        AdminOperation(&rebalance::RebalanceStart {}),
    )?;
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/rebalance/status").as_str(),
        AdminOperation(&rebalance::RebalanceStatus {}),
    )?;
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/rebalance/stop").as_str(),
        AdminOperation(&rebalance::RebalanceStop {}),
    )?;

    // Some APIs are only available in EC mode
    // if is_dist_erasure().await || is_erasure().await {
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/heal/{bucket}").as_str(),
        AdminOperation(&handlers::HealHandler {}),
    )?;
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/heal/{bucket}/{prefix}").as_str(),
        AdminOperation(&handlers::HealHandler {}),
    )?;
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/background-heal/status").as_str(),
        AdminOperation(&handlers::BackgroundHealStatusHandler {}),
    )?;

    // ?
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/tier").as_str(),
        AdminOperation(&tier::ListTiers {}),
    )?;
    // ?
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/tier-stats").as_str(),
        AdminOperation(&tier::GetTierInfo {}),
    )?;
    // ?force=xxx
    r.insert(
        Method::DELETE,
        format!("{}{}", ADMIN_PREFIX, "/v3/tier/{tiername}").as_str(),
        AdminOperation(&tier::RemoveTier {}),
    )?;
    // ?force=xxx
    // body: AddOrUpdateTierReq
    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/tier").as_str(),
        AdminOperation(&tier::AddTier {}),
    )?;
    // ?
    // body: AddOrUpdateTierReq
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/tier/{tiername}").as_str(),
        AdminOperation(&tier::EditTier {}),
    )?;
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/tier/clear").as_str(),
        AdminOperation(&tier::ClearTier {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/export-bucket-metadata").as_str(),
        AdminOperation(&bucket_meta::ExportBucketMetadata {}),
    )?;

    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/import-bucket-metadata").as_str(),
        AdminOperation(&bucket_meta::ImportBucketMetadata {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/list-remote-targets").as_str(),
        AdminOperation(&ListRemoteTargetHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/replicationmetrics").as_str(),
        AdminOperation(&GetReplicationMetricsHandler {}),
    )?;

    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/set-remote-target").as_str(),
        AdminOperation(&SetRemoteTargetHandler {}),
    )?;

    r.insert(
        Method::DELETE,
        format!("{}{}", ADMIN_PREFIX, "/v3/remove-remote-target").as_str(),
        AdminOperation(&RemoveRemoteTargetHandler {}),
    )?;

    // Performance profiling endpoints (available on all platforms, with platform-specific responses)
    #[cfg(not(target_os = "windows"))]
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/debug/pprof/profile").as_str(),
        AdminOperation(&handlers::ProfileHandler {}),
    )?;

    #[cfg(not(target_os = "windows"))]
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/debug/pprof/status").as_str(),
        AdminOperation(&handlers::ProfileStatusHandler {}),
    )?;

    // KMS management endpoints
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/create-key").as_str(),
        AdminOperation(&kms::CreateKeyHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/describe-key").as_str(),
        AdminOperation(&kms::DescribeKeyHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/list-keys").as_str(),
        AdminOperation(&kms::ListKeysHandler {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/generate-data-key").as_str(),
        AdminOperation(&kms::GenerateDataKeyHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/status").as_str(),
        AdminOperation(&kms::KmsStatusHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/config").as_str(),
        AdminOperation(&kms::KmsConfigHandler {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/clear-cache").as_str(),
        AdminOperation(&kms::KmsClearCacheHandler {}),
    )?;

    // KMS Dynamic Configuration APIs
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/configure").as_str(),
        AdminOperation(&kms_dynamic::ConfigureKmsHandler {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/start").as_str(),
        AdminOperation(&kms_dynamic::StartKmsHandler {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/stop").as_str(),
        AdminOperation(&kms_dynamic::StopKmsHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/service-status").as_str(),
        AdminOperation(&kms_dynamic::GetKmsStatusHandler {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/reconfigure").as_str(),
        AdminOperation(&kms_dynamic::ReconfigureKmsHandler {}),
    )?;

    // KMS key management endpoints
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/keys").as_str(),
        AdminOperation(&kms_keys::CreateKmsKeyHandler {}),
    )?;

    r.insert(
        Method::DELETE,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/keys/delete").as_str(),
        AdminOperation(&kms_keys::DeleteKmsKeyHandler {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/keys/cancel-deletion").as_str(),
        AdminOperation(&kms_keys::CancelKmsKeyDeletionHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/keys").as_str(),
        AdminOperation(&kms_keys::ListKmsKeysHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/keys/{key_id}").as_str(),
        AdminOperation(&kms_keys::DescribeKmsKeyHandler {}),
    )?;

    Ok(r)
}

/// user router
fn register_user_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    // 1
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/accountinfo").as_str(),
        AdminOperation(&handlers::AccountInfoHandler {}),
    )?;

    // ?[bucket=xxx]
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/list-users").as_str(),
        AdminOperation(&user::ListUsers {}),
    )?;

    // ?accessKey=xxx
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/user-info").as_str(),
        AdminOperation(&user::GetUserInfo {}),
    )?;

    // ?accessKey=xxx
    r.insert(
        Method::DELETE,
        format!("{}{}", ADMIN_PREFIX, "/v3/remove-user").as_str(),
        AdminOperation(&user::RemoveUser {}),
    )?;

    // ?accessKey=xxx
    // body: AddOrUpdateUserReq
    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/add-user").as_str(),
        AdminOperation(&user::AddUser {}),
    )?;
    // ?accessKey=xxx&status=enabled
    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/set-user-status").as_str(),
        AdminOperation(&user::SetUserStatus {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/groups").as_str(),
        AdminOperation(&group::ListGroups {}),
    )?;

    // ?group=xxx
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/group").as_str(),
        AdminOperation(&group::GetGroup {}),
    )?;

    // ?group=xxx&status=xxx
    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/set-group-status").as_str(),
        AdminOperation(&group::SetGroupStatus {}),
    )?;

    // @body GroupAddRemove
    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/update-group-members").as_str(),
        AdminOperation(&group::UpdateGroupMembers {}),
    )?;

    // Service accounts
    // ?accessKey=xxx
    // @body: UpdateServiceAccountReq
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/update-service-account").as_str(),
        AdminOperation(&UpdateServiceAccount {}),
    )?;
    // ?accessKey=xxx
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/info-service-account").as_str(),
        AdminOperation(&InfoServiceAccount {}),
    )?;

    // ?[user=xxx]
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/list-service-accounts").as_str(),
        AdminOperation(&ListServiceAccount {}),
    )?;
    // ?accessKey=xxx
    r.insert(
        Method::DELETE,
        format!("{}{}", ADMIN_PREFIX, "/v3/delete-service-accounts").as_str(),
        AdminOperation(&DeleteServiceAccount {}),
    )?;
    // @body: AddServiceAccountReq
    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/add-service-accounts").as_str(),
        AdminOperation(&AddServiceAccount {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/export-iam").as_str(),
        AdminOperation(&user::ExportIam {}),
    )?;

    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/import-iam").as_str(),
        AdminOperation(&user::ImportIam {}),
    )?;

    // list-canned-policies?bucket=xxx
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/list-canned-policies").as_str(),
        AdminOperation(&policies::ListCannedPolicies {}),
    )?;

    // info-canned-policy?name=xxx
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/info-canned-policy").as_str(),
        AdminOperation(&policies::InfoCannedPolicy {}),
    )?;

    // add-canned-policy?name=xxx
    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/add-canned-policy").as_str(),
        AdminOperation(&policies::AddCannedPolicy {}),
    )?;

    // remove-canned-policy?name=xxx
    r.insert(
        Method::DELETE,
        format!("{}{}", ADMIN_PREFIX, "/v3/remove-canned-policy").as_str(),
        AdminOperation(&policies::RemoveCannedPolicy {}),
    )?;

    // set-user-or-group-policy?policyName=xxx&userOrGroup=xxx&isGroup=xxx
    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/set-user-or-group-policy").as_str(),
        AdminOperation(&policies::SetPolicyForUserOrGroup {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/target/list").as_str(),
        AdminOperation(&ListNotificationTargets {}),
    )?;

    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/target/{target_type}/{target_name}").as_str(),
        AdminOperation(&NotificationTarget {}),
    )?;

    // Remove notification target
    // This endpoint removes a notification target based on its type and name.
    // target-remove?target_type=xxx&target_name=xxx
    // * `target_type` - Target type, such as "notify_webhook" or "notify_mqtt".
    // * `target_name` - A unique name for a Target, such as "1".
    r.insert(
        Method::DELETE,
        format!("{}{}", ADMIN_PREFIX, "/v3/target/{target_type}/{target_name}/reset").as_str(),
        AdminOperation(&RemoveNotificationTarget {}),
    )?;

    // arns list
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/target/arns").as_str(),
        AdminOperation(&ListTargetsArns {}),
    )?;

    Ok(())
}
