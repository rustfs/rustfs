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

pub mod console;
pub mod handlers;
pub mod router;
mod rpc;
pub mod utils;

// use ecstore::global::{is_dist_erasure, is_erasure};
use handlers::{
    bucket_encryption::{
        DeleteBucketEncryptionHandler, GetBucketEncryptionHandler, ListBucketEncryptionsHandler, PutBucketEncryptionHandler,
    },
    GetReplicationMetricsHandler, ListRemoteTargetHandler, RemoveRemoteTargetHandler, SetRemoteTargetHandler, bucket_meta,
    event::{
        GetBucketNotification, ListNotificationTargets, NotificationTarget, RemoveBucketNotification, RemoveNotificationTarget,
        SetBucketNotification,
    },
    group,
    kms::{ConfigureKms, CreateKmsKey, DisableKmsKey, EnableKmsKey, GetKmsKeyStatus, GetKmsStatus, ListKmsKeys},
    policies, pools, rebalance,
    service_account::{AddServiceAccount, DeleteServiceAccount, InfoServiceAccount, ListServiceAccount, UpdateServiceAccount},
    sts, tier, user,
};
use hyper::Method;
use router::{AdminOperation, S3Router};
use rpc::register_rpc_route;
use s3s::route::S3Route;

const ADMIN_PREFIX: &str = "/rustfs/admin";

pub fn make_admin_route(console_enabled: bool, fs: std::sync::Arc<crate::storage::ecfs::FS>) -> std::io::Result<impl S3Route> {
    let mut r: S3Router<AdminOperation> = S3Router::new(console_enabled);

    // 1
    r.insert(Method::POST, "/", AdminOperation(Box::new(sts::AssumeRoleHandle {})))?;

    register_rpc_route(&mut r)?;
    register_user_route(&mut r)?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/service").as_str(),
        AdminOperation(Box::new(handlers::ServiceHandle {})),
    )?;
    // 1
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/info").as_str(),
        AdminOperation(Box::new(handlers::ServerInfoHandler {})),
    )?;
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/inspect-data").as_str(),
        AdminOperation(Box::new(handlers::InspectDataHandler {})),
    )?;
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/inspect-data").as_str(),
        AdminOperation(Box::new(handlers::InspectDataHandler {})),
    )?;
    // 1
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/storageinfo").as_str(),
        AdminOperation(Box::new(handlers::StorageInfoHandler {})),
    )?;
    // 1
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/datausageinfo").as_str(),
        AdminOperation(Box::new(handlers::DataUsageInfoHandler {})),
    )?;
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/metrics").as_str(),
        AdminOperation(Box::new(handlers::MetricsHandler {})),
    )?;

    // 1
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/pools/list").as_str(),
        AdminOperation(Box::new(pools::ListPools {})),
    )?;
    // 1
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/pools/status").as_str(),
        AdminOperation(Box::new(pools::StatusPool {})),
    )?;
    // todo
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/pools/decommission").as_str(),
        AdminOperation(Box::new(pools::StartDecommission {})),
    )?;
    // todo
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/pools/cancel").as_str(),
        AdminOperation(Box::new(pools::CancelDecommission {})),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/rebalance/start").as_str(),
        AdminOperation(Box::new(rebalance::RebalanceStart {})),
    )?;
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/rebalance/status").as_str(),
        AdminOperation(Box::new(rebalance::RebalanceStatus {})),
    )?;
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/rebalance/stop").as_str(),
        AdminOperation(Box::new(rebalance::RebalanceStop {})),
    )?;

    // Some APIs are only available in EC mode
    // if is_dist_erasure().await || is_erasure().await {
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/heal/{bucket}/{prefix}").as_str(),
        AdminOperation(Box::new(handlers::HealHandler {})),
    )?;
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/background-heal/status").as_str(),
        AdminOperation(Box::new(handlers::BackgroundHealStatusHandler {})),
    )?;

    // ?
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/tier").as_str(),
        AdminOperation(Box::new(tier::ListTiers {})),
    )?;
    // ?
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/tier-stats").as_str(),
        AdminOperation(Box::new(tier::GetTierInfo {})),
    )?;
    // ?force=xxx
    r.insert(
        Method::DELETE,
        format!("{}{}", ADMIN_PREFIX, "/v3/tier/{tiername}").as_str(),
        AdminOperation(Box::new(tier::RemoveTier {})),
    )?;
    // ?force=xxx
    // body: AddOrUpdateTierReq
    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/tier").as_str(),
        AdminOperation(Box::new(tier::AddTier {})),
    )?;
    // ?
    // body: AddOrUpdateTierReq
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/tier/{tiername}").as_str(),
        AdminOperation(Box::new(tier::EditTier {})),
    )?;
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/tier/clear").as_str(),
        AdminOperation(Box::new(tier::ClearTier {})),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/export-bucket-metadata").as_str(),
        AdminOperation(Box::new(bucket_meta::ExportBucketMetadata {})),
    )?;

    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/import-bucket-metadata").as_str(),
        AdminOperation(Box::new(bucket_meta::ImportBucketMetadata {})),
    )?;

    // KMS management endpoints
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/key/create").as_str(),
        AdminOperation(Box::new(CreateKmsKey {})),
    )?;
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/key/status").as_str(),
        AdminOperation(Box::new(GetKmsKeyStatus {})),
    )?;
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/key/list").as_str(),
        AdminOperation(Box::new(ListKmsKeys {})),
    )?;
    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/key/enable").as_str(),
        AdminOperation(Box::new(EnableKmsKey {})),
    )?;
    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/key/disable").as_str(),
        AdminOperation(Box::new(DisableKmsKey {})),
    )?;
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/status").as_str(),
        AdminOperation(Box::new(GetKmsStatus {})),
    )?;
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/configure").as_str(),
        AdminOperation(Box::new(ConfigureKms {})),
    )?;

    // Bucket encryption endpoints - COMPLETED
    // All bucket encryption handlers are fully implemented and integrated
    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/bucket-encryption/{bucket}").as_str(),
        AdminOperation(Box::new(PutBucketEncryptionHandler { fs: fs.clone() })),
    )?;
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/bucket-encryption/{bucket}").as_str(),
        AdminOperation(Box::new(GetBucketEncryptionHandler { fs: fs.clone() })),
    )?;
    r.insert(
        Method::DELETE,
        format!("{}{}", ADMIN_PREFIX, "/v3/bucket-encryption/{bucket}").as_str(),
        AdminOperation(Box::new(DeleteBucketEncryptionHandler { fs: fs.clone() })),
    )?;
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/bucket-encryptions").as_str(),
        AdminOperation(Box::new(ListBucketEncryptionsHandler { fs: fs.clone() })),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/list-remote-targets").as_str(),
        AdminOperation(Box::new(ListRemoteTargetHandler {})),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/replicationmetrics").as_str(),
        AdminOperation(Box::new(GetReplicationMetricsHandler {})),
    )?;

    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/set-remote-target").as_str(),
        AdminOperation(Box::new(SetRemoteTargetHandler {})),
    )?;

    r.insert(
        Method::DELETE,
        format!("{}{}", ADMIN_PREFIX, "/v3/remove-remote-target").as_str(),
        AdminOperation(Box::new(RemoveRemoteTargetHandler {})),
    )?;

    Ok(r)
}

/// user router
fn register_user_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    // 1
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/accountinfo").as_str(),
        AdminOperation(Box::new(handlers::AccountInfoHandler {})),
    )?;

    // ?[bucket=xxx]
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/list-users").as_str(),
        AdminOperation(Box::new(user::ListUsers {})),
    )?;

    // ?accessKey=xxx
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/user-info").as_str(),
        AdminOperation(Box::new(user::GetUserInfo {})),
    )?;

    // ?accessKey=xxx
    r.insert(
        Method::DELETE,
        format!("{}{}", ADMIN_PREFIX, "/v3/remove-user").as_str(),
        AdminOperation(Box::new(user::RemoveUser {})),
    )?;

    // ?accessKey=xxx
    // body: AddOrUpdateUserReq
    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/add-user").as_str(),
        AdminOperation(Box::new(user::AddUser {})),
    )?;
    // ?accessKey=xxx&status=enabled
    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/set-user-status").as_str(),
        AdminOperation(Box::new(user::SetUserStatus {})),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/groups").as_str(),
        AdminOperation(Box::new(group::ListGroups {})),
    )?;

    // ?group=xxx
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/group").as_str(),
        AdminOperation(Box::new(group::GetGroup {})),
    )?;

    // ?group=xxx&status=xxx
    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/set-group-status").as_str(),
        AdminOperation(Box::new(group::SetGroupStatus {})),
    )?;

    // @body GroupAddRemove
    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/update-group-members").as_str(),
        AdminOperation(Box::new(group::UpdateGroupMembers {})),
    )?;

    // Service accounts
    // ?accessKey=xxx
    // @body: UpdateServiceAccountReq
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/update-service-account").as_str(),
        AdminOperation(Box::new(UpdateServiceAccount {})),
    )?;
    // ?accessKey=xxx
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/info-service-account").as_str(),
        AdminOperation(Box::new(InfoServiceAccount {})),
    )?;

    // ?[user=xxx]
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/list-service-accounts").as_str(),
        AdminOperation(Box::new(ListServiceAccount {})),
    )?;
    // ?accessKey=xxx
    r.insert(
        Method::DELETE,
        format!("{}{}", ADMIN_PREFIX, "/v3/delete-service-accounts").as_str(),
        AdminOperation(Box::new(DeleteServiceAccount {})),
    )?;
    // @body: AddServiceAccountReq
    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/add-service-accounts").as_str(),
        AdminOperation(Box::new(AddServiceAccount {})),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/export-iam").as_str(),
        AdminOperation(Box::new(user::ExportIam {})),
    )?;

    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/import-iam").as_str(),
        AdminOperation(Box::new(user::ImportIam {})),
    )?;

    // list-canned-policies?bucket=xxx
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/list-canned-policies").as_str(),
        AdminOperation(Box::new(policies::ListCannedPolicies {})),
    )?;

    // info-canned-policy?name=xxx
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/info-canned-policy").as_str(),
        AdminOperation(Box::new(policies::InfoCannedPolicy {})),
    )?;

    // add-canned-policy?name=xxx
    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/add-canned-policy").as_str(),
        AdminOperation(Box::new(policies::AddCannedPolicy {})),
    )?;

    // remove-canned-policy?name=xxx
    r.insert(
        Method::DELETE,
        format!("{}{}", ADMIN_PREFIX, "/v3/remove-canned-policy").as_str(),
        AdminOperation(Box::new(policies::RemoveCannedPolicy {})),
    )?;

    // set-user-or-group-policy?policyName=xxx&userOrGroup=xxx&isGroup=xxx
    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/set-user-or-group-policy").as_str(),
        AdminOperation(Box::new(policies::SetPolicyForUserOrGroup {})),
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
    // arns
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/target/arns").as_str(),
        AdminOperation(&ListNotificationTargets {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/target-set-bucket").as_str(),
        AdminOperation(&SetBucketNotification {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/target-get-bucket").as_str(),
        AdminOperation(&GetBucketNotification {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/target-remove-bucket").as_str(),
        AdminOperation(&RemoveBucketNotification {}),
    )?;

    Ok(())
}
