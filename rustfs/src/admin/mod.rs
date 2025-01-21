pub mod handlers;
pub mod router;
pub mod utils;

use common::error::Result;
// use ecstore::global::{is_dist_erasure, is_erasure};
use handlers::{
    group,
    service_account::{AddServiceAccount, DeleteServiceAccount, InfoServiceAccount, ListServiceAccount, UpdateServiceAccount},
    user,
};
use hyper::Method;
use router::{AdminOperation, S3Router};
use s3s::route::S3Route;

const ADMIN_PREFIX: &str = "/rustfs/admin";

pub fn make_admin_route() -> Result<impl S3Route> {
    let mut r: S3Router<AdminOperation> = S3Router::new();

    // 1
    r.insert(Method::POST, "/", AdminOperation(&handlers::AssumeRoleHandle {}))?;

    regist_user_route(&mut r)?;

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
        AdminOperation(&handlers::ListPools {}),
    )?;
    // 1
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/pools/status").as_str(),
        AdminOperation(&handlers::StatusPool {}),
    )?;
    // todo
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/pools/decommission").as_str(),
        AdminOperation(&handlers::StartDecommission {}),
    )?;
    // todo
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/pools/cancel").as_str(),
        AdminOperation(&handlers::CancelDecommission {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/rebalance/start").as_str(),
        AdminOperation(&handlers::RebalanceStart {}),
    )?;
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/rebalance/status").as_str(),
        AdminOperation(&handlers::RebalanceStatus {}),
    )?;
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/rebalance/stop").as_str(),
        AdminOperation(&handlers::RebalanceStop {}),
    )?;

    // Some APIs are only available in EC mode
    // if is_dist_erasure().await || is_erasure().await {
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
    // }

    Ok(r)
}

fn regist_user_route(r: &mut S3Router<AdminOperation>) -> Result<()> {
    // 1
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/accountinfo").as_str(),
        AdminOperation(&handlers::AccountInfoHandler {}),
    )?;

    // 1
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/list-users").as_str(),
        AdminOperation(&user::ListUsers {}),
    )?;

    // 1
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/user-info").as_str(),
        AdminOperation(&user::GetUserInfo {}),
    )?;

    // 1
    r.insert(
        Method::DELETE,
        format!("{}{}", ADMIN_PREFIX, "/v3/remove-user").as_str(),
        AdminOperation(&user::RemoveUser {}),
    )?;

    // 1
    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/add-user").as_str(),
        AdminOperation(&user::AddUser {}),
    )?;
    // 1
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

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/group").as_str(),
        AdminOperation(&group::GetGroup {}),
    )?;

    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/set-group-status").as_str(),
        AdminOperation(&group::SetGroupStatus {}),
    )?;

    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/update-group-members").as_str(),
        AdminOperation(&group::UpdateGroupMembers {}),
    )?;

    // Service accounts
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/update-service-account").as_str(),
        AdminOperation(&UpdateServiceAccount {}),
    )?;
    // 1
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/info-service-account").as_str(),
        AdminOperation(&InfoServiceAccount {}),
    )?;

    // 1
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/list-service-accounts").as_str(),
        AdminOperation(&ListServiceAccount {}),
    )?;
    // 1
    r.insert(
        Method::DELETE,
        format!("{}{}", ADMIN_PREFIX, "/v3/delete-service-accounts").as_str(),
        AdminOperation(&DeleteServiceAccount {}),
    )?;
    // 1
    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/add-service-accounts").as_str(),
        AdminOperation(&AddServiceAccount {}),
    )?;

    Ok(())
}
