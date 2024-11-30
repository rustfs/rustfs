pub mod handlers;
pub mod router;

use common::error::Result;
// use ecstore::global::{is_dist_erasure, is_erasure};
use hyper::Method;
use router::{AdminOperation, S3Router};
use s3s::route::S3Route;

const ADMIN_PREFIX: &str = "/rustfs/admin";

pub fn make_admin_route() -> Result<impl S3Route> {
    let mut r = S3Router::new();

    r.insert(Method::POST, "/", AdminOperation(&handlers::AssumeRoleHandle {}))?;
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/accountinfo").as_str(),
        AdminOperation(&handlers::AccountInfoHandler {}),
    )?;
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/service").as_str(),
        AdminOperation(&handlers::ServiceHandle {}),
    )?;
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
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/storageinfo").as_str(),
        AdminOperation(&handlers::StorageInfoHandler {}),
    )?;
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

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/pools/list").as_str(),
        AdminOperation(&handlers::ListPools {}),
    )?;
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/pools/status").as_str(),
        AdminOperation(&handlers::StatusPool {}),
    )?;
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/pools/decommission").as_str(),
        AdminOperation(&handlers::StartDecommission {}),
    )?;
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
