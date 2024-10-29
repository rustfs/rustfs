use crate::error::ErrorCode;
use crate::Result as LocalResult;

use axum::{extract::State, Json};
use serde::Serialize;
use time::OffsetDateTime;

#[derive(Serialize)]
pub struct PoolStatus {
    id: i64,
    cmdline: String,
    #[serde(rename = "lastUpdate")]
    #[serde(serialize_with = "time::serde::rfc3339::serialize")]
    last_updat: OffsetDateTime,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "decommissionInfo")]
    decommission_info: Option<PoolDecommissionInfo>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PoolDecommissionInfo {
    #[serde(serialize_with = "time::serde::rfc3339::serialize")]
    start_time: OffsetDateTime,
    start_size: i64,
    total_size: i64,
    current_size: i64,
    complete: bool,
    failed: bool,
    canceled: bool,

    #[serde(rename = "objectsDecommissioned")]
    items_decommissioned: i64,
    #[serde(rename = "objectsDecommissionedFailed")]
    items_decommission_failed: i64,
    #[serde(rename = "bytesDecommissioned")]
    bytes_done: i64,
    #[serde(rename = "bytesDecommissionedFailed")]
    bytes_failed: i64,
}

pub async fn handler() -> LocalResult<Json<Vec<PoolStatus>>> {
    // if ecstore::is_legacy().await {
    //     return Err(ErrorCode::ErrNotImplemented);
    // }
    //
    //

    // todo 实用oncelock作为全局变量
    let layer = ecstore::new_object_layer_fn();
    let lock = layer.read().await;
    let pools = lock.as_ref().ok_or(ErrorCode::ErrNotImplemented)?;

    // todo, 调用pool.status()接口获取每个池的数据
    //
    let mut result = Vec::new();
    for (idx, _pool) in pools.pools.iter().enumerate() {
        // 这里mock一下数据
        result.push(PoolStatus {
            id: idx as _,
            cmdline: "cmdline".into(),
            last_updat: OffsetDateTime::now_utc(),
            decommission_info: if idx % 2 == 0 {
                Some(PoolDecommissionInfo {
                    start_time: OffsetDateTime::now_utc(),
                    start_size: 1,
                    total_size: 2,
                    current_size: 2,
                    complete: true,
                    failed: true,
                    canceled: true,
                    items_decommissioned: 1,
                    items_decommission_failed: 1,
                    bytes_done: 1,
                    bytes_failed: 1,
                })
            } else {
                None
            },
        })
    }

    Ok(Json(result))
}
