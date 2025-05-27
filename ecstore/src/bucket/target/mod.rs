use common::error::Result;
use rmp_serde::Serializer as rmpSerializer;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use time::OffsetDateTime;

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct Credentials {
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
    expiration: Option<OffsetDateTime>,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub enum ServiceType {
    #[default]
    Replication,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct LatencyStat {
    curr: Duration, // 当前延迟
    avg: Duration,  // 平均延迟
    max: Duration,  // 最大延迟
}

// 定义 BucketTarget 结构体
#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct BucketTarget {
    source_bucket: String,

    endpoint: String,

    credentials: Option<Credentials>,

    target_bucket: String,

    secure: bool,

    path: Option<String>,

    api: Option<String>,

    arn: Option<String>,

    type_: ServiceType,

    region: Option<String>,

    bandwidth_limit: Option<i64>,

    replication_sync: bool,

    storage_class: Option<String>,

    health_check_duration: Option<Duration>,

    disable_proxy: bool,

    reset_before_date: Option<OffsetDateTime>,

    reset_id: Option<String>,

    total_downtime: Duration,

    last_online: Option<OffsetDateTime>,

    online: bool,

    latency: LatencyStat,

    deployment_id: Option<String>,

    edge: bool,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct BucketTargets {
    pub targets: Vec<BucketTarget>,
}

impl BucketTargets {
    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();

        self.serialize(&mut rmpSerializer::new(&mut buf).with_struct_map())?;

        Ok(buf)
    }

    pub fn unmarshal(buf: &[u8]) -> Result<Self> {
        let t: BucketTargets = rmp_serde::from_slice(buf)?;
        Ok(t)
    }
}
