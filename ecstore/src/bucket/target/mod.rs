use time::OffsetDateTime;

#[derive(Debug)]
pub struct Credentials {
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
    expiration: Option<OffsetDateTime>,
}

#[derive(Debug)]
pub enum ServiceType {
    Replication,
}

// pub struct LatencyStat {
//     curr: Duration, // 当前延迟
//     avg: Duration,  // 平均延迟
//     max: Duration,  // 最大延迟
// }

// #[derive(Serialize, Deserialize, Debug)]
// pub struct LatencyStat {
//     curr: Duration,
//     avg: Duration,
//     max: Duration,
// }

// // 定义BucketTarget结构体
// #[derive(Serialize, Deserialize, Debug)]
// pub struct BucketTarget {
//     #[serde(rename = "sourcebucket")]
//     source_bucket: String,

//     #[serde(rename = "endpoint")]
//     endpoint: String,

//     #[serde(rename = "credentials")]
//     credentials: Option<Credentials>,

//     #[serde(rename = "targetbucket")]
//     target_bucket: String,

//     #[serde(rename = "secure")]
//     secure: bool,

//     #[serde(rename = "path", skip_serializing_if = "Option::is_none")]
//     path: Option<String>,

//     #[serde(rename = "api", skip_serializing_if = "Option::is_none")]
//     api: Option<String>,

//     #[serde(rename = "arn", skip_serializing_if = "Option::is_none")]
//     arn: Option<String>,

//     #[serde(rename = "type")]
//     type_: ServiceType,

//     #[serde(rename = "region", skip_serializing_if = "Option::is_none")]
//     region: Option<String>,

//     #[serde(rename = "bandwidthlimit")]
//     bandwidth_limit: Option<i64>,

//     #[serde(rename = "replicationSync")]
//     replication_sync: bool,

//     #[serde(rename = "storageclass", skip_serializing_if = "Option::is_none")]
//     storage_class: Option<String>,

//     #[serde(rename = "healthCheckDuration")]
//     health_check_duration: Option<Duration>,

//     #[serde(rename = "disableProxy")]
//     disable_proxy: bool,

//     #[serde(rename = "resetBeforeDate", skip_serializing_if = "Option::is_none")]
//     reset_before_date: Option<SystemTime>,

//     #[serde(rename = "resetID", skip_serializing_if = "Option::is_none")]
//     reset_id: Option<String>,

//     #[serde(rename = "totalDowntime")]
//     total_downtime: Duration,

//     #[serde(rename = "lastOnline")]
//     last_online: SystemTime,

//     #[serde(rename = "isOnline")]
//     online: bool,

//     #[serde(rename = "latency")]
//     latency: LatencyStat,

//     #[serde(rename = "deploymentID", skip_serializing_if = "Option::is_none")]
//     deployment_id: Option<String>,

//     #[serde(rename = "edge")]
//     edge: bool,
// }
