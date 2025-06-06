#![allow(unused_variables)]
#![allow(dead_code)]
use crate::{
    bucket::{metadata_sys, target::BucketTarget},
    endpoints::Node,
    peer::{PeerS3Client, RemotePeerS3Client},
    StorageAPI,
};
use crate::{
    bucket::{self, target::BucketTargets},
    new_object_layer_fn, peer, store_api,
};
use chrono::Utc;
use lazy_static::lazy_static;
use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};
//use tokio::sync::RwLock;
use aws_sdk_s3::Client as S3Client;
use std::sync::{Arc};
use tokio::sync::RwLock;
use thiserror::Error;

pub struct TClient {
    pub s3cli: S3Client,
    pub remote_peer_client: peer::RemotePeerS3Client,
    pub arn: String,
}
impl TClient {
    pub fn new(s3cli: S3Client, remote_peer_client: RemotePeerS3Client, arn: String) -> Self {
        TClient {
            s3cli,
            remote_peer_client,
            arn,
        }
    }
}

pub struct EpHealth {
    pub endpoint: String,
    pub scheme: String,
    pub online: bool,
    pub last_online: SystemTime,
    pub last_hc_at: SystemTime,
    pub offline_duration: Duration,
    pub latency: LatencyStat, // Assuming LatencyStat is a custom struct
}

impl EpHealth {
    pub fn new(
        endpoint: String,
        scheme: String,
        online: bool,
        last_online: SystemTime,
        last_hc_at: SystemTime,
        offline_duration: Duration,
        latency: LatencyStat,
    ) -> Self {
        EpHealth {
            endpoint,
            scheme,
            online,
            last_online,
            last_hc_at,
            offline_duration,
            latency,
        }
    }
}

pub struct LatencyStat {
    // Define the fields of LatencyStat as per your requirements
}

pub struct ArnTarget {
    client: TargetClient,
    last_refresh: chrono::DateTime<Utc>,
}
impl ArnTarget {
    pub fn new(bucket: String, endpoint: String, ak:String, sk:String) -> Self {
        Self {
            client: TargetClient {
                bucket: bucket,
                storage_class: "STANDRD".to_string(),
                disable_proxy: false,
                health_check_duration: Duration::from_secs(100),
                endpoint: endpoint,
                reset_id: "0".to_string(),
                replicate_sync: false,
                secure: false,
                arn: "".to_string(),
                client: reqwest::Client::new(),
                ak:ak,
                sk:sk,
            },
            last_refresh: Utc::now(),
        }
    }
}

// pub fn get_s3client_from_para(
//     ak: &str,
//     sk: &str,
//     url: &str,
//     _region: &str,
// ) -> Result<S3Client, Box<dyn Error>> {
//     let credentials = Credentials::new(ak, sk, None, None, "");
//     let region = Region::new("us-east-1".to_string());

//     let config = Config::builder()
//         .region(region)
//         .endpoint_url(url.to_string())
//         .credentials_provider(credentials)
//         .behavior_version(BehaviorVersion::latest()) // Adjust as necessary
//         .build();
//     Ok(S3Client::from_conf(config))
// }

pub struct BucketTargetSys {
    arn_remote_map: Arc<RwLock<HashMap<String, ArnTarget>>>,
    targets_map: Arc<RwLock<HashMap<String, Vec<bucket::target::BucketTarget>>>>,
    hc: HashMap<String, EpHealth>,
    //store:Option<Arc<ecstore::store::ECStore>>,
}

lazy_static! {
    pub static ref GLOBAL_Bucket_Target_Sys: std::sync::OnceLock<BucketTargetSys> = BucketTargetSys::new().into();
}

//#[derive(Debug)]
// pub enum SetTargetError {
//   NotFound,
// }

pub async fn get_bucket_target_client(bucket: &str, arn: &str) -> Result<TargetClient, SetTargetError> {
    if let Some(sys) = GLOBAL_Bucket_Target_Sys.get() {
        sys.get_remote_target_client2(arn).await
    } else {
        Err(SetTargetError::TargetNotFound(bucket.to_string()))
    }
}

#[derive(Debug)]
pub struct BucketRemoteTargetNotFound {
    pub bucket: String,
}

pub async fn init_bucket_targets(bucket: &str, meta: Arc<bucket::metadata::BucketMetadata>) {
    println!("140 {}", bucket);
    if let Some(sys) = GLOBAL_Bucket_Target_Sys.get() {
        if let  Some(tgts) = meta.bucket_target_config.clone() {
            for tgt in tgts.targets {
                warn!("ak and sk is:{:?}",tgt.credentials);
                let _ = sys.set_target(bucket, &tgt, false, true).await;
                //sys.targets_map.
            }
        }
    }
}

pub async fn remove_bucket_target(bucket: &str, arn_str:&str) {
    if let Some(sys) = GLOBAL_Bucket_Target_Sys.get() {
        let _ = sys.remove_target(bucket, arn_str).await;
    }
}


impl BucketTargetSys {
    pub fn new() -> Self {
        BucketTargetSys {
            arn_remote_map: Arc::new(RwLock::new(HashMap::new())),
            targets_map: Arc::new(RwLock::new(HashMap::new())),
            hc: HashMap::new(),
        }
    }

    pub async fn list_bucket_targets(&self, bucket: &str) -> Result<BucketTargets, BucketRemoteTargetNotFound> {
        let targets_map = self.targets_map.read().await;
        if let Some(targets) = targets_map.get(bucket) {
            Ok(BucketTargets {
                targets: targets.clone(),
            })
        } else {
            Err(BucketRemoteTargetNotFound {
                bucket: bucket.to_string(),
            })
        }
    }

    pub async fn list_targets(&self, bucket: Option<&str>, _arn_type: Option<&str>) -> Vec<BucketTarget> {
        let _ = _arn_type;
        //let health_stats = self.health_stats();

        let mut targets = Vec::new();

        if let Some(bucket_name) = bucket {
            if let Ok(ts) = self.list_bucket_targets(bucket_name).await {
                for t in ts.targets {
                    //if arn_type.map_or(true, |arn| t.target_type == arn) {
                    //if let Some(hs) = health_stats.get(&t.url().host) {
                    // t.total_downtime = hs.offline_duration;
                    // t.online = hs.online;
                    // t.last_online = hs.last_online;
                    // t.latency = LatencyStat {
                    //     curr: hs.latency.curr,
                    //     avg: hs.latency.avg,
                    //     max: hs.latency.peak,
                    // };
                    //}
                    targets.push(t.clone());
                    //}
                }
            }
            return targets;
        }

        // Locking and iterating over all targets in the system
        let targets_map = self.targets_map.read().await;
        for tgts in targets_map.values() {
            for t in tgts {
                //if arn_type.map_or(true, |arn| t.target_type == arn) {
                // if let Some(hs) = health_stats.get(&t.url().host) {
                //     t.total_downtime = hs.offline_duration;
                //     t.online = hs.online;
                //     t.last_online = hs.last_online;
                //     t.latency = LatencyStat {
                //         curr: hs.latency.curr,
                //         avg: hs.latency.avg,
                //         max: hs.latency.peak,
                //     };
                // }
                targets.push(t.clone());
                //}
            }
        }

        targets
    }

    pub async fn remove_target(&self, bucket:&str, arn_str:&str) -> Result<(), SetTargetError> {
        //to do need lock;
        let mut targets_map = self.targets_map.write().await;
        let tgts = targets_map.get(bucket);
        let mut arn_remotes_map = self.arn_remote_map.write().await;
        if tgts.is_none() {
            //Err(SetTargetError::TargetNotFound(bucket.to_string()));
            return Ok(());
        }

        let tgts = tgts.unwrap(); // 安全解引用
        let mut targets = Vec::with_capacity(tgts.len());
        let mut found = false;

        // 遍历 targets，找出不匹配的 ARN
        for tgt in tgts {
            if tgt.arn != Some(arn_str.to_string()) {
                targets.push(tgt.clone()); // 克隆符合条件的项
            } else {
                found = true; // 找到匹配的 ARN
            }
        }

        // 如果没有找到匹配的 ARN，则返回错误
        if !found {
             return Ok(());
        }

        // 更新 targets_map
        targets_map.insert(bucket.to_string(), targets);
        arn_remotes_map.remove(arn_str);


        let targets = self.list_targets(Some(&bucket), None).await;
        println!("targets is {}", targets.len());
        match serde_json::to_vec(&targets) {
            Ok(json) => {
                let _ = metadata_sys::update(bucket, "bucket-targets.json", json).await;
            }
            Err(e) => {
                println!("序列化失败{}", e);
            }
        }

        Ok(())
    }

    pub async fn get_remote_arn(&self, bucket: &str, target: Option<&BucketTarget>, depl_id: &str) -> (Option<String>, bool) {
        if target.is_none() {
            return (None, false);
        }

        let target = target.unwrap();

        let targets_map = self.targets_map.read().await;

        // 获取锁以访问 arn_remote_map
        let mut _arn_remotes_map = self.arn_remote_map.read().await;
        if let Some(tgts) = targets_map.get(bucket) {
            for tgt in tgts {
                if tgt.type_ == target.type_
                    && tgt.target_bucket == target.target_bucket
                    && tgt.endpoint == target.endpoint
                    && tgt.credentials.as_ref().unwrap().access_key == target.credentials.as_ref().unwrap().access_key
                {
                    return (tgt.arn.clone(), true);
                }
            }
        }

        // if !target.type_.is_valid() {
        //     return (None, false);
        // }

        println!("generate_arn");

        (Some(generate_arn(target.clone(), depl_id.to_string())), false)
    }

    pub async fn get_remote_target_client2(&self, arn: &str) -> Result<TargetClient, SetTargetError> {
        let map = self.arn_remote_map.read().await;
        info!("get remote target client and arn is: {}", arn);
        if let Some(value) = map.get(arn) {
            let mut x = value.client.clone();
            x.arn = arn.to_string();
            Ok(x)
        } else {
            error!("not find target");
            Err(SetTargetError::TargetNotFound(arn.to_string()))
        }
    }

    // pub async fn get_remote_target_client(&self, _tgt: &BucketTarget) -> Result<TargetClient, SetTargetError> {
    //     // Mocked implementation for obtaining a remote client
    //     let tcli = TargetClient {
    //         bucket: _tgt.target_bucket.clone(),
    //         storage_class: "STANDRD".to_string(),
    //         disable_proxy: false,
    //         health_check_duration: Duration::from_secs(100),
    //         endpoint: _tgt.endpoint.clone(),
    //         reset_id: "0".to_string(),
    //         replicate_sync: false,
    //         secure: false,
    //         arn: "".to_string(),
    //         client: reqwest::Client::new(),
    //         ak: _tgt.

    //     };
    //     Ok(tcli)
    // }
    // pub async fn get_remote_target_client_with_bucket(&self, _bucket: String) -> Result<TargetClient, SetTargetError> {
    //     // Mocked implementation for obtaining a remote client
    //     let tcli = TargetClient {
    //         bucket: _tgt.target_bucket.clone(),
    //         storage_class: "STANDRD".to_string(),
    //         disable_proxy: false,
    //         health_check_duration: Duration::from_secs(100),
    //         endpoint: _tgt.endpoint.clone(),
    //         reset_id: "0".to_string(),
    //         replicate_sync: false,
    //         secure: false,
    //         arn: "".to_string(),
    //         client: reqwest::Client::new(),
    //     };
    //     Ok(tcli)
    // }

    async fn local_is_bucket_versioned(&self, _bucket: &str) -> bool {
        let Some(store) = new_object_layer_fn() else {
            return false;
        };
        //store.get_bucket_info(bucket, opts)

        // let binfo:BucketInfo = store
        // .get_bucket_info(bucket, &ecstore::store_api::BucketOptions::default()).await;
        match store.get_bucket_info(_bucket, &store_api::BucketOptions::default()).await {
            Ok(info) => {
                println!("Bucket Info: {:?}", info);
                return info.versionning;
            }
            Err(err) => {
                eprintln!("Error: {:?}", err);
                return false;
            }
        }
    }

    async fn is_bucket_versioned(&self, _bucket: &str) -> bool {
        return true;
        // let url_str = "http://127.0.0.1:9001";

        // // 转换为 Url 类型
        // let parsed_url = url::Url::parse(url_str).unwrap();

        // let node = Node {
        //     url: parsed_url,
        //     pools: vec![],
        //     is_local: false,
        //     grid_host: "".to_string(),
        // };
        // let cli = ecstore::peer::RemotePeerS3Client::new(Some(node), None);

        // match cli.get_bucket_info(_bucket, &ecstore::store_api::BucketOptions::default()).await
        // {
        //     Ok(info) => {
        //         println!("Bucket Info: {:?}", info);
        //         info.versionning
        //     }
        //     Err(err) => {
        //         eprintln!("Error: {:?}", err);
        //         return false;
        //     }
        // }
    }

    pub async fn set_target(&self, bucket: &str, tgt: &BucketTarget, update: bool, fromdisk: bool) -> Result<(), SetTargetError> {
        // if !tgt.type_.is_valid() && !update {
        //     return Err(SetTargetError::InvalidTargetType(bucket.to_string()));
        // }

        //let client = self.get_remote_target_client(tgt).await?;
        if tgt.type_ == Some("replication".to_string()) {
            if !fromdisk {
                let versioning_config = self.local_is_bucket_versioned(bucket).await;
                if !versioning_config {
                    // println!("111111111");
                    return Err(SetTargetError::TargetNotVersioned(bucket.to_string()));
                }
            }
        }

        let url_str = format!("http://{}", tgt.endpoint.clone());

        println!("url str is {}", url_str);
        // 转换为 Url 类型
        let parsed_url = url::Url::parse(&url_str).unwrap();

        let node = Node {
            url: parsed_url,
            pools: vec![],
            is_local: false,
            grid_host: "".to_string(),
        };

        let cli = peer::RemotePeerS3Client::new(Some(node), None);

        match cli
            .get_bucket_info(&tgt.target_bucket, &store_api::BucketOptions::default())
            .await
        {
            Ok(info) => {
                println!("Bucket Info: {:?}", info);
                if !info.versionning {
                    println!("2222222222 {}", info.versionning);
                    return Err(SetTargetError::TargetNotVersioned(tgt.target_bucket.to_string()));
                }
            }
            Err(err) => {
                println!("remote bucket 369 is:{}", tgt.target_bucket);
                eprintln!("Error: {:?}", err);
                return Err(SetTargetError::SourceNotVersioned(tgt.target_bucket.to_string()));
            }
        }

        //if tgt.target_type == BucketTargetType::ReplicationService {
        // Check if target is a RustFS server and alive
        // let hc_result = tokio::time::timeout(Duration::from_secs(3), client.health_check(&tgt.endpoint)).await;
        // match hc_result {
        //     Ok(Ok(true)) => {} // Server is alive
        //     Ok(Ok(false)) | Ok(Err(_)) | Err(_) => {
        //         return Err(SetTargetError::HealthCheckFailed(tgt.target_bucket.clone()));
        //     }
        // }

        //Lock and update target maps
        let mut targets_map = self.targets_map.write().await;
        let mut arn_remotes_map = self.arn_remote_map.write().await;

        let targets = targets_map.entry(bucket.to_string()).or_default();
        let mut found = false;

        for existing_target in targets.iter_mut() {
            println!("418 exist:{}", existing_target.source_bucket.clone());
            if existing_target.type_ == tgt.type_ {
                if existing_target.arn == tgt.arn {
                    if !update {
                        return Err(SetTargetError::TargetAlreadyExists(existing_target.target_bucket.clone()));
                    }
                    *existing_target = tgt.clone();
                    found = true;
                    break;
                }

                if existing_target.endpoint == tgt.endpoint {
                    println!("endpoint is same:{}", tgt.endpoint.clone());
                    return Err(SetTargetError::TargetAlreadyExists(existing_target.target_bucket.clone()));
                }
            }
        }

        if !found && !update {
            println!("437 exist:{}", tgt.arn.clone().unwrap());
            targets.push(tgt.clone());
        }
        let arntgt: ArnTarget = ArnTarget::new(tgt.target_bucket.clone(), tgt.endpoint.clone(),tgt.credentials.clone().unwrap().access_key.clone(), tgt.credentials.clone().unwrap().secret_key);

        arn_remotes_map.insert(tgt.arn.clone().unwrap().clone(), arntgt);
        //self.update_bandwidth_limit(bucket, &tgt.arn, tgt.bandwidth_limit).await;

        Ok(())
    }
}

#[derive(Clone)]
pub struct TargetClient {
    pub client: reqwest::Client, // Using reqwest HTTP client
    pub health_check_duration: Duration,
    pub bucket: String, // Remote bucket target
    pub replicate_sync: bool,
    pub storage_class: String, // Storage class on remote
    pub disable_proxy: bool,
    pub arn: String, // ARN to uniquely identify remote target
    pub reset_id: String,
    pub endpoint: String,
    pub secure: bool,
    pub ak:String,
    pub sk:String,
}

impl TargetClient {
    pub fn new(
        client: reqwest::Client,
        health_check_duration: Duration,
        bucket: String,
        replicate_sync: bool,
        storage_class: String,
        disable_proxy: bool,
        arn: String,
        reset_id: String,
        endpoint: String,
        secure: bool,
        ak:String,
        sk:String,
    ) -> Self {
        TargetClient {
            client,
            health_check_duration,
            bucket,
            replicate_sync,
            storage_class,
            disable_proxy,
            arn,
            reset_id,
            endpoint,
            secure,
            ak,
            sk,
        }
    }
    pub async fn bucket_exists(&self, _bucket: &str) -> Result<bool, SetTargetError> {
        Ok(true) // Mocked implementation
    }
}
use tracing::{error, warn, info};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct VersioningConfig {
    pub enabled: bool,
}

impl VersioningConfig {
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

#[derive(Debug)]
pub struct Client;

impl Client {
    pub async fn bucket_exists(&self, _bucket: &str) -> Result<bool, SetTargetError> {
        Ok(true) // Mocked implementation
    }

    pub async fn get_bucket_versioning(&self, _bucket: &str) -> Result<VersioningConfig, SetTargetError> {
        Ok(VersioningConfig { enabled: true })
    }

    pub async fn health_check(&self, _endpoint: &str) -> Result<bool, SetTargetError> {
        Ok(true) // Mocked health check
    }
}

#[derive(Debug, PartialEq)]
pub struct ServiceType(String);

impl ServiceType {
    pub fn is_valid(&self) -> bool {
        !self.0.is_empty() // 根据需求添加具体的验证逻辑
    }
}

#[derive(Debug, PartialEq)]
pub struct ARN {
    pub arn_type: String,
    pub id: String,
    pub region: String,
    pub bucket: String,
}

impl ARN {
    /// 检查 ARN 是否为空
    pub fn is_empty(&self) -> bool {
        //!self.arn_type.is_valid()
        return false;
    }

    /// 将 ARN 转为字符串格式
    pub fn to_string(&self) -> String {
        format!("arn:rustfs:{}:{}:{}:{}", self.arn_type, self.region, self.id, self.bucket)
    }

    /// 从字符串解析 ARN
    pub fn parse(s: &str) -> Result<Self, String> {
        // ARN 必须是格式 arn:rustfs:<Type>:<REGION>:<ID>:<remote-bucket>
        if !s.starts_with("arn:rustfs:") {
            return Err(format!("Invalid ARN {}", s).into());
        }

        let tokens: Vec<&str> = s.split(':').collect();
        if tokens.len() != 6 || tokens[4].is_empty() || tokens[5].is_empty() {
            return Err(format!("Invalid ARN {}", s).into());
        }

        Ok(ARN {
            arn_type: tokens[2].to_string(),
            region: tokens[3].to_string(),
            id: tokens[4].to_string(),
            bucket: tokens[5].to_string(),
        })
    }
}

// 实现 `Display` trait，使得可以直接使用 `format!` 或 `{}` 输出 ARN
impl std::fmt::Display for ARN {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

fn must_get_uuid() -> String {
    Uuid::new_v4().to_string()
    // match Uuid::new_v4() {
    //     Ok(uuid) => uuid.to_string(),
    //     Err(err) => {
    //         error!("Critical error: {}", err);
    //         panic!("Failed to generate UUID: {}", err); // Ensures similar behavior as Go's logger.CriticalIf
    //     }
    // }
}
fn generate_arn(target: BucketTarget, depl_id: String) -> String {
    let mut uuid: String = depl_id;
    if uuid == "" {
        uuid = must_get_uuid();
    }

    let arn: ARN = ARN {
        arn_type: target.type_.unwrap(),
        id: (uuid),
        region: "us-east-1".to_string(),
        bucket: (target.target_bucket),
    };
    return arn.to_string();
}

// use std::collections::HashMap;
// use std::sync::{Arc, Mutex, RwLock};
// use std::time::Duration;
// use tokio::time::timeout;
// use tokio::sync::RwLock as AsyncRwLock;
// use serde::Deserialize;
// use thiserror::Error;

// #[derive(Debug, Clone, PartialEq)]
// pub enum BucketTargetType {
//     ReplicationService,
//     // Add other service types as needed
// }

// impl BucketTargetType {
//     pub fn is_valid(&self) -> bool {
//         matches!(self, BucketTargetType::ReplicationService)
//     }
// }

// #[derive(Debug, Clone)]
// pub struct BucketTarget {
//     pub arn: String,
//     pub target_bucket: String,
//     pub endpoint: String,
//     pub credentials: Credentials,
//     pub secure: bool,
//     pub bandwidth_limit: Option<u64>,
//     pub target_type: BucketTargetType,
// }

// #[derive(Debug, Clone)]
// pub struct Credentials {
//     pub access_key: String,
//     pub secret_key: String,
// }

// #[derive(Debug)]
// pub struct BucketTargetSys {
//     targets_map: Arc<RwLock<HashMap<String, Vec<BucketTarget>>>>,
//     arn_remotes_map: Arc<Mutex<HashMap<String, ArnTarget>>>,
// }

// impl BucketTargetSys {
//     pub fn new() -> Self {
//         Self {
//             targets_map: Arc::new(RwLock::new(HashMap::new())),
//             arn_remotes_map: Arc::new(Mutex::new(HashMap::new())),
//         }
//     }

//     pub async fn set_target(
//         &self,
//         bucket: &str,
//         tgt: &BucketTarget,
//         update: bool,
//     ) -> Result<(), SetTargetError> {
//         if !tgt.target_type.is_valid() && !update {
//             return Err(SetTargetError::InvalidTargetType(bucket.to_string()));
//         }

//         let client = self.get_remote_target_client(tgt).await?;

//         // Validate if target credentials are OK
//         let exists = client.bucket_exists(&tgt.target_bucket).await?;
//         if !exists {
//             return Err(SetTargetError::TargetNotFound(tgt.target_bucket.clone()));
//         }

//         if tgt.target_type == BucketTargetType::ReplicationService {
//             if !self.is_bucket_versioned(bucket).await {
//                 return Err(SetTargetError::SourceNotVersioned(bucket.to_string()));
//             }

//             let versioning_config = client.get_bucket_versioning(&tgt.target_bucket).await?;
//             if !versioning_config.is_enabled() {
//                 return Err(SetTargetError::TargetNotVersioned(tgt.target_bucket.clone()));
//             }
//         }

//         // Check if target is a RustFS server and alive
//         let hc_result = timeout(Duration::from_secs(3), client.health_check(&tgt.endpoint)).await;
//         match hc_result {
//             Ok(Ok(true)) => {} // Server is alive
//             Ok(Ok(false)) | Ok(Err(_)) | Err(_) => {
//                 return Err(SetTargetError::HealthCheckFailed(tgt.target_bucket.clone()));
//             }
//         }

//         // Lock and update target maps
//         let mut targets_map = self.targets_map.write().await;
//         let mut arn_remotes_map = self.arn_remotes_map.lock().unwrap();

//         let targets = targets_map.entry(bucket.to_string()).or_default();
//         let mut found = false;

//         for existing_target in targets.iter_mut() {
//             if existing_target.target_type == tgt.target_type {
//                 if existing_target.arn == tgt.arn {
//                     if !update {
//                         return Err(SetTargetError::TargetAlreadyExists(existing_target.target_bucket.clone()));
//                     }
//                     *existing_target = tgt.clone();
//                     found = true;
//                     break;
//                 }

//                 if existing_target.endpoint == tgt.endpoint {
//                     return Err(SetTargetError::TargetAlreadyExists(existing_target.target_bucket.clone()));
//                 }
//             }
//         }

//         if !found && !update {
//             targets.push(tgt.clone());
//         }

//         arn_remotes_map.insert(tgt.arn.clone(), ArnTarget { client });
//         self.update_bandwidth_limit(bucket, &tgt.arn, tgt.bandwidth_limit).await;

//         Ok(())
//     }

//     async fn get_remote_target_client(&self, tgt: &BucketTarget) -> Result<Client, SetTargetError> {
//         // Mocked implementation for obtaining a remote client
//         Ok(Client {})
//     }

//     async fn is_bucket_versioned(&self, bucket: &str) -> bool {
//         // Mocked implementation for checking if a bucket is versioned
//         true
//     }

//     async fn update_bandwidth_limit(
//         &self,
//         bucket: &str,
//         arn: &str,
//         limit: Option<u64>,
//     ) {
//         // Mocked implementation for updating bandwidth limits
//     }
// }

// #[derive(Debug)]
// pub struct Client;

// impl Client {
//     pub async fn bucket_exists(&self, _bucket: &str) -> Result<bool, SetTargetError> {
//         Ok(true) // Mocked implementation
//     }

//     pub async fn get_bucket_versioning(
//         &self,
//         _bucket: &str,
//     ) -> Result<VersioningConfig, SetTargetError> {
//         Ok(VersioningConfig { enabled: true })
//     }

//     pub async fn health_check(&self, _endpoint: &str) -> Result<bool, SetTargetError> {
//         Ok(true) // Mocked health check
//     }
// }

// #[derive(Debug, Clone)]
// pub struct ArnTarget {
//     pub client: Client,
// }

#[derive(Debug, Error)]
pub enum SetTargetError {
    #[error("Invalid target type for bucket {0}")]
    InvalidTargetType(String),

    #[error("Target bucket {0} not found")]
    TargetNotFound(String),

    #[error("Source bucket {0} is not versioned")]
    SourceNotVersioned(String),

    #[error("Target bucket {0} is not versioned")]
    TargetNotVersioned(String),

    #[error("Health check failed for bucket {0}")]
    HealthCheckFailed(String),

    #[error("Target bucket {0} already exists")]
    TargetAlreadyExists(String),
}
