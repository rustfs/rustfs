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

use super::object_lock::ObjectLockApi;
use super::versioning::VersioningApi;
use super::{quota::BucketQuota, target::BucketTargets};
use crate::bucket::utils::deserialize;
use crate::config::com::{read_config, save_config};
use crate::disk::BUCKET_META_PREFIX;
use crate::error::{Error, Result};
use crate::new_object_layer_fn;
use crate::store::ECStore;
use byteorder::{BigEndian, ByteOrder, LittleEndian};
use rmp_serde::Serializer as rmpSerializer;
use rustfs_policy::policy::BucketPolicy;
use s3s::dto::{
    BucketLifecycleConfiguration, CORSConfiguration, NotificationConfiguration, ObjectLockConfiguration,
    ReplicationConfiguration, ServerSideEncryptionConfiguration, Tagging, VersioningConfiguration,
};
use serde::Serializer;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use time::OffsetDateTime;
use tracing::error;

pub const BUCKET_METADATA_FILE: &str = ".metadata.bin";
pub const BUCKET_METADATA_FORMAT: u16 = 1;
pub const BUCKET_METADATA_VERSION: u16 = 1;

pub const BUCKET_POLICY_CONFIG: &str = "policy.json";
pub const BUCKET_NOTIFICATION_CONFIG: &str = "notification.xml";
pub const BUCKET_LIFECYCLE_CONFIG: &str = "lifecycle.xml";
pub const BUCKET_SSECONFIG: &str = "bucket-encryption.xml";
pub const BUCKET_TAGGING_CONFIG: &str = "tagging.xml";
pub const BUCKET_QUOTA_CONFIG_FILE: &str = "quota.json";
pub const OBJECT_LOCK_CONFIG: &str = "object-lock.xml";
pub const BUCKET_VERSIONING_CONFIG: &str = "versioning.xml";
pub const BUCKET_REPLICATION_CONFIG: &str = "replication.xml";
pub const BUCKET_TARGETS_FILE: &str = "bucket-targets.json";
pub const BUCKET_CORS_CONFIG: &str = "cors.xml";

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "PascalCase", default)]
pub struct BucketMetadata {
    pub name: String,
    pub created: OffsetDateTime,
    pub lock_enabled: bool, // While marked as unused, it may need to be retained
    pub policy_config_json: Vec<u8>,
    pub notification_config_xml: Vec<u8>,
    pub lifecycle_config_xml: Vec<u8>,
    pub object_lock_config_xml: Vec<u8>,
    pub versioning_config_xml: Vec<u8>,
    pub encryption_config_xml: Vec<u8>,
    pub tagging_config_xml: Vec<u8>,
    pub quota_config_json: Vec<u8>,
    pub replication_config_xml: Vec<u8>,
    pub bucket_targets_config_json: Vec<u8>,
    pub bucket_targets_config_meta_json: Vec<u8>,
    pub cors_config_xml: Vec<u8>,

    pub policy_config_updated_at: OffsetDateTime,
    pub object_lock_config_updated_at: OffsetDateTime,
    pub encryption_config_updated_at: OffsetDateTime,
    pub tagging_config_updated_at: OffsetDateTime,
    pub quota_config_updated_at: OffsetDateTime,
    pub replication_config_updated_at: OffsetDateTime,
    pub versioning_config_updated_at: OffsetDateTime,
    pub lifecycle_config_updated_at: OffsetDateTime,
    pub notification_config_updated_at: OffsetDateTime,
    pub bucket_targets_config_updated_at: OffsetDateTime,
    pub bucket_targets_config_meta_updated_at: OffsetDateTime,
    pub cors_config_updated_at: OffsetDateTime,

    #[serde(skip)]
    pub new_field_updated_at: OffsetDateTime,

    #[serde(skip)]
    pub policy_config: Option<BucketPolicy>,
    #[serde(skip)]
    pub notification_config: Option<NotificationConfiguration>,
    #[serde(skip)]
    pub lifecycle_config: Option<BucketLifecycleConfiguration>,
    #[serde(skip)]
    pub object_lock_config: Option<ObjectLockConfiguration>,
    #[serde(skip)]
    pub versioning_config: Option<VersioningConfiguration>,
    #[serde(skip)]
    pub sse_config: Option<ServerSideEncryptionConfiguration>,
    #[serde(skip)]
    pub tagging_config: Option<Tagging>,
    #[serde(skip)]
    pub quota_config: Option<BucketQuota>,
    #[serde(skip)]
    pub replication_config: Option<ReplicationConfiguration>,
    #[serde(skip)]
    pub bucket_target_config: Option<BucketTargets>,
    #[serde(skip)]
    pub bucket_target_config_meta: Option<HashMap<String, String>>,
    #[serde(skip)]
    pub cors_config: Option<CORSConfiguration>,
}

impl Default for BucketMetadata {
    fn default() -> Self {
        Self {
            name: Default::default(),
            created: OffsetDateTime::UNIX_EPOCH,
            lock_enabled: Default::default(),
            policy_config_json: Default::default(),
            notification_config_xml: Default::default(),
            lifecycle_config_xml: Default::default(),
            object_lock_config_xml: Default::default(),
            versioning_config_xml: Default::default(),
            encryption_config_xml: Default::default(),
            tagging_config_xml: Default::default(),
            quota_config_json: Default::default(),
            replication_config_xml: Default::default(),
            bucket_targets_config_json: Default::default(),
            bucket_targets_config_meta_json: Default::default(),
            cors_config_xml: Default::default(),
            policy_config_updated_at: OffsetDateTime::UNIX_EPOCH,
            object_lock_config_updated_at: OffsetDateTime::UNIX_EPOCH,
            encryption_config_updated_at: OffsetDateTime::UNIX_EPOCH,
            tagging_config_updated_at: OffsetDateTime::UNIX_EPOCH,
            quota_config_updated_at: OffsetDateTime::UNIX_EPOCH,
            replication_config_updated_at: OffsetDateTime::UNIX_EPOCH,
            versioning_config_updated_at: OffsetDateTime::UNIX_EPOCH,
            lifecycle_config_updated_at: OffsetDateTime::UNIX_EPOCH,
            notification_config_updated_at: OffsetDateTime::UNIX_EPOCH,
            bucket_targets_config_updated_at: OffsetDateTime::UNIX_EPOCH,
            bucket_targets_config_meta_updated_at: OffsetDateTime::UNIX_EPOCH,
            cors_config_updated_at: OffsetDateTime::UNIX_EPOCH,
            new_field_updated_at: OffsetDateTime::UNIX_EPOCH,
            policy_config: Default::default(),
            notification_config: Default::default(),
            lifecycle_config: Default::default(),
            object_lock_config: Default::default(),
            versioning_config: Default::default(),
            sse_config: Default::default(),
            tagging_config: Default::default(),
            quota_config: Default::default(),
            replication_config: Default::default(),
            bucket_target_config: Default::default(),
            bucket_target_config_meta: Default::default(),
            cors_config: Default::default(),
        }
    }
}

impl BucketMetadata {
    pub fn new(name: &str) -> Self {
        BucketMetadata {
            name: name.to_string(),
            ..Default::default()
        }
    }

    pub fn save_file_path(&self) -> String {
        format!("{}/{}/{}", BUCKET_META_PREFIX, self.name.as_str(), BUCKET_METADATA_FILE)
    }

    pub fn versioning(&self) -> bool {
        self.lock_enabled
            || (self.object_lock_config.as_ref().is_some_and(|v| v.enabled())
                || self.versioning_config.as_ref().is_some_and(|v| v.enabled()))
    }

    pub fn object_locking(&self) -> bool {
        self.lock_enabled || (self.versioning_config.as_ref().is_some_and(|v| v.enabled()))
    }

    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();

        self.serialize(&mut rmpSerializer::new(&mut buf).with_struct_map())?;

        Ok(buf)
    }

    pub fn unmarshal(buf: &[u8]) -> Result<Self> {
        let t: BucketMetadata = rmp_serde::from_slice(buf)?;
        Ok(t)
    }

    pub fn check_header(buf: &[u8]) -> Result<()> {
        if buf.len() <= 4 {
            return Err(Error::other("read_bucket_metadata: data invalid"));
        }

        let format = LittleEndian::read_u16(&buf[0..2]);
        let version = LittleEndian::read_u16(&buf[2..4]);

        match format {
            BUCKET_METADATA_FORMAT => {}
            _ => return Err(Error::other("read_bucket_metadata: format invalid")),
        }

        match version {
            BUCKET_METADATA_VERSION => {}
            _ => return Err(Error::other("read_bucket_metadata: version invalid")),
        }

        Ok(())
    }

    fn default_timestamps(&mut self) {
        if self.policy_config_updated_at == OffsetDateTime::UNIX_EPOCH {
            self.policy_config_updated_at = self.created
        }
        if self.encryption_config_updated_at == OffsetDateTime::UNIX_EPOCH {
            self.encryption_config_updated_at = self.created
        }

        if self.tagging_config_updated_at == OffsetDateTime::UNIX_EPOCH {
            self.tagging_config_updated_at = self.created
        }
        if self.object_lock_config_updated_at == OffsetDateTime::UNIX_EPOCH {
            self.object_lock_config_updated_at = self.created
        }
        if self.quota_config_updated_at == OffsetDateTime::UNIX_EPOCH {
            self.quota_config_updated_at = self.created
        }

        if self.replication_config_updated_at == OffsetDateTime::UNIX_EPOCH {
            self.replication_config_updated_at = self.created
        }

        if self.versioning_config_updated_at == OffsetDateTime::UNIX_EPOCH {
            self.versioning_config_updated_at = self.created
        }

        if self.lifecycle_config_updated_at == OffsetDateTime::UNIX_EPOCH {
            self.lifecycle_config_updated_at = self.created
        }
        if self.notification_config_updated_at == OffsetDateTime::UNIX_EPOCH {
            self.notification_config_updated_at = self.created
        }

        if self.bucket_targets_config_updated_at == OffsetDateTime::UNIX_EPOCH {
            self.bucket_targets_config_updated_at = self.created
        }
        if self.bucket_targets_config_meta_updated_at == OffsetDateTime::UNIX_EPOCH {
            self.bucket_targets_config_meta_updated_at = self.created
        }
    }

    pub fn update_config(&mut self, config_file: &str, data: Vec<u8>) -> Result<OffsetDateTime> {
        let updated = OffsetDateTime::now_utc();

        match config_file {
            BUCKET_POLICY_CONFIG => {
                self.policy_config_json = data;
                self.policy_config_updated_at = updated;
            }
            BUCKET_NOTIFICATION_CONFIG => {
                self.notification_config_xml = data;
                self.notification_config_updated_at = updated;
            }
            BUCKET_LIFECYCLE_CONFIG => {
                self.lifecycle_config_xml = data;
                self.lifecycle_config_updated_at = updated;
            }
            BUCKET_SSECONFIG => {
                self.encryption_config_xml = data;
                self.encryption_config_updated_at = updated;
            }
            BUCKET_TAGGING_CONFIG => {
                self.tagging_config_xml = data;
                self.tagging_config_updated_at = updated;
            }
            BUCKET_QUOTA_CONFIG_FILE => {
                self.quota_config_json = data;
                self.quota_config_updated_at = updated;
            }
            OBJECT_LOCK_CONFIG => {
                self.object_lock_config_xml = data;
                self.object_lock_config_updated_at = updated;
            }
            BUCKET_VERSIONING_CONFIG => {
                self.versioning_config_xml = data;
                self.versioning_config_updated_at = updated;
            }
            BUCKET_REPLICATION_CONFIG => {
                self.replication_config_xml = data;
                self.replication_config_updated_at = updated;
            }
            BUCKET_TARGETS_FILE => {
                // let x = data.clone();
                // let str = std::str::from_utf8(&x).expect("Invalid UTF-8");
                // println!("update config:{}", str);
                self.bucket_targets_config_json = data.clone();
                self.bucket_targets_config_updated_at = updated;
            }
            BUCKET_CORS_CONFIG => {
                self.cors_config_xml = data;
                self.cors_config_updated_at = updated;
            }
            _ => return Err(Error::other(format!("config file not found : {config_file}"))),
        }

        Ok(updated)
    }

    pub fn set_created(&mut self, created: Option<OffsetDateTime>) {
        self.created = created.unwrap_or_else(OffsetDateTime::now_utc)
    }

    pub async fn save(&mut self) -> Result<()> {
        let Some(store) = new_object_layer_fn() else {
            return Err(Error::other("errServerNotInitialized"));
        };

        self.parse_all_configs(store.clone())?;

        let mut buf: Vec<u8> = vec![0; 4];

        LittleEndian::write_u16(&mut buf[0..2], BUCKET_METADATA_FORMAT);

        LittleEndian::write_u16(&mut buf[2..4], BUCKET_METADATA_VERSION);

        let data = self
            .marshal_msg()
            .map_err(|e| Error::other(format!("save bucket metadata failed: {e}")))?;

        buf.extend_from_slice(&data);

        save_config(store, self.save_file_path().as_str(), buf).await?;

        Ok(())
    }

    fn parse_all_configs(&mut self, _api: Arc<ECStore>) -> Result<()> {
        if !self.policy_config_json.is_empty() {
            self.policy_config = Some(serde_json::from_slice(&self.policy_config_json)?);
        }
        if !self.notification_config_xml.is_empty() {
            self.notification_config = Some(deserialize::<NotificationConfiguration>(&self.notification_config_xml)?);
        }
        if !self.lifecycle_config_xml.is_empty() {
            self.lifecycle_config = Some(deserialize::<BucketLifecycleConfiguration>(&self.lifecycle_config_xml)?);
        }

        if !self.object_lock_config_xml.is_empty() {
            self.object_lock_config = Some(deserialize::<ObjectLockConfiguration>(&self.object_lock_config_xml)?);
        }
        if !self.versioning_config_xml.is_empty() {
            self.versioning_config = Some(deserialize::<VersioningConfiguration>(&self.versioning_config_xml)?);
        }
        if !self.encryption_config_xml.is_empty() {
            self.sse_config = Some(deserialize::<ServerSideEncryptionConfiguration>(&self.encryption_config_xml)?);
        }
        if !self.tagging_config_xml.is_empty() {
            self.tagging_config = Some(deserialize::<Tagging>(&self.tagging_config_xml)?);
        }
        if !self.quota_config_json.is_empty() {
            self.quota_config = Some(serde_json::from_slice(&self.quota_config_json)?);
        }
        if !self.replication_config_xml.is_empty() {
            self.replication_config = Some(deserialize::<ReplicationConfiguration>(&self.replication_config_xml)?);
        }
        //let temp = self.bucket_targets_config_json.clone();
        if !self.bucket_targets_config_json.is_empty() {
            let bucket_targets: BucketTargets = serde_json::from_slice(&self.bucket_targets_config_json)?;
            self.bucket_target_config = Some(bucket_targets);
        } else {
            self.bucket_target_config = Some(BucketTargets::default())
        }
        if !self.cors_config_xml.is_empty() {
            self.cors_config = Some(deserialize::<CORSConfiguration>(&self.cors_config_xml)?);
        }

        Ok(())
    }
}

pub async fn load_bucket_metadata(api: Arc<ECStore>, bucket: &str) -> Result<BucketMetadata> {
    load_bucket_metadata_parse(api, bucket, true).await
}

pub async fn load_bucket_metadata_parse(api: Arc<ECStore>, bucket: &str, parse: bool) -> Result<BucketMetadata> {
    let mut bm = match read_bucket_metadata(api.clone(), bucket).await {
        Ok(res) => res,
        Err(err) => {
            if err != Error::ConfigNotFound {
                return Err(err);
            }

            // info!("bucketmeta {} not found with err {:?}, start to init ", bucket, &err);

            BucketMetadata::new(bucket)
        }
    };

    bm.default_timestamps();

    if parse {
        bm.parse_all_configs(api)?;
    }

    // TODO: parse_all_configs

    Ok(bm)
}

async fn read_bucket_metadata(api: Arc<ECStore>, bucket: &str) -> Result<BucketMetadata> {
    if bucket.is_empty() {
        error!("bucket name empty");
        return Err(Error::other("invalid argument"));
    }

    let bm = BucketMetadata::new(bucket);
    let file_path = bm.save_file_path();

    let data = read_config(api, &file_path).await?;

    BucketMetadata::check_header(&data)?;

    let bm = BucketMetadata::unmarshal(&data[4..])?;

    Ok(bm)
}

fn _write_time<S>(t: &OffsetDateTime, s: S) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut buf = vec![0x0; 15];

    let sec = t.unix_timestamp() - 62135596800;
    let nsec = t.nanosecond();
    buf[0] = 0xc7; // mext8
    buf[1] = 0x0c; // Length
    buf[2] = 0x05; // Time extension type
    BigEndian::write_u64(&mut buf[3..], sec as u64);
    BigEndian::write_u32(&mut buf[11..], nsec);
    s.serialize_bytes(&buf)
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn marshal_msg() {
        // write_time(OffsetDateTime::UNIX_EPOCH).unwrap();

        let bm = BucketMetadata::new("dada");

        let buf = bm.marshal_msg().unwrap();

        let new = BucketMetadata::unmarshal(&buf).unwrap();

        assert_eq!(bm.name, new.name);
    }

    #[tokio::test]
    async fn marshal_msg_complete_example() {
        // Create a complete BucketMetadata with various configurations
        let mut bm = BucketMetadata::new("test-bucket");

        // Set creation time to current time
        bm.created = OffsetDateTime::now_utc();
        bm.lock_enabled = true;

        // Add policy configuration
        let policy_json = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::test-bucket/*"}]}"#;
        bm.policy_config_json = policy_json.as_bytes().to_vec();
        bm.policy_config_updated_at = OffsetDateTime::now_utc();

        // Add lifecycle configuration
        let lifecycle_xml = r#"<LifecycleConfiguration><Rule><ID>rule1</ID><Status>Enabled</Status><Expiration><Days>30</Days></Expiration></Rule></LifecycleConfiguration>"#;
        bm.lifecycle_config_xml = lifecycle_xml.as_bytes().to_vec();
        bm.lifecycle_config_updated_at = OffsetDateTime::now_utc();

        // Add versioning configuration
        let versioning_xml = r#"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"#;
        bm.versioning_config_xml = versioning_xml.as_bytes().to_vec();
        bm.versioning_config_updated_at = OffsetDateTime::now_utc();

        // Add encryption configuration
        let encryption_xml = r#"<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>"#;
        bm.encryption_config_xml = encryption_xml.as_bytes().to_vec();
        bm.encryption_config_updated_at = OffsetDateTime::now_utc();

        // Add tagging configuration
        let tagging_xml = r#"<Tagging><TagSet><Tag><Key>Environment</Key><Value>Test</Value></Tag><Tag><Key>Owner</Key><Value>RustFS</Value></Tag></TagSet></Tagging>"#;
        bm.tagging_config_xml = tagging_xml.as_bytes().to_vec();
        bm.tagging_config_updated_at = OffsetDateTime::now_utc();

        // Add quota configuration
        let quota_json =
            r#"{"quota":1073741824,"quota_type":"Hard","created_at":"2024-01-01T00:00:00Z","updated_at":"2024-01-01T00:00:00Z"}"#; // 1GB quota
        bm.quota_config_json = quota_json.as_bytes().to_vec();
        bm.quota_config_updated_at = OffsetDateTime::now_utc();

        // Add object lock configuration
        let object_lock_xml = r#"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>7</Days></DefaultRetention></Rule></ObjectLockConfiguration>"#;
        bm.object_lock_config_xml = object_lock_xml.as_bytes().to_vec();
        bm.object_lock_config_updated_at = OffsetDateTime::now_utc();

        // Add notification configuration
        let notification_xml = r#"<NotificationConfiguration><CloudWatchConfiguration><Id>notification1</Id><Event>s3:ObjectCreated:*</Event><CloudWatchConfiguration><LogGroupName>test-log-group</LogGroupName></CloudWatchConfiguration></CloudWatchConfiguration></NotificationConfiguration>"#;
        bm.notification_config_xml = notification_xml.as_bytes().to_vec();
        bm.notification_config_updated_at = OffsetDateTime::now_utc();

        // Add replication configuration
        let replication_xml = r#"<ReplicationConfiguration><Role>arn:aws:iam::123456789012:role/replication-role</Role><Rule><ID>rule1</ID><Status>Enabled</Status><Prefix>documents/</Prefix><Destination><Bucket>arn:aws:s3:::destination-bucket</Bucket></Destination></Rule></ReplicationConfiguration>"#;
        bm.replication_config_xml = replication_xml.as_bytes().to_vec();
        bm.replication_config_updated_at = OffsetDateTime::now_utc();

        // Add bucket targets configuration
        let bucket_targets_json = r#"[{"endpoint":"http://target1.example.com","credentials":{"accessKey":"key1","secretKey":"secret1"},"targetBucket":"target-bucket-1","region":"us-east-1"},{"endpoint":"http://target2.example.com","credentials":{"accessKey":"key2","secretKey":"secret2"},"targetBucket":"target-bucket-2","region":"us-west-2"}]"#;
        bm.bucket_targets_config_json = bucket_targets_json.as_bytes().to_vec();
        bm.bucket_targets_config_updated_at = OffsetDateTime::now_utc();

        // Add bucket targets meta configuration
        let bucket_targets_meta_json = r#"{"replicationId":"repl-123","syncMode":"async","bandwidth":"100MB"}"#;
        bm.bucket_targets_config_meta_json = bucket_targets_meta_json.as_bytes().to_vec();
        bm.bucket_targets_config_meta_updated_at = OffsetDateTime::now_utc();

        // Test serialization
        let buf = bm.marshal_msg().unwrap();
        assert!(!buf.is_empty(), "Serialized buffer should not be empty");

        // Test deserialization
        let deserialized_bm = BucketMetadata::unmarshal(&buf).unwrap();

        // Verify all fields are correctly serialized and deserialized
        assert_eq!(bm.name, deserialized_bm.name);
        assert_eq!(bm.created.unix_timestamp(), deserialized_bm.created.unix_timestamp());
        assert_eq!(bm.lock_enabled, deserialized_bm.lock_enabled);

        // Verify configuration data
        assert_eq!(bm.policy_config_json, deserialized_bm.policy_config_json);
        assert_eq!(bm.lifecycle_config_xml, deserialized_bm.lifecycle_config_xml);
        assert_eq!(bm.versioning_config_xml, deserialized_bm.versioning_config_xml);
        assert_eq!(bm.encryption_config_xml, deserialized_bm.encryption_config_xml);
        assert_eq!(bm.tagging_config_xml, deserialized_bm.tagging_config_xml);
        assert_eq!(bm.quota_config_json, deserialized_bm.quota_config_json);
        assert_eq!(bm.object_lock_config_xml, deserialized_bm.object_lock_config_xml);
        assert_eq!(bm.notification_config_xml, deserialized_bm.notification_config_xml);
        assert_eq!(bm.replication_config_xml, deserialized_bm.replication_config_xml);
        assert_eq!(bm.bucket_targets_config_json, deserialized_bm.bucket_targets_config_json);
        assert_eq!(bm.bucket_targets_config_meta_json, deserialized_bm.bucket_targets_config_meta_json);

        // Verify timestamps (comparing unix timestamps to avoid precision issues)
        assert_eq!(
            bm.policy_config_updated_at.unix_timestamp(),
            deserialized_bm.policy_config_updated_at.unix_timestamp()
        );
        assert_eq!(
            bm.lifecycle_config_updated_at.unix_timestamp(),
            deserialized_bm.lifecycle_config_updated_at.unix_timestamp()
        );
        assert_eq!(
            bm.versioning_config_updated_at.unix_timestamp(),
            deserialized_bm.versioning_config_updated_at.unix_timestamp()
        );
        assert_eq!(
            bm.encryption_config_updated_at.unix_timestamp(),
            deserialized_bm.encryption_config_updated_at.unix_timestamp()
        );
        assert_eq!(
            bm.tagging_config_updated_at.unix_timestamp(),
            deserialized_bm.tagging_config_updated_at.unix_timestamp()
        );
        assert_eq!(
            bm.quota_config_updated_at.unix_timestamp(),
            deserialized_bm.quota_config_updated_at.unix_timestamp()
        );
        assert_eq!(
            bm.object_lock_config_updated_at.unix_timestamp(),
            deserialized_bm.object_lock_config_updated_at.unix_timestamp()
        );
        assert_eq!(
            bm.notification_config_updated_at.unix_timestamp(),
            deserialized_bm.notification_config_updated_at.unix_timestamp()
        );
        assert_eq!(
            bm.replication_config_updated_at.unix_timestamp(),
            deserialized_bm.replication_config_updated_at.unix_timestamp()
        );
        assert_eq!(
            bm.bucket_targets_config_updated_at.unix_timestamp(),
            deserialized_bm.bucket_targets_config_updated_at.unix_timestamp()
        );
        assert_eq!(
            bm.bucket_targets_config_meta_updated_at.unix_timestamp(),
            deserialized_bm.bucket_targets_config_meta_updated_at.unix_timestamp()
        );

        // Test that the serialized data contains expected content
        let buf_str = String::from_utf8_lossy(&buf);
        assert!(buf_str.contains("test-bucket"), "Serialized data should contain bucket name");

        // Verify the buffer size is reasonable (should be larger due to all the config data)
        assert!(buf.len() > 1000, "Buffer should be substantial in size due to all configurations");

        println!("✅ Complete BucketMetadata serialization test passed");
        println!("   - Bucket name: {}", deserialized_bm.name);
        println!("   - Lock enabled: {}", deserialized_bm.lock_enabled);
        println!("   - Policy config size: {} bytes", deserialized_bm.policy_config_json.len());
        println!("   - Lifecycle config size: {} bytes", deserialized_bm.lifecycle_config_xml.len());
        println!("   - Serialized buffer size: {} bytes", buf.len());
    }
}
