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
use byteorder::{ByteOrder, LittleEndian};
use rmp_serde::Serializer as rmpSerializer;
use rustfs_policy::policy::BucketPolicy;
use s3s::dto::{
    BucketLifecycleConfiguration, CORSConfiguration, NotificationConfiguration, ObjectLockConfiguration,
    PublicAccessBlockConfiguration, ReplicationConfiguration, ServerSideEncryptionConfiguration, Tagging,
    VersioningConfiguration,
};
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use std::collections::HashMap;
use std::sync::Arc;
use time::OffsetDateTime;
use tracing::error;

/// MessagePack extension type 5 time format
/// Format: ext8 (0xc7), len 12, type 5, 8 bytes sec (BE) + 4 bytes nsec (BE).
mod msgp_time {
    use super::{ByteBuf, OffsetDateTime};
    use byteorder::{BigEndian, ByteOrder};
    use serde::de::Error;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    #[serde(rename = "_ExtStruct")]
    struct MsgpTimeExt((i8, ByteBuf));

    pub fn serialize<S>(t: &OffsetDateTime, s: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut buf = vec![0u8; 12];
        BigEndian::write_i64(&mut buf[0..8], t.unix_timestamp());
        BigEndian::write_u32(&mut buf[8..12], t.nanosecond());
        let ext = MsgpTimeExt((5i8, ByteBuf::from(buf)));
        ext.serialize(s)
    }

    pub fn deserialize<'de, D>(d: D) -> std::result::Result<OffsetDateTime, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let ext = MsgpTimeExt::deserialize(d)?;
        if ext.0.0 != 5 || ext.0.1.len() != 12 {
            return Err(D::Error::custom("invalid msgp time format"));
        }
        let sec = BigEndian::read_i64(&ext.0.1[0..8]);
        let nsec = BigEndian::read_u32(&ext.0.1[8..12]);
        OffsetDateTime::from_unix_timestamp(sec)
            .map_err(|_| D::Error::custom("invalid timestamp"))?
            .replace_nanosecond(nsec)
            .map_err(|_| D::Error::custom("invalid nanosecond"))
    }
}

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
pub const BUCKET_PUBLIC_ACCESS_BLOCK_CONFIG: &str = "public-access-block.xml";
pub const BUCKET_ACL_CONFIG: &str = "bucket-acl.json";

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "PascalCase", default)]
pub struct BucketMetadata {
    pub name: String,
    #[serde(with = "msgp_time")]
    pub created: OffsetDateTime,
    pub lock_enabled: bool, // While marked as unused, it may need to be retained
    #[serde(rename = "PolicyConfigJSON")]
    pub policy_config_json: Vec<u8>,
    #[serde(rename = "NotificationConfigXML")]
    pub notification_config_xml: Vec<u8>,
    #[serde(rename = "LifecycleConfigXML")]
    pub lifecycle_config_xml: Vec<u8>,
    #[serde(rename = "ObjectLockConfigXML")]
    pub object_lock_config_xml: Vec<u8>,
    #[serde(rename = "VersioningConfigXML")]
    pub versioning_config_xml: Vec<u8>,
    #[serde(rename = "EncryptionConfigXML")]
    pub encryption_config_xml: Vec<u8>,
    #[serde(rename = "TaggingConfigXML")]
    pub tagging_config_xml: Vec<u8>,
    #[serde(rename = "QuotaConfigJSON")]
    pub quota_config_json: Vec<u8>,
    #[serde(rename = "ReplicationConfigXML")]
    pub replication_config_xml: Vec<u8>,
    #[serde(rename = "BucketTargetsConfigJSON")]
    pub bucket_targets_config_json: Vec<u8>,
    #[serde(rename = "BucketTargetsConfigMetaJSON")]
    pub bucket_targets_config_meta_json: Vec<u8>,
    pub cors_config_xml: Vec<u8>,
    pub public_access_block_config_xml: Vec<u8>,
    pub bucket_acl_config_json: Vec<u8>,

    #[serde(with = "msgp_time")]
    pub policy_config_updated_at: OffsetDateTime,
    #[serde(with = "msgp_time")]
    pub object_lock_config_updated_at: OffsetDateTime,
    #[serde(with = "msgp_time")]
    pub encryption_config_updated_at: OffsetDateTime,
    #[serde(with = "msgp_time")]
    pub tagging_config_updated_at: OffsetDateTime,
    #[serde(with = "msgp_time")]
    pub quota_config_updated_at: OffsetDateTime,
    #[serde(with = "msgp_time")]
    pub replication_config_updated_at: OffsetDateTime,
    #[serde(with = "msgp_time")]
    pub versioning_config_updated_at: OffsetDateTime,
    #[serde(with = "msgp_time")]
    pub lifecycle_config_updated_at: OffsetDateTime,
    #[serde(with = "msgp_time")]
    pub notification_config_updated_at: OffsetDateTime,
    #[serde(with = "msgp_time")]
    pub bucket_targets_config_updated_at: OffsetDateTime,
    #[serde(with = "msgp_time")]
    pub bucket_targets_config_meta_updated_at: OffsetDateTime,
    #[serde(with = "msgp_time")]
    pub cors_config_updated_at: OffsetDateTime,
    #[serde(with = "msgp_time")]
    pub public_access_block_config_updated_at: OffsetDateTime,
    #[serde(with = "msgp_time")]
    pub bucket_acl_config_updated_at: OffsetDateTime,

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
    #[serde(skip)]
    pub public_access_block_config: Option<PublicAccessBlockConfiguration>,
    #[serde(skip)]
    pub bucket_acl_config: Option<String>,
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
            public_access_block_config_xml: Default::default(),
            bucket_acl_config_json: Default::default(),
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
            public_access_block_config_updated_at: OffsetDateTime::UNIX_EPOCH,
            bucket_acl_config_updated_at: OffsetDateTime::UNIX_EPOCH,
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
            public_access_block_config: Default::default(),
            bucket_acl_config: Default::default(),
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
        if self.public_access_block_config_updated_at == OffsetDateTime::UNIX_EPOCH {
            self.public_access_block_config_updated_at = self.created
        }
        if self.bucket_acl_config_updated_at == OffsetDateTime::UNIX_EPOCH {
            self.bucket_acl_config_updated_at = self.created
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
            BUCKET_PUBLIC_ACCESS_BLOCK_CONFIG => {
                self.public_access_block_config_xml = data;
                self.public_access_block_config_updated_at = updated;
            }
            BUCKET_ACL_CONFIG => {
                self.bucket_acl_config_json = data;
                self.bucket_acl_config_updated_at = updated;
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
        if !self.public_access_block_config_xml.is_empty() {
            self.public_access_block_config =
                Some(deserialize::<PublicAccessBlockConfiguration>(&self.public_access_block_config_xml)?);
        }
        if !self.bucket_acl_config_json.is_empty() {
            let acl = String::from_utf8(self.bucket_acl_config_json.clone())
                .map_err(|e| Error::other(format!("invalid UTF-8 in bucket ACL: {}", e)))?;
            self.bucket_acl_config = Some(acl);
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
