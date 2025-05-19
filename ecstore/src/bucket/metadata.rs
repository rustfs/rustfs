use super::{quota::BucketQuota, target::BucketTargets};

use super::object_lock::ObjectLockApi;
use super::versioning::VersioningApi;
use byteorder::{BigEndian, ByteOrder, LittleEndian};
use policy::policy::BucketPolicy;
use rmp_serde::Serializer as rmpSerializer;
use s3s::dto::{
    BucketLifecycleConfiguration, NotificationConfiguration, ObjectLockConfiguration, ReplicationConfiguration,
    ServerSideEncryptionConfiguration, Tagging, VersioningConfiguration,
};
use serde::Serializer;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use time::OffsetDateTime;
use tracing::error;

use crate::config::com::{read_config, save_config};
use crate::{config, new_object_layer_fn};
use common::error::{Error, Result};

use crate::disk::BUCKET_META_PREFIX;
use crate::store::ECStore;
use crate::utils::xml::deserialize;

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

#[derive(Debug, Deserialize, Serialize)]
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
            return Err(Error::msg("read_bucket_metadata: data invalid"));
        }

        let format = LittleEndian::read_u16(&buf[0..2]);
        let version = LittleEndian::read_u16(&buf[2..4]);

        match format {
            BUCKET_METADATA_FORMAT => {}
            _ => return Err(Error::msg("read_bucket_metadata: format invalid")),
        }

        match version {
            BUCKET_METADATA_VERSION => {}
            _ => return Err(Error::msg("read_bucket_metadata: version invalid")),
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
                self.tagging_config_xml = data;
                self.tagging_config_updated_at = updated;
            }
            _ => return Err(Error::msg(format!("config file not found : {}", config_file))),
        }

        Ok(updated)
    }

    pub fn set_created(&mut self, created: Option<OffsetDateTime>) {
        self.created = {
            match created {
                Some(t) => t,
                None => OffsetDateTime::now_utc(),
            }
        }
    }

    pub async fn save(&mut self) -> Result<()> {
        let Some(store) = new_object_layer_fn() else { return Err(Error::msg("errServerNotInitialized")) };

        self.parse_all_configs(store.clone())?;

        let mut buf: Vec<u8> = vec![0; 4];

        LittleEndian::write_u16(&mut buf[0..2], BUCKET_METADATA_FORMAT);

        LittleEndian::write_u16(&mut buf[2..4], BUCKET_METADATA_VERSION);

        let data = self.marshal_msg()?;

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
            self.quota_config = Some(BucketQuota::unmarshal(&self.quota_config_json)?);
        }
        if !self.replication_config_xml.is_empty() {
            self.replication_config = Some(deserialize::<ReplicationConfiguration>(&self.replication_config_xml)?);
        }
        if !self.bucket_targets_config_json.is_empty() {
            self.bucket_target_config = Some(BucketTargets::unmarshal(&self.bucket_targets_config_json)?);
        } else {
            self.bucket_target_config = Some(BucketTargets::default())
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
            if !config::error::is_err_config_not_found(&err) {
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
        return Err(Error::msg("invalid argument"));
    }

    let bm = BucketMetadata::new(bucket);
    let file_path = bm.save_file_path();

    let data = read_config(api, &file_path).await?;

    BucketMetadata::check_header(&data)?;

    let bm = BucketMetadata::unmarshal(&data[4..])?;

    Ok(bm)
}

fn _write_time<S>(t: &OffsetDateTime, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut buf = vec![0x0; 15];

    let sec = t.unix_timestamp() - 62135596800;
    let nsec = t.nanosecond();
    buf[0] = 0xc7; // mext8
    buf[1] = 0x0c; // 长度
    buf[2] = 0x05; // 时间扩展类型
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
}
