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

use super::msgp_decode::{read_msgp_ext8_time, skip_msgp_value, write_msgp_time};
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
use rustfs_policy::policy::BucketPolicy;
use s3s::dto::{
    BucketLifecycleConfiguration, CORSConfiguration, NotificationConfiguration, ObjectLockConfiguration,
    PublicAccessBlockConfiguration, ReplicationConfiguration, ServerSideEncryptionConfiguration, Tagging,
    VersioningConfiguration,
};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::sync::Arc;
use time::OffsetDateTime;
use tracing::error;

fn read_msgp_str<R: Read>(rd: &mut R) -> Result<String> {
    let len = rmp::decode::read_str_len(rd)? as usize;
    let mut buf = vec![0u8; len];
    rd.read_exact(&mut buf)?;
    Ok(String::from_utf8(buf)?)
}

fn read_msgp_time_value<R: Read>(rd: &mut R) -> Result<OffsetDateTime> {
    let marker = rmp::decode::read_marker(rd).map_err(|e| Error::other(format!("{e:?}")))?;
    match marker {
        rmp::Marker::Null => Ok(OffsetDateTime::UNIX_EPOCH),
        rmp::Marker::Ext8 => read_msgp_ext8_time(rd),
        _ => Err(Error::other(format!("expected time ext or nil, got marker: {marker:?}"))),
    }
}

fn read_msgp_bin<R: Read>(rd: &mut R) -> Result<Vec<u8>> {
    let len = rmp::decode::read_bin_len(rd)? as usize;
    let mut buf = vec![0u8; len];
    rd.read_exact(&mut buf)?;
    Ok(buf)
}

fn write_bin_field<W: Write>(wr: &mut W, key: &str, val: &[u8]) -> Result<()> {
    rmp::encode::write_str(wr, key)?;
    rmp::encode::write_bin(wr, val)?;
    Ok(())
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

#[derive(Debug, Clone)]
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
    pub public_access_block_config_xml: Vec<u8>,
    pub bucket_acl_config_json: Vec<u8>,

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
    pub public_access_block_config_updated_at: OffsetDateTime,
    pub bucket_acl_config_updated_at: OffsetDateTime,

    pub new_field_updated_at: OffsetDateTime,

    pub policy_config: Option<BucketPolicy>,
    pub notification_config: Option<NotificationConfiguration>,
    pub lifecycle_config: Option<BucketLifecycleConfiguration>,
    pub object_lock_config: Option<ObjectLockConfiguration>,
    pub versioning_config: Option<VersioningConfiguration>,
    pub sse_config: Option<ServerSideEncryptionConfiguration>,
    pub tagging_config: Option<Tagging>,
    pub quota_config: Option<BucketQuota>,
    pub replication_config: Option<ReplicationConfiguration>,
    pub bucket_target_config: Option<BucketTargets>,
    pub bucket_target_config_meta: Option<HashMap<String, String>>,
    pub cors_config: Option<CORSConfiguration>,
    pub public_access_block_config: Option<PublicAccessBlockConfiguration>,
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

    /// Decode from msgp bytes. Field order follows MinIO BucketMetadata for compatibility.
    pub fn decode_from<R: Read>(&mut self, rd: &mut R) -> Result<()> {
        let mut fields = rmp::decode::read_map_len(rd)?;
        *self = Self::default();

        while fields > 0 {
            fields -= 1;

            let key_len = rmp::decode::read_str_len(rd)?;
            let mut key_buf = vec![0u8; key_len as usize];
            rd.read_exact(&mut key_buf)?;
            let key = String::from_utf8(key_buf)?;

            match key.as_str() {
                "Name" => self.name = read_msgp_str(rd)?,
                "Created" => self.created = read_msgp_time_value(rd)?,
                "LockEnabled" => self.lock_enabled = rmp::decode::read_bool(rd)?,
                "PolicyConfigJSON" => self.policy_config_json = read_msgp_bin(rd)?,
                "NotificationConfigXML" => self.notification_config_xml = read_msgp_bin(rd)?,
                "LifecycleConfigXML" => self.lifecycle_config_xml = read_msgp_bin(rd)?,
                "ObjectLockConfigXML" => self.object_lock_config_xml = read_msgp_bin(rd)?,
                "VersioningConfigXML" => self.versioning_config_xml = read_msgp_bin(rd)?,
                "EncryptionConfigXML" => self.encryption_config_xml = read_msgp_bin(rd)?,
                "TaggingConfigXML" => self.tagging_config_xml = read_msgp_bin(rd)?,
                "QuotaConfigJSON" => self.quota_config_json = read_msgp_bin(rd)?,
                "ReplicationConfigXML" => self.replication_config_xml = read_msgp_bin(rd)?,
                "BucketTargetsConfigJSON" => self.bucket_targets_config_json = read_msgp_bin(rd)?,
                "BucketTargetsConfigMetaJSON" => self.bucket_targets_config_meta_json = read_msgp_bin(rd)?,
                "PolicyConfigUpdatedAt" => self.policy_config_updated_at = read_msgp_time_value(rd)?,
                "ObjectLockConfigUpdatedAt" => self.object_lock_config_updated_at = read_msgp_time_value(rd)?,
                "EncryptionConfigUpdatedAt" => self.encryption_config_updated_at = read_msgp_time_value(rd)?,
                "TaggingConfigUpdatedAt" => self.tagging_config_updated_at = read_msgp_time_value(rd)?,
                "QuotaConfigUpdatedAt" => self.quota_config_updated_at = read_msgp_time_value(rd)?,
                "ReplicationConfigUpdatedAt" => self.replication_config_updated_at = read_msgp_time_value(rd)?,
                "VersioningConfigUpdatedAt" => self.versioning_config_updated_at = read_msgp_time_value(rd)?,
                "LifecycleConfigUpdatedAt" => self.lifecycle_config_updated_at = read_msgp_time_value(rd)?,
                "NotificationConfigUpdatedAt" => self.notification_config_updated_at = read_msgp_time_value(rd)?,
                "BucketTargetsConfigUpdatedAt" => self.bucket_targets_config_updated_at = read_msgp_time_value(rd)?,
                "BucketTargetsConfigMetaUpdatedAt" => self.bucket_targets_config_meta_updated_at = read_msgp_time_value(rd)?,
                "CorsConfigXML" => self.cors_config_xml = read_msgp_bin(rd)?,
                "PublicAccessBlockConfigXML" => self.public_access_block_config_xml = read_msgp_bin(rd)?,
                "BucketAclConfigJSON" => self.bucket_acl_config_json = read_msgp_bin(rd)?,
                "CorsConfigUpdatedAt" => self.cors_config_updated_at = read_msgp_time_value(rd)?,
                "PublicAccessBlockConfigUpdatedAt" => self.public_access_block_config_updated_at = read_msgp_time_value(rd)?,
                "BucketAclConfigUpdatedAt" => self.bucket_acl_config_updated_at = read_msgp_time_value(rd)?,
                other => {
                    tracing::debug!(field = %other, "BucketMetadata decode_from: skipping unknown field");
                    skip_msgp_value(rd)?;
                }
            }
        }

        Ok(())
    }

    /// Encode to msgp bytes. Field order follows MinIO BucketMetadata for compatibility.
    pub fn encode_to<W: Write>(&self, wr: &mut W) -> Result<()> {
        // Map size: MinIO fields (25) + RustFS extensions (6)
        let map_len: u32 = 31;
        rmp::encode::write_map_len(wr, map_len)?;

        // MinIO field order (same as Go struct)
        rmp::encode::write_str(wr, "Name")?;
        rmp::encode::write_str(wr, &self.name)?;

        rmp::encode::write_str(wr, "Created")?;
        write_msgp_time(wr, self.created)?;

        rmp::encode::write_str(wr, "LockEnabled")?;
        rmp::encode::write_bool(wr, self.lock_enabled)?;

        write_bin_field(wr, "PolicyConfigJSON", &self.policy_config_json)?;
        write_bin_field(wr, "NotificationConfigXML", &self.notification_config_xml)?;
        write_bin_field(wr, "LifecycleConfigXML", &self.lifecycle_config_xml)?;
        write_bin_field(wr, "ObjectLockConfigXML", &self.object_lock_config_xml)?;
        write_bin_field(wr, "VersioningConfigXML", &self.versioning_config_xml)?;
        write_bin_field(wr, "EncryptionConfigXML", &self.encryption_config_xml)?;
        write_bin_field(wr, "TaggingConfigXML", &self.tagging_config_xml)?;
        write_bin_field(wr, "QuotaConfigJSON", &self.quota_config_json)?;
        write_bin_field(wr, "ReplicationConfigXML", &self.replication_config_xml)?;
        write_bin_field(wr, "BucketTargetsConfigJSON", &self.bucket_targets_config_json)?;
        write_bin_field(wr, "BucketTargetsConfigMetaJSON", &self.bucket_targets_config_meta_json)?;

        rmp::encode::write_str(wr, "PolicyConfigUpdatedAt")?;
        write_msgp_time(wr, self.policy_config_updated_at)?;
        rmp::encode::write_str(wr, "ObjectLockConfigUpdatedAt")?;
        write_msgp_time(wr, self.object_lock_config_updated_at)?;
        rmp::encode::write_str(wr, "EncryptionConfigUpdatedAt")?;
        write_msgp_time(wr, self.encryption_config_updated_at)?;
        rmp::encode::write_str(wr, "TaggingConfigUpdatedAt")?;
        write_msgp_time(wr, self.tagging_config_updated_at)?;
        rmp::encode::write_str(wr, "QuotaConfigUpdatedAt")?;
        write_msgp_time(wr, self.quota_config_updated_at)?;
        rmp::encode::write_str(wr, "ReplicationConfigUpdatedAt")?;
        write_msgp_time(wr, self.replication_config_updated_at)?;
        rmp::encode::write_str(wr, "VersioningConfigUpdatedAt")?;
        write_msgp_time(wr, self.versioning_config_updated_at)?;
        rmp::encode::write_str(wr, "LifecycleConfigUpdatedAt")?;
        write_msgp_time(wr, self.lifecycle_config_updated_at)?;
        rmp::encode::write_str(wr, "NotificationConfigUpdatedAt")?;
        write_msgp_time(wr, self.notification_config_updated_at)?;
        rmp::encode::write_str(wr, "BucketTargetsConfigUpdatedAt")?;
        write_msgp_time(wr, self.bucket_targets_config_updated_at)?;
        rmp::encode::write_str(wr, "BucketTargetsConfigMetaUpdatedAt")?;
        write_msgp_time(wr, self.bucket_targets_config_meta_updated_at)?;

        // RustFS extensions
        write_bin_field(wr, "CorsConfigXML", &self.cors_config_xml)?;
        write_bin_field(wr, "PublicAccessBlockConfigXML", &self.public_access_block_config_xml)?;
        write_bin_field(wr, "BucketAclConfigJSON", &self.bucket_acl_config_json)?;
        rmp::encode::write_str(wr, "CorsConfigUpdatedAt")?;
        write_msgp_time(wr, self.cors_config_updated_at)?;
        rmp::encode::write_str(wr, "PublicAccessBlockConfigUpdatedAt")?;
        write_msgp_time(wr, self.public_access_block_config_updated_at)?;
        rmp::encode::write_str(wr, "BucketAclConfigUpdatedAt")?;
        write_msgp_time(wr, self.bucket_acl_config_updated_at)?;

        Ok(())
    }

    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        self.encode_to(&mut buf)?;
        Ok(buf)
    }

    pub fn unmarshal(buf: &[u8]) -> Result<Self> {
        let mut bm = Self::default();
        let mut cur = std::io::Cursor::new(buf);
        bm.decode_from(&mut cur)?;
        Ok(bm)
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
        if !self.policy_config_json.is_empty()
            && let Err(e) = serde_json::from_slice(&self.policy_config_json).map(|c| self.policy_config = Some(c))
        {
            tracing::warn!(bucket = %self.name, config = "policy", error = %e, "parse_all_configs: failed to parse");
        }
        if !self.notification_config_xml.is_empty()
            && let Err(e) = deserialize::<NotificationConfiguration>(&self.notification_config_xml)
                .map(|c| self.notification_config = Some(c))
        {
            tracing::warn!(bucket = %self.name, config = "notification", error = %e, "parse_all_configs: failed to parse");
        }
        if !self.lifecycle_config_xml.is_empty()
            && let Err(e) =
                deserialize::<BucketLifecycleConfiguration>(&self.lifecycle_config_xml).map(|c| self.lifecycle_config = Some(c))
        {
            tracing::warn!(bucket = %self.name, config = "lifecycle", error = %e, "parse_all_configs: failed to parse");
        }
        if !self.object_lock_config_xml.is_empty()
            && let Err(e) =
                deserialize::<ObjectLockConfiguration>(&self.object_lock_config_xml).map(|c| self.object_lock_config = Some(c))
        {
            tracing::warn!(bucket = %self.name, config = "object_lock", error = %e, "parse_all_configs: failed to parse");
        }
        if !self.versioning_config_xml.is_empty()
            && let Err(e) =
                deserialize::<VersioningConfiguration>(&self.versioning_config_xml).map(|c| self.versioning_config = Some(c))
        {
            tracing::warn!(bucket = %self.name, config = "versioning", error = %e, "parse_all_configs: failed to parse");
        }
        if !self.encryption_config_xml.is_empty()
            && let Err(e) =
                deserialize::<ServerSideEncryptionConfiguration>(&self.encryption_config_xml).map(|c| self.sse_config = Some(c))
        {
            tracing::warn!(bucket = %self.name, config = "encryption", error = %e, "parse_all_configs: failed to parse");
        }
        if !self.tagging_config_xml.is_empty()
            && let Err(e) = deserialize::<Tagging>(&self.tagging_config_xml).map(|c| self.tagging_config = Some(c))
        {
            tracing::warn!(bucket = %self.name, config = "tagging", error = %e, "parse_all_configs: failed to parse");
        }
        if !self.quota_config_json.is_empty()
            && let Err(e) = serde_json::from_slice(&self.quota_config_json).map(|c| self.quota_config = Some(c))
        {
            tracing::warn!(bucket = %self.name, config = "quota", error = %e, "parse_all_configs: failed to parse");
        }
        if !self.replication_config_xml.is_empty()
            && let Err(e) =
                deserialize::<ReplicationConfiguration>(&self.replication_config_xml).map(|c| self.replication_config = Some(c))
        {
            tracing::warn!(bucket = %self.name, config = "replication", error = %e, "parse_all_configs: failed to parse");
        }
        if !self.bucket_targets_config_json.is_empty() {
            if let Err(e) = serde_json::from_slice::<BucketTargets>(&self.bucket_targets_config_json)
                .map(|t| self.bucket_target_config = Some(t))
            {
                tracing::warn!(bucket = %self.name, config = "bucket_targets", error = %e, "parse_all_configs: failed to parse");
                self.bucket_target_config = Some(BucketTargets::default());
            }
        } else {
            self.bucket_target_config = Some(BucketTargets::default());
        }
        if !self.cors_config_xml.is_empty()
            && let Err(e) = deserialize::<CORSConfiguration>(&self.cors_config_xml).map(|c| self.cors_config = Some(c))
        {
            tracing::warn!(bucket = %self.name, config = "cors", error = %e, "parse_all_configs: failed to parse");
        }
        if !self.public_access_block_config_xml.is_empty()
            && let Err(e) = deserialize::<PublicAccessBlockConfiguration>(&self.public_access_block_config_xml)
                .map(|c| self.public_access_block_config = Some(c))
        {
            tracing::warn!(bucket = %self.name, config = "public_access_block", error = %e, "parse_all_configs: failed to parse");
        }
        if !self.bucket_acl_config_json.is_empty()
            && let Err(e) = String::from_utf8(self.bucket_acl_config_json.clone()).map(|acl| self.bucket_acl_config = Some(acl))
        {
            tracing::warn!(bucket = %self.name, config = "bucket_acl", error = %e, "parse_all_configs: failed to parse");
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
