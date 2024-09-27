use std::collections::HashMap;

use rmp_serde::Serializer;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use super::{
    encryption::BucketSSEConfig, event, lifecycle::lifecycle::Lifecycle, objectlock, policy::bucket_policy::BucketPolicy,
    quota::BucketQuota, replication, tags::Tags, target::BucketTargets, versioning::Versioning,
};

use crate::error::Result;

use crate::disk::BUCKET_META_PREFIX;

pub const BUCKET_METADATA_FILE: &str = ".metadata.bin";
pub const BUCKET_METADATA_FORMAT: u16 = 1;
pub const BUCKET_METADATA_VERSION: u16 = 1;

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct BucketMetadata {
    format: u16,
    version: u16,
    pub name: String,
    pub created: Option<OffsetDateTime>,
    pub lock_enabled: bool, // 虽然标记为不使用，但可能需要保留
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

    pub policy_config_updated_at: Option<OffsetDateTime>,
    pub object_lock_config_updated_at: Option<OffsetDateTime>,
    pub encryption_config_updated_at: Option<OffsetDateTime>,
    pub tagging_config_updated_at: Option<OffsetDateTime>,
    pub quota_config_updated_at: Option<OffsetDateTime>,
    pub replication_config_updated_at: Option<OffsetDateTime>,
    pub versioning_config_updated_at: Option<OffsetDateTime>,
    pub lifecycle_config_updated_at: Option<OffsetDateTime>,
    pub notification_config_updated_at: Option<OffsetDateTime>,
    pub bucket_targets_config_updated_at: Option<OffsetDateTime>,
    pub bucket_targets_config_meta_updated_at: Option<OffsetDateTime>,

    // 新增UpdatedAt字段
    pub new_field_updated_at: Option<OffsetDateTime>,

    // 私有字段，需要原子更新
    pub policy_config: Option<BucketPolicy>, // 假设Policy是一个已经定义好的结构体
    pub notification_config: Option<event::Config>,
    pub lifecycle_config: Option<Lifecycle>,
    pub object_lock_config: Option<objectlock::Config>,
    pub versioning_config: Option<Versioning>,
    pub sse_config: Option<BucketSSEConfig>,
    pub tagging_config: Option<Tags>,
    pub quota_config: Option<BucketQuota>,
    pub replication_config: Option<replication::Config>,
    pub bucket_target_config: Option<BucketTargets>,
    pub bucket_target_config_meta: Option<HashMap<String, String>>,
}

impl BucketMetadata {
    pub fn new(name: &str) -> Self {
        BucketMetadata {
            format: BUCKET_METADATA_FORMAT,
            version: BUCKET_METADATA_VERSION,
            name: name.to_string(),
            ..Default::default()
        }
    }

    pub fn save_file_path(&self) -> String {
        format!("{}/{}/{}", BUCKET_META_PREFIX, self.name.as_str(), BUCKET_METADATA_FILE)
    }

    fn msg_size(&self) -> usize {
        unimplemented!()
    }

    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        unimplemented!()
    }

    pub fn unmarshal(_buf: &[u8]) -> Result<Self> {
        unimplemented!()
    }
}
