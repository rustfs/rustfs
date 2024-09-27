use time::OffsetDateTime;

use super::{
    encryption::BucketSSEConfig, event, lifecycle::lifecycle::Lifecycle, objectlock, policy::bucket_policy::BucketPolicy,
    quota::BucketQuota, replication, tags::Tags, versioning::Versioning,
};

#[derive(Debug, Default)]
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
    new_field_updated_at: Option<OffsetDateTime>,

    // 私有字段，需要原子更新
    policy_config: Option<BucketPolicy>, // 假设Policy是一个已经定义好的结构体
    notification_config: Option<event::config::Config>,
    lifecycle_config: Option<Lifecycle>,
    object_lock_config: Option<objectlock::Config>,
    versioning_config: Option<Versioning>,
    sse_config: Option<BucketSSEConfig>,
    tagging_config: Option<Tags>,
    quota_config: Option<BucketQuota>,
    replication_config: Option<replication::Config>,
    bucket_target_config: Option<BucketTargets>,
    bucket_target_config_meta: Option<std::collections::HashMap<String, String>>,
}

#[derive(Debug, Default)]
struct BucketTargets;
