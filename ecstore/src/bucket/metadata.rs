use byteorder::{BigEndian, ByteOrder};
use rmp_serde::Serializer as rmpSerializer;
use serde::Serializer;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;
use std::fmt::Display;
use std::str::FromStr;
use time::macros::datetime;
use time::OffsetDateTime;

use super::{
    encryption::BucketSSEConfig, event, lifecycle::lifecycle::Lifecycle, objectlock, policy::bucket_policy::BucketPolicy,
    quota::BucketQuota, replication, tags::Tags, target::BucketTargets, versioning::Versioning,
};

use crate::error::Result;

use crate::disk::BUCKET_META_PREFIX;
use crate::utils::crypto::hex;

pub const BUCKET_METADATA_FILE: &str = ".metadata.bin";
pub const BUCKET_METADATA_FORMAT: u16 = 1;
pub const BUCKET_METADATA_VERSION: u16 = 1;

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase", default)]
pub struct BucketMetadata {
    pub name: String,
    #[serde(serialize_with = "write_times")]
    pub created: OffsetDateTime,
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

    #[serde(serialize_with = "write_times")]
    pub policy_config_updated_at: OffsetDateTime,
    #[serde(serialize_with = "write_times")]
    pub object_lock_config_updated_at: OffsetDateTime,
    #[serde(serialize_with = "write_times")]
    pub encryption_config_updated_at: OffsetDateTime,
    #[serde(serialize_with = "write_times")]
    pub tagging_config_updated_at: OffsetDateTime,
    #[serde(serialize_with = "write_times")]
    pub quota_config_updated_at: OffsetDateTime,
    #[serde(serialize_with = "write_times")]
    pub replication_config_updated_at: OffsetDateTime,
    #[serde(serialize_with = "write_times")]
    pub versioning_config_updated_at: OffsetDateTime,
    #[serde(serialize_with = "write_times")]
    pub lifecycle_config_updated_at: OffsetDateTime,
    #[serde(serialize_with = "write_times")]
    pub notification_config_updated_at: OffsetDateTime,
    #[serde(serialize_with = "write_times")]
    pub bucket_targets_config_updated_at: OffsetDateTime,
    #[serde(serialize_with = "write_times")]
    pub bucket_targets_config_meta_updated_at: OffsetDateTime,

    #[serde(skip)]
    pub new_field_updated_at: OffsetDateTime,

    #[serde(skip)]
    pub policy_config: Option<BucketPolicy>,
    #[serde(skip)]
    pub notification_config: Option<event::Config>,
    #[serde(skip)]
    pub lifecycle_config: Option<Lifecycle>,
    #[serde(skip)]
    pub object_lock_config: Option<objectlock::Config>,
    #[serde(skip)]
    pub versioning_config: Option<Versioning>,
    #[serde(skip)]
    pub sse_config: Option<BucketSSEConfig>,
    #[serde(skip)]
    pub tagging_config: Option<Tags>,
    #[serde(skip)]
    pub quota_config: Option<BucketQuota>,
    #[serde(skip)]
    pub replication_config: Option<replication::Config>,
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

    fn msg_size(&self) -> usize {
        unimplemented!()
    }

    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();

        self.serialize(&mut rmpSerializer::new(&mut buf).with_struct_map())?;

        Ok(buf)
    }

    pub fn unmarshal(_buf: &[u8]) -> Result<Self> {
        unimplemented!()
    }
}

fn deserialize_from_str<'de, S, D>(deserializer: D) -> core::result::Result<S, D::Error>
where
    S: FromStr,
    S::Err: Display,
    D: Deserializer<'de>,
{
    // let s: String = Deserialize::deserialize(deserializer)?;
    // S::from_str(&s).map_err(de::Error::custom)
    unimplemented!()
}

fn write_times<S>(t: &OffsetDateTime, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut buf = vec![0x0; 15];

    let sec = t.unix_timestamp() - 62135596800;
    let nsec = t.nanosecond();
    buf[0] = 0xc7;
    buf[1] = 0x0c;
    buf[2] = 0x05;
    BigEndian::write_u64(&mut buf[3..], sec as u64);
    BigEndian::write_u32(&mut buf[11..], nsec as u32);
    s.serialize_bytes(&buf)
}

fn write_time(t: OffsetDateTime) -> Result<(), String> {
    // let t = t.saturating_sub(0); // 转换为自 UNIX_EPOCH 以来的时间
    println!("t:{:?}", t);
    println!("offset:{:?}", datetime!(0-01-01 0:00 UTC));

    let mut buf = vec![0x0; 15];

    let sec = t.unix_timestamp() - 62135596800;
    let nsec = t.nanosecond();

    println!("sec:{:?}", sec);
    println!("nsec:{:?}", nsec);

    buf[0] = 0xc7;
    buf[1] = 0x0c;
    buf[2] = 0x05;
    // 扩展格式的时间类型
    // buf.push(0xc7); // mext8
    // buf.push(12); // 长度
    // buf.push(0x05); // 时间扩展类型

    // 将 Unix 时间戳和纳秒部分写入缓冲区
    BigEndian::write_u64(&mut buf[3..], sec as u64);
    BigEndian::write_u32(&mut buf[11..], nsec as u32);

    println!("hex:{:?}", hex(buf));

    Ok(())
}

#[cfg(test)]
mod test {
    use crate::utils::crypto::hex;

    use super::*;

    #[tokio::test]
    async fn marshal_msg() {
        // write_time(OffsetDateTime::UNIX_EPOCH).unwrap();

        let bm = BucketMetadata::new("dada");

        let buf = bm.marshal_msg().unwrap();

        println!("{:?}", hex(buf))
    }
}
