use common::error::Result;
use rmp_serde::Serializer as rmpSerializer;
use serde::{Deserialize, Serialize};

// 定义 QuotaType 枚举类型
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum QuotaType {
    Hard,
}

// 定义 BucketQuota 结构体
#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct BucketQuota {
    quota: Option<u64>, // 使用 Option 来表示可能不存在的字段

    size: u64,

    rate: u64,

    requests: u64,

    quota_type: Option<QuotaType>,
}

impl BucketQuota {
    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();

        self.serialize(&mut rmpSerializer::new(&mut buf).with_struct_map())?;

        Ok(buf)
    }

    pub fn unmarshal(buf: &[u8]) -> Result<Self> {
        let t: BucketQuota = rmp_serde::from_slice(buf)?;
        Ok(t)
    }
}
