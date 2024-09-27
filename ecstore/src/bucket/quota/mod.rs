use serde::{Deserialize, Serialize};

// 定义QuotaType枚举类型
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum QuotaType {
    Hard,
}

// 定义BucketQuota结构体
#[derive(Serialize, Deserialize, Debug)]
pub struct BucketQuota {
    quota: Option<u64>, // 使用Option来表示可能不存在的字段

    size: u64,

    rate: u64,

    requests: u64,

    quota_type: Option<QuotaType>,
}
