use crate::error::Result;
use rmp_serde::Serializer as rmpSerializer;
use serde::{Deserialize, Serialize};

// 定义Algorithm枚举类型
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Algorithm {
    AES256,
    AWSKms,
}

// 实现从字符串到Algorithm的转换
impl std::str::FromStr for Algorithm {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "AES256" => Ok(Algorithm::AES256),
            "aws:kms" => Ok(Algorithm::AWSKms),
            _ => Err(format!("未知的 SSE 算法: {}", s)),
        }
    }
}

// 定义EncryptionAction结构体
#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct EncryptionAction {
    algorithm: Option<Algorithm>,
    master_key_id: Option<String>,
}

// 定义Rule结构体
#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct Rule {
    default_encryption_action: EncryptionAction,
}

// 定义BucketSSEConfig结构体
#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct BucketSSEConfig {
    xml_ns: String,
    xml_name: String,
    rules: Vec<Rule>,
}

impl BucketSSEConfig {
    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();

        self.serialize(&mut rmpSerializer::new(&mut buf).with_struct_map())?;

        Ok(buf)
    }

    pub fn unmarshal(buf: &[u8]) -> Result<Self> {
        let t: BucketSSEConfig = rmp_serde::from_slice(buf)?;
        Ok(t)
    }
}
