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
#[derive(Serialize, Deserialize, Debug)]
pub struct EncryptionAction {
    algorithm: Option<Algorithm>,
    master_key_id: Option<String>,
}

// 定义Rule结构体
#[derive(Serialize, Deserialize, Debug)]
pub struct Rule {
    default_encryption_action: EncryptionAction,
}

// 定义BucketSSEConfig结构体
#[derive(Serialize, Deserialize, Debug)]
pub struct BucketSSEConfig {
    xml_ns: String,
    xml_name: String,
    rules: Vec<Rule>,
}
