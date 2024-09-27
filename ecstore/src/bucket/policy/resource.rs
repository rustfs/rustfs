use std::{collections::HashSet, default};

// 定义ResourceARNType枚举类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ResourceARNType {
    #[default]
    UnknownARN,
    ResourceARNS3,
    ResourceARNKMS,
}

// 定义资源ARN前缀
const RESOURCE_ARN_PREFIX: &str = "arn:aws:s3:::";
const RESOURCE_ARN_KMS_PREFIX: &str = "arn:minio:kms::::";

// 定义Resource结构体
#[derive(Debug, Default)]
pub struct Resource {
    pattern: String,
    r#type: ResourceARNType,
}

#[derive(Debug, Default)]
pub struct ResourceSet(HashSet<Resource>);
