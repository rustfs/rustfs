use super::tag::Tag;
use serde::{Deserialize, Serialize};

// 定义And结构体
#[derive(Debug, Deserialize, Serialize, Default,Clone)]
pub struct And {
    prefix: Option<String>,
    tags: Option<Vec<Tag>>,
}
