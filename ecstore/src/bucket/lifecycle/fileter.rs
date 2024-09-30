use std::collections::HashMap;

use super::{and::And, prefix::Prefix, tag::Tag};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Default,Clone)]
pub struct Filter {
    pub set: bool,

    pub prefix: Prefix,

    pub object_size_greater_than: Option<i64>,
    pub object_size_less_than: Option<i64>,

    pub and_condition: And,
    pub and_set: bool,

    pub tag: Tag,
    pub tag_set: bool,

    // 使用HashMap存储缓存的标签
    pub cached_tags: HashMap<String, String>,
}
