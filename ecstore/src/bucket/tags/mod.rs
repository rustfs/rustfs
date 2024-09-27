use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// 定义tagSet结构体
#[derive(Serialize, Deserialize, Debug)]
pub struct TagSet {
    #[serde(rename = "Tag")]
    tag_map: HashMap<String, String>,
    is_object: bool,
}

// 定义tagging结构体
#[derive(Serialize, Deserialize, Debug)]
pub struct Tags {
    #[serde(rename = "Tagging")]
    xml_name: String,
    #[serde(rename = "TagSet")]
    tag_set: Option<TagSet>,
}
