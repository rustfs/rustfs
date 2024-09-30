use crate::error::{Error, Result};
use rmp_serde::Serializer as rmpSerializer;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// 定义tagSet结构体
#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct TagSet {
    #[serde(rename = "Tag")]
    tag_map: HashMap<String, String>,
    is_object: bool,
}

// 定义tagging结构体
#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct Tags {
    #[serde(rename = "Tagging")]
    xml_name: String,
    #[serde(rename = "TagSet")]
    tag_set: Option<TagSet>,
}

impl Tags {
    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();

        self.serialize(&mut rmpSerializer::new(&mut buf).with_struct_map())?;

        Ok(buf)
    }

    pub fn unmarshal(buf: &[u8]) -> Result<Self> {
        let t: Tags = rmp_serde::from_slice(buf)?;
        Ok(t)
    }
}
