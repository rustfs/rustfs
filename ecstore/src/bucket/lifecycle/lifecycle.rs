use super::rule::Rule;
use crate::error::Result;
use rmp_serde::Serializer as rmpSerializer;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct Lifecycle {
    pub rules: Vec<Rule>,
    pub expiry_updated_at: Option<OffsetDateTime>,
}

impl Lifecycle {
    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();

        self.serialize(&mut rmpSerializer::new(&mut buf).with_struct_map())?;

        Ok(buf)
    }

    pub fn unmarshal(buf: &[u8]) -> Result<Self> {
        let t: Lifecycle = rmp_serde::from_slice(buf)?;
        Ok(t)
    }
}
