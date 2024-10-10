mod and;
mod filter;
mod rule;
mod tag;

use crate::error::Result;
use rmp_serde::Serializer as rmpSerializer;
use rule::Rule;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct Config {
    rules: Vec<Rule>,
    role_arn: String,
}

impl Config {
    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();

        self.serialize(&mut rmpSerializer::new(&mut buf).with_struct_map())?;

        Ok(buf)
    }

    pub fn unmarshal(buf: &[u8]) -> Result<Self> {
        let t: Config = rmp_serde::from_slice(buf)?;
        Ok(t)
    }
}
