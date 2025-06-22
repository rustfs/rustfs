use std::{fmt::Display, pin::Pin, sync::Arc};
use tracing::info;
use common::error::{Error, Result};
use crate::bucket::tier_config::{TierType, TierConfig,};

impl TierType {
    fn decode_msg(&self/*, dc *msgp.Reader*/) -> Result<()> {
        todo!();
    }

    fn encode_msg(&self/*, en *msgp.Writer*/) -> Result<()> {
        todo!();
    }

    pub fn marshal_msg(&self, b: &[u8]) -> Result<Vec<u8>> {
        todo!();
    }

    pub fn unmarshal_msg(&self, bts: &[u8]) -> Result<Vec<u8>> {
        todo!();
    }

    pub fn msg_size() -> usize {
        todo!();
    }
}

impl TierConfig {
    fn decode_msg(&self, dc *msgp.Reader) -> Result<()> {
        todo!();
    }

    pub fn encode_msg(&self, en *msgp.Writer) -> Result<()> {
        todo!();
    }

    pub fn marshal_msg(&self, b: &[u8]) -> Result<Vec<u8>> {
        todo!();
    }

    pub fn unmarshal_msg(&self, bts: &[u8]) -> Result<Vec<u8>> {
        todo!();
    }

    fn msg_size(&self) -> usize {
        todo!();
    }
}
