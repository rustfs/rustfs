use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use std::io::{Cursor, Read};

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct InlineData(Vec<u8>);

const INLINE_DATA_VER: u8 = 1;

impl InlineData {
    pub fn new() -> Self {
        Self(Vec::new())
    }
    pub fn update(&mut self, buf: &[u8]) {
        self.0 = buf.to_vec()
    }
    pub fn as_slice(&self) -> &[u8] {
        &self.0.as_slice()
    }
    pub fn version_ok(&self) -> bool {
        if self.0.is_empty() {
            return true;
        }

        self.0[0] > 0 && self.0[0] <= INLINE_DATA_VER
    }

    pub fn after_version(&self) -> &[u8] {
        if self.0.is_empty() {
            &self.0
        } else {
            &self.0[1..]
        }
    }

    pub fn find(&self, key: &str) -> Result<Option<Vec<u8>>> {
        if self.0.is_empty() || !self.version_ok() {
            return Ok(None);
        }

        let buf = self.after_version();

        let mut cur = Cursor::new(buf);

        let mut fields_len = rmp::decode::read_map_len(&mut cur)?;

        while fields_len > 0 {
            fields_len -= 1;

            let str_len = rmp::decode::read_str_len(&mut cur)?;

            let mut field_buff = vec![0u8; str_len as usize];

            cur.read_exact(&mut field_buff)?;

            let field = String::from_utf8(field_buff)?;

            let bin_len = rmp::decode::read_bin_len(&mut cur)? as usize;
            let start = cur.position() as usize;
            let end = start + bin_len;
            cur.set_position(end as u64);

            if field.as_str() == key {
                let buf = &buf[start..end];
                return Ok(Some(buf.to_vec()));
            }
        }

        Ok(None)
    }

    fn validate(&self) -> Result<()> {
        if self.0.is_empty() {
            return Ok(());
        }

        let mut cur = Cursor::new(self.after_version());

        let mut fields_len = rmp::decode::read_map_len(&mut cur)?;

        while fields_len > 0 {
            fields_len -= 1;

            let str_len = rmp::decode::read_str_len(&mut cur)?;

            let mut field_buff = vec![0u8; str_len as usize];

            cur.read_exact(&mut field_buff)?;

            let field = String::from_utf8(field_buff)?;
            if field.is_empty() {
                return Err(Error::msg("InlineData key empty"));
            }

            let bin_len = rmp::decode::read_bin_len(&mut cur)? as usize;
            let start = cur.position() as usize;
            let end = start + bin_len;
            cur.set_position(end as u64);
        }

        Ok(())
    }

    pub fn replace(&mut self, key: &str, value: Vec<u8>) -> Result<()> {
        if self.after_version().is_empty() {
            let mut keys = Vec::with_capacity(1);
            let mut values = Vec::with_capacity(1);

            keys.push(key.to_owned());
            values.push(value);

            return self.serialize(keys, values);
        }

        let buf = self.after_version();
        let mut cur = Cursor::new(buf);

        let mut fields_len = rmp::decode::read_map_len(&mut cur)? as usize;
        let mut keys = Vec::with_capacity(fields_len + 1);
        let mut values = Vec::with_capacity(fields_len + 1);

        let mut replaced = false;

        while fields_len > 0 {
            fields_len -= 1;

            let str_len = rmp::decode::read_str_len(&mut cur)?;

            let mut field_buff = vec![0u8; str_len as usize];

            cur.read_exact(&mut field_buff)?;

            let find_key = String::from_utf8(field_buff)?;

            let bin_len = rmp::decode::read_bin_len(&mut cur)? as usize;
            let start = cur.position() as usize;
            let end = start + bin_len;
            cur.set_position(end as u64);

            let find_value = &buf[start..end];

            if find_key.as_str() == key {
                values.push(value.clone());
                replaced = true
            } else {
                values.push(find_value.to_vec());
            }

            keys.push(find_key);
        }

        if !replaced {
            keys.push(key.to_owned());
            values.push(value);
        }

        self.serialize(keys, values)
    }

    fn serialize(&mut self, keys: Vec<String>, values: Vec<Vec<u8>>) -> Result<()> {
        assert_eq!(keys.len(), values.len(), "InlineData serialize: keys/values not match");

        if keys.is_empty() {
            self.0 = Vec::new();
            return Ok(());
        }

        let mut wr = Vec::new();

        wr.push(INLINE_DATA_VER);

        let map_len = keys.len();

        rmp::encode::write_map_len(&mut wr, map_len as u32)?;

        for i in 0..map_len {
            rmp::encode::write_str(&mut wr, keys[i].as_str())?;
            rmp::encode::write_bin(&mut wr, values[i].as_slice())?;
        }

        self.0 = wr;

        Ok(())
    }
}
