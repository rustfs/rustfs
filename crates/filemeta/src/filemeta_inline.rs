// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::error::{Error, Result};
use crate::filemeta::msgp_decode::{MAX_MSGP_ELEMENT_SIZE, prealloc_hint, read_exact_vec};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::io::Cursor;
use uuid::Uuid;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct InlineData(Vec<u8>);

const INLINE_DATA_VER: u8 = 1;

impl InlineData {
    fn contains_key_by<F>(&self, mut should_remove: F) -> Result<bool>
    where
        F: FnMut(&[u8]) -> bool,
    {
        let buf = self.after_version();
        if buf.is_empty() {
            return Ok(false);
        }

        let mut scan_cur = Cursor::new(buf);
        let mut scan_fields_len = rmp::decode::read_map_len(&mut scan_cur)? as usize;

        while scan_fields_len > 0 {
            scan_fields_len -= 1;

            let str_len = rmp::decode::read_str_len(&mut scan_cur)? as usize;
            let key_start = scan_cur.position() as usize;
            let key_end = key_start + str_len;
            let key = buf
                .get(key_start..key_end)
                .ok_or_else(|| Error::other("InlineData key out of range"))?;
            scan_cur.set_position(key_end as u64);

            let bin_len = rmp::decode::read_bin_len(&mut scan_cur)? as usize;
            let value_start = scan_cur.position() as usize;
            let value_end = value_start + bin_len;
            if value_end > buf.len() {
                return Err(Error::other("InlineData value out of range"));
            }
            scan_cur.set_position(value_end as u64);

            if should_remove(key) {
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn borrowed_entries(&self) -> Result<Vec<(&str, &[u8])>> {
        let buf = self.after_version();
        if buf.is_empty() {
            return Ok(Vec::new());
        }
        let mut cur = Cursor::new(buf);
        let count = usize::try_from(rmp::decode::read_map_len(&mut cur)?).map_err(Error::other)?;
        let mut entries = Vec::with_capacity(prealloc_hint(count));
        for _ in 0..count {
            let key_len = usize::try_from(rmp::decode::read_str_len(&mut cur)?).map_err(Error::other)?;
            if key_len > MAX_MSGP_ELEMENT_SIZE {
                return Err(Error::other("InlineData key exceeds the msgpack element limit"));
            }
            let start = usize::try_from(cur.position()).map_err(Error::other)?;
            let end = start
                .checked_add(key_len)
                .ok_or_else(|| Error::other("InlineData key out of range"))?;
            let key = buf
                .get(start..end)
                .ok_or_else(|| Error::other("InlineData key out of range"))?;
            let key = std::str::from_utf8(key).map_err(|err| Error::FromUtf8(err.to_string()))?;
            cur.set_position(u64::try_from(end).map_err(Error::other)?);

            let value_len = usize::try_from(rmp::decode::read_bin_len(&mut cur)?).map_err(Error::other)?;
            let start = usize::try_from(cur.position()).map_err(Error::other)?;
            let end = start
                .checked_add(value_len)
                .ok_or_else(|| Error::other("InlineData value out of range"))?;
            let value = buf
                .get(start..end)
                .ok_or_else(|| Error::other("InlineData value out of range"))?;
            cur.set_position(u64::try_from(end).map_err(Error::other)?);
            entries.push((key, value));
        }
        Ok(entries)
    }

    pub(crate) fn key_set(&self) -> Result<HashSet<Vec<u8>>> {
        if !self.version_ok() {
            return Err(Error::FileCorrupt);
        }
        Ok(self
            .borrowed_entries()?
            .into_iter()
            .map(|(key, _)| key.as_bytes().to_vec())
            .collect())
    }

    pub(crate) fn remove_keys(&mut self, keys: &HashSet<Vec<u8>>) -> Result<bool> {
        if keys.is_empty() {
            return Ok(false);
        }
        self.remove_keys_by(|key| keys.contains(key))
    }

    fn remove_keys_by<F>(&mut self, mut should_remove: F) -> Result<bool>
    where
        F: FnMut(&[u8]) -> bool,
    {
        let mut entries = self.borrowed_entries()?;
        let original_count = entries.len();
        entries.retain(|(key, _)| !should_remove(key.as_bytes()));
        if entries.len() == original_count {
            return Ok(false);
        }
        if entries.is_empty() {
            self.0.clear();
            return Ok(true);
        }

        // Borrow payloads until the final encoding so every retained body is
        // copied only once, regardless of the number of removed versions.
        let capacity = entries.iter().try_fold(6_usize, |size, (key, value)| {
            // A string and binary value each need at most five header bytes.
            size.checked_add(key.len())
                .and_then(|size| size.checked_add(value.len()))
                .and_then(|size| size.checked_add(10))
                .ok_or_else(|| Error::other("InlineData encoded size overflow"))
        })?;
        let mut encoded = Vec::with_capacity(capacity);
        encoded.push(INLINE_DATA_VER);
        rmp::encode::write_map_len(&mut encoded, u32::try_from(entries.len()).map_err(Error::other)?)?;
        for (key, value) in entries {
            rmp::encode::write_str(&mut encoded, key)?;
            rmp::encode::write_bin(&mut encoded, value)?;
        }
        self.0 = encoded;
        Ok(true)
    }

    fn remove_two_keys_by_bytes(&mut self, first_key: &[u8], second_key: &[u8]) -> Result<bool> {
        self.remove_keys_by(|key| key == first_key || key == second_key)
    }

    pub fn new() -> Self {
        Self(Vec::new())
    }
    pub fn update(&mut self, buf: &[u8]) {
        self.0 = buf.to_vec()
    }
    pub fn as_slice(&self) -> &[u8] {
        self.0.as_slice()
    }
    pub fn version_ok(&self) -> bool {
        Self::version_ok_slice(&self.0)
    }

    pub(crate) fn version_ok_slice(raw: &[u8]) -> bool {
        raw.first().is_none_or(|version| *version > 0 && *version <= INLINE_DATA_VER)
    }

    pub fn after_version(&self) -> &[u8] {
        if self.0.is_empty() { &self.0 } else { &self.0[1..] }
    }

    pub fn entries(&self) -> Result<usize> {
        if self.0.is_empty() || !self.version_ok() {
            return Ok(0);
        }

        let buf = self.after_version();

        let mut cur = Cursor::new(buf);

        let fields_len = rmp::decode::read_map_len(&mut cur)?;

        Ok(fields_len as usize)
    }

    #[inline]
    fn read_entry_slice<'a>(buf: &'a [u8], cur: &mut Cursor<&'a [u8]>, reject_empty_key: bool) -> Result<(&'a str, &'a [u8])> {
        let str_len = rmp::decode::read_str_len(cur)? as usize;
        if str_len > MAX_MSGP_ELEMENT_SIZE {
            return Err(Error::other(format!(
                "corrupt msgpack element: length {str_len} exceeds the {MAX_MSGP_ELEMENT_SIZE} byte limit"
            )));
        }
        let key_start = cur.position() as usize;
        let key_end = key_start
            .checked_add(str_len)
            .ok_or_else(|| Error::other("InlineData key out of range"))?;
        let key_bytes = buf
            .get(key_start..key_end)
            .ok_or_else(|| Error::from(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "failed to fill whole buffer")))?;
        let field = std::str::from_utf8(key_bytes).map_err(|e| Error::FromUtf8(e.to_string()))?;
        if reject_empty_key && field.is_empty() {
            return Err(Error::other("InlineData key empty"));
        }
        cur.set_position(key_end as u64);

        let bin_len = rmp::decode::read_bin_len(cur)? as usize;
        let start = cur.position() as usize;
        let end = start
            .checked_add(bin_len)
            .ok_or_else(|| Error::other("InlineData value out of range"))?;
        let value = buf
            .get(start..end)
            .ok_or_else(|| Error::other("InlineData value out of range"))?;
        cur.set_position(end as u64);
        Ok((field, value))
    }

    pub fn find(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Self::find_in_slice(&self.0, key)
    }

    pub(crate) fn find_in_slice(raw: &[u8], key: &str) -> Result<Option<Vec<u8>>> {
        if raw.is_empty() || raw[0] == 0 || raw[0] > INLINE_DATA_VER {
            return Ok(None);
        }

        let buf = &raw[1..];
        let mut cur = Cursor::new(buf);
        let mut fields_len = rmp::decode::read_map_len(&mut cur)?;

        while fields_len > 0 {
            fields_len -= 1;
            let (field, value) = Self::read_entry_slice(buf, &mut cur, false)?;
            if field == key {
                return Ok(Some(value.to_vec()));
            }
        }

        Ok(None)
    }

    pub fn validate(&self) -> Result<()> {
        Self::validate_slice(&self.0)
    }

    pub(crate) fn validate_slice(raw: &[u8]) -> Result<()> {
        if raw.is_empty() {
            return Ok(());
        }

        let buf = &raw[1..];
        let mut cur = Cursor::new(buf);
        let mut fields_len = rmp::decode::read_map_len(&mut cur)?;

        while fields_len > 0 {
            fields_len -= 1;
            let _ = Self::read_entry_slice(buf, &mut cur, true)?;
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
        let mut keys = Vec::with_capacity(prealloc_hint(fields_len) + 1);
        let mut values = Vec::with_capacity(prealloc_hint(fields_len) + 1);

        let mut replaced = false;

        while fields_len > 0 {
            fields_len -= 1;

            let str_len = rmp::decode::read_str_len(&mut cur)?;

            let field_buff = read_exact_vec(&mut cur, str_len as usize)?;

            let find_key = String::from_utf8(field_buff)?;

            let bin_len = rmp::decode::read_bin_len(&mut cur)? as usize;
            let start = cur.position() as usize;
            let end = start + bin_len;
            let find_value = buf
                .get(start..end)
                .ok_or_else(|| Error::other("InlineData value out of range"))?;
            cur.set_position(end as u64);

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

    pub fn remove_key(&mut self, key: &str) -> Result<bool> {
        let key_bytes = key.as_bytes();
        if !self.contains_key_by(|candidate| candidate == key_bytes)? {
            return Ok(false);
        }
        self.remove_keys_by(|candidate| candidate == key_bytes)
    }

    pub fn remove(&mut self, remove_keys: Vec<Uuid>) -> Result<bool> {
        let mut encoded_keys = Vec::with_capacity(remove_keys.len());
        for key in remove_keys {
            let mut buf = Uuid::encode_buffer();
            encoded_keys.push(key.hyphenated().encode_lower(&mut buf).to_string().into_bytes());
        }

        self.remove_keys_by(|candidate| encoded_keys.iter().any(|key| candidate == key.as_slice()))
    }

    pub fn remove_two(&mut self, first: Uuid, second: Uuid) -> Result<bool> {
        let mut first_buf = Uuid::encode_buffer();
        let mut second_buf = Uuid::encode_buffer();
        let first_key = first.hyphenated().encode_lower(&mut first_buf).as_bytes();
        let second_key = second.hyphenated().encode_lower(&mut second_buf).as_bytes();
        self.remove_two_keys_by_bytes(first_key, second_key)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remove_key_miss_keeps_inline_data_unchanged() {
        let mut data = InlineData::new();
        data.replace("keep", b"value".to_vec()).expect("seed inline data");
        let before = data.as_slice().to_vec();

        let removed = data.remove_key("missing").expect("remove_key should succeed");

        assert!(!removed);
        assert_eq!(data.as_slice(), before.as_slice());
    }

    #[test]
    fn remove_two_removes_only_matching_keys() {
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        let keep = Uuid::new_v4();
        let mut data = InlineData::new();
        data.replace(first.hyphenated().to_string().as_str(), b"first".to_vec())
            .expect("seed first key");
        data.replace(second.hyphenated().to_string().as_str(), b"second".to_vec())
            .expect("seed second key");
        data.replace(keep.hyphenated().to_string().as_str(), b"keep".to_vec())
            .expect("seed keep key");

        let removed = data.remove_two(first, second).expect("remove_two should succeed");

        assert!(removed);
        assert_eq!(data.find(first.hyphenated().to_string().as_str()).expect("find first"), None);
        assert_eq!(data.find(second.hyphenated().to_string().as_str()).expect("find second"), None);
        assert_eq!(
            data.find(keep.hyphenated().to_string().as_str()).expect("find keep"),
            Some(b"keep".to_vec())
        );
    }

    #[test]
    fn remove_keys_preserves_wire_bytes_and_rejects_truncated_payloads() {
        let mut original = InlineData::new();
        for id in 0_u128..8 {
            original
                .replace(&Uuid::from_u128(id).to_string(), vec![u8::try_from(id).expect("small id"); 128])
                .expect("seed inline map");
        }
        for mask in 0_u32..256 {
            let keys: HashSet<_> = (0_u128..8)
                .filter(|id| mask & (1 << id) != 0)
                .map(|id| Uuid::from_u128(id).to_string().into_bytes())
                .collect();
            let mut actual = original.clone();
            actual.remove_keys(&keys).expect("batch removal");
            let retained: Vec<_> = (0_u128..8).filter(|id| mask & (1 << id) == 0).collect();
            let mut expected = InlineData::new();
            for id in retained {
                expected
                    .replace(&Uuid::from_u128(id).to_string(), vec![u8::try_from(id).expect("small id"); 128])
                    .expect("encode survivor");
            }
            assert_eq!(actual, expected, "mask={mask}");
        }
        let keys = HashSet::from([Uuid::nil().to_string().into_bytes()]);
        for end in 2..original.as_slice().len() {
            let mut truncated = InlineData::new();
            truncated.update(&original.as_slice()[..end]);
            let before = truncated.clone();
            assert!(truncated.remove_keys(&keys).is_err(), "end={end}");
            assert_eq!(truncated, before);
        }
    }
}
