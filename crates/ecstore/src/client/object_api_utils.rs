#![allow(clippy::map_entry)]
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
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_assignments)]
#![allow(unused_must_use)]
#![allow(clippy::all)]

use http::HeaderMap;
use std::io::Cursor;
use std::{collections::HashMap, sync::Arc};
use tokio::io::BufReader;

use crate::error::ErrorResponse;
use crate::store_api::{GetObjectReader, HTTPRangeSpec, ObjectInfo, ObjectOptions};
use rustfs_filemeta::fileinfo::ObjectPartInfo;
use rustfs_rio::HashReader;
use s3s::S3ErrorCode;

//#[derive(Clone)]
pub struct PutObjReader {
    pub reader: HashReader,
    pub raw_reader: HashReader,
    //pub sealMD5Fn: SealMD5CurrFn,
}

#[allow(dead_code)]
impl PutObjReader {
    pub fn new(raw_reader: HashReader) -> Self {
        todo!();
    }

    fn md5_current_hex_string(&self) -> String {
        todo!();
    }

    fn with_encryption(&mut self, enc_reader: HashReader) -> Result<(), std::io::Error> {
        self.reader = enc_reader;

        Ok(())
    }
}

pub type ObjReaderFn = Arc<dyn Fn(BufReader<Cursor<Vec<u8>>>, HeaderMap) -> GetObjectReader + 'static>;

fn part_number_to_rangespec(oi: ObjectInfo, part_number: usize) -> Option<HTTPRangeSpec> {
    if oi.size == 0 || oi.parts.len() == 0 {
        return None;
    }

    let mut start: i64 = 0;
    let mut end: i64 = -1;
    let mut i = 0;
    while i < oi.parts.len() && i < part_number {
        start = end + 1;
        end = start + oi.parts[i].actual_size as i64 - 1;
        i += 1;
    }

    Some(HTTPRangeSpec {
        start,
        end,
        is_suffix_length: false,
    })
}

fn get_compressed_offsets(oi: ObjectInfo, offset: i64) -> (i64, i64, i64, i64, u64) {
    let mut skip_length: i64 = 0;
    let mut cumulative_actual_size: i64 = 0;
    let mut first_part_idx: i64 = 0;
    let mut compressed_offset: i64 = 0;
    let mut part_skip: i64 = 0;
    let mut decrypt_skip: i64 = 0;
    let mut seq_num: u64 = 0;
    for (i, part) in oi.parts.iter().enumerate() {
        cumulative_actual_size += part.actual_size as i64;
        if cumulative_actual_size <= offset {
            compressed_offset += part.size as i64;
        } else {
            first_part_idx = i as i64;
            skip_length = cumulative_actual_size - part.actual_size as i64;
            break;
        }
    }
    skip_length = offset - skip_length;

    let parts: &[ObjectPartInfo] = &oi.parts;
    if skip_length > 0
        && parts.len() > first_part_idx as usize
        && parts[first_part_idx as usize].index.as_ref().expect("err").len() > 0
    {
        todo!();
    }

    (compressed_offset, part_skip, first_part_idx, decrypt_skip, seq_num)
}

pub fn new_getobjectreader(
    rs: HTTPRangeSpec,
    oi: &ObjectInfo,
    opts: &ObjectOptions,
    h: &HeaderMap,
) -> Result<(ObjReaderFn, i64, i64), ErrorResponse> {
    //let (_, mut is_encrypted) = crypto.is_encrypted(oi.user_defined)?;
    let mut is_encrypted = false;
    let is_compressed = false; //oi.is_compressed_ok();

    let mut get_fn: ObjReaderFn;

    let (off, length) = match rs.get_offset_length(oi.size) {
        Ok(x) => x,
        Err(err) => {
            return Err(ErrorResponse {
                code: S3ErrorCode::InvalidRange,
                message: err.to_string(),
                key: None,
                bucket_name: None,
                region: None,
                request_id: None,
                host_id: "".to_string(),
            });
        }
    };
    get_fn = Arc::new(move |input_reader: BufReader<Cursor<Vec<u8>>>, _: HeaderMap| {
        //Box::pin({
        /*let r = GetObjectReader {
            object_info: oi.clone(),
            stream: StreamingBlob::new(HashReader::new(input_reader, 10, None, None, 10)),
        };
        r*/
        todo!();
        //})
    });

    Ok((get_fn, off as i64, length as i64))
}

/// Format an ETag value according to HTTP standards (wrap with quotes if not already wrapped)
pub fn format_etag(etag: &str) -> String {
    if etag.starts_with('"') && etag.ends_with('"') {
        // Already properly formatted
        etag.to_string()
    } else if etag.starts_with("W/\"") && etag.ends_with('"') {
        // Already a weak ETag, properly formatted
        etag.to_string()
    } else {
        // Need to wrap with quotes
        format!("\"{}\"", etag)
    }
}

pub fn extract_etag(metadata: &HashMap<String, String>) -> String {
    let etag = if let Some(etag) = metadata.get("etag") {
        etag.clone()
    } else {
        metadata["md5Sum"].clone()
    };
    format_etag(&etag)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_etag() {
        // Test unquoted ETag - should add quotes
        assert_eq!(format_etag("6af8d12c0c74b78094884349f3c8a079"), "\"6af8d12c0c74b78094884349f3c8a079\"");

        // Test already quoted ETag - should not double quote
        assert_eq!(
            format_etag("\"6af8d12c0c74b78094884349f3c8a079\""),
            "\"6af8d12c0c74b78094884349f3c8a079\""
        );

        // Test weak ETag - should keep as is
        assert_eq!(
            format_etag("W/\"6af8d12c0c74b78094884349f3c8a079\""),
            "W/\"6af8d12c0c74b78094884349f3c8a079\""
        );

        // Test empty ETag - should add quotes
        assert_eq!(format_etag(""), "\"\"");

        // Test malformed quote (only starting quote) - should wrap properly
        assert_eq!(format_etag("\"incomplete"), "\"\"incomplete\"");

        // Test malformed quote (only ending quote) - should wrap properly
        assert_eq!(format_etag("incomplete\""), "\"incomplete\"\"");
    }

    #[test]
    fn test_extract_etag() {
        let mut metadata = HashMap::new();

        // Test with etag field
        metadata.insert("etag".to_string(), "abc123".to_string());
        assert_eq!(extract_etag(&metadata), "\"abc123\"");

        // Test with already quoted etag field
        metadata.insert("etag".to_string(), "\"def456\"".to_string());
        assert_eq!(extract_etag(&metadata), "\"def456\"");

        // Test fallback to md5Sum
        metadata.remove("etag");
        metadata.insert("md5Sum".to_string(), "xyz789".to_string());
        assert_eq!(extract_etag(&metadata), "\"xyz789\"");
    }
}
