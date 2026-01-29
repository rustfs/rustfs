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
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_assignments)]
#![allow(unused_must_use)]
#![allow(clippy::all)]

use crate::client::{
    api_error_response::{err_entity_too_large, err_invalid_argument},
    api_put_object::PutObjectOptions,
    constants::{ABS_MIN_PART_SIZE, MAX_MULTIPART_PUT_OBJECT_SIZE, MAX_PART_SIZE, MAX_PARTS_COUNT, MIN_PART_SIZE},
    transition_api::ReaderImpl,
    transition_api::TransitionClient,
};

pub fn is_object(reader: &ReaderImpl) -> bool {
    todo!();
}

pub fn is_read_at(reader: ReaderImpl) -> bool {
    todo!();
}

pub fn optimal_part_info(object_size: i64, configured_part_size: u64) -> Result<(i64, i64, i64), std::io::Error> {
    let unknown_size;
    let mut object_size = object_size;
    if object_size == -1 {
        unknown_size = true;
        object_size = MAX_MULTIPART_PUT_OBJECT_SIZE;
    } else {
        unknown_size = false;
    }

    if object_size > MAX_MULTIPART_PUT_OBJECT_SIZE {
        return Err(std::io::Error::other(err_entity_too_large(
            object_size,
            MAX_MULTIPART_PUT_OBJECT_SIZE,
            "",
            "",
        )));
    }

    let mut part_size_flt: f64;
    if configured_part_size > 0 {
        if configured_part_size as i64 > object_size {
            return Err(std::io::Error::other(err_entity_too_large(
                configured_part_size as i64,
                object_size,
                "",
                "",
            )));
        }

        if !unknown_size && object_size > (configured_part_size as i64 * MAX_PARTS_COUNT) {
            return Err(std::io::Error::other(err_invalid_argument(
                "Part size * max_parts(10000) is lesser than input objectSize.",
            )));
        }

        if (configured_part_size as i64) < ABS_MIN_PART_SIZE {
            return Err(std::io::Error::other(err_invalid_argument(
                "Input part size is smaller than allowed minimum of 5MiB.",
            )));
        }

        if configured_part_size as i64 > MAX_PART_SIZE {
            return Err(std::io::Error::other(err_invalid_argument(
                "Input part size is bigger than allowed maximum of 5GiB.",
            )));
        }

        part_size_flt = configured_part_size as f64;
        if unknown_size {
            object_size = configured_part_size as i64 * MAX_PARTS_COUNT;
        }
    } else {
        let min_part = MIN_PART_SIZE as f64;
        part_size_flt = (object_size as f64 / MAX_PARTS_COUNT as f64).ceil();
        part_size_flt = part_size_flt.max(min_part);
        part_size_flt = (part_size_flt / min_part).ceil() * min_part;
    }

    let total_parts_count = (object_size as f64 / part_size_flt).ceil() as i64;
    let part_size = part_size_flt as i64;
    let last_part_size = object_size - (total_parts_count - 1) * part_size;
    Ok((total_parts_count, part_size, last_part_size))
}

impl TransitionClient {
    pub async fn new_upload_id(
        &self,
        bucket_name: &str,
        object_name: &str,
        opts: &PutObjectOptions,
    ) -> Result<String, std::io::Error> {
        let init_multipart_upload_result = self.initiate_multipart_upload(bucket_name, object_name, opts).await?;
        Ok(init_multipart_upload_result.upload_id)
    }
}
