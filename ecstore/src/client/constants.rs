#![allow(unused_imports)]
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

use lazy_static::lazy_static;
use std::{collections::HashMap, sync::Arc};
use time::{format_description::FormatItem, macros::format_description};

pub const ABS_MIN_PART_SIZE: i64 = 1024 * 1024 * 5;
pub const MAX_PARTS_COUNT: i64 = 10000;
pub const MAX_PART_SIZE: i64 = 1024 * 1024 * 1024 * 5;
pub const MIN_PART_SIZE: i64 = 1024 * 1024 * 16;

pub const MAX_SINGLE_PUT_OBJECT_SIZE: i64 = 1024 * 1024 * 1024 * 5;
pub const MAX_MULTIPART_PUT_OBJECT_SIZE: i64 = 1024 * 1024 * 1024 * 1024 * 5;

pub const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";
pub const UNSIGNED_PAYLOAD_TRAILER: &str = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";

pub const TOTAL_WORKERS: i64 = 4;

pub const SIGN_V4_ALGORITHM: &str = "AWS4-HMAC-SHA256";
pub const ISO8601_DATEFORMAT: &[FormatItem<'_>] =
    format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond]Z");

pub const GET_OBJECT_ATTRIBUTES_TAGS: &str = "ETag,Checksum,StorageClass,ObjectSize,ObjectParts";
pub const GET_OBJECT_ATTRIBUTES_MAX_PARTS: i64 = 1000;
