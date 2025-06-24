#![allow(clippy::map_entry)]
#![allow(unused_imports)]
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
const RUSTFS_BUCKET_SOURCE_MTIME: &str = "X-RustFs-Source-Mtime";
