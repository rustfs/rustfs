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

pub const ISO8601_DATEFORMAT: &[FormatItem<'_>] =
    format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond]Z");

// S3 response and request headers this client reads or writes.
pub const X_AMZ_BYPASS_GOVERNANCE_RETENTION: http::HeaderName =
    http::HeaderName::from_static("x-amz-bypass-governance-retention");
pub const X_AMZ_DELETE_MARKER: http::HeaderName = http::HeaderName::from_static("x-amz-delete-marker");
pub const X_AMZ_EXPIRATION: http::HeaderName = http::HeaderName::from_static("x-amz-expiration");
pub const X_AMZ_OBJECT_LOCK_LEGAL_HOLD: http::HeaderName = http::HeaderName::from_static("x-amz-object-lock-legal-hold");
pub const X_AMZ_OBJECT_LOCK_MODE: http::HeaderName = http::HeaderName::from_static("x-amz-object-lock-mode");
pub const X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE: http::HeaderName =
    http::HeaderName::from_static("x-amz-object-lock-retain-until-date");
pub const X_AMZ_REPLICATION_STATUS: http::HeaderName = http::HeaderName::from_static("x-amz-replication-status");
pub const X_AMZ_STORAGE_CLASS: http::HeaderName = http::HeaderName::from_static("x-amz-storage-class");
pub const X_AMZ_VERSION_ID: http::HeaderName = http::HeaderName::from_static("x-amz-version-id");
pub const X_AMZ_WEBSITE_REDIRECT_LOCATION: http::HeaderName = http::HeaderName::from_static("x-amz-website-redirect-location");
