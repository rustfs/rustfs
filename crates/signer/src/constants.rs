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

use time::{format_description::FormatItem, macros::format_description};

pub const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";
pub const UNSIGNED_PAYLOAD_TRAILER: &str = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";

pub const TOTAL_WORKERS: i64 = 4;

pub const SIGN_V4_ALGORITHM: &str = "AWS4-HMAC-SHA256";
pub const ISO8601_DATEFORMAT: &[FormatItem<'_>] =
    format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond]Z");
