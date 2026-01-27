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

pub const QUOTA_CONFIG_FILE: &str = "quota.json";
pub const QUOTA_TYPE_HARD: &str = "HARD";

pub const QUOTA_EXCEEDED_ERROR_CODE: &str = "XRustfsQuotaExceeded";
pub const QUOTA_INVALID_CONFIG_ERROR_CODE: &str = "InvalidArgument";
pub const QUOTA_NOT_FOUND_ERROR_CODE: &str = "NoSuchBucket";
pub const QUOTA_INTERNAL_ERROR_CODE: &str = "InternalError";

pub const QUOTA_API_PATH: &str = "/rustfs/admin/v3/quota/{bucket}";

pub const QUOTA_INVALID_TYPE_ERROR_MSG: &str = "Only HARD quota type is supported";
pub const QUOTA_METADATA_SYSTEM_ERROR_MSG: &str = "Bucket metadata system not initialized";
