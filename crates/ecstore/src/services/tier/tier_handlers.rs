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

use crate::client::admin_handler_utils::AdminError;
use http::status::StatusCode;
use lazy_static::lazy_static;

lazy_static! {
    pub static ref ERR_TIER_ALREADY_EXISTS: AdminError = AdminError {
        code: "XRustFSAdminTierAlreadyExists".to_string(),
        message: "Specified remote tier already exists".to_string(),
        status_code: StatusCode::CONFLICT,
    };
    pub static ref ERR_TIER_NOT_FOUND: AdminError = AdminError {
        code: "XRustFSAdminTierNotFound".to_string(),
        message: "Specified remote tier was not found".to_string(),
        status_code: StatusCode::NOT_FOUND,
    };
    pub static ref ERR_TIER_NAME_NOT_UPPERCASE: AdminError = AdminError {
        code: "XRustFSAdminTierNameNotUpperCase".to_string(),
        message: "Tier name must be in uppercase".to_string(),
        status_code: StatusCode::BAD_REQUEST,
    };
    pub static ref ERR_TIER_BUCKET_NOT_FOUND: AdminError = AdminError {
        code: "XRustFSAdminTierBucketNotFound".to_string(),
        message: "Remote tier bucket not found".to_string(),
        status_code: StatusCode::BAD_REQUEST,
    };
    pub static ref ERR_TIER_INVALID_CREDENTIALS: AdminError = AdminError {
        code: "XRustFSAdminTierInvalidCredentials".to_string(),
        message: "Invalid remote tier credentials".to_string(),
        status_code: StatusCode::BAD_REQUEST,
    };
    pub static ref ERR_TIER_RESERVED_NAME: AdminError = AdminError {
        code: "XRustFSAdminTierReserved".to_string(),
        message: "Cannot use reserved tier name".to_string(),
        status_code: StatusCode::BAD_REQUEST,
    };
    pub static ref ERR_TIER_PERM_ERR: AdminError = AdminError {
        code: "TierPermErr".to_string(),
        message: "Tier Perm Err".to_string(),
        status_code: StatusCode::OK,
    };
    pub static ref ERR_TIER_CONNECT_ERR: AdminError = AdminError {
        code: "TierConnectErr".to_string(),
        message: "Tier Connect Err".to_string(),
        status_code: StatusCode::OK,
    };
}
