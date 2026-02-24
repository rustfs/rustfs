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

//! KMS management route registration.

use super::kms::{KmsClearCacheHandler, KmsConfigHandler, KmsStatusHandler};
use super::kms_keys::{CreateKeyHandler, DescribeKeyHandler, GenerateDataKeyHandler, ListKeysHandler};
use crate::admin::router::{AdminOperation, S3Router};
use crate::server::ADMIN_PREFIX;
use hyper::Method;

pub fn register_kms_management_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/create-key").as_str(),
        AdminOperation(&CreateKeyHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/describe-key").as_str(),
        AdminOperation(&DescribeKeyHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/list-keys").as_str(),
        AdminOperation(&ListKeysHandler {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/generate-data-key").as_str(),
        AdminOperation(&GenerateDataKeyHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/status").as_str(),
        AdminOperation(&KmsStatusHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/config").as_str(),
        AdminOperation(&KmsConfigHandler {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/clear-cache").as_str(),
        AdminOperation(&KmsClearCacheHandler {}),
    )?;

    Ok(())
}
