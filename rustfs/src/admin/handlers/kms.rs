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

//! KMS admin handlers for HTTP API

use super::{kms_dynamic, kms_keys, kms_management};
use crate::admin::router::{AdminOperation, S3Router};

pub fn register_kms_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    kms_management::register_kms_management_route(r)?;
    kms_dynamic::register_kms_dynamic_route(r)?;
    kms_keys::register_kms_key_route(r)?;
    Ok(())
}
