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

use super::user::{ExportIam, ImportIam};
use crate::{
    admin::router::{AdminOperation, S3Router},
    server::ADMIN_PREFIX,
};
use hyper::Method;

pub fn register_user_iam_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/export-iam").as_str(),
        AdminOperation(&ExportIam {}),
    )?;

    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/import-iam").as_str(),
        AdminOperation(&ImportIam {}),
    )?;

    Ok(())
}
