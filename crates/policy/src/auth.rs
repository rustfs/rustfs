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

mod credentials;

pub use credentials::Credentials;
pub use credentials::*;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct UserIdentity {
    pub version: i64,
    pub credentials: Credentials,
    pub update_at: Option<OffsetDateTime>,
}

impl UserIdentity {
    pub fn new(credentials: Credentials) -> Self {
        UserIdentity {
            version: 1,
            credentials,
            update_at: Some(OffsetDateTime::now_utc()),
        }
    }
}

impl From<Credentials> for UserIdentity {
    fn from(value: Credentials) -> Self {
        UserIdentity {
            version: 1,
            credentials: value,
            update_at: Some(OffsetDateTime::now_utc()),
        }
    }
}
