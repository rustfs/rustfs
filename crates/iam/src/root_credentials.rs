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

use rustfs_credentials::{Credentials, get_global_action_cred};

pub(crate) fn credentials() -> Option<Credentials> {
    get_global_action_cred()
}

pub(crate) fn credentials_or_default() -> Credentials {
    credentials().unwrap_or_default()
}

pub(crate) fn token_signing_key() -> Option<String> {
    credentials().map(|cred| cred.secret_key)
}

pub(crate) fn is_root_access_key(access_key: &str) -> bool {
    credentials().is_some_and(|cred| cred.access_key == access_key)
}
