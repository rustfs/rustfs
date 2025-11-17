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

//opa env vars
pub const ENV_POLICY_PLUGIN_OPA_URL: &str = "RUSTFS_POLICY_PLUGIN_URL";
pub const ENV_POLICY_PLUGIN_AUTH_TOKEN: &str = "RUSTFS_POLICY_PLUGIN_AUTH_TOKEN";

pub const ENV_POLICY_PLUGIN_KEYS: &[&str] = &[ENV_POLICY_PLUGIN_OPA_URL, ENV_POLICY_PLUGIN_AUTH_TOKEN];

pub const POLICY_PLUGIN_SUB_SYS: &str = "policy_plugin";
