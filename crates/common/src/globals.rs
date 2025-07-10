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

#![allow(non_upper_case_globals)] // FIXME

use std::collections::HashMap;
use std::sync::LazyLock;

use tokio::sync::RwLock;
use tonic::transport::Channel;

pub static GLOBAL_Local_Node_Name: LazyLock<RwLock<String>> = LazyLock::new(|| RwLock::new("".to_string()));
pub static GLOBAL_Rustfs_Host: LazyLock<RwLock<String>> = LazyLock::new(|| RwLock::new("".to_string()));
pub static GLOBAL_Rustfs_Port: LazyLock<RwLock<String>> = LazyLock::new(|| RwLock::new("9000".to_string()));
pub static GLOBAL_Rustfs_Addr: LazyLock<RwLock<String>> = LazyLock::new(|| RwLock::new("".to_string()));
pub static GLOBAL_Conn_Map: LazyLock<RwLock<HashMap<String, Channel>>> = LazyLock::new(|| RwLock::new(HashMap::new()));

pub async fn set_global_addr(addr: &str) {
    *GLOBAL_Rustfs_Addr.write().await = addr.to_string();
}
