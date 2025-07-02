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

use std::collections::HashMap;

use lazy_static::lazy_static;
use tokio::sync::RwLock;
use tonic::transport::Channel;

lazy_static! {
    pub static ref GLOBAL_Local_Node_Name: RwLock<String> = RwLock::new("".to_string());
    pub static ref GLOBAL_Rustfs_Host: RwLock<String> = RwLock::new("".to_string());
    pub static ref GLOBAL_Rustfs_Port: RwLock<String> = RwLock::new("9000".to_string());
    pub static ref GLOBAL_Rustfs_Addr: RwLock<String> = RwLock::new("".to_string());
    pub static ref GLOBAL_Conn_Map: RwLock<HashMap<String, Channel>> = RwLock::new(HashMap::new());
}

pub async fn set_global_addr(addr: &str) {
    *GLOBAL_Rustfs_Addr.write().await = addr.to_string();
}
