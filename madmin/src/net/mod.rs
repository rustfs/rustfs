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

use serde::{Deserialize, Serialize};

use crate::health::NodeCommon;

#[cfg(target_os = "linux")]
pub fn get_net_info(addr: &str, iface: &str) -> NetInfo {
    let mut ni = NetInfo::default();
    ni.node_common.addr = addr.to_string();
    ni.interface = iface.to_string();

    ni
}

#[cfg(not(target_os = "linux"))]
pub fn get_net_info(addr: &str, iface: &str) -> NetInfo {
    NetInfo {
        node_common: NodeCommon {
            addr: addr.to_owned(),
            error: Some("Not implemented for non-linux platforms".to_string()),
        },
        interface: iface.to_owned(),
        ..Default::default()
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct NetInfo {
    node_common: NodeCommon,
    interface: String,
    driver: String,
    firmware_version: String,
}
