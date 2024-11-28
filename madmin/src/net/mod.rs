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
            error: "Not implemented for non-linux platforms".to_owned(),
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
