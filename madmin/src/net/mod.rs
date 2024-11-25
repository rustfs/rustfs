use serde::{Deserialize, Serialize};

use crate::health::NodeCommon;

pub mod net_linux;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct NetInfo {
    node_common: NodeCommon,
    interface: String,
    driver: String,
    firmware_version: String,
}
