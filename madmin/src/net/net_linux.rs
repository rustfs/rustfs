use super::NetInfo;

pub fn get_net_info(addr: &str, iface: &str) -> NetInfo {
    let mut ni = NetInfo::default();
    ni.node_common.addr = addr.to_string();
    ni.interface = iface.to_string();

    ni
}
