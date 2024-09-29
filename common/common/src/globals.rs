use std::collections::HashMap;

use lazy_static::lazy_static;
use tokio::sync::RwLock;
use tonic::transport::Channel;

lazy_static! {
    pub static ref GLOBAL_Local_Node_Name: RwLock<String> = RwLock::new("".to_string());
    pub static ref GLOBAL_Rustfs_Host: RwLock<String> = RwLock::new("".to_string());
    pub static ref GLOBAL_Rustfs_Port: RwLock<String> = RwLock::new("9000".to_string());
    pub static ref GLOBAL_Conn_Map: RwLock<HashMap<String, Channel>> = RwLock::new(HashMap::new());
}
