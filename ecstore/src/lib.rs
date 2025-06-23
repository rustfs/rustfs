extern crate core;

pub mod admin_server_info;
pub mod bitrot;
pub mod bucket;
pub mod cache_value;
mod chunk_stream;
pub mod cmd;
pub mod compress;
pub mod config;
pub mod disk;
pub mod disks_layout;
pub mod endpoints;
pub mod erasure_coding;
pub mod error;
pub mod global;
pub mod heal;
pub mod metrics_realtime;
pub mod notification_sys;
pub mod pools;
pub mod rebalance;
pub mod rpc;
pub mod set_disk;
mod sets;
pub mod store;
pub mod store_api;
mod store_init;
pub mod store_list_objects;
pub mod store_utils;

pub mod checksum;
pub mod client;
pub mod event;
pub mod event_notification;
pub mod signer;
pub mod tier;

pub use global::new_object_layer_fn;
pub use global::set_global_endpoints;
pub use global::update_erasure_type;

pub use global::GLOBAL_Endpoints;
pub use store_api::StorageAPI;
