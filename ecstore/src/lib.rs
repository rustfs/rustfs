pub mod bucket_meta;
mod chunk_stream;
mod config;
pub mod disk;
pub mod disks_layout;
pub mod endpoints;
pub mod erasure;
pub mod error;
mod file_meta;
mod global;
pub mod peer;
mod quorum;
pub mod set_disk;
mod sets;
mod storage_class;
pub mod store;
pub mod store_api;
mod store_init;
mod utils;

pub mod bucket;

pub use global::new_object_layer_fn;
pub use global::update_erasure_type;
