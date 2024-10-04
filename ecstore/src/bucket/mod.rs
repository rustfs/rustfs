mod encryption;
mod error;
mod event;
mod lifecycle;
pub mod metadata;
mod metadata_sys;
mod objectlock;
mod policy;
mod quota;
mod replication;
mod tags;
mod target;
pub mod utils;
mod versioning;

pub use metadata_sys::{get_bucket_metadata_sys, init_bucket_metadata_sys};
