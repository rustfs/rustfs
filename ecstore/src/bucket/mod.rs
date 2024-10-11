mod encryption;
pub mod error;
mod event;
mod lifecycle;
pub mod metadata;
mod metadata_sys;
mod objectlock;
pub mod policy;
pub mod policy_sys;
mod quota;
mod replication;
pub mod tags;
mod target;
pub mod utils;
pub mod versioning;
pub mod versioning_sys;

pub use metadata_sys::{bucket_metadata_sys_set, get_bucket_metadata_sys, init_bucket_metadata_sys};
