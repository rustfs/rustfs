pub mod error;
pub mod format;
mod local;

pub use local::{init_disks, DiskOption, DiskStore};

pub const RUSTFS_META_BUCKET: &str = ".rustfs.sys";
pub const RUSTFS_META_MULTIPART_BUCKET: &str = ".rustfs.sys/multipart";
pub const RUSTFS_META_TMP_BUCKET: &str = ".rustfs.sys/tmp";
pub const RUSTFS_META_TMP_DELETED_BUCKET: &str = ".rustfs.sys/tmp/.trash";
pub const BUCKET_META_PREFIX: &str = "buckets";
pub const FORMAT_CONFIG_FILE: &str = "format.json";
const STORAGE_FORMAT_FILE: &str = "xl.meta";
