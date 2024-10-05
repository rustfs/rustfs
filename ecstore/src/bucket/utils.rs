use crate::disk::RUSTFS_META_BUCKET;

pub fn is_meta_bucketname(name: &str) -> bool {
    name.starts_with(RUSTFS_META_BUCKET)
}
