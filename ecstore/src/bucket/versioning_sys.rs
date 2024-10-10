use super::get_bucket_metadata_sys;
use super::versioning::Versioning;
use crate::disk::RUSTFS_META_BUCKET;
use crate::error::Result;
use tracing::warn;

pub struct BucketVersioningSys {}

impl BucketVersioningSys {
    pub async fn enabled(bucket: &str) -> bool {
        match Self::get(bucket).await {
            Ok(res) => res.enabled(),
            Err(err) => {
                warn!("{:?}", err);
                false
            }
        }
    }

    pub async fn prefix_enabled(bucket: &str, prefix: &str) -> bool {
        match Self::get(bucket).await {
            Ok(res) => res.prefix_enabled(prefix),
            Err(err) => {
                warn!("{:?}", err);
                false
            }
        }
    }

    pub async fn suspended(bucket: &str) -> bool {
        match Self::get(bucket).await {
            Ok(res) => res.suspended(),
            Err(err) => {
                warn!("{:?}", err);
                false
            }
        }
    }

    pub async fn prefix_suspended(bucket: &str, prefix: &str) -> bool {
        match Self::get(bucket).await {
            Ok(res) => res.prefix_suspended(prefix),
            Err(err) => {
                warn!("{:?}", err);
                false
            }
        }
    }

    pub async fn get(bucket: &str) -> Result<Versioning> {
        if bucket == RUSTFS_META_BUCKET || bucket.starts_with(RUSTFS_META_BUCKET) {
            return Ok(Versioning::default());
        }

        let bucket_meta_sys_lock = get_bucket_metadata_sys().await;
        let bucket_meta_sys = bucket_meta_sys_lock.write().await;

        let (cfg, _) = bucket_meta_sys.get_versioning_config(bucket).await?;

        Ok(cfg)
    }
}
