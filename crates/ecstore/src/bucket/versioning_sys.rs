// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::config_parse_mode::{BucketConfigParseMode, bucket_config_parse_mode};
use super::metadata::unreadable_config_refusal;
use super::{metadata_sys::get_bucket_metadata_sys, versioning::VersioningApi};
use crate::disk::RUSTFS_META_BUCKET;
use crate::error::Result;
use s3s::dto::VersioningConfiguration;
use tracing::{error, warn};

pub struct BucketVersioningSys {}

impl Default for BucketVersioningSys {
    fn default() -> Self {
        Self::new()
    }
}

impl BucketVersioningSys {
    pub fn new() -> Self {
        Self {}
    }
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

    pub async fn get(bucket: &str) -> Result<VersioningConfiguration> {
        if bucket == RUSTFS_META_BUCKET || bucket.starts_with(RUSTFS_META_BUCKET) {
            return Ok(VersioningConfiguration::default());
        }

        // Read lock is sufficient — get_versioning_config() handles its own
        // internal locking via metadata_map RwLock. The previous write lock
        // serialized all concurrent GET requests on this global lock.
        let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
        let bucket_meta_sys = bucket_meta_sys_lock.read().await;

        let (cfg, _) = bucket_meta_sys.get_versioning_config(bucket).await?;

        Ok(cfg)
    }

    /// Versioning configuration for laying out an object write.
    ///
    /// An unreadable stored configuration is refused in strict mode: writing
    /// as if unversioned would overwrite versions of a bucket that may have
    /// versioning enabled. Any other lookup failure keeps the historical
    /// fallback to the default configuration.
    pub async fn get_for_write(bucket: &str) -> Result<VersioningConfiguration> {
        resolve_versioning_for_write(bucket, Self::get(bucket).await, bucket_config_parse_mode())
    }

    /// `(versioned, version_suspended)` for an object write under `prefix`,
    /// from one [`Self::get_for_write`] lookup.
    pub async fn write_state(bucket: &str, prefix: &str) -> Result<(bool, bool)> {
        let config = Self::get_for_write(bucket).await?;
        Ok((config.prefix_enabled(prefix), config.prefix_suspended(prefix)))
    }

    /// Instance-scoped variant of [`Self::get`] (backlog#1052): resolves the
    /// caller's own instance context so a second in-process store never
    /// answers with the first instance's versioning state; falls back to the
    /// ambient system when the instance cell is not initialized.
    pub(crate) async fn get_in(ctx: &crate::runtime::instance::InstanceContext, bucket: &str) -> Result<VersioningConfiguration> {
        if bucket == RUSTFS_META_BUCKET || bucket.starts_with(RUSTFS_META_BUCKET) {
            return Ok(VersioningConfiguration::default());
        }

        let bucket_meta_sys_lock = crate::bucket::metadata_sys::bucket_metadata_sys_of(ctx)?;
        let bucket_meta_sys = bucket_meta_sys_lock.read().await;

        let (cfg, _) = bucket_meta_sys.get_versioning_config(bucket).await?;

        Ok(cfg)
    }
}

fn resolve_versioning_for_write(
    bucket: &str,
    lookup: Result<VersioningConfiguration>,
    mode: BucketConfigParseMode,
) -> Result<VersioningConfiguration> {
    match lookup {
        Ok(config) => Ok(config),
        Err(err) => match (unreadable_config_refusal(&err), mode) {
            (Some(_), BucketConfigParseMode::Strict) => Err(err),
            (Some(refusal), BucketConfigParseMode::Permissive) => {
                // RUSTFS_COMPAT_TODO(s3gate-parse-strict): permissive mode keeps the historical unversioned write for an unreadable versioning config so a rollout can measure affected buckets first. Remove after the parse-failure metric has read zero fleet-wide for two releases and one further release has shipped with strict as the default.
                error!(
                    event = "bucket_versioning_config_unreadable",
                    component = "ecstore",
                    subsystem = "bucket_versioning",
                    bucket = %bucket,
                    config = %refusal.config_file,
                    raw_len = refusal.raw_len,
                    mode = mode.as_str(),
                    result = "write_unversioned",
                    "Bucket versioning configuration is unreadable; writing as unversioned"
                );
                Ok(VersioningConfiguration::default())
            }
            (None, _) => {
                warn!(bucket = %bucket, error = ?err, "failed to load bucket versioning configuration; using default configuration");
                Ok(VersioningConfiguration::default())
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucket::metadata::{BUCKET_VERSIONING_CONFIG, is_unreadable_config_error, unreadable_config_error};
    use crate::error::Error;

    fn enabled() -> VersioningConfiguration {
        VersioningConfiguration {
            status: Some(s3s::dto::BucketVersioningStatus::from_static(s3s::dto::BucketVersioningStatus::ENABLED)),
            ..Default::default()
        }
    }

    /// rustfs/backlog#1734: strict mode refuses a write whose versioning
    /// state is unknown instead of laying it out as unversioned.
    #[test]
    fn strict_mode_refuses_a_write_against_unreadable_versioning() {
        let err = resolve_versioning_for_write(
            "b",
            Err(unreadable_config_error("b", BUCKET_VERSIONING_CONFIG, 9)),
            BucketConfigParseMode::Strict,
        )
        .expect_err("strict mode must refuse");
        assert!(is_unreadable_config_error(&err), "{err}");
    }

    #[test]
    fn permissive_mode_keeps_the_historical_unversioned_write() {
        let config = resolve_versioning_for_write(
            "b",
            Err(unreadable_config_error("b", BUCKET_VERSIONING_CONFIG, 9)),
            BucketConfigParseMode::Permissive,
        )
        .expect("permissive mode keeps writing");
        assert!(!config.enabled());
    }

    #[test]
    fn readable_config_and_lookup_faults_behave_as_before_in_every_mode() {
        for mode in [BucketConfigParseMode::Permissive, BucketConfigParseMode::Strict] {
            assert!(
                resolve_versioning_for_write("b", Ok(enabled()), mode)
                    .expect("readable")
                    .enabled()
            );
            let fallback =
                resolve_versioning_for_write("b", Err(Error::other("metadata read failed")), mode).expect("historical fallback");
            assert!(!fallback.enabled());
        }
    }
}
