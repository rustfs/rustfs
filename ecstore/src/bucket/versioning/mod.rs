use s3s::dto::{BucketVersioningStatus, VersioningConfiguration};

use crate::utils::wildcard;

pub trait VersioningApi {
    fn enabled(&self) -> bool;
    fn prefix_enabled(&self, prefix: &str) -> bool;
    fn prefix_suspended(&self, prefix: &str) -> bool;
    fn versioned(&self, prefix: &str) -> bool;
    fn suspended(&self) -> bool;
}

impl VersioningApi for VersioningConfiguration {
    fn enabled(&self) -> bool {
        self.status == Some(BucketVersioningStatus::from_static(BucketVersioningStatus::ENABLED))
    }
    fn suspended(&self) -> bool {
        self.status == Some(BucketVersioningStatus::from_static(BucketVersioningStatus::SUSPENDED))
    }

    fn prefix_enabled(&self, prefix: &str) -> bool {
        if self.status == Some(BucketVersioningStatus::from_static(BucketVersioningStatus::ENABLED)) {
            return false;
        }

        if prefix.is_empty() {
            return true;
        }

        if let Some(exclude_folders) = self.exclude_folders {
            if exclude_folders && prefix.ends_with('/') {
                return false;
            }
        }

        if let Some(ref excluded_prefixes) = self.excluded_prefixes {
            for p in excluded_prefixes.iter() {
                if let Some(ref sprefix) = p.prefix {
                    let pattern = format!("{}*", sprefix);
                    if wildcard::match_simple(&pattern, prefix) {
                        return false;
                    }
                }
            }
        }

        true
    }
    fn prefix_suspended(&self, prefix: &str) -> bool {
        if self.status == Some(BucketVersioningStatus::from_static(BucketVersioningStatus::SUSPENDED)) {
            return true;
        }

        if self.status == Some(BucketVersioningStatus::from_static(BucketVersioningStatus::ENABLED)) {
            if prefix.is_empty() {
                return false;
            }

            if let Some(exclude_folders) = self.exclude_folders {
                if exclude_folders && prefix.ends_with('/') {
                    return true;
                }
            }

            if let Some(ref excluded_prefixes) = self.excluded_prefixes {
                for p in excluded_prefixes.iter() {
                    if let Some(ref sprefix) = p.prefix {
                        let pattern = format!("{}*", sprefix);
                        if wildcard::match_simple(&pattern, prefix) {
                            return true;
                        }
                    }
                }
            }
        }

        false
    }
    fn versioned(&self, prefix: &str) -> bool {
        self.prefix_enabled(prefix) || self.prefix_suspended(prefix)
    }
}
