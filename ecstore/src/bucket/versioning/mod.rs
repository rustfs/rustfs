use s3s::dto::{BucketVersioningStatus, VersioningConfiguration};

pub trait VersioningApi {
    fn enabled(&self) -> bool;
    fn prefix_enabled(&self, prefix: &str) -> bool;
    fn prefix_suspended(&self, prefix: &str) -> bool;
}

impl VersioningApi for VersioningConfiguration {
    fn enabled(&self) -> bool {
        self.status
            .as_ref()
            .is_some_and(|v| v.as_str() == BucketVersioningStatus::ENABLED)
    }

    fn prefix_enabled(&self, prefix: &str) -> bool {
        if !self
            .status
            .as_ref()
            .is_some_and(|v| v.as_str() == BucketVersioningStatus::ENABLED)
        {
            return false;
        }

        if prefix.is_empty() {
            return true;
        }

        // TODO: ExcludeFolders

        true
    }
    fn prefix_suspended(&self, prefix: &str) -> bool {
        if self
            .status
            .as_ref()
            .is_some_and(|v| v.as_str() == BucketVersioningStatus::SUSPENDED)
        {
            return true;
        }

        if let Some(status) = self.status.as_ref() {
            if status.as_str() == BucketVersioningStatus::ENABLED {
                if prefix.is_empty() {
                    return false;
                }

                // TODO: ExcludeFolders
            }
        }

        false
    }
}
