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

use s3s::dto::{BucketVersioningStatus, VersioningConfiguration};

use rustfs_utils::string::match_simple;

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
    fn prefix_enabled(&self, prefix: &str) -> bool {
        if self.status != Some(BucketVersioningStatus::from_static(BucketVersioningStatus::ENABLED)) {
            return false;
        }

        if prefix.is_empty() {
            return true;
        }

        if let Some(exclude_folders) = self.exclude_folders
            && exclude_folders
            && prefix.ends_with('/')
        {
            return false;
        }

        if let Some(ref excluded_prefixes) = self.excluded_prefixes {
            for p in excluded_prefixes.iter() {
                if let Some(ref sprefix) = p.prefix {
                    let pattern = format!("{sprefix}*");
                    if match_simple(&pattern, prefix) {
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

            if let Some(exclude_folders) = self.exclude_folders
                && exclude_folders
                && prefix.ends_with('/')
            {
                return true;
            }

            if let Some(ref excluded_prefixes) = self.excluded_prefixes {
                for p in excluded_prefixes.iter() {
                    if let Some(ref sprefix) = p.prefix {
                        let pattern = format!("{sprefix}*");
                        if match_simple(&pattern, prefix) {
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
    fn suspended(&self) -> bool {
        self.status == Some(BucketVersioningStatus::from_static(BucketVersioningStatus::SUSPENDED))
    }
}
