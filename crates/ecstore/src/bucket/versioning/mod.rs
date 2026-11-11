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

use rustfs_gateway_types::persistence::PersistedVersioningConfiguration as VersioningConfiguration;
use rustfs_utils::string::match_simple;

pub trait VersioningApi {
    fn enabled(&self) -> bool;
    fn prefix_enabled(&self, prefix: &str) -> bool;
    fn prefix_suspended(&self, prefix: &str) -> bool;
    fn delete_state(&self, prefix: &str) -> (bool, bool) {
        (self.prefix_enabled(prefix), self.suspended())
    }
    fn versioned(&self, prefix: &str) -> bool;
    fn suspended(&self) -> bool;
}

impl VersioningApi for VersioningConfiguration {
    fn enabled(&self) -> bool {
        self.status.as_deref() == Some("Enabled")
    }
    fn prefix_enabled(&self, prefix: &str) -> bool {
        if self.status.as_deref() != Some("Enabled") {
            return false;
        }

        if prefix.is_empty() {
            return true;
        }

        if self.exclude_folders.unwrap_or(false) && prefix.ends_with('/') {
            return false;
        }

        if let Some(ref excluded_prefixes) = self.excluded_prefixes {
            for sprefix in excluded_prefixes.iter().flatten() {
                let pattern = format!("{sprefix}*");
                if match_simple(&pattern, prefix) {
                    return false;
                }
            }
        }

        true
    }

    fn prefix_suspended(&self, prefix: &str) -> bool {
        if self.status.as_deref() == Some("Suspended") {
            return true;
        }

        if self.status.as_deref() == Some("Enabled") {
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
                for sprefix in excluded_prefixes.iter().flatten() {
                    let pattern = format!("{sprefix}*");
                    if match_simple(&pattern, prefix) {
                        return true;
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
        self.status.as_deref() == Some("Suspended")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct LegacyVersioning;

    impl VersioningApi for LegacyVersioning {
        fn enabled(&self) -> bool {
            false
        }

        fn prefix_enabled(&self, _prefix: &str) -> bool {
            false
        }

        fn prefix_suspended(&self, _prefix: &str) -> bool {
            false
        }

        fn versioned(&self, _prefix: &str) -> bool {
            false
        }

        fn suspended(&self) -> bool {
            false
        }
    }

    #[test]
    fn delete_state_has_a_backward_compatible_default() {
        assert_eq!(LegacyVersioning.delete_state("object"), (false, false));
    }

    #[test]
    fn delete_state_treats_excluded_prefixes_as_unversioned() {
        let config = VersioningConfiguration {
            status: Some("Enabled".to_owned()),
            excluded_prefixes: Some(vec![Some("archive/".to_string())]),
            ..Default::default()
        };

        assert_eq!(config.delete_state("archive/object"), (false, false));
        assert!(config.prefix_suspended("archive/object"));
        assert_eq!(config.delete_state("live/object"), (true, false));

        let suspended = VersioningConfiguration {
            status: Some("Suspended".to_owned()),
            ..Default::default()
        };
        assert_eq!(suspended.delete_state("archive/object"), (false, true));
    }
}
