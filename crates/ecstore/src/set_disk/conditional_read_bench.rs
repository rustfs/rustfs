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

//! Test-util-only controls for the paired read benchmark's dedicated bucket.

use std::sync::atomic::{AtomicBool, Ordering};

static ACTIVE: AtomicBool = AtomicBool::new(false);

/// Keeps metadata off the cache and applies configured slow-disk faults to
/// metadata-only prepared reads as well as ordinary GET reads. Only the
/// benchmark's isolated bucket is affected; body-cache policy remains intact.
#[must_use]
pub struct ConditionalReadBenchmarkMetadataGuard {
    _private: (),
}

impl ConditionalReadBenchmarkMetadataGuard {
    pub fn acquire() -> Self {
        assert!(
            ACTIVE
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        );
        Self { _private: () }
    }
}

impl Drop for ConditionalReadBenchmarkMetadataGuard {
    fn drop(&mut self) {
        ACTIVE.store(false, Ordering::Release);
    }
}

pub(super) fn cache_bypass_applies(bucket: &str) -> bool {
    bucket == "conditional-bench" && ACTIVE.load(Ordering::Acquire)
}

pub(super) fn applies(bucket: &str, object: &str) -> bool {
    cache_bypass_applies(bucket) && object.starts_with("bench-")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn benchmark_metadata_profile_is_scoped_to_its_bucket_and_owner() {
        assert!(!cache_bypass_applies("conditional-bench"));
        let owner = ConditionalReadBenchmarkMetadataGuard::acquire();
        assert!(applies("conditional-bench", "bench-4096"));
        assert!(!applies("other-bucket", "bench-4096"));
        assert!(!applies("conditional-bench", "other-object"));
        drop(owner);
        assert!(!cache_bypass_applies("conditional-bench"));
    }
}
