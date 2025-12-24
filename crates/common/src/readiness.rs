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

use std::sync::atomic::{AtomicU8, Ordering};

/// Represents the various stages of system startup
#[repr(u8)]
pub enum SystemStage {
    Booting = 0,
    StorageReady = 1, // Disks online, Quorum met
    IamReady = 2,     // Users and Policies loaded into cache
    FullReady = 3,    // System ready to serve all traffic
}

/// Global readiness tracker for the service
/// This struct uses atomic operations to track the readiness status of various components
/// of the service in a thread-safe manner.
pub struct GlobalReadiness {
    status: AtomicU8,
}

impl Default for GlobalReadiness {
    fn default() -> Self {
        Self::new()
    }
}

impl GlobalReadiness {
    /// Create a new GlobalReadiness instance with initial status as Starting
    /// # Returns
    /// A new instance of GlobalReadiness
    pub fn new() -> Self {
        Self {
            status: AtomicU8::new(SystemStage::Booting as u8),
        }
    }

    /// Update the system to a new stage
    ///
    /// # Arguments
    /// * `step` - The SystemStage step to mark as ready
    pub fn mark_stage(&self, step: SystemStage) {
        self.status.fetch_max(step as u8, Ordering::SeqCst);
    }

    /// Check if the service is fully ready
    /// # Returns
    /// `true` if the service is fully ready, `false` otherwise
    pub fn is_ready(&self) -> bool {
        self.status.load(Ordering::SeqCst) == SystemStage::FullReady as u8
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_initial_state() {
        let readiness = GlobalReadiness::new();
        assert!(!readiness.is_ready());
        assert_eq!(readiness.status.load(Ordering::SeqCst), SystemStage::Booting as u8);
    }

    #[test]
    fn test_mark_stage_progression() {
        let readiness = GlobalReadiness::new();
        readiness.mark_stage(SystemStage::StorageReady);
        assert!(!readiness.is_ready());
        assert_eq!(readiness.status.load(Ordering::SeqCst), SystemStage::StorageReady as u8);

        readiness.mark_stage(SystemStage::IamReady);
        assert!(!readiness.is_ready());
        assert_eq!(readiness.status.load(Ordering::SeqCst), SystemStage::IamReady as u8);

        readiness.mark_stage(SystemStage::FullReady);
        assert!(readiness.is_ready());
    }

    #[test]
    fn test_no_regression() {
        let readiness = GlobalReadiness::new();
        readiness.mark_stage(SystemStage::FullReady);
        readiness.mark_stage(SystemStage::IamReady); // Should not regress
        assert!(readiness.is_ready());
    }

    #[test]
    fn test_concurrent_marking() {
        let readiness = Arc::new(GlobalReadiness::new());
        let mut handles = vec![];

        for _ in 0..10 {
            let r = Arc::clone(&readiness);
            handles.push(thread::spawn(move || {
                r.mark_stage(SystemStage::StorageReady);
                r.mark_stage(SystemStage::IamReady);
                r.mark_stage(SystemStage::FullReady);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert!(readiness.is_ready());
    }

    #[test]
    fn test_is_ready_only_at_full_ready() {
        let readiness = GlobalReadiness::new();
        assert!(!readiness.is_ready());

        readiness.mark_stage(SystemStage::StorageReady);
        assert!(!readiness.is_ready());

        readiness.mark_stage(SystemStage::IamReady);
        assert!(!readiness.is_ready());

        readiness.mark_stage(SystemStage::FullReady);
        assert!(readiness.is_ready());
    }
}
