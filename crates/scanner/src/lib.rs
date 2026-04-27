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

#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![warn(
    // missing_docs,
    rustdoc::missing_crate_level_docs,
    unreachable_pub,
    rust_2018_idioms
)]

pub mod data_usage_define;
pub mod error;
pub mod last_minute;
pub mod scanner;
pub mod scanner_folder;
pub mod scanner_io;
pub mod sleeper;

pub use data_usage_define::*;
pub use error::ScannerError;
pub use scanner::init_data_scanner;
pub use sleeper::{DynamicSleeper, SCANNER_IDLE_MODE, SCANNER_SLEEPER};
use std::sync::atomic::{AtomicU64, Ordering};

static SCANNER_ACTIVE_WORK_UNITS: AtomicU64 = AtomicU64::new(0);

pub fn current_scanner_activity() -> u64 {
    SCANNER_ACTIVE_WORK_UNITS.load(Ordering::Relaxed)
}

pub(crate) struct ScannerActivityGuard;

impl ScannerActivityGuard {
    pub(crate) fn new() -> Self {
        SCANNER_ACTIVE_WORK_UNITS.fetch_add(1, Ordering::Relaxed);
        Self
    }
}

impl Drop for ScannerActivityGuard {
    fn drop(&mut self) {
        let _ = SCANNER_ACTIVE_WORK_UNITS
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| Some(current.saturating_sub(1)));
    }
}
