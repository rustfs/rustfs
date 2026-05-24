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

use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReloadDetectMode {
    Poll,
    Watch,  // TODO: implement fs::watch-based reload
    Hybrid, // TODO: implement poll + fs::watch hybrid
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReloadApplyHint {
    Lazy,
    SoftReconnect,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsReloadOptions {
    pub enabled: bool,
    pub detect_mode: ReloadDetectMode,
    pub interval: Duration,
    pub debounce: Duration,
    pub min_stable_age: Duration,
    pub apply_hint: ReloadApplyHint,
}

impl Default for TlsReloadOptions {
    fn default() -> Self {
        Self {
            enabled: true,
            detect_mode: ReloadDetectMode::Poll,
            interval: Duration::from_secs(15),
            debounce: Duration::from_secs(2),
            min_stable_age: Duration::from_secs(1),
            apply_hint: ReloadApplyHint::Lazy,
        }
    }
}
