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

use std::sync::atomic::AtomicI64;

/// Placeholder notification target list held by `EventNotifier`.
///
/// The working notification stack lives in `rustfs-notify` / `rustfs-targets`;
/// this type never grew past its counter. `total_events` is read by the
/// notifier's log line but nothing increments it, so that field reports zero.
#[derive(Default)]
#[allow(
    dead_code,
    reason = "held only by the dead ecstore EventNotifier; see services/event_notification.rs (backlog#1823)"
)]
pub struct TargetList {
    pub total_events: AtomicI64,
}

impl TargetList {
    pub fn new() -> TargetList {
        TargetList::default()
    }
}
