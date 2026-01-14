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

use crate::event::targetid::TargetID;
use std::sync::atomic::AtomicI64;

#[derive(Default)]
pub struct TargetList {
    pub current_send_calls: AtomicI64,
    pub total_events: AtomicI64,
    pub events_skipped: AtomicI64,
    pub events_errors_total: AtomicI64,
    //pub targets: HashMap<TargetID, Target>,
    //pub queue:   AsyncEvent,
    //pub targetStats: HashMap<TargetID, TargetStat>,
}

impl TargetList {
    pub fn new() -> TargetList {
        TargetList::default()
    }
}

struct TargetStat {
    current_send_calls: i64,
    total_events: i64,
    failed_events: i64,
}

struct TargetIDResult {
    id: TargetID,
    err: std::io::Error,
}
