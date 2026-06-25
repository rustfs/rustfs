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

use tracing::info;

const LOG_COMPONENT_MAIN: &str = "main";
const LOG_SUBSYSTEM_STARTUP: &str = "startup";
const EVENT_DEADLOCK_DETECTOR_STATE: &str = "deadlock_detector_state";

pub(crate) fn init_deadlock_detector_runtime() {
    let detector = crate::storage_api::deadlock_detector::get_deadlock_detector();
    if detector.is_enabled() {
        detector.start();
        info!(
            target: "rustfs::main::run",
            event = EVENT_DEADLOCK_DETECTOR_STATE,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            state = "started",
            "Deadlock detector started"
        );
    } else {
        info!(
            target: "rustfs::main::run",
            event = EVENT_DEADLOCK_DETECTOR_STATE,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            state = "disabled",
            "Deadlock detector disabled"
        );
    }
}
