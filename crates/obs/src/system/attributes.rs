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

use crate::GlobalError;
use opentelemetry::KeyValue;
use sysinfo::{Pid, System};

pub(crate) const PROCESS_PID: opentelemetry::Key = opentelemetry::Key::from_static_str("process.pid");
pub(crate) const PROCESS_EXECUTABLE_NAME: opentelemetry::Key = opentelemetry::Key::from_static_str("process.executable.name");
pub(crate) const PROCESS_EXECUTABLE_PATH: opentelemetry::Key = opentelemetry::Key::from_static_str("process.executable.path");
pub(crate) const PROCESS_COMMAND: opentelemetry::Key = opentelemetry::Key::from_static_str("process.command");

/// Struct to hold process attributes
pub struct ProcessAttributes {
    pub attributes: Vec<KeyValue>,
}

impl ProcessAttributes {
    /// Creates a new instance of `ProcessAttributes` for the given PID.
    pub fn new(pid: Pid, system: &mut System) -> Result<Self, GlobalError> {
        system.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), true);
        let process = system
            .process(pid)
            .ok_or_else(|| GlobalError::ProcessNotFound(pid.as_u32()))?;

        let attributes = vec![
            KeyValue::new(PROCESS_PID, pid.as_u32() as i64),
            KeyValue::new(PROCESS_EXECUTABLE_NAME, process.name().to_os_string().into_string().unwrap_or_default()),
            KeyValue::new(
                PROCESS_EXECUTABLE_PATH,
                process
                    .exe()
                    .map(|path| path.to_string_lossy().into_owned())
                    .unwrap_or_default(),
            ),
            KeyValue::new(
                PROCESS_COMMAND,
                process
                    .cmd()
                    .iter()
                    .fold(String::new(), |t1, t2| t1 + " " + t2.to_str().unwrap_or_default()),
            ),
        ];

        Ok(ProcessAttributes { attributes })
    }
}
