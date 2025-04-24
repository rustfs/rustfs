use crate::GlobalError;
use opentelemetry::KeyValue;
use sysinfo::{Pid, System};

pub const PROCESS_PID: opentelemetry::Key = opentelemetry::Key::from_static_str("process.pid");
pub const PROCESS_EXECUTABLE_NAME: opentelemetry::Key = opentelemetry::Key::from_static_str("process.executable.name");
pub const PROCESS_EXECUTABLE_PATH: opentelemetry::Key = opentelemetry::Key::from_static_str("process.executable.path");
pub const PROCESS_COMMAND: opentelemetry::Key = opentelemetry::Key::from_static_str("process.command");

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
