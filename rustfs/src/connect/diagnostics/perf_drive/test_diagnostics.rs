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

use std::time::Instant;

pub(super) struct IoDiagnostic {
    phase: &'static str,
    started: Instant,
    read_bytes: u64,
    write_bytes: u64,
    io_count: u64,
}

impl IoDiagnostic {
    pub(super) fn new(phase: &'static str, read_bytes: u64, write_bytes: u64, io_count: u64) -> Self {
        Self {
            phase,
            started: Instant::now(),
            read_bytes,
            write_bytes,
            io_count,
        }
    }

    fn line(&self, outcome: &'static str) -> String {
        format!(
            "connect_drive_phase phase={} outcome={} elapsed_us={} completed_read_bytes={} completed_write_bytes={} completed_io_count={}",
            self.phase,
            outcome,
            self.started.elapsed().as_micros(),
            self.read_bytes,
            self.write_bytes,
            self.io_count,
        )
    }

    pub(super) fn emit(&self, outcome: &'static str) {
        eprintln!("{}", self.line(outcome));
    }
}

#[cfg(test)]
mod tests {
    use super::IoDiagnostic;
    use std::time::Duration;

    #[test]
    fn timeout_retains_completed_io_progress() {
        let diagnostic = IoDiagnostic::new("read", 4_096, 32_768, 9);
        let line = diagnostic.line("timed_out");
        let fields: std::collections::BTreeMap<_, _> =
            line.split_whitespace().skip(1).map(|f| f.split_once('=').unwrap()).collect();
        assert_eq!(fields["phase"], "read");
        assert_eq!(fields["outcome"], "timed_out");
        assert_eq!(fields["completed_read_bytes"], "4096");
        assert_eq!(fields["completed_write_bytes"], "32768");
        assert_eq!(fields["completed_io_count"], "9");
    }

    #[test]
    fn diagnostic_elapsed_is_not_clamped_to_request_budget() {
        let mut diagnostic = IoDiagnostic::new("sync_all", 0, 32_768, 8);
        diagnostic.started -= Duration::from_secs(2);
        let line = diagnostic.line("timed_out");
        let micros = line
            .split_whitespace()
            .find_map(|f| f.strip_prefix("elapsed_us="))
            .unwrap()
            .parse::<u128>()
            .unwrap();
        assert!(micros >= 2_000_000);
    }
}
