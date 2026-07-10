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

//! End-to-end check that a traced runtime actually records events to disk.
//!
//! Guards against the regression where `TracedRuntime::builder()...build(..)`
//! was used instead of `build_and_start(..)`: that returns a guard with
//! recording disabled, so telemetry appeared healthy and produced empty traces.
//!
//! Configuration comes from the environment (the crate forbids `unsafe`, so this
//! cannot call `set_var`). Run with:
//!
//! ```bash
//! RUSTFLAGS="--cfg tokio_unstable" \
//! RUSTFS_RUNTIME_DIAL9_ENABLED=true \
//! RUSTFS_RUNTIME_DIAL9_OUTPUT_DIR=/tmp/rustfs-dial9-smoke \
//! RUSTFS_RUNTIME_DIAL9_MAX_FILE_SIZE=65536 \
//! RUSTFS_RUNTIME_DIAL9_ROTATION_COUNT=4 \
//!   cargo run -p rustfs-obs --features dial9 --example dial9_smoke
//! ```

use rustfs_obs::dial9::{Dial9Config, build_traced_runtime};
use std::path::Path;

const USAGE: &str = "\
set RUSTFS_RUNTIME_DIAL9_ENABLED=true and RUSTFS_RUNTIME_DIAL9_OUTPUT_DIR=<dir> \
before running this example (see the module docs for a full command line)";

/// A segment that was opened but never recorded an event still carries a header
/// (~292 bytes when this was written). A workload like the one below produces
/// several KiB. Anything under this threshold means events are not being
/// recorded, even though the file exists and the guard looks healthy.
const MIN_EXPECTED_TRACE_BYTES: u64 = 2048;

fn segment_bytes(dir: &Path, prefix: &str) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .filter_map(Result::ok)
        .filter(|e| e.file_name().to_str().is_some_and(|n| n.starts_with(prefix)))
        .filter_map(|e| e.metadata().ok())
        .map(|m| m.len())
        .sum()
}

fn main() {
    let config = Dial9Config::from_env();
    if !config.enabled {
        eprintln!("{USAGE}");
        std::process::exit(2);
    }

    let output_dir = std::path::PathBuf::from(&config.output_dir);
    println!("output_dir      = {}", config.output_dir);
    println!("disk budget     = {} bytes", config.total_disk_budget());

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.worker_threads(2).enable_all();

    let (runtime, guard) = build_traced_runtime(builder).expect("traced runtime should build");
    // Necessary but not sufficient: a guard built without `build_and_start` also
    // reports active. Only the byte count below proves events were recorded.
    assert!(guard.is_active(), "telemetry session must be live");
    println!("session active  = {}", guard.is_active());

    // Generate poll/park/spawn traffic, including a deliberately long poll.
    runtime.block_on(async {
        let mut handles = Vec::new();
        for i in 0..64_u64 {
            handles.push(tokio::spawn(async move {
                if i % 16 == 0 {
                    // Block the worker: exactly the fault dial9 exists to surface.
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                i * 2
            }));
        }
        for h in handles {
            h.await.expect("task should not panic");
        }
    });

    // Dropping the guard flushes buffered events and seals the active segment.
    drop(guard);
    drop(runtime);

    let bytes = segment_bytes(&output_dir, &config.file_prefix);
    println!("trace bytes     = {bytes}");

    assert!(
        bytes >= MIN_EXPECTED_TRACE_BYTES,
        "dial9 wrote only {bytes} bytes to {} (expected >= {MIN_EXPECTED_TRACE_BYTES}); \
         the segment holds a header but no events, so recording never started",
        output_dir.display()
    );
    println!("\nOK: dial9 recorded {bytes} bytes of trace data.");
}
