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

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    // Wall-time profiling guard for `--features hotpath` builds. The guard
    // lives for the whole process; dropping it on normal exit (SIGINT/SIGTERM
    // unwind back through run_process) prints the timing report.
    //
    // hotpath's alloc mode is intentionally NOT wired up: hotpath 0.21.x
    // panics when a MeasurementGuardSync drops on a different thread than it
    // was created on (TLS index mismatch, alloc/core.rs:151), which tokio's
    // work-stealing runtime does constantly. See rustfs/backlog#935.
    #[cfg(feature = "hotpath")]
    let _hotpath_guard = hotpath::HotpathGuardBuilder::new("main")
        .percentiles(&[50.0, 95.0, 99.0])
        .functions_limit(0)
        .build();

    rustfs::startup_entrypoint::run_process();
}
