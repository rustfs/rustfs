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

//! Enforces that the `dial9` feature is paired with `--cfg tokio_unstable`.
//!
//! `dial9-tokio-telemetry` hooks Tokio's unstable runtime instrumentation. The
//! flag lives in `RUSTFLAGS`, which a caller can silently clobber by exporting
//! their own value — that used to turn telemetry off with no diagnostic. Fail
//! the build instead.

fn main() {
    println!("cargo::rerun-if-changed=build.rs");
    println!("cargo::rerun-if-env-changed=RUSTFLAGS");

    let dial9_enabled = std::env::var_os("CARGO_FEATURE_DIAL9").is_some();
    let tokio_unstable = std::env::var_os("CARGO_CFG_TOKIO_UNSTABLE").is_some();

    if dial9_enabled && !tokio_unstable {
        panic!(
            "\n\
             rustfs-obs: the `dial9` feature requires a `--cfg tokio_unstable` build.\n\
             \n\
             Build telemetry-enabled binaries with:\n\
             \n    RUSTFLAGS=\"--cfg tokio_unstable\" cargo build --features dial9\n\
             \n\
             or run `make build-profiling`. Note that setting RUSTFLAGS in the\n\
             environment overrides `.cargo/config.toml`, so the flag must be\n\
             present in the value you export.\n"
        );
    }
}
