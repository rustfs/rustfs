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

fn main() {
    println!("cargo::rerun-if-changed=build.rs");
    println!("cargo::rerun-if-env-changed=RUSTFLAGS");

    let ping_observability_enabled = std::env::var_os("CARGO_FEATURE_DIAGNOSTIC_INTERNODE_HTTP2_PING").is_some();
    let hyper_unstable_tracing = std::env::var_os("CARGO_CFG_HYPER_UNSTABLE_TRACING").is_some();

    if ping_observability_enabled && !hyper_unstable_tracing {
        panic!(
            "\n\
             rustfs-protos: the `diagnostic-internode-http2-ping` feature requires\n\
             `--cfg hyper_unstable_tracing` so Hyper emits the exact keepalive\n\
             timeout event. Build with:\n\
             \n    RUSTFLAGS=\"--cfg hyper_unstable_tracing\" cargo build -p rustfs --features diagnostic-internode-http2-ping\n"
        );
    }
}
