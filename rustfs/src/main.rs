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

#[cfg(all(target_os = "linux", target_env = "gnu", target_arch = "x86_64"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// Configure jemalloc: limit arenas to reduce memory fragmentation.
// Default narenas = 4 * CPU cores, which can cause excessive fragmentation
// in high-concurrency workloads. Setting to 4 provides a good balance
// between concurrency and memory efficiency.
#[cfg(all(target_os = "linux", target_env = "gnu", target_arch = "x86_64"))]
#[unsafe(export_name = "malloc_conf")]
pub static MALLOC_CONF: &[u8] = b"narenas:4\0";

#[cfg(not(all(target_os = "linux", target_env = "gnu", target_arch = "x86_64")))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    rustfs::startup_entrypoint::run_process();
}
