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

#![allow(dead_code)]

// Default value function
fn default_batch_size() -> usize {
    10
}
fn default_queue_size() -> usize {
    10000
}
fn default_max_retry() -> u32 {
    5
}
fn default_retry_interval() -> std::time::Duration {
    std::time::Duration::from_secs(3)
}
