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

pub mod cloud;
pub mod config;
pub mod error;
pub mod global;
pub mod logging;
pub mod middleware;
pub mod proxy;
pub mod state;
pub mod utils;

// Re-export core types for convenience
pub use cloud::*;
pub use config::*;
pub use global::{init, layer};
pub use middleware::{ClientInfo, TrustedProxyLayer, TrustedProxyMiddleware};
pub use proxy::*;
#[cfg(test)]
pub use state::AppState;
