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

use tokio_util::sync::CancellationToken;

/// Initializes the global metrics system. This should be called once at the start of the application.
/// The provided `CancellationToken` will be used to gracefully shut down the metrics system when needed.
///
/// # Arguments
/// * `token` - A `CancellationToken` that can be used to signal the metrics system to shut down gracefully.
///
/// # Example
/// ```ignore
/// use tokio_util::sync::CancellationToken;
/// use rustfs_metrics::init_metrics_system;
///
/// let token = CancellationToken::new();
/// init_metrics_system(token.clone());
///
/// // Later, when you want to shut down the metrics system:
/// token.cancel();
/// ```
/// Note: This function should only be called once during the application's lifecycle. Calling it multiple times may lead to unexpected behavior.
pub fn init_metrics_system(token: CancellationToken) {
    tracing::info!("init metrics system start");
    crate::collectors::init_metrics_collectors(token);
    tracing::info!("init metrics system done");
}
