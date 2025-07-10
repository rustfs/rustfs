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

use std::sync::OnceLock;
use tokio_util::sync::CancellationToken;

pub mod error;
pub mod scanner;

pub use error::{Error, Result};
pub use scanner::{
    load_data_usage_from_backend, store_data_usage_in_backend, BucketTargetUsageInfo, BucketUsageInfo, DataUsageInfo, Scanner,
    ScannerMetrics,
};

// Global cancellation token for AHM services (scanner and other background tasks)
static GLOBAL_AHM_SERVICES_CANCEL_TOKEN: OnceLock<CancellationToken> = OnceLock::new();

/// Initialize the global AHM services cancellation token
pub fn init_ahm_services_cancel_token(cancel_token: CancellationToken) -> Result<()> {
    GLOBAL_AHM_SERVICES_CANCEL_TOKEN
        .set(cancel_token)
        .map_err(|_| Error::Config("AHM services cancel token already initialized".to_string()))
}

/// Get the global AHM services cancellation token
pub fn get_ahm_services_cancel_token() -> Option<&'static CancellationToken> {
    GLOBAL_AHM_SERVICES_CANCEL_TOKEN.get()
}

/// Create and initialize the global AHM services cancellation token
pub fn create_ahm_services_cancel_token() -> CancellationToken {
    let cancel_token = CancellationToken::new();
    init_ahm_services_cancel_token(cancel_token.clone()).expect("AHM services cancel token already initialized");
    cancel_token
}

/// Shutdown all AHM services gracefully
pub fn shutdown_ahm_services() {
    if let Some(cancel_token) = GLOBAL_AHM_SERVICES_CANCEL_TOKEN.get() {
        cancel_token.cancel();
    }
}
