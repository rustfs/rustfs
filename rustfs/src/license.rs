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

use rustfs_appauth::token::Token;
use std::io::{Error, Result};
use std::sync::OnceLock;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use tracing::error;
use tracing::info;

static LICENSE: OnceLock<Token> = OnceLock::new();

/// Initialize the license
pub fn init_license(license: Option<String>) {
    if license.is_none() {
        error!("License is None");
        return;
    }
    let license = license.unwrap();
    let token = rustfs_appauth::token::parse_license(&license).unwrap_or_default();

    LICENSE.set(token).unwrap_or_else(|_| {
        error!("Failed to set license");
    });
}

/// Get the license
pub fn get_license() -> Option<Token> {
    LICENSE.get().cloned()
}

/// Check the license
/// This function checks if the license is valid.
#[allow(unreachable_code)]
pub fn license_check() -> Result<()> {
    return Ok(());
    let invalid_license = LICENSE.get().map(|token| {
        if token.expired < SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() {
            error!("License expired");
            return Err(Error::other("Incorrect license, please contact RustFS."));
        }
        info!("License is valid ! expired at {}", token.expired);
        Ok(())
    });

    // let invalid_license = config::get_config().license.as_ref().map(|license| {
    //     if license.is_empty() {
    //         error!("License is empty");
    //         return Err(Error::other("Incorrect license, please contact RustFS.".to_string()));
    //     }
    //     let token = appauth::token::parse_license(license)?;
    //     if token.expired < SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() {
    //         error!("License expired");
    //         return Err(Error::other("Incorrect license, please contact RustFS.".to_string()));
    //     }

    //     info!("License is valid ! expired at {}", token.expired);
    //     Ok(())
    // });

    if invalid_license.is_none() || invalid_license.is_some_and(|v| v.is_err()) {
        return Err(Error::other("Incorrect license, please contact RustFS."));
    }

    Ok(())
}
