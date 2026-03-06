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

use crate::error::{Error, Result};
use manager::IamCache;
use oidc::OidcSys;
use rustfs_ecstore::store::ECStore;
use std::sync::{Arc, OnceLock};
use store::object::ObjectStore;
use sys::IamSys;
use tracing::{error, info, instrument, warn};

pub mod cache;
pub mod error;
pub mod manager;
pub mod oidc;
pub mod oidc_state;
pub mod store;
pub mod sys;
pub mod utils;

static IAM_SYS: OnceLock<Arc<IamSys<ObjectStore>>> = OnceLock::new();
static OIDC_SYS: OnceLock<Arc<OidcSys>> = OnceLock::new();

#[instrument(skip(ecstore))]
pub async fn init_iam_sys(ecstore: Arc<ECStore>) -> Result<()> {
    if IAM_SYS.get().is_some() {
        info!("IAM system already initialized, skipping.");
        return Ok(());
    }

    info!("Starting IAM system initialization sequence...");

    // 1. Create the persistent storage adapter
    let storage_adapter = ObjectStore::new(ecstore);

    // 2. Create the cache manager.
    // The `new` method now performs a blocking initial load from disk.
    let cache_manager = IamCache::new(storage_adapter).await;

    // 3. Construct the system interface
    let iam_instance = Arc::new(IamSys::new(cache_manager));

    // 4. Securely set the global singleton
    if IAM_SYS.set(iam_instance).is_err() {
        error!("Critical: Race condition detected during IAM initialization!");
        return Err(Error::IamSysAlreadyInitialized);
    }

    info!("IAM system initialization completed successfully.");
    Ok(())
}

#[inline]
pub fn get() -> Result<Arc<IamSys<ObjectStore>>> {
    let sys = IAM_SYS.get().map(Arc::clone).ok_or(Error::IamSysNotInitialized)?;

    // Double-check the internal readiness state. The OnceLock is only set
    // after initialization and data loading complete, so this is a defensive
    // guard to ensure callers never operate on a partially initialized system.
    if !sys.is_ready() {
        return Err(Error::IamSysNotInitialized);
    }

    Ok(sys)
}

pub fn get_global_iam_sys() -> Option<Arc<IamSys<ObjectStore>>> {
    IAM_SYS.get().cloned()
}

/// Initialize the global OIDC system. Non-fatal if no OIDC providers are configured.
pub async fn init_oidc_sys() -> Result<()> {
    if OIDC_SYS.get().is_some() {
        info!("OIDC system already initialized, skipping.");
        return Ok(());
    }

    info!("Starting OIDC system initialization...");

    let oidc_sys = match OidcSys::new().await {
        Ok(sys) => {
            if sys.has_providers() {
                info!("OIDC system initialized with {} provider(s)", sys.list_providers().len());
            } else {
                info!("No OIDC providers configured");
            }
            sys
        }
        Err(e) => {
            warn!("OIDC initialization failed (non-fatal): {}", e);
            OidcSys::empty()
        }
    };

    if OIDC_SYS.set(Arc::new(oidc_sys)).is_err() {
        warn!("Race condition during OIDC initialization (non-fatal)");
    }

    Ok(())
}

/// Get the global OIDC system.
pub fn get_oidc() -> Option<Arc<OidcSys>> {
    OIDC_SYS.get().cloned()
}
