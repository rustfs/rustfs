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

use super::global::{AppContext, get_global_app_context, init_global_app_context};
use crate::app::storage_compat::ECStore;
use rustfs_kms::KmsServiceManager;
use std::io::{Error, Result};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StartupAppContextBootstrap {
    AlreadyAvailable,
    Initialized,
}

impl AppContext {
    pub(crate) fn ensure_startup_after_iam(store: Arc<ECStore>, kms_interface: Arc<KmsServiceManager>) -> Result<()> {
        ensure_startup_app_context_after_iam_with(
            || get_global_app_context().is_some(),
            || {
                let iam_interface = rustfs_iam::get().map_err(|_| Error::other("IAM is initialized but unavailable"))?;
                init_global_app_context(AppContext::with_default_interfaces(store, iam_interface, kms_interface));
                Ok(())
            },
        )?;
        Ok(())
    }
}

fn ensure_startup_app_context_after_iam_with<IsAvailable, InitContext>(
    is_available: IsAvailable,
    init_context: InitContext,
) -> Result<StartupAppContextBootstrap>
where
    IsAvailable: FnOnce() -> bool,
    InitContext: FnOnce() -> Result<()>,
{
    if is_available() {
        return Ok(StartupAppContextBootstrap::AlreadyAvailable);
    }

    init_context()?;
    Ok(StartupAppContextBootstrap::Initialized)
}

#[cfg(test)]
mod tests {
    use super::{StartupAppContextBootstrap, ensure_startup_app_context_after_iam_with};
    use std::io::Error;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[test]
    fn startup_app_context_bootstrap_reuses_existing_context() {
        let init_calls = Arc::new(AtomicUsize::new(0));
        let init_calls_for_assert = init_calls.clone();

        let disposition = ensure_startup_app_context_after_iam_with(
            || true,
            move || {
                init_calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        )
        .expect("existing app context should be reused");

        assert_eq!(disposition, StartupAppContextBootstrap::AlreadyAvailable);
        assert_eq!(init_calls_for_assert.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn startup_app_context_bootstrap_initializes_missing_context() {
        let init_calls = Arc::new(AtomicUsize::new(0));
        let init_calls_for_assert = init_calls.clone();

        let disposition = ensure_startup_app_context_after_iam_with(
            || false,
            move || {
                init_calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        )
        .expect("missing app context should initialize");

        assert_eq!(disposition, StartupAppContextBootstrap::Initialized);
        assert_eq!(init_calls_for_assert.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn startup_app_context_bootstrap_returns_init_error() {
        let err = ensure_startup_app_context_after_iam_with(|| false, || Err(Error::other("iam unavailable")))
            .expect_err("init failure should be returned");

        assert_eq!(err.to_string(), "iam unavailable");
    }
}
