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

use crate::{
    config::Config,
    startup_runtime_hooks::{init_profiling_runtime, install_default_crypto_provider, log_startup_runtime_diagnostics},
    startup_tls_material::init_outbound_tls_material,
};
use rustfs_config::{DEFAULT_API_OBJECT_MAX_VERSIONS, ENV_API_OBJECT_MAX_VERSIONS};
use rustfs_utils::EnvParseOutcome;
use std::io::{Error, Result};

pub(crate) async fn init_startup_runtime_foundation(config: &Config) -> Result<()> {
    log_startup_runtime_diagnostics();
    init_profiling_runtime().await;
    rustfs_trusted_proxies::init();
    install_default_crypto_provider();
    init_object_max_versions_config()?;
    init_outbound_tls_material(config).await
}

fn init_object_max_versions_config() -> Result<()> {
    let limit = match rustfs_utils::get_env_parse_outcome::<u64>(ENV_API_OBJECT_MAX_VERSIONS) {
        EnvParseOutcome::Absent => DEFAULT_API_OBJECT_MAX_VERSIONS,
        EnvParseOutcome::Invalid => return Err(object_max_versions_config_error()),
        EnvParseOutcome::Parsed(value) => object_max_versions_limit_from_u64(value)?,
    };

    rustfs_filemeta::set_object_max_versions(limit).map_err(Error::other)
}

fn object_max_versions_limit_from_u64(value: u64) -> Result<usize> {
    if value == 0 {
        return Err(object_max_versions_config_error());
    }

    let limit = usize::try_from(value).map_err(|_| object_max_versions_config_error())?;
    if limit > DEFAULT_API_OBJECT_MAX_VERSIONS {
        return Err(object_max_versions_config_error());
    }
    Ok(limit)
}

fn object_max_versions_config_error() -> Error {
    Error::other(format!(
        "{ENV_API_OBJECT_MAX_VERSIONS} must be a positive integer no greater than {DEFAULT_API_OBJECT_MAX_VERSIONS}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct ObjectMaxVersionsRestore {
        previous: usize,
    }

    impl Drop for ObjectMaxVersionsRestore {
        fn drop(&mut self) {
            rustfs_filemeta::set_object_max_versions(self.previous).expect("restore object max versions limit after test");
        }
    }

    fn with_object_max_versions_env<R>(rustfs_value: Option<&str>, minio_value: Option<&str>, test: impl FnOnce() -> R) -> R {
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _serial = LOCK.lock().expect("serialize object max versions env tests");
        let previous = rustfs_filemeta::object_max_versions();
        let _restore = ObjectMaxVersionsRestore { previous };

        temp_env::with_vars(
            [
                (rustfs_config::ENV_API_OBJECT_MAX_VERSIONS, rustfs_value),
                ("MINIO_API_OBJECT_MAX_VERSIONS", minio_value),
            ],
            test,
        )
    }

    #[test]
    fn object_max_versions_env_sets_filemeta_limit() {
        with_object_max_versions_env(Some("3"), None, || {
            init_object_max_versions_config().expect("valid object max versions env must initialize");
            assert_eq!(rustfs_filemeta::object_max_versions(), 3);
        });
    }

    #[test]
    fn minio_object_max_versions_env_alias_sets_filemeta_limit() {
        with_object_max_versions_env(None, Some("4"), || {
            init_object_max_versions_config().expect("valid MinIO alias must initialize");
            assert_eq!(rustfs_filemeta::object_max_versions(), 4);
        });
    }

    #[test]
    fn object_max_versions_env_rejects_zero() {
        with_object_max_versions_env(Some("0"), None, || {
            let err = init_object_max_versions_config().expect_err("zero object max versions must fail startup config");
            assert!(err.to_string().contains(rustfs_config::ENV_API_OBJECT_MAX_VERSIONS));
        });
    }

    #[test]
    fn object_max_versions_env_rejects_malformed_value() {
        with_object_max_versions_env(Some("not-a-number"), None, || {
            let err = init_object_max_versions_config().expect_err("malformed object max versions must fail startup config");
            assert!(err.to_string().contains(rustfs_config::ENV_API_OBJECT_MAX_VERSIONS));
        });
    }
}
