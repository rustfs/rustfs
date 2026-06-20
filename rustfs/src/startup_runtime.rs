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
use std::io::Result;

pub async fn init_startup_runtime_foundation(config: &Config) -> Result<()> {
    log_startup_runtime_diagnostics();
    init_profiling_runtime().await;
    rustfs_trusted_proxies::init();
    install_default_crypto_provider();
    init_outbound_tls_material(config).await
}
