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

use crate::storage_compat::{ECStore, EndpointServerPools};
use crate::{
    config::Config,
    init::{init_buffer_profile_system, init_kms_system},
    server::ServiceStateManager,
    startup_iam::IamBootstrapDisposition,
    startup_optional_runtime_sidecars::{OptionalRuntimeServices, init_optional_runtime_services},
    startup_service_components::{
        init_audit_runtime, init_auth_integrations, init_background_service_runtime, init_bucket_metadata_runtime,
        init_deadlock_detector_runtime, init_embedded_bucket_metadata_runtime, init_embedded_iam_runtime,
        init_embedded_notification_runtime, init_embedded_optional_service_runtime, init_iam_runtime, init_notification_runtime,
        init_observability_runtime,
    },
};
use rustfs_common::GlobalReadiness;
use std::{io::Result, sync::Arc};
use tokio_util::sync::CancellationToken;

pub struct StartupServiceRuntime {
    pub optional_runtimes: OptionalRuntimeServices,
    pub iam_bootstrap: IamBootstrapDisposition,
    pub enable_scanner: bool,
}

pub struct EmbeddedStartupServiceRuntime {
    pub iam_bootstrap: IamBootstrapDisposition,
}

pub async fn init_embedded_startup_runtime_services(
    config: &Config,
    endpoint_pools: EndpointServerPools,
    store: Arc<ECStore>,
    ctx: CancellationToken,
    readiness: Arc<GlobalReadiness>,
) -> Result<EmbeddedStartupServiceRuntime> {
    init_embedded_optional_service_runtime(config).await;
    let buckets = init_embedded_bucket_metadata_runtime(store.clone()).await?;
    let iam_bootstrap = init_embedded_iam_runtime(store, ctx, readiness)
        .await
        .map_err(|err| std::io::Error::other(format!("IAM bootstrap setup: {err}")))?;
    init_embedded_notification_runtime(endpoint_pools, buckets).await;

    Ok(EmbeddedStartupServiceRuntime { iam_bootstrap })
}

pub async fn init_startup_runtime_services(
    config: &Config,
    endpoint_pools: EndpointServerPools,
    store: Arc<ECStore>,
    ctx: CancellationToken,
    readiness: Arc<GlobalReadiness>,
    state_manager: Arc<ServiceStateManager>,
) -> Result<StartupServiceRuntime> {
    init_kms_system(config).await?;

    let optional_runtimes = init_optional_runtime_services().await?;

    init_buffer_profile_system(config);
    init_audit_runtime().await;
    init_deadlock_detector_runtime();

    let buckets = init_bucket_metadata_runtime(store.clone(), ctx.clone()).await?;
    let iam_bootstrap = init_iam_runtime(store.clone(), ctx.clone(), readiness, state_manager).await?;
    init_auth_integrations().await?;
    init_notification_runtime(endpoint_pools, buckets).await?;
    let enable_scanner = init_background_service_runtime(store.clone()).await?;
    init_observability_runtime(ctx.clone()).await;

    Ok(StartupServiceRuntime {
        optional_runtimes,
        iam_bootstrap,
        enable_scanner,
    })
}
