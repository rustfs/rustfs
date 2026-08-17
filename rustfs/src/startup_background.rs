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

use crate::bitrot_selftest::run_startup_bitrot_self_test;
use crate::module_switches::{
    bitrot_selftest_enabled_from_env, bitrot_selftest_strict_from_env, heal_enabled_from_env, scanner_enabled_from_env,
};
use crate::storage_api::startup::background::{ECStore, set_workload_admission_snapshot_provider};
use crate::workload_admission::RustFsWorkloadAdmissionSnapshotProvider;
use rustfs_concurrency::WorkloadAdmissionSnapshotProvider;
use rustfs_heal::{
    create_ahm_services_cancel_token, heal::storage::ECStoreHealStorage, init_heal_manager_with_workload_provider,
};
use std::{io::Result, sync::Arc};
use tracing::{debug, info};

const LOG_COMPONENT_MAIN: &str = "main";
const LOG_SUBSYSTEM_STARTUP: &str = "startup";
const EVENT_BACKGROUND_SERVICES_CONFIGURED: &str = "background_services_configured";

pub(crate) async fn init_background_service_runtime(store: Arc<ECStore>) -> Result<bool> {
    // Pin the bitrot algorithms before anything can write or verify a shard:
    // the check costs well under a millisecond, and in strict mode a drifted
    // build must abort here rather than after it has touched data
    // (rustfs/backlog#1873).
    run_startup_bitrot_self_test(bitrot_selftest_enabled_from_env(), bitrot_selftest_strict_from_env()).await?;

    let _ = create_ahm_services_cancel_token();

    let enable_scanner = scanner_enabled_from_env();
    let enable_heal = heal_enabled_from_env();

    info!(
        target: "rustfs::main::run",
        event = EVENT_BACKGROUND_SERVICES_CONFIGURED,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        enable_scanner = enable_scanner,
        enable_heal = enable_heal,
        "Background services configured"
    );

    let workload_provider: Arc<dyn WorkloadAdmissionSnapshotProvider + Send + Sync> =
        Arc::new(RustFsWorkloadAdmissionSnapshotProvider);
    let _ = set_workload_admission_snapshot_provider(workload_provider.clone());

    if enable_heal || enable_scanner {
        let heal_storage = Arc::new(ECStoreHealStorage::new(store));
        init_heal_manager_with_workload_provider(heal_storage, None, Some(workload_provider)).await?;
    }

    if !enable_heal && !enable_scanner {
        debug!(
            target: "rustfs::main::run",
            event = EVENT_BACKGROUND_SERVICES_CONFIGURED,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            enable_scanner = false,
            enable_heal = false,
            ahm_state = "skipped",
            reason = "disabled",
            "Background services disabled"
        );
    }

    Ok(enable_scanner)
}
