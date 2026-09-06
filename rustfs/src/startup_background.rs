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
    bitrot_selftest_enabled_from_env, bitrot_selftest_strict_from_env, heal_enabled_from_env,
    is_on_demand_migration_module_enabled, scanner_enabled_from_env,
};
use crate::on_demand_migration::OnDemandMigrationSys;
use crate::on_demand_migration::backfill::{
    BackfillRunner, SysBackfillContexts, install_global_backfill_runner, spawn_backfill_recovery_loop,
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
const EVENT_ODM_BACKFILL_RECOVERY_CONFIGURED: &str = "odm_backfill_recovery_configured";

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
    rustfs_scanner::set_scanner_workload_admission_snapshot_provider(workload_provider.clone());

    if enable_heal || enable_scanner {
        let heal_storage = Arc::new(ECStoreHealStorage::new(store.clone()));
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

    init_on_demand_migration_backfill_runtime(store).await;

    Ok(enable_scanner)
}

/// Installs the backfill runner (admin start/cancel/status need it even
/// while the module switch is off, to read checkpoints) and, with the switch
/// on, the recovery loop that takes over expired leases (rustfs/backlog#2159).
async fn init_on_demand_migration_backfill_runtime(store: Arc<ECStore>) {
    let contexts = Arc::new(SysBackfillContexts::new(store.clone(), OnDemandMigrationSys::get()));
    let runner = BackfillRunner::for_local_node(store.clone(), contexts).await;
    if !install_global_backfill_runner(runner.clone()) {
        debug!(
            target: "rustfs::main::run",
            event = EVENT_ODM_BACKFILL_RECOVERY_CONFIGURED,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            state = "already_installed",
            "On-demand migration backfill runner already installed"
        );
        return;
    }
    let module_enabled = is_on_demand_migration_module_enabled();
    let node = runner.node().to_string();
    let state = if !module_enabled {
        "skipped_module_disabled"
    } else if spawn_backfill_recovery_loop(runner) {
        "started"
    } else {
        "skipped_no_cancel_token"
    };
    info!(
        target: "rustfs::main::run",
        event = EVENT_ODM_BACKFILL_RECOVERY_CONFIGURED,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        state = state,
        module_enabled = module_enabled,
        node = %node,
        "On-demand migration backfill recovery configured"
    );
}
