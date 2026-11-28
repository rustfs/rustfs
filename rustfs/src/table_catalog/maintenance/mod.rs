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

mod config;
#[cfg(test)]
pub(in crate::table_catalog) mod fault_observation;
mod periodic;
mod periodic_runtime;
mod worker;

pub(crate) use config::*;
pub(crate) use periodic::{
    VectorMaintenanceActiveBucketCursor, VectorMaintenanceDiscoveryCursor, VectorMaintenanceDiscoveryFailure,
    VectorMaintenancePreparationKind, VectorMaintenanceResourceStage, VectorMaintenanceSource, VectorMaintenanceTarget,
    VectorMaintenanceTargetPage,
};
#[cfg(test)]
pub(crate) use periodic::{VectorMaintenancePassReport, VectorMaintenanceScheduler};
pub(crate) use periodic_runtime::{PeriodicMaintenanceConfig, PeriodicMaintenanceRuntime, spawn_vector_maintenance};
pub(crate) use worker::*;

#[cfg(test)]
pub(crate) use fault_observation::{
    VectorMaintenanceCompletionObserver, VectorMaintenanceCompletionSnapshot, VectorMaintenanceGcLockSnapshot,
    VectorMaintenanceObservedSourceKind,
};
