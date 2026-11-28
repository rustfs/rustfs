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

use super::super::*;

fn validate_table_maintenance_config_version(version: u16) -> TableCatalogStoreResult<()> {
    if version != TABLE_MAINTENANCE_CONFIG_VERSION {
        return Err(TableCatalogStoreError::Invalid(
            "unsupported table maintenance config entry version".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_table_maintenance_config(config: &TableMaintenanceConfig) -> TableCatalogStoreResult<()> {
    validate_table_maintenance_config_version(config.version)?;
    if config.worker_lease_timeout_seconds == 0 {
        return Err(TableCatalogStoreError::Invalid(
            "worker-lease-timeout-seconds must be greater than zero".to_string(),
        ));
    }
    if config.worker_lease_timeout_seconds > TABLE_MAINTENANCE_WORKER_LEASE_TIMEOUT_MAX_SECONDS {
        return Err(TableCatalogStoreError::Invalid(format!(
            "worker-lease-timeout-seconds cannot exceed {TABLE_MAINTENANCE_WORKER_LEASE_TIMEOUT_MAX_SECONDS}"
        )));
    }
    if config.max_retry_attempts > 10 {
        return Err(TableCatalogStoreError::Invalid("max-retry-attempts cannot exceed 10".to_string()));
    }
    if config.max_retry_attempts > 0 && config.retry_initial_backoff_seconds == 0 {
        return Err(TableCatalogStoreError::Invalid(
            "retry-initial-backoff-seconds must be greater than zero when retry is enabled".to_string(),
        ));
    }
    if config.max_retry_attempts > 0 && config.retry_initial_backoff_seconds > TABLE_MAINTENANCE_RETRY_BACKOFF_MAX_SECONDS {
        return Err(TableCatalogStoreError::Invalid(format!(
            "retry-initial-backoff-seconds cannot exceed {TABLE_MAINTENANCE_RETRY_BACKOFF_MAX_SECONDS}"
        )));
    }
    if config.max_retry_attempts > 0 && config.retry_max_backoff_seconds > TABLE_MAINTENANCE_RETRY_BACKOFF_MAX_SECONDS {
        return Err(TableCatalogStoreError::Invalid(format!(
            "retry-max-backoff-seconds cannot exceed {TABLE_MAINTENANCE_RETRY_BACKOFF_MAX_SECONDS}"
        )));
    }
    if config.max_retry_attempts > 0 && config.retry_max_backoff_seconds < config.retry_initial_backoff_seconds {
        return Err(TableCatalogStoreError::Invalid(
            "retry-max-backoff-seconds must be greater than or equal to retry-initial-backoff-seconds".to_string(),
        ));
    }
    if config.quarantine_enabled && config.quarantine_retention_seconds == 0 {
        return Err(TableCatalogStoreError::Invalid(
            "quarantine-retention-seconds must be greater than zero when quarantine is enabled".to_string(),
        ));
    }
    Ok(())
}

use rustfs_concurrency::adaptive::{NodeAdmission, NodePermit};
use std::num::{NonZeroU16, NonZeroU64, NonZeroUsize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

pub(crate) const VECTOR_MAINTENANCE_MEMORY_UNIT_BYTES: u64 = 65_536;
pub(crate) const VECTOR_MAINTENANCE_MAX_BATCH_BYTES: u64 = 16 * 1024 * 1024;
pub(crate) const VECTOR_MAINTENANCE_MAX_RETAINED_BATCHES: usize = 2;

#[derive(Clone, Debug)]
pub(crate) struct VectorMaintenanceRuntimeConfig {
    pub(crate) enabled: bool,
    pub(crate) execution_account: String,
    pub(crate) discovery_interval: Duration,
    pub(crate) pass_admission_budget: Duration,
    pub(crate) execution_admission_budget: Duration,
    pub(crate) resources_per_pass: NonZeroUsize,
    pub(crate) discovery_calls_per_pass: NonZeroUsize,
    pub(crate) queue_capacity: NonZeroUsize,
    pub(crate) queue_bytes: NonZeroUsize,
    pub(crate) compute_concurrency: NonZeroUsize,
    pub(crate) control_concurrency: NonZeroUsize,
    pub(crate) memory_bytes: NonZeroU64,
    pub(crate) io_concurrency: NonZeroUsize,
    pub(crate) delete_batch_size: NonZeroUsize,
    pub(crate) lease_duration: Duration,
    pub(crate) heartbeat_interval: Duration,
    pub(crate) max_attempts: NonZeroU16,
    pub(crate) retry_initial_backoff: Duration,
    pub(crate) retry_max_backoff: Duration,
}

impl Default for VectorMaintenanceRuntimeConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            execution_account: String::new(),
            discovery_interval: Duration::from_secs(300),
            pass_admission_budget: Duration::from_secs(30),
            execution_admission_budget: Duration::from_secs(900),
            resources_per_pass: NonZeroUsize::new(8).expect("nonzero constant"),
            discovery_calls_per_pass: NonZeroUsize::new(64).expect("nonzero constant"),
            queue_capacity: NonZeroUsize::new(64).expect("nonzero constant"),
            queue_bytes: NonZeroUsize::new(1024 * 1024).expect("nonzero constant"),
            compute_concurrency: NonZeroUsize::MIN,
            control_concurrency: NonZeroUsize::MIN,
            memory_bytes: NonZeroU64::new(256 * 1024 * 1024).expect("nonzero constant"),
            io_concurrency: NonZeroUsize::new(2).expect("nonzero constant"),
            delete_batch_size: NonZeroUsize::new(32).expect("nonzero constant"),
            lease_duration: Duration::from_secs(900),
            heartbeat_interval: Duration::from_secs(60),
            max_attempts: NonZeroU16::new(3).expect("nonzero constant"),
            retry_initial_backoff: Duration::from_secs(5),
            retry_max_backoff: Duration::from_secs(300),
        }
    }
}

impl VectorMaintenanceRuntimeConfig {
    /// Parse one instance's persisted snapshot. IAM admission remains instance-bound.
    pub(crate) fn from_server_config(config: &rustfs_config::server_config::Config) -> TableCatalogStoreResult<Self> {
        let mut runtime = Self::default();
        let Some(targets) = config.0.get(rustfs_config::S3VECTORS_MAINTENANCE_SUB_SYS) else {
            return Ok(runtime);
        };
        if targets.len() != 1 || !targets.contains_key(rustfs_config::DEFAULT_DELIMITER) {
            return Err(vector_config_error("only the default target is supported"));
        }
        let kvs = &targets[rustfs_config::DEFAULT_DELIMITER];
        let mut enable_seen = false;
        let mut account_seen = false;
        for kv in &kvs.0 {
            match kv.key.as_str() {
                rustfs_config::ENABLE_KEY if !enable_seen => {
                    enable_seen = true;
                    runtime.enabled = match kv.value.as_str() {
                        "off" => false,
                        "on" => true,
                        _ => return Err(vector_config_error("enable must be off or on")),
                    };
                }
                rustfs_config::S3VECTORS_MAINTENANCE_EXECUTION_ACCOUNT if !account_seen => {
                    account_seen = true;
                    runtime.execution_account.clone_from(&kv.value);
                }
                _ => return Err(vector_config_error("unknown or duplicate key")),
            }
        }
        runtime.validate()?;
        Ok(runtime)
    }

    pub(crate) fn validate(&self) -> TableCatalogStoreResult<()> {
        let account = &self.execution_account;
        if (self.enabled || !account.is_empty())
            && (!rustfs_policy::auth::is_access_key_valid(account)
                || account.len() > 65_536
                || account.chars().any(char::is_control)
                || account.contains(['/', '\\'])
                || matches!(account.as_str(), "." | "..")
                || rustfs_policy::auth::contains_reserved_chars(account)
                || account.ends_with(rustfs_utils::path::GLOBAL_DIR_SUFFIX))
        {
            return Err(vector_config_error("invalid execution_account"));
        }
        for (value, min, max) in [
            (self.discovery_interval, 30, 86_400),
            (self.pass_admission_budget, 1, 300),
            (self.execution_admission_budget, 1, 86_400),
            (self.lease_duration, 120, 86_400),
            (self.heartbeat_interval, 1, 28_800),
            (self.retry_initial_backoff, 1, 86_400),
            (self.retry_max_backoff, 1, 86_400),
        ] {
            if value.subsec_nanos() != 0 || !(min..=max).contains(&value.as_secs()) {
                return Err(vector_config_error("duration is outside its supported whole-second range"));
            }
        }
        for (value, min, max) in [
            (self.resources_per_pass.get(), 1, 64),
            (self.discovery_calls_per_pass.get(), 1, 1024),
            (self.queue_capacity.get(), 2, 1024),
            (self.queue_bytes.get(), 65_536, 16 * 1024 * 1024),
            (self.compute_concurrency.get(), 1, 8),
            (self.control_concurrency.get(), 1, 4),
            (self.io_concurrency.get(), 1, 16),
            (self.delete_batch_size.get(), 1, 32),
            (usize::from(self.max_attempts.get()), 1, 10),
        ] {
            if !(min..=max).contains(&value) {
                return Err(vector_config_error("resource limit is outside its supported range"));
            }
        }
        if !self.queue_capacity.get().is_multiple_of(2)
            || !self.queue_bytes.get().is_multiple_of(2)
            || !(VECTOR_MAINTENANCE_MEMORY_UNIT_BYTES..=4 * 1024 * 1024 * 1024).contains(&self.memory_bytes.get())
            || !self.memory_bytes.get().is_multiple_of(VECTOR_MAINTENANCE_MEMORY_UNIT_BYTES)
            || u64::try_from(self.queue_bytes.get()).map_or(true, |bytes| bytes > self.memory_bytes.get())
            || self
                .heartbeat_interval
                .checked_mul(3)
                .is_none_or(|interval| interval > self.lease_duration)
            || self.retry_initial_backoff > self.retry_max_backoff
        {
            return Err(vector_config_error("inconsistent resource or lease limits"));
        }
        Ok(())
    }
}

fn vector_config_error(message: &str) -> TableCatalogStoreError {
    TableCatalogStoreError::Invalid(format!("invalid vector maintenance configuration: {message}"))
}

pub(crate) struct VectorMaintenanceDiscoveryBudget {
    cancellation: CancellationToken,
    deadline: Instant,
    remaining_calls: AtomicUsize,
}

impl VectorMaintenanceDiscoveryBudget {
    pub(crate) fn new(cancellation: CancellationToken, deadline: Instant, calls: NonZeroUsize) -> Self {
        Self {
            cancellation,
            deadline,
            remaining_calls: AtomicUsize::new(calls.get()),
        }
    }

    pub(crate) fn try_start_call(&self) -> bool {
        !self.stopped()
            && self
                .remaining_calls
                .try_update(Ordering::AcqRel, Ordering::Acquire, |remaining| remaining.checked_sub(1))
                .is_ok()
    }

    pub(crate) fn stopped(&self) -> bool {
        self.cancellation.is_cancelled() || Instant::now() >= self.deadline || self.remaining_calls() == 0
    }

    pub(crate) fn remaining_calls(&self) -> usize {
        self.remaining_calls.load(Ordering::Acquire)
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum VectorMaintenanceBudgetError {
    #[error("vector maintenance admission stopped")]
    Stopped,
    #[error("vector maintenance capacity is unavailable")]
    Unavailable,
    #[error("vector maintenance buffer exceeds its budget")]
    Exceeded { requested_bytes: u64, limit_bytes: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VectorMaintenanceLane {
    Compute,
    Control,
}

pub(crate) struct VectorMaintenanceBudgets {
    memory: Arc<Semaphore>,
    memory_bytes: u64,
    io: Arc<Semaphore>,
    batches: Arc<Semaphore>,
    compute: Arc<Semaphore>,
    control: Arc<Semaphore>,
    active_resources: Mutex<Vec<CatalogResourceIdentity>>,
    node: Option<Arc<NodeAdmission>>,
}

#[derive(Debug)]
pub(crate) struct VectorMaintenanceMemoryReservation {
    _permit: OwnedSemaphorePermit,
}

#[derive(Debug)]
pub(crate) struct VectorMaintenanceBatchReservation {
    _memory: Arc<VectorMaintenanceMemoryReservation>,
    _slot: OwnedSemaphorePermit,
}

pub(crate) struct VectorMaintenanceTaskPermit {
    budgets: Arc<VectorMaintenanceBudgets>,
    resource: CatalogResourceIdentity,
    _lane: OwnedSemaphorePermit,
    node: Option<NodePermit>,
}

impl VectorMaintenanceBudgets {
    pub(crate) fn new(
        config: &VectorMaintenanceRuntimeConfig,
        node: Option<Arc<NodeAdmission>>,
    ) -> TableCatalogStoreResult<Arc<Self>> {
        config.validate()?;
        let units = usize::try_from(config.memory_bytes.get() / VECTOR_MAINTENANCE_MEMORY_UNIT_BYTES)
            .map_err(|_| vector_config_error("memory units exceed the platform limit"))?;
        Ok(Arc::new(Self {
            memory: Arc::new(Semaphore::new(units)),
            memory_bytes: config.memory_bytes.get(),
            io: Arc::new(Semaphore::new(config.io_concurrency.get())),
            batches: Arc::new(Semaphore::new(VECTOR_MAINTENANCE_MAX_RETAINED_BATCHES)),
            compute: Arc::new(Semaphore::new(config.compute_concurrency.get())),
            control: Arc::new(Semaphore::new(config.control_concurrency.get())),
            active_resources: Mutex::new(Vec::with_capacity(config.compute_concurrency.get() + config.control_concurrency.get())),
            node,
        }))
    }

    /// Shared buffers retain this same owner until their last clone is released.
    pub(crate) fn try_reserve_memory(
        &self,
        bytes: u64,
    ) -> Result<Arc<VectorMaintenanceMemoryReservation>, VectorMaintenanceBudgetError> {
        let units = bytes
            .checked_add(VECTOR_MAINTENANCE_MEMORY_UNIT_BYTES - 1)
            .map(|bytes| bytes / VECTOR_MAINTENANCE_MEMORY_UNIT_BYTES)
            .and_then(|units| u32::try_from(units).ok())
            .filter(|_| bytes <= self.memory_bytes)
            .ok_or(VectorMaintenanceBudgetError::Exceeded {
                requested_bytes: bytes,
                limit_bytes: self.memory_bytes,
            })?;
        let permit = self
            .memory
            .clone()
            .try_acquire_many_owned(units)
            .map_err(|_| VectorMaintenanceBudgetError::Unavailable)?;
        Ok(Arc::new(VectorMaintenanceMemoryReservation { _permit: permit }))
    }

    pub(crate) fn try_reserve_batch(
        &self,
        bytes: u64,
    ) -> Result<Arc<VectorMaintenanceBatchReservation>, VectorMaintenanceBudgetError> {
        if bytes > VECTOR_MAINTENANCE_MAX_BATCH_BYTES {
            return Err(VectorMaintenanceBudgetError::Exceeded {
                requested_bytes: bytes,
                limit_bytes: VECTOR_MAINTENANCE_MAX_BATCH_BYTES,
            });
        }
        let slot = self
            .batches
            .clone()
            .try_acquire_owned()
            .map_err(|_| VectorMaintenanceBudgetError::Unavailable)?;
        let memory = self.try_reserve_memory(bytes)?;
        Ok(Arc::new(VectorMaintenanceBatchReservation {
            _memory: memory,
            _slot: slot,
        }))
    }

    #[cfg(test)]
    pub(crate) fn try_acquire_io(&self) -> Result<OwnedSemaphorePermit, VectorMaintenanceBudgetError> {
        self.io
            .clone()
            .try_acquire_owned()
            .map_err(|_| VectorMaintenanceBudgetError::Unavailable)
    }

    /// Acquire all scheduling units before any Catalog or object protection.
    pub(crate) fn try_admit(
        self: &Arc<Self>,
        resource: &CatalogResourceIdentity,
        lane: VectorMaintenanceLane,
    ) -> Result<VectorMaintenanceTaskPermit, VectorMaintenanceBudgetError> {
        let lane = match lane {
            VectorMaintenanceLane::Compute => &self.compute,
            VectorMaintenanceLane::Control => &self.control,
        };
        let lane = lane
            .clone()
            .try_acquire_owned()
            .map_err(|_| VectorMaintenanceBudgetError::Unavailable)?;
        let node = self
            .node
            .as_ref()
            .map(|owner| owner.try_acquire().ok_or(VectorMaintenanceBudgetError::Unavailable))
            .transpose()?;
        let mut active = self
            .active_resources
            .lock()
            .map_err(|_| VectorMaintenanceBudgetError::Unavailable)?;
        if active.contains(resource) {
            return Err(VectorMaintenanceBudgetError::Unavailable);
        }
        active.push(resource.clone());
        Ok(VectorMaintenanceTaskPermit {
            budgets: self.clone(),
            resource: resource.clone(),
            _lane: lane,
            node,
        })
    }
}

impl VectorMaintenanceTaskPermit {
    pub(crate) fn complete(mut self) {
        if let Some(node) = self.node.take() {
            node.complete();
        }
    }
}

impl Drop for VectorMaintenanceTaskPermit {
    fn drop(&mut self) {
        let mut active = self
            .budgets
            .active_resources
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(index) = active.iter().position(|resource| resource == &self.resource) {
            active.swap_remove(index);
        }
    }
}

#[derive(Clone)]
pub(crate) struct VectorMaintenanceControl {
    pub(crate) stop_new_work: CancellationToken,
    pub(crate) admission_deadline: Instant,
    pub(crate) budgets: Arc<VectorMaintenanceBudgets>,
    dispatch: Option<Arc<VectorMaintenanceDispatchGuard>>,
}

impl VectorMaintenanceControl {
    pub(crate) fn new(
        stop_new_work: CancellationToken,
        admission_deadline: Instant,
        budgets: Arc<VectorMaintenanceBudgets>,
    ) -> Self {
        Self {
            stop_new_work,
            admission_deadline,
            budgets,
            dispatch: None,
        }
    }

    pub(crate) fn with_dispatch(mut self, dispatch: Arc<VectorMaintenanceDispatchGuard>) -> Self {
        self.dispatch = Some(dispatch);
        self
    }

    pub(crate) fn dispatch_guard(&self) -> TableCatalogStoreResult<&VectorMaintenanceDispatchGuard> {
        self.dispatch
            .as_deref()
            .ok_or_else(|| TableCatalogStoreError::Invalid("maintenance dispatch owner is missing".into()))
    }

    pub(crate) fn check_admission(&self) -> Result<(), VectorMaintenanceBudgetError> {
        if self.stop_new_work.is_cancelled() || Instant::now() >= self.admission_deadline {
            return Err(VectorMaintenanceBudgetError::Stopped);
        }
        Ok(())
    }

    pub(crate) fn reserve_memory(
        &self,
        bytes: u64,
    ) -> Result<Arc<VectorMaintenanceMemoryReservation>, VectorMaintenanceBudgetError> {
        self.check_admission()?;
        self.budgets.try_reserve_memory(bytes)
    }

    pub(crate) fn reserve_batch(
        &self,
        measured_bytes: u64,
    ) -> Result<Arc<VectorMaintenanceBatchReservation>, VectorMaintenanceBudgetError> {
        self.check_admission()?;
        self.budgets.try_reserve_batch(measured_bytes)
    }

    /// Only waiting for admission is cancellable. The returned permit follows real I/O.
    pub(crate) async fn acquire_io(&self) -> Result<OwnedSemaphorePermit, VectorMaintenanceBudgetError> {
        self.check_admission()?;
        let permit = tokio::select! {
            biased;
            _ = self.stop_new_work.cancelled() => return Err(VectorMaintenanceBudgetError::Stopped),
            _ = tokio::time::sleep_until(self.admission_deadline) => return Err(VectorMaintenanceBudgetError::Stopped),
            permit = self.budgets.io.clone().acquire_owned() => permit.map_err(|_| VectorMaintenanceBudgetError::Unavailable)?,
        };
        self.check_admission()?;
        Ok(permit)
    }
}

#[cfg(test)]
mod vector_tests {
    use super::*;
    use crate::storage_api::table::managed::ManagedTableId;
    use rustfs_config::server_config::{Config, KV, KVS};
    use std::collections::HashMap;

    fn server_config(target: &str, values: &[(&str, &str)]) -> Config {
        Config(HashMap::from([(
            rustfs_config::S3VECTORS_MAINTENANCE_SUB_SYS.to_owned(),
            HashMap::from([(
                target.to_owned(),
                KVS(values
                    .iter()
                    .map(|(key, value)| KV {
                        key: (*key).to_owned(),
                        value: (*value).to_owned(),
                        hidden_if_empty: false,
                    })
                    .collect()),
            )]),
        )]))
    }

    #[test]
    fn vector_runtime_is_disabled_by_default() {
        let config = VectorMaintenanceRuntimeConfig::from_server_config(&Config(HashMap::new())).unwrap();
        assert!(!config.enabled);
        assert!(config.execution_account.is_empty());
        assert_eq!(config.discovery_interval, std::time::Duration::from_secs(300));
        assert_eq!(config.memory_bytes.get(), 256 * 1024 * 1024);
        assert_eq!(config.compute_concurrency.get(), 1);
        assert_eq!(config.control_concurrency.get(), 1);
        config.validate().unwrap();
    }

    #[test]
    fn vector_server_config_rejects_invalid_structure_and_accounts() {
        for (target, values) in [
            ("other", vec![("enable", "off")]),
            ("_", vec![("enable", "true")]),
            ("_", vec![("enable", "on")]),
            ("_", vec![("enable", "off"), ("enable", "off")]),
            ("_", vec![("comment", "unused")]),
            ("_", vec![("execution_account", "ab")]),
            ("_", vec![("execution_account", "abc,def")]),
            ("_", vec![("execution_account", "abc/def/")]),
            ("_", vec![("execution_account", "abc\ndef")]),
        ] {
            assert!(VectorMaintenanceRuntimeConfig::from_server_config(&server_config(target, &values)).is_err());
        }
        let config = VectorMaintenanceRuntimeConfig::from_server_config(&server_config(
            "_",
            &[("enable", "on"), ("execution_account", "maintenance-user")],
        ))
        .unwrap();
        assert!(config.enabled);
        assert_eq!(config.execution_account, "maintenance-user");
        assert!(
            VectorMaintenanceRuntimeConfig::from_server_config(&server_config(
                "_",
                &[("execution_account", &"a".repeat(65_537))],
            ))
            .is_err()
        );
    }

    #[test]
    fn vector_runtime_limits_reject_unsafe_combinations() {
        let default = VectorMaintenanceRuntimeConfig::default();
        for config in [
            VectorMaintenanceRuntimeConfig {
                queue_capacity: std::num::NonZeroUsize::new(1).unwrap(),
                ..default.clone()
            },
            VectorMaintenanceRuntimeConfig {
                queue_capacity: std::num::NonZeroUsize::new(3).unwrap(),
                ..default.clone()
            },
            VectorMaintenanceRuntimeConfig {
                memory_bytes: std::num::NonZeroU64::new(65_537).unwrap(),
                ..default.clone()
            },
            VectorMaintenanceRuntimeConfig {
                memory_bytes: std::num::NonZeroU64::new(65_536).unwrap(),
                ..default.clone()
            },
            VectorMaintenanceRuntimeConfig {
                heartbeat_interval: std::time::Duration::from_secs(301),
                ..default.clone()
            },
            VectorMaintenanceRuntimeConfig {
                retry_initial_backoff: std::time::Duration::ZERO,
                ..default.clone()
            },
            VectorMaintenanceRuntimeConfig {
                discovery_interval: std::time::Duration::from_millis(30_001),
                ..default
            },
        ] {
            assert!(config.validate().is_err());
        }
    }

    #[test]
    fn vector_memory_and_io_follow_last_real_owner() {
        let config = VectorMaintenanceRuntimeConfig {
            memory_bytes: std::num::NonZeroU64::new(65_536).unwrap(),
            queue_bytes: std::num::NonZeroUsize::new(65_536).unwrap(),
            io_concurrency: std::num::NonZeroUsize::new(1).unwrap(),
            ..Default::default()
        };
        let budgets = VectorMaintenanceBudgets::new(&config, None).unwrap();
        let reservation = budgets.try_reserve_memory(1).unwrap();
        let retained = reservation.clone();
        drop(reservation);
        assert!(matches!(budgets.try_reserve_memory(1), Err(VectorMaintenanceBudgetError::Unavailable)));
        drop(retained);
        assert!(budgets.try_reserve_memory(65_536).is_ok());
        assert!(matches!(
            budgets.try_reserve_memory(u64::MAX),
            Err(VectorMaintenanceBudgetError::Exceeded { .. })
        ));
        let io = budgets.try_acquire_io().unwrap();
        assert!(budgets.try_acquire_io().is_err());
        drop(io);
        assert!(budgets.try_acquire_io().is_ok());
    }

    #[test]
    fn vector_lanes_and_resource_admission_remain_owned_until_drop() {
        let config = VectorMaintenanceRuntimeConfig::default();
        let budgets = VectorMaintenanceBudgets::new(&config, None).unwrap();
        let first = CatalogResourceIdentity::try_table(
            "budget-resource".into(),
            uuid::Uuid::from_u128(1),
            ManagedTableId::try_new("first".into()).unwrap(),
        )
        .unwrap();
        let second = CatalogResourceIdentity::try_table(
            "budget-resource".into(),
            uuid::Uuid::from_u128(1),
            ManagedTableId::try_new("second".into()).unwrap(),
        )
        .unwrap();
        let compute = budgets.try_admit(&first, VectorMaintenanceLane::Compute).unwrap();
        assert!(budgets.try_admit(&first, VectorMaintenanceLane::Control).is_err());
        assert!(budgets.try_admit(&second, VectorMaintenanceLane::Compute).is_err());
        let control = budgets.try_admit(&second, VectorMaintenanceLane::Control).unwrap();
        drop(compute);
        assert!(budgets.try_admit(&first, VectorMaintenanceLane::Compute).is_ok());
        assert!(budgets.try_admit(&second, VectorMaintenanceLane::Compute).is_err());
        control.complete();
        assert!(budgets.try_admit(&second, VectorMaintenanceLane::Compute).is_ok());
        let inactive_node = rustfs_concurrency::adaptive::NodeAdmission::new(std::num::NonZeroU16::MIN);
        let budgets = VectorMaintenanceBudgets::new(&config, Some(inactive_node)).unwrap();
        assert!(budgets.try_admit(&first, VectorMaintenanceLane::Compute).is_err());
    }

    #[test]
    fn vector_retained_batches_enforce_count_and_measured_bytes() {
        let budgets = VectorMaintenanceBudgets::new(&VectorMaintenanceRuntimeConfig::default(), None).unwrap();
        let first = budgets.try_reserve_batch(VECTOR_MAINTENANCE_MAX_BATCH_BYTES).unwrap();
        let retained = first.clone();
        let second = budgets.try_reserve_batch(1).unwrap();
        assert!(budgets.try_reserve_batch(1).is_err());
        drop(first);
        assert!(budgets.try_reserve_batch(1).is_err());
        drop(retained);
        assert!(budgets.try_reserve_batch(1).is_ok());
        assert!(matches!(
            budgets.try_reserve_batch(VECTOR_MAINTENANCE_MAX_BATCH_BYTES + 1),
            Err(VectorMaintenanceBudgetError::Exceeded { .. })
        ));
        drop(second);
    }

    #[tokio::test(start_paused = true)]
    async fn vector_discovery_budget_counts_calls_and_stops() {
        let cancel = tokio_util::sync::CancellationToken::new();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        let budget = VectorMaintenanceDiscoveryBudget::new(cancel.clone(), deadline, std::num::NonZeroUsize::new(2).unwrap());
        assert!(budget.try_start_call());
        assert!(budget.try_start_call());
        assert!(!budget.try_start_call());
        assert_eq!(budget.remaining_calls(), 0);
        let budget = VectorMaintenanceDiscoveryBudget::new(cancel.clone(), deadline, std::num::NonZeroUsize::MIN);
        tokio::time::advance(std::time::Duration::from_secs(30)).await;
        assert!(!budget.try_start_call());
        assert_eq!(budget.remaining_calls(), 1);
        let budget = VectorMaintenanceDiscoveryBudget::new(
            cancel.clone(),
            deadline + std::time::Duration::from_secs(30),
            std::num::NonZeroUsize::MIN,
        );
        cancel.cancel();
        assert!(!budget.try_start_call());
    }

    #[tokio::test(start_paused = true)]
    async fn vector_io_wait_stops_without_releasing_inflight_owner() {
        let config = VectorMaintenanceRuntimeConfig {
            io_concurrency: std::num::NonZeroUsize::MIN,
            ..Default::default()
        };
        let budgets = VectorMaintenanceBudgets::new(&config, None).unwrap();
        let io = budgets.try_acquire_io().unwrap();
        let cancellation = tokio_util::sync::CancellationToken::new();
        let control = VectorMaintenanceControl::new(
            cancellation.clone(),
            tokio::time::Instant::now() + std::time::Duration::from_secs(5),
            budgets.clone(),
        );
        assert!(matches!(control.acquire_io().await, Err(VectorMaintenanceBudgetError::Stopped)));
        assert!(budgets.try_acquire_io().is_err());
        drop(io);
        assert!(budgets.try_acquire_io().is_ok());
    }
}
