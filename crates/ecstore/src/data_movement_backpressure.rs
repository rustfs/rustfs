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
use crate::runtime_sources::{self, WorkloadSnapshotProviderRef};
use metrics::{counter, histogram};
use rustfs_concurrency::{AdmissionState, WorkloadAdmissionSnapshotProvider, WorkloadClass};
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::debug;

const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_DATA_MOVEMENT: &str = "data_movement";
const EVENT_DATA_MOVEMENT_BACKPRESSURE: &str = "data_movement_backpressure";
const DATA_MOVEMENT_BACKPRESSURE_ENABLE_ENV: &str = "RUSTFS_DATA_MOVEMENT_BACKPRESSURE_ENABLE";
const DATA_MOVEMENT_FOREGROUND_READ_HIGH_PERCENT_ENV: &str = "RUSTFS_DATA_MOVEMENT_FOREGROUND_READ_HIGH_PERCENT";
const DATA_MOVEMENT_FOREGROUND_WRITE_HIGH_PERCENT_ENV: &str = "RUSTFS_DATA_MOVEMENT_FOREGROUND_WRITE_HIGH_PERCENT";
const DATA_MOVEMENT_RECHECK_MS_ENV: &str = "RUSTFS_DATA_MOVEMENT_RECHECK_MS";
const DEFAULT_DATA_MOVEMENT_BACKPRESSURE_ENABLE: bool = true;
const DEFAULT_DATA_MOVEMENT_FOREGROUND_READ_HIGH_PERCENT: usize = 80;
const DEFAULT_DATA_MOVEMENT_FOREGROUND_WRITE_HIGH_PERCENT: usize = 80;
const DEFAULT_DATA_MOVEMENT_RECHECK_MS: u64 = 250;
const MIN_DATA_MOVEMENT_RECHECK_MS: u64 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DataMovementOperation {
    Decommission,
    Rebalance,
}

impl DataMovementOperation {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Decommission => "decommission",
            Self::Rebalance => "rebalance",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DataMovementBackpressureConfig {
    enabled: bool,
    foreground_read_high_percent: usize,
    foreground_write_high_percent: usize,
    recheck_delay: Duration,
}

impl DataMovementBackpressureConfig {
    fn from_env() -> Self {
        Self {
            enabled: rustfs_utils::get_env_bool(DATA_MOVEMENT_BACKPRESSURE_ENABLE_ENV, DEFAULT_DATA_MOVEMENT_BACKPRESSURE_ENABLE),
            foreground_read_high_percent: rustfs_utils::get_env_usize(
                DATA_MOVEMENT_FOREGROUND_READ_HIGH_PERCENT_ENV,
                DEFAULT_DATA_MOVEMENT_FOREGROUND_READ_HIGH_PERCENT,
            ),
            foreground_write_high_percent: rustfs_utils::get_env_usize(
                DATA_MOVEMENT_FOREGROUND_WRITE_HIGH_PERCENT_ENV,
                DEFAULT_DATA_MOVEMENT_FOREGROUND_WRITE_HIGH_PERCENT,
            ),
            recheck_delay: Duration::from_millis(
                rustfs_utils::get_env_u64(DATA_MOVEMENT_RECHECK_MS_ENV, DEFAULT_DATA_MOVEMENT_RECHECK_MS)
                    .max(MIN_DATA_MOVEMENT_RECHECK_MS),
            ),
        }
    }
}

pub(crate) async fn wait_for_data_movement_admission(
    operation: DataMovementOperation,
    pool_index: usize,
    cancel_token: &CancellationToken,
) -> Result<()> {
    wait_for_data_movement_admission_with_provider(
        operation,
        pool_index,
        cancel_token,
        DataMovementBackpressureConfig::from_env(),
        runtime_sources::workload_admission_snapshot_provider(),
    )
    .await
}

async fn wait_for_data_movement_admission_with_provider(
    operation: DataMovementOperation,
    pool_index: usize,
    cancel_token: &CancellationToken,
    config: DataMovementBackpressureConfig,
    provider: Option<WorkloadSnapshotProviderRef>,
) -> Result<()> {
    if cancel_token.is_cancelled() {
        return Err(Error::OperationCanceled);
    }
    if !config.enabled || (config.foreground_read_high_percent == 0 && config.foreground_write_high_percent == 0) {
        return Ok(());
    }
    let Some(provider) = provider else {
        return Ok(());
    };

    let mut delayed_since: Option<(Instant, ForegroundPressure)> = None;
    loop {
        if cancel_token.is_cancelled() {
            record_delay_completion(operation, pool_index, delayed_since, "cancelled");
            return Err(Error::OperationCanceled);
        }

        if let Some(pressure) = foreground_pressure(&config, Some(provider.as_ref())) {
            if delayed_since.is_none() {
                delayed_since = Some((Instant::now(), pressure));
                record_delay_start(operation, pool_index, pressure, &config);
            }

            tokio::select! {
                _ = cancel_token.cancelled() => {
                    record_delay_completion(operation, pool_index, delayed_since, "cancelled");
                    return Err(Error::OperationCanceled);
                }
                _ = sleep(config.recheck_delay) => {}
            }
            continue;
        }

        record_delay_completion(operation, pool_index, delayed_since, "admitted");
        return Ok(());
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ForegroundPressure {
    class: WorkloadClass,
    usage_pct: usize,
    threshold_pct: usize,
}

impl ForegroundPressure {
    const fn reason(self) -> &'static str {
        match self.class {
            WorkloadClass::ForegroundRead => "foreground_read_pressure",
            WorkloadClass::ForegroundWrite => "foreground_write_pressure",
            _ => "foreground_pressure",
        }
    }
}

fn foreground_pressure(
    config: &DataMovementBackpressureConfig,
    provider: Option<&(dyn WorkloadAdmissionSnapshotProvider + Send + Sync)>,
) -> Option<ForegroundPressure> {
    if !config.enabled {
        return None;
    }

    let snapshot = provider?.workload_admission_snapshot();
    [
        (WorkloadClass::ForegroundRead, config.foreground_read_high_percent),
        (WorkloadClass::ForegroundWrite, config.foreground_write_high_percent),
    ]
    .into_iter()
    .filter_map(|(class, threshold_pct)| {
        if threshold_pct == 0 {
            return None;
        }

        let entry = snapshot.get(class)?;
        let usage_pct = if matches!(entry.state, AdmissionState::Saturated) {
            100
        } else {
            let limit = entry.limit?;
            if limit == 0 {
                return None;
            }
            entry
                .active
                .unwrap_or(0)
                .saturating_mul(100)
                .checked_div(limit)
                .unwrap_or(100)
        };

        (usage_pct >= threshold_pct).then_some(ForegroundPressure {
            class,
            usage_pct,
            threshold_pct,
        })
    })
    .max_by_key(|pressure| pressure.usage_pct)
}

fn record_delay_start(
    operation: DataMovementOperation,
    pool_index: usize,
    pressure: ForegroundPressure,
    config: &DataMovementBackpressureConfig,
) {
    counter!(
        "rustfs_data_movement_backpressure_total",
        "operation" => operation.as_str().to_string(),
        "reason" => pressure.reason().to_string(),
        "result" => "delayed".to_string(),
        "pool_index" => pool_index.to_string()
    )
    .increment(1);

    debug!(
        target: "rustfs::ecstore::data_movement",
        event = EVENT_DATA_MOVEMENT_BACKPRESSURE,
        component = LOG_COMPONENT_ECSTORE,
        subsystem = LOG_SUBSYSTEM_DATA_MOVEMENT,
        operation = operation.as_str(),
        pool_index,
        state = "delayed",
        reason = pressure.reason(),
        workload_class = pressure.class.as_str(),
        foreground_usage_pct = pressure.usage_pct,
        threshold_pct = pressure.threshold_pct,
        recheck_delay_ms = config.recheck_delay.as_millis(),
        "Data movement delayed under foreground pressure"
    );
}

fn record_delay_completion(
    operation: DataMovementOperation,
    pool_index: usize,
    delayed_since: Option<(Instant, ForegroundPressure)>,
    result: &'static str,
) {
    let Some((delayed_since, pressure)) = delayed_since else {
        return;
    };

    let delay = delayed_since.elapsed();
    counter!(
        "rustfs_data_movement_backpressure_total",
        "operation" => operation.as_str().to_string(),
        "reason" => pressure.reason().to_string(),
        "result" => result.to_string(),
        "pool_index" => pool_index.to_string()
    )
    .increment(1);
    histogram!(
        "rustfs_data_movement_backpressure_delay_seconds",
        "operation" => operation.as_str().to_string(),
        "reason" => pressure.reason().to_string(),
        "result" => result.to_string(),
        "pool_index" => pool_index.to_string()
    )
    .record(delay.as_secs_f64());

    debug!(
        target: "rustfs::ecstore::data_movement",
        event = EVENT_DATA_MOVEMENT_BACKPRESSURE,
        component = LOG_COMPONENT_ECSTORE,
        subsystem = LOG_SUBSYSTEM_DATA_MOVEMENT,
        operation = operation.as_str(),
        pool_index,
        state = result,
        reason = pressure.reason(),
        workload_class = pressure.class.as_str(),
        delay_secs = delay.as_secs_f64(),
        "Data movement backpressure wait completed"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_concurrency::{WorkloadAdmissionRegistrySnapshot, WorkloadAdmissionSnapshot};
    use std::sync::Arc;

    #[derive(Debug)]
    struct StaticWorkloadProvider {
        snapshot: WorkloadAdmissionRegistrySnapshot,
    }

    impl StaticWorkloadProvider {
        fn new(snapshot: WorkloadAdmissionSnapshot) -> Self {
            Self {
                snapshot: WorkloadAdmissionRegistrySnapshot::new(vec![snapshot]),
            }
        }
    }

    impl WorkloadAdmissionSnapshotProvider for StaticWorkloadProvider {
        fn workload_admission_snapshot(&self) -> WorkloadAdmissionRegistrySnapshot {
            self.snapshot.clone()
        }
    }

    fn test_config() -> DataMovementBackpressureConfig {
        DataMovementBackpressureConfig {
            enabled: true,
            foreground_read_high_percent: 80,
            foreground_write_high_percent: 80,
            recheck_delay: Duration::from_millis(1),
        }
    }

    #[test]
    fn foreground_read_pressure_treats_saturated_as_full() {
        let provider =
            StaticWorkloadProvider::new(WorkloadAdmissionSnapshot::new(WorkloadClass::ForegroundRead, AdmissionState::Saturated));

        assert_eq!(
            foreground_pressure(&test_config(), Some(&provider)),
            Some(ForegroundPressure {
                class: WorkloadClass::ForegroundRead,
                usage_pct: 100,
                threshold_pct: 80,
            })
        );
    }

    #[test]
    fn foreground_read_pressure_uses_active_limit_threshold() {
        let provider = StaticWorkloadProvider::new(
            WorkloadAdmissionSnapshot::new(WorkloadClass::ForegroundRead, AdmissionState::Open).with_counts(
                Some(8),
                None,
                Some(10),
            ),
        );

        assert_eq!(
            foreground_pressure(&test_config(), Some(&provider)),
            Some(ForegroundPressure {
                class: WorkloadClass::ForegroundRead,
                usage_pct: 80,
                threshold_pct: 80,
            })
        );
    }

    #[test]
    fn foreground_write_pressure_uses_active_limit_threshold() {
        let provider = StaticWorkloadProvider::new(
            WorkloadAdmissionSnapshot::new(WorkloadClass::ForegroundWrite, AdmissionState::Open).with_counts(
                Some(9),
                None,
                Some(10),
            ),
        );

        assert_eq!(
            foreground_pressure(&test_config(), Some(&provider)),
            Some(ForegroundPressure {
                class: WorkloadClass::ForegroundWrite,
                usage_pct: 90,
                threshold_pct: 80,
            })
        );
    }

    #[test]
    fn foreground_pressure_ignores_open_low_usage() {
        let provider = StaticWorkloadProvider::new(
            WorkloadAdmissionSnapshot::new(WorkloadClass::ForegroundRead, AdmissionState::Open).with_counts(
                Some(7),
                None,
                Some(10),
            ),
        );

        assert_eq!(foreground_pressure(&test_config(), Some(&provider)), None);
    }

    #[tokio::test]
    async fn wait_for_data_movement_admission_returns_when_open() {
        let provider: WorkloadSnapshotProviderRef = Arc::new(StaticWorkloadProvider::new(
            WorkloadAdmissionSnapshot::new(WorkloadClass::ForegroundRead, AdmissionState::Open).with_counts(
                Some(0),
                None,
                Some(10),
            ),
        ));
        let cancel_token = CancellationToken::new();

        let result = wait_for_data_movement_admission_with_provider(
            DataMovementOperation::Rebalance,
            0,
            &cancel_token,
            test_config(),
            Some(provider),
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn wait_for_data_movement_admission_cancels_under_pressure() {
        let provider: WorkloadSnapshotProviderRef = Arc::new(StaticWorkloadProvider::new(WorkloadAdmissionSnapshot::new(
            WorkloadClass::ForegroundRead,
            AdmissionState::Saturated,
        )));
        let cancel_token = CancellationToken::new();
        let waiter_token = cancel_token.clone();

        let waiter = tokio::spawn(async move {
            wait_for_data_movement_admission_with_provider(
                DataMovementOperation::Decommission,
                1,
                &waiter_token,
                test_config(),
                Some(provider),
            )
            .await
        });

        tokio::task::yield_now().await;
        cancel_token.cancel();

        let err = waiter
            .await
            .expect("waiter task should join")
            .expect_err("cancelled admission wait should return operation-canceled");
        assert!(matches!(err, Error::OperationCanceled));
    }
}
