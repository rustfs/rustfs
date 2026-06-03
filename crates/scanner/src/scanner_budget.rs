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

use std::sync::{
    Arc,
    atomic::{AtomicU8, AtomicU64, Ordering},
};

use tokio::time::Duration;
use tokio_util::sync::CancellationToken;

const BUDGET_REASON_NONE: u8 = 0;
const BUDGET_REASON_RUNTIME: u8 = 1;
const BUDGET_REASON_OBJECTS: u8 = 2;
const BUDGET_REASON_DIRECTORIES: u8 = 3;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ScannerCycleBudgetConfig {
    pub max_duration: Option<Duration>,
    pub max_objects: Option<u64>,
    pub max_directories: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ScannerCycleBudgetReason {
    Runtime,
    Objects,
    Directories,
}

impl ScannerCycleBudgetReason {
    fn code(self) -> u8 {
        match self {
            Self::Runtime => BUDGET_REASON_RUNTIME,
            Self::Objects => BUDGET_REASON_OBJECTS,
            Self::Directories => BUDGET_REASON_DIRECTORIES,
        }
    }

    fn from_code(code: u8) -> Option<Self> {
        match code {
            BUDGET_REASON_RUNTIME => Some(Self::Runtime),
            BUDGET_REASON_OBJECTS => Some(Self::Objects),
            BUDGET_REASON_DIRECTORIES => Some(Self::Directories),
            _ => None,
        }
    }
}

pub struct ScannerCycleBudget {
    token: CancellationToken,
    reason: Arc<AtomicU8>,
    max_duration: Option<Duration>,
    max_objects: Option<u64>,
    max_directories: Option<u64>,
    objects_scanned: AtomicU64,
    directories_started: AtomicU64,
}

impl ScannerCycleBudget {
    pub(crate) fn new(parent: &CancellationToken, config: ScannerCycleBudgetConfig) -> Arc<Self> {
        let token = parent.child_token();
        let reason = Arc::new(AtomicU8::new(BUDGET_REASON_NONE));

        if let Some(duration) = config.max_duration {
            let parent = parent.clone();
            let token_wait = token.clone();
            let token_cancel = token.clone();
            let reason = reason.clone();
            tokio::spawn(async move {
                tokio::select! {
                    _ = parent.cancelled() => {}
                    _ = token_wait.cancelled() => {}
                    _ = tokio::time::sleep(duration) => {
                        Self::cancel_for_reason(&reason, &token_cancel, ScannerCycleBudgetReason::Runtime);
                    }
                }
            });
        }

        Arc::new(Self {
            token,
            reason,
            max_duration: config.max_duration,
            max_objects: config.max_objects,
            max_directories: config.max_directories,
            objects_scanned: AtomicU64::new(0),
            directories_started: AtomicU64::new(0),
        })
    }

    pub(crate) fn token(&self) -> CancellationToken {
        self.token.clone()
    }

    pub(crate) fn budget_elapsed(&self) -> bool {
        self.reason.load(Ordering::Relaxed) != BUDGET_REASON_NONE
    }

    pub(crate) fn reason(&self) -> Option<ScannerCycleBudgetReason> {
        ScannerCycleBudgetReason::from_code(self.reason.load(Ordering::Relaxed))
    }

    pub(crate) fn max_duration(&self) -> Option<Duration> {
        self.max_duration
    }

    pub(crate) fn max_objects(&self) -> Option<u64> {
        self.max_objects
    }

    pub(crate) fn max_directories(&self) -> Option<u64> {
        self.max_directories
    }

    pub(crate) fn try_start_directory(&self) -> bool {
        let Some(max_directories) = self.max_directories else {
            return true;
        };

        let directories = self.directories_started.fetch_add(1, Ordering::Relaxed) + 1;
        if directories <= max_directories {
            return true;
        }

        self.cancel_for(ScannerCycleBudgetReason::Directories);
        false
    }

    pub(crate) fn record_object_scanned(&self) {
        let Some(max_objects) = self.max_objects else {
            return;
        };

        let objects = self.objects_scanned.fetch_add(1, Ordering::Relaxed) + 1;
        if objects >= max_objects {
            self.cancel_for(ScannerCycleBudgetReason::Objects);
        }
    }

    fn cancel_for(&self, reason: ScannerCycleBudgetReason) {
        Self::cancel_for_reason(&self.reason, &self.token, reason);
    }

    fn cancel_for_reason(reason: &AtomicU8, token: &CancellationToken, budget_reason: ScannerCycleBudgetReason) {
        if reason
            .compare_exchange(BUDGET_REASON_NONE, budget_reason.code(), Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            token.cancel();
        }
    }
}

impl Drop for ScannerCycleBudget {
    fn drop(&mut self) {
        self.token.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn runtime_budget_cancels_child_token() {
        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new(
            &parent,
            ScannerCycleBudgetConfig {
                max_duration: Some(Duration::from_millis(1)),
                ..Default::default()
            },
        );

        tokio::time::sleep(Duration::from_millis(10)).await;

        assert!(budget.budget_elapsed());
        assert_eq!(budget.reason(), Some(ScannerCycleBudgetReason::Runtime));
        assert!(budget.token().is_cancelled());
    }

    #[test]
    fn object_budget_cancels_after_reaching_limit() {
        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new(
            &parent,
            ScannerCycleBudgetConfig {
                max_objects: Some(2),
                ..Default::default()
            },
        );

        budget.record_object_scanned();
        assert!(!budget.budget_elapsed());

        budget.record_object_scanned();
        assert!(budget.budget_elapsed());
        assert_eq!(budget.reason(), Some(ScannerCycleBudgetReason::Objects));
        assert!(budget.token().is_cancelled());
    }

    #[test]
    fn directory_budget_rejects_directory_after_limit() {
        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new(
            &parent,
            ScannerCycleBudgetConfig {
                max_directories: Some(1),
                ..Default::default()
            },
        );

        assert!(budget.try_start_directory());
        assert!(!budget.budget_elapsed());
        assert!(!budget.try_start_directory());
        assert_eq!(budget.reason(), Some(ScannerCycleBudgetReason::Directories));
        assert!(budget.token().is_cancelled());
    }
}
