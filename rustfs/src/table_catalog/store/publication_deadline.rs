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

use super::*;
use std::{
    future::Future,
    sync::{
        OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    task::Poll,
};

/// One admission budget, retained across repeated preparation and completed recovery.
/// It never cancels a transaction after its first possible durable side effect.
#[derive(Default)]
pub(crate) struct TablePublicationPreparation {
    deadline: OnceLock<tokio::time::Instant>,
    mutation_started: AtomicBool,
    fence: parking_lot::Mutex<Option<TableCatalogObjectMutationFence>>,
}

tokio::task_local! {
    static PUBLICATION_PREPARATION: Arc<TablePublicationPreparation>;
}

/// Budget for one publication preparation: the configured interval, never
/// shorter than the lock-acquire timeout that queueing alone may consume and
/// never longer than the ceiling, so `Instant + Duration` cannot overflow.
pub(crate) fn publication_preparation_timeout() -> std::time::Duration {
    let configured = rustfs_utils::get_env_u64(
        crate::table_catalog::ENV_TABLE_CATALOG_PUBLICATION_PREPARATION_TIMEOUT_SECS,
        crate::table_catalog::DEFAULT_TABLE_CATALOG_PUBLICATION_PREPARATION_TIMEOUT_SECS,
    )
    .min(crate::table_catalog::MAX_TABLE_CATALOG_PUBLICATION_PREPARATION_TIMEOUT_SECS);
    std::time::Duration::from_secs(configured).max(get_lock_acquire_timeout())
}

impl TablePublicationPreparation {
    pub(super) fn deadline(&self) -> tokio::time::Instant {
        *self
            .deadline
            .get_or_init(|| tokio::time::Instant::now() + publication_preparation_timeout())
    }

    fn expired() -> TableCatalogStoreError {
        TableCatalogStoreError::Unavailable("table publication preparation deadline exceeded; retry the operation".to_string())
    }

    fn check(&self) -> TableCatalogStoreResult<()> {
        if !self.mutation_started.load(Ordering::Acquire) && tokio::time::Instant::now() >= self.deadline() {
            return Err(Self::expired());
        }
        Ok(())
    }

    /// Fail once the budget is spent without committing the transaction.
    pub(crate) fn check_deadline(&self) -> TableCatalogStoreResult<()> {
        self.check()
    }

    /// Whether a durable catalog write may already have begun.
    pub(crate) fn mutation_started(&self) -> bool {
        self.mutation_started.load(Ordering::Acquire)
    }

    pub(crate) fn begin_mutation(&self) -> TableCatalogStoreResult<()> {
        self.check()?;
        if let Some(fence) = self.fence.lock().as_ref() {
            fence.ensure_held()?;
        }
        self.mutation_started.store(true, Ordering::Release);
        Ok(())
    }

    // Only use for pure acquisition/validation. The publication is cleared on
    // timeout or caller cancellation, including partially acquired guards.
    pub(crate) async fn read<T>(
        &self,
        publication: &(dyn TableCommitPublication + Sync),
        future: impl Future<Output = TableCatalogStoreResult<T>>,
    ) -> TableCatalogStoreResult<T> {
        let mut cleanup = PreparationReadCleanup {
            publication,
            armed: true,
        };
        let result = if self.mutation_started.load(Ordering::Acquire) {
            future.await
        } else {
            tokio::time::timeout_at(self.deadline(), future)
                .await
                .map_err(|_| Self::expired())?
        };
        if result.is_ok() {
            self.check()?;
        }
        cleanup.armed = false;
        result
    }

    async fn run<T>(&self, future: impl Future<Output = TableCatalogStoreResult<T>>) -> TableCatalogStoreResult<T> {
        let timer = tokio::time::sleep_until(self.deadline());
        tokio::pin!(timer, future);
        std::future::poll_fn(|cx| {
            // Check before polling new work. A mutation boundary also checks
            // synchronously, so CPU work cannot publish after an expired timer.
            if !self.mutation_started.load(Ordering::Acquire) && timer.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Err(Self::expired()));
            }
            match future.as_mut().poll(cx) {
                Poll::Ready(result) => Poll::Ready(self.check().and(result)),
                Poll::Pending => Poll::Pending,
            }
        })
        .await
    }
}

struct PreparationReadCleanup<'a> {
    publication: &'a (dyn TableCommitPublication + Sync),
    armed: bool,
}

impl Drop for PreparationReadCleanup<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.publication.complete();
        }
    }
}

pub(super) async fn publication_preparation<T>(
    publication: &(dyn TableCommitPublication + Sync),
    future: impl Future<Output = TableCatalogStoreResult<T>>,
) -> TableCatalogStoreResult<T> {
    let _completion = TableCommitPublicationCompletion::new(publication);
    let preparation = publication.preparation_budget();
    PUBLICATION_PREPARATION
        .scope(preparation.clone(), preparation.run(future))
        .await
}

pub(super) fn retain_publication_preparation_fence(publication: &(dyn TableCommitPublication + Sync)) {
    let _ = PUBLICATION_PREPARATION.try_with(|preparation| {
        *preparation.fence.lock() = publication.object_mutation_fence();
    });
}

/// Call immediately before the first possible persistent side effect, after
/// read-only validation and existing lease checks. A failed/ambiguous write is
/// still a mutation: recovery must retain the original cancellation semantics.
pub(super) fn begin_publication_mutation() -> TableCatalogStoreResult<()> {
    PUBLICATION_PREPARATION
        .try_with(|preparation| preparation.begin_mutation())
        .unwrap_or(Ok(()))
}

/// Recovery is a preceding transaction, not permission for the new transaction
/// to bypass admission. Complete its original protocol once a write starts,
/// then resume the unchanged deadline before any new preparation.
pub(super) async fn publication_recovery<T>(
    future: impl Future<Output = TableCatalogStoreResult<T>>,
) -> TableCatalogStoreResult<T> {
    let preparation = PUBLICATION_PREPARATION.try_with(Arc::clone).ok();
    let already_mutating = preparation
        .as_ref()
        .is_some_and(|budget| budget.mutation_started.load(Ordering::Acquire));
    let result = future.await;
    if result.is_ok()
        && !already_mutating
        && let Some(preparation) = preparation
    {
        preparation.mutation_started.store(false, Ordering::Release);
        preparation.check()?;
    }
    result
}

#[cfg(test)]
#[path = "publication_deadline_tests.rs"]
mod tests;
