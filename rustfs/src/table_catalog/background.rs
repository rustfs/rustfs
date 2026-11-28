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

use std::{future::Future, sync::Arc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Retains a catalog worker until its in-flight publications have drained.
pub(crate) struct CatalogBackgroundTask {
    pub(in crate::table_catalog) cancellation: CancellationToken,
    pub(in crate::table_catalog) task: Option<JoinHandle<()>>,
    failure: Arc<parking_lot::Mutex<Option<Box<dyn std::error::Error + Send + Sync>>>>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CatalogBackgroundTaskError {
    #[error("catalog worker join failed: {0}")]
    Join(#[source] tokio::task::JoinError),
    #[error("catalog worker failed: {0}")]
    Worker(#[source] Box<dyn std::error::Error + Send + Sync>),
}

impl CatalogBackgroundTask {
    #[cfg(test)]
    pub(in crate::table_catalog) fn from_test_task(cancellation: CancellationToken, task: Option<JoinHandle<()>>) -> Self {
        Self {
            cancellation,
            task,
            failure: Arc::default(),
        }
    }

    pub(crate) fn start<F, W>(shutdown: &CancellationToken, worker: W) -> Self
    where
        F: Future<Output = ()> + Send + 'static,
        W: FnOnce(CancellationToken) -> F,
    {
        let cancellation = shutdown.child_token();
        let task = tokio::spawn(worker(cancellation.clone()));
        Self {
            cancellation,
            task: Some(task),
            failure: Arc::default(),
        }
    }

    /// Retain a typed worker failure until the actual task is joined.
    pub(crate) fn start_fallible<F, W, E>(shutdown: &CancellationToken, worker: W) -> Self
    where
        F: Future<Output = Result<(), E>> + Send + 'static,
        W: FnOnce(CancellationToken) -> F,
        E: std::error::Error + Send + Sync + 'static,
    {
        let cancellation = shutdown.child_token();
        let failure = Arc::new(parking_lot::Mutex::new(None));
        let retained = failure.clone();
        let future = worker(cancellation.clone());
        let task = tokio::spawn(async move {
            if let Err(error) = future.await {
                *retained.lock() = Some(Box::new(error) as Box<dyn std::error::Error + Send + Sync>);
            }
        });
        Self {
            cancellation,
            task: Some(task),
            failure,
        }
    }

    pub(crate) fn request_stop(&self) {
        self.cancellation.cancel();
    }

    pub(crate) async fn join(&mut self) -> Option<Result<(), tokio::task::JoinError>> {
        // A cancelled waiter leaves the actual task in this owner.
        let result = self.task.as_mut()?.await;
        self.task = None;
        Some(result)
    }

    pub(crate) async fn join_result(&mut self) -> Option<Result<(), CatalogBackgroundTaskError>> {
        match self.join().await? {
            Err(error) => Some(Err(CatalogBackgroundTaskError::Join(error))),
            Ok(()) => Some(match self.failure.lock().take() {
                Some(error) => Err(CatalogBackgroundTaskError::Worker(error)),
                None => Ok(()),
            }),
        }
    }
}

impl Drop for CatalogBackgroundTask {
    fn drop(&mut self) {
        // Detach, never abort: in-flight writes retain their publication owner.
        self.cancellation.cancel();
    }
}
