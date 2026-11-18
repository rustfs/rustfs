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

use std::future::Future;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Retains a catalog worker until its in-flight publications have drained.
pub(crate) struct CatalogBackgroundTask {
    pub(in crate::table_catalog) cancellation: CancellationToken,
    pub(in crate::table_catalog) task: Option<JoinHandle<()>>,
}

impl CatalogBackgroundTask {
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
}

impl Drop for CatalogBackgroundTask {
    fn drop(&mut self) {
        // Detach, never abort: in-flight writes retain their publication owner.
        self.cancellation.cancel();
    }
}
