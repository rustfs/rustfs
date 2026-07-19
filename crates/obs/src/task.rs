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

//! Task helpers for preserving tracing context across Tokio task boundaries.

use tracing::Instrument;

/// Spawn a Tokio task that inherits the current tracing span.
///
/// Tokio does not propagate [`tracing::Span::current`] into spawned tasks.
/// Use this for work that remains part of the caller's operation. Detached
/// background side effects should start a new span instead.
pub fn spawn_traced<F>(future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(future.instrument(tracing::Span::current()))
}

#[cfg(test)]
mod tests {
    use super::spawn_traced;

    #[tokio::test]
    async fn spawned_task_inherits_current_span() {
        tracing::subscriber::set_global_default(tracing_subscriber::Registry::default())
            .expect("task tracing test should install its subscriber");

        let (parent_id, task) = {
            let parent = tracing::info_span!("parent");
            let parent_id = parent.id().expect("enabled parent span should have an id");
            let task = parent
                .in_scope(|| spawn_traced(async { tracing::Span::current().id().expect("task should retain its parent span") }));
            (parent_id, task)
        };

        let task_span_id = task.await.expect("task should complete");
        assert_eq!(task_span_id, parent_id);
    }
}
