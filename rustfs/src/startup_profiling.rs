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

pub async fn init_profiling_runtime() {
    init_profiling_runtime_with(crate::profiling::init_from_env).await;
}

async fn init_profiling_runtime_with<InitFn, InitFuture>(init: InitFn)
where
    InitFn: FnOnce() -> InitFuture,
    InitFuture: Future<Output = ()>,
{
    init().await;
}

pub fn shutdown_profiling_runtime() {
    shutdown_profiling_runtime_with(crate::profiling::shutdown_profiling);
}

fn shutdown_profiling_runtime_with<ShutdownFn>(shutdown: ShutdownFn)
where
    ShutdownFn: FnOnce(),
{
    shutdown();
}

#[cfg(test)]
mod tests {
    use super::{init_profiling_runtime_with, shutdown_profiling_runtime_with};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    #[tokio::test]
    async fn init_profiling_runtime_invokes_registered_hook() {
        let called = Arc::new(AtomicBool::new(false));
        let hook_called = called.clone();

        init_profiling_runtime_with(move || async move {
            hook_called.store(true, Ordering::SeqCst);
        })
        .await;

        assert!(called.load(Ordering::SeqCst));
    }

    #[test]
    fn shutdown_profiling_runtime_invokes_registered_hook() {
        let called = AtomicBool::new(false);

        shutdown_profiling_runtime_with(|| {
            called.store(true, Ordering::SeqCst);
        });

        assert!(called.load(Ordering::SeqCst));
    }
}
