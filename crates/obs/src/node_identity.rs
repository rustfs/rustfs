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

pub(crate) const RUSTFS_NODE_ATTRIBUTE: &str = "rustfs.node";
pub(crate) const SERVER_LABEL: &str = "server";

#[cfg(test)]
static LOCAL_NODE_IDENTITY_TEST_LOCK: std::sync::LazyLock<tokio::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

pub(crate) fn local_node_identity(local_ip: &str) -> String {
    rustfs_common::try_get_global_local_node_name().unwrap_or_else(|| local_ip.to_string())
}

pub(crate) fn current_local_node_identity() -> String {
    let local_ip = rustfs_utils::get_local_ip_with_default();
    local_node_identity(&local_ip)
}

#[cfg(test)]
pub(crate) async fn local_node_identity_test_guard() -> tokio::sync::MutexGuard<'static, ()> {
    LOCAL_NODE_IDENTITY_TEST_LOCK.lock().await
}
