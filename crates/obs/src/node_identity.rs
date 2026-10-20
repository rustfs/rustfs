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
    resolve_local_node_identity(rustfs_common::try_get_global_local_node_name, rustfs_utils::get_local_ip_with_default)
}

fn resolve_local_node_identity(node_name: impl Fn() -> Option<String>, local_ip: impl FnOnce() -> String) -> String {
    if let Some(name) = node_name() {
        return name;
    }

    let local_ip = local_ip();
    // Initialization can finish while the address fallback is resolving.
    node_name().unwrap_or(local_ip)
}

#[cfg(test)]
pub(crate) async fn local_node_identity_test_guard() -> tokio::sync::MutexGuard<'static, ()> {
    LOCAL_NODE_IDENTITY_TEST_LOCK.lock().await
}

#[cfg(test)]
mod tests {
    use super::resolve_local_node_identity;
    use std::cell::Cell;

    #[test]
    fn stable_identity_skips_address_resolution() {
        let identity = resolve_local_node_identity(
            || Some("node-a".to_string()),
            || panic!("stable identity must not probe the local address"),
        );

        assert_eq!(identity, "node-a");
    }

    #[test]
    fn uninitialized_identity_resolves_address_once() {
        let calls = Cell::new(0);
        let identity = resolve_local_node_identity(
            || None,
            || {
                calls.set(calls.get() + 1);
                "192.0.2.1".to_string()
            },
        );

        assert_eq!(identity, "192.0.2.1");
        assert_eq!(calls.get(), 1);
    }

    #[test]
    fn initialization_during_address_resolution_keeps_stable_identity() {
        let initialized = Cell::new(false);
        let identity = resolve_local_node_identity(
            || initialized.get().then(|| "node-a".to_string()),
            || {
                initialized.set(true);
                "192.0.2.1".to_string()
            },
        );

        assert_eq!(identity, "node-a");
    }

    #[test]
    fn identity_changes_are_visible_on_the_next_call() {
        let name = Cell::new("node-a");
        let resolve = || {
            resolve_local_node_identity(
                || Some(name.get().to_string()),
                || panic!("initialized identity must not probe the local address"),
            )
        };

        assert_eq!(resolve(), "node-a");
        name.set("node-b");
        assert_eq!(resolve(), "node-b");
    }

    #[test]
    fn missing_identity_is_not_cached_after_initialization() {
        let name = Cell::new(None);
        let fallback_calls = Cell::new(0);
        let resolve = || {
            resolve_local_node_identity(
                || name.get().map(str::to_string),
                || {
                    fallback_calls.set(fallback_calls.get() + 1);
                    "192.0.2.1".to_string()
                },
            )
        };

        assert_eq!(resolve(), "192.0.2.1");
        name.set(Some("node-a"));
        assert_eq!(resolve(), "node-a");
        assert_eq!(fallback_calls.get(), 1);
    }

    #[test]
    fn address_fallback_preserves_ipv6_literal() {
        let identity = resolve_local_node_identity(|| None, || "2001:db8::1".to_string());

        assert_eq!(identity, "2001:db8::1");
    }

    #[test]
    #[should_panic(expected = "address fallback failed")]
    fn address_fallback_panic_is_not_converted_to_an_identity() {
        resolve_local_node_identity(|| None, || panic!("address fallback failed"));
    }

    #[tokio::test]
    async fn concurrent_publication_during_fallback_uses_published_identity() {
        use std::sync::mpsc;
        use std::time::Duration;

        let _guard = super::local_node_identity_test_guard().await;
        let previous = rustfs_common::get_global_local_node_name().await;
        rustfs_common::set_global_local_node_name("").await;
        let (resolving_tx, resolving_rx) = mpsc::channel();
        let (published_tx, published_rx) = mpsc::channel();
        let runtime = tokio::runtime::Handle::current();
        let writer = std::thread::spawn(move || {
            resolving_rx
                .recv_timeout(Duration::from_secs(30))
                .expect("address fallback should announce readiness before publication");
            runtime.block_on(rustfs_common::set_global_local_node_name("published-node"));
            published_tx.send(()).expect("address fallback should wait for publication");
        });

        let result = std::panic::catch_unwind(|| {
            resolve_local_node_identity(rustfs_common::try_get_global_local_node_name, || {
                resolving_tx.send(()).expect("identity publisher should be waiting");
                published_rx
                    .recv_timeout(Duration::from_secs(30))
                    .expect("identity publisher must not be blocked by an address-resolution read guard");
                "192.0.2.1".to_string()
            })
        });
        let writer_result = writer.join();
        rustfs_common::set_global_local_node_name(&previous).await;

        writer_result.expect("identity publisher should complete");
        assert_eq!(result.expect("address fallback should complete"), "published-node");
    }
}

#[cfg(all(test, target_os = "linux", target_arch = "x86_64"))]
#[path = "node_identity/linux_fault_tests.rs"]
mod linux_fault_tests;
