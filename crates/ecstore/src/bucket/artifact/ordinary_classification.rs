// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

/// Shares only reads started after a caller arrives; retains no idle results.
#[derive(Default)]
pub(crate) struct OrdinaryClassificationRegistry {
    gates: Mutex<HashMap<String, Weak<Gate>>>,
    #[cfg(test)]
    after_unregister: Option<std::sync::Barrier>,
}

#[derive(Default)]
struct Gate {
    started_generation: AtomicU64,
    completed: tokio::sync::Mutex<Option<CompletedRead>>,
}

struct CompletedRead {
    generation: u64,
    allowed: bool,
}

struct Registration<'a> {
    registry: &'a OrdinaryClassificationRegistry,
    bucket: &'a str,
    gate: Option<Arc<Gate>>,
    observed_generation: u64,
}

impl OrdinaryClassificationRegistry {
    pub(crate) async fn classify(&self, bucket: &str, read: impl Future<Output = bool>) -> bool {
        self.register(bucket).classify(read).await
    }

    fn register<'a>(&'a self, bucket: &'a str) -> Registration<'a> {
        // Callers requiring physical admission must register after acquiring it.
        // Release the registry lock before awaiting the gate, then acquire the
        // existing metadata locks.
        let mut gates = self.gates.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let gate = gates.get(bucket).and_then(Weak::upgrade).unwrap_or_else(|| {
            let gate = Arc::new(Gate::default());
            gates.insert(bucket.to_owned(), Arc::downgrade(&gate));
            gate
        });
        let observed_generation = gate.started_generation.load(Ordering::Acquire);
        Registration {
            registry: self,
            bucket,
            gate: Some(gate),
            observed_generation,
        }
    }
}

impl Registration<'_> {
    async fn classify(self, read: impl Future<Output = bool>) -> bool {
        let Some(gate) = self.gate.as_ref() else {
            return false;
        };
        let mut completed = gate.completed.lock().await;
        // Only a read started after this caller registered can authorize it.
        // Arrivals during a read therefore wait for the next fresh read.
        if let Some(result) = completed
            .as_ref()
            .filter(|result| result.generation > self.observed_generation)
        {
            return result.allowed;
        }
        let Ok(previous) = gate
            .started_generation
            .try_update(Ordering::AcqRel, Ordering::Acquire, |generation| generation.checked_add(1))
        else {
            return false;
        };
        // Keep the gate through the original bounded metadata read. Cancellation
        // releases it without publishing a result; another caller can read again.
        let allowed = read.await;
        *completed = Some(CompletedRead {
            generation: previous + 1,
            allowed,
        });
        allowed
    }
}

impl Drop for Registration<'_> {
    fn drop(&mut self) {
        let Some(gate) = self.gate.take() else {
            return;
        };
        let mut gates = self.registry.gates.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        // Strong references are acquired and released under this registry lock,
        // so concurrent final callers cannot both miss the last reference.
        if Arc::strong_count(&gate) == 1
            && gates
                .get(self.bucket)
                .is_some_and(|registered| registered.ptr_eq(&Arc::downgrade(&gate)))
        {
            gates.remove(self.bucket);
        }
        drop(gate);
        drop(gates);
        #[cfg(test)]
        if let Some(barrier) = &self.registry.after_unregister {
            barrier.wait();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::poll;
    use std::task::Poll;
    use tokio::sync::oneshot;

    fn assert_empty(registry: &OrdinaryClassificationRegistry) {
        assert!(registry.gates.lock().expect("classification registry").is_empty());
    }

    #[tokio::test]
    async fn late_arrivals_share_only_the_next_fail_closed_read() {
        let registry = OrdinaryClassificationRegistry::default();
        let reads = AtomicU64::new(0);
        let (release, waiting) = oneshot::channel();
        let mut first = Box::pin(registry.classify("bucket", async {
            reads.fetch_add(1, Ordering::Relaxed);
            waiting.await.expect("release first read");
            true
        }));
        assert!(poll!(first.as_mut()).is_pending());
        let mut followers = (0..16)
            .map(|_| {
                Box::pin(registry.classify("bucket", async {
                    reads.fetch_add(1, Ordering::Relaxed);
                    false
                }))
            })
            .collect::<Vec<_>>();
        for follower in &mut followers {
            assert!(poll!(follower.as_mut()).is_pending());
        }
        release.send(()).expect("first reader still alive");
        assert_eq!(poll!(first.as_mut()), Poll::Ready(true));
        for follower in &mut followers {
            assert_eq!(poll!(follower.as_mut()), Poll::Ready(false));
        }
        assert_eq!(reads.load(Ordering::Relaxed), 2);
        assert_empty(&registry);
    }

    #[tokio::test]
    async fn successive_waves_always_read_again() {
        let registry = OrdinaryClassificationRegistry::default();
        let reads = AtomicU64::new(0);
        for expected in [true, false, true] {
            assert_eq!(
                registry
                    .classify("bucket", async {
                        reads.fetch_add(1, Ordering::Relaxed);
                        expected
                    })
                    .await,
                expected
            );
            assert_empty(&registry);
        }
        assert_eq!(reads.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn canceled_leader_does_not_publish_or_strand_waiters() {
        let registry = OrdinaryClassificationRegistry::default();
        let reads = AtomicU64::new(0);
        let old_registration = registry.register("bucket");
        assert!(
            registry
                .classify("bucket", async {
                    reads.fetch_add(1, Ordering::Relaxed);
                    true
                })
                .await
        );
        let mut leader = Box::pin(registry.classify("bucket", async {
            reads.fetch_add(1, Ordering::Relaxed);
            std::future::pending::<bool>().await
        }));
        assert!(poll!(leader.as_mut()).is_pending());
        let mut old_waiter =
            Box::pin(old_registration.classify(async { panic!("old waiter must reuse the completed post-registration read") }));
        assert!(poll!(old_waiter.as_mut()).is_pending());
        let mut late_waiter = Box::pin(registry.classify("bucket", async {
            reads.fetch_add(1, Ordering::Relaxed);
            false
        }));
        assert!(poll!(late_waiter.as_mut()).is_pending());
        drop(leader);
        assert_eq!(poll!(old_waiter.as_mut()), Poll::Ready(true));
        assert_eq!(poll!(late_waiter.as_mut()), Poll::Ready(false));
        assert_eq!(reads.load(Ordering::Relaxed), 3);
        assert_empty(&registry);
    }

    #[tokio::test]
    async fn canceled_follower_does_not_retain_the_gate() {
        let registry = OrdinaryClassificationRegistry::default();
        let (release, waiting) = oneshot::channel();
        let mut leader = Box::pin(registry.classify("bucket", async {
            waiting.await.expect("release leader");
            true
        }));
        assert!(poll!(leader.as_mut()).is_pending());
        let mut follower = Box::pin(registry.classify("bucket", async { panic!("canceled follower must not read") }));
        assert!(poll!(follower.as_mut()).is_pending());
        drop(follower);
        assert_eq!(registry.gates.lock().expect("classification registry").len(), 1);
        release.send(()).expect("leader still alive");
        assert_eq!(poll!(leader.as_mut()), Poll::Ready(true));
        assert_empty(&registry);
    }

    #[test]
    fn concurrent_final_registrations_remove_the_gate() {
        let registry = OrdinaryClassificationRegistry {
            after_unregister: Some(std::sync::Barrier::new(2)),
            ..Default::default()
        };
        let first = registry.register("bucket");
        let second = registry.register("bucket");
        // Both destructors reach the unlocked barrier before either returns.
        // Moving the Arc release after this barrier leaves a stale registry key.
        std::thread::scope(|scope| {
            scope.spawn(move || drop(first));
            scope.spawn(move || drop(second));
        });
        assert_empty(&registry);
    }

    #[tokio::test]
    async fn buckets_and_stores_do_not_share_gates() {
        let first_store = OrdinaryClassificationRegistry::default();
        let second_store = OrdinaryClassificationRegistry::default();
        let mut blocked = Box::pin(first_store.classify("first", std::future::pending()));
        assert!(poll!(blocked.as_mut()).is_pending());
        assert_eq!(poll!(Box::pin(first_store.classify("second", async { true }))), Poll::Ready(true));
        assert_eq!(poll!(Box::pin(second_store.classify("first", async { false }))), Poll::Ready(false));
        assert_empty(&second_store);
        drop(blocked);
        assert_empty(&first_store);
    }

    #[tokio::test]
    async fn generation_exhaustion_fails_closed_without_reading() {
        let registry = OrdinaryClassificationRegistry::default();
        let registration = registry.register("bucket");
        let gate = registration.gate.as_ref().expect("registered gate");
        gate.started_generation.store(u64::MAX, Ordering::Release);
        *gate.completed.lock().await = Some(CompletedRead {
            generation: u64::MAX,
            allowed: true,
        });
        assert!(
            !registry
                .classify("bucket", async { panic!("exhausted generation must deny without reading") })
                .await
        );
        assert_eq!(gate.started_generation.load(Ordering::Acquire), u64::MAX);
        drop(registration);
        assert_empty(&registry);
    }
}
