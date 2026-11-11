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

//! Engine registry: implementations by name and the routing state of every
//! bucket bound to an engine.
//!
//! Publications update the desired binding synchronously and spawn a
//! reconcile task. Reconciles of one bucket run one at a time under the
//! bucket's lifecycle lock and always drive the attached engine toward the
//! latest desired binding, so out-of-order task completion cannot leave a
//! stale engine attached. A route only changes to ready or failed when no
//! newer publication arrived while the reconcile ran.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use parking_lot::RwLock;
use rustfs_bucket_engine::{
    BucketContext, BucketEngine, BucketEngineBinding, EngineError, EngineName, NodeEnvironment, OBJECT_ENGINE,
};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

const EVENT_BUCKET_ENGINE_BINDING_APPLIED: &str = "bucket_engine_binding_applied";
const LOG_COMPONENT_APP: &str = "app";
const LOG_SUBSYSTEM_BUCKET_ENGINE: &str = "bucket_engine";

const REASON_ATTACHING: &str = "bucket engine is attaching";
const REASON_SHUTTING_DOWN: &str = "server is shutting down";

/// A bucket attached to its engine.
pub struct BoundBucket {
    pub context: BucketContext,
    pub engine: Arc<dyn BucketEngine>,
}

/// Where requests for a bucket go.
#[derive(Clone)]
pub enum EngineRoute {
    /// The object layer: no binding, or the `object` engine.
    Object,
    Engine(Arc<BoundBucket>),
    /// Bound to an engine this node cannot serve right now. Retryable while
    /// attaching; otherwise it needs an operator or a new binding.
    Unavailable {
        engine: String,
        reason: String,
    },
}

impl EngineRoute {
    pub fn is_object(&self) -> bool {
        matches!(self, Self::Object)
    }
}

impl std::fmt::Debug for EngineRoute {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Object => f.write_str("Object"),
            Self::Engine(bound) => f
                .debug_struct("Engine")
                .field("engine", &bound.context.binding.engine.as_str())
                .field("incarnation", &bound.context.incarnation)
                .finish(),
            Self::Unavailable { engine, reason } => f
                .debug_struct("Unavailable")
                .field("engine", engine)
                .field("reason", reason)
                .finish(),
        }
    }
}

#[derive(Clone)]
enum Desired {
    Object,
    Bind {
        engine: Arc<dyn BucketEngine>,
        context: BucketContext,
    },
    Refuse {
        engine: String,
        reason: String,
    },
}

impl Desired {
    fn same_as(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Object, Self::Object) => true,
            (Self::Bind { context: a, .. }, Self::Bind { context: b, .. }) => a == b,
            (Self::Refuse { engine: a, reason: ra }, Self::Refuse { engine: b, reason: rb }) => a == b && ra == rb,
            _ => false,
        }
    }
}

type Attached = Arc<Mutex<Option<Arc<BoundBucket>>>>;

struct BucketSlot {
    /// Engine currently attached for the bucket; held across reconciles.
    attached: Attached,
    generation: u64,
    desired: Desired,
    route: EngineRoute,
    /// The last reconcile of `desired` failed; the next publication retries.
    failed: bool,
}

/// Engine implementations by name and the engine route of each bucket.
#[derive(Default)]
pub struct EngineRegistry {
    engines: RwLock<HashMap<EngineName, Arc<dyn BucketEngine>>>,
    buckets: RwLock<HashMap<String, BucketSlot>>,
    /// This node's drives and placement view; absent when the process has no
    /// local drives (then no engine directories are managed).
    environment: OnceLock<Arc<NodeEnvironment>>,
    /// Set by [`EngineRegistry::shutdown`]: publications are ignored and no
    /// engine is attached again.
    shutting_down: AtomicBool,
}

static GLOBAL_REGISTRY: OnceLock<Arc<EngineRegistry>> = OnceLock::new();

impl EngineRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn global() -> &'static Arc<EngineRegistry> {
        GLOBAL_REGISTRY.get_or_init(|| Arc::new(Self::new()))
    }

    /// Registers an engine implementation. Returns false when the name is
    /// already taken or is the reserved `object` engine.
    pub fn register_engine(&self, engine: Arc<dyn BucketEngine>) -> bool {
        let name = engine.descriptor().name.clone();
        if name.as_str() == OBJECT_ENGINE {
            return false;
        }
        let mut engines = self.engines.write();
        if engines.contains_key(&name) {
            return false;
        }
        engines.insert(name, engine);
        true
    }

    /// Sets this node's environment once, at startup; later calls are ignored.
    pub fn set_environment(&self, environment: Arc<NodeEnvironment>) -> bool {
        self.environment.set(environment).is_ok()
    }

    pub fn environment(&self) -> Option<Arc<NodeEnvironment>> {
        self.environment.get().cloned()
    }

    /// Removes a purged bucket incarnation's engine directories from this
    /// node's drives.
    pub(crate) async fn remove_engine_dirs(&self, bucket: &BucketContext) -> Result<(), EngineError> {
        match self.environment.get() {
            Some(environment) => super::environment::remove_engine_dirs(environment, bucket)
                .await
                .map_err(EngineError::from),
            None => Ok(()),
        }
    }

    pub fn engine(&self, name: &EngineName) -> Option<Arc<dyn BucketEngine>> {
        self.engines.read().get(name).cloned()
    }

    /// Names of the engines this node implements, sorted.
    pub fn engine_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.engines.read().keys().map(|name| name.as_str().to_owned()).collect();
        names.sort();
        names
    }

    /// Route for a bucket. Buckets never bound to an engine take the fast
    /// path to the object layer.
    pub fn route(&self, bucket: &str) -> EngineRoute {
        self.buckets
            .read()
            .get(bucket)
            .map_or(EngineRoute::Object, |slot| slot.route.clone())
    }

    /// Applies a published `engine.json` entry: `stored` is the raw binding
    /// and the bucket incarnation, `None` when the bucket or entry is gone.
    /// Returns the reconcile task when the binding changed.
    pub fn publish(self: &Arc<Self>, bucket: &str, stored: Option<(&[u8], u128)>) -> Option<JoinHandle<()>> {
        if self.shutting_down.load(Ordering::Acquire) {
            return None;
        }
        let desired = self.desired(bucket, stored);
        let route = match &desired {
            Desired::Object => EngineRoute::Object,
            Desired::Bind { context, .. } => EngineRoute::Unavailable {
                engine: context.binding.engine.as_str().to_owned(),
                reason: REASON_ATTACHING.to_owned(),
            },
            Desired::Refuse { engine, reason } => EngineRoute::Unavailable {
                engine: engine.clone(),
                reason: reason.clone(),
            },
        };
        {
            let mut buckets = self.buckets.write();
            match buckets.get_mut(bucket) {
                Some(slot) => {
                    if slot.desired.same_as(&desired) && !slot.failed {
                        return None;
                    }
                    // A reconfigure of the attached incarnation keeps serving
                    // the current binding until the engine accepts the new one.
                    let keeps_serving = matches!(
                        (&slot.route, &desired),
                        (EngineRoute::Engine(bound), Desired::Bind { context, .. })
                            if bound.context.incarnation == context.incarnation
                                && bound.context.binding.engine == context.binding.engine
                    );
                    slot.generation += 1;
                    slot.desired = desired;
                    slot.failed = false;
                    if !keeps_serving {
                        slot.route = route;
                    }
                }
                None => {
                    if matches!(desired, Desired::Object) {
                        return None;
                    }
                    buckets.insert(
                        bucket.to_owned(),
                        BucketSlot {
                            attached: Arc::default(),
                            generation: 0,
                            desired,
                            route,
                            failed: false,
                        },
                    );
                }
            }
        }
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            warn!(
                event = EVENT_BUCKET_ENGINE_BINDING_APPLIED,
                component = LOG_COMPONENT_APP,
                subsystem = LOG_SUBSYSTEM_BUCKET_ENGINE,
                result = "skipped",
                bucket = %bucket,
                "Bucket engine binding published outside a Tokio runtime"
            );
            return None;
        };
        let registry = Arc::clone(self);
        let bucket = bucket.to_owned();
        Some(handle.spawn(async move { registry.reconcile(&bucket).await }))
    }

    fn desired(&self, bucket: &str, stored: Option<(&[u8], u128)>) -> Desired {
        let Some((bytes, incarnation)) = stored else {
            return Desired::Object;
        };
        let binding = match BucketEngineBinding::from_slice(bytes) {
            Ok(binding) => binding,
            Err(err) => {
                return Desired::Refuse {
                    engine: String::new(),
                    reason: err.to_string(),
                };
            }
        };
        if binding.is_object_engine() {
            return Desired::Object;
        }
        let engine_name = binding.engine.as_str().to_owned();
        if incarnation == 0 {
            return Desired::Refuse {
                engine: engine_name,
                reason: "bucket has no incarnation id".to_owned(),
            };
        }
        let Some(engine) = self.engine(&binding.engine) else {
            return Desired::Refuse {
                engine: engine_name,
                reason: "engine is not available on this node".to_owned(),
            };
        };
        if let Err(err) = engine.validate_config(&binding) {
            return Desired::Refuse {
                engine: engine_name,
                reason: err.to_string(),
            };
        }
        Desired::Bind {
            engine,
            context: BucketContext {
                bucket: bucket.to_owned(),
                incarnation,
                binding,
            },
        }
    }

    async fn reconcile(&self, bucket: &str) {
        let Some(attached) = self.buckets.read().get(bucket).map(|slot| Arc::clone(&slot.attached)) else {
            return;
        };
        let mut attached = attached.lock().await;
        if self.shutting_down.load(Ordering::Acquire) {
            // Shutdown detached (or will detach) this bucket; never re-attach.
            return;
        }
        let Some((generation, desired)) = self
            .buckets
            .read()
            .get(bucket)
            .map(|slot| (slot.generation, slot.desired.clone()))
        else {
            return;
        };

        let outcome = match desired {
            Desired::Bind { engine, context } => {
                let result = match attached.as_ref() {
                    Some(current)
                        if current.context.incarnation == context.incarnation
                            && current.context.binding.engine == context.binding.engine =>
                    {
                        if current.context.binding == context.binding {
                            Ok(())
                        } else {
                            engine.reconfigure(&context, &current.context.binding).await
                        }
                    }
                    _ => {
                        if let Some(previous) = attached.take() {
                            detach(bucket, &previous).await;
                        }
                        match self.environment.get() {
                            Some(environment) => match super::environment::create_engine_dirs(environment, &context).await {
                                Ok(()) => engine.attach(&context).await,
                                Err(err) => Err(EngineError::from(err)),
                            },
                            None => engine.attach(&context).await,
                        }
                    }
                };
                match result {
                    Ok(()) => {
                        let bound = Arc::new(BoundBucket { context, engine });
                        *attached = Some(Arc::clone(&bound));
                        Ok(EngineRoute::Engine(bound))
                    }
                    Err(err) => Err(EngineRoute::Unavailable {
                        engine: context.binding.engine.as_str().to_owned(),
                        reason: err.to_string(),
                    }),
                }
            }
            Desired::Object | Desired::Refuse { .. } => {
                if let Some(previous) = attached.take() {
                    detach(bucket, &previous).await;
                }
                Ok(self.route(bucket))
            }
        };

        let mut buckets = self.buckets.write();
        let Some(slot) = buckets.get_mut(bucket) else {
            return;
        };
        if slot.generation != generation {
            // A newer publication is queued behind the lifecycle lock.
            return;
        }
        match outcome {
            Ok(route) => {
                log_applied(bucket, &route);
                if matches!(route, EngineRoute::Object) {
                    buckets.remove(bucket);
                } else {
                    slot.route = route;
                }
            }
            Err(route) => {
                log_applied(bucket, &route);
                slot.route = route;
                slot.failed = true;
            }
        }
    }
}

impl EngineRegistry {
    /// Detaches every attached engine so engine workers stop before the
    /// runtime shuts down. Afterwards publications are ignored and bound
    /// buckets answer unavailable. Returns the number of engines detached.
    pub async fn shutdown(&self) -> usize {
        self.shutting_down.store(true, Ordering::Release);
        let slots: Vec<(String, Attached)> = {
            let mut buckets = self.buckets.write();
            for slot in buckets.values_mut() {
                // Invalidate in-flight reconciles so none publishes a route.
                slot.generation += 1;
                let engine = match &slot.route {
                    EngineRoute::Engine(bound) => bound.context.binding.engine.as_str().to_owned(),
                    EngineRoute::Unavailable { engine, .. } => engine.clone(),
                    EngineRoute::Object => continue,
                };
                slot.route = EngineRoute::Unavailable {
                    engine,
                    reason: REASON_SHUTTING_DOWN.to_owned(),
                };
            }
            buckets
                .iter()
                .map(|(bucket, slot)| (bucket.clone(), Arc::clone(&slot.attached)))
                .collect()
        };
        let mut detached = 0;
        for (bucket, attached) in slots {
            if let Some(previous) = attached.lock().await.take() {
                detach(&bucket, &previous).await;
                detached += 1;
            }
        }
        detached
    }
}

async fn detach(bucket: &str, bound: &BoundBucket) {
    if let Err(err) = bound.engine.detach(&bound.context).await {
        warn!(
            event = EVENT_BUCKET_ENGINE_BINDING_APPLIED,
            component = LOG_COMPONENT_APP,
            subsystem = LOG_SUBSYSTEM_BUCKET_ENGINE,
            result = "detach_failed",
            bucket = %bucket,
            engine = %bound.context.binding.engine.as_str(),
            error = %err,
            "Bucket engine detach failed"
        );
    }
}

fn log_applied(bucket: &str, route: &EngineRoute) {
    match route {
        EngineRoute::Object => info!(
            event = EVENT_BUCKET_ENGINE_BINDING_APPLIED,
            component = LOG_COMPONENT_APP,
            subsystem = LOG_SUBSYSTEM_BUCKET_ENGINE,
            result = "object",
            bucket = %bucket,
            "Bucket engine binding removed"
        ),
        EngineRoute::Engine(bound) => info!(
            event = EVENT_BUCKET_ENGINE_BINDING_APPLIED,
            component = LOG_COMPONENT_APP,
            subsystem = LOG_SUBSYSTEM_BUCKET_ENGINE,
            result = "bound",
            bucket = %bucket,
            engine = %bound.context.binding.engine.as_str(),
            config_version = bound.context.binding.config_version,
            "Bucket engine attached"
        ),
        EngineRoute::Unavailable { engine, reason } => error!(
            event = EVENT_BUCKET_ENGINE_BINDING_APPLIED,
            component = LOG_COMPONENT_APP,
            subsystem = LOG_SUBSYSTEM_BUCKET_ENGINE,
            result = "unavailable",
            bucket = %bucket,
            engine = %engine,
            reason = %reason,
            "Bucket engine unavailable; bucket requests are refused"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use rustfs_bucket_engine::{EngineCapabilities, EngineDescriptor, EngineError, EngineResult};
    use std::sync::atomic::{AtomicBool, Ordering};

    struct RecordingEngine {
        descriptor: EngineDescriptor,
        events: parking_lot::Mutex<Vec<String>>,
        fail_attach: AtomicBool,
    }

    impl RecordingEngine {
        fn new(name: &str) -> Arc<Self> {
            Arc::new(Self {
                descriptor: EngineDescriptor {
                    name: EngineName::new(name).expect("valid engine name"),
                    version: "test".to_owned(),
                    config_version: 1,
                    capabilities: EngineCapabilities::default(),
                },
                events: parking_lot::Mutex::default(),
                fail_attach: AtomicBool::new(false),
            })
        }

        fn events(&self) -> Vec<String> {
            self.events.lock().clone()
        }

        fn record(&self, event: &str, bucket: &BucketContext) {
            self.events
                .lock()
                .push(format!("{event}:{}:{}:{}", bucket.bucket, bucket.incarnation, bucket.binding.config));
        }
    }

    #[async_trait]
    impl BucketEngine for RecordingEngine {
        fn descriptor(&self) -> &EngineDescriptor {
            &self.descriptor
        }

        fn validate_config(&self, binding: &BucketEngineBinding) -> EngineResult<()> {
            if binding.config.get("invalid").is_some() {
                return Err(EngineError::InvalidConfig("rejected by test engine".to_owned()));
            }
            Ok(())
        }

        async fn bind(&self, _bucket: &BucketContext) -> EngineResult<()> {
            Ok(())
        }

        async fn attach(&self, bucket: &BucketContext) -> EngineResult<()> {
            self.record("attach", bucket);
            if self.fail_attach.load(Ordering::SeqCst) {
                return Err(EngineError::Unavailable("attach failed".to_owned()));
            }
            Ok(())
        }

        async fn reconfigure(&self, bucket: &BucketContext, previous: &BucketEngineBinding) -> EngineResult<()> {
            self.record(&format!("reconfigure-from-{}", previous.config), bucket);
            Ok(())
        }

        async fn detach(&self, bucket: &BucketContext) -> EngineResult<()> {
            self.record("detach", bucket);
            Ok(())
        }

        async fn holds_data(&self, _bucket: &BucketContext) -> EngineResult<bool> {
            Ok(false)
        }

        async fn purge(&self, _bucket: &BucketContext) -> EngineResult<()> {
            Ok(())
        }
    }

    fn registry_with(engine: &Arc<RecordingEngine>) -> Arc<EngineRegistry> {
        let registry = Arc::new(EngineRegistry::new());
        assert!(registry.register_engine(engine.clone()));
        registry
    }

    fn binding(engine: &str, config: &str) -> Vec<u8> {
        format!(r#"{{"engine":"{engine}","config_version":1,"config":{config}}}"#).into_bytes()
    }

    async fn publish(registry: &Arc<EngineRegistry>, bucket: &str, stored: Option<(&[u8], u128)>) {
        if let Some(task) = registry.publish(bucket, stored) {
            task.await.expect("reconcile task");
        }
    }

    fn assert_unavailable(route: EngineRoute, expected_reason: &str) {
        match route {
            EngineRoute::Unavailable { reason, .. } => assert!(reason.contains(expected_reason), "{reason}"),
            other => panic!("expected an unavailable route, got {other:?}"),
        }
    }

    fn bound(route: EngineRoute) -> Arc<BoundBucket> {
        match route {
            EngineRoute::Engine(bound) => bound,
            other => panic!("expected an engine route, got {other:?}"),
        }
    }

    #[test]
    fn object_engine_name_is_reserved_and_names_are_unique() {
        let engine = RecordingEngine::new("kv-cache");
        let registry = registry_with(&engine);
        assert!(!registry.register_engine(engine));
        assert!(!registry.register_engine(RecordingEngine::new(OBJECT_ENGINE)));
    }

    #[tokio::test]
    async fn unbound_and_object_bound_buckets_route_to_the_object_layer() {
        let engine = RecordingEngine::new("kv-cache");
        let registry = registry_with(&engine);
        assert!(registry.route("plain").is_object());
        assert!(registry.publish("plain", None).is_none());
        assert!(registry.publish("plain", Some((&binding("object", "{}"), 7))).is_none());
        assert!(registry.route("plain").is_object());
        assert!(engine.events().is_empty());
    }

    #[tokio::test]
    async fn binding_attaches_once_and_routes_after_attach() {
        let engine = RecordingEngine::new("kv-cache");
        let registry = registry_with(&engine);
        let stored = binding("kv-cache", r#"{"a":1}"#);
        let task = registry.publish("kv", Some((&stored, 7))).expect("binding changed");
        assert_unavailable(registry.route("kv"), REASON_ATTACHING);
        task.await.expect("reconcile task");

        let bound = bound(registry.route("kv"));
        assert_eq!(bound.context.bucket, "kv");
        assert_eq!(bound.context.incarnation, 7);
        assert_eq!(bound.context.binding.config["a"], 1);

        // Metadata reloads republish the same entry; nothing changes.
        assert!(registry.publish("kv", Some((&stored, 7))).is_none());
        assert_eq!(engine.events(), vec![r#"attach:kv:7:{"a":1}"#]);
    }

    #[tokio::test]
    async fn unknown_engine_invalid_binding_and_invalid_config_are_refused_not_served_by_object_layer() {
        let engine = RecordingEngine::new("kv-cache");
        let registry = registry_with(&engine);

        publish(&registry, "unknown", Some((&binding("table-cache", "{}"), 7))).await;
        assert_unavailable(registry.route("unknown"), "not available on this node");

        publish(&registry, "garbled", Some((b"{not json", 7))).await;
        assert_unavailable(registry.route("garbled"), "binding");

        publish(&registry, "rejected", Some((&binding("kv-cache", r#"{"invalid":true}"#), 7))).await;
        assert_unavailable(registry.route("rejected"), "rejected by test engine");

        publish(&registry, "nil", Some((&binding("kv-cache", "{}"), 0))).await;
        assert_unavailable(registry.route("nil"), "incarnation");
        assert!(engine.events().is_empty());
    }

    #[tokio::test]
    async fn config_change_reconfigures_and_keeps_serving_during_the_change() {
        let engine = RecordingEngine::new("kv-cache");
        let registry = registry_with(&engine);
        publish(&registry, "kv", Some((&binding("kv-cache", r#"{"a":1}"#), 7))).await;

        let task = registry
            .publish("kv", Some((&binding("kv-cache", r#"{"a":2}"#), 7)))
            .expect("binding changed");
        assert_eq!(bound(registry.route("kv")).context.binding.config["a"], 1);
        task.await.expect("reconcile task");

        assert_eq!(bound(registry.route("kv")).context.binding.config["a"], 2);
        assert_eq!(
            engine.events(),
            vec![r#"attach:kv:7:{"a":1}"#, r#"reconfigure-from-{"a":1}:kv:7:{"a":2}"#]
        );
    }

    #[tokio::test]
    async fn new_incarnation_detaches_the_old_one_before_attaching() {
        let engine = RecordingEngine::new("kv-cache");
        let registry = registry_with(&engine);
        publish(&registry, "kv", Some((&binding("kv-cache", "{}"), 7))).await;
        publish(&registry, "kv", Some((&binding("kv-cache", "{}"), 8))).await;

        assert_eq!(bound(registry.route("kv")).context.incarnation, 8);
        assert_eq!(engine.events(), vec!["attach:kv:7:{}", "detach:kv:7:{}", "attach:kv:8:{}"]);
    }

    #[tokio::test]
    async fn removal_detaches_and_returns_to_the_object_layer() {
        let engine = RecordingEngine::new("kv-cache");
        let registry = registry_with(&engine);
        publish(&registry, "kv", Some((&binding("kv-cache", "{}"), 7))).await;
        publish(&registry, "kv", None).await;

        assert!(registry.route("kv").is_object());
        assert!(registry.buckets.read().is_empty());
        assert_eq!(engine.events(), vec!["attach:kv:7:{}", "detach:kv:7:{}"]);
    }

    #[tokio::test]
    async fn failed_attach_is_refused_and_retried_on_the_next_publication() {
        let engine = RecordingEngine::new("kv-cache");
        let registry = registry_with(&engine);
        let stored = binding("kv-cache", "{}");
        engine.fail_attach.store(true, Ordering::SeqCst);
        publish(&registry, "kv", Some((&stored, 7))).await;
        assert_unavailable(registry.route("kv"), "attach failed");

        engine.fail_attach.store(false, Ordering::SeqCst);
        publish(&registry, "kv", Some((&stored, 7))).await;
        assert_eq!(bound(registry.route("kv")).context.incarnation, 7);
        assert_eq!(engine.events(), vec!["attach:kv:7:{}", "attach:kv:7:{}"]);
    }

    #[tokio::test]
    async fn shutdown_detaches_everything_and_ignores_later_publications() {
        let engine = RecordingEngine::new("kv-cache");
        let registry = registry_with(&engine);
        publish(&registry, "a", Some((&binding("kv-cache", "{}"), 7))).await;
        publish(&registry, "b", Some((&binding("kv-cache", "{}"), 8))).await;
        // Published before shutdown, reconciled after it: must not attach.
        let pending = registry.publish("c", Some((&binding("kv-cache", "{}"), 9)));

        assert_eq!(registry.shutdown().await, 2);
        if let Some(task) = pending {
            task.await.expect("reconcile task");
        }
        assert!(registry.publish("d", Some((&binding("kv-cache", "{}"), 10))).is_none());

        for bucket in ["a", "b", "c"] {
            assert_unavailable(registry.route(bucket), REASON_SHUTTING_DOWN);
        }
        assert!(registry.route("d").is_object());
        let events = engine.events();
        let attaches = events.iter().filter(|e| e.starts_with("attach")).count();
        let detaches = events.iter().filter(|e| e.starts_with("detach")).count();
        assert_eq!((attaches, detaches), (2, 2), "{events:?}");
    }

    #[tokio::test]
    async fn racing_publications_converge_on_the_latest_binding() {
        let engine = RecordingEngine::new("kv-cache");
        let registry = registry_with(&engine);
        let first = registry.publish("kv", Some((&binding("kv-cache", "{}"), 7)));
        let second = registry.publish("kv", Some((&binding("kv-cache", "{}"), 8)));
        let third = registry.publish("kv", None);
        let fourth = registry.publish("kv", Some((&binding("kv-cache", "{}"), 9)));
        for task in [fourth, third, second, first].into_iter().flatten() {
            task.await.expect("reconcile task");
        }

        assert_eq!(bound(registry.route("kv")).context.incarnation, 9);
        let events = engine.events();
        let attaches = events.iter().filter(|e| e.starts_with("attach")).count();
        let detaches = events.iter().filter(|e| e.starts_with("detach")).count();
        assert_eq!(attaches, detaches + 1, "exactly one incarnation stays attached: {events:?}");
        assert_eq!(events.last().map(String::as_str), Some("attach:kv:9:{}"));
    }
}
