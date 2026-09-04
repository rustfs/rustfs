#![allow(unused_imports)]
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
#![allow(unused_variables)]

use crate::bucket::metadata::BucketMetadata;
// use crate::event::name::EventName;
use crate::event::targetlist::TargetList;
use crate::object_api::ObjectInfo;
use crate::store::ECStore;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::Ordering;
use tokio::sync::RwLock;
use tracing::warn;

/// Dead ecstore-side notification skeleton.
///
/// The working notification stack is `rustfs-notify`, whose own `EventNotifier`
/// is the one bucket configuration actually drives. Nothing calls the methods
/// below; `init_bucket_targets` even logs that it is a no-op in this build.
/// Removing it means also retiring the `InstanceContext` slot that holds it
/// (backlog#939 Phase 5), so it is left explicit here rather than half-removed.
#[allow(
    dead_code,
    reason = "ecstore-side notification skeleton superseded by rustfs-notify; see module note (backlog#1823)"
)]
pub struct EventNotifier {
    target_list: TargetList,
    //bucket_rules_map: HashMap<String , HashMap<EventName, Rules>>,
}

#[allow(
    dead_code,
    reason = "ecstore-side notification skeleton superseded by rustfs-notify; see module note (backlog#1823)"
)]
impl EventNotifier {
    pub fn new() -> Arc<RwLock<Self>> {
        Arc::new(RwLock::new(Self {
            target_list: TargetList::new(),
            //bucket_rules_map: HashMap::new(),
        }))
    }

    fn get_arn_list(&self) -> Vec<String> {
        warn!(
            event_count = self.target_list.total_events.load(Ordering::Relaxed),
            "event notifier arn list requested but not implemented in this build"
        );
        Vec::new()
    }

    fn set(&self, bucket: &str, meta: BucketMetadata) {
        warn!(bucket = bucket, "event notifier set() called but currently no-op in this build");
    }

    fn init_bucket_targets(&self, _api: ECStore) -> Result<(), std::io::Error> {
        warn!("init_bucket_targets called but currently no-op in this build");
        Ok(())
    }

    fn send(&self, args: EventArgs) {
        warn!(
            event_name = args.event_name,
            bucket = args.bucket_name,
            "event send() called but notifier is not fully implemented in this build"
        );
    }
}

#[derive(Debug, Default)]
pub struct EventArgs {
    pub event_name: String,
    pub bucket_name: String,
    pub object: ObjectInfo,
    pub req_params: HashMap<String, String>,
    pub resp_elements: HashMap<String, String>,
    pub host: String,
    pub user_agent: String,
}

impl EventArgs {}

type EventDispatchHook = Arc<dyn Fn(EventArgs) + Send + Sync + 'static>;

static EVENT_DISPATCH_HOOK: OnceLock<EventDispatchHook> = OnceLock::new();

pub fn register_event_dispatch_hook<F>(hook: F) -> bool
where
    F: Fn(EventArgs) + Send + Sync + 'static,
{
    EVENT_DISPATCH_HOOK.set(Arc::new(hook)).is_ok()
}

pub fn send_event(args: EventArgs) {
    if let Some(hook) = EVENT_DISPATCH_HOOK.get() {
        hook(args);
        return;
    }

    warn!(
        event_name = args.event_name,
        bucket = args.bucket_name,
        "event send() dropped because no event dispatch hook is registered"
    );
}

/// Shared event recorder for this crate's tests.
///
/// [`register_event_dispatch_hook`] backs a `OnceLock`, so only the first
/// caller in a test binary can install a hook. Every test that needs to
/// observe dispatched events must therefore go through this single recorder
/// instead of registering its own.
#[cfg(test)]
pub(crate) mod test_recorder {
    use super::register_event_dispatch_hook;
    use std::sync::{Mutex, OnceLock};
    use uuid::Uuid;

    /// The fields a test needs from a dispatched event. `EventArgs` itself is
    /// not `Clone`, and recording a reduced shape keeps this test seam from
    /// constraining the production type.
    #[derive(Clone, Debug)]
    pub(crate) struct RecordedEvent {
        pub(crate) event_name: String,
        pub(crate) bucket: String,
        pub(crate) object: String,
        pub(crate) version_id: Option<Uuid>,
        pub(crate) delete_marker: bool,
    }

    static RECORDED: OnceLock<Mutex<Vec<RecordedEvent>>> = OnceLock::new();

    fn recorded() -> &'static Mutex<Vec<RecordedEvent>> {
        RECORDED.get_or_init(|| Mutex::new(Vec::new()))
    }

    pub(crate) fn install() {
        static INSTALLED: OnceLock<()> = OnceLock::new();
        INSTALLED.get_or_init(|| {
            assert!(
                register_event_dispatch_hook(|args| {
                    recorded().lock().unwrap_or_else(|err| err.into_inner()).push(RecordedEvent {
                        event_name: args.event_name,
                        bucket: args.bucket_name,
                        object: args.object.name,
                        version_id: args.object.version_id,
                        delete_marker: args.object.delete_marker,
                    });
                }),
                "the test event recorder must own this binary's dispatch hook"
            );
        });
    }

    /// Everything recorded for one bucket. Tests select by their own unique
    /// bucket name rather than draining, because tests that are not
    /// `#[serial]` may dispatch events concurrently.
    pub(crate) fn recorded_for_bucket(bucket: &str) -> Vec<RecordedEvent> {
        recorded()
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .iter()
            .filter(|event| event.bucket == bucket)
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_event_dispatches_to_registered_hook() {
        test_recorder::install();
        let bucket = format!("event-dispatch-{}", uuid::Uuid::new_v4().simple());

        send_event(EventArgs {
            event_name: "s3:ObjectCreated:Put".to_string(),
            bucket_name: bucket.clone(),
            ..Default::default()
        });

        let dispatched = test_recorder::recorded_for_bucket(&bucket);
        assert_eq!(dispatched.len(), 1);
        assert_eq!(dispatched[0].event_name, "s3:ObjectCreated:Put");
    }
}
