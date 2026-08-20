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

use smallvec::SmallVec;
use std::{
    sync::{
        Arc, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};
use tokio::sync::broadcast;

const DEFAULT_TRACE_BUS_CAPACITY: usize = 1024;
const TRACE_ATTR_INLINE_CAPACITY: usize = 8;

static GLOBAL_TRACE_BUS: OnceLock<TraceBus> = OnceLock::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceKind {
    Heal,
    Scanner,
}

impl TraceKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Heal => "heal",
            Self::Scanner => "scanner",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceFunc {
    HealTask,
    HealBucket,
    HealObject,
    HealCheckAbandonedParts,
    HealErasureSetPage,
    ScannerFolder,
    ScannerIlmAction,
    ScannerHealCandidate,
    Dropped,
}

impl TraceFunc {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HealTask => "heal.Task",
            Self::HealBucket => "heal.Bucket",
            Self::HealObject => "heal.Object",
            Self::HealCheckAbandonedParts => "heal.CheckAbandonedParts",
            Self::HealErasureSetPage => "heal.ErasureSetPage",
            Self::ScannerFolder => "scanner.Folder",
            Self::ScannerIlmAction => "scanner.IlmAction",
            Self::ScannerHealCandidate => "scanner.HealCandidate",
            Self::Dropped => "trace.Dropped",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TraceVal {
    Bool(bool),
    U64(u64),
    I64(i64),
    Str(Arc<str>),
}

impl From<bool> for TraceVal {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<u64> for TraceVal {
    fn from(value: u64) -> Self {
        Self::U64(value)
    }
}

impl From<i64> for TraceVal {
    fn from(value: i64) -> Self {
        Self::I64(value)
    }
}

impl From<&str> for TraceVal {
    fn from(value: &str) -> Self {
        Self::Str(Arc::from(value))
    }
}

impl From<String> for TraceVal {
    fn from(value: String) -> Self {
        Self::Str(Arc::from(value))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceAttr {
    pub key: &'static str,
    pub value: TraceVal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceEvent {
    pub kind: TraceKind,
    pub func: TraceFunc,
    pub time: SystemTime,
    pub bucket: Option<Arc<str>>,
    pub object: Option<Arc<str>>,
    pub duration: Duration,
    pub bytes: u64,
    pub attrs: SmallVec<[TraceAttr; TRACE_ATTR_INLINE_CAPACITY]>,
}

impl TraceEvent {
    pub fn new(kind: TraceKind, func: TraceFunc) -> Self {
        Self {
            kind,
            func,
            time: SystemTime::now(),
            bucket: None,
            object: None,
            duration: Duration::ZERO,
            bytes: 0,
            attrs: SmallVec::new(),
        }
    }

    pub fn with_bucket(mut self, bucket: impl Into<Arc<str>>) -> Self {
        self.bucket = Some(bucket.into());
        self
    }

    pub fn with_object(mut self, object: impl Into<Arc<str>>) -> Self {
        self.object = Some(object.into());
        self
    }

    pub fn with_duration(mut self, duration: Duration) -> Self {
        self.duration = duration;
        self
    }

    pub fn with_bytes(mut self, bytes: u64) -> Self {
        self.bytes = bytes;
        self
    }

    pub fn with_attr(mut self, key: &'static str, value: impl Into<TraceVal>) -> Self {
        self.attrs.push(TraceAttr {
            key,
            value: value.into(),
        });
        self
    }
}

#[derive(Debug)]
pub struct TraceBus {
    sender: broadcast::Sender<Arc<TraceEvent>>,
    subscriber_count: Arc<AtomicUsize>,
}

impl TraceBus {
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        let (sender, _receiver) = broadcast::channel(capacity);
        Self {
            sender,
            subscriber_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn subscriber_count(&self) -> usize {
        self.subscriber_count.load(Ordering::Acquire)
    }

    pub fn subscribe(&self) -> TraceSubscription {
        let receiver = self.sender.subscribe();
        self.subscriber_count.fetch_add(1, Ordering::AcqRel);
        TraceSubscription {
            receiver,
            subscriber_count: Arc::clone(&self.subscriber_count),
        }
    }

    pub fn emit(&self, build: impl FnOnce() -> TraceEvent) -> bool {
        if self.subscriber_count() == 0 {
            return false;
        }

        self.sender.send(Arc::new(build())).is_ok()
    }
}

impl Default for TraceBus {
    fn default() -> Self {
        Self::new(DEFAULT_TRACE_BUS_CAPACITY)
    }
}

#[derive(Debug)]
pub struct TraceSubscription {
    receiver: broadcast::Receiver<Arc<TraceEvent>>,
    subscriber_count: Arc<AtomicUsize>,
}

impl TraceSubscription {
    pub async fn recv(&mut self) -> Result<Arc<TraceEvent>, broadcast::error::RecvError> {
        self.receiver.recv().await
    }

    pub fn try_recv(&mut self) -> Result<Arc<TraceEvent>, broadcast::error::TryRecvError> {
        self.receiver.try_recv()
    }
}

impl Drop for TraceSubscription {
    fn drop(&mut self) {
        self.subscriber_count.fetch_sub(1, Ordering::AcqRel);
    }
}

pub fn global_trace_bus() -> &'static TraceBus {
    GLOBAL_TRACE_BUS.get_or_init(TraceBus::default)
}

pub fn subscribe_trace_events() -> TraceSubscription {
    global_trace_bus().subscribe()
}

pub fn trace_emit(build: impl FnOnce() -> TraceEvent) -> bool {
    global_trace_bus().emit(build)
}

pub fn trace_subscriber_count() -> usize {
    global_trace_bus().subscriber_count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn trace_emit_skips_builder_without_subscribers() {
        let bus = TraceBus::new(4);
        let built = AtomicUsize::new(0);

        let sent = bus.emit(|| {
            built.fetch_add(1, Ordering::Relaxed);
            TraceEvent::new(TraceKind::Heal, TraceFunc::HealTask)
        });

        assert!(!sent);
        assert_eq!(built.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn trace_subscriber_receives_event() {
        let bus = TraceBus::new(4);
        let mut subscription = bus.subscribe();

        assert!(bus.emit(|| {
            TraceEvent::new(TraceKind::Heal, TraceFunc::HealObject)
                .with_bucket("bucket")
                .with_object("object")
                .with_duration(Duration::from_millis(7))
                .with_bytes(11)
                .with_attr("dry", true)
        }));

        let event = subscription
            .recv()
            .await
            .expect("subscriber should receive emitted trace event");

        assert_eq!(event.kind, TraceKind::Heal);
        assert_eq!(event.func, TraceFunc::HealObject);
        assert_eq!(event.bucket.as_deref(), Some("bucket"));
        assert_eq!(event.object.as_deref(), Some("object"));
        assert_eq!(event.duration, Duration::from_millis(7));
        assert_eq!(event.bytes, 11);
        assert_eq!(
            event.attrs.as_slice(),
            &[TraceAttr {
                key: "dry",
                value: TraceVal::Bool(true)
            }]
        );
    }

    #[test]
    fn trace_subscription_drop_decrements_count() {
        let bus = TraceBus::new(4);
        let subscription = bus.subscribe();

        assert_eq!(bus.subscriber_count(), 1);
        drop(subscription);
        assert_eq!(bus.subscriber_count(), 0);
    }

    #[tokio::test]
    async fn lagged_subscriber_drops_events_without_blocking_publishers() {
        let bus = TraceBus::new(2);
        let mut subscription = bus.subscribe();

        for index in 0_u64..4 {
            assert!(bus.emit(|| { TraceEvent::new(TraceKind::Scanner, TraceFunc::ScannerFolder).with_attr("index", index) }));
        }

        let err = subscription
            .recv()
            .await
            .expect_err("receiver should observe lag instead of blocking publishers");
        assert!(matches!(err, broadcast::error::RecvError::Lagged(_)));
    }
}
