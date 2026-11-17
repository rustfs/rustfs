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

use tracing::{Event, Subscriber, field::Visit};
use tracing_subscriber::{Layer, layer::Context};

pub(super) const HYPER_HTTP2_CLIENT_TARGET: &str = "hyper::proto::h2::client";
const HYPER_KEEPALIVE_TIMEOUT_MESSAGE: &str = "connection keep-alive timed out";

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Http2PingTimeoutLayer;

pub(crate) const fn http2_ping_timeout_layer() -> Option<Http2PingTimeoutLayer> {
    if cfg!(feature = "diagnostic-internode-http2-ping") {
        Some(Http2PingTimeoutLayer)
    } else {
        None
    }
}

#[derive(Default)]
struct TimeoutMessageVisitor {
    matched: bool,
}

impl Visit for TimeoutMessageVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" && format!("{value:?}") == HYPER_KEEPALIVE_TIMEOUT_MESSAGE {
            self.matched = true;
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" && value == HYPER_KEEPALIVE_TIMEOUT_MESSAGE {
            self.matched = true;
        }
    }
}

impl<S> Layer<S> for Http2PingTimeoutLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
        if event.metadata().target() != HYPER_HTTP2_CLIENT_TARGET
            || !rustfs_io_metrics::internode_metrics::is_internode_http2_client_task()
        {
            return;
        }

        let mut visitor = TimeoutMessageVisitor::default();
        event.record(&mut visitor);
        if visitor.matched {
            rustfs_io_metrics::internode_metrics::record_http2_ping_timeout();
        }
    }
}

#[cfg(all(test, feature = "diagnostic-internode-http2-ping"))]
mod tests {
    use super::*;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use tracing_subscriber::{Registry, layer::SubscriberExt};

    static METRICS_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    #[tokio::test]
    async fn exact_scoped_hyper_timeout_event_records_once_without_close_inference() {
        let _test_guard = METRICS_TEST_LOCK.lock().await;
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let _recorder_guard = metrics::set_default_local_recorder(&recorder);
        let previously_enabled = rustfs_io_metrics::metrics_enabled();
        rustfs_io_metrics::set_metrics_enabled(true);

        let subscriber = Registry::default()
            .with(crate::telemetry::filter::build_env_filter("info", Some("info")))
            .with(http2_ping_timeout_layer());
        let dispatch = tracing::Dispatch::new(subscriber);
        tracing::dispatcher::with_default(&dispatch, || {
            tracing::debug!(target: HYPER_HTTP2_CLIENT_TARGET, "connection keep-alive timed out");
        });
        rustfs_io_metrics::internode_metrics::scope_internode_http2_client_task(async {
            tracing::dispatcher::with_default(&dispatch, || {
                tracing::debug!(target: HYPER_HTTP2_CLIENT_TARGET, "connection keep-alive timed out");
                tracing::debug!(target: HYPER_HTTP2_CLIENT_TARGET, "connection error");
                tracing::debug!(target: "rustfs::test", "connection keep-alive timed out");
            });
        })
        .await;
        rustfs_io_metrics::set_metrics_enabled(previously_enabled);

        let timeouts = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .find(|(key, _, _, _)| key.key().name() == "rustfs_system_network_internode_http2_ping_timeouts_total")
            .map(|(_, _, _, value)| value)
            .expect("exact HTTP/2 PING timeout counter");
        assert!(matches!(timeouts, DebugValue::Counter(1)));
    }
}
