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

//! The gateway's [`Observer`], feeding RustFS's per-request S3 counter.
//!
//! Responsible for: turning the gateway's [`RequestEvent`] into one [`RequestSample`] — the
//! `method`, `op` and `outcome` labels of `rustfs_s3_http_requests_total`, each bounded to the
//! value set the legacy counter uses — and handing it to a [`RequestMetricsSink`].
//! NOT responsible for: anything the gateway cannot know when it reports, which is before the first
//! body byte. Those contract items stay with RustFS's outer layers: every `rustfs_http_server_*`
//! series (duration, request and response bytes, body chunks, transport failures),
//! `rustfs_s3_http_entry_to_dispatch_duration_seconds`, and the `cancelled` and `service_error`
//! outcomes of the counter this module feeds (a request whose future is dropped, or whose service
//! fails, is never observed). Handler-entry series (`rustfs_s3_operations_total`, lock failures,
//! log-chain counters) stay with handler code. Who owns each contract item is recorded in
//! `tests/fixtures/observability-contract.json`; [`OWNED_CONTRACT_ITEMS`] is this module's half.
//! Upstream: the gateway service, once per answered request. Downstream: the binary's
//! [`RequestMetricsSink`], which increments the existing counter.
//!
//! # Cardinality
//!
//! Every label is a `&'static str` from a fixed set, as in `crates/io-metrics/src/s3_http_metrics.rs`:
//! a method outside the nine known ones is `OTHER`, an operation that is not one of the
//! [`S3Operation`] variants is `unknown`, a status outside `100..=599` is `unknown`. Nothing a caller
//! sends can mint a new series. The caller's identity and the S3 error code are deliberately not
//! labels: the legacy counter has neither, and an access key is unbounded.
//!
//! # Sink contract
//!
//! [`Observer::on_response`] is synchronous and must neither block nor panic, so the sink is called
//! inline with a `Copy` value; an implementation increments an atomic or a cached counter handle and
//! returns.

use rustfs_gateway::{ErrorCode, Observer, RequestEvent};
use rustfs_s3_ops::S3Operation;

/// The `op` label of a request routing named no [`S3Operation`] for, as the legacy counter spells it.
pub const UNKNOWN_OPERATION: &str = "unknown";

/// The method labels the legacy counter keeps apart; anything else is the last one, `OTHER`.
const METHODS: [&str; 10] = [
    "GET", "PUT", "POST", "DELETE", "HEAD", "OPTIONS", "PATCH", "CONNECT", "TRACE", "OTHER",
];

/// The status-class outcome labels; `cancelled` and `service_error` belong to the outer layer.
const OUTCOMES: [&str; 6] = ["1xx", "2xx", "3xx", "4xx", "5xx", "unknown"];

/// The contract items (ids in `tests/fixtures/observability-contract.json`) this module supplies.
pub const OWNED_CONTRACT_ITEMS: [&str; 4] = [
    "rustfs_s3_http_requests_total",
    "rustfs_s3_http_requests_total{method}",
    "rustfs_s3_http_requests_total{op}",
    "rustfs_s3_http_requests_total{outcome}",
];

/// One answered request, as the labels of `rustfs_s3_http_requests_total{method, op, outcome}`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequestSample {
    /// The `method` label: one of the nine known methods, or `OTHER`.
    pub method: &'static str,
    /// The `op` label: the operation's legacy spelling (`s3:GetObject`), or [`UNKNOWN_OPERATION`].
    pub op: &'static str,
    /// The `outcome` label: the response status class, or `unknown` for a status outside `100..=599`.
    pub outcome: &'static str,
    /// The operation behind [`Self::op`], for a sink that indexes counters by it.
    pub operation: Option<S3Operation>,
    /// Whether the request succeeded: a `2xx` head that carried no S3 error. A committed answer whose
    /// head was `200` but whose body ended in an error document is not a success.
    pub succeeded: bool,
}

/// Where answered-request samples go. Implemented by the binary over `rustfs-io-metrics`.
pub trait RequestMetricsSink: Send + Sync + 'static {
    /// Records one answered request. Called inline on the response path: must not block or panic.
    fn record_answered(&self, sample: RequestSample);
}

/// An [`Observer`] that maps every answered request onto RustFS's S3 request counter.
pub struct RustfsObserver<S> {
    sink: S,
}

impl<S: RequestMetricsSink> RustfsObserver<S> {
    /// An observer reporting into `sink`.
    #[must_use]
    pub fn new(sink: S) -> Self {
        Self { sink }
    }
}

impl<S: RequestMetricsSink> RustfsObserver<S> {
    /// Reports one answered request. [`Observer::on_response`] calls it with the event's fields; it
    /// is separate because a [`RequestEvent`] can only be built by the gateway.
    fn observe(&self, method: &str, operation: Option<&str>, status: u16, error: Option<&ErrorCode>) {
        self.sink.record_answered(request_sample(method, operation, status, error));
    }
}

impl<S: RequestMetricsSink> Observer for RustfsObserver<S> {
    fn on_response(&self, event: &RequestEvent<'_>) {
        self.observe(event.method().as_str(), event.operation, event.status, event.error);
    }
}

/// The sample for one answered request; [`RustfsObserver`] calls it with the event's fields.
#[must_use]
pub fn request_sample(method: &str, operation: Option<&str>, status: u16, error: Option<&ErrorCode>) -> RequestSample {
    let operation = operation.and_then(s3_operation);
    let outcome = match status {
        100..=599 => OUTCOMES[usize::from(status / 100 - 1)],
        _ => OUTCOMES[OUTCOMES.len() - 1],
    };
    RequestSample {
        method: METHODS
            .iter()
            .copied()
            .find(|known| *known == method)
            .unwrap_or(METHODS[METHODS.len() - 1]),
        op: operation.map_or(UNKNOWN_OPERATION, S3Operation::as_str),
        outcome,
        operation,
        succeeded: (200..300).contains(&status) && error.is_none(),
    }
}

/// The [`S3Operation`] whose legacy spelling is `s3:<name>`, for the gateway's `Operation::NAME`.
fn s3_operation(name: &str) -> Option<S3Operation> {
    S3Operation::ALL
        .iter()
        .copied()
        .find(|op| op.as_str().strip_prefix("s3:") == Some(name))
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Recorded(Mutex<Vec<RequestSample>>);

    impl RequestMetricsSink for Recorded {
        fn record_answered(&self, sample: RequestSample) {
            self.0.lock().expect("not poisoned").push(sample);
        }
    }

    fn labels(sample: &RequestSample) -> [&'static str; 3] {
        [sample.method, sample.op, sample.outcome]
    }

    /// Positive — an answered read is one sample with the legacy label spellings.
    #[test]
    fn observer_success_maps_to_legacy_labels() {
        let sample = request_sample("GET", Some("GetObject"), 200, None);
        assert_eq!(labels(&sample), ["GET", "s3:GetObject", "2xx"]);
        assert_eq!(sample.operation, Some(S3Operation::GetObject));
        assert!(sample.succeeded);
    }

    /// Negative — an error response is counted, under its own status class, as a failure.
    #[test]
    fn observer_error_response_is_counted_under_its_status_class() {
        let observer = RustfsObserver::new(Recorded::default());
        observer.observe("GET", Some("GetObject"), 404, Some(&ErrorCode::NO_SUCH_KEY));
        observer.observe("PUT", Some("PutObject"), 403, Some(&ErrorCode::ACCESS_DENIED));
        let recorded = observer.sink.0.lock().expect("not poisoned");
        assert_eq!(recorded.len(), 2);
        assert_eq!(labels(&recorded[0]), ["GET", "s3:GetObject", "4xx"]);
        assert_eq!(labels(&recorded[1]), ["PUT", "s3:PutObject", "4xx"]);
        assert!(recorded.iter().all(|sample| !sample.succeeded));
    }

    /// Negative — a 5xx is a `5xx` failure, never a success.
    #[test]
    fn observer_server_error_is_not_success() {
        for status in [500, 503, 599] {
            let sample = request_sample("GET", Some("GetObject"), status, Some(&ErrorCode::INTERNAL_ERROR));
            assert_eq!(sample.outcome, "5xx", "{status}");
            assert!(!sample.succeeded, "{status}");
        }
        assert!(!request_sample("GET", Some("GetObject"), 503, None).succeeded);
    }

    /// Negative — a committed `200` whose body ended in an error is not a success, though its
    /// outcome label stays the head's class as the legacy layer reads it.
    #[test]
    fn observer_committed_error_after_200_is_not_success() {
        let sample = request_sample("POST", Some("CompleteMultipartUpload"), 200, Some(&ErrorCode::INTERNAL_ERROR));
        assert_eq!(sample.outcome, "2xx");
        assert!(!sample.succeeded);
    }

    /// Negative — operation names outside the bounded set, including caller-shaped text and the
    /// unrouted case, all collapse to `unknown`; no sample can carry a label the set lacks.
    #[test]
    fn observer_unknown_operation_is_bounded() {
        let unbounded = [
            None,
            Some(""),
            Some("NoSuchOperation"),
            Some("s3:GetObject"),
            Some("getobject"),
            Some("GetObject "),
            Some("GetObject?X-Amz-Signature=deadbeef"),
        ];
        for operation in unbounded {
            let sample = request_sample("GET", operation, 200, None);
            assert_eq!(sample.op, UNKNOWN_OPERATION, "{operation:?}");
            assert_eq!(sample.operation, None, "{operation:?}");
        }
        let known: Vec<&str> = S3Operation::ALL.iter().map(|op| op.as_str()).collect();
        for op in S3Operation::ALL {
            let name = op.as_str().trim_start_matches("s3:");
            let sample = request_sample("GET", Some(name), 200, None);
            assert!(known.contains(&sample.op), "{name}");
            assert_eq!(sample.operation, Some(op));
        }
    }

    /// Negative — unknown and lower-case methods fold into `OTHER`, as the legacy counter does.
    #[test]
    fn observer_unknown_method_is_bounded() {
        for method in ["PROPFIND", "get", "", "X-SECRET-METHOD"] {
            assert_eq!(request_sample(method, Some("GetObject"), 200, None).method, "OTHER", "{method}");
        }
        for method in &METHODS[..9] {
            assert_eq!(request_sample(method, None, 200, None).method, *method);
        }
    }

    /// Negative — a status outside the HTTP range is `unknown`, not a class that does not exist.
    #[test]
    fn observer_out_of_range_status_is_unknown() {
        for status in [0, 99, 600, u16::MAX] {
            let sample = request_sample("GET", Some("GetObject"), status, None);
            assert_eq!(sample.outcome, "unknown", "{status}");
            assert!(!sample.succeeded, "{status}");
        }
        assert_eq!(request_sample("GET", None, 100, None).outcome, "1xx");
        assert_eq!(request_sample("GET", None, 304, None).outcome, "3xx");
    }

    /// Negative — the S3 error code, however a handler spelled it, never reaches the sample.
    #[test]
    fn observer_error_code_is_not_a_label() {
        let custom = ErrorCode::custom("SecretSessionTokenAQoDYXdzEJr", http::StatusCode::BAD_REQUEST);
        let rendered = format!("{:?}", request_sample("GET", Some("GetObject"), 400, Some(&custom)));
        assert!(!rendered.contains("SecretSessionTokenAQoDYXdzEJr"), "{rendered}");
    }
}
