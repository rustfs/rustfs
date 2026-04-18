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

//! Console realtime metrics API.
//!
//! This preserves the console's fixed `/admin/v3/metrics` contract while
//! keeping the response format explicitly NDJSON. It is not a Prometheus text
//! exposition endpoint.

use crate::admin::router::Operation;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use http::{HeaderMap, HeaderValue, Uri};
use hyper::StatusCode;
use matchit::Params;
use rustfs_ecstore::metrics_realtime::{CollectMetricsOpts, MetricType, collect_local_metrics};
use rustfs_madmin::metrics::RealtimeMetrics;
use rustfs_madmin::utils::parse_duration;
use s3s::header::CONTENT_TYPE;
use s3s::stream::{ByteStream, DynByteStream};
use s3s::{Body, S3Request, S3Response, S3Result, StdError, s3_error};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration as StdDuration;
use tokio::sync::mpsc;
use tokio::time::interval;
use tokio::{select, spawn};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, warn};

const DEFAULT_METRICS_SAMPLES: u64 = 1;
const MAX_METRICS_SAMPLES: u64 = 120;
const CONSOLE_METRICS_CONTENT_TYPE: &str = "application/x-ndjson";

#[derive(Debug, Serialize, Deserialize)]
struct MetricsParams {
    disks: String,
    hosts: String,
    #[serde(rename = "interval")]
    tick: String,
    n: u64,
    types: u32,
    #[serde(rename = "by-disk")]
    by_disk: String,
    #[serde(rename = "by-host")]
    by_host: String,
    #[serde(rename = "by-jobID")]
    by_job_id: String,
    #[serde(rename = "by-depID")]
    by_dep_id: String,
    #[serde(skip)]
    n_set: bool,
}

impl Default for MetricsParams {
    fn default() -> Self {
        Self {
            disks: Default::default(),
            hosts: Default::default(),
            tick: Default::default(),
            n: DEFAULT_METRICS_SAMPLES,
            types: Default::default(),
            by_disk: Default::default(),
            by_host: Default::default(),
            by_job_id: Default::default(),
            by_dep_id: Default::default(),
            n_set: false,
        }
    }
}

fn extract_metrics_init_params(uri: &Uri) -> MetricsParams {
    let mut mp = MetricsParams::default();
    if let Some(query) = uri.query() {
        let params: Vec<&str> = query.split('&').collect();
        for param in params {
            let mut parts = param.split('=');
            if let Some(key) = parts.next() {
                if key == "disks"
                    && let Some(value) = parts.next()
                {
                    mp.disks = value.to_string();
                }
                if key == "hosts"
                    && let Some(value) = parts.next()
                {
                    mp.hosts = value.to_string();
                }
                if key == "interval"
                    && let Some(value) = parts.next()
                {
                    mp.tick = value.to_string();
                }
                if key == "n"
                    && let Some(value) = parts.next()
                {
                    mp.n_set = true;
                    mp.n = value.parse::<u64>().unwrap_or(DEFAULT_METRICS_SAMPLES);
                }
                if key == "types"
                    && let Some(value) = parts.next()
                {
                    mp.types = value.parse::<u32>().unwrap_or_default();
                }
                if key == "by-disk"
                    && let Some(value) = parts.next()
                {
                    mp.by_disk = value.to_string();
                }
                if key == "by-host"
                    && let Some(value) = parts.next()
                {
                    mp.by_host = value.to_string();
                }
                if key == "by-jobID"
                    && let Some(value) = parts.next()
                {
                    mp.by_job_id = value.to_string();
                }
                if key == "by-depID"
                    && let Some(value) = parts.next()
                {
                    mp.by_dep_id = value.to_string();
                }
            }
        }
    }
    mp
}

fn resolve_sample_count(mp: &MetricsParams) -> u64 {
    let requested = if mp.n_set { mp.n } else { DEFAULT_METRICS_SAMPLES };

    if requested == 0 {
        return DEFAULT_METRICS_SAMPLES;
    }

    if requested > MAX_METRICS_SAMPLES {
        warn!(
            requested,
            max = MAX_METRICS_SAMPLES,
            "metrics request sample count too large, capping to safety limit"
        );
        return MAX_METRICS_SAMPLES;
    }

    requested
}

struct MetricsStream {
    inner: ReceiverStream<Result<Bytes, StdError>>,
}

impl Stream for MetricsStream {
    type Item = Result<Bytes, StdError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = Pin::into_inner(self);
        this.inner.poll_next_unpin(cx)
    }
}

impl ByteStream for MetricsStream {}

pub struct MetricsHandler {}

#[async_trait::async_trait]
impl Operation for MetricsHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        debug!("handle MetricsHandler, uri: {:?}, params: {:?}", req.uri, params);
        let Some(_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };
        debug!("validated console metrics request credentials");

        let mp = extract_metrics_init_params(&req.uri);
        debug!("mp: {:?}", mp);

        let tick = parse_duration(&mp.tick).unwrap_or_else(|_| StdDuration::from_secs(3));
        let mut n = resolve_sample_count(&mp);

        let types = if mp.types != 0 {
            MetricType::new(mp.types)
        } else {
            MetricType::ALL
        };

        fn parse_comma_separated(s: &str) -> HashSet<String> {
            s.split(',').filter(|part| !part.is_empty()).map(String::from).collect()
        }

        let by_disk = mp.by_disk == "true";
        let by_host = mp.by_host == "true";
        let mut interval = interval(tick);
        let opts = CollectMetricsOpts {
            hosts: parse_comma_separated(&mp.hosts),
            disks: parse_comma_separated(&mp.disks),
            job_id: mp.by_job_id,
            dep_id: mp.by_dep_id,
        };
        let (tx, rx) = mpsc::channel(10);
        let in_stream: DynByteStream = Box::pin(MetricsStream {
            inner: ReceiverStream::new(rx),
        });
        let body = Body::from(in_stream);

        spawn(async move {
            while n > 0 {
                let mut metrics = RealtimeMetrics::default();
                let local_metrics = collect_local_metrics(types, &opts).await;
                metrics.merge(local_metrics);

                if !by_host {
                    metrics.by_host = HashMap::new();
                }
                if !by_disk {
                    metrics.by_disk = HashMap::new();
                }

                metrics.finally = n <= 1;

                match serde_json::to_vec(&metrics) {
                    Ok(mut encoded) => {
                        encoded.push(b'\n');
                        let _ = tx.send(Ok(Bytes::from(encoded))).await;
                    }
                    Err(err) => {
                        error!("MetricsHandler: json encode failed, err: {:?}", err);
                        return;
                    }
                }

                n -= 1;
                if n == 0 {
                    break;
                }

                select! {
                    _ = tx.closed() => { return; }
                    _ = interval.tick() => {}
                }
            }
        });

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, HeaderValue::from_static(CONSOLE_METRICS_CONTENT_TYPE));

        Ok(S3Response::with_headers((StatusCode::OK, body), header))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CONSOLE_METRICS_CONTENT_TYPE, DEFAULT_METRICS_SAMPLES, MAX_METRICS_SAMPLES, extract_metrics_init_params,
        resolve_sample_count,
    };
    use http::Uri;

    #[test]
    fn metrics_params_default_to_single_sample() {
        let uri: Uri = "/rustfs/admin/v3/metrics".parse().unwrap();
        let mp = extract_metrics_init_params(&uri);

        assert_eq!(resolve_sample_count(&mp), DEFAULT_METRICS_SAMPLES);
    }

    #[test]
    fn metrics_params_treat_zero_as_single_sample() {
        let uri: Uri = "/rustfs/admin/v3/metrics?n=0".parse().unwrap();
        let mp = extract_metrics_init_params(&uri);

        assert_eq!(resolve_sample_count(&mp), DEFAULT_METRICS_SAMPLES);
    }

    #[test]
    fn metrics_params_cap_samples_to_safety_limit() {
        let uri: Uri = "/rustfs/admin/v3/metrics?n=9999".parse().unwrap();
        let mp = extract_metrics_init_params(&uri);

        assert_eq!(resolve_sample_count(&mp), MAX_METRICS_SAMPLES);
    }

    #[test]
    fn metrics_handler_uses_ndjson_content_type() {
        assert_eq!(CONSOLE_METRICS_CONTENT_TYPE, "application/x-ndjson");
    }
}
