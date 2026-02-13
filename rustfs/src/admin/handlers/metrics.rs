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

use crate::admin::router::Operation;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use http::Uri;
use hyper::StatusCode;
use matchit::Params;
use rustfs_ecstore::metrics_realtime::{CollectMetricsOpts, MetricType, collect_local_metrics};
use rustfs_madmin::metrics::RealtimeMetrics;
use rustfs_madmin::utils::parse_duration;
use s3s::stream::{ByteStream, DynByteStream};
use s3s::{Body, S3Request, S3Response, S3Result, StdError, s3_error};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration as std_Duration;
use tokio::sync::mpsc;
use tokio::time::interval;
use tokio::{select, spawn};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error};

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
}

impl Default for MetricsParams {
    fn default() -> Self {
        Self {
            disks: Default::default(),
            hosts: Default::default(),
            tick: Default::default(),
            n: u64::MAX,
            types: Default::default(),
            by_disk: Default::default(),
            by_host: Default::default(),
            by_job_id: Default::default(),
            by_dep_id: Default::default(),
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
                    mp.n = value.parse::<u64>().unwrap_or(u64::MAX);
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
        let Some(_cred) = req.credentials else { return Err(s3_error!(InvalidRequest, "get cred failed")) };
        debug!("validated metrics request credentials");

        let mp = extract_metrics_init_params(&req.uri);
        debug!("mp: {:?}", mp);

        let tick = parse_duration(&mp.tick).unwrap_or_else(|_| std_Duration::from_secs(3));

        let mut n = mp.n;
        if n == 0 {
            n = u64::MAX;
        }

        let types = if mp.types != 0 {
            MetricType::new(mp.types)
        } else {
            MetricType::ALL
        };

        fn parse_comma_separated(s: &str) -> HashSet<String> {
            s.split(',').filter(|part| !part.is_empty()).map(String::from).collect()
        }

        let disks = parse_comma_separated(&mp.disks);
        let by_disk = mp.by_disk == "true";
        let disk_map = disks;

        let job_id = mp.by_job_id;
        let hosts = parse_comma_separated(&mp.hosts);
        let by_host = mp.by_host == "true";
        let host_map = hosts;

        let d_id = mp.by_dep_id;
        let mut interval = interval(tick);

        let opts = CollectMetricsOpts {
            hosts: host_map,
            disks: disk_map,
            job_id,
            dep_id: d_id,
        };
        let (tx, rx) = mpsc::channel(10);
        let in_stream: DynByteStream = Box::pin(MetricsStream {
            inner: ReceiverStream::new(rx),
        });
        let body = Body::from(in_stream);
        spawn(async move {
            while n > 0 {
                let mut m = RealtimeMetrics::default();
                let m_local = collect_local_metrics(types, &opts).await;
                m.merge(m_local);

                if !by_host {
                    m.by_host = HashMap::new();
                }
                if !by_disk {
                    m.by_disk = HashMap::new();
                }

                m.finally = n <= 1;

                // todo write resp
                match serde_json::to_vec(&m) {
                    Ok(re) => {
                        let _ = tx.send(Ok(Bytes::from(re))).await;
                    }
                    Err(e) => {
                        error!("MetricsHandler: json encode failed, err: {:?}", e);
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

        Ok(S3Response::new((StatusCode::OK, body)))
    }
}
