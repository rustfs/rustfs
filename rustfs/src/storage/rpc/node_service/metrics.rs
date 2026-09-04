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

use super::NodeService;
use crate::storage::rpc::encode_msgpack_map;
use crate::storage::storage_api::rpc_consumer::node_service::{
    CollectMetricsOpts, MetricType, TierDailyStatsWire, collect_local_metrics, get_global_transition_state,
};
use bytes::Bytes;
use rmp_serde::Deserializer;
use rustfs_protos::proto_gen::node_service::*;
use serde::Deserialize;
use std::collections::HashMap;
use std::io::Cursor;
use tonic::{Request, Response, Status};
use tracing::error;

impl NodeService {
    pub(super) async fn handle_get_metrics(
        &self,
        request: Request<GetMetricsRequest>,
    ) -> Result<Response<GetMetricsResponse>, Status> {
        let request = request.into_inner();

        // Deserialize metric_type with error handling
        let mut buf_t = Deserializer::new(Cursor::new(request.metric_type));
        let t: MetricType = match Deserialize::deserialize(&mut buf_t) {
            Ok(t) => t,
            Err(err) => {
                error!(error = %err, "failed to deserialize metric_type");
                return Ok(Response::new(GetMetricsResponse {
                    success: false,
                    realtime_metrics: Bytes::new(),
                    error_info: Some(format!("Invalid metric_type: {err}")),
                }));
            }
        };

        // Deserialize opts with error handling
        let mut buf_o = Deserializer::new(Cursor::new(request.opts));
        let opts: CollectMetricsOpts = match Deserialize::deserialize(&mut buf_o) {
            Ok(opts) => opts,
            Err(err) => {
                error!(error = %err, "failed to deserialize opts");
                return Ok(Response::new(GetMetricsResponse {
                    success: false,
                    realtime_metrics: Bytes::new(),
                    error_info: Some(format!("Invalid opts: {err}")),
                }));
            }
        };

        let info = collect_local_metrics(t, &opts).await;
        match encode_msgpack_map(&info) {
            Ok(buf) => Ok(Response::new(GetMetricsResponse {
                success: true,
                realtime_metrics: buf.into(),
                error_info: None,
            })),
            Err(err) => Ok(Response::new(GetMetricsResponse {
                success: false,
                realtime_metrics: Bytes::new(),
                error_info: Some(err.to_string()),
            })),
        }
    }

    /// This node's own rolling-day transition counters.
    ///
    /// Only this node's completions are reported; the caller sums the rings of
    /// every member, so answering with anything wider would double count.
    pub(super) async fn handle_tier_daily_stats(
        &self,
        _request: Request<TierDailyStatsRequest>,
    ) -> Result<Response<TierDailyStatsResponse>, Status> {
        let stats = get_global_transition_state()
            .get_daily_all_tier_stats()
            .into_iter()
            .map(|(tier, stats)| (tier, stats.to_wire()))
            .collect::<HashMap<String, TierDailyStatsWire>>();

        match encode_msgpack_map(&stats) {
            Ok(buf) => Ok(Response::new(TierDailyStatsResponse {
                success: true,
                tier_daily_stats: buf.into(),
                error_info: None,
            })),
            Err(err) => {
                error!(error = %err, "failed to serialize tier daily stats");
                Ok(Response::new(TierDailyStatsResponse {
                    success: false,
                    tier_daily_stats: Bytes::new(),
                    error_info: Some(err.to_string()),
                }))
            }
        }
    }
}
