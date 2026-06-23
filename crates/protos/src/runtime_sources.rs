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

use rustfs_io_metrics::internode_metrics::global_internode_metrics;
use rustfs_tls_runtime::{GlobalPublishedOutboundTlsState, load_global_outbound_tls_state, record_tls_consumer_stale_generation};
use std::time::Duration;

const PROTOS_GRPC_CHANNEL_TLS_CONSUMER: &str = "protos_grpc_channel";

pub(crate) async fn outbound_tls_state() -> GlobalPublishedOutboundTlsState {
    load_global_outbound_tls_state().await
}

pub(crate) fn record_stale_grpc_channel_tls_generation() {
    record_tls_consumer_stale_generation(PROTOS_GRPC_CHANNEL_TLS_CONSUMER);
}

pub(crate) fn record_grpc_dial_result(duration: Duration, success: bool) {
    global_internode_metrics().record_dial_result(duration, success);
}
