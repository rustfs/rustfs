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

//! Bounded, authenticated inter-node network probes.

use std::time::{Duration, Instant};

use bytes::Bytes;
use rustfs_protos::ChannelClass;
use rustfs_protos::models::{PingBody, PingBodyBuilder};
use rustfs_protos::proto_gen::node_service::{PingRequest, PingResponse};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tonic::{Code, Request};

use crate::cluster::rpc::client::{TonicInterceptor, gen_tonic_signature_interceptor, node_service_time_out_client_for_class};
use crate::layout::endpoints::EndpointServerPools;

pub const MAX_NETWORK_PROBE_BYTES: u64 = 1_048_576;
pub const MAX_NETWORK_PROBE_DURATION: Duration = Duration::from_secs(30);

const PING_PROTOCOL_VERSION: u64 = 1;
const LATENCY_PAYLOAD: &[u8] = b"network-probe-latency-v1";
const EXPECTED_RESPONSE_PAYLOAD: &[u8] = b"hello, caller";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NetworkPeerTarget {
    pub alias: String,
    pub address: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NetworkPeerProbeMeasurement {
    pub transferred_bytes: u64,
    pub duration: Duration,
    pub latency: Duration,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum NetworkPeerProbeError {
    #[error("network peer is not part of the current topology")]
    UnknownPeer,
    #[error("network peer probe exceeds its resource limits")]
    LimitExceeded,
    #[error("network peer probe was cancelled")]
    Cancelled,
    #[error("network peer is unreachable")]
    Unreachable,
    #[error("network peer probe timed out")]
    TimedOut,
    #[error("network peer returned an invalid probe response")]
    ProtocolFailure,
}

#[derive(Clone, Debug)]
pub struct NetworkPeerProbeClient {
    targets: Vec<NetworkPeerTarget>,
}

impl NetworkPeerProbeClient {
    pub fn from_endpoint_pools(endpoint_pools: &EndpointServerPools) -> Self {
        let targets = endpoint_pools
            .peer_grid_host_slots_sorted()
            .into_iter()
            .filter_map(|(_, address, is_local)| (!is_local).then_some(address).flatten())
            .enumerate()
            .map(|(index, address)| NetworkPeerTarget {
                alias: format!("peer-{}", index + 1),
                address,
            })
            .collect();
        Self { targets }
    }

    pub fn targets(&self) -> Vec<NetworkPeerTarget> {
        self.targets.clone()
    }

    pub async fn probe(
        &self,
        peer_alias: &str,
        traffic_bytes: u64,
        max_duration: Duration,
        cancel: &CancellationToken,
    ) -> Result<NetworkPeerProbeMeasurement, NetworkPeerProbeError> {
        if traffic_bytes == 0
            || traffic_bytes > MAX_NETWORK_PROBE_BYTES
            || max_duration.is_zero()
            || max_duration > MAX_NETWORK_PROBE_DURATION
        {
            return Err(NetworkPeerProbeError::LimitExceeded);
        }
        if cancel.is_cancelled() {
            return Err(NetworkPeerProbeError::Cancelled);
        }
        let address = self
            .targets
            .iter()
            .find(|target| target.alias == peer_alias)
            .map(|target| target.address.as_str())
            .ok_or(NetworkPeerProbeError::UnknownPeer)?;
        let probe = probe_peer(address, traffic_bytes);
        tokio::select! {
            () = cancel.cancelled() => Err(NetworkPeerProbeError::Cancelled),
            result = tokio::time::timeout(max_duration, probe) => {
                result.map_err(|_| NetworkPeerProbeError::TimedOut)?
            }
        }
    }
}

async fn probe_peer(address: &str, traffic_bytes: u64) -> Result<NetworkPeerProbeMeasurement, NetworkPeerProbeError> {
    let started = Instant::now();
    let mut control = client(address, ChannelClass::Control).await?;
    let latency_started = Instant::now();
    let latency_response = control
        .ping(Request::new(ping_request(LATENCY_PAYLOAD)))
        .await
        .map_err(map_status)?
        .into_inner();
    validate_ping_response(&latency_response)?;
    let latency = latency_started.elapsed();

    let payload_len = usize::try_from(traffic_bytes).map_err(|_| NetworkPeerProbeError::LimitExceeded)?;
    let payload = vec![0x5a; payload_len];
    let mut bulk = client(address, ChannelClass::Bulk).await?;
    let response = bulk
        .ping(Request::new(ping_request(&payload)))
        .await
        .map_err(map_status)?
        .into_inner();
    validate_ping_response(&response)?;

    Ok(NetworkPeerProbeMeasurement {
        transferred_bytes: traffic_bytes,
        duration: started.elapsed(),
        latency,
    })
}

async fn client(
    address: &str,
    class: ChannelClass,
) -> Result<
    rustfs_protos::proto_gen::node_service::node_service_client::NodeServiceClient<
        tonic::service::interceptor::InterceptedService<super::client::AuthenticatedChannel, TonicInterceptor>,
    >,
    NetworkPeerProbeError,
> {
    node_service_time_out_client_for_class(
        &address.to_owned(),
        TonicInterceptor::Signature(gen_tonic_signature_interceptor()),
        class,
    )
    .await
    .map_err(|_| NetworkPeerProbeError::Unreachable)
}

fn ping_request(payload: &[u8]) -> PingRequest {
    let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(payload.len().saturating_add(64));
    let payload = builder.create_vector(payload);
    let mut body = PingBodyBuilder::new(&mut builder);
    body.add_payload(payload);
    let root = body.finish();
    builder.finish(root, None);
    PingRequest {
        version: PING_PROTOCOL_VERSION,
        body: Bytes::copy_from_slice(builder.finished_data()),
    }
}

fn validate_ping_response(response: &PingResponse) -> Result<(), NetworkPeerProbeError> {
    if response.version != PING_PROTOCOL_VERSION {
        return Err(NetworkPeerProbeError::ProtocolFailure);
    }
    let body = flatbuffers::root::<PingBody>(&response.body).map_err(|_| NetworkPeerProbeError::ProtocolFailure)?;
    if body
        .payload()
        .is_none_or(|payload| payload.bytes() != EXPECTED_RESPONSE_PAYLOAD)
    {
        return Err(NetworkPeerProbeError::ProtocolFailure);
    }
    Ok(())
}

fn map_status(status: tonic::Status) -> NetworkPeerProbeError {
    match status.code() {
        Code::Unavailable | Code::Unknown => NetworkPeerProbeError::Unreachable,
        Code::DeadlineExceeded => NetworkPeerProbeError::TimedOut,
        _ => NetworkPeerProbeError::ProtocolFailure,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::endpoint::Endpoint;
    use crate::layout::endpoints::{Endpoints, PoolEndpoints};

    fn endpoint(value: &str, is_local: bool) -> Endpoint {
        let mut endpoint = Endpoint::try_from(value).expect("test endpoint should parse");
        endpoint.is_local = is_local;
        endpoint
    }

    fn topology() -> EndpointServerPools {
        EndpointServerPools::from(vec![PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 4,
            endpoints: Endpoints::from(vec![
                endpoint("http://node-b:9000/data", false),
                endpoint("http://node-a:9000/data", true),
                endpoint("http://node-c:9000/data", false),
                endpoint("http://node-b:9000/other", false),
            ]),
            cmd_line: String::new(),
            platform: String::new(),
        }])
    }

    #[test]
    fn targets_are_unique_sorted_remote_nodes_with_stable_aliases() {
        let client = NetworkPeerProbeClient::from_endpoint_pools(&topology());
        assert_eq!(
            client.targets(),
            vec![
                NetworkPeerTarget {
                    alias: "peer-1".to_owned(),
                    address: "http://node-b:9000".to_owned(),
                },
                NetworkPeerTarget {
                    alias: "peer-2".to_owned(),
                    address: "http://node-c:9000".to_owned(),
                },
            ]
        );
    }

    #[test]
    fn peer_aliases_remain_in_numeric_order_past_nine_peers() {
        let endpoints: Vec<Endpoint> = (1..=12)
            .map(|index| endpoint(&format!("http://node-{index:02}:9000/data"), false))
            .collect();
        let topology = EndpointServerPools::from(vec![PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 12,
            endpoints: Endpoints::from(endpoints),
            cmd_line: String::new(),
            platform: String::new(),
        }]);

        let aliases = NetworkPeerProbeClient::from_endpoint_pools(&topology)
            .targets()
            .into_iter()
            .map(|target| target.alias)
            .collect::<Vec<_>>();

        assert_eq!(aliases, (1..=12).map(|index| format!("peer-{index}")).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn invalid_limits_and_unknown_peer_fail_before_dial() {
        let client = NetworkPeerProbeClient::from_endpoint_pools(&topology());
        let cancel = CancellationToken::new();
        assert_eq!(
            client
                .probe("peer-1", MAX_NETWORK_PROBE_BYTES + 1, Duration::from_secs(1), &cancel)
                .await,
            Err(NetworkPeerProbeError::LimitExceeded)
        );
        assert_eq!(
            client.probe("peer-3", 1, Duration::from_secs(1), &cancel).await,
            Err(NetworkPeerProbeError::UnknownPeer)
        );
        cancel.cancel();
        assert_eq!(
            client.probe("peer-1", 1, Duration::from_secs(1), &cancel).await,
            Err(NetworkPeerProbeError::Cancelled)
        );
    }

    #[test]
    fn ping_body_carries_exact_requested_payload_and_response_is_validated() {
        let request = ping_request(&vec![0x5a; 4096]);
        let body = flatbuffers::root::<PingBody>(&request.body).expect("probe ping should be valid");
        assert_eq!(body.payload().expect("probe payload").len(), 4096);

        let valid = PingResponse {
            version: PING_PROTOCOL_VERSION,
            body: ping_request(EXPECTED_RESPONSE_PAYLOAD).body,
        };
        assert_eq!(validate_ping_response(&valid), Ok(()));

        let wrong_version = PingResponse {
            version: 2,
            ..valid.clone()
        };
        assert_eq!(validate_ping_response(&wrong_version), Err(NetworkPeerProbeError::ProtocolFailure));
        let malformed = PingResponse {
            version: PING_PROTOCOL_VERSION,
            body: Bytes::from_static(b"not-flatbuffers"),
        };
        assert_eq!(validate_ping_response(&malformed), Err(NetworkPeerProbeError::ProtocolFailure));
    }
}
