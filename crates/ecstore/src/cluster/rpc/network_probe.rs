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

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use rustfs_protos::ChannelClass;
use rustfs_protos::models::{PingBody, PingBodyBuilder};
use rustfs_protos::proto_gen::node_service::{PingRequest, PingResponse};
use thiserror::Error;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tonic::{Code, Request};

use crate::cluster::rpc::client::{TonicInterceptor, gen_tonic_signature_interceptor, node_service_time_out_client_for_class};
use crate::layout::endpoints::EndpointServerPools;

pub const MAX_NETWORK_PROBE_BYTES: u64 = 1_048_576;
pub const MAX_NETWORK_PROBE_DURATION: Duration = Duration::from_secs(30);

const PING_PROTOCOL_VERSION: u64 = 1;
const LATENCY_PAYLOAD: &[u8] = b"network-probe-latency-v1";
const EXPECTED_RESPONSE_PAYLOAD: &[u8] = b"hello, caller";
const DIAGNOSTIC_BYTES_PER_SECOND: u64 = 1_048_576;
const DIAGNOSTIC_CHUNK_BYTES: u64 = 16_384;

#[derive(Debug)]
struct DiagnosticPacing {
    started: Instant,
    deadline: Instant,
    charged_bytes: AtomicU64,
}

impl DiagnosticPacing {
    fn reserve(&self, bytes: u64) -> Result<Instant, NetworkPeerProbeError> {
        if bytes == 0 {
            return Err(NetworkPeerProbeError::LimitExceeded);
        }
        // Reservation is never refunded: a failed or dropped RPC may have sent some payload.
        let previous = self
            .charged_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |charged| {
                charged.checked_add(bytes).filter(|total| *total <= MAX_NETWORK_PROBE_BYTES)
            })
            .map_err(|_| NetworkPeerProbeError::LimitExceeded)?;
        let charged = previous.checked_add(bytes).ok_or(NetworkPeerProbeError::LimitExceeded)?;
        let millis = u128::from(charged)
            .checked_mul(1_000)
            .ok_or(NetworkPeerProbeError::LimitExceeded)?
            .div_ceil(u128::from(DIAGNOSTIC_BYTES_PER_SECOND));
        self.started
            .checked_add(Duration::from_millis(
                u64::try_from(millis).map_err(|_| NetworkPeerProbeError::LimitExceeded)?,
            ))
            .ok_or(NetworkPeerProbeError::LimitExceeded)
    }
}

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
    diagnostic_pacing: Option<Arc<DiagnosticPacing>>,
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
        Self {
            targets,
            diagnostic_pacing: None,
        }
    }

    /// Opt in one diagnostic run; clones share its charged payload and absolute deadline.
    /// Default/admin probes retain their existing unpaced behavior.
    pub fn with_diagnostic_pacing(mut self, started: Instant, duration: Duration) -> Result<Self, NetworkPeerProbeError> {
        if self.diagnostic_pacing.is_some() || duration.is_zero() || duration > MAX_NETWORK_PROBE_DURATION {
            return Err(NetworkPeerProbeError::LimitExceeded);
        }
        let deadline = started.checked_add(duration).ok_or(NetworkPeerProbeError::LimitExceeded)?;
        self.diagnostic_pacing = Some(Arc::new(DiagnosticPacing {
            started,
            deadline,
            charged_bytes: AtomicU64::new(0),
        }));
        Ok(self)
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
        let probe = probe_peer(address, traffic_bytes, self.diagnostic_pacing.as_deref(), cancel);
        if let Some(pacing) = &self.diagnostic_pacing {
            let deadline = Instant::now()
                .checked_add(max_duration)
                .ok_or(NetworkPeerProbeError::LimitExceeded)?
                .min(pacing.deadline);
            return tokio::select! {
                biased;
                () = cancel.cancelled() => Err(NetworkPeerProbeError::Cancelled),
                () = tokio::time::sleep_until(deadline) => Err(NetworkPeerProbeError::TimedOut),
                result = probe => result,
            };
        }
        tokio::select! {
            () = cancel.cancelled() => Err(NetworkPeerProbeError::Cancelled),
            result = tokio::time::timeout(max_duration, probe) => {
                result.map_err(|_| NetworkPeerProbeError::TimedOut)?
            }
        }
    }
}

async fn probe_peer(
    address: &str,
    traffic_bytes: u64,
    pacing: Option<&DiagnosticPacing>,
    cancel: &CancellationToken,
) -> Result<NetworkPeerProbeMeasurement, NetworkPeerProbeError> {
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

    let mut bulk = client(address, ChannelClass::Bulk).await?;
    send_payload(traffic_bytes, pacing, cancel, &mut bulk, |bulk, payload| {
        Box::pin(async move {
            let response = bulk
                .ping(Request::new(ping_request(&payload)))
                .await
                .map_err(map_status)?
                .into_inner();
            validate_ping_response(&response)
        })
    })
    .await?;

    Ok(NetworkPeerProbeMeasurement {
        transferred_bytes: traffic_bytes,
        duration: started.elapsed(),
        latency,
    })
}

type PayloadSendFuture<'a> = Pin<Box<dyn Future<Output = Result<(), NetworkPeerProbeError>> + Send + 'a>>;

async fn send_payload<T, F>(
    traffic_bytes: u64,
    pacing: Option<&DiagnosticPacing>,
    cancel: &CancellationToken,
    sender: &mut T,
    mut send: F,
) -> Result<(), NetworkPeerProbeError>
where
    T: Send,
    F: for<'a> FnMut(&'a mut T, Vec<u8>) -> PayloadSendFuture<'a> + Send,
{
    let mut remaining = traffic_bytes;
    while remaining > 0 {
        let bytes = if pacing.is_some() {
            remaining.min(DIAGNOSTIC_CHUNK_BYTES)
        } else {
            remaining
        };
        if let Some(pacing) = pacing {
            if cancel.is_cancelled() {
                return Err(NetworkPeerProbeError::Cancelled);
            }
            if Instant::now() >= pacing.deadline {
                return Err(NetworkPeerProbeError::TimedOut);
            }
            let due = pacing.reserve(bytes)?;
            tokio::select! {
                biased;
                () = cancel.cancelled() => return Err(NetworkPeerProbeError::Cancelled),
                () = tokio::time::sleep_until(pacing.deadline) => return Err(NetworkPeerProbeError::TimedOut),
                () = tokio::time::sleep_until(due) => {},
            }
        }
        let payload_len = usize::try_from(bytes).map_err(|_| NetworkPeerProbeError::LimitExceeded)?;
        let payload = vec![0x5a; payload_len];
        if let Some(pacing) = pacing {
            // Check before polling the send future, including a cancellation/deadline tie.
            tokio::select! {
                biased;
                () = cancel.cancelled() => return Err(NetworkPeerProbeError::Cancelled),
                () = tokio::time::sleep_until(pacing.deadline) => return Err(NetworkPeerProbeError::TimedOut),
                result = send(sender, payload) => result?,
            }
        } else {
            send(sender, payload).await?;
        }
        remaining -= bytes;
    }
    Ok(())
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

    fn pacing(duration: Duration) -> Arc<DiagnosticPacing> {
        NetworkPeerProbeClient::from_endpoint_pools(&topology())
            .with_diagnostic_pacing(Instant::now(), duration)
            .expect("bounded diagnostic pacing window")
            .diagnostic_pacing
            .expect("opt-in pacing is present")
    }

    #[test]
    fn pacing_uses_exact_millisecond_ceiling_at_n_and_n_plus_one() {
        for duration in [Duration::ZERO, MAX_NETWORK_PROBE_DURATION + Duration::from_nanos(1)] {
            assert!(matches!(
                NetworkPeerProbeClient::from_endpoint_pools(&topology()).with_diagnostic_pacing(Instant::now(), duration),
                Err(NetworkPeerProbeError::LimitExceeded)
            ));
        }
        for (bytes, millis) in [(1, 1), (1_047_527, 999), (1_047_528, 1_000), (1_048_576, 1_000)] {
            let pacing = pacing(Duration::from_secs(2));
            assert_eq!(
                pacing.reserve(bytes).expect("bounded reservation") - pacing.started,
                Duration::from_millis(millis)
            );
        }
        for bytes in [0, MAX_NETWORK_PROBE_BYTES + 1, u64::MAX] {
            let pacing = pacing(Duration::from_secs(2));
            assert_eq!(pacing.reserve(bytes), Err(NetworkPeerProbeError::LimitExceeded));
            assert_eq!(pacing.charged_bytes.load(Ordering::Acquire), 0);
        }
    }

    #[test]
    fn cloned_diagnostic_clients_share_one_atomic_budget_without_reset() {
        let client = NetworkPeerProbeClient::from_endpoint_pools(&topology())
            .with_diagnostic_pacing(Instant::now(), Duration::from_secs(2))
            .expect("pacing");
        let cloned = client.clone();
        let first = client.diagnostic_pacing.as_ref().expect("first budget");
        let second = cloned.diagnostic_pacing.as_ref().expect("cloned budget");
        assert!(Arc::ptr_eq(first, second));
        let mut due = std::thread::scope(|scope| {
            let first = scope.spawn(|| first.reserve(MAX_NETWORK_PROBE_BYTES / 2));
            let second = scope.spawn(|| second.reserve(MAX_NETWORK_PROBE_BYTES / 2));
            [
                first.join().expect("first reservation thread").expect("first half"),
                second.join().expect("second reservation thread").expect("second half"),
            ]
        });
        due.sort();
        assert_eq!(
            due,
            [
                first.started + Duration::from_millis(500),
                first.started + Duration::from_secs(1)
            ]
        );
        assert_eq!(first.reserve(1), Err(NetworkPeerProbeError::LimitExceeded));
        assert_eq!(first.charged_bytes.load(Ordering::Acquire), MAX_NETWORK_PROBE_BYTES);
        assert!(matches!(
            cloned.with_diagnostic_pacing(Instant::now(), Duration::from_secs(2)),
            Err(NetworkPeerProbeError::LimitExceeded)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn failed_peer_payload_stays_charged_before_the_next_peer_send() {
        let pacing = pacing(Duration::from_secs(2));
        let cancel = CancellationToken::new();
        let mut observed_partial_bytes = 0;
        let failed = send_payload(32_768, Some(&pacing), &cancel, &mut observed_partial_bytes, |partial, payload| {
            Box::pin(async move {
                assert_eq!(payload.len(), 16_384);
                // Model failure after a partial write: the sender cannot safely refund the remainder.
                *partial += 8_192;
                Err(NetworkPeerProbeError::Unreachable)
            })
        })
        .await;
        assert_eq!(failed, Err(NetworkPeerProbeError::Unreachable));
        assert_eq!(observed_partial_bytes, 8_192);
        assert_eq!(pacing.charged_bytes.load(Ordering::Acquire), 16_384);
        let started = pacing.started;
        let mut second_bytes = 0;
        send_payload(16_384, Some(&pacing), &cancel, &mut second_bytes, move |bytes, payload| {
            Box::pin(async move {
                assert!(started.elapsed() >= Duration::from_millis(32));
                *bytes += payload.len();
                Ok(())
            })
        })
        .await
        .expect("second peer uses remaining shared budget");
        assert_eq!(second_bytes, 16_384);
        assert_eq!(pacing.charged_bytes.load(Ordering::Acquire), 32_768);
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_and_deadline_take_priority_over_an_earned_send() {
        for cancelled in [false, true] {
            let pacing = pacing(Duration::from_millis(16));
            let cancel = CancellationToken::new();
            let mut sends = 0;
            {
                let mut sending =
                    std::pin::pin!(send_payload(8_192, Some(&pacing), &cancel, &mut sends, |sends, _| Box::pin(async move {
                        *sends += 1;
                        Ok(())
                    })));
                assert!(futures::poll!(&mut sending).is_pending());
                tokio::time::advance(Duration::from_millis(16)).await;
                if cancelled {
                    cancel.cancel();
                }
                let expected = if cancelled {
                    NetworkPeerProbeError::Cancelled
                } else {
                    NetworkPeerProbeError::TimedOut
                };
                assert_eq!(sending.await, Err(expected));
            }
            assert_eq!(sends, 0);
            assert_eq!(pacing.charged_bytes.load(Ordering::Acquire), 8_192);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_during_a_payload_rpc_retains_its_attempted_charge() {
        let pacing = pacing(Duration::from_secs(2));
        let cancel = CancellationToken::new();
        let mut sends = 0;
        {
            let mut sending = std::pin::pin!(send_payload(16_384, Some(&pacing), &cancel, &mut sends, |sends, _| Box::pin(
                async move {
                    *sends += 1;
                    std::future::pending().await
                }
            )));
            assert!(futures::poll!(&mut sending).is_pending());
            tokio::time::advance(Duration::from_millis(16)).await;
            assert!(futures::poll!(&mut sending).is_pending());
            cancel.cancel();
            assert_eq!(sending.await, Err(NetworkPeerProbeError::Cancelled));
        }
        assert_eq!(sends, 1);
        assert_eq!(pacing.charged_bytes.load(Ordering::Acquire), 16_384);
    }

    #[tokio::test(start_paused = true)]
    async fn default_payload_send_remains_one_unpaced_rpc() {
        let started = Instant::now();
        let mut sends = 0;
        send_payload(32_768, None, &CancellationToken::new(), &mut sends, move |sends, payload| {
            Box::pin(async move {
                assert_eq!(started.elapsed(), Duration::ZERO);
                assert_eq!(payload.len(), 32_768);
                *sends += 1;
                Ok(())
            })
        })
        .await
        .expect("legacy admin send");
        assert_eq!(sends, 1);
        assert!(
            NetworkPeerProbeClient::from_endpoint_pools(&topology())
                .diagnostic_pacing
                .is_none()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn diagnostic_payload_is_charged_before_each_actual_send_poll() {
        let started = tokio::time::Instant::now();
        let pacing = DiagnosticPacing {
            started,
            deadline: started + Duration::from_secs(2),
            charged_bytes: AtomicU64::new(0),
        };
        let mut attempted = 0_u64;
        send_payload(
            32_768,
            Some(&pacing),
            &CancellationToken::new(),
            &mut attempted,
            move |attempted, payload| {
                Box::pin(async move {
                    *attempted += u64::try_from(payload.len()).expect("bounded payload length");
                    let available = u128::from(MAX_NETWORK_PROBE_BYTES) * started.elapsed().as_millis() / 1_000;
                    assert!(
                        u128::from(*attempted) <= available,
                        "send boundary emitted {attempted} bytes with only {available} bytes earned"
                    );
                    Ok(())
                })
            },
        )
        .await
        .expect("bounded diagnostic payload");
        assert_eq!(attempted, 32_768);
    }

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
    fn probe_future_remains_send() {
        fn require_send<T: Send>(_: T) {}
        let client = NetworkPeerProbeClient::from_endpoint_pools(&topology());
        let cancel = CancellationToken::new();
        require_send(client.probe("peer-1", 1, Duration::from_secs(1), &cancel));
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
            body: valid.body,
        };
        assert_eq!(validate_ping_response(&wrong_version), Err(NetworkPeerProbeError::ProtocolFailure));
        let malformed = PingResponse {
            version: PING_PROTOCOL_VERSION,
            body: Bytes::from_static(b"not-flatbuffers"),
        };
        assert_eq!(validate_ping_response(&malformed), Err(NetworkPeerProbeError::ProtocolFailure));
    }
}
