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

//! Internode bucket engine calls.
//!
//! One signed, body-bound NodeService RPC (`EngineCall`) carries every
//! engine message between nodes. Operations named `framework-*` are answered
//! by the engine registry: the engines a node implements (the fleet gate for
//! binding), whether its engine holds data for a bucket, and purging that
//! data (bucket delete). Any other operation goes to the bound engine's
//! `handle_peer_call` after the bucket incarnation and engine are checked.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::future::join_all;
use rustfs_bucket_engine::{
    EngineError, EngineResult, FRAMEWORK_DESCRIBE_ENGINES, FRAMEWORK_HOLDS_DATA, FRAMEWORK_OPERATION_PREFIX, FRAMEWORK_PURGE,
    OperationName,
};
use rustfs_protos::proto_gen::node_service::{EngineCallRequest, EngineCallResponse};

use super::storage_api::get_global_notification_sys;
use super::{BoundBucket, EngineRegistry, EngineRoute};

/// Deadline for framework calls (capability gate, holds-data, purge).
pub(crate) const FRAMEWORK_CALL_TIMEOUT: Duration = Duration::from_secs(10);

/// An engine call as received from a peer.
#[derive(Debug, Clone)]
pub(crate) struct PeerCall {
    pub bucket: String,
    /// Zero for calls that are not about a bucket.
    pub incarnation: u128,
    pub engine: String,
    pub operation: String,
    pub body: Bytes,
}

impl PeerCall {
    pub(crate) fn from_request(request: EngineCallRequest) -> EngineResult<Self> {
        let incarnation = match request.bucket_incarnation_id.len() {
            0 => 0,
            16 => u128::from_be_bytes(
                request.bucket_incarnation_id[..]
                    .try_into()
                    .map_err(|_| EngineError::InvalidRequest("bucket incarnation must be 16 bytes".to_owned()))?,
            ),
            _ => return Err(EngineError::InvalidRequest("bucket incarnation must be 16 bytes".to_owned())),
        };
        Ok(Self {
            bucket: request.bucket,
            incarnation,
            engine: request.engine,
            operation: request.operation,
            body: request.body,
        })
    }

    fn into_request(self) -> EngineCallRequest {
        EngineCallRequest {
            bucket: self.bucket,
            bucket_incarnation_id: if self.incarnation == 0 {
                Bytes::new()
            } else {
                Bytes::copy_from_slice(&self.incarnation.to_be_bytes())
            },
            engine: self.engine,
            operation: self.operation,
            body: self.body,
        }
    }

    fn for_bucket(bound: &BoundBucket, operation: &str) -> Self {
        Self {
            bucket: bound.context.bucket.clone(),
            incarnation: bound.context.incarnation,
            engine: bound.context.binding.engine.as_str().to_owned(),
            operation: operation.to_owned(),
            body: Bytes::new(),
        }
    }
}

pub(crate) fn response(result: EngineResult<Bytes>) -> EngineCallResponse {
    match result {
        Ok(body) => EngineCallResponse {
            success: true,
            body,
            error_info: None,
            error_kind: None,
        },
        Err(err) => EngineCallResponse {
            success: false,
            body: Bytes::new(),
            error_info: Some(err.wire_message()),
            error_kind: Some(err.kind().to_owned()),
        },
    }
}

fn result(response: EngineCallResponse) -> EngineResult<Bytes> {
    if response.success {
        return Ok(response.body);
    }
    Err(EngineError::from_wire(
        response.error_kind.as_deref().unwrap_or("internal"),
        response.error_info.unwrap_or_default(),
    ))
}

/// The engine this node attached for the call's bucket, checked against the
/// caller's incarnation and engine so a call never reaches another
/// incarnation's data.
fn bound_for_call(registry: &EngineRegistry, call: &PeerCall) -> EngineResult<Arc<BoundBucket>> {
    match registry.route(&call.bucket) {
        EngineRoute::Engine(bound)
            if bound.context.incarnation == call.incarnation && bound.context.binding.engine.as_str() == call.engine =>
        {
            Ok(bound)
        }
        EngineRoute::Engine(_) => Err(EngineError::Unavailable(format!(
            "bucket {} is bound to another incarnation or engine on this node",
            call.bucket
        ))),
        EngineRoute::Unavailable { reason, .. } => Err(EngineError::Unavailable(reason)),
        EngineRoute::Object => Err(EngineError::Unavailable(format!(
            "bucket {} is not bound to an engine on this node",
            call.bucket
        ))),
    }
}

fn json<T: serde::Serialize>(value: &T) -> EngineResult<Bytes> {
    serde_json::to_vec(value)
        .map(Bytes::from)
        .map_err(|err| EngineError::Internal(err.to_string()))
}

/// Serves an engine call from a peer on this node.
pub(crate) async fn serve(registry: &EngineRegistry, call: PeerCall) -> EngineResult<Bytes> {
    match call.operation.as_str() {
        FRAMEWORK_DESCRIBE_ENGINES => json(&registry.engine_names()),
        FRAMEWORK_HOLDS_DATA => {
            let bound = bound_for_call(registry, &call)?;
            json(&bound.engine.holds_data(&bound.context).await?)
        }
        FRAMEWORK_PURGE => {
            let bound = bound_for_call(registry, &call)?;
            bound.engine.purge(&bound.context).await?;
            registry.remove_engine_dirs(&bound.context).await?;
            Ok(Bytes::new())
        }
        operation if operation.starts_with(FRAMEWORK_OPERATION_PREFIX) => {
            Err(EngineError::InvalidRequest(format!("unknown framework operation {operation}")))
        }
        operation => {
            let operation =
                OperationName::new(operation).map_err(|err| EngineError::InvalidRequest(format!("invalid operation: {err}")))?;
            let bound = bound_for_call(registry, &call)?;
            bound.engine.handle_peer_call(&bound.context, &operation, call.body).await
        }
    }
}

/// One peer's answer to a fan-out call.
pub(crate) struct PeerReply {
    pub peer: String,
    pub result: EngineResult<Bytes>,
}

/// Sends `call` to every other node. A configured peer without a client
/// answers `Unavailable`.
pub(crate) async fn call_all_peers(call: PeerCall, timeout: Duration) -> Vec<PeerReply> {
    let Some(notification_sys) = get_global_notification_sys() else {
        // Single-node deployments have no notification system peers.
        return Vec::new();
    };
    join_all(notification_sys.peer_clients.iter().map(|client| {
        let call = call.clone();
        async move {
            let Some(client) = client else {
                return PeerReply {
                    peer: "unknown".to_owned(),
                    result: Err(EngineError::Unavailable("configured peer has no client".to_owned())),
                };
            };
            let peer = client.host.to_string();
            let result = match client.engine_call(call.into_request(), timeout).await {
                Ok(response) => result(response),
                Err(err) => Err(EngineError::Unavailable(format!("peer {peer}: {err}"))),
            };
            PeerReply { peer, result }
        }
    }))
    .await
}

/// Sends `call` to the peer whose identity is `node` (its `host:port`) and
/// returns its answer. An unknown or unreachable peer answers `Unavailable`.
#[cfg(feature = "bucket-engines")]
pub(crate) async fn call_peer(node: &str, call: PeerCall, timeout: Duration) -> EngineResult<Bytes> {
    let notification_sys = get_global_notification_sys()
        .ok_or_else(|| EngineError::Unavailable(format!("peer {node} is unreachable: no peer clients")))?;
    let client = notification_sys
        .peer_clients
        .iter()
        .flatten()
        .find(|client| client.host.to_string() == node)
        .ok_or_else(|| EngineError::Unavailable(format!("peer {node} is not configured")))?;
    match client.engine_call(call.into_request(), timeout).await {
        Ok(response) => result(response),
        Err(err) => Err(EngineError::Unavailable(format!("peer {node}: {err}"))),
    }
}

/// Refuses a binding unless every peer reports the engine. A peer that does
/// not answer, or an older peer without engine calls, cannot serve the
/// bucket's requests and would see an empty object bucket instead.
pub(crate) async fn require_engine_on_every_node(engine: &str) -> EngineResult<()> {
    let call = PeerCall {
        bucket: String::new(),
        incarnation: 0,
        engine: engine.to_owned(),
        operation: FRAMEWORK_DESCRIBE_ENGINES.to_owned(),
        body: Bytes::new(),
    };
    for reply in call_all_peers(call, FRAMEWORK_CALL_TIMEOUT).await {
        let names: Vec<String> = match reply.result {
            Ok(body) => serde_json::from_slice(&body)
                .map_err(|err| EngineError::Unavailable(format!("peer {} sent an invalid engine list: {err}", reply.peer)))?,
            Err(err) => {
                return Err(EngineError::Unavailable(format!(
                    "peer {} cannot confirm engine {engine}: {err}",
                    reply.peer
                )));
            }
        };
        if !names.iter().any(|name| name == engine) {
            return Err(EngineError::Unavailable(format!(
                "peer {} does not implement engine {engine}",
                reply.peer
            )));
        }
    }
    Ok(())
}

/// Whether any peer's engine holds data for the bucket. A peer that cannot
/// answer makes the answer unknown, reported as `Unavailable`.
pub(crate) async fn any_peer_holds_data(bound: &BoundBucket) -> EngineResult<bool> {
    let mut holds = false;
    for reply in call_all_peers(PeerCall::for_bucket(bound, FRAMEWORK_HOLDS_DATA), FRAMEWORK_CALL_TIMEOUT).await {
        let body = reply
            .result
            .map_err(|err| EngineError::Unavailable(format!("peer {} cannot report engine data: {err}", reply.peer)))?;
        holds |= serde_json::from_slice::<bool>(&body)
            .map_err(|err| EngineError::Unavailable(format!("peer {} sent an invalid answer: {err}", reply.peer)))?;
    }
    Ok(holds)
}

/// Purges the bucket's engine data on every peer; returns the peers that
/// failed (their files are left for the engine to collect).
pub(crate) async fn purge_on_peers(bound: &BoundBucket) -> Vec<PeerReply> {
    call_all_peers(PeerCall::for_bucket(bound, FRAMEWORK_PURGE), FRAMEWORK_CALL_TIMEOUT)
        .await
        .into_iter()
        .filter(|reply| reply.result.is_err())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucket_engine::memory::{MEMORY_ENGINE, MemoryEngine};
    use rustfs_bucket_engine::ObjectWriteMeta;

    async fn registry_with_bucket() -> (Arc<EngineRegistry>, Arc<BoundBucket>) {
        let registry = Arc::new(EngineRegistry::new());
        assert!(registry.register_engine(Arc::new(MemoryEngine::new())));
        registry
            .publish("kv", Some((br#"{"engine":"memory","config_version":1}"#, 7)))
            .expect("binding changed")
            .await
            .expect("reconcile task");
        let EngineRoute::Engine(bound) = registry.route("kv") else {
            panic!("bucket should be attached");
        };
        (registry, bound)
    }

    fn call(operation: &str, incarnation: u128) -> PeerCall {
        PeerCall {
            bucket: "kv".to_owned(),
            incarnation,
            engine: MEMORY_ENGINE.to_owned(),
            operation: operation.to_owned(),
            body: Bytes::new(),
        }
    }

    #[tokio::test]
    async fn framework_operations_answer_from_the_registry() {
        let (registry, bound) = registry_with_bucket().await;

        let names = serve(&registry, call(FRAMEWORK_DESCRIBE_ENGINES, 0)).await.expect("describe");
        assert_eq!(serde_json::from_slice::<Vec<String>>(&names).expect("json"), vec![MEMORY_ENGINE]);

        let holds = serve(&registry, call(FRAMEWORK_HOLDS_DATA, 7)).await.expect("holds data");
        assert!(!serde_json::from_slice::<bool>(&holds).expect("json"));
        bound
            .engine
            .put_object(&bound.context, "page", Bytes::from_static(b"x"), ObjectWriteMeta::default())
            .await
            .expect("put");
        let holds = serve(&registry, call(FRAMEWORK_HOLDS_DATA, 7)).await.expect("holds data");
        assert!(serde_json::from_slice::<bool>(&holds).expect("json"));

        serve(&registry, call(FRAMEWORK_PURGE, 7)).await.expect("purge");
        assert!(!bound.engine.holds_data(&bound.context).await.expect("holds data"));

        let unknown = serve(&registry, call("framework-reboot", 7))
            .await
            .expect_err("unknown framework op");
        assert_eq!(unknown.kind(), "invalid-request");
    }

    #[tokio::test]
    async fn calls_for_another_incarnation_engine_or_bucket_are_refused() {
        let (registry, _) = registry_with_bucket().await;

        let stale = serve(&registry, call(FRAMEWORK_PURGE, 8))
            .await
            .expect_err("other incarnation");
        assert_eq!(stale.kind(), "unavailable");

        let mut other_engine = call(FRAMEWORK_HOLDS_DATA, 7);
        other_engine.engine = "kv-cache".to_owned();
        assert_eq!(serve(&registry, other_engine).await.expect_err("other engine").kind(), "unavailable");

        let mut unbound = call(FRAMEWORK_HOLDS_DATA, 7);
        unbound.bucket = "plain".to_owned();
        assert_eq!(serve(&registry, unbound).await.expect_err("unbound bucket").kind(), "unavailable");

        // Engine operations reach the engine, which has no peer operations.
        let engine_op = serve(&registry, call("batch-read", 7)).await.expect_err("no peer operations");
        assert_eq!(engine_op.kind(), "unknown-operation");
        let bad_name = serve(&registry, call("Batch Read", 7)).await.expect_err("invalid name");
        assert_eq!(bad_name.kind(), "invalid-request");
    }

    #[test]
    fn requests_and_responses_round_trip() {
        let original = call(FRAMEWORK_PURGE, 0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);
        let request = original.clone().into_request();
        assert_eq!(request.bucket_incarnation_id.len(), 16);
        let decoded = PeerCall::from_request(request).expect("decode");
        assert_eq!(decoded.incarnation, original.incarnation);
        assert_eq!(decoded.operation, FRAMEWORK_PURGE);

        let bucketless = call(FRAMEWORK_DESCRIBE_ENGINES, 0).into_request();
        assert!(bucketless.bucket_incarnation_id.is_empty());
        assert_eq!(PeerCall::from_request(bucketless).expect("decode").incarnation, 0);

        let mut short = call(FRAMEWORK_PURGE, 7).into_request();
        short.bucket_incarnation_id = Bytes::from_static(&[1, 2, 3]);
        assert_eq!(PeerCall::from_request(short).expect_err("short id").kind(), "invalid-request");

        assert_eq!(result(response(Ok(Bytes::from_static(b"ok")))).expect("ok"), Bytes::from_static(b"ok"));
        let error = result(response(Err(EngineError::StalePlacement { client: 1, current: 2 }))).expect_err("error");
        assert!(matches!(error, EngineError::StalePlacement { client: 1, current: 2 }));
    }
}
