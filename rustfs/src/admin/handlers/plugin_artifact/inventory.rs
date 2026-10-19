// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::{AttemptIdentity, owner_for_resource, request_body, request_store, require_builtin_authorization};
use crate::admin::{
    auth::AdminResourceScope,
    router::Operation,
    storage_api::{ArtifactAttemptInventoryFrame, ECStore, access::spawn_traced},
};
use bytes::Bytes;
use futures::{Stream, StreamExt as _};
use http::{HeaderMap, HeaderValue, StatusCode};
use matchit::Params;
use rustfs_policy::policy::action::AdminAction;
use s3s::{
    Body, S3Request, S3Response, S3Result, StdError, s3_error,
    stream::{ByteStream, DynByteStream},
};
use serde::{Deserialize, Serialize};
use std::{
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{sync::mpsc, time::Instant};
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

const DEADLINE: Duration = Duration::from_secs(30);
const MAX_LINE: usize = 1024 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InventoryRequest {
    version: u32,
    invocation_id: Uuid,
    target: AttemptIdentity,
}

impl InventoryRequest {
    fn validate(&self) -> S3Result<()> {
        self.target.validate()?;
        if self.version != 1
            || self.invocation_id.is_nil()
            || self.target.attempt_prefix.starts_with('/')
            || self.target.attempt_prefix[..self.target.attempt_prefix.len() - 1]
                .split('/')
                .any(|part| part.is_empty() || part == "." || part == "..")
        {
            return Err(s3_error!(InvalidRequest, "invalid artifact inventory request"));
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct InventoryFrame<'a> {
    version: u32,
    invocation_id: Uuid,
    target: &'a AttemptIdentity,
    sequence: u64,
    payload: Payload<'a>,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Payload<'a> {
    Observation {
        disk_id: Uuid,
        key: &'a str,
        version_id: Uuid,
        delete_marker: bool,
    },
    Complete {
        members: u64,
        observations: u64,
    },
}

struct InventoryStream(ReceiverStream<Result<Bytes, StdError>>);
impl Stream for InventoryStream {
    type Item = Result<Bytes, StdError>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::into_inner(self).0.poll_next_unpin(cx)
    }
}
impl ByteStream for InventoryStream {}

pub(super) struct Inventory;
#[async_trait::async_trait]
impl Operation for Inventory {
    async fn call(&self, mut req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let deadline = Instant::now() + DEADLINE;
        let (store, input) = tokio::time::timeout_at(deadline, async {
            require_builtin_authorization().await?;
            let input: InventoryRequest = request_body(&mut req).await?;
            input.validate()?;
            let store = request_store(&req)?;
            owner_for_resource(
                &req,
                AdminAction::InventoryPluginArtifactAttemptAction,
                AdminResourceScope::bucket_object(&input.target.bucket, &input.target.attempt_prefix),
            )
            .await?;
            Ok::<_, s3s::S3Error>((store, input))
        })
        .await
        .map_err(|_| s3_error!(ServiceUnavailable, "artifact inventory unavailable"))??;
        let (output, receiver) = mpsc::channel(2);
        let stream: DynByteStream = Box::pin(InventoryStream(ReceiverStream::new(receiver)));
        spawn_traced(produce(store, input, deadline, output));
        let mut headers = HeaderMap::new();
        headers.insert(http::header::CONTENT_TYPE, HeaderValue::from_static("application/x-ndjson"));
        headers.insert(http::header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        Ok(S3Response::with_headers((StatusCode::OK, Body::from(stream)), headers))
    }
}

fn incomplete(output: &mpsc::Sender<Result<Bytes, StdError>>) {
    // Never put internal storage errors or physical paths on the HTTP wire.
    let _ = output.try_send(Err(Box::new(io::Error::other("artifact inventory incomplete"))));
}

async fn send(
    output: &mpsc::Sender<Result<Bytes, StdError>>,
    input: &InventoryRequest,
    sequence: &mut u64,
    payload: Payload<'_>,
    deadline: Instant,
) -> bool {
    let Some(next) = sequence.checked_add(1) else {
        return false;
    };
    let frame = InventoryFrame {
        version: 1,
        invocation_id: input.invocation_id,
        target: &input.target,
        sequence: *sequence,
        payload,
    };
    let Ok(mut bytes) = serde_json::to_vec(&frame) else {
        return false;
    };
    bytes.push(b'\n');
    if bytes.len() > MAX_LINE {
        return false;
    }
    if Instant::now() >= deadline {
        return false;
    }
    let delivered = tokio::select! {
        biased;
        _ = tokio::time::sleep_until(deadline) => false,
        result = output.send(Ok(Bytes::from(bytes))) => result.is_ok(),
    };
    if !delivered {
        return false;
    }
    *sequence = next;
    true
}

async fn produce(store: Arc<ECStore>, input: InventoryRequest, deadline: Instant, output: mpsc::Sender<Result<Bytes, StdError>>) {
    let (sender, mut frames) = mpsc::channel(2);
    let inventory = store.inventory_artifact_attempt(
        &input.target.bucket,
        input.target.bucket_incarnation,
        &input.target.attempt_prefix,
        deadline,
        sender,
    );
    tokio::pin!(inventory);
    let mut sequence = 0;
    let mut observations = 0u64;
    let mut complete = None;
    let mut done = false;
    let mut ended = false;
    loop {
        tokio::select! {
            biased;
            _ = output.closed() => return,
            _ = tokio::time::sleep_until(deadline) => { incomplete(&output); return; },
            result = &mut inventory, if !done => {
                if result.is_err() { incomplete(&output); return; }
                done = true;
            }
            frame = frames.recv(), if !ended => match frame {
                Some(ArtifactAttemptInventoryFrame::Observations { endpoint: _, disk_id, versions }) if complete.is_none() => {
                    for version in versions {
                        let Some(next) = observations.checked_add(1) else { incomplete(&output); return; };
                        if !send(&output, &input, &mut sequence, Payload::Observation { disk_id, key: &version.key, version_id: version.version_id, delete_marker: version.delete_marker }, deadline).await {
                            incomplete(&output); return;
                        }
                        observations = next;
                    }
                }
                Some(ArtifactAttemptInventoryFrame::Complete { members, observations: total }) if complete.is_none() && total == observations && members > 0 => {
                    let Ok(members) = u64::try_from(members) else { incomplete(&output); return; };
                    complete = Some((members, total));
                }
                Some(_) => { incomplete(&output); return; }
                None => ended = true,
            }
        }
        // A received Complete is only staging: I2 can still fail after sending it.
        // Both the final result and clean input EOF are necessary before exposure.
        if done && ended {
            if let Some((members, observations)) = complete {
                if !send(&output, &input, &mut sequence, Payload::Complete { members, observations }, deadline).await {
                    incomplete(&output);
                }
            } else {
                incomplete(&output);
            }
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AttemptIdentity, InventoryRequest, MAX_LINE, Payload, send};
    use serde_json::json;
    use tokio::{sync::mpsc, time::Instant};
    use uuid::Uuid;

    fn input() -> InventoryRequest {
        InventoryRequest {
            version: 1,
            invocation_id: Uuid::new_v4(),
            target: AttemptIdentity {
                bucket: "artifacts".to_owned(),
                bucket_incarnation: Uuid::new_v4(),
                attempt_prefix: format!("outputs/61/{}/{}/", Uuid::new_v4(), Uuid::new_v4()),
            },
        }
    }

    #[test]
    fn artifact_inventory_request_rejects_ambiguous_scope() {
        let valid = input();
        valid.validate().expect("valid inventory request");
        let body = json!({"version":1,"invocation_id":valid.invocation_id,"target":valid.target});
        for field in [
            "version",
            "invocation_id",
            "bucket_incarnation",
            "attempt_prefix",
            "unknown",
            "target_unknown",
        ] {
            let mut bad = body.clone();
            match field {
                "version" => bad[field] = json!(2),
                "invocation_id" => bad[field] = json!(Uuid::nil()),
                "bucket_incarnation" => bad["target"][field] = json!(Uuid::nil()),
                "attempt_prefix" => bad["target"][field] = json!("outputs/../attempt/"),
                "target_unknown" => bad["target"]["unknown"] = json!(true),
                _ => bad[field] = json!(true),
            }
            let rejected = serde_json::from_value::<InventoryRequest>(bad)
                .map(|value| value.validate().is_err())
                .unwrap_or(true);
            assert!(rejected, "strict inventory request {field}");
        }
    }

    #[tokio::test]
    async fn artifact_inventory_http_encoded_line_and_elapsed_deadline() {
        let input = input();
        let (output, mut receiver) = mpsc::channel(2);
        let mut sequence = 0;
        let key = "\"".repeat(MAX_LINE / 2);
        assert!(key.len() < MAX_LINE);
        assert!(
            !send(
                &output,
                &input,
                &mut sequence,
                Payload::Observation {
                    disk_id: Uuid::new_v4(),
                    key: &key,
                    version_id: Uuid::new_v4(),
                    delete_marker: false
                },
                Instant::now() + std::time::Duration::from_secs(1)
            )
            .await
        );
        assert!(receiver.try_recv().is_err(), "actual encoded JSON bound must precede enqueue");
        assert!(
            !send(
                &output,
                &input,
                &mut sequence,
                Payload::Complete {
                    members: 4,
                    observations: 0
                },
                Instant::now()
            )
            .await
        );
        assert!(
            receiver.try_recv().is_err(),
            "expired budget cannot enqueue Complete even into an empty channel"
        );
        assert_eq!(sequence, 0);
    }
}
