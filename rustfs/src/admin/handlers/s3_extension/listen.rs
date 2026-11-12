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

//! The live notification stream: `GET /?events=…` (every bucket) and
//! `GET /{bucket}?events=…`, answered with an open `text/event-stream` that
//! carries matching events from this node and its peers.
//!
//! Responsible for: the route handler, query validation and filtering, and the
//! stream task, which re-authorizes the subscriber while it runs and ends when
//! the client goes away or loses access.
//!
//! Not responsible for: recognising or admitting the request (`super`), nor
//! producing events (`rustfs_notify`).

#[cfg(test)]
mod tests;

use crate::admin::operation::{AdminRequest, AdminResponse, Operation};
use crate::admin::runtime_sources::{
    app_context_from_envelope, current_notification_system, current_object_store_handle, object_store_from_envelope,
};
use crate::admin::storage_api::access::{authorize_owned_request, spawn_traced};
use crate::admin::storage_api::contract::bucket::{BucketOperations, BucketOptions};
use crate::admin::storage_api::runtime::PeerRestClient;
use crate::admin::utils::{query_value_exact, query_values_exact};
use crate::auth::get_session_token;
use crate::error::ApiError;
use bytes::Bytes;
use http::header::{CACHE_CONTROL, CONTENT_TYPE};
use http::{HeaderMap, HeaderValue, StatusCode, Uri};
use matchit::Params;
use rustfs_notify::{Event as NotificationEvent, notification_system};
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_s3_types::{Body, EventName, S3Error, S3ErrorCode, S3Result, StdError};
use tokio::sync::{broadcast, mpsc};
use tokio::time::Duration;
use tokio_stream::wrappers::ReceiverStream;
use tracing::warn;

const LOG_COMPONENT_ADMIN: &str = "admin";
const LOG_SUBSYSTEM_LIVE_EVENTS: &str = "live_events";
const EVENT_ADMIN_ROUTER_STATE: &str = "admin_router_state";

/// The listen route; the bucket parameter is absent on the service-level route.
pub(super) struct ListenNotification;

#[async_trait::async_trait]
impl Operation for ListenNotification {
    async fn call(&self, mut req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let bucket = params.get("bucket");
        let action = Action::S3Action(if bucket.is_some() {
            S3Action::ListenBucketNotificationAction
        } else {
            S3Action::ListenNotificationAction
        });
        super::authorize(&mut req, bucket, action).await?;
        validate_listen_notification_query(req.uri())?;
        if let Some(bucket) = bucket {
            let Some(store) = current_object_store_handle() else {
                return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init"));
            };
            store
                .get_bucket_info(bucket, &BucketOptions::default())
                .await
                .map_err(ApiError::from)?;
        }
        let uri = req.uri().clone();
        let authorization = ListenNotificationAuthorization::capture(req, bucket, action).await?;
        build_listen_notification_response(&uri, bucket, authorization)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ListenNotificationFilter {
    bucket: Option<String>,
    event_mask: u64,
    prefix: Option<String>,
    suffix: Option<String>,
}

fn is_valid_filter_rule_value(value: &str) -> bool {
    if value.len() > 1024 || value.contains('\\') {
        return false;
    }
    !value.split('/').any(|segment| segment == "." || segment == "..")
}

fn validate_listen_notification_query(uri: &Uri) -> S3Result<()> {
    let events = query_values_exact(uri, "events");
    if events.is_empty() {
        return Err(S3Error::with_message(S3ErrorCode::InvalidArgument, "events query parameter is required"));
    }

    for event in events {
        EventName::parse(&event)
            .map_err(|_| S3Error::with_message(S3ErrorCode::InvalidArgument, "invalid event in events query parameter"))?;
    }

    let prefixes = query_values_exact(uri, "prefix");
    if prefixes.len() > 1 {
        return Err(S3Error::with_message(
            S3ErrorCode::InvalidArgument,
            "prefix query parameter must not be repeated",
        ));
    }
    if let Some(prefix) = prefixes.first()
        && !is_valid_filter_rule_value(prefix)
    {
        return Err(S3Error::with_message(S3ErrorCode::InvalidArgument, "invalid prefix filter value"));
    }

    let suffixes = query_values_exact(uri, "suffix");
    if suffixes.len() > 1 {
        return Err(S3Error::with_message(
            S3ErrorCode::InvalidArgument,
            "suffix query parameter must not be repeated",
        ));
    }
    if let Some(suffix) = suffixes.first()
        && !is_valid_filter_rule_value(suffix)
    {
        return Err(S3Error::with_message(S3ErrorCode::InvalidArgument, "invalid suffix filter value"));
    }

    let pings = query_values_exact(uri, "ping");
    if pings.len() > 1 {
        return Err(S3Error::with_message(
            S3ErrorCode::InvalidArgument,
            "ping query parameter must not be repeated",
        ));
    }
    if let Some(ping) = pings.first() {
        let ping_interval = ping.parse::<u64>().map_err(|_| {
            S3Error::with_message(S3ErrorCode::InvalidArgument, "ping query parameter must be a positive integer")
        })?;
        if ping_interval == 0 {
            return Err(S3Error::with_message(
                S3ErrorCode::InvalidArgument,
                "ping query parameter must be greater than zero",
            ));
        }
    }

    Ok(())
}

fn parse_listen_notification_filter(uri: &Uri, bucket: Option<&str>) -> S3Result<ListenNotificationFilter> {
    let mut event_mask = 0_u64;
    for event in query_values_exact(uri, "events") {
        event_mask |= EventName::parse(&event)
            .map_err(|_| S3Error::with_message(S3ErrorCode::InvalidArgument, "invalid event in events query parameter"))?
            .mask();
    }

    Ok(ListenNotificationFilter {
        bucket: bucket.map(str::to_string),
        event_mask,
        prefix: query_value_exact(uri, "prefix").filter(|value| !value.is_empty()),
        suffix: query_value_exact(uri, "suffix").filter(|value| !value.is_empty()),
    })
}

struct PeerLiveEventCursor {
    client: PeerRestClient,
    next_sequence: u64,
}

const LISTEN_NOTIFICATION_PEER_BATCH_LIMIT: u32 = 128;

const LISTEN_NOTIFICATION_PEER_POLL_INTERVAL: Duration = Duration::from_millis(250);

fn listen_notification_keepalive_plan(uri: &Uri) -> (Duration, Bytes) {
    if let Some(ping_seconds) = query_value_exact(uri, "ping").and_then(|v| v.parse::<u64>().ok()) {
        return (Duration::from_secs(ping_seconds), Bytes::from_static(b"{\"Records\":[]}\n"));
    }

    (Duration::from_millis(500), Bytes::from_static(b" "))
}

fn event_matches_listen_notification(event: &NotificationEvent, filter: &ListenNotificationFilter) -> bool {
    if let Some(bucket) = &filter.bucket
        && event.s3.bucket.name != *bucket
    {
        return false;
    }

    if filter.event_mask != 0 && event.event_name.mask() & filter.event_mask == 0 {
        return false;
    }

    let object_key = urlencoding::decode(&event.s3.object.key)
        .map(|decoded| decoded.into_owned())
        .unwrap_or_else(|_| event.s3.object.key.clone());

    if let Some(prefix) = &filter.prefix
        && !object_key.starts_with(prefix)
    {
        return false;
    }

    if let Some(suffix) = &filter.suffix
        && !object_key.ends_with(suffix)
    {
        return false;
    }

    true
}

fn serialize_listen_notification_event(event: &NotificationEvent) -> S3Result<Bytes> {
    #[derive(serde::Serialize)]
    struct ListenNotificationEnvelope<'a> {
        #[serde(rename = "Records")]
        records: [&'a NotificationEvent; 1],
    }

    serde_json::to_vec(&ListenNotificationEnvelope { records: [event] })
        .map(|mut payload| {
            payload.push(b'\n');
            Bytes::from(payload)
        })
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("failed to serialize notification event: {e}")))
}

fn list_remote_live_event_peers() -> Vec<PeerLiveEventCursor> {
    current_notification_system()
        .map(|system| {
            system
                .peer_clients
                .iter()
                .flatten()
                .cloned()
                .map(|client| PeerLiveEventCursor {
                    client,
                    next_sequence: 0,
                })
                .collect()
        })
        .unwrap_or_default()
}

fn deserialize_peer_live_events(payload: &[u8]) -> Result<Vec<NotificationEvent>, serde_json::Error> {
    serde_json::from_slice(payload)
}

const LISTEN_AUTH_REFRESH_INTERVAL: Duration = Duration::from_secs(1);

const LISTEN_AUTH_TIMEOUT: Duration = Duration::from_secs(5);

// No Debug implementation: the admitted request retains its signing credentials.
struct ListenNotificationAuthorization {
    request: AdminRequest,
    action: Action,
    bucket_incarnation: Option<(String, uuid::Uuid)>,
    keystone_credentials: Option<rustfs_credentials::Credentials>,
    refresh_at: tokio::time::Instant,
}

impl ListenNotificationAuthorization {
    async fn capture(request: AdminRequest, bucket: Option<&str>, action: Action) -> S3Result<Self> {
        let bucket_incarnation = if let Some(bucket) = bucket {
            let store = object_store_from_envelope(request.envelope())
                .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "Object store is unavailable"))?;
            Some((
                bucket.to_string(),
                store.bucket_incarnation_id_from_disk(bucket).await.map_err(ApiError::from)?,
            ))
        } else {
            None
        };
        Ok(Self {
            request,
            action,
            bucket_incarnation,
            keystone_credentials: rustfs_keystone::KEYSTONE_CREDENTIALS.try_with(Clone::clone).ok().flatten(),
            refresh_at: tokio::time::Instant::now(),
        })
    }

    async fn still_allowed(&mut self) -> bool {
        matches!(tokio::time::timeout(LISTEN_AUTH_TIMEOUT, self.reauthorize()).await, Ok(Ok(())))
    }

    async fn reauthorize(&mut self) -> S3Result<()> {
        let now = tokio::time::Instant::now();
        if now >= self.refresh_at {
            if let Some(credentials) = &self.keystone_credentials {
                let provider =
                    crate::auth_keystone::get_keystone_auth().ok_or_else(|| S3Error::new(S3ErrorCode::AccessDenied))?;
                let token = self
                    .request
                    .headers()
                    .get("x-auth-token")
                    .and_then(|v| v.to_str().ok())
                    .or_else(|| self.request.headers().get("x-storage-token").and_then(|v| v.to_str().ok()))
                    .ok_or_else(|| S3Error::new(S3ErrorCode::AccessDenied))?;
                let current = provider
                    .revalidate_subscription_token(token)
                    .await
                    .map_err(|_| S3Error::new(S3ErrorCode::AccessDenied))?;
                if current.access_key != credentials.access_key {
                    return Err(S3Error::new(S3ErrorCode::AccessDenied));
                }
                self.keystone_credentials = Some(current);
            }
            if let Some((bucket, expected)) = &self.bucket_incarnation {
                let store =
                    object_store_from_envelope(self.request.envelope()).ok_or_else(|| S3Error::new(S3ErrorCode::AccessDenied))?;
                let actual = store.bucket_incarnation_id_from_disk(bucket).await.map_err(ApiError::from)?;
                if actual != *expected {
                    return Err(S3Error::new(S3ErrorCode::AccessDenied));
                }
            }
            self.refresh_at = now + LISTEN_AUTH_REFRESH_INTERVAL;
        }
        if self
            .keystone_credentials
            .as_ref()
            .and_then(|cred| cred.expiration)
            .is_some_and(|expiry| expiry <= time::OffsetDateTime::now_utc())
        {
            return Err(S3Error::new(S3ErrorCode::AccessDenied));
        }
        let context = app_context_from_envelope(self.request.envelope());
        if context.is_none() && self.request.envelope().server_context().is_some() {
            return Err(S3Error::new(S3ErrorCode::AccessDenied));
        }
        let keystone = self.keystone_credentials.clone();
        let access_key = self
            .request
            .credentials()
            .map(|credentials| credentials.access_key().to_owned())
            .or_else(|| keystone.as_ref().map(|credentials| credentials.access_key.clone()))
            .ok_or_else(|| S3Error::new(S3ErrorCode::AccessDenied))?;
        rustfs_keystone::KEYSTONE_CREDENTIALS
            .scope(keystone, async {
                let (cred, is_owner) = crate::auth::check_key_valid_with_context(
                    get_session_token(self.request.uri(), self.request.headers()).unwrap_or_default(),
                    &access_key,
                    context.as_deref(),
                )
                .await?;
                if self.keystone_credentials.is_none()
                    && !self
                        .request
                        .credentials()
                        .is_some_and(|credentials| credentials.secret_key_matches(&cred.secret_key))
                {
                    return Err(S3Error::new(S3ErrorCode::AccessDenied));
                }
                let info = self
                    .request
                    .req_info_mut()
                    .ok_or_else(|| S3Error::new(S3ErrorCode::AccessDenied))?;
                info.cred = Some(cred);
                info.is_owner = is_owner;
                authorize_owned_request(self.request.envelope_mut(), self.action, None).await
            })
            .await
    }

    async fn still_allowed_until_closed(&mut self, tx: &mpsc::Sender<Result<Bytes, StdError>>) -> bool {
        tokio::select! {
            biased;
            _ = tx.closed() => false,
            allowed = self.still_allowed() => allowed,
        }
    }

    async fn send(&mut self, tx: &mpsc::Sender<Result<Bytes, StdError>>, bytes: Bytes) -> bool {
        loop {
            tokio::select! {
                permit = tx.reserve() => {
                    let Ok(permit) = permit else { return false; };
                    if !self.still_allowed_until_closed(tx).await { return false; }
                    permit.send(Ok(bytes));
                    return true;
                }
                _ = tokio::time::sleep(LISTEN_AUTH_REFRESH_INTERVAL) => {
                    if !self.still_allowed_until_closed(tx).await { return false; }
                }
            }
        }
    }
}

async fn fan_in_remote_live_events(
    peers: &mut [PeerLiveEventCursor],
    filter: &ListenNotificationFilter,
    tx: &mpsc::Sender<Result<Bytes, StdError>>,
    authorization: &mut ListenNotificationAuthorization,
) -> bool {
    for peer in peers.iter_mut() {
        loop {
            if !authorization.still_allowed().await {
                return false;
            }
            let batch = match tokio::time::timeout(
                Duration::from_secs(2),
                peer.client
                    .get_live_events(peer.next_sequence, LISTEN_NOTIFICATION_PEER_BATCH_LIMIT),
            )
            .await
            {
                Ok(Ok(batch)) => batch,
                Ok(Err(err)) => {
                    warn!(
                        event = EVENT_ADMIN_ROUTER_STATE,
                        component = LOG_COMPONENT_ADMIN,
                        subsystem = LOG_SUBSYSTEM_LIVE_EVENTS,
                        peer = %peer.client.host,
                        result = "peer_fetch_failed",
                        error = %err,
                        "admin router state"
                    );
                    break;
                }
                Err(_) => {
                    warn!(
                        event = EVENT_ADMIN_ROUTER_STATE,
                        component = LOG_COMPONENT_ADMIN,
                        subsystem = LOG_SUBSYSTEM_LIVE_EVENTS,
                        peer = %peer.client.host,
                        result = "peer_fetch_timeout",
                        error = "timeout",
                        "admin router state"
                    );
                    break;
                }
            };

            peer.next_sequence = batch.next_sequence.max(peer.next_sequence);

            if !batch.events.is_empty() {
                match deserialize_peer_live_events(&batch.events) {
                    Ok(events) => {
                        for event in events {
                            if !event_matches_listen_notification(&event, filter) {
                                continue;
                            }
                            match serialize_listen_notification_event(&event) {
                                Ok(serialized) => {
                                    if !authorization.send(tx, serialized).await {
                                        return false;
                                    }
                                }
                                Err(err) => {
                                    warn!(
                                        event = EVENT_ADMIN_ROUTER_STATE,
                                        component = LOG_COMPONENT_ADMIN,
                                        subsystem = LOG_SUBSYSTEM_LIVE_EVENTS,
                                        source = "remote_peer",
                                        peer = %peer.client.host,
                                        result = "event_serialize_failed",
                                        error = %err,
                                        "admin router state"
                                    );
                                }
                            }
                        }
                    }
                    Err(err) => {
                        warn!(
                            event = EVENT_ADMIN_ROUTER_STATE,
                            component = LOG_COMPONENT_ADMIN,
                            subsystem = LOG_SUBSYSTEM_LIVE_EVENTS,
                            peer = %peer.client.host,
                            result = "peer_decode_failed",
                            error = %err,
                            "admin router state"
                        );
                    }
                }
            }

            if !batch.truncated {
                break;
            }
        }
    }

    true
}

fn build_listen_notification_response(
    uri: &Uri,
    bucket: Option<&str>,
    mut authorization: ListenNotificationAuthorization,
) -> S3Result<AdminResponse> {
    let (interval_duration, payload) = listen_notification_keepalive_plan(uri);
    let filter = parse_listen_notification_filter(uri, bucket)?;
    let mut live_events = notification_system().map(|system| system.subscribe_live_events());
    let mut peer_live_events = list_remote_live_event_peers();

    let (tx, rx) = mpsc::channel(16);
    let body = Body::from_stream(ReceiverStream::new(rx));

    spawn_traced(async move {
        let mut authorization_ticker = tokio::time::interval(LISTEN_AUTH_REFRESH_INTERVAL);
        let mut ticker = tokio::time::interval(interval_duration);
        let mut peer_ticker = tokio::time::interval(LISTEN_NOTIFICATION_PEER_POLL_INTERVAL);
        // Skip the immediate first tick so behavior starts after interval duration.
        ticker.tick().await;
        peer_ticker.tick().await;
        loop {
            if let Some(events_rx) = live_events.as_mut() {
                tokio::select! {
                    _ = tx.closed() => break,
                    _ = authorization_ticker.tick() => {
                        if !authorization.still_allowed_until_closed(&tx).await { break; }
                    },
                    _ = ticker.tick() => {
                        if !authorization.send(&tx, payload.clone()).await {
                            break;
                        }
                    }
                    event = events_rx.recv() => {
                        match event {
                            Ok(event) => {
                                if !event_matches_listen_notification(&event, &filter) {
                                    continue;
                                }
                                match serialize_listen_notification_event(&event) {
                                    Ok(serialized) => {
                                        if !authorization.send(&tx, serialized).await {
                                            break;
                                        }
                                    }
                                    Err(err) => {
                                        warn!(
                                            event = EVENT_ADMIN_ROUTER_STATE,
                                            component = LOG_COMPONENT_ADMIN,
                                            subsystem = LOG_SUBSYSTEM_LIVE_EVENTS,
                                            source = "local_stream",
                                            result = "event_serialize_failed",
                                            error = %err,
                                            "admin router state"
                                        );
                                    }
                                }
                            }
                            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                                warn!(
                                    event = EVENT_ADMIN_ROUTER_STATE,
                                    component = LOG_COMPONENT_ADMIN,
                                    subsystem = LOG_SUBSYSTEM_LIVE_EVENTS,
                                    result = "stream_lagged",
                                    skipped,
                                    "admin router state"
                                );
                            }
                            Err(broadcast::error::RecvError::Closed) => break,
                        }
                    }
                    _ = peer_ticker.tick(), if !peer_live_events.is_empty() => {
                        if !tokio::select! {
                            biased;
                            _ = tx.closed() => false,
                            result = fan_in_remote_live_events(&mut peer_live_events, &filter, &tx, &mut authorization) => result,
                        } {
                            break;
                        }
                    }
                }
            } else {
                tokio::select! {
                    _ = tx.closed() => break,
                    _ = authorization_ticker.tick() => {
                        if !authorization.still_allowed_until_closed(&tx).await { break; }
                    },
                    _ = ticker.tick() => {
                        if !authorization.send(&tx, payload.clone()).await {
                            break;
                        }
                    }
                    _ = peer_ticker.tick(), if !peer_live_events.is_empty() => {
                        if !tokio::select! {
                            biased;
                            _ = tx.closed() => false,
                            result = fan_in_remote_live_events(&mut peer_live_events, &filter, &tx, &mut authorization) => result,
                        } {
                            break;
                        }
                    }
                }
            }
        }
    });

    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    headers.insert("x-accel-buffering", HeaderValue::from_static("no"));
    Ok(AdminResponse::with_headers(StatusCode::OK, body, headers))
}
