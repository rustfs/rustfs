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

use super::profile::{authorize_profile_request, profile_not_implemented_response};
use crate::admin::auth::validate_admin_request;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::storage_api::access::spawn_traced;
use crate::auth::{check_key_valid, get_session_token};
use crate::server::{ADMIN_PREFIX, RemoteAddr};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use http::{HeaderMap, HeaderValue};
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_madmin::service_commands::ServiceTraceOpts;
use rustfs_policy::policy::action::{Action, AdminAction};
use s3s::header::CONTENT_TYPE;
use s3s::stream::{ByteStream, DynByteStream};
use s3s::{Body, S3Request, S3Response, S3Result, StdError, s3_error};
use serde::Serialize;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::error;

#[derive(Serialize)]
struct ProfileStatus {
    enabled: &'static str,
    status: &'static str,
    platform: &'static str,
    message: &'static str,
}

pub fn register_profiling_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/debug/pprof/profile").as_str(),
        AdminOperation(&ProfileHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/debug/pprof/status").as_str(),
        AdminOperation(&ProfileStatusHandler {}),
    )?;

    // MinIO-compatible profiling / trace family (#606).
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/profiling/start").as_str(),
        AdminOperation(&ProfilingStartHandler {}),
    )?;
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/profiling/download").as_str(),
        AdminOperation(&ProfilingDownloadHandler {}),
    )?;
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/profile").as_str(),
        AdminOperation(&ProfileControlHandler {}),
    )?;
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/trace").as_str(),
        AdminOperation(&TraceHandler {}),
    )?;

    Ok(())
}

/// Authorize a request against a single admin action (profiling or trace).
async fn authorize_action(req: &S3Request<Body>, action: AdminAction) -> S3Result<()> {
    let Some(input_cred) = req.credentials.as_ref() else {
        return Err(s3_error!(AccessDenied, "Signature is required"));
    };
    let (cred, owner) =
        check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;
    let remote_addr = req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0));
    validate_admin_request(&req.headers, &cred, owner, false, vec![Action::AdminAction(action)], remote_addr).await
}

pub struct ProfileHandler {}

#[async_trait::async_trait]
impl Operation for ProfileHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_profile_request(&req).await?;

        let requested_url = req.uri.to_string();
        crate::profiling::log_cpu_pprof_dump_skipped();
        Ok(profile_not_implemented_response(format!(
            "{}; requested_url={requested_url}",
            crate::profiling::local_cpu_pprof_unsupported_message()
        )))
    }
}

pub struct ProfileStatusHandler {}

#[async_trait::async_trait]
impl Operation for ProfileStatusHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_profile_request(&req).await?;

        let status = ProfileStatus {
            enabled: "false",
            status: "not_supported",
            platform: std::env::consts::OS,
            message: crate::profiling::LOCAL_CPU_PPROF_UNSUPPORTED_SUMMARY,
        };

        match serde_json::to_string(&status) {
            Ok(json) => {
                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
                Ok(S3Response::with_headers((StatusCode::OK, Body::from(json)), headers))
            }
            Err(e) => {
                error!("Failed to serialize status: {}", e);
                Ok(S3Response::new((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Body::from("Failed to serialize status".to_string()),
                )))
            }
        }
    }
}

/// `POST /v3/profiling/start` — begin a profiling session.
///
/// RustFS builds with the mimalloc allocator and ships no in-process pprof/CPU
/// sampler (see `crate::profiling`): profiling is exported out-of-process via
/// Pyroscope. We therefore honor the MinIO request shape but return
/// `501 Not Implemented` with a clear reason rather than pretending to have
/// started a capture that will never produce data.
pub struct ProfilingStartHandler {}

#[async_trait::async_trait]
impl Operation for ProfilingStartHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_action(&req, AdminAction::ProfilingAdminAction).await?;
        crate::profiling::log_cpu_pprof_dump_skipped();
        Ok(profile_not_implemented_response(format!(
            "profiling start is not supported: {}",
            crate::profiling::local_cpu_pprof_unsupported_message()
        )))
    }
}

/// `GET /v3/profiling/download` — download the captured profile archive.
///
/// No capture is ever produced (see `ProfilingStartHandler`), so there is
/// nothing to download.
pub struct ProfilingDownloadHandler {}

#[async_trait::async_trait]
impl Operation for ProfilingDownloadHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_action(&req, AdminAction::ProfilingAdminAction).await?;
        Ok(profile_not_implemented_response(format!(
            "profiling download is not supported: {}",
            crate::profiling::local_cpu_pprof_unsupported_message()
        )))
    }
}

/// `POST /v3/profile` — start/stop profiling in one call (legacy MinIO shape).
pub struct ProfileControlHandler {}

#[async_trait::async_trait]
impl Operation for ProfileControlHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_action(&req, AdminAction::ProfilingAdminAction).await?;
        crate::profiling::log_cpu_pprof_dump_skipped();
        Ok(profile_not_implemented_response(format!(
            "in-process profiling is not supported: {}",
            crate::profiling::local_cpu_pprof_unsupported_message()
        )))
    }
}

struct TraceStream {
    inner: ReceiverStream<Result<Bytes, StdError>>,
}

impl Stream for TraceStream {
    type Item = Result<Bytes, StdError>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::into_inner(self).inner.poll_next_unpin(cx)
    }
}

impl ByteStream for TraceStream {}

/// `GET /v3/trace` — stream real-time server trace events.
///
/// RustFS emits diagnostics through the `tracing` pipeline but does not expose
/// an in-process subscriber that can fan trace events out to an admin client
/// (there is no request-trace broadcast channel). Rather than return an opaque
/// `501` — which would make `mc admin trace` fail to connect — this honors the
/// streaming NDJSON contract: it validates the requested trace filters, opens
/// the stream, emits a single capability record explaining that live tracing is
/// not wired, then holds the connection open with keep-alives. No fabricated
/// trace records are ever sent.
pub struct TraceHandler {}

#[async_trait::async_trait]
impl Operation for TraceHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_action(&req, AdminAction::TraceAdminAction).await?;

        // Validate the trace options so malformed filters are rejected up front,
        // matching MinIO's behavior.
        let mut opts = ServiceTraceOpts::default();
        opts.parse_params(&req.uri)
            .map_err(|_| s3_error!(InvalidRequest, "invalid trace parameters"))?;

        let node_name = sysinfo::System::host_name().unwrap_or_else(|| "rustfs".to_string());
        let (tx, rx) = mpsc::channel::<Result<Bytes, StdError>>(8);

        spawn_traced(async move {
            let notice = serde_json::json!({
                "nodename": node_name,
                "funcname": "admin.Trace",
                "msg": "RustFS does not expose an in-process trace-event subscriber; live tracing is not yet available",
                "err": "trace_streaming_unsupported",
            });
            if let Ok(mut encoded) = serde_json::to_vec(&notice) {
                encoded.push(b'\n');
                if tx.send(Ok(Bytes::from(encoded))).await.is_err() {
                    return;
                }
            }

            let mut ticker = tokio::time::interval(Duration::from_secs(15));
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = tx.closed() => break,
                    _ = ticker.tick() => {
                        if tx.send(Ok(Bytes::from_static(b" \n"))).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });

        let stream: DynByteStream = Box::pin(TraceStream {
            inner: ReceiverStream::new(rx),
        });
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/x-ndjson"));
        Ok(S3Response::with_headers((StatusCode::OK, Body::from(stream)), headers))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ProfileControlHandler, ProfileHandler, ProfileStatusHandler, ProfilingDownloadHandler, ProfilingStartHandler,
        TraceHandler,
    };
    use crate::admin::router::Operation;
    use http::{Extensions, HeaderMap, Uri};
    use hyper::Method;
    use matchit::Params;
    use s3s::{Body, S3ErrorCode, S3Request};

    fn build_profile_request(uri: &'static str) -> S3Request<Body> {
        S3Request {
            input: Body::empty(),
            method: Method::GET,
            uri: Uri::from_static(uri),
            headers: HeaderMap::new(),
            extensions: Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    #[tokio::test]
    async fn profile_handler_rejects_missing_credentials() {
        let result = ProfileHandler {}
            .call(build_profile_request("/rustfs/admin/debug/pprof/profile?format=protobuf"), Params::new())
            .await;
        let err = match result {
            Ok(_) => panic!("profile handler must reject unauthenticated requests"),
            Err(err) => err,
        };

        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
        assert_eq!(err.message(), Some("Signature is required"));
    }

    #[tokio::test]
    async fn profile_status_handler_rejects_missing_credentials() {
        let result = ProfileStatusHandler {}
            .call(build_profile_request("/rustfs/admin/debug/pprof/status"), Params::new())
            .await;
        let err = match result {
            Ok(_) => panic!("profile status handler must reject unauthenticated requests"),
            Err(err) => err,
        };

        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
        assert_eq!(err.message(), Some("Signature is required"));
    }

    #[tokio::test]
    async fn profiling_start_rejects_missing_credentials() {
        let err = ProfilingStartHandler {}
            .call(build_profile_request("/rustfs/admin/v3/profiling/start"), Params::new())
            .await
            .expect_err("profiling start must reject anonymous requests");
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
    }

    #[tokio::test]
    async fn profiling_download_rejects_missing_credentials() {
        let err = ProfilingDownloadHandler {}
            .call(build_profile_request("/rustfs/admin/v3/profiling/download"), Params::new())
            .await
            .expect_err("profiling download must reject anonymous requests");
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
    }

    #[tokio::test]
    async fn profile_control_rejects_missing_credentials() {
        let err = ProfileControlHandler {}
            .call(build_profile_request("/rustfs/admin/v3/profile"), Params::new())
            .await
            .expect_err("profile control must reject anonymous requests");
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
    }

    #[tokio::test]
    async fn trace_handler_rejects_missing_credentials() {
        let err = TraceHandler {}
            .call(build_profile_request("/rustfs/admin/v3/trace?s3=true"), Params::new())
            .await
            .expect_err("trace must reject anonymous requests");
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
    }
}
