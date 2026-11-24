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

//! The loopback target's authenticated gateway and scripted wire responses.

use super::*;
use rustfs_gateway::{
    Authorizer, AuthzRequest, BoxFuture, CodecError, Credentials, Decision, ErrorCode, Handler, HandlerDeadlineConfig,
    HandlerError, HandlerErrorContext, HandlerResult, InputAuthzRequest, InputDecisions, LegacyRustfsFacts, LegacyRustfsRefusal,
    Operation as GatewayOperation, PlaintextCustomerKeyAck, RegionSet, Req, RequestBodyDeadlineConfig, RequestContext, Resp,
    ServiceBuilder, ServiceConfig, SigV4Authenticator, SseConfig, StaticCredentials,
};

#[derive(Clone)]
pub(super) struct RequestState(Arc<State>);

struct State {
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    sequence: RequestSequence,
    admitted: OnceLock<http::Extensions>,
    // A scripted target can deliberately replace a typed header such as ETag on the wire.
    response_headers: OnceLock<(u16, HeaderMap)>,
}

impl RequestState {
    pub(super) fn new<B>(request: &Request<B>) -> Self {
        Self(Arc::new(State {
            method: request.method().clone(),
            uri: request.uri().clone(),
            headers: request.headers().clone(),
            sequence: RequestSequence::default(),
            admitted: OnceLock::new(),
            response_headers: OnceLock::new(),
        }))
    }

    pub(super) fn sequence(&self) -> Option<u64> {
        self.0.sequence.0.get().copied()
    }

    fn save_headers(&self, status: u16, headers: HeaderMap) {
        if !headers.is_empty() {
            let _ = self.0.response_headers.set((status, headers));
        }
    }

    pub(super) fn apply_response<B>(&self, response: &mut Response<B>) {
        let Some((status, headers)) = self.0.response_headers.get() else { return };
        if *status == response.status().as_u16() {
            response.headers_mut().extend(headers.clone());
        } else if let Some(disconnect) = headers.get(DISCONNECT_HEADER) {
            // Dropping a partially consumed upload can also produce a gateway body error. The
            // scripted transport disconnect was already reached and must still close the socket.
            response.headers_mut().insert(DISCONNECT_HEADER, disconnect.clone());
        }
    }

    fn error(&self, error: S3Error) -> HandlerError {
        let parts = error.into_parts();
        let status = parts
            .status
            .or_else(|| parts.code.status_code())
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let code = ErrorCode::custom(parts.code.as_str().to_owned(), status);
        let message = parts
            .message
            .map(|message| message.into_owned())
            .or_else(|| parts.code.default_message().map(str::to_owned));
        match LegacyRustfsRefusal::new(code, message, LegacyRustfsFacts::default()) {
            Ok(refusal) => {
                self.save_headers(status.as_u16(), parts.headers.unwrap_or_default());
                HandlerErrorContext::legacy_rustfs(refusal).into()
            }
            Err(_) => HandlerError::internal_error("invalid fake target error"),
        }
    }
}

fn credential_decision(request: &AuthzRequest<'_>) -> Decision {
    if request
        .identity
        .is_some_and(|identity| identity.access_key_id() == FAKE_ACCESS_KEY)
    {
        Decision::Allow
    } else {
        Decision::Deny
    }
}

impl Authorizer for FaultAccess {
    fn authorize_route<'a>(&'a self, _context: &'a RequestContext<'a>, request: &'a AuthzRequest<'a>) -> BoxFuture<'a, Decision> {
        Box::pin(async move { credential_decision(request) })
    }

    fn authorize_route_result<'a>(
        &'a self,
        context: &'a RequestContext<'a>,
        request: &'a AuthzRequest<'a>,
    ) -> BoxFuture<'a, Result<Decision, HandlerError>> {
        Box::pin(async move {
            if credential_decision(request) != Decision::Allow {
                return Err(HandlerError::new(ErrorCode::ACCESS_DENIED, "valid fixture credentials are required"));
            }
            let state = context.server_extensions().get::<RequestState>().ok_or_else(missing_state)?;
            // The gateway can ask several permissions for a single operation. Dispatch consumes
            // one fault and journals one request, after the signature has been verified.
            if state.0.admitted.get().is_none() {
                let extensions = self
                    .admit(
                        &state.0.method,
                        &state.0.uri,
                        &state.0.headers,
                        operation_from_s3_name(request.operation),
                        &state.0.sequence,
                    )
                    .await
                    .map_err(|error| state.error(error))?;
                state
                    .0
                    .admitted
                    .set(extensions)
                    .map_err(|_| HandlerError::internal_error("fixture request admitted twice"))?;
            }
            Ok(Decision::Allow)
        })
    }

    fn authorize_input<'a>(
        &'a self,
        _context: &'a RequestContext<'a>,
        request: &'a InputAuthzRequest<'a>,
    ) -> BoxFuture<'a, InputDecisions> {
        Box::pin(async move { request.decide_all(credential_decision(request.route()), credential_decision) })
    }
}

fn missing_state() -> HandlerError {
    HandlerError::internal_error("missing fake target request state")
}

fn target_request<O: GatewayOperation>(request: Req<O>) -> Result<(RequestState, TargetRequest<O::Input>), HandlerError> {
    let state = request
        .context()
        .transport_extensions()
        .get::<RequestState>()
        .cloned()
        .ok_or_else(missing_state)?;
    let extensions = state.0.admitted.get().cloned().ok_or_else(missing_state)?;
    let target = TargetRequest {
        input: request.into_input(),
        headers: state.0.headers.clone(),
        extensions,
    };
    Ok((state, target))
}

fn target_response<O: GatewayOperation>(state: &RequestState, response: S3Result<TargetResponse<O::Output>>) -> HandlerResult<O> {
    let TargetResponse { output, headers, status } = response.map_err(|error| state.error(error))?;
    let response = match status {
        Some(status) => Resp::<O>::with_status(output, status.as_u16()),
        None => Resp::<O>::new(output),
    };
    state.save_headers(response.status(), headers);
    Ok(response)
}

macro_rules! fixture_operations {
    ($($operation:ident => $method:ident),+ $(,)?) => {
        $(impl Handler<dto::$operation> for FakeBackend {
            async fn call(&self, request: Req<dto::$operation>) -> HandlerResult<dto::$operation> {
                let (state, request) = target_request(request)?;
                target_response::<dto::$operation>(&state, self.$method(request).await)
            }
        })+

        fn register_operations(builder: ServiceBuilder, backend: &Arc<FakeBackend>) -> ServiceBuilder {
            builder$(.register::<dto::$operation, _>(Arc::clone(backend)))+
        }
    }
}

fixture_operations! {
    HeadBucket => head_bucket,
    GetBucketVersioning => get_bucket_versioning,
    GetObjectLockConfiguration => get_object_lock_configuration,
    ListObjectsV2 => list_objects_v2,
    ListObjectVersions => list_object_versions,
    PutObject => put_object,
    GetObject => get_object,
    HeadObject => head_object,
    GetObjectTagging => get_object_tagging,
    PutObjectTagging => put_object_tagging,
    DeleteObjectTagging => delete_object_tagging,
    GetObjectRetention => get_object_retention,
    PutObjectRetention => put_object_retention,
    GetObjectLegalHold => get_object_legal_hold,
    PutObjectLegalHold => put_object_legal_hold,
    DeleteObject => delete_object,
    CreateMultipartUpload => create_multipart_upload,
    UploadPart => upload_part,
    AbortMultipartUpload => abort_multipart_upload,
}

impl Handler<dto::CompleteMultipartUpload> for FakeBackend {
    async fn call(&self, request: Req<dto::CompleteMultipartUpload>) -> HandlerResult<dto::CompleteMultipartUpload> {
        let (state, request) = target_request(request)?;
        target_response::<dto::CompleteMultipartUpload>(&state, self.complete_multipart_upload(request).await)
    }
}

pub(super) fn build(backend: FakeBackend, access: FaultAccess) -> Result<S3Service, BoxError> {
    let credentials = Credentials::new(FAKE_ACCESS_KEY, FAKE_SECRET_KEY.as_bytes())?;
    let authenticator =
        SigV4Authenticator::new(Arc::new(StaticCredentials::new().with(credentials)), RegionSet::new(["us-east-1"])?);
    // The target bounds the body reader and the entire request itself. A scripted delay or
    // slow drain must reach that bound without the gateway timing it out earlier.
    let config = ServiceConfig::new(MAX_BUFFERED_BODY_BYTES as u64)
        .with_handler_deadlines(HandlerDeadlineConfig::new(MAX_REQUEST_DURATION, MAX_REQUEST_DURATION)?)
        .with_request_body_deadlines(
            RequestBodyDeadlineConfig::S3
                .without_idle_deadlines()
                .without_throughput_floor(),
        );
    let (builder, _) = ServiceBuilder::new()
        .rustfs_profile()
        .authenticate_before_rejecting_unframed_upload_trailers()
        .config(config);
    let backend = Arc::new(backend);
    let service = register_operations(builder, &backend)
        .sse_config(SseConfig::allowing_customer_keys_over_plaintext(
            PlaintextCustomerKeyAck::i_understand_customer_keys_will_be_sent_in_the_clear(),
        ))
        .authenticator(authenticator.rustfs_profile())
        .authorizer(access)
        .register_with_decode_error_mapping::<dto::CompleteMultipartUpload, _>(
            backend,
            CodecError::malformed_xml("the body omits a member the schema requires").about("PartNumber"),
            CodecError::new(ErrorCode::INVALID_PART, "part number is missing"),
        )
        .build()?;
    Ok(service)
}
