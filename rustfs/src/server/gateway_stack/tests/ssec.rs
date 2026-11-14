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
//! Deployment SSE-C policy through the production builder and outer transport layer.
//! The handler observes key handoff; this does not exercise object storage.

use super::*;
use crate::server::gateway_handlers::test_support::signed_request_with_headers;
use md5::Digest;
use rustfs_gateway::{Handler, HandlerResult, Req, Resp, dto};
use std::sync::atomic::{AtomicUsize, Ordering};
use subtle::ConstantTimeEq;
use tower::{Layer, ServiceExt};

const ALGORITHM: &str = "x-amz-server-side-encryption-customer-algorithm";
const KEY: &str = "x-amz-server-side-encryption-customer-key";
const MD5: &str = "x-amz-server-side-encryption-customer-key-md5";

struct KeyProbe(AtomicUsize);

impl Handler<dto::GetObject> for KeyProbe {
    async fn call(&self, request: Req<dto::GetObject>) -> HandlerResult<dto::GetObject> {
        let value = request.context().headers().iter_raw().find(|(name, _)| name.as_str() == KEY);
        let matches = value.is_some_and(|(_, value)| {
            base64_simd::STANDARD
                .decode_to_vec(value.as_bytes())
                .is_ok_and(|key| bool::from(key.as_slice().ct_eq(&[7u8; 32])))
        });
        assert!(matches, "the handler must receive the original customer key");
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Resp::new(dto::GetObjectOutput::default()))
    }
}

#[test]
fn ssec_deployment_policy_and_key_handoff() {
    use crate::server::ssec_transport::{ENV_SSE_C_REQUIRE_TLS, SsecTransportLayer};
    // Keep the environment override and every async request inside temp_env's serialized scope.
    for setting in [None, Some("false"), Some("true")] {
        temp_env::with_var(ENV_SSE_C_REQUIRE_TLS, setting, || {
            tokio::runtime::Runtime::new().expect("test runtime").block_on(async {
                let probe = Arc::new(KeyProbe(AtomicUsize::new(0)));
                let builder = GatewayStack::configured_builder(
                    &assembly(),
                    Arc::new(ScriptedIam { ready: true }),
                    root().source(),
                    rustfs_gateway::allow_when(|request| request.operation == "GetObject" && request.identity.is_some()),
                )
                .expect("the production builder assembles");
                let stack = GatewayStack {
                    service: builder
                        .register::<dto::GetObject, _>(Arc::clone(&probe))
                        .build()
                        .expect("probe registration"),
                };
                // Also exercise the inner gate alone: the outer refusal must not mask it.
                for outer in [true, false] {
                    for (secure, invalid, authenticated) in [
                        (true, None, true),
                        (false, None, true),
                        (false, Some("malformed"), true),
                        (true, Some("malformed"), true),
                        (false, Some("digest"), true),
                        (true, Some("digest"), true),
                        (false, Some("missing"), true),
                        (true, Some("missing"), true),
                        (false, None, false),
                        (true, None, false),
                    ] {
                        if !outer && secure {
                            continue;
                        }
                        let mut headers = HeaderMap::new();
                        let encoded = base64_simd::STANDARD.encode_to_string([7u8; 32]);
                        headers
                            .insert(ALGORITHM, "AES256".parse().expect("static header"));
                        headers.insert(KEY, encoded.parse().expect("base64 header"));
                        let digest = base64_simd::STANDARD.encode_to_string(md5::Md5::digest([7u8; 32]));
                        headers.insert(MD5, digest.parse().expect("base64 digest"));
                        match invalid {
                            Some("malformed") => {
                                headers.insert(KEY, "!".parse().expect("static header"));
                            }
                            Some("digest") => {
                                headers.insert(
                                    MD5,
                                    base64_simd::STANDARD
                                        .encode_to_string([0u8; 16])
                                        .parse()
                                        .expect("base64 digest"),
                                );
                            }
                            Some("missing") => {
                                headers.remove(MD5);
                            }
                            _ => {}
                        }
                        // A raw forwarding claim must not turn a plaintext connection into TLS.
                        headers
                            .insert("x-forwarded-proto", "https".parse().expect("static header"));
                        let mut request = signed_request_with_headers(
                            Signed::Query, Method::GET, "/photos/key", "", ROOT_ACCESS_KEY, ROOT_SECRET_KEY, headers,
                        );
                        let signed_headers = url::form_urlencoded::parse(request.uri().query().expect("signed query").as_bytes())
                            .find(|(name, _)| name.eq_ignore_ascii_case("X-Amz-SignedHeaders"))
                            .map(|(_, value)| value.into_owned())
                            .expect("the signer declares its signed headers");
                        for header in [ALGORITHM, KEY, MD5] {
                            if request.headers().contains_key(header) {
                                assert!(signed_headers.split(';').any(|name| name == header), "SSE-C headers must be signed");
                            }
                        }
                        if !authenticated {
                            *request.uri_mut() = "/photos/key".parse().expect("static URI");
                        }
                        let before = probe.0.load(Ordering::SeqCst);
                        let service = tower::service_fn(|request| async {
                            let response = stack.call(request).await;
                            Ok::<_, std::convert::Infallible>(response.map(|body| {
                                body.map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { Box::new(error) })
                                    .boxed_unsync()
                            }))
                        });
                        let response = if outer {
                            SsecTransportLayer::new(secure).layer(service).oneshot(request).await
                        } else {
                            service.oneshot(request).await
                        }.expect("infallible service");
                        let status = response.status();
                        let body = response.into_body().collect().await.expect("response body").to_bytes();
                        let text = String::from_utf8_lossy(&body);
                        let observed_code = ["InvalidRequest", "InvalidArgument", "AccessDenied", "SignatureDoesNotMatch", "InvalidAccessKeyId", "AuthorizationHeaderMalformed", "InternalError", "NotImplemented"]
                            .into_iter().find(|code| text.contains(&format!("<Code>{code}</Code>"))).unwrap_or("none_or_other");
                        let allowed = invalid.is_none() && authenticated && (secure || setting != Some("true"));
                        let expected_status = if allowed {
                            StatusCode::OK
                        } else if !authenticated && (secure || setting != Some("true")) {
                            StatusCode::FORBIDDEN
                        } else {
                            StatusCode::BAD_REQUEST
                        };
                        assert_eq!(
                            status,
                            expected_status,
                            "outer={outer} setting={setting:?} secure={secure} invalid={invalid:?} authenticated={authenticated} code={observed_code} prior_handler_calls={before}"
                        );
                        assert_eq!(probe.0.load(Ordering::SeqCst) - before, usize::from(allowed), "handler invocation count");
                        if !allowed {
                            let code = if !secure && setting == Some("true") {
                                "InvalidRequest"
                            } else if !authenticated {
                                "AccessDenied"
                            } else {
                                "InvalidArgument"
                            };
                            assert_eq!(observed_code, code, "the refusal code");
                        }

                        assert!(
                            !body.windows(encoded.len()).any(|window| bool::from(window.ct_eq(encoded.as_bytes()))),
                            "responses must not expose the customer key"
                        );
                    }
                }
            });
        });
    }
}
