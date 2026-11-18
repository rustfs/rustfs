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
//! `S3ErrorMessageCompatLayer` against the gateway stack alone: a `403 SignatureDoesNotMatch`
//! always carries a `<Message>`, directly after the `<Code>`.
//!
//! The layer inserts a sentence only when the stack's own document has none; the legacy stack's
//! protocol library supplies one, so the sentence on the legacy wire is that library's default
//! for the code. The gateway mechanism is `answer_credential_refusals_with_legacy_rustfs_sentences`
//! (the renderer writes a `<Message>` whenever the refusal has one).

use super::*;
use crate::server::gateway_handlers::test_support::signed_request_with_headers;

const SIGNATURE_DOES_NOT_MATCH: &str = "The request signature we calculated does not match the signature you provided. Check your AWS secret access key and signing method. For more information, see REST Authentication and SOAP Authentication for details.";
const INVALID_ACCESS_KEY_ID: &str = "The Access Key Id you provided does not exist in our records.";

/// `request` with the first digit of its signature changed, in whichever form carries it.
fn wrong_signature<B>(mut request: Request<B>) -> Request<B> {
    fn corrupt(text: &str, marker: &str) -> String {
        let at = text.find(marker).expect("the signature marker") + marker.len();
        let mut bytes = text.as_bytes().to_vec();
        bytes[at] = if bytes[at] == b'0' { b'1' } else { b'0' };
        String::from_utf8(bytes).expect("ASCII stays ASCII")
    }
    if request.uri().query().is_some_and(|query| query.contains("X-Amz-Signature=")) {
        let target = request.uri().path_and_query().expect("a target").to_string();
        *request.uri_mut() = corrupt(&target, "X-Amz-Signature=").parse().expect("a request target");
    } else {
        let value = request
            .headers()
            .get(http::header::AUTHORIZATION)
            .expect("an Authorization header")
            .to_str()
            .expect("text")
            .to_owned();
        request.headers_mut().insert(
            http::header::AUTHORIZATION,
            corrupt(&value, "Signature=").parse().expect("a header value"),
        );
    }
    request
}

fn assert_message_follows_code(answer: &Observed, code: &str, message: &str, label: &str) {
    assert!(
        answer
            .body
            .contains(&format!("<Code>{code}</Code><Message>{message}</Message>")),
        "{label}: {}",
        answer.body
    );
}

#[tokio::test]
async fn a_signature_that_does_not_match_carries_a_message_in_every_signature_form() {
    let probe = Probe::new();
    let stack = probe_stack(&probe);
    for (form, label) in [(Signed::Header, "header"), (Signed::Query, "presigned")] {
        let request =
            signed_request_with_headers(form, Method::GET, "/photos/key", "", ROOT_ACCESS_KEY, ROOT_SECRET_KEY, HeaderMap::new());
        let answer = observe(&stack, wrong_signature(request)).await;
        assert_eq!(answer.status, StatusCode::FORBIDDEN, "{label}: {}", answer.body);
        assert_message_follows_code(&answer, "SignatureDoesNotMatch", SIGNATURE_DOES_NOT_MATCH, label);
        assert_eq!(
            answer.headers.get(http::header::CONTENT_TYPE).map(|value| value.as_bytes()),
            Some(&b"application/xml"[..]),
            "{label}"
        );
    }
    assert_eq!(probe.calls(), 0);
}

#[tokio::test]
async fn an_unknown_access_key_keeps_its_own_sentence() {
    // The layer touches `SignatureDoesNotMatch` and nothing else.
    let probe = Probe::new();
    let stack = probe_stack(&probe);
    let request = signed_request_with_headers(
        Signed::Header,
        Method::GET,
        "/photos/key",
        "",
        "unknownaccesskey",
        ROOT_SECRET_KEY,
        HeaderMap::new(),
    );
    let answer = observe(&stack, request).await;
    assert_eq!(answer.status, StatusCode::FORBIDDEN, "{}", answer.body);
    assert_message_follows_code(&answer, "InvalidAccessKeyId", INVALID_ACCESS_KEY_ID, "unknown key");
    assert!(!answer.body.contains("SignatureDoesNotMatch"), "{}", answer.body);
}

#[tokio::test]
async fn a_correct_signature_is_served_and_no_other_refusal_borrows_the_sentence() {
    let probe = Probe::new();
    let stack = probe_stack(&probe);
    let served = observe(
        &stack,
        signed_request_with_headers(
            Signed::Header,
            Method::GET,
            "/photos/key",
            "",
            ROOT_ACCESS_KEY,
            ROOT_SECRET_KEY,
            HeaderMap::new(),
        ),
    )
    .await;
    assert_eq!(served.status, StatusCode::OK, "{}", served.body);
    // An anonymous caller is refused by the gate, and says `AccessDenied`, not a signature fault.
    let anonymous = observe(
        &stack,
        request(Method::GET, "/photos/key")
            .body(http_body_util::Empty::<Bytes>::new())
            .expect("request"),
    )
    .await;
    assert_eq!(anonymous.status, StatusCode::FORBIDDEN, "{}", anonymous.body);
    assert!(!anonymous.body.contains("SignatureDoesNotMatch"), "{}", anonymous.body);
    assert_eq!(probe.calls(), 1);
}
