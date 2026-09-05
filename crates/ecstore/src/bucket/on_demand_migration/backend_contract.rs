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

//! One contract every [`SourceBackend`] implementation must satisfy.
//!
//! The migration pipeline talks to a source only through the trait, so a new
//! provider is correct exactly when it answers the same questions the same way:
//! the same head fields, the same range semantics, the same page shape, the
//! same error classes. Each backend supplies a fixture that answers this fixed
//! corpus in its own dialect and then runs [`assert_backend_contract`], so a
//! provider-specific mapping bug shows up as a contract failure rather than as
//! a surprise in the pull pipeline.
//!
//! Backends differ in two documented ways, declared through
//! [`BackendCapabilities`]: whether the provider's ETag is a content digest,
//! and whether the provider can resume a listing from a key.

use super::source_client::{SourceBackend, SourceError, SourceListRequest};
use crate::storage_api_contracts::range::HTTPRangeSpec;
use std::collections::HashMap;

/// The single object every fixture serves.
pub(super) const OBJECT_KEY: &str = "dir/a.txt";
pub(super) const OBJECT_BODY: &[u8] = b"hello";
/// MD5 of [`OBJECT_BODY`]; the ETag of the object on a digest provider.
pub(super) const OBJECT_MD5: &str = "5d41402abc4b2a76b9719d911017c592";
/// The second key the fixture's listing returns, on its second page.
pub(super) const SECOND_KEY: &str = "dir/b.txt";
pub(super) const COMMON_PREFIX: &str = "dir/sub/";
pub(super) const LIST_CURSOR: &str = "cursor-1";
/// A key the fixture answers with the provider's "no such object".
pub(super) const MISSING_KEY: &str = "missing";
/// A key the fixture answers with the provider's "not authorized".
pub(super) const FORBIDDEN_KEY: &str = "secret";

/// Where backends are allowed to differ.
#[derive(Clone, Copy, Debug)]
pub(super) struct BackendCapabilities {
    /// The provider's ETag is an opaque token, not a digest of the bytes.
    pub(super) etag_is_opaque: bool,
    /// The provider can resume a listing from a key rather than only from an
    /// opaque cursor.
    pub(super) supports_start_after: bool,
    /// The provider has an object-tagging concept at all. GCS does not, and
    /// answers with an empty map instead of failing a pull.
    pub(super) supports_tagging: bool,
}

/// Drives `backend` through the shared corpus. Fixtures are scripted in
/// request order, so the call order here is part of the contract.
pub(super) async fn assert_backend_contract(backend: &dyn SourceBackend, caps: BackendCapabilities) {
    // 1. HEAD maps the object's shared fields.
    let head = backend.head(OBJECT_KEY).await.expect("HEAD of the fixture object");
    assert_eq!(head.size, OBJECT_BODY.len() as u64, "HEAD reports the object size");
    assert_eq!(head.content_type.as_deref(), Some("text/plain"));
    assert_eq!(
        head.user_metadata,
        HashMap::from([("owner".to_string(), "alice".to_string())]),
        "user metadata is keyed without the provider prefix"
    );
    assert!(head.storage_class.is_some(), "the provider's tier is recorded");
    assert!(head.last_modified.is_some(), "the provider's timestamp is parsed");
    assert!(head.sse.is_none(), "the fixture object is not server-side encrypted");
    assert!(!head.is_multipart_etag);
    assert_eq!(head.etag_is_opaque, caps.etag_is_opaque);
    match caps.etag_is_opaque {
        false => assert_eq!(head.etag.as_deref(), Some(OBJECT_MD5), "a digest ETag is mapped verbatim"),
        true => assert!(head.etag.is_some(), "an opaque ETag is still recorded"),
    }

    // 2. An unranged GET streams the whole object and reports no range.
    let got = backend.get(OBJECT_KEY, None).await.expect("unranged GET");
    assert_eq!(got.head.size, OBJECT_BODY.len() as u64);
    assert!(got.content_range.is_none(), "an unranged GET has no content-range");
    assert_eq!(got.head.etag_is_opaque, caps.etag_is_opaque, "GET and HEAD agree about the ETag");
    let body = got.body.collect().await.expect("body streams").into_bytes();
    assert_eq!(body.as_ref(), OBJECT_BODY);

    // 3. A ranged GET returns exactly the requested interval, and `size` is
    //    the length of the returned bytes rather than of the object.
    let range = HTTPRangeSpec {
        is_suffix_length: false,
        start: 1,
        end: 3,
    };
    let got = backend.get(OBJECT_KEY, Some(&range)).await.expect("ranged GET");
    assert_eq!(got.head.size, 3, "a ranged GET reports the range length");
    assert_eq!(got.content_range.as_deref(), Some("bytes 1-3/5"));
    let body = got.body.collect().await.expect("body streams").into_bytes();
    assert_eq!(body.as_ref(), &OBJECT_BODY[1..=3]);

    // 4. A delimiter listing rolls prefixes up and hands back a cursor.
    let page = backend
        .list(&SourceListRequest {
            prefix: Some("dir/"),
            delimiter: Some("/"),
            max_keys: 2,
            ..Default::default()
        })
        .await
        .expect("first listing page");
    assert_eq!(page.objects.len(), 1, "the first page holds one object");
    assert_eq!(page.objects[0].key, OBJECT_KEY, "listing keys are in the source namespace");
    assert_eq!(page.objects[0].size, OBJECT_BODY.len() as u64);
    assert!(page.objects[0].last_modified.is_some());
    assert_eq!(page.common_prefixes, vec![COMMON_PREFIX.to_string()]);
    assert!(page.is_truncated);
    assert_eq!(page.next_continuation_token.as_deref(), Some(LIST_CURSOR));

    // 5. The cursor is passed back verbatim and the last page ends the walk.
    let page = backend
        .list(&SourceListRequest {
            prefix: Some("dir/"),
            delimiter: Some("/"),
            continuation_token: Some(LIST_CURSOR),
            max_keys: 2,
            ..Default::default()
        })
        .await
        .expect("second listing page");
    assert_eq!(page.objects.len(), 1);
    assert_eq!(page.objects[0].key, SECOND_KEY);
    assert!(!page.is_truncated);
    assert!(page.next_continuation_token.is_none(), "a complete listing carries no cursor");

    // 6. Tags come back as a flat map, empty on a provider without tags.
    let tags = backend.tagging(OBJECT_KEY).await.expect("object tags");
    match caps.supports_tagging {
        true => assert_eq!(tags, HashMap::from([("env".to_string(), "prod".to_string())])),
        false => assert!(tags.is_empty(), "a provider without tags reports none: {tags:?}"),
    }

    // 7. The probe confirms the bucket or container answers.
    backend.probe().await.expect("probe of the fixture bucket");

    // 8. A missing object is `NotFound`, and never retried.
    let err = backend.head(MISSING_KEY).await.expect_err("a missing object must fail");
    assert!(matches!(err, SourceError::NotFound), "{err:?}");
    assert_eq!(err.class_label(), "not_found");
    assert!(!err.is_retryable());

    // 9. A denied object is `AccessDenied`, and never retried.
    let err = backend.head(FORBIDDEN_KEY).await.expect_err("a denied object must fail");
    assert!(matches!(err, SourceError::AccessDenied), "{err:?}");
    assert_eq!(err.class_label(), "access_denied");
    assert!(!err.is_retryable());

    // 10. A provider without a key cursor must refuse one instead of listing
    //     from the wrong position. This issues no request either way.
    if !caps.supports_start_after {
        let err = backend
            .list(&SourceListRequest {
                start_after: Some(OBJECT_KEY),
                max_keys: 1,
                ..Default::default()
            })
            .await
            .expect_err("a backend without a key cursor must refuse start_after");
        assert!(matches!(err, SourceError::Unsupported(_)), "{err:?}");
    }
}
