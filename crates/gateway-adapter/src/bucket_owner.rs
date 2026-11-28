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

//! The gateway's [`BucketOwnerSource`]: the one owner RustFS reports, for a bucket that exists.
//!
//! Responsible for: confirming through a [`BucketLookup`] that a bucket exists, answering the
//! single owner id the deployment reports ([`RustfsBucketOwners::new`]) for it, and translating
//! missing buckets, metadata backend faults, and unusable owner configuration into the matching
//! [`BucketOwnerError`] classes.
//! NOT responsible for: choosing the owner id (the binary passes the constant it already reports
//! as the owner in listings), comparing it with `x-amz-expected-bucket-owner` (the gateway does,
//! byte for byte), or authorizing anything.
//! Upstream: the gateway's expected-owner check, which consults this source only when a request
//! carries the header, after authorization allowed it. Downstream: the lookup the binary supplies
//! over the ecstore bucket metadata system.
//!
//! # What an owner is in RustFS
//!
//! RustFS stores no per-bucket owner: bucket ACL writes are no-ops and nothing writes the ACL
//! document, so the only owner RustFS reports anywhere (ListBuckets, ACL and multipart listings)
//! is one constant. The source answers exactly that constant. A caller therefore passes the
//! gateway's check only by presenting the owner id RustFS lists, which separates no tenants; it is
//! a compatibility answer, and the constant's own documentation says it must not decide
//! authorization.
//!
//! # Difference from legacy (ledger entry R-071)
//!
//! Legacy RustFS ignores `x-amz-expected-bucket-owner` entirely. On the gateway stack a request
//! carrying it is served only when the value equals the reported owner id; otherwise it is `403
//! AccessDenied`. Once authorized, a missing bucket yields [`BucketOwnerError::not_found`] (`404
//! NoSuchBucket`, or bodyless `404` on `HEAD`), a metadata lookup fault yields
//! [`BucketOwnerError::backend_failure`] (retryable `500 InternalError`), and an empty configured
//! owner fails closed with [`BucketOwnerError::unavailable`] (`403 AccessDenied`) without probing
//! bucket existence.

use std::sync::Arc;

use rustfs_gateway::{BoxFuture, BucketName, BucketOwnerError, BucketOwnerSource};

/// Why a bucket yielded no owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnerLookupError {
    /// The bucket does not exist.
    NoSuchBucket,
    /// The metadata backend failed, could not be reached, or the owner is not usable (an empty
    /// owner id is a misconfiguration), so the bucket's state is unknown.
    Backend,
}

/// The narrow slice of RustFS bucket metadata an owner answer needs: whether a bucket exists.
///
/// Exists so the source can be tested against a fake. The binary implements it over the ecstore
/// bucket metadata system, mapping `BucketNotFound` to [`OwnerLookupError::NoSuchBucket`] and every
/// other error to [`OwnerLookupError::Backend`].
pub trait BucketLookup: Send + Sync + 'static {
    /// `Ok(())` when `bucket` exists.
    fn check<'a>(&'a self, bucket: &'a str) -> BoxFuture<'a, Result<(), OwnerLookupError>>;
}

/// A [`BucketOwnerSource`] answering one owner id for every bucket that exists.
pub struct RustfsBucketOwners<L> {
    lookup: L,
    owner: Arc<str>,
}

impl<L: BucketLookup> RustfsBucketOwners<L> {
    /// A source answering `owner` for every bucket `lookup` finds.
    ///
    /// An empty `owner` is no owner: every answer is then [`OwnerLookupError::Backend`].
    #[must_use]
    pub fn new(lookup: L, owner: impl Into<Arc<str>>) -> Self {
        Self {
            lookup,
            owner: owner.into(),
        }
    }

    /// The owner of `bucket`, or why there is none. Missing and failing stay distinct here.
    ///
    /// # Errors
    ///
    /// [`OwnerLookupError::NoSuchBucket`] when the bucket does not exist;
    /// [`OwnerLookupError::Backend`] on any lookup failure or when the configured owner is empty.
    pub async fn resolve(&self, bucket: &str) -> Result<Arc<str>, OwnerLookupError> {
        if self.owner.is_empty() {
            return Err(OwnerLookupError::Backend);
        }
        self.lookup.check(bucket).await?;
        Ok(Arc::clone(&self.owner))
    }
}

impl<L: BucketLookup> BucketOwnerSource for RustfsBucketOwners<L> {
    fn owner<'a>(&'a self, bucket: &'a BucketName) -> BoxFuture<'a, Result<Arc<str>, BucketOwnerError>> {
        Box::pin(async move {
            if self.owner.is_empty() {
                return Err(BucketOwnerError::unavailable());
            }
            match self.lookup.check(bucket.as_str()).await {
                Ok(()) => Ok(Arc::clone(&self.owner)),
                Err(OwnerLookupError::NoSuchBucket) => Err(BucketOwnerError::not_found()),
                Err(OwnerLookupError::Backend) => Err(BucketOwnerError::backend_failure()),
            }
        })
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Stands for the owner id RustFS reports; the binary passes its own constant.
    const OWNER: &str = "reported-owner-id";

    struct Fake {
        answer: Result<(), OwnerLookupError>,
        asked: Mutex<Vec<String>>,
    }

    impl Fake {
        fn answering(answer: Result<(), OwnerLookupError>) -> Self {
            Self {
                answer,
                asked: Mutex::new(Vec::new()),
            }
        }
    }

    impl BucketLookup for Fake {
        fn check<'a>(&'a self, bucket: &'a str) -> BoxFuture<'a, Result<(), OwnerLookupError>> {
            self.asked.lock().unwrap().push(bucket.to_owned());
            let answer = self.answer;
            Box::pin(async move { answer })
        }
    }

    fn source(answer: Result<(), OwnerLookupError>) -> RustfsBucketOwners<Fake> {
        RustfsBucketOwners::new(Fake::answering(answer), OWNER)
    }

    async fn gateway_answer(answer: Result<(), OwnerLookupError>) -> Result<Arc<str>, BucketOwnerError> {
        source(answer).owner(&BucketName::new("photos").expect("a bucket name")).await
    }

    /// Positive: an existing bucket's owner is the configured id, byte for byte.
    #[tokio::test]
    async fn an_existing_bucket_has_the_configured_owner() {
        assert_eq!(gateway_answer(Ok(())).await.as_deref(), Ok(OWNER));
    }

    /// Positive: every bucket gets the same owner, since RustFS stores none per bucket.
    #[tokio::test]
    async fn every_bucket_has_the_same_owner() {
        let source = source(Ok(()));
        assert_eq!(source.resolve("photos").await.as_deref(), Ok(OWNER));
        assert_eq!(source.resolve("backups").await.as_deref(), Ok(OWNER));
    }

    /// Positive: the lookup is asked about exactly the bucket the gateway named.
    #[tokio::test]
    async fn the_lookup_is_asked_about_the_named_bucket() {
        let source = source(Ok(()));
        source
            .owner(&BucketName::new("photos").expect("a bucket name"))
            .await
            .expect("an owner");
        assert_eq!(*source.lookup.asked.lock().unwrap(), vec!["photos".to_owned()]);
    }

    /// Negative: a missing bucket is reported as missing at the adapter boundary.
    #[tokio::test]
    async fn a_missing_bucket_is_reported_as_missing() {
        assert_eq!(
            source(Err(OwnerLookupError::NoSuchBucket)).resolve("photos").await,
            Err(OwnerLookupError::NoSuchBucket)
        );
    }

    /// Negative: a failing backend is reported as a failure, never as an owner and never as missing.
    #[tokio::test]
    async fn a_backend_failure_is_reported_as_a_failure() {
        assert_eq!(
            source(Err(OwnerLookupError::Backend)).resolve("photos").await,
            Err(OwnerLookupError::Backend)
        );
    }

    /// Negative: the gateway distinguishes a missing bucket (`404 NoSuchBucket`) from a metadata
    /// backend failure (`500 InternalError`), and never treats either as an owner.
    #[tokio::test]
    async fn the_gateway_distinguishes_missing_bucket_from_backend_failure() {
        assert_eq!(
            gateway_answer(Err(OwnerLookupError::NoSuchBucket)).await,
            Err(BucketOwnerError::not_found())
        );
        assert_eq!(
            gateway_answer(Err(OwnerLookupError::Backend)).await,
            Err(BucketOwnerError::backend_failure())
        );
    }

    /// Negative: an empty configured owner is no owner; it must fail closed as `unavailable`
    /// without probing bucket existence, even when the lookup would report `NoSuchBucket`.
    #[tokio::test]
    async fn an_empty_owner_is_refused() {
        for answer in [Ok(()), Err(OwnerLookupError::NoSuchBucket), Err(OwnerLookupError::Backend)] {
            let source = RustfsBucketOwners::new(Fake::answering(answer), "");
            assert_eq!(source.resolve("photos").await, Err(OwnerLookupError::Backend));
            assert_eq!(
                source.owner(&BucketName::new("photos").expect("a bucket name")).await,
                Err(BucketOwnerError::unavailable())
            );
            assert!(
                source.lookup.asked.lock().expect("lock").is_empty(),
                "empty configured owner must not probe bucket existence"
            );
        }
    }

    /// Assembled service: `RustfsBucketOwners` surfaces `200`, `403 AccessDenied`,
    /// `404 NoSuchBucket` (and bodyless `404` on `HEAD`), `500 InternalError`, and empty-owner
    /// fail-closed `403 AccessDenied` through the real gateway pipeline.
    #[tokio::test]
    async fn assembled_service_surfaces_distinct_bucket_owner_outcomes() {
        use bytes::Bytes;
        use rustfs_gateway::dto::{GetBucketLocation, GetBucketLocationOutput, HeadBucket, HeadBucketOutput};
        use rustfs_gateway::{
            Decision, Handler, HandlerResult, RegionSet, Req, Resp, SecurityFloor, ServiceBuilder, SigV4Authenticator,
            StaticCredentials, decide_with,
        };

        struct OkHandler;
        impl Handler<HeadBucket> for OkHandler {
            async fn call(&self, _request: Req<HeadBucket>) -> HandlerResult<HeadBucket> {
                Ok(Resp::new(HeadBucketOutput::default()))
            }
        }
        impl Handler<GetBucketLocation> for OkHandler {
            async fn call(&self, _request: Req<GetBucketLocation>) -> HandlerResult<GetBucketLocation> {
                Ok(Resp::new(GetBucketLocationOutput::default()))
            }
        }

        let build = |answer: Result<(), OwnerLookupError>, owner: &'static str| {
            let handler = Arc::new(OkHandler);
            let regions = RegionSet::new(["us-east-1"]).expect("a region");
            ServiceBuilder::new()
                .authenticator(SigV4Authenticator::new(Arc::new(StaticCredentials::new()), regions))
                .security_floor(SecurityFloor::new().delegate_anonymous_to_authorizer_after_listing_in_the_posture_report())
                .authorizer(decide_with(|_| Decision::Allow))
                .bucket_owner_source(RustfsBucketOwners::new(Fake::answering(answer), owner))
                .register::<HeadBucket, _>(Arc::clone(&handler))
                .register::<GetBucketLocation, _>(handler)
                .build()
                .expect("assemble gateway service")
        };
        let send = async |service: &rustfs_gateway::S3Service, method: http::Method, uri: &str, expected_owner: &str| {
            let req = http::Request::builder()
                .method(method)
                .uri(uri)
                .header(http::header::HOST, "s3.example.com")
                .header("x-amz-expected-bucket-owner", expected_owner)
                .body(Bytes::new())
                .expect("request");
            rustfs_gateway::collect(service.call_bytes(req).await)
                .await
                .expect("collect response")
        };

        let existing = build(Ok(()), OWNER);
        let ok = send(&existing, http::Method::GET, "/photos?location", OWNER).await;
        assert_eq!(ok.status(), http::StatusCode::OK);
        let wrong = send(&existing, http::Method::GET, "/photos?location", "wrong-owner").await;
        assert_eq!(wrong.status(), http::StatusCode::FORBIDDEN);
        assert!(String::from_utf8_lossy(wrong.body()).contains("<Code>AccessDenied</Code>"));

        let missing = build(Err(OwnerLookupError::NoSuchBucket), OWNER);
        let missing_get = send(&missing, http::Method::GET, "/photos?location", OWNER).await;
        assert_eq!(missing_get.status(), http::StatusCode::NOT_FOUND);
        assert!(String::from_utf8_lossy(missing_get.body()).contains("<Code>NoSuchBucket</Code>"));
        let missing_head = send(&missing, http::Method::HEAD, "/photos", OWNER).await;
        assert_eq!(missing_head.status(), http::StatusCode::NOT_FOUND);
        assert!(missing_head.body().is_empty(), "HEAD 404 must be bodyless");

        let fault = build(Err(OwnerLookupError::Backend), OWNER);
        let backend_err = send(&fault, http::Method::GET, "/photos?location", OWNER).await;
        assert_eq!(backend_err.status(), http::StatusCode::INTERNAL_SERVER_ERROR);
        assert!(String::from_utf8_lossy(backend_err.body()).contains("<Code>InternalError</Code>"));

        let unconfigured = build(Err(OwnerLookupError::NoSuchBucket), "");
        let refused = send(&unconfigured, http::Method::GET, "/photos?location", OWNER).await;
        assert_eq!(refused.status(), http::StatusCode::FORBIDDEN);
        assert!(String::from_utf8_lossy(refused.body()).contains("<Code>AccessDenied</Code>"));
    }
}
