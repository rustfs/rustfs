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
//! single owner id the deployment reports ([`RustfsBucketOwners::new`]) for it, and turning every
//! outcome that is not a usable owner into the gateway's one refusal, [`BucketOwnerError`].
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
//! AccessDenied`. A missing bucket and a backend failure with the header present are also `403`
//! at the pinned gateway, where legacy answers `404` and `5xx`: [`BucketOwnerError`] has one value,
//! so it cannot express either. [`RustfsBucketOwners::resolve`] keeps the two apart at this
//! adapter's boundary so the gateway can surface them once it grows the classes.

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
        self.lookup.check(bucket).await?;
        if self.owner.is_empty() {
            return Err(OwnerLookupError::Backend);
        }
        Ok(Arc::clone(&self.owner))
    }
}

impl<L: BucketLookup> BucketOwnerSource for RustfsBucketOwners<L> {
    fn owner<'a>(&'a self, bucket: &'a BucketName) -> BoxFuture<'a, Result<Arc<str>, BucketOwnerError>> {
        Box::pin(async move {
            // The pinned gateway collapses both classes (R-071), so both are the one refusal.
            self.resolve(bucket.as_str())
                .await
                .map_err(|_: OwnerLookupError| BucketOwnerError::unavailable())
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

    /// Negative: the pinned gateway sees one refusal for both, and never an owner.
    #[tokio::test]
    async fn the_gateway_is_refused_in_both_cases() {
        assert_eq!(
            gateway_answer(Err(OwnerLookupError::NoSuchBucket)).await,
            Err(BucketOwnerError::unavailable())
        );
        assert_eq!(gateway_answer(Err(OwnerLookupError::Backend)).await, Err(BucketOwnerError::unavailable()));
    }

    /// Negative: an empty configured owner is no owner; it must not equal an empty assertion.
    #[tokio::test]
    async fn an_empty_owner_is_refused() {
        let source = RustfsBucketOwners::new(Fake::answering(Ok(())), "");
        assert_eq!(source.resolve("photos").await, Err(OwnerLookupError::Backend));
        assert_eq!(
            source.owner(&BucketName::new("photos").expect("a bucket name")).await,
            Err(BucketOwnerError::unavailable())
        );
    }
}
