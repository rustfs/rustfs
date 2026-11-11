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

//! The gateway's [`BucketOwnerSource`], answered by RustFS bucket metadata.
//!
//! Responsible for: asking a [`BucketOwnerLookup`] for the owner of one bucket and turning every
//! outcome that is not a usable owner into the gateway's one refusal, [`BucketOwnerError`].
//! NOT responsible for: where the owner is stored (the lookup's; RustFS has no dedicated owner
//! field, so the binary's lookup reads it from the bucket's metadata), comparing it with
//! `x-amz-expected-bucket-owner` (the gateway does, byte for byte), or authorizing anything.
//! Upstream: the gateway's expected-owner check, which consults this source only when a request
//! carries the header. Downstream: the lookup the binary supplies over `bucket::metadata_sys`.
//!
//! # Failure classes
//!
//! The lookup separates [`OwnerLookupError::NoSuchBucket`] from [`OwnerLookupError::Backend`], and
//! this source maps **both** to [`BucketOwnerError::unavailable`], which the gateway answers
//! `403 AccessDenied` and never as a matching owner. The pinned gateway's [`BucketOwnerError`] is
//! a unit type with that single constructor, so it can express neither "no such bucket" nor a 5xx;
//! the distinction is kept in the lookup trait so it can be surfaced once the gateway grows it.
//! An owner that is empty is no owner and is refused the same way.

use std::sync::Arc;

use rustfs_gateway::{BoxFuture, BucketName, BucketOwnerError, BucketOwnerSource};

/// Why a [`BucketOwnerLookup`] produced no owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnerLookupError {
    /// The bucket does not exist.
    NoSuchBucket,
    /// The metadata backend failed, or could not be reached, so the bucket's state is unknown.
    Backend,
}

/// The narrow slice of RustFS bucket metadata an owner lookup needs.
///
/// Exists so the source can be tested against a fake. The binary implements it over
/// `rustfs_ecstore::bucket::metadata_sys`, mapping `BucketNotFound` to
/// [`OwnerLookupError::NoSuchBucket`] and every other error to [`OwnerLookupError::Backend`].
pub trait BucketOwnerLookup: Send + Sync + 'static {
    /// The owner account id recorded for `bucket`.
    fn owner_of<'a>(&'a self, bucket: &'a str) -> BoxFuture<'a, Result<String, OwnerLookupError>>;
}

/// A [`BucketOwnerSource`] over a [`BucketOwnerLookup`].
pub struct RustfsBucketOwners<L> {
    lookup: L,
}

impl<L: BucketOwnerLookup> RustfsBucketOwners<L> {
    /// A source answering from `lookup`.
    #[must_use]
    pub fn new(lookup: L) -> Self {
        Self { lookup }
    }
}

impl<L: BucketOwnerLookup> BucketOwnerSource for RustfsBucketOwners<L> {
    fn owner<'a>(&'a self, bucket: &'a BucketName) -> BoxFuture<'a, Result<Arc<str>, BucketOwnerError>> {
        Box::pin(async move {
            match self.lookup.owner_of(bucket.as_str()).await {
                Ok(owner) if !owner.is_empty() => Ok(Arc::from(owner)),
                Ok(_) | Err(OwnerLookupError::NoSuchBucket | OwnerLookupError::Backend) => Err(BucketOwnerError::unavailable()),
            }
        })
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    const OWNER: &str = "rustfsadmin";

    struct Fake {
        answer: Result<String, OwnerLookupError>,
        asked: Mutex<Vec<String>>,
    }

    impl Fake {
        fn answering(answer: Result<String, OwnerLookupError>) -> Self {
            Self {
                answer,
                asked: Mutex::new(Vec::new()),
            }
        }
    }

    impl BucketOwnerLookup for Fake {
        fn owner_of<'a>(&'a self, bucket: &'a str) -> BoxFuture<'a, Result<String, OwnerLookupError>> {
            self.asked.lock().unwrap().push(bucket.to_owned());
            let answer = self.answer.clone();
            Box::pin(async move { answer })
        }
    }

    async fn owner_of(answer: Result<String, OwnerLookupError>) -> Result<Arc<str>, BucketOwnerError> {
        let source = RustfsBucketOwners::new(Fake::answering(answer));
        source.owner(&BucketName::new("photos").expect("a bucket name")).await
    }

    /// Positive: an owner the lookup finds is returned unchanged.
    #[tokio::test]
    async fn the_owner_is_returned() {
        assert_eq!(owner_of(Ok(OWNER.to_owned())).await.as_deref(), Ok(OWNER));
    }

    /// Positive: the lookup is asked about exactly the bucket the gateway named.
    #[tokio::test]
    async fn the_lookup_is_asked_about_the_named_bucket() {
        let source = RustfsBucketOwners::new(Fake::answering(Ok(OWNER.to_owned())));
        source
            .owner(&BucketName::new("photos").expect("a bucket name"))
            .await
            .expect("an owner");
        assert_eq!(*source.lookup.asked.lock().unwrap(), vec!["photos".to_owned()]);
    }

    /// Negative: a missing bucket yields no owner, so it can never match an assertion.
    #[tokio::test]
    async fn a_missing_bucket_is_refused() {
        assert_eq!(owner_of(Err(OwnerLookupError::NoSuchBucket)).await, Err(BucketOwnerError::unavailable()));
    }

    /// Negative: a failing backend is a refusal, never "owner unknown, allow".
    #[tokio::test]
    async fn a_backend_failure_is_refused() {
        assert_eq!(owner_of(Err(OwnerLookupError::Backend)).await, Err(BucketOwnerError::unavailable()));
    }

    /// Negative: an empty owner is no owner; it must not equal an empty assertion.
    #[tokio::test]
    async fn an_empty_owner_is_refused() {
        assert_eq!(owner_of(Ok(String::new())).await, Err(BucketOwnerError::unavailable()));
    }

    /// Negative: the refusal carries nothing the lookup said, so no formatter can leak it.
    #[tokio::test]
    async fn the_refusal_names_nothing() {
        let error = owner_of(Err(OwnerLookupError::Backend)).await.expect_err("a refusal");
        assert_eq!(error.to_string(), "bucket owner unavailable");
        assert!(!format!("{error:?}").contains("photos"));
    }
}
