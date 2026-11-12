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

//! Stored values as the gateway's answer types hold them, for every use case that answers in them.
//!
//! Responsible for: the bucket name, timestamp and owner conversions a use case needs to build a
//! gateway output from what the store returned, under the rule in [`crate::app`].
//! NOT responsible for: validating names a client sends (the edge that accepted the request did),
//! or deciding what a use case lists.
//! Upstream: the use cases in `crate::app`. Downstream: `rustfs_gateway_types`.

use super::storage_api::bucket_usecase::s3_api::bucket::{RUSTFS_OWNER_DISPLAY_NAME, RUSTFS_OWNER_ID};
use rustfs_gateway_types::dto::Owner;
use rustfs_gateway_types::{BucketName, NamePolicy, NameValidator, Stricter, Timestamp};
use rustfs_s3_types::{S3Error, S3ErrorCode, S3Result};
use std::sync::{Arc, LazyLock};
use time::OffsetDateTime;

/// Adds no rule above the gateway's floor: every name the store holds was admitted by RustFS's own
/// create rule when it was made, and the store's listing drops anything else.
struct StoredNames;

impl NameValidator for StoredNames {
    fn check_bucket(&self, _name: &str) -> Stricter {
        Stricter::NoOpinion
    }

    fn check_key(&self, _key: &str) -> Stricter {
        Stricter::NoOpinion
    }
}

static STORED_NAMES: LazyLock<NamePolicy> = LazyLock::new(|| NamePolicy::default().with_validator(Arc::new(StoredNames)));

/// The bucket name the store returned, as an answer carries it.
///
/// # Errors
///
/// `InternalError` for a name the gateway floor refuses. The store's listing never returns one
/// (pinned by `dto_values::tests`), so this is a store defect, never a name to leave out.
pub(crate) fn stored_bucket_name(name: &str) -> S3Result<BucketName> {
    BucketName::materialize(name, &STORED_NAMES).map_err(|rejection| {
        S3Error::with_message(
            S3ErrorCode::InternalError,
            format!("a stored bucket name the gateway cannot represent: {rejection}"),
        )
    })
}

/// `at` with its sub-second part.
pub(crate) fn timestamp(at: OffsetDateTime) -> Timestamp {
    let seconds = at.unix_timestamp();
    // `nanosecond()` is below one second by type, the only input `from_secs_nanos` refuses.
    Timestamp::from_secs_nanos(seconds, at.nanosecond()).expect("a sub-second remainder is always representable")
}

/// The one owner RustFS answers with.
pub(crate) fn owner() -> Owner {
    Owner {
        display_name: Some(RUSTFS_OWNER_DISPLAY_NAME.to_owned()),
        id: Some(RUSTFS_OWNER_ID.to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::storage_api::ecstore_capacity::is_reserved_or_invalid_bucket;

    /// Names RustFS's create rule admits and the AWS bucket grammar refuses.
    const RUSTFS_ONLY_NAMES: [&str; 5] = ["a-s3alias", "sthree-x", "x--ol-s3", "x--x-s3", "xn--abc"];

    #[test]
    fn names_rustfs_creates_and_aws_refuses_are_represented_unchanged() {
        for name in RUSTFS_ONLY_NAMES.into_iter().chain(["256.1.1.1", "sftp_host_keys", "UPPER"]) {
            assert!(BucketName::new(name).is_err(), "{name} must be outside the AWS grammar for this test");
            let materialized = stored_bucket_name(name).unwrap_or_else(|error| panic!("{name}: {error:?}"));
            assert_eq!(materialized.as_str(), name);
        }
    }

    #[test]
    fn a_name_the_floor_refuses_is_a_server_fault_not_a_gap_in_the_listing() {
        for name in ["ab", "a b", "a%2fb", "a/b"] {
            let error = stored_bucket_name(name).expect_err(name);
            assert_eq!(error.code(), &S3ErrorCode::InternalError, "{name}");
        }
    }

    /// The store's listing is the only source of stored names a use case answers with, and it
    /// drops every name its own rule refuses. Every byte in every position the rule constrains,
    /// and both length bounds: whatever it keeps, the floor represents.
    #[test]
    fn every_name_the_store_lists_is_representable() {
        let mut names: Vec<String> = (0..=u8::MAX)
            .filter_map(|byte| String::from_utf8(vec![b'a', byte, b'b']).ok())
            .chain((0..=u8::MAX).filter_map(|byte| String::from_utf8(vec![byte, b'a', b'b']).ok()))
            .collect();
        names.extend(["ab", "abc", &"a".repeat(63), &"a".repeat(64)].map(str::to_owned));

        let listed: Vec<&String> = names
            .iter()
            .filter(|name| !is_reserved_or_invalid_bucket(name, false))
            .collect();

        assert!(listed.len() > 60, "the probe must exercise names the store keeps: {}", listed.len());
        for name in listed {
            assert!(stored_bucket_name(name).is_ok(), "the store lists {name:?} and the floor refuses it");
        }
    }

    #[test]
    fn a_timestamp_keeps_its_sub_second_part() {
        let at = OffsetDateTime::from_unix_timestamp_nanos(1_700_000_000_123_000_000).expect("valid timestamp");

        let converted = timestamp(at);

        assert_eq!((converted.secs(), converted.subsec_nanos()), (1_700_000_000, 123_000_000));
    }
}
