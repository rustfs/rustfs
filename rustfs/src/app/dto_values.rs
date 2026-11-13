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
//! Responsible for: the bucket name, timestamp, owner and stored-configuration conversions a use
//! case needs to build a gateway output from what the store returned, under the rule in
//! [`crate::app`]; and [`seam_error`], the one answer for a value the s3s compatibility seam
//! refused.
//! NOT responsible for: validating names a client sends (the edge that accepted the request did),
//! or deciding what a use case lists.
//! Upstream: the use cases in `crate::app`. Downstream: `rustfs_gateway_types`.

use super::storage_api::bucket_usecase::bucket::utils::{BucketConfigXml, serialize};
use super::storage_api::bucket_usecase::s3_api::bucket::{RUSTFS_OWNER_DISPLAY_NAME, RUSTFS_OWNER_ID};
use rustfs_gateway_types::compat::{ConversionError, NameRefusal};
use rustfs_gateway_types::dto::Owner;
use rustfs_gateway_types::{BucketName, ETag, NamePolicy, NameValidator, Stricter, Timestamp};
use rustfs_s3_types::{S3Error, S3ErrorCode, S3Result};
use std::fmt::Display;
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

/// What a legacy edge answers when the s3s compatibility seam refuses a value of an admitted request.
///
/// - An object key below the gateway's key floor (empty, over 1024 bytes, a NUL) is refused as
///   `validate_object_key` refuses one: `400 InvalidArgument`, with a fixed message. An edge whose
///   legacy answer names the key runs `validate_object_key` before the seam.
/// - A bucket name below the bucket floor is a server fault: s3s admits only `[a-z0-9.-]{3,63}`,
///   which the floor always accepts, so no bucket a request names reaches it.
/// - Any other refusal is a value the gateway shape cannot hold: a server fault.
pub(crate) fn seam_error(error: ConversionError) -> S3Error {
    match error.name_kind() {
        Some(NameRefusal::ObjectKey) => S3Error::with_message(S3ErrorCode::InvalidArgument, "Invalid object key"),
        Some(NameRefusal::BucketName) | None => S3Error::with_message(
            S3ErrorCode::InternalError,
            format!("failed to convert through the gateway compatibility seam: {error}"),
        ),
    }
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

/// The entity tag the store holds, as an answer carries it: every answer that carries a stored tag
/// converts it here, and none drops, rewrites or refuses one.
///
/// It reads the stored spelling exactly as the legacy edge always has (`to_s3s_etag`): `W/"…"` is
/// a weak tag and one surrounding quote pair is the store's spelling of a strong one; the tag
/// inside is kept byte for byte, so the legacy stack's answer does not change. A weak tag whose
/// inside the gateway's reader refuses or rewrites cannot stay weak (the gateway has no lossless
/// weak constructor) and is carried as a strong stored tag.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the object reads of batch C and the listings of B2 are its first callers"
    )
)]
pub(crate) fn stored_etag(stored: &str) -> ETag {
    if let Some(rest) = stored.strip_prefix("W/\"") {
        let inside = rest.strip_suffix('"').unwrap_or(rest);
        return ETag::new_weak(inside.to_owned())
            .ok()
            .filter(|etag| etag.opaque_tag() == inside)
            .unwrap_or_else(|| ETag::from_stored(inside.to_owned()));
    }
    let inside = stored
        .strip_prefix('"')
        .and_then(|rest| rest.strip_suffix('"'))
        .unwrap_or(stored);
    ETag::from_stored(inside.to_owned())
}

/// A stored bucket configuration as the gateway DTO holds it: the bytes RustFS persists, read by
/// `parse`, one of the gateway's DTO bridges. The answer carries every stored member and no other.
///
/// # Errors
///
/// `InternalError` when the stored document does not encode or the bridge refuses it: a store
/// defect, never a member to leave out.
pub(crate) fn stored_config<T: BucketConfigXml, D, E: Display>(stored: &T, parse: fn(&[u8]) -> Result<D, E>) -> S3Result<D> {
    let unreadable = |error: &dyn Display| {
        S3Error::with_message(
            S3ErrorCode::InternalError,
            format!("a stored bucket configuration the gateway cannot represent: {error}"),
        )
    };
    let bytes = serialize(stored).map_err(|error| unreadable(&error))?;
    parse(&bytes).map_err(|error| unreadable(&error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::storage_api::ecstore_capacity::is_reserved_or_invalid_bucket;
    use rustfs_gateway_types::persistence::{
        PersistedLifecycleConfiguration, PersistedReplicationConfiguration, parse_lifecycle_dto, parse_replication_dto,
        serialize_lifecycle_dto, serialize_replication_dto,
    };

    /// Names RustFS's create rule admits and the AWS bucket grammar refuses.
    const RUSTFS_ONLY_NAMES: [&str; 5] = ["a-s3alias", "sthree-x", "x--ol-s3", "x--x-s3", "xn--abc"];

    #[test]
    fn names_rustfs_creates_and_aws_refuses_are_represented_unchanged() {
        for name in RUSTFS_ONLY_NAMES.into_iter().chain(["sftp_host_keys", "UPPER"]) {
            assert!(BucketName::new(name).is_err(), "{name} must be outside the AWS grammar for this test");
            let materialized = stored_bucket_name(name).unwrap_or_else(|error| panic!("{name}: {error:?}"));
            assert_eq!(materialized.as_str(), name);
        }
        // Dotted quads the standard address parser refuses. RustFS's create rule refuses every
        // dotted quad, so none is stored, but the floor would not drop one either.
        for name in ["256.1.1.1", "01.2.3.4"] {
            assert_eq!(stored_bucket_name(name).map(|name| name.as_str().to_owned()).ok().as_deref(), Some(name));
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

    const STORED_LIFECYCLE: &[u8] = b"<LifecycleConfiguration><Rule><DelMarkerExpiration><Days>7</Days></DelMarkerExpiration>\
<Expiration><Days>30</Days><ExpiredObjectAllVersions>true</ExpiredObjectAllVersions></Expiration><Filter><Prefix>logs/</Prefix>\
</Filter><ID>r1</ID><Status>Enabled</Status><Transition><Days>10</Days><StorageClass>WARM</StorageClass></Transition></Rule>\
</LifecycleConfiguration>";

    const STORED_REPLICATION: &[u8] = b"<ReplicationConfiguration><Role>arn:minio:replication::id:target</Role><Rule>\
<DeleteMarkerReplication><Status>Enabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Enabled</Status>\
</DeleteReplication><Destination><Bucket>arn:aws:s3:::dest</Bucket><StorageClass>STANDARD</StorageClass></Destination>\
<ExistingObjectReplication><Status>Enabled</Status></ExistingObjectReplication><Filter><Prefix>a/</Prefix></Filter><ID>r1</ID>\
<Priority>1</Priority><SourceSelectionCriteria><ReplicaModifications><Status>Enabled</Status></ReplicaModifications>\
</SourceSelectionCriteria><Status>Enabled</Status></Rule></ReplicationConfiguration>";

    /// The answer holds what is stored: written back through the gateway's bridge, it is the
    /// stored document byte for byte, the MinIO members and `Transition` included.
    #[test]
    fn a_stored_configuration_is_answered_with_every_stored_member() {
        let lifecycle = PersistedLifecycleConfiguration::decode_config_xml(STORED_LIFECYCLE).expect("stored lifecycle");
        let answered = stored_config(&lifecycle, parse_lifecycle_dto).expect("lifecycle answer");
        assert_eq!(serialize_lifecycle_dto(&answered).expect("lifecycle bytes"), STORED_LIFECYCLE);
        assert_eq!(serialize(&lifecycle).expect("stored bytes"), STORED_LIFECYCLE);

        let replication = PersistedReplicationConfiguration::decode_config_xml(STORED_REPLICATION).expect("stored replication");
        let answered = stored_config(&replication, parse_replication_dto).expect("replication answer");
        assert_eq!(serialize_replication_dto(&answered).expect("replication bytes"), STORED_REPLICATION);
    }

    #[test]
    fn a_stored_configuration_the_bridge_refuses_is_a_server_fault() {
        let lifecycle = PersistedLifecycleConfiguration::decode_config_xml(STORED_LIFECYCLE).expect("stored lifecycle");

        let error = stored_config(&lifecycle, |_| Err::<(), _>("refused")).expect_err("the bridge refused it");

        assert_eq!(error.code(), &S3ErrorCode::InternalError);
    }

    #[test]
    fn a_seam_refusal_is_answered_as_the_legacy_edge_answers_it() {
        use crate::app::storage_api::s3::dto::{HeadBucketInput, HeadObjectInput};
        use rustfs_gateway_types::compat::s3s_5761ddfe::generated::ops::{head_bucket, head_object};

        let input = HeadObjectInput::builder()
            .bucket("bucket".into())
            .key("a\u{0}b".into())
            .build()
            .unwrap();
        let key = head_object::input_from_s3s(input).expect_err("a NUL is below the key floor");
        assert_eq!(seam_error(key).code(), &S3ErrorCode::InvalidArgument);

        let input = HeadBucketInput::builder().bucket("a/b".into()).build().unwrap();
        let bucket = head_bucket::input_from_s3s(input).expect_err("a separator is below the bucket floor");
        assert_eq!(seam_error(bucket).code(), &S3ErrorCode::InternalError);

        let input = HeadBucketInput::builder().bucket("sthree-x".into()).build().unwrap();
        assert!(head_bucket::input_from_s3s(input).is_ok(), "a name the floor admits crosses the seam");
    }

    #[test]
    fn a_timestamp_keeps_its_sub_second_part() {
        let at = OffsetDateTime::from_unix_timestamp_nanos(1_700_000_000_123_000_000).expect("valid timestamp");

        let converted = timestamp(at);

        assert_eq!((converted.secs(), converted.subsec_nanos()), (1_700_000_000, 123_000_000));
    }

    /// Stored tag spellings retain their expected forms across conversions and renderers.
    #[test]
    fn a_stored_tag_is_answered_as_main_answers_it_on_both_edges() {
        use crate::app::storage_api::object_usecase::object_utils::to_s3s_etag;
        use rustfs_gateway_types::EtagRender;
        use rustfs_gateway_types::compat::s3s_5761ddfe::leaf::etag_to_s3s;

        // Stored spelling, legacy header, gateway header, and XML text.
        let rows: [(&str, &str, &str, &str); 11] = [
            ("abc", r#""abc""#, r#""abc""#, r#""abc""#),
            (r#""abc""#, r#""abc""#, r#""abc""#, r#""abc""#),
            (r#"W/"abc""#, r#"W/"abc""#, r#"W/"abc""#, r#""abc""#),
            ("abc-3", r#""abc-3""#, r#""abc-3""#, r#""abc-3""#),
            (r#""""#, r#""""#, r#""""#, r#""""#),
            ("", r#""""#, r#""""#, r#""""#),
            ("W/abc", r#""W/abc""#, r#""W/abc""#, r#""W/abc""#),
            ("a&b", r#""a&b""#, r#""a&b""#, r#""a&b""#),
            (r#""abc"#, r#"""abc""#, r#""\"abc""#, r#"""abc""#),
            (r#"""abc"""#, r#"""abc"""#, r#""\"abc\"""#, r#"""abc"""#),
            (r#"w/"abc""#, r#""w/"abc"""#, r#""w/\"abc\"""#, r#""w/"abc"""#),
        ];
        // Every row is checked before the test fails, so one run names every row that differs.
        let mut differing = Vec::new();
        for (stored, main_header, gateway_header, xml) in rows {
            let etag = stored_etag(stored);
            let legacy = to_s3s_etag(stored);
            let answered = (
                etag_to_s3s(&etag) == legacy,
                legacy.to_http_header().is_ok_and(|header| header == main_header),
                etag.render(EtagRender::HeaderQuoted) == gateway_header,
                etag.render(EtagRender::XmlQuoted) == xml,
            );
            if answered != (true, true, true, true) {
                differing.push((stored, answered));
            }
        }
        assert!(differing.is_empty(), "(legacy edge, main header, gateway header, XML): {differing:?}");
    }

    #[test]
    fn stored_control_characters_are_preserved() {
        for stored in ["abc\u{1}def", "abc\ndef"] {
            let etag = stored_etag(stored);
            assert_eq!(etag.opaque_tag(), stored, "{stored:?}");
            assert!(!etag.is_weak(), "{stored:?}");
        }
    }

    /// A weak spelling with an empty tag remains readable.
    #[test]
    fn a_weak_stored_tag_the_gateway_cannot_hold_weak_is_answered_strong() {
        let etag = stored_etag(r#"W/"""#);

        assert!(!etag.is_weak());
        assert_eq!(etag.opaque_tag(), "");
    }
}
