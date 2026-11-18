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

//! ListObjectsV2 and ListObjects: one page of a bucket listing and its answer as the gateway's
//! output types hold it.
//!
//! Responsible for: reading the page (plain, table-isolated or merged with an on-demand migration
//! source) and building the answer from the stored values, raw.
//! NOT responsible for: `encoding-type=url` and the `max-keys` ceiling (the edge that writes the
//! answer: the gateway under its RustFS profile, the legacy edge in `storage/legacy_edge`), the
//! authorization and its table snapshot (`storage/access_owned/listing_multipart.rs`), or the
//! `?metadata=true` listings (`listing.rs`).
//! Upstream: both S3 edges. Downstream: the store, `bucket_list_through`, `table_list_isolation`.

use super::*;
use crate::app::storage_api::bucket_usecase::contract::list::ListOperations as _;
use rustfs_gateway_types::OpaqueString;
use rustfs_s3_types::{S3Error, S3ErrorCode, S3Result};

type ListObjectsV2Info = StorageListObjectsV2Info<ObjectInfo>;

/// What one page of a ListObjectsV2-shaped listing asked for, from either listing's input.
struct PageRequest {
    bucket: String,
    params: ListObjectsV2Params,
    fetch_owner: bool,
    /// Whether an on-demand migration source may be merged in. ListObjects markers are plain keys
    /// and cannot carry the merged cursor, so only ListObjectsV2 merges.
    list_through: bool,
}

/// One page as the store returned it.
struct Page {
    info: ListObjectsV2Info,
    /// The source could not be consulted and the page is local state only.
    source_left_out: bool,
}

impl DefaultBucketUsecase {
    /// One page of ListObjectsV2.
    ///
    /// # Errors
    ///
    /// `InvalidArgument` for a negative `max-keys`, an undecodable continuation token or an
    /// unordered delimited listing; `NoSuchBucket`; `ServiceUnavailable` for a protected table
    /// listing while list-through is active; otherwise the store's error.
    #[instrument(level = "trace", skip(self, envelope, input))]
    pub async fn execute_list_objects_v2(
        &self,
        mut envelope: RequestEnvelope,
        input: gateway_dto::ListObjectsV2Input,
    ) -> S3Result<gateway_dto::ListObjectsV2Output> {
        let request = PageRequest {
            bucket: input.bucket.as_str().to_owned(),
            params: parse_list_objects_v2_params(
                input.prefix,
                input.delimiter,
                input.max_keys,
                input.continuation_token.map(|token| token.as_str().to_owned()),
                input.start_after,
            )?,
            fetch_owner: input.fetch_owner.unwrap_or_default(),
            list_through: true,
        };
        let page = self.list_objects_page(&mut envelope, &request).await?;
        list_objects_v2_output(page.info, &request.bucket, request.params, request.fetch_owner, input.encoding_type).map(
            |output| gateway_dto::ListObjectsV2Output {
                rustfs_on_demand_migration_list: page
                    .source_left_out
                    .then(|| ON_DEMAND_MIGRATION_LIST_LOCAL_ONLY_VALUE.to_owned()),
                ..output
            },
        )
    }

    /// One page of ListObjects: the ListObjectsV2 page from `marker`, never merged with a source.
    ///
    /// # Errors
    ///
    /// As [`Self::execute_list_objects_v2`].
    #[instrument(level = "debug", skip(self, envelope, input))]
    pub async fn execute_list_objects(
        &self,
        mut envelope: RequestEnvelope,
        input: gateway_dto::ListObjectsInput,
    ) -> S3Result<gateway_dto::ListObjectsOutput> {
        let request = PageRequest {
            bucket: input.bucket.as_str().to_owned(),
            params: parse_list_objects_v2_params(input.prefix, input.delimiter, input.max_keys, None, input.marker.clone())?,
            fetch_owner: false,
            list_through: false,
        };
        let page = self.list_objects_page(&mut envelope, &request).await?;
        // A protected table listing's cursor is sealed; ListObjects exposes a key instead.
        let cursor_is_a_key = envelope.table_list_access().is_none();
        list_objects_output(
            page.info,
            &request.bucket,
            request.params,
            input.marker,
            cursor_is_a_key,
            input.encoding_type,
        )
    }

    async fn list_objects_page(&self, envelope: &mut RequestEnvelope, request: &PageRequest) -> S3Result<Page> {
        let PageRequest { bucket, params, .. } = request;
        validate_list_object_unordered_with_delimiter(params.delimiter.as_ref(), envelope.uri().query())?;
        let store = get_validated_store(bucket).await?;
        let incl_deleted = get_header(envelope.headers(), rustfs_utils::http::SUFFIX_INCLUDE_DELETED).as_deref() == Some("true");
        let table_list_access = envelope.table_list_access().cloned();

        // The on-demand migration envelope is decoded whether or not this bucket still merges: a
        // token handed out under `list_through` must keep paginating after the policy is turned
        // off (rustfs/backlog#2164).
        let (merged_token, source_state) = if request.list_through {
            prepare_odm_read_generation(&store, envelope, bucket).await;
            let source_state = list_through::list_through_state(&store, bucket, envelope, params).await?;
            if table_list_access.is_some() {
                if source_state.is_some() {
                    return Err(S3Error::with_message(
                        S3ErrorCode::ServiceUnavailable,
                        "protected table listings are unavailable while on-demand migration list-through is active",
                    ));
                }
                (None, None)
            } else {
                (
                    list_through::decode_list_cursor(params.decoded_continuation_token.as_deref())?,
                    source_state,
                )
            }
        } else {
            (None, None)
        };

        if let Some(access) = table_list_access.as_ref() {
            let info = table_list_isolation::list_objects_v2(
                store.clone(),
                access,
                table_list_isolation::ListObjectsV2Request {
                    bucket,
                    prefix: &params.prefix,
                    continuation_token: params.decoded_continuation_token.as_deref(),
                    delimiter: params.delimiter.as_deref(),
                    max_keys: params.max_keys,
                    start_after: params.start_after_for_query.as_deref(),
                    incl_deleted,
                    opaque_cursor_supported: request.list_through,
                },
            )
            .await?;
            return Ok(Page {
                info,
                source_left_out: false,
            });
        }

        Ok(match (source_state, merged_token.as_ref()) {
            (None, Some(token)) if params.max_keys == 0 => {
                // No source was consulted, so retain every unconsumed side and the original wire
                // format without spending its progress budget.
                let is_truncated = !token.local_done || !token.source_done;
                Page {
                    info: ListObjectsV2Info {
                        is_truncated,
                        next_continuation_token: params.decoded_continuation_token.clone().filter(|_| is_truncated),
                        ..Default::default()
                    },
                    source_left_out: false,
                }
            }
            (None, None) => Page {
                info: store
                    .list_objects_v2(
                        bucket,
                        &params.prefix,
                        params.decoded_continuation_token.clone(),
                        params.delimiter.clone(),
                        params.max_keys,
                        request.fetch_owner,
                        params.start_after_for_query.clone(),
                        incl_deleted,
                    )
                    .await
                    .map_err(ApiError::from)?,
                source_left_out: false,
            },
            (state, token) => {
                let outcome = list_through::merged_list_objects_v2(
                    &store,
                    state.as_ref(),
                    bucket,
                    params,
                    request.fetch_owner,
                    incl_deleted,
                    token,
                )
                .await?;
                Page {
                    info: outcome.info,
                    source_left_out: outcome.degraded,
                }
            }
        })
    }
}

/// The ListObjectsV2 answer for one page the store returned.
///
/// # Errors
///
/// `InternalError` for a stored value the answer cannot carry.
pub(crate) fn list_objects_v2_output(
    info: ListObjectsV2Info,
    bucket: &str,
    params: ListObjectsV2Params,
    fetch_owner: bool,
    encoding_type: Option<gateway_dto::EncodingType>,
) -> S3Result<gateway_dto::ListObjectsV2Output> {
    let (contents, common_prefixes) = page_entries(&info, fetch_owner)?;
    Ok(gateway_dto::ListObjectsV2Output {
        is_truncated: info.is_truncated,
        key_count: key_count(contents.len() + common_prefixes.len()),
        contents,
        common_prefixes,
        name: dto_values::stored_bucket_name(bucket)?,
        prefix: params.prefix,
        delimiter: params.delimiter,
        max_keys: params.max_keys,
        encoding_type,
        continuation_token: params.response_continuation_token.map(OpaqueString::from),
        next_continuation_token: info
            .next_continuation_token
            .map(|cursor| OpaqueString::from(encode_list_objects_v2_continuation_token(&cursor))),
        start_after: params.response_start_after,
        ..Default::default()
    })
}

/// The ListObjects answer for one page the store returned. `marker` is echoed as the request sent
/// it; `cursor_is_a_key` says whether the store's cursor may be shown as `NextMarker`.
///
/// # Errors
///
/// `InternalError` for a stored value the answer cannot carry.
fn list_objects_output(
    info: ListObjectsV2Info,
    bucket: &str,
    params: ListObjectsV2Params,
    marker: Option<String>,
    cursor_is_a_key: bool,
    encoding_type: Option<gateway_dto::EncodingType>,
) -> S3Result<gateway_dto::ListObjectsOutput> {
    let cursor = info.next_continuation_token.as_deref().filter(|_| cursor_is_a_key);
    let next_marker = list_objects_next_marker(&info, cursor);
    let (contents, common_prefixes) = page_entries(&info, false)?;
    Ok(gateway_dto::ListObjectsOutput {
        is_truncated: info.is_truncated,
        marker: marker.unwrap_or_default(),
        next_marker,
        contents,
        common_prefixes,
        name: dto_values::stored_bucket_name(bucket)?,
        prefix: params.prefix,
        delimiter: params.delimiter,
        max_keys: params.max_keys,
        encoding_type,
        ..Default::default()
    })
}

/// The value of `x-rustfs-on-demand-migration-list` on a page that left the source out.
const ON_DEMAND_MIGRATION_LIST_LOCAL_ONLY_VALUE: &str = "local_only";

fn key_count(entries: usize) -> i32 {
    // A page holds at most `max-keys` entries, an `i32`.
    i32::try_from(entries).unwrap_or(i32::MAX)
}

/// The entries of a page as an answer carries them. A keyless entry is the store's placeholder for
/// a page boundary, never an object.
fn page_entries(
    info: &ListObjectsV2Info,
    fetch_owner: bool,
) -> S3Result<(Vec<gateway_dto::Object>, Vec<gateway_dto::CommonPrefix>)> {
    let contents = info
        .objects
        .iter()
        .filter(|object| !object.name.is_empty())
        .map(|object| {
            Ok(gateway_dto::Object {
                key: dto_values::stored_object_key(&object.name)?,
                last_modified: listed_last_modified(object.mod_time),
                // Compressed legacy objects may retain an unknown (-1) logical-size sentinel; never
                // expose that internal value.
                size: object.get_actual_size_or_physical(),
                e_tag: listed_etag(object),
                storage_class: listed_storage_class(object.storage_class.as_deref()),
                owner: fetch_owner.then(dto_values::owner),
                ..Default::default()
            })
        })
        .collect::<S3Result<_>>()?;
    let common_prefixes = info
        .prefixes
        .iter()
        .map(|prefix| gateway_dto::CommonPrefix { prefix: prefix.clone() })
        .collect();
    Ok((contents, common_prefixes))
}

/// The entity tag a listing shows: none for an object whose tag is an encryption fingerprint.
pub(super) fn listed_etag(object: &ObjectInfo) -> Option<rustfs_gateway_types::ETag> {
    object
        .etag
        .as_deref()
        .filter(|_| rustfs_utils::http::etag_can_be_listed(&object.user_defined))
        .map(dto_values::stored_etag)
}

/// A listing entry's `LastModified`. An entry the store holds without one (an on-demand migration
/// source that omits it) carries the placeholder, which the gateway writes as no element, as
/// legacy RustFS does (rustfs/gateway#1613).
pub(super) fn listed_last_modified(mod_time: Option<time::OffsetDateTime>) -> rustfs_gateway_types::Timestamp {
    mod_time.map(dto_values::timestamp).unwrap_or_default()
}

/// A listing entry's `StorageClass`, absent as [`listed_last_modified`] is.
pub(super) fn listed_storage_class(storage_class: Option<&str>) -> gateway_dto::StorageClass {
    storage_class
        .map(|storage_class| gateway_dto::StorageClass::custom(storage_class.to_owned()))
        .unwrap_or_default()
}

/// ListObjects' `NextMarker`: on a truncated page, the key the walker stopped at (the cursor the
/// store returned), else the last entry in raw name order.
fn list_objects_next_marker(info: &ListObjectsV2Info, cursor: Option<&str>) -> Option<String> {
    if !info.is_truncated {
        return None;
    }
    if let Some(cursor) = cursor {
        // V2 cursors protect literal cache-shaped keys with an outer tag. V1 exposes the key
        // itself, which is sent back as a literal marker.
        // RUSTFS_COMPAT_TODO(listing-cursor-legacy-tag): Remove after supported upgrade sources stop issuing tagged V2 cursors.
        let key = cursor
            .strip_suffix("[rustfs_cache:v2,return:]")
            .filter(|key| key.rfind("[rustfs_cache:").is_some_and(|start| key[start..].contains(']')))
            .unwrap_or(cursor);
        return Some(key.to_owned());
    }
    let last_key = info
        .objects
        .iter()
        .rev()
        .find(|object| !object.name.is_empty())
        .map(|object| &object.name);
    last_key.into_iter().chain(info.prefixes.last()).max().cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::bucket_usecase::test_support::build_request;
    use rustfs_gateway_types::BucketName;

    fn object(name: &str) -> ObjectInfo {
        ObjectInfo {
            name: name.to_owned(),
            mod_time: Some(time::OffsetDateTime::UNIX_EPOCH),
            storage_class: Some("STANDARD".to_owned()),
            ..Default::default()
        }
    }

    fn page(objects: Vec<ObjectInfo>, prefixes: &[&str]) -> ListObjectsV2Info {
        ListObjectsV2Info {
            objects,
            prefixes: prefixes.iter().map(|prefix| (*prefix).to_owned()).collect(),
            ..Default::default()
        }
    }

    fn params(continuation_token: Option<&str>, start_after: Option<&str>) -> ListObjectsV2Params {
        parse_list_objects_v2_params(
            Some("prefix a/".to_owned()),
            Some("/".to_owned()),
            None,
            continuation_token.map(str::to_owned),
            start_after.map(str::to_owned),
        )
        .expect("valid parameters")
    }

    fn v2(info: ListObjectsV2Info) -> gateway_dto::ListObjectsV2Output {
        list_objects_v2_output(info, "bucket", params(None, None), false, None).expect("a representable page")
    }

    fn v1_next_marker(info: ListObjectsV2Info) -> Option<String> {
        list_objects_output(info, "bucket", params(None, None), None, true, None)
            .expect("a representable page")
            .next_marker
    }

    fn envelope() -> RequestEnvelope {
        RequestEnvelope::from_s3s(&build_request((), http::Method::GET))
    }

    #[test]
    fn key_count_includes_objects_and_prefixes_and_a_keyless_entry_is_not_an_object() {
        let output = v2(page(vec![object("obj-a"), object("")], &["p1/", "p2/"]));

        assert_eq!(output.key_count, 3);
        assert_eq!(output.contents.len(), 1);
        assert_eq!(output.common_prefixes.len(), 2);
    }

    /// The use case answers stored values raw; `encoding-type=url` is the writing edge's.
    #[test]
    fn stored_values_and_the_request_echo_are_answered_raw() {
        let info = page(vec![object("dir a/file+b%.txt")], &["prefix a/sub+"]);
        let output = list_objects_v2_output(
            info,
            "bucket-b",
            params(None, Some("start after")),
            true,
            Some(gateway_dto::EncodingType::URL),
        )
        .expect("a representable page");

        assert_eq!(output.contents[0].key.as_str(), "dir a/file+b%.txt");
        assert_eq!(output.common_prefixes[0].prefix, "prefix a/sub+");
        assert_eq!(output.prefix, "prefix a/");
        assert_eq!(output.delimiter.as_deref(), Some("/"));
        assert_eq!(output.start_after.as_deref(), Some("start after"));
        assert_eq!(output.encoding_type.as_ref().map(|encoding| encoding.as_str()), Some("url"));
        assert_eq!(output.name.as_str(), "bucket-b");
        let owner = output.contents[0].owner.as_ref().expect("fetch-owner lists the owner");
        assert_eq!(owner.id, dto_values::owner().id);
    }

    #[test]
    fn the_next_continuation_token_is_base64_of_the_cursor_and_the_request_token_is_echoed() {
        let wire = encode_list_objects_v2_continuation_token("echo");
        let info = ListObjectsV2Info {
            next_continuation_token: Some("token-123".to_owned()),
            ..Default::default()
        };
        let output =
            list_objects_v2_output(info, "bucket", params(Some(&wire), None), false, None).expect("a representable page");

        assert_eq!(output.continuation_token.as_ref().map(OpaqueString::as_str), Some(wire.as_str()));
        let next = output.next_continuation_token.expect("a truncated page carries a token");
        assert_eq!(next.as_str(), base64_simd::STANDARD.encode_to_string(b"token-123"));
        assert_eq!(params(Some(next.as_str()), None).decoded_continuation_token.as_deref(), Some("token-123"));
    }

    #[test]
    fn a_compressed_object_never_lists_the_unknown_size_sentinel() {
        let mut metadata = HashMap::new();
        rustfs_utils::http::insert_str(&mut metadata, rustfs_utils::http::SUFFIX_COMPRESSION, "klauspost/compress/s2".to_owned());
        let output = v2(page(
            vec![ObjectInfo {
                size: 128,
                actual_size: -1,
                user_defined: Arc::new(metadata),
                ..object("legacy-compressed")
            }],
            &[],
        ));

        assert_eq!(output.contents[0].size, 128);
    }

    #[test]
    fn an_encryption_fingerprint_is_never_listed_as_an_entity_tag() {
        let mut protected = HashMap::from([("x-amz-server-side-encryption-customer-algorithm".to_owned(), "AES256".to_owned())]);
        rustfs_utils::http::metadata_compat::insert_str(
            &mut protected,
            rustfs_utils::http::SUFFIX_OPAQUE_ENCRYPTED_ETAG,
            "v1".to_owned(),
        );
        let metadata = [
            HashMap::from([("x-amz-server-side-encryption-customer-algorithm".to_owned(), "AES256".to_owned())]),
            HashMap::from([("x-amz-server-side-encryption".to_owned(), "aws:kms".to_owned())]),
            protected,
            HashMap::new(),
            HashMap::from([("x-rustfs-encryption-original-size".to_owned(), "42".to_owned())]),
        ];
        let objects = metadata
            .into_iter()
            .enumerate()
            .map(|(index, metadata)| ObjectInfo {
                etag: Some("0123456789abcdef0123456789abcdef".to_owned()),
                user_defined: Arc::new(metadata),
                ..object(&format!("object-{index}"))
            })
            .collect();

        let output = v2(page(objects, &[]));

        assert_eq!(output.contents.len(), 5);
        for (index, entry) in output.contents.iter().enumerate() {
            assert_eq!(entry.e_tag.is_some(), index >= 2, "object-{index}");
        }
    }

    #[test]
    fn a_legacy_label_only_storage_class_lists_as_standard() {
        let file_info = rustfs_filemeta::FileInfo {
            name: "legacy-object".to_owned(),
            mod_time: Some(time::OffsetDateTime::UNIX_EPOCH),
            metadata: HashMap::from([(rustfs_utils::http::headers::AMZ_STORAGE_CLASS.to_owned(), "STANDARD_IA".to_owned())]),
            ..Default::default()
        };
        let output = v2(page(vec![ObjectInfo::from_file_info(&file_info, "bucket", "legacy-object", true)], &[]));

        assert_eq!(output.contents[0].storage_class.as_str(), "STANDARD");
    }

    /// The stored entity tags the gateway's strict constructor refuses (rustfs/gateway#1611):
    /// each is listed as stored, on the object it belongs to.
    #[test]
    fn every_stored_entity_tag_is_listed_as_stored() {
        let stored = ["\"\"", "ab\tcd", "</ETag><Contents><Key>victim</Key></Contents><ETag>&amp;"];
        let objects = stored
            .iter()
            .enumerate()
            .map(|(index, etag)| ObjectInfo {
                etag: Some((*etag).to_owned()),
                ..object(&format!("object-{index}"))
            })
            .collect();

        let output = v2(page(objects, &[]));

        let listed: Vec<_> = output.contents.iter().map(|entry| entry.e_tag.is_some()).collect();
        assert_eq!(listed, [true; 3]);
    }

    /// A fact the store does not hold stays unheld: the placeholder the gateway writes as no
    /// element (rustfs/gateway#1613). A fact it holds is never replaced by one.
    #[test]
    fn an_unheld_modification_time_or_storage_class_stays_unheld() {
        use rustfs_gateway_types::WirePlaceholder as _;
        let entries = vec![
            ObjectInfo {
                mod_time: None,
                ..object("no-time")
            },
            ObjectInfo {
                storage_class: None,
                ..object("no-class")
            },
            object("both"),
        ];

        let output = v2(page(entries, &[]));

        let unheld: Vec<_> = output
            .contents
            .iter()
            .map(|entry| {
                (
                    entry.key.as_str(),
                    entry.last_modified.is_wire_placeholder(),
                    entry.storage_class.is_wire_placeholder(),
                )
            })
            .collect();
        assert_eq!(unheld, [("no-time", true, false), ("no-class", false, true), ("both", false, false)]);
    }

    #[test]
    fn list_objects_echoes_the_request_marker_or_an_empty_one() {
        for (marker, echoed) in [(Some("logs and more/start after"), "logs and more/start after"), (None, "")] {
            let output =
                list_objects_output(page(vec![], &[]), "bucket", params(None, None), marker.map(str::to_owned), true, None)
                    .expect("a representable page");
            assert_eq!(output.marker, echoed);
        }
    }

    /// NextMarker on a truncated page: the store's cursor (with the legacy outer tag removed) when
    /// it may be shown, else the last entry in raw name order; none on a complete page.
    #[test]
    fn list_objects_next_marker_follows_the_cursor_then_the_last_raw_entry() {
        let truncated = |objects: Vec<ObjectInfo>, prefixes: &[&str], cursor: Option<&str>| ListObjectsV2Info {
            is_truncated: true,
            next_continuation_token: cursor.map(str::to_owned),
            ..page(objects, prefixes)
        };
        let cache_shaped = "key[rustfs_cache:v2,id:list-cache-id,src:walker,gen:live]";
        let cases: [(ListObjectsV2Info, Option<&str>); 9] = [
            (truncated(vec![object("apple")], &["zebra/"], None), Some("zebra/")),
            (truncated(vec![object("z")], &["~/"], None), Some("~/")),
            (truncated(vec![], &["data-"], Some("data-1001")), Some("data-1001")),
            (truncated(vec![object(cache_shaped)], &[], Some(cache_shaped)), Some(cache_shaped)),
            (
                truncated(vec![], &[], Some("key[rustfs_cache:v2,return:][rustfs_cache:v2,return:]")),
                Some("key[rustfs_cache:v2,return:]"),
            ),
            (
                truncated(vec![], &[], Some("key[rustfs_cache:v1,id:x]suffix[rustfs_cache:v2,return:]")),
                Some("key[rustfs_cache:v1,id:x]suffix"),
            ),
            (
                truncated(
                    vec![],
                    &[],
                    Some("key[rustfs_cache:v1,id:x][rustfs_cache:v2,return:][rustfs_cache:v2,return:]"),
                ),
                Some("key[rustfs_cache:v1,id:x][rustfs_cache:v2,return:]"),
            ),
            (
                truncated(vec![], &[], Some("key[rustfs_cache:v2,return:]")),
                Some("key[rustfs_cache:v2,return:]"),
            ),
            (page(vec![object("only-item")], &[]), None),
        ];
        for (info, expected) in cases {
            assert_eq!(v1_next_marker(info).as_deref(), expected);
        }
    }

    #[test]
    fn a_sealed_cursor_is_never_a_next_marker() {
        let info = ListObjectsV2Info {
            is_truncated: true,
            next_continuation_token: Some("sealed".to_owned()),
            ..page(vec![object("visible")], &[])
        };
        let output = list_objects_output(info, "bucket", params(None, None), None, false, None).expect("a representable page");

        assert_eq!(output.next_marker.as_deref(), Some("visible"));
    }

    #[tokio::test]
    async fn list_objects_v2_refuses_a_negative_max_keys() {
        let input = gateway_dto::ListObjectsV2Input {
            bucket: BucketName::new("test-bucket").expect("bucket"),
            max_keys: Some(-1),
            ..Default::default()
        };

        let err = DefaultBucketUsecase::without_context()
            .execute_list_objects_v2(envelope(), input)
            .await
            .expect_err("a negative page size");
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    async fn list_objects_v2_refuses_an_undecodable_continuation_token_before_the_store() {
        let input = gateway_dto::ListObjectsV2Input {
            bucket: BucketName::new("test-bucket").expect("bucket"),
            continuation_token: Some(OpaqueString::from("%%%".to_owned())),
            ..Default::default()
        };

        let err = DefaultBucketUsecase::without_context()
            .execute_list_objects_v2(envelope(), input)
            .await
            .expect_err("an undecodable token");
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        assert_eq!(err.message(), Some("Invalid continuation token"));
    }

    #[tokio::test]
    async fn list_objects_returns_internal_error_when_the_store_is_not_initialized() {
        let input = gateway_dto::ListObjectsInput {
            bucket: BucketName::new("test-bucket").expect("bucket"),
            ..Default::default()
        };

        let err = DefaultBucketUsecase::without_context()
            .execute_list_objects(envelope(), input)
            .await
            .expect_err("no store");
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }
}
