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

//! Object and version listing use-case operations, including the metadata listing extension.

use super::*;
use crate::app::storage_api::bucket_usecase::contract::list::ListOperations as _;
use crate::app::storage_api::s3::dto::{
    CommonPrefix, DeleteMarkerEntry, EncodingType, ListObjectVersionsInput, ListObjectVersionsOutput, ListObjectsInput,
    ListObjectsOutput, ListObjectsV2Input, ListObjectsV2Output, MetadataEntry, Object, ObjectStorageClass, ObjectVersion,
    ObjectVersionStorageClass, Timestamp, UserMetadata,
};
use crate::app::storage_api::s3::xml::{SerResult, SerializeContent};
use crate::app::storage_api::s3::{S3Error, S3ErrorCode, S3Request, S3Response, S3Result, xml};

type ListObjectVersionsInfo = StorageListObjectVersionsInfo<ObjectInfo>;

type ListObjectsV2Info = StorageListObjectsV2Info<ObjectInfo>;

const XMLNS_S3: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct ObjectInternalInfo {
    pub k: i32,
    pub m: i32,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct ObjectMetadataExtension {
    pub user_metadata: Option<UserMetadata>,
    pub user_tags: Option<String>,
    pub internal: Option<ObjectInternalInfo>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ListObjectVersionMetadataEntry {
    DeleteMarker(DeleteMarkerEntry, ObjectMetadataExtension),
    Version(ObjectVersion, ObjectMetadataExtension),
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct ListObjectVersionsMetadataOutput {
    pub output: ListObjectVersionsOutput,
    pub entries: Vec<ListObjectVersionMetadataEntry>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct ObjectMetadataEntry {
    pub object: Object,
    pub extension: ObjectMetadataExtension,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct ListObjectsV2MetadataOutput {
    pub output: ListObjectsV2Output,
    pub contents: Vec<ObjectMetadataEntry>,
}

impl xml::Serialize for ListObjectVersionsMetadataOutput {
    fn serialize<W: Write>(&self, s: &mut xml::Serializer<W>) -> SerResult {
        s.content_with_ns("ListVersionsResult", XMLNS_S3, self)
    }
}

impl SerializeContent for ListObjectVersionsMetadataOutput {
    fn serialize_content<W: Write>(&self, s: &mut xml::Serializer<W>) -> SerResult {
        if let Some(iter) = &self.output.common_prefixes {
            s.flattened_list("CommonPrefixes", iter)?;
        }
        if let Some(ref val) = self.output.delimiter {
            s.content("Delimiter", val)?;
        }
        if let Some(ref val) = self.output.encoding_type {
            s.content("EncodingType", val)?;
        }
        if let Some(ref val) = self.output.is_truncated {
            s.content("IsTruncated", val)?;
        }
        if let Some(ref val) = self.output.key_marker {
            s.content("KeyMarker", val)?;
        }
        if let Some(ref val) = self.output.max_keys {
            s.content("MaxKeys", val)?;
        }
        if let Some(ref val) = self.output.name {
            s.content("Name", val)?;
        }
        if let Some(ref val) = self.output.next_key_marker {
            s.content("NextKeyMarker", val)?;
        }
        if let Some(ref val) = self.output.next_version_id_marker {
            s.content("NextVersionIdMarker", val)?;
        }
        if let Some(ref val) = self.output.prefix {
            s.content("Prefix", val)?;
        }
        if let Some(ref val) = self.output.version_id_marker {
            s.content("VersionIdMarker", val)?;
        }
        for entry in &self.entries {
            match entry {
                ListObjectVersionMetadataEntry::DeleteMarker(marker, extension) => {
                    s.content(
                        "DeleteMarker",
                        &VersionMetadataContent {
                            value: marker,
                            extension,
                        },
                    )?;
                }
                ListObjectVersionMetadataEntry::Version(version, extension) => {
                    s.content(
                        "Version",
                        &VersionMetadataContent {
                            value: version,
                            extension,
                        },
                    )?;
                }
            }
        }
        Ok(())
    }
}

impl xml::Serialize for ListObjectsV2MetadataOutput {
    fn serialize<W: Write>(&self, s: &mut xml::Serializer<W>) -> SerResult {
        s.content_with_ns("ListBucketResult", XMLNS_S3, self)
    }
}

impl SerializeContent for ListObjectsV2MetadataOutput {
    fn serialize_content<W: Write>(&self, s: &mut xml::Serializer<W>) -> SerResult {
        if let Some(ref val) = self.output.name {
            s.content("Name", val)?;
        }
        if let Some(ref val) = self.output.prefix {
            s.content("Prefix", val)?;
        }
        if let Some(ref val) = self.output.max_keys {
            s.content("MaxKeys", val)?;
        }
        if let Some(ref val) = self.output.key_count {
            s.content("KeyCount", val)?;
        }
        if let Some(ref val) = self.output.continuation_token {
            s.content("ContinuationToken", val)?;
        }
        if let Some(ref val) = self.output.is_truncated {
            s.content("IsTruncated", val)?;
        }
        if let Some(ref val) = self.output.next_continuation_token {
            s.content("NextContinuationToken", val)?;
        }
        for entry in &self.contents {
            s.content("Contents", entry)?;
        }
        if let Some(iter) = &self.output.common_prefixes {
            s.flattened_list("CommonPrefixes", iter)?;
        }
        if let Some(ref val) = self.output.delimiter {
            s.content("Delimiter", val)?;
        }
        if let Some(ref val) = self.output.encoding_type {
            s.content("EncodingType", val)?;
        }
        if let Some(ref val) = self.output.start_after {
            s.content("StartAfter", val)?;
        }
        Ok(())
    }
}

struct VersionMetadataContent<'a, T> {
    value: &'a T,
    extension: &'a ObjectMetadataExtension,
}

impl<T: SerializeContent> SerializeContent for VersionMetadataContent<'_, T> {
    fn serialize_content<W: Write>(&self, s: &mut xml::Serializer<W>) -> SerResult {
        self.value.serialize_content(s)?;
        self.extension.serialize_content(s)
    }
}

impl SerializeContent for ObjectMetadataEntry {
    fn serialize_content<W: Write>(&self, s: &mut xml::Serializer<W>) -> SerResult {
        self.object.serialize_content(s)?;
        self.extension.serialize_content(s)
    }
}

/// Whether `name` is a legal XML 1.0 element name.
///
/// User metadata keys are emitted as XML *element names* in the `metadata=true`
/// listing (`<key>value</key>`). Keys derived from HTTP headers can legally
/// contain characters that are illegal in an XML `Name` (spaces, `$`, `%`, `#`,
/// leading digits, control bytes, ...). Emitting such a key verbatim as a tag
/// produces a malformed document that the console's XML parser rejects wholesale,
/// blanking the entire prefix in the Web UI while plain `ListObjectsV2` (which
/// never serializes user metadata) keeps working. See issue #2743.
fn is_valid_xml_name(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    let is_name_start = |c: char| c == '_' || c == ':' || c.is_alphabetic();
    let is_name_char = |c: char| is_name_start(c) || c == '-' || c == '.' || c.is_numeric();
    is_name_start(first) && chars.all(is_name_char)
}

/// Drop characters that are illegal in XML 1.0 text content.
///
/// XML 1.0 permits tab/newline/carriage-return but forbids the other C0 control
/// characters. The serializer escapes `< > & ' "` but does not strip these, so a
/// metadata value carrying e.g. a `\u{1}` byte would still emit an unparsable
/// document. Returns a borrowed slice when nothing needs stripping.
fn sanitize_xml_text(value: &str) -> Cow<'_, str> {
    let is_illegal = |c: char| (c as u32) < 0x20 && c != '\t' && c != '\n' && c != '\r';
    if value.contains(is_illegal) {
        Cow::Owned(value.chars().filter(|c| !is_illegal(*c)).collect())
    } else {
        Cow::Borrowed(value)
    }
}

impl SerializeContent for ObjectMetadataExtension {
    fn serialize_content<W: Write>(&self, s: &mut xml::Serializer<W>) -> SerResult {
        if let Some(metadata) = &self.user_metadata {
            s.element("UserMetadata", |s| {
                for entry in metadata {
                    if let (Some(name), Some(value)) = (&entry.name, &entry.value) {
                        // Skip entries whose key is not a valid XML element name and strip
                        // XML-illegal control characters from the value, so a single poison
                        // object can never corrupt the whole listing document. See issue #2743.
                        if is_valid_xml_name(name) {
                            let sanitized = sanitize_xml_text(value);
                            s.content(name, sanitized.as_ref())?;
                        }
                    }
                }
                Ok(())
            })?;
        }
        if let Some(tags) = &self.user_tags {
            s.content("UserTags", tags)?;
        }
        if let Some(internal) = &self.internal {
            s.content("Internal", internal)?;
        }
        Ok(())
    }
}

impl SerializeContent for ObjectInternalInfo {
    fn serialize_content<W: Write>(&self, s: &mut xml::Serializer<W>) -> SerResult {
        s.content("K", &self.k)?;
        s.content("M", &self.m)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct ObjectMetadataPermissions {
    metadata_allowed: bool,
    tags_allowed: bool,
}

fn encode_list_versions_value(value: &str, encoding_type: Option<&EncodingType>) -> String {
    if encoding_type.is_some_and(|encoding| encoding.as_str() == EncodingType::URL) {
        encode(value).into_owned()
    } else {
        value.to_string()
    }
}

fn encode_list_objects_v2_value(value: &str, encoding_type: Option<&EncodingType>) -> String {
    if encoding_type.is_some_and(|encoding| encoding.as_str() == EncodingType::URL) {
        value
            .split('/')
            .map(|part| encode(part).into_owned())
            .collect::<Vec<_>>()
            .join("/")
    } else {
        value.to_string()
    }
}

fn build_metadata_extension_user_metadata(user_defined: &HashMap<String, String>) -> Option<UserMetadata> {
    // Even builds unable to decrypt MinIO SSE-C must not expose its key-protected metadata.
    if crate::app::storage_api::bucket_usecase::sse::stored_ssec_metadata(user_defined)
        || user_defined.contains_key(rustfs_utils::http::object_encryption_keys::MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER)
    {
        return None;
    }
    let mut items = extract_user_defined_metadata(user_defined)
        .into_iter()
        .filter(|(key, _)| !key.is_empty())
        .map(|(key, value)| MetadataEntry {
            name: Some(key),
            value: Some(value),
        })
        .collect::<Vec<_>>();
    items.sort_by(|left, right| left.name.cmp(&right.name));

    if items.is_empty() { None } else { Some(items) }
}

fn metadata_count_to_i32(value: usize) -> i32 {
    i32::try_from(value).unwrap_or(i32::MAX)
}

async fn is_list_objects_metadata_action_allowed<T>(
    req: &S3Request<T>,
    bucket: &str,
    object: &str,
    action: S3Action,
) -> S3Result<bool> {
    let mut auth_req = S3Request {
        input: (),
        method: req.method.clone(),
        uri: req.uri.clone(),
        headers: req.headers.clone(),
        extensions: req.extensions.clone(),
        credentials: req.credentials.clone(),
        region: req.region.clone(),
        service: req.service.clone(),
        trailing_headers: req.trailing_headers.clone(),
    };

    let mut req_info = req_info_ref(req)?.clone();
    req_info.bucket = Some(bucket.to_string());
    req_info.object = Some(object.to_string());
    req_info.version_id = None;
    // Denial here is an expected filter outcome, not an error (issue #5740).
    req_info.suppress_denial_log = true;
    auth_req.extensions.insert(req_info);

    match authorize_request(&mut auth_req, Action::S3Action(action)).await {
        Ok(()) => Ok(true),
        Err(err) if err.code() == &S3ErrorCode::AccessDenied => Ok(false),
        Err(err) => Err(err),
    }
}

async fn collect_list_objects_metadata_permissions<T>(
    req: &S3Request<T>,
    bucket: &str,
    objects: &[ObjectInfo],
) -> S3Result<HashMap<String, ObjectMetadataPermissions>> {
    let mut permissions = HashMap::new();

    for object in objects {
        if object.name.is_empty() || permissions.contains_key(&object.name) {
            continue;
        }

        let metadata_allowed =
            is_list_objects_metadata_action_allowed(req, bucket, &object.name, S3Action::GetObjectAction).await?;
        let tags_allowed =
            is_list_objects_metadata_action_allowed(req, bucket, &object.name, S3Action::GetObjectTaggingAction).await?;

        permissions.insert(
            object.name.clone(),
            ObjectMetadataPermissions {
                metadata_allowed,
                tags_allowed,
            },
        );
    }

    Ok(permissions)
}

fn build_list_object_versions_metadata_output(
    object_infos: ListObjectVersionsInfo,
    bucket: &str,
    params: &ListObjectVersionsParams,
    encoding_type: Option<&EncodingType>,
    permissions: &HashMap<String, ObjectMetadataPermissions>,
) -> ListObjectVersionsMetadataOutput {
    let owner = rustfs_owner();
    let common_prefixes = object_infos
        .prefixes
        .into_iter()
        .map(|prefix_value| CommonPrefix {
            prefix: Some(encode_list_versions_value(&prefix_value, encoding_type)),
        })
        .collect::<Vec<_>>();

    let entries = object_infos
        .objects
        .into_iter()
        .filter(|object| !object.name.is_empty())
        .map(|object| {
            let object_name = encode_list_versions_value(&object.name, encoding_type);
            // A null version surfaces as `Some(Uuid::nil())` on the
            // client-facing `ObjectInfo`; AWS advertises it as the literal
            // string `null`, and a nil UUID must never reach the wire
            // (issue #6745).
            let version_id = object
                .version_id
                .filter(|version| !version.is_nil())
                .map(|version| version.to_string())
                .unwrap_or_else(|| "null".to_string());
            let permission = permissions.get(&object.name).copied().unwrap_or_default();
            let user_metadata = if permission.metadata_allowed {
                build_metadata_extension_user_metadata(&object.user_defined)
            } else {
                None
            };
            let user_tags = if permission.tags_allowed && !object.user_tags.is_empty() {
                Some((*object.user_tags).clone())
            } else {
                None
            };
            let internal = if permission.metadata_allowed && (object.data_blocks > 0 || object.parity_blocks > 0) {
                Some(ObjectInternalInfo {
                    k: metadata_count_to_i32(object.data_blocks),
                    m: metadata_count_to_i32(object.parity_blocks),
                })
            } else {
                None
            };

            let extension = ObjectMetadataExtension {
                user_metadata,
                user_tags,
                internal,
            };

            if object.delete_marker {
                ListObjectVersionMetadataEntry::DeleteMarker(
                    DeleteMarkerEntry {
                        key: Some(object_name),
                        last_modified: object.mod_time.map(Timestamp::from),
                        owner: Some(owner.clone()),
                        version_id: Some(version_id),
                        is_latest: Some(object.is_latest),
                    },
                    extension,
                )
            } else {
                ListObjectVersionMetadataEntry::Version(
                    ObjectVersion {
                        key: Some(object_name),
                        last_modified: object.mod_time.map(Timestamp::from),
                        size: Some(object.size),
                        version_id: Some(version_id),
                        is_latest: Some(object.is_latest),
                        e_tag: object
                            .etag
                            .as_ref()
                            .filter(|_| rustfs_utils::http::etag_can_be_listed(&object.user_defined))
                            .map(|etag| to_s3s_xml_etag(etag)),
                        storage_class: Some(ObjectVersionStorageClass::from(
                            object
                                .storage_class
                                .unwrap_or_else(|| ObjectVersionStorageClass::STANDARD.to_string()),
                        )),
                        owner: Some(owner.clone()),
                        ..Default::default()
                    },
                    extension,
                )
            }
        })
        .collect::<Vec<_>>();

    let next_key_marker = object_infos
        .next_marker
        .filter(|marker| !marker.is_empty())
        .map(|marker| encode_list_versions_value(&marker, encoding_type));
    let next_version_id_marker = object_infos.next_version_idmarker.filter(|marker| !marker.is_empty());

    ListObjectVersionsMetadataOutput {
        output: ListObjectVersionsOutput {
            common_prefixes: Some(common_prefixes),
            delimiter: params
                .delimiter
                .clone()
                .map(|value| encode_list_versions_value(&value, encoding_type)),
            encoding_type: encoding_type.cloned(),
            is_truncated: Some(object_infos.is_truncated),
            key_marker: Some(encode_list_versions_value(
                params.key_marker.as_deref().unwrap_or_default(),
                encoding_type,
            )),
            max_keys: Some(params.max_keys),
            name: Some(bucket.to_owned()),
            next_key_marker,
            next_version_id_marker,
            prefix: Some(encode_list_versions_value(&params.prefix, encoding_type)),
            request_charged: None,
            version_id_marker: Some(params.version_id_marker.clone().unwrap_or_default()),
            ..Default::default()
        },
        entries,
    }
}

fn build_list_objects_v2_metadata_output(
    object_infos: ListObjectsV2Info,
    bucket: &str,
    params: &ListObjectsV2Params,
    encoding_type: Option<&EncodingType>,
    fetch_owner: bool,
    permissions: &HashMap<String, ObjectMetadataPermissions>,
) -> ListObjectsV2MetadataOutput {
    let owner = rustfs_owner();

    let contents = object_infos
        .objects
        .iter()
        .filter(|object| !object.name.is_empty())
        .map(|object| {
            let permission = permissions.get(&object.name).copied().unwrap_or_default();
            let user_metadata = if permission.metadata_allowed {
                build_metadata_extension_user_metadata(&object.user_defined)
            } else {
                None
            };
            let user_tags = if permission.tags_allowed && !object.user_tags.is_empty() {
                Some((*object.user_tags).clone())
            } else {
                None
            };
            let internal = if permission.metadata_allowed && (object.data_blocks > 0 || object.parity_blocks > 0) {
                Some(ObjectInternalInfo {
                    k: metadata_count_to_i32(object.data_blocks),
                    m: metadata_count_to_i32(object.parity_blocks),
                })
            } else {
                None
            };

            ObjectMetadataEntry {
                object: Object {
                    key: Some(encode_list_objects_v2_value(&object.name, encoding_type)),
                    last_modified: object.mod_time.map(Timestamp::from),
                    size: Some(object.get_actual_size_or_physical()),
                    e_tag: object
                        .etag
                        .as_ref()
                        .filter(|_| rustfs_utils::http::etag_can_be_listed(&object.user_defined))
                        .map(|etag| to_s3s_xml_etag(etag)),
                    storage_class: Some(ObjectStorageClass::from(
                        object
                            .storage_class
                            .clone()
                            .unwrap_or_else(|| ObjectStorageClass::STANDARD.to_string()),
                    )),
                    owner: fetch_owner.then_some(owner.clone()),
                    ..Default::default()
                },
                extension: ObjectMetadataExtension {
                    user_metadata,
                    user_tags,
                    internal,
                },
            }
        })
        .collect::<Vec<_>>();

    let common_prefixes = object_infos
        .prefixes
        .into_iter()
        .map(|prefix| CommonPrefix {
            prefix: Some(encode_list_objects_v2_value(&prefix, encoding_type)),
        })
        .collect::<Vec<_>>();

    let key_count = metadata_count_to_i32(contents.len() + common_prefixes.len());
    let next_continuation_token = object_infos
        .next_continuation_token
        .map(|cursor| encode_list_objects_v2_continuation_token(&cursor));

    ListObjectsV2MetadataOutput {
        output: ListObjectsV2Output {
            name: Some(bucket.to_owned()),
            prefix: Some(params.prefix.clone()),
            max_keys: Some(params.max_keys),
            key_count: Some(key_count),
            continuation_token: params.response_continuation_token.clone(),
            is_truncated: Some(object_infos.is_truncated),
            next_continuation_token,
            common_prefixes: Some(common_prefixes),
            delimiter: params.delimiter.clone(),
            encoding_type: encoding_type.cloned(),
            start_after: params.response_start_after.clone(),
            ..Default::default()
        },
        contents,
    }
}

impl DefaultBucketUsecase {
    #[instrument(level = "trace", skip(self, req))]
    pub async fn execute_list_objects_v2(&self, req: S3Request<ListObjectsV2Input>) -> S3Result<S3Response<ListObjectsV2Output>> {
        self.execute_list_objects_v2_inner(req, true).await
    }

    async fn execute_list_objects_v2_inner(
        &self,
        mut req: S3Request<ListObjectsV2Input>,
        allow_list_through: bool,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let ListObjectsV2Input {
            bucket,
            continuation_token,
            delimiter,
            encoding_type,
            fetch_owner,
            max_keys,
            prefix,
            start_after,
            ..
        } = req.input.clone();

        let params = parse_list_objects_v2_params(prefix, delimiter, max_keys, continuation_token, start_after)?;

        validate_list_object_unordered_with_delimiter(params.delimiter.as_ref(), req.uri.query())?;

        let store = get_validated_store(&bucket).await?;

        let incl_deleted = get_header(&req.headers, rustfs_utils::http::SUFFIX_INCLUDE_DELETED)
            .map(|v| v.as_ref() == "true")
            .unwrap_or_default();
        let table_list_access = req.extensions.get::<TableDataPlaneListAccess>().cloned();

        // The on-demand migration envelope is decoded whether or not this
        // bucket still merges: a token handed out under `list_through` must keep
        // paginating after the policy is turned off (rustfs/backlog#2164).
        if allow_list_through {
            prepare_odm_read_generation(&store, &mut req, &bucket).await;
        }
        let (merged_token, source_state) = if allow_list_through {
            let source_state = list_through::list_through_state(&store, &bucket, &req, &params).await?;
            if table_list_access.is_some() {
                if source_state.is_some() {
                    return Err(S3Error::with_message(
                        S3ErrorCode::ServiceUnavailable,
                        "protected table listings are unavailable while on-demand migration list-through is active".to_string(),
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
        let (object_infos, degraded) = if let Some(access) = table_list_access.as_ref() {
            (
                table_list_isolation::list_objects_v2(
                    store.clone(),
                    access,
                    table_list_isolation::ListObjectsV2Request {
                        bucket: &bucket,
                        prefix: &params.prefix,
                        continuation_token: params.decoded_continuation_token.as_deref(),
                        delimiter: params.delimiter.as_deref(),
                        max_keys: params.max_keys,
                        start_after: params.start_after_for_query.as_deref(),
                        incl_deleted,
                        opaque_cursor_supported: allow_list_through,
                    },
                )
                .await?,
                false,
            )
        } else {
            match (source_state, merged_token.as_ref()) {
                (None, Some(token)) if params.max_keys == 0 => {
                    // No source was consulted, so retain every unconsumed side and
                    // the original wire format without spending its progress budget.
                    let is_truncated = !token.local_done || !token.source_done;
                    (
                        StorageListObjectsV2Info {
                            is_truncated,
                            next_continuation_token: params.decoded_continuation_token.clone().filter(|_| is_truncated),
                            ..Default::default()
                        },
                        false,
                    )
                }
                (None, None) => {
                    let infos = store
                        .list_objects_v2(
                            &bucket,
                            &params.prefix,
                            params.decoded_continuation_token.clone(),
                            params.delimiter.clone(),
                            params.max_keys,
                            fetch_owner.unwrap_or_default(),
                            params.start_after_for_query.clone(),
                            incl_deleted,
                        )
                        .await
                        .map_err(ApiError::from)?;
                    (infos, false)
                }
                (state, token) => {
                    let outcome = list_through::merged_list_objects_v2(
                        &store,
                        state.as_ref(),
                        &bucket,
                        &params,
                        fetch_owner.unwrap_or_default(),
                        incl_deleted,
                        token,
                    )
                    .await?;
                    (outcome.info, outcome.degraded)
                }
            }
        };

        let output = build_list_objects_v2_output(
            object_infos,
            fetch_owner.unwrap_or_default(),
            params.max_keys,
            bucket,
            params.prefix,
            params.delimiter,
            encoding_type,
            params.response_continuation_token,
            params.response_start_after,
        );

        let mut response = S3Response::new(output);
        if degraded {
            mark_on_demand_migration_list_local_only(&mut response.headers);
        }
        Ok(response)
    }

    pub(crate) async fn execute_list_objects_v2m(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2MetadataOutput>> {
        let input = req.input.clone();
        let ListObjectsV2Input {
            bucket,
            continuation_token,
            delimiter,
            encoding_type,
            fetch_owner,
            max_keys,
            prefix,
            start_after,
            ..
        } = input;

        let params = parse_list_objects_v2_params(prefix, delimiter, max_keys, continuation_token, start_after)?;

        validate_list_object_unordered_with_delimiter(params.delimiter.as_ref(), req.uri.query())?;

        let store = get_validated_store(&bucket).await?;
        let incl_deleted = get_header(&req.headers, rustfs_utils::http::SUFFIX_INCLUDE_DELETED)
            .map(|value| value.as_ref() == "true")
            .unwrap_or_default();

        let object_infos = match req.extensions.get::<TableDataPlaneListAccess>() {
            Some(access) => {
                table_list_isolation::list_objects_v2(
                    store.clone(),
                    access,
                    table_list_isolation::ListObjectsV2Request {
                        bucket: &bucket,
                        prefix: &params.prefix,
                        continuation_token: params.decoded_continuation_token.as_deref(),
                        delimiter: params.delimiter.as_deref(),
                        max_keys: params.max_keys,
                        start_after: params.start_after_for_query.as_deref(),
                        incl_deleted,
                        opaque_cursor_supported: true,
                    },
                )
                .await?
            }
            None => store
                .list_objects_v2(
                    &bucket,
                    &params.prefix,
                    params.decoded_continuation_token.clone(),
                    params.delimiter.clone(),
                    params.max_keys,
                    fetch_owner.unwrap_or_default(),
                    params.start_after_for_query.clone(),
                    incl_deleted,
                )
                .await
                .map_err(ApiError::from)?,
        };

        let permissions = collect_list_objects_metadata_permissions(&req, &bucket, &object_infos.objects).await?;
        let output = build_list_objects_v2_metadata_output(
            object_infos,
            &bucket,
            &params,
            encoding_type.as_ref(),
            fetch_owner.unwrap_or_default(),
            &permissions,
        );

        Ok(S3Response::new(output))
    }

    pub async fn execute_list_object_versions(
        &self,
        req: S3Request<ListObjectVersionsInput>,
    ) -> S3Result<S3Response<ListObjectVersionsOutput>> {
        let table_list_access = req.extensions.get::<TableDataPlaneListAccess>().cloned();
        let ListObjectVersionsInput {
            bucket,
            delimiter,
            encoding_type,
            key_marker,
            version_id_marker,
            max_keys,
            prefix,
            ..
        } = req.input;

        let params = parse_list_object_versions_params(prefix, delimiter, key_marker, version_id_marker, max_keys)?;

        let store = get_validated_store(&bucket).await?;

        let object_infos = match table_list_access.as_ref() {
            Some(access) => {
                table_list_isolation::list_object_versions(
                    store.clone(),
                    access,
                    table_list_isolation::ListObjectVersionsRequest {
                        bucket: &bucket,
                        prefix: &params.prefix,
                        key_marker: params.key_marker.as_deref(),
                        version_id_marker: params.version_id_marker.as_deref(),
                        delimiter: params.delimiter.as_deref(),
                        max_keys: params.max_keys,
                    },
                )
                .await?
            }
            None => store
                .list_object_versions(
                    &bucket,
                    &params.prefix,
                    params.key_marker.clone(),
                    params.version_id_marker.clone(),
                    params.delimiter.clone(),
                    params.max_keys,
                )
                .await
                .map_err(ApiError::from)?,
        };

        let output = build_list_object_versions_output(object_infos, bucket, &params, encoding_type.as_ref());

        Ok(S3Response::new(output))
    }

    pub(crate) async fn execute_list_object_versions_m(
        &self,
        req: S3Request<ListObjectVersionsInput>,
    ) -> S3Result<S3Response<ListObjectVersionsMetadataOutput>> {
        let input = req.input.clone();
        let ListObjectVersionsInput {
            bucket,
            delimiter,
            encoding_type,
            key_marker,
            version_id_marker,
            max_keys,
            prefix,
            ..
        } = input;

        let params = parse_list_object_versions_params(prefix, delimiter, key_marker, version_id_marker, max_keys)?;

        let store = get_validated_store(&bucket).await?;
        let object_infos = match req.extensions.get::<TableDataPlaneListAccess>() {
            Some(access) => {
                table_list_isolation::list_object_versions(
                    store.clone(),
                    access,
                    table_list_isolation::ListObjectVersionsRequest {
                        bucket: &bucket,
                        prefix: &params.prefix,
                        key_marker: params.key_marker.as_deref(),
                        version_id_marker: params.version_id_marker.as_deref(),
                        delimiter: params.delimiter.as_deref(),
                        max_keys: params.max_keys,
                    },
                )
                .await?
            }
            None => store
                .list_object_versions(
                    &bucket,
                    &params.prefix,
                    params.key_marker.clone(),
                    params.version_id_marker.clone(),
                    params.delimiter.clone(),
                    params.max_keys,
                )
                .await
                .map_err(ApiError::from)?,
        };

        let permissions = collect_list_objects_metadata_permissions(&req, &bucket, &object_infos.objects).await?;
        let output =
            build_list_object_versions_metadata_output(object_infos, &bucket, &params, encoding_type.as_ref(), &permissions);

        Ok(S3Response::new(output))
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_list_objects(&self, req: S3Request<ListObjectsInput>) -> S3Result<S3Response<ListObjectsOutput>> {
        let request_marker = req.input.marker.clone();
        let protected_table_list = req.extensions.get::<TableDataPlaneListAccess>().is_some();
        // V1 markers are object keys, so they cannot carry the opaque merged
        // pagination state used by V2 list-through.
        let mut v2_resp = self.execute_list_objects_v2_inner(req.map_input(Into::into), false).await?;
        if protected_table_list {
            v2_resp.output.next_continuation_token = None;
        }

        Ok(v2_resp.map_output(|v2| build_list_objects_output(v2, request_marker)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::bucket_usecase::test_support::build_request;
    use http::Method;

    /// Response bodies are not bucket configurations: they are written by the
    /// s3s encoder itself, as the legacy stack does.
    fn serialize_s3s_response<T: xml::Serialize>(value: &T) -> Vec<u8> {
        let mut buf = Vec::new();
        value
            .serialize(&mut xml::Serializer::new(&mut buf))
            .expect("metadata output should serialize");
        buf
    }

    #[tokio::test]
    async fn execute_list_object_versions_returns_internal_error_when_store_uninitialized() {
        let input = ListObjectVersionsInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_list_object_versions(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_list_object_versions_m_returns_internal_error_when_store_uninitialized() {
        let input = ListObjectVersionsInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_list_object_versions_m(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[test]
    fn keyless_object_and_version_listings_do_not_expose_legacy_encrypted_fingerprints() {
        let legacy_md5 = "fbfcbcc6d2f035411e6268c8b119593e";
        let mut protected_metadata =
            HashMap::from([("x-amz-server-side-encryption-customer-algorithm".to_owned(), "AES256".to_owned())]);
        rustfs_utils::http::metadata_compat::insert_str(
            &mut protected_metadata,
            rustfs_utils::http::SUFFIX_OPAQUE_ENCRYPTED_ETAG,
            "v1".to_owned(),
        );
        let objects = vec![
            ObjectInfo {
                name: "legacy-encrypted".to_owned(),
                etag: Some(legacy_md5.to_owned()),
                user_defined: Arc::new(HashMap::from([(
                    "x-amz-server-side-encryption-customer-algorithm".to_owned(),
                    "AES256".to_owned(),
                )])),
                ..Default::default()
            },
            ObjectInfo {
                name: "protected-encrypted".to_owned(),
                etag: Some("0123456789abcdef0123456789abcdef".to_owned()),
                user_defined: Arc::new(protected_metadata),
                ..Default::default()
            },
            ObjectInfo {
                name: "plain".to_owned(),
                etag: Some("9876543210abcdef9876543210abcdef".to_owned()),
                ..Default::default()
            },
            ObjectInfo {
                name: "legacy-plain-multipart".to_owned(),
                etag: Some("1b8a77dff5845270a8d81d86a28f38ea-2".to_owned()),
                user_defined: Arc::new(HashMap::from([("x-rustfs-encryption-original-size".to_owned(), "10485760".to_owned())])),
                ..Default::default()
            },
        ];
        let listings = build_list_objects_v2_metadata_output(
            ListObjectsV2Info {
                objects: objects.clone(),
                ..Default::default()
            },
            "demo-bucket",
            &ListObjectsV2Params {
                prefix: String::new(),
                max_keys: 1000,
                delimiter: None,
                response_start_after: None,
                start_after_for_query: None,
                response_continuation_token: None,
                decoded_continuation_token: None,
            },
            None,
            false,
            &HashMap::new(),
        );
        assert!(listings.contents[0].object.e_tag.is_none());
        assert!(listings.contents[1].object.e_tag.is_some());
        assert!(listings.contents[2].object.e_tag.is_some());
        assert_eq!(
            listings.contents[3].object.e_tag,
            Some(to_s3s_xml_etag("1b8a77dff5845270a8d81d86a28f38ea-2"))
        );
        let xml = String::from_utf8(serialize_s3s_response(&listings)).expect("UTF-8 objects");
        assert!(!xml.contains(legacy_md5));

        let versions = build_list_object_versions_metadata_output(
            ListObjectVersionsInfo {
                objects,
                ..Default::default()
            },
            "demo-bucket",
            &ListObjectVersionsParams {
                prefix: String::new(),
                max_keys: 1000,
                delimiter: None,
                key_marker: None,
                version_id_marker: None,
            },
            None,
            &HashMap::new(),
        );
        for (index, entry) in versions.entries.iter().enumerate() {
            let ListObjectVersionMetadataEntry::Version(version, _) = entry else {
                panic!("expected object version");
            };
            assert_eq!(version.e_tag, listings.contents[index].object.e_tag);
        }
        let xml = String::from_utf8(serialize_s3s_response(&versions)).expect("UTF-8 versions");
        assert!(!xml.contains(legacy_md5));
    }

    #[test]
    fn metadata_listings_hide_ssec_user_metadata_even_with_read_permission() {
        let objects = [
            ("customer-key", Some(("x-amz-server-side-encryption-customer-algorithm", "AES256"))),
            (
                "minio-customer-key",
                Some((
                    rustfs_utils::http::object_encryption_keys::MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER,
                    "sealed-key",
                )),
            ),
            ("server-key", Some(("x-amz-server-side-encryption", "AES256"))),
            ("plain", None),
        ]
        .into_iter()
        .map(|(name, encryption)| {
            let mut metadata = HashMap::from([("project".to_owned(), "private-project".to_owned())]);
            if let Some((key, value)) = encryption {
                metadata.insert(key.to_owned(), value.to_owned());
            }
            ObjectInfo {
                name: name.to_owned(),
                user_defined: Arc::new(metadata),
                ..Default::default()
            }
        })
        .collect::<Vec<_>>();
        let permissions = objects
            .iter()
            .map(|object| {
                (
                    object.name.clone(),
                    ObjectMetadataPermissions {
                        metadata_allowed: true,
                        tags_allowed: true,
                    },
                )
            })
            .collect::<HashMap<_, _>>();
        let listings = build_list_objects_v2_metadata_output(
            ListObjectsV2Info {
                objects: objects.clone(),
                ..Default::default()
            },
            "demo-bucket",
            &ListObjectsV2Params {
                prefix: String::new(),
                max_keys: 1000,
                delimiter: None,
                response_start_after: None,
                start_after_for_query: None,
                response_continuation_token: None,
                decoded_continuation_token: None,
            },
            None,
            false,
            &permissions,
        );
        assert!(listings.contents[0].extension.user_metadata.is_none());
        assert!(listings.contents[1].extension.user_metadata.is_none());
        assert!(listings.contents[2].extension.user_metadata.is_some());
        assert!(listings.contents[3].extension.user_metadata.is_some());
        let versions = build_list_object_versions_metadata_output(
            ListObjectVersionsInfo {
                objects,
                ..Default::default()
            },
            "demo-bucket",
            &ListObjectVersionsParams {
                prefix: String::new(),
                max_keys: 1000,
                delimiter: None,
                key_marker: None,
                version_id_marker: None,
            },
            None,
            &permissions,
        );
        for (index, entry) in versions.entries.iter().enumerate() {
            let ListObjectVersionMetadataEntry::Version(_, extension) = entry else {
                panic!("expected version");
            };
            assert_eq!(extension.user_metadata.is_some(), index >= 2);
        }
    }

    #[test]
    fn build_list_object_versions_metadata_output_maps_metadata_and_preserves_entry_order() {
        use time::macros::datetime;
        use uuid::Uuid;

        let object_infos = ListObjectVersionsInfo {
            is_truncated: true,
            next_marker: Some("obj-z".to_string()),
            next_version_idmarker: Some("null".to_string()),
            prefixes: vec!["logs/".to_string()],
            objects: vec![
                ObjectInfo {
                    bucket: "demo-bucket".to_string(),
                    name: "obj-a".to_string(),
                    mod_time: Some(datetime!(2025-01-01 00:00 UTC)),
                    size: 11,
                    user_defined: Arc::new(HashMap::from([("project".to_string(), "alpha".to_string())])),
                    parity_blocks: 2,
                    data_blocks: 4,
                    version_id: Some(Uuid::nil()),
                    user_tags: Arc::new("env=prod".to_string()),
                    is_latest: true,
                    etag: Some("0123456789abcdef0123456789abcdef".to_string()),
                    ..Default::default()
                },
                ObjectInfo {
                    bucket: "demo-bucket".to_string(),
                    name: "obj-b".to_string(),
                    mod_time: Some(datetime!(2025-01-02 00:00 UTC)),
                    delete_marker: true,
                    user_defined: Arc::new(HashMap::from([("marker".to_string(), "true".to_string())])),
                    version_id: None,
                    ..Default::default()
                },
            ],
        };

        let permissions = HashMap::from([
            (
                "obj-a".to_string(),
                ObjectMetadataPermissions {
                    metadata_allowed: true,
                    tags_allowed: true,
                },
            ),
            (
                "obj-b".to_string(),
                ObjectMetadataPermissions {
                    metadata_allowed: true,
                    tags_allowed: false,
                },
            ),
        ]);

        let params = ListObjectVersionsParams {
            prefix: "pre".to_string(),
            delimiter: Some("/".to_string()),
            key_marker: Some("start marker".to_string()),
            version_id_marker: Some("vid-1".to_string()),
            max_keys: 1000,
        };
        let output = build_list_object_versions_metadata_output(
            object_infos,
            "demo-bucket",
            &params,
            Some(&EncodingType::from_static(EncodingType::URL)),
            &permissions,
        );

        assert_eq!(output.output.name.as_deref(), Some("demo-bucket"));
        assert_eq!(output.output.prefix.as_deref(), Some("pre"));
        assert_eq!(output.output.key_marker.as_deref(), Some("start%20marker"));
        assert_eq!(output.output.next_key_marker.as_deref(), Some("obj-z"));
        assert_eq!(output.output.next_version_id_marker.as_deref(), Some("null"));
        assert_eq!(output.entries.len(), 2);

        match &output.entries[0] {
            ListObjectVersionMetadataEntry::Version(version, extension) => {
                assert_eq!(version.key.as_deref(), Some("obj-a"));
                // A null version's synthesized nil UUID must surface as the
                // literal `null`, never as `00000000-…` (issue #6745).
                assert_eq!(version.version_id.as_deref(), Some("null"));
                assert_eq!(extension.user_tags.as_deref(), Some("env=prod"));
                assert_eq!(extension.internal, Some(ObjectInternalInfo { k: 4, m: 2 }));
                assert_eq!(
                    extension.user_metadata.clone(),
                    Some(vec![MetadataEntry {
                        name: Some("project".to_string()),
                        value: Some("alpha".to_string()),
                    }])
                );
            }
            other => panic!("expected version entry, got {other:?}"),
        }

        match &output.entries[1] {
            ListObjectVersionMetadataEntry::DeleteMarker(marker, extension) => {
                assert_eq!(marker.key.as_deref(), Some("obj-b"));
                assert_eq!(marker.version_id.as_deref(), Some("null"));
                assert!(extension.user_tags.is_none());
                assert_eq!(
                    extension.user_metadata.clone(),
                    Some(vec![MetadataEntry {
                        name: Some("marker".to_string()),
                        value: Some("true".to_string()),
                    }])
                );
            }
            other => panic!("expected delete marker entry, got {other:?}"),
        }
    }

    #[test]
    fn build_list_object_versions_metadata_output_uses_params_and_hides_metadata_without_permissions() {
        use time::macros::datetime;

        let object_infos = ListObjectVersionsInfo {
            is_truncated: false,
            next_marker: Some(String::new()),
            next_version_idmarker: Some(String::new()),
            prefixes: vec!["logs and more/".to_string()],
            objects: vec![ObjectInfo {
                bucket: "demo-bucket".to_string(),
                name: "logs and more/object one.txt".to_string(),
                mod_time: Some(datetime!(2025-01-04 00:00 UTC)),
                size: 7,
                user_defined: Arc::new(HashMap::from([("secret".to_string(), "value".to_string())])),
                user_tags: Arc::new("env=prod".to_string()),
                parity_blocks: 1,
                data_blocks: 2,
                ..Default::default()
            }],
        };

        let params = ListObjectVersionsParams {
            prefix: "logs and more/".to_string(),
            delimiter: Some(" ".to_string()),
            key_marker: Some("marker value".to_string()),
            version_id_marker: None,
            max_keys: 25,
        };

        let output = build_list_object_versions_metadata_output(
            object_infos,
            "demo-bucket",
            &params,
            Some(&EncodingType::from_static(EncodingType::URL)),
            &HashMap::new(),
        );

        assert_eq!(output.output.name.as_deref(), Some("demo-bucket"));
        assert_eq!(output.output.prefix.as_deref(), Some("logs%20and%20more%2F"));
        assert_eq!(output.output.delimiter.as_deref(), Some("%20"));
        assert_eq!(output.output.key_marker.as_deref(), Some("marker%20value"));
        assert_eq!(output.output.version_id_marker.as_deref(), Some(""));
        assert_eq!(output.output.next_key_marker, None);
        assert_eq!(output.output.next_version_id_marker, None);

        match &output.entries[0] {
            ListObjectVersionMetadataEntry::Version(version, extension) => {
                assert_eq!(version.key.as_deref(), Some("logs%20and%20more%2Fobject%20one.txt"));
                assert!(extension.user_metadata.is_none());
                assert!(extension.user_tags.is_none());
                assert!(extension.internal.is_none());
            }
            other => panic!("expected version entry, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn execute_list_objects_returns_internal_error_when_store_uninitialized() {
        let input = ListObjectsInput::builder().bucket("test-bucket".to_string()).build().unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_list_objects(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_list_objects_v2m_returns_internal_error_when_store_uninitialized() {
        let input = ListObjectsV2Input::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_list_objects_v2m(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[test]
    fn build_list_objects_v2_metadata_output_maps_metadata_and_key_count() {
        use time::macros::datetime;

        let object_infos = ListObjectsV2Info {
            is_truncated: true,
            next_continuation_token: Some("next-token".to_string()),
            objects: vec![ObjectInfo {
                bucket: "demo-bucket".to_string(),
                name: "logs/obj a.txt".to_string(),
                mod_time: Some(datetime!(2025-01-03 00:00 UTC)),
                size: 11,
                user_defined: Arc::new(HashMap::from([("project".to_string(), "alpha".to_string())])),
                parity_blocks: 2,
                data_blocks: 4,
                user_tags: Arc::new("env=prod".to_string()),
                etag: Some("0123456789abcdef0123456789abcdef".to_string()),
                ..Default::default()
            }],
            prefixes: vec!["logs/archive/".to_string()],
            ..Default::default()
        };

        let permissions = HashMap::from([(
            "logs/obj a.txt".to_string(),
            ObjectMetadataPermissions {
                metadata_allowed: true,
                tags_allowed: true,
            },
        )]);

        let params = ListObjectsV2Params {
            prefix: "logs/".to_string(),
            max_keys: 1000,
            delimiter: Some("/".to_string()),
            response_start_after: Some("logs/start after".to_string()),
            start_after_for_query: None,
            response_continuation_token: Some("start token".to_string()),
            decoded_continuation_token: None,
        };

        let output = build_list_objects_v2_metadata_output(
            object_infos,
            "demo-bucket",
            &params,
            Some(&EncodingType::from_static(EncodingType::URL)),
            true,
            &permissions,
        );

        assert_eq!(output.output.name.as_deref(), Some("demo-bucket"));
        assert_eq!(output.output.prefix.as_deref(), Some("logs/"));
        assert_eq!(output.output.continuation_token.as_deref(), Some("start token"));
        assert_eq!(output.output.start_after.as_deref(), Some("logs/start after"));
        assert_eq!(output.output.next_continuation_token.as_deref(), Some("bmV4dC10b2tlbg=="));
        assert_eq!(output.output.key_count, Some(2));
        assert_eq!(output.contents.len(), 1);
        assert_eq!(output.output.common_prefixes.as_ref().map(Vec::len), Some(1));

        let entry = output.contents.first().unwrap();
        assert_eq!(entry.object.key.as_deref(), Some("logs/obj%20a.txt"));
        assert_eq!(entry.extension.user_tags.as_deref(), Some("env=prod"));
        assert_eq!(entry.extension.internal, Some(ObjectInternalInfo { k: 4, m: 2 }));
        assert!(entry.object.owner.is_some());
        assert_eq!(
            entry.extension.user_metadata.clone(),
            Some(vec![MetadataEntry {
                name: Some("project".to_string()),
                value: Some("alpha".to_string()),
            }])
        );

        let prefix = output.output.common_prefixes.as_ref().unwrap().first().unwrap();
        assert_eq!(prefix.prefix.as_deref(), Some("logs/archive/"));
    }

    #[test]
    fn list_objects_v2_metadata_output_serializes_minio_extension_xml() {
        use time::macros::datetime;

        let object_infos = ListObjectsV2Info {
            is_truncated: false,
            objects: vec![ObjectInfo {
                bucket: "demo-bucket".to_string(),
                name: "logs/obj-a.txt".to_string(),
                mod_time: Some(datetime!(2025-01-03 00:00 UTC)),
                size: 11,
                user_defined: Arc::new(HashMap::from([("project".to_string(), "alpha".to_string())])),
                parity_blocks: 2,
                data_blocks: 4,
                user_tags: Arc::new("env=prod&project=alpha".to_string()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let permissions = HashMap::from([(
            "logs/obj-a.txt".to_string(),
            ObjectMetadataPermissions {
                metadata_allowed: true,
                tags_allowed: true,
            },
        )]);
        let params = ListObjectsV2Params {
            prefix: "logs/".to_string(),
            max_keys: 1000,
            delimiter: None,
            response_start_after: None,
            start_after_for_query: None,
            response_continuation_token: None,
            decoded_continuation_token: None,
        };

        let output = build_list_objects_v2_metadata_output(object_infos, "demo-bucket", &params, None, false, &permissions);
        let xml = String::from_utf8(serialize_s3s_response(&output)).expect("metadata output should be UTF-8");

        assert!(xml.contains("<ListBucketResult"));
        assert!(xml.contains("<Contents>"));
        assert!(xml.contains("<UserMetadata><project>alpha</project></UserMetadata>"));
        assert!(xml.contains("<UserTags>env=prod&amp;project=alpha</UserTags>"));
        assert!(xml.contains("<Internal><K>4</K><M>2</M></Internal>"));
        assert!(!xml.contains("<MetadataEntry>"));
    }

    #[test]
    fn is_valid_xml_name_rejects_non_name_keys() {
        // Regression for #2743: keys that are illegal as XML element names.
        for good in ["project", "_x", "a-b.c", "Content_Type", "ns:key", "键名"] {
            assert!(is_valid_xml_name(good), "{good:?} should be a valid XML name");
        }
        for bad in ["", "bad key", "1abc", "a$b", "a%b", "a#b", "a*b", "a|b", "a\u{1}b", "-lead"] {
            assert!(!is_valid_xml_name(bad), "{bad:?} should be rejected as an XML name");
        }
    }

    #[test]
    fn sanitize_xml_text_strips_illegal_control_chars() {
        // Regression for #2743: XML 1.0 forbids C0 controls other than tab/newline/CR.
        assert_eq!(sanitize_xml_text("clean value").as_ref(), "clean value");
        assert_eq!(sanitize_xml_text("keep\ttab\nnl\rcr").as_ref(), "keep\ttab\nnl\rcr");
        assert_eq!(sanitize_xml_text("bad\u{1}\u{7}\u{1b}chars").as_ref(), "badchars");
    }

    #[test]
    fn list_objects_v2_metadata_output_survives_poison_metadata_key() {
        // Regression for #2743: a single object carrying an XML-unsafe user-metadata key
        // (space in the key) or an illegal control char in the value must not corrupt the
        // whole listing document. The offending key is dropped; well-formed siblings remain.
        use time::macros::datetime;

        let object_infos = ListObjectsV2Info {
            is_truncated: false,
            objects: vec![ObjectInfo {
                bucket: "demo-bucket".to_string(),
                name: "logs/obj-a.txt".to_string(),
                mod_time: Some(datetime!(2025-01-03 00:00 UTC)),
                size: 11,
                user_defined: Arc::new(HashMap::from([
                    ("project".to_string(), "alpha".to_string()),
                    ("bad key".to_string(), "should-be-dropped".to_string()),
                    ("note".to_string(), "line1\u{1}line2".to_string()),
                ])),
                parity_blocks: 2,
                data_blocks: 4,
                ..Default::default()
            }],
            ..Default::default()
        };
        let permissions = HashMap::from([(
            "logs/obj-a.txt".to_string(),
            ObjectMetadataPermissions {
                metadata_allowed: true,
                tags_allowed: true,
            },
        )]);
        let params = ListObjectsV2Params {
            prefix: "logs/".to_string(),
            max_keys: 1000,
            delimiter: None,
            response_start_after: None,
            start_after_for_query: None,
            response_continuation_token: None,
            decoded_continuation_token: None,
        };

        let output = build_list_objects_v2_metadata_output(object_infos, "demo-bucket", &params, None, false, &permissions);
        let xml = String::from_utf8(serialize_s3s_response(&output)).expect("metadata output should be UTF-8");

        // Good sibling metadata survives.
        assert!(xml.contains("<project>alpha</project>"), "well-formed key must remain: {xml}");
        // The XML-unsafe key is dropped entirely (never emitted as a malformed tag).
        assert!(!xml.contains("bad key"), "poison key must not appear: {xml}");
        // The control char is stripped from the value.
        assert!(xml.contains("<note>line1line2</note>"), "control char must be stripped: {xml}");
        assert!(!xml.contains('\u{1}'), "no raw control chars in output");
        // The document is well-formed and re-parses cleanly.
        let mut reader = quick_xml::Reader::from_str(&xml);
        loop {
            match reader.read_event() {
                Ok(quick_xml::events::Event::Eof) => break,
                Ok(_) => {}
                Err(err) => panic!("serialized listing must be well-formed XML, got {err} in {xml}"),
            }
        }
    }

    #[test]
    fn build_list_objects_v2_metadata_output_uses_params_and_hides_owner_without_fetch_owner() {
        use time::macros::datetime;

        let object_infos = ListObjectsV2Info {
            is_truncated: false,
            next_continuation_token: None,
            objects: vec![ObjectInfo {
                bucket: "demo-bucket".to_string(),
                name: "logs and more/object one.txt".to_string(),
                mod_time: Some(datetime!(2025-01-05 00:00 UTC)),
                size: 13,
                user_defined: Arc::new(HashMap::from([("secret".to_string(), "value".to_string())])),
                user_tags: Arc::new("env=prod".to_string()),
                parity_blocks: 1,
                data_blocks: 2,
                ..Default::default()
            }],
            prefixes: vec!["logs and more/archive/".to_string()],
            ..Default::default()
        };

        let params = ListObjectsV2Params {
            prefix: "logs and more/".to_string(),
            max_keys: 25,
            delimiter: Some("/".to_string()),
            response_start_after: Some("logs and more/start after".to_string()),
            start_after_for_query: Some("decoded start after".to_string()),
            response_continuation_token: Some("opaque token".to_string()),
            decoded_continuation_token: Some("decoded token".to_string()),
        };

        let output = build_list_objects_v2_metadata_output(
            object_infos,
            "demo-bucket",
            &params,
            Some(&EncodingType::from_static(EncodingType::URL)),
            false,
            &HashMap::new(),
        );

        assert_eq!(output.output.name.as_deref(), Some("demo-bucket"));
        assert_eq!(output.output.prefix.as_deref(), Some("logs and more/"));
        assert_eq!(output.output.delimiter.as_deref(), Some("/"));
        assert_eq!(output.output.continuation_token.as_deref(), Some("opaque token"));
        assert_eq!(output.output.start_after.as_deref(), Some("logs and more/start after"));
        assert_eq!(output.output.key_count, Some(2));
        assert_eq!(output.output.encoding_type.as_ref().map(EncodingType::as_str), Some(EncodingType::URL));

        let entry = output.contents.first().unwrap();
        assert_eq!(entry.object.key.as_deref(), Some("logs%20and%20more/object%20one.txt"));
        assert!(entry.object.owner.is_none());
        assert!(entry.extension.user_metadata.is_none());
        assert!(entry.extension.user_tags.is_none());
        assert!(entry.extension.internal.is_none());

        let prefix = output.output.common_prefixes.as_ref().unwrap().first().unwrap();
        assert_eq!(prefix.prefix.as_deref(), Some("logs%20and%20more/archive/"));
    }

    #[tokio::test]
    async fn execute_list_objects_v2_rejects_negative_max_keys() {
        let input = ListObjectsV2Input::builder()
            .bucket("test-bucket".to_string())
            .max_keys(Some(-1))
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_list_objects_v2(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    async fn execute_list_objects_v2_rejects_invalid_continuation_token_before_store_lookup() {
        let input = ListObjectsV2Input::builder()
            .bucket("test-bucket".to_string())
            .continuation_token(Some("%%%".to_string()))
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_list_objects_v2(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        assert_eq!(err.message(), Some("Invalid continuation token"));
    }

    #[tokio::test]
    async fn execute_list_objects_v2m_rejects_negative_max_keys() {
        let input = ListObjectsV2Input::builder()
            .bucket("test-bucket".to_string())
            .max_keys(Some(-1))
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_list_objects_v2m(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    async fn execute_list_objects_v2m_rejects_invalid_continuation_token_before_store_lookup() {
        let input = ListObjectsV2Input::builder()
            .bucket("test-bucket".to_string())
            .continuation_token(Some("%%%".to_string()))
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_list_objects_v2m(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        assert_eq!(err.message(), Some("Invalid continuation token"));
    }

    #[tokio::test]
    async fn execute_list_object_versions_rejects_negative_max_keys_before_store_lookup() {
        let input = ListObjectVersionsInput::builder()
            .bucket("test-bucket".to_string())
            .max_keys(Some(-1))
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_list_object_versions(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    async fn execute_list_object_versions_m_rejects_negative_max_keys_before_store_lookup() {
        let input = ListObjectVersionsInput::builder()
            .bucket("test-bucket".to_string())
            .max_keys(Some(-1))
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_list_object_versions_m(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }
}
