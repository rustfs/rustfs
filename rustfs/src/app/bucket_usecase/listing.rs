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

use super::list_objects::listed_etag;
use super::*;
use crate::app::metadata_listing::{MetadataListQuery, ObjectsQuery, VersionsQuery};
use crate::app::storage_api::bucket_usecase::access::{versioned_read_action, versioned_tagging_action};
use crate::app::storage_api::bucket_usecase::contract::list::ListOperations as _;
use quick_xml::{
    Writer,
    events::{BytesDecl, BytesText, Event},
};
use rustfs_gateway_types::ETag;
use rustfs_gateway_types::dto::{EncodingType, Owner, StorageClass};
use std::io;

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
    pub user_metadata: Option<Vec<(String, String)>>,
    pub user_tags: Option<String>,
    pub internal: Option<ObjectInternalInfo>,
}

#[derive(Clone, Debug)]
pub(crate) enum ListObjectVersionMetadataEntry {
    DeleteMarker(MetadataObject, ObjectMetadataExtension),
    Version(ObjectMetadataEntry),
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ListObjectVersionsMetadataOutput {
    page: MetadataPage,
    key_marker: Option<String>,
    version_id_marker: Option<String>,
    next_key_marker: Option<String>,
    next_version_id_marker: Option<String>,
    pub entries: Vec<ListObjectVersionMetadataEntry>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct MetadataObject {
    key: String,
    is_latest: Option<bool>,
    last_modified: Option<time::OffsetDateTime>,
    owner: Option<Owner>,
    size: Option<i64>,
    storage_class: Option<String>,
    version_id: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ObjectMetadataEntry {
    pub object: MetadataObject,
    pub e_tag: Option<ETag>,
    pub extension: ObjectMetadataExtension,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ListObjectsV2MetadataOutput {
    page: MetadataPage,
    key_count: Option<i32>,
    continuation_token: Option<String>,
    next_continuation_token: Option<String>,
    start_after: Option<String>,
    pub contents: Vec<ObjectMetadataEntry>,
}

#[derive(Clone, Debug, Default)]
struct MetadataPage {
    name: Option<String>,
    prefix: Option<String>,
    max_keys: Option<i32>,
    is_truncated: Option<bool>,
    delimiter: Option<String>,
    encoding_type: Option<EncodingType>,
    common_prefixes: Vec<String>,
}

type ListingWriter = Writer<Vec<u8>>;

fn document(root: &str, content: impl FnOnce(&mut ListingWriter) -> io::Result<()>) -> io::Result<Vec<u8>> {
    let mut writer = Writer::new(Vec::with_capacity(1024));
    writer.write_event(Event::Decl(BytesDecl::new("1.0", Some("UTF-8"), None)))?;
    writer
        .create_element(root)
        .with_attribute(("xmlns", XMLNS_S3))
        .write_inner_content(content)?;
    Ok(writer.into_inner())
}

fn text(xml: &mut Writer<impl Write>, name: &str, value: Option<&str>) -> io::Result<()> {
    if let Some(value) = value {
        xml.create_element(name).write_text_content(BytesText::new(value))?;
    }
    Ok(())
}

fn display_field(xml: &mut ListingWriter, name: &str, value: Option<impl Display>) -> io::Result<()> {
    text(xml, name, value.map(|v| v.to_string()).as_deref())
}

fn write_prefixes(xml: &mut ListingWriter, prefixes: &[String]) -> io::Result<()> {
    for prefix in prefixes {
        xml.create_element("CommonPrefixes")
            .write_inner_content(|xml| text(xml, "Prefix", Some(prefix)))?;
    }
    Ok(())
}

impl ListObjectVersionsMetadataOutput {
    pub(crate) fn to_xml(&self) -> io::Result<Vec<u8>> {
        let page = &self.page;
        document("ListVersionsResult", |xml| {
            write_prefixes(xml, &page.common_prefixes)?;
            text(xml, "Delimiter", page.delimiter.as_deref())?;
            text(xml, "EncodingType", page.encoding_type.as_ref().map(EncodingType::as_str))?;
            display_field(xml, "IsTruncated", page.is_truncated)?;
            text(xml, "KeyMarker", self.key_marker.as_deref())?;
            display_field(xml, "MaxKeys", page.max_keys)?;
            text(xml, "Name", page.name.as_deref())?;
            text(xml, "NextKeyMarker", self.next_key_marker.as_deref())?;
            text(xml, "NextVersionIdMarker", self.next_version_id_marker.as_deref())?;
            text(xml, "Prefix", page.prefix.as_deref())?;
            text(xml, "VersionIdMarker", self.version_id_marker.as_deref())?;
            for entry in &self.entries {
                match entry {
                    ListObjectVersionMetadataEntry::DeleteMarker(marker, extension) => {
                        xml.create_element("DeleteMarker").write_inner_content(|xml| {
                            marker.write_xml(xml)?;
                            extension.write_xml(xml)
                        })?;
                    }
                    ListObjectVersionMetadataEntry::Version(entry) => {
                        xml.create_element("Version")
                            .write_inner_content(|xml| entry.write_xml(xml))?;
                    }
                }
            }
            Ok(())
        })
    }
}

impl ListObjectsV2MetadataOutput {
    pub(crate) fn to_xml(&self) -> io::Result<Vec<u8>> {
        let page = &self.page;
        document("ListBucketResult", |xml| {
            text(xml, "Name", page.name.as_deref())?;
            text(xml, "Prefix", page.prefix.as_deref())?;
            display_field(xml, "MaxKeys", page.max_keys)?;
            display_field(xml, "KeyCount", self.key_count)?;
            text(xml, "ContinuationToken", self.continuation_token.as_deref())?;
            display_field(xml, "IsTruncated", page.is_truncated)?;
            text(xml, "NextContinuationToken", self.next_continuation_token.as_deref())?;
            for entry in &self.contents {
                xml.create_element("Contents")
                    .write_inner_content(|xml| entry.write_xml(xml))?;
            }
            write_prefixes(xml, &page.common_prefixes)?;
            text(xml, "Delimiter", page.delimiter.as_deref())?;
            text(xml, "EncodingType", page.encoding_type.as_ref().map(EncodingType::as_str))?;
            text(xml, "StartAfter", self.start_after.as_deref())
        })
    }
}

impl ObjectMetadataEntry {
    fn write_xml(&self, xml: &mut ListingWriter) -> io::Result<()> {
        if let Some(etag) = &self.e_tag {
            // The XML listing uses literal outer quotes even for a weak tag. The opaque text is escaped.
            let quoted = format!("\"{}\"", quick_xml::escape::escape(etag.opaque_tag()));
            xml.create_element("ETag")
                .write_text_content(BytesText::from_escaped(quoted))?;
        }
        self.object.write_xml(xml)?;
        self.extension.write_xml(xml)
    }
}

impl MetadataObject {
    fn write_xml(&self, xml: &mut ListingWriter) -> io::Result<()> {
        display_field(xml, "IsLatest", self.is_latest)?;
        text(xml, "Key", Some(&self.key))?;
        if let Some(date) = &self.last_modified {
            xml.create_element("LastModified").write_inner_content(|xml| {
                // Metadata listings write the stored wall-clock fields with a literal Z.
                if !(0..=9999).contains(&date.year()) {
                    return Err(io::Error::other(format!("time: {}", time::error::Format::InvalidComponent("year"))));
                }
                date.format_into(
                    xml.get_mut(),
                    time::macros::format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z"),
                )
                .map(|_| ())
                .map_err(io::Error::other)
            })?;
        }
        if let Some(owner) = &self.owner {
            xml.create_element("Owner").write_inner_content(|xml| {
                text(xml, "DisplayName", owner.display_name.as_deref())?;
                text(xml, "ID", owner.id.as_deref())
            })?;
        }
        display_field(xml, "Size", self.size)?;
        text(xml, "StorageClass", self.storage_class.as_deref())?;
        text(xml, "VersionId", self.version_id.as_deref())
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

impl ObjectMetadataExtension {
    fn write_xml(&self, xml: &mut ListingWriter) -> io::Result<()> {
        if let Some(metadata) = &self.user_metadata {
            xml.create_element("UserMetadata").write_inner_content(|xml| {
                for (name, value) in metadata {
                    if is_valid_xml_name(name) {
                        text(xml, name, Some(&sanitize_xml_text(value)))?;
                    }
                }
                Ok(())
            })?;
        }
        text(xml, "UserTags", self.user_tags.as_deref())?;
        if let Some(internal) = &self.internal {
            xml.create_element("Internal").write_inner_content(|xml| {
                display_field(xml, "K", Some(internal.k))?;
                display_field(xml, "M", Some(internal.m))
            })?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct ObjectMetadataPermissions {
    metadata_allowed: bool,
    tags_allowed: bool,
}

/// Metadata permissions for a version listing, keyed per `(object key, version id)` so
/// permissions for one version never expose metadata or tags of another version.
type ObjectVersionMetadataPermissions = HashMap<(String, Option<uuid::Uuid>), ObjectMetadataPermissions>;

/// The version id a version listing advertises for an entry. A null version
/// surfaces as `Some(Uuid::nil())` on the client-facing `ObjectInfo`; AWS
/// advertises it as the literal string `null`, and a nil UUID must never reach
/// the wire (issue #6745).
fn list_object_version_id(version_id: Option<uuid::Uuid>) -> String {
    version_id
        .filter(|version| !version.is_nil())
        .map(|version| version.to_string())
        .unwrap_or_else(|| "null".to_string())
}

/// The version selector a version-listing entry authorizes against. The
/// `IsLatest` entry matches what a plain GET/HEAD returns, so it authorizes as
/// a current-object read (`None`); a noncurrent entry is only reachable via
/// `?versionId=`, so it authorizes as a versioned read of that version.
fn list_object_version_authorized_version(object: &ObjectInfo) -> Option<String> {
    (!object.is_latest).then(|| list_object_version_id(object.version_id))
}

fn encode_list_versions_value(value: &str, encoding_type: Option<&EncodingType>) -> String {
    if encoding_type == Some(&EncodingType::URL) {
        encode(value).into_owned()
    } else {
        value.to_string()
    }
}

fn encode_list_objects_v2_value(value: &str, encoding_type: Option<&EncodingType>) -> String {
    if encoding_type == Some(&EncodingType::URL) {
        value
            .split('/')
            .map(|part| encode(part).into_owned())
            .collect::<Vec<_>>()
            .join("/")
    } else {
        value.to_string()
    }
}

fn build_metadata_extension_user_metadata(user_defined: &HashMap<String, String>) -> Option<Vec<(String, String)>> {
    // Even builds unable to decrypt MinIO SSE-C must not expose its key-protected metadata.
    if crate::app::storage_api::bucket_usecase::sse::stored_ssec_metadata(user_defined)
        || user_defined.contains_key(rustfs_utils::http::object_encryption_keys::MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER)
    {
        return None;
    }
    let mut items = extract_user_defined_metadata(user_defined)
        .into_iter()
        .filter(|(key, _)| !key.is_empty())
        .collect::<Vec<_>>();
    items.sort_by(|left, right| left.0.cmp(&right.0));

    if items.is_empty() { None } else { Some(items) }
}

fn metadata_count_to_i32(value: usize) -> i32 {
    i32::try_from(value).unwrap_or(i32::MAX)
}

async fn is_list_objects_metadata_action_allowed(
    envelope: &RequestEnvelope,
    bucket: &str,
    object: &str,
    version_id: Option<&str>,
    action: Action,
) -> S3Result<bool> {
    let mut auth_req = envelope.fork_for_authorization();
    let mut req_info = req_info_ref(envelope)?.clone();
    req_info.bucket = Some(bucket.to_string());
    req_info.object = Some(object.to_string());
    req_info.version_id = version_id.map(str::to_string);
    // Denial here is an expected filter outcome, not an error (issue #5740).
    req_info.suppress_denial_log = true;
    auth_req.install_req_info(req_info);

    match authorize_request(&mut auth_req, action).await {
        Ok(()) => Ok(true),
        Err(err) if err.code() == &rustfs_s3_types::S3ErrorCode::AccessDenied => Ok(false),
        Err(err) => Err(err),
    }
}

/// Authorizes metadata and tag disclosure for a single listing entry using the
/// same read/tagging actions as a direct read of that object or version.
async fn list_objects_metadata_permissions(
    envelope: &RequestEnvelope,
    bucket: &str,
    object: &str,
    version_id: Option<&str>,
) -> S3Result<ObjectMetadataPermissions> {
    let metadata_allowed =
        is_list_objects_metadata_action_allowed(envelope, bucket, object, version_id, versioned_read_action(version_id)).await?;
    let tags_allowed = is_list_objects_metadata_action_allowed(
        envelope,
        bucket,
        object,
        version_id,
        versioned_tagging_action(S3Action::GetObjectTaggingAction, version_id),
    )
    .await?;

    Ok(ObjectMetadataPermissions {
        metadata_allowed,
        tags_allowed,
    })
}

async fn collect_list_objects_metadata_permissions(
    envelope: &RequestEnvelope,
    bucket: &str,
    objects: &[ObjectInfo],
) -> S3Result<HashMap<String, ObjectMetadataPermissions>> {
    let mut permissions = HashMap::new();

    for object in objects {
        if object.name.is_empty() || permissions.contains_key(&object.name) {
            continue;
        }

        let permission = list_objects_metadata_permissions(envelope, bucket, &object.name, None).await?;
        permissions.insert(object.name.clone(), permission);
    }

    Ok(permissions)
}

async fn collect_list_object_versions_metadata_permissions(
    envelope: &RequestEnvelope,
    bucket: &str,
    objects: &[ObjectInfo],
) -> S3Result<ObjectVersionMetadataPermissions> {
    let mut permissions = HashMap::new();

    for object in objects {
        // Delete markers carry no user metadata, tags, or erasure layout, so
        // skip per-entry metadata/tag authorization checks for them.
        if object.name.is_empty() || object.delete_marker {
            continue;
        }

        let key = (object.name.clone(), object.version_id);
        if permissions.contains_key(&key) {
            continue;
        }

        let version_id = list_object_version_authorized_version(object);
        let permission = list_objects_metadata_permissions(envelope, bucket, &object.name, version_id.as_deref()).await?;
        permissions.insert(key, permission);
    }

    Ok(permissions)
}

fn build_list_object_versions_metadata_output(
    object_infos: ListObjectVersionsInfo,
    bucket: &str,
    params: &ListObjectVersionsParams,
    encoding_type: Option<&EncodingType>,
    permissions: &ObjectVersionMetadataPermissions,
) -> ListObjectVersionsMetadataOutput {
    let owner = rustfs_owner();
    let common_prefixes = object_infos
        .prefixes
        .into_iter()
        .map(|prefix_value| encode_list_versions_value(&prefix_value, encoding_type))
        .collect::<Vec<_>>();

    let entries = object_infos
        .objects
        .into_iter()
        .filter(|object| !object.name.is_empty())
        .map(|object| {
            let object_name = encode_list_versions_value(&object.name, encoding_type);
            let version_id = list_object_version_id(object.version_id);
            let permission = permissions
                .get(&(object.name.clone(), object.version_id))
                .copied()
                .unwrap_or_default();
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
                    MetadataObject {
                        key: object_name,
                        last_modified: object.mod_time,
                        owner: Some(owner.clone()),
                        version_id: Some(version_id),
                        is_latest: Some(object.is_latest),
                        ..Default::default()
                    },
                    extension,
                )
            } else {
                ListObjectVersionMetadataEntry::Version(ObjectMetadataEntry {
                    e_tag: listed_etag(&object),
                    object: MetadataObject {
                        key: object_name,
                        last_modified: object.mod_time,
                        size: Some(object.size),
                        version_id: Some(version_id),
                        is_latest: Some(object.is_latest),
                        storage_class: Some(
                            object
                                .storage_class
                                .unwrap_or_else(|| StorageClass::STANDARD.as_str().to_owned()),
                        ),
                        owner: Some(owner.clone()),
                    },
                    extension,
                })
            }
        })
        .collect::<Vec<_>>();

    let next_key_marker = object_infos
        .next_marker
        .filter(|marker| !marker.is_empty())
        .map(|marker| encode_list_versions_value(&marker, encoding_type));
    let next_version_id_marker = object_infos.next_version_idmarker.filter(|marker| !marker.is_empty());

    ListObjectVersionsMetadataOutput {
        page: MetadataPage {
            common_prefixes,
            delimiter: params
                .delimiter
                .clone()
                .map(|value| encode_list_versions_value(&value, encoding_type)),
            encoding_type: encoding_type.cloned(),
            is_truncated: Some(object_infos.is_truncated),
            max_keys: Some(params.max_keys),
            name: Some(bucket.to_owned()),
            prefix: Some(encode_list_versions_value(&params.prefix, encoding_type)),
        },
        key_marker: Some(encode_list_versions_value(
            params.key_marker.as_deref().unwrap_or_default(),
            encoding_type,
        )),
        next_key_marker,
        next_version_id_marker,
        version_id_marker: Some(params.version_id_marker.clone().unwrap_or_default()),
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
                e_tag: listed_etag(object),
                object: MetadataObject {
                    key: encode_list_objects_v2_value(&object.name, encoding_type),
                    last_modified: object.mod_time,
                    size: Some(object.get_actual_size_or_physical()),
                    storage_class: Some(
                        object
                            .storage_class
                            .clone()
                            .unwrap_or_else(|| StorageClass::STANDARD.as_str().to_owned()),
                    ),
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
        .map(|prefix| encode_list_objects_v2_value(&prefix, encoding_type))
        .collect::<Vec<_>>();

    let key_count = metadata_count_to_i32(contents.len() + common_prefixes.len());
    let next_continuation_token = object_infos
        .next_continuation_token
        .map(|cursor| encode_list_objects_v2_continuation_token(&cursor));

    ListObjectsV2MetadataOutput {
        page: MetadataPage {
            name: Some(bucket.to_owned()),
            prefix: Some(params.prefix.clone()),
            max_keys: Some(params.max_keys),
            is_truncated: Some(object_infos.is_truncated),
            common_prefixes,
            delimiter: params.delimiter.clone(),
            encoding_type: encoding_type.cloned(),
        },
        key_count: Some(key_count),
        continuation_token: params.response_continuation_token.clone(),
        next_continuation_token,
        start_after: params.response_start_after.clone(),
        contents,
    }
}

impl DefaultBucketUsecase {
    /// One page of `GET /{bucket}?list-type=2&metadata=true`, for the caller the envelope's
    /// authorization installed: each entry's metadata and tags as far as that caller may read them.
    pub(crate) async fn list_objects_v2_metadata(
        &self,
        envelope: &RequestEnvelope,
        input: ObjectsQuery,
    ) -> S3Result<ListObjectsV2MetadataOutput> {
        let ObjectsQuery {
            listing:
                MetadataListQuery {
                    bucket,
                    delimiter,
                    encoding_type,
                    max_keys,
                    prefix,
                },
            continuation_token,
            fetch_owner,
            start_after,
        } = input;

        let params = parse_list_objects_v2_params(prefix, delimiter, max_keys, continuation_token, start_after)?;

        validate_list_object_unordered_with_delimiter(params.delimiter.as_deref(), envelope.uri().query())?;

        let store = get_validated_store(&bucket).await?;
        let incl_deleted = get_header(envelope.headers(), rustfs_utils::http::SUFFIX_INCLUDE_DELETED)
            .map(|value| value.as_ref() == "true")
            .unwrap_or_default();

        let object_infos = match envelope.table_list_access() {
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

        let permissions = collect_list_objects_metadata_permissions(envelope, &bucket, &object_infos.objects).await?;
        Ok(build_list_objects_v2_metadata_output(
            object_infos,
            &bucket,
            &params,
            encoding_type.as_ref(),
            fetch_owner.unwrap_or_default(),
            &permissions,
        ))
    }

    /// One page of `GET /{bucket}?versions&metadata=true`, as [`Self::list_objects_v2_metadata`].
    pub(crate) async fn list_object_versions_metadata(
        &self,
        envelope: &RequestEnvelope,
        input: VersionsQuery,
    ) -> S3Result<ListObjectVersionsMetadataOutput> {
        let VersionsQuery {
            listing:
                MetadataListQuery {
                    bucket,
                    delimiter,
                    encoding_type,
                    max_keys,
                    prefix,
                },
            key_marker,
            version_id_marker,
        } = input;

        let params = parse_list_object_versions_params(prefix, delimiter, key_marker, version_id_marker, max_keys)?;

        let store = get_validated_store(&bucket).await?;
        let object_infos = match envelope.table_list_access() {
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

        let permissions = collect_list_object_versions_metadata_permissions(envelope, &bucket, &object_infos.objects).await?;
        Ok(build_list_object_versions_metadata_output(
            object_infos,
            &bucket,
            &params,
            encoding_type.as_ref(),
            &permissions,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::bucket_usecase::test_support::owned_envelope;
    use http::Method;

    #[test]
    fn metadata_listings_match_the_legacy_xml_bytes() {
        let owner = Owner {
            display_name: Some("user<&".into()),
            id: Some("a'b".into()),
        };
        let at = time::OffsetDateTime::from_unix_timestamp_nanos(1_700_000_000_123_456_789).unwrap();
        let object = MetadataObject {
            key: "key&".into(),
            last_modified: Some(at),
            owner: Some(owner),
            ..Default::default()
        };
        let extension = ObjectMetadataExtension {
            user_metadata: Some(vec![
                ("bad key".into(), "absent".into()),
                ("键名".into(), "a\u{1}\t\n\r<&>\"'".into()),
            ]),
            user_tags: Some("tag=x&z=y".into()),
            internal: Some(ObjectInternalInfo { k: 4, m: 2 }),
        };
        let prefixes = vec!["dir/".into()];
        let v2 = ListObjectsV2MetadataOutput {
            page: MetadataPage {
                name: Some("bucket".into()),
                prefix: Some(String::new()),
                max_keys: Some(3),
                is_truncated: Some(true),
                common_prefixes: prefixes.clone(),
                delimiter: Some("/".into()),
                encoding_type: Some(EncodingType::URL),
            },
            key_count: Some(2),
            continuation_token: Some("a+b".into()),
            next_continuation_token: Some("c=".into()),
            start_after: Some("key\r".into()),
            contents: vec![ObjectMetadataEntry {
                object: MetadataObject {
                    size: Some(9),
                    storage_class: Some("STANDARD".into()),
                    ..object.clone()
                },
                e_tag: Some(ETag::from_stored("x<&\"'")),
                extension: extension.clone(),
            }],
        };
        let versions = ListObjectVersionsMetadataOutput {
            page: MetadataPage {
                common_prefixes: prefixes,
                delimiter: Some("/".into()),
                encoding_type: Some(EncodingType::URL),
                is_truncated: Some(false),
                max_keys: Some(3),
                name: Some("bucket".into()),
                prefix: Some("prefix".into()),
            },
            key_marker: Some(String::new()),
            next_key_marker: Some("mark".into()),
            next_version_id_marker: Some("next".into()),
            version_id_marker: Some(String::new()),
            entries: vec![
                ListObjectVersionMetadataEntry::DeleteMarker(
                    MetadataObject {
                        is_latest: Some(true),
                        version_id: Some("null".into()),
                        ..object.clone()
                    },
                    ObjectMetadataExtension {
                        user_metadata: Some(vec![]),
                        ..Default::default()
                    },
                ),
                ListObjectVersionMetadataEntry::Version(ObjectMetadataEntry {
                    object: MetadataObject {
                        is_latest: Some(false),
                        size: Some(99),
                        storage_class: Some("GLACIER".into()),
                        version_id: Some("v1".into()),
                        ..object
                    },
                    e_tag: Some(ETag::new_weak("weak").unwrap()),
                    extension,
                }),
            ],
        };
        // Complete documents captured from the metadata route's s3s serializer at 056e59af7.
        assert_eq!(
            v2.to_xml().unwrap(),
            concat!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
                "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>bucket</Name>",
                "<Prefix></Prefix><MaxKeys>3</MaxKeys><KeyCount>2</KeyCount>",
                "<ContinuationToken>a+b</ContinuationToken><IsTruncated>true</IsTruncated>",
                "<NextContinuationToken>c=</NextContinuationToken><Contents>",
                "<ETag>\"x&lt;&amp;&quot;&apos;\"</ETag><Key>key&amp;</Key>",
                "<LastModified>2023-11-14T22:13:20.123Z</LastModified><Owner>",
                "<DisplayName>user&lt;&amp;</DisplayName><ID>a&apos;b</ID></Owner><Size>9</Size>",
                "<StorageClass>STANDARD</StorageClass><UserMetadata>",
                "<键名>a\t\n&#13;&lt;&amp;&gt;&quot;&apos;</键名></UserMetadata>",
                "<UserTags>tag=x&amp;z=y</UserTags><Internal><K>4</K><M>2</M></Internal></Contents>",
                "<CommonPrefixes><Prefix>dir/</Prefix></CommonPrefixes><Delimiter>/</Delimiter>",
                "<EncodingType>url</EncodingType><StartAfter>key&#13;</StartAfter></ListBucketResult>",
            )
            .as_bytes()
        );
        assert_eq!(
            versions.to_xml().unwrap(),
            concat!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
                "<ListVersionsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><CommonPrefixes>",
                "<Prefix>dir/</Prefix></CommonPrefixes><Delimiter>/</Delimiter>",
                "<EncodingType>url</EncodingType><IsTruncated>false</IsTruncated><KeyMarker></KeyMarker>",
                "<MaxKeys>3</MaxKeys><Name>bucket</Name><NextKeyMarker>mark</NextKeyMarker>",
                "<NextVersionIdMarker>next</NextVersionIdMarker><Prefix>prefix</Prefix><VersionIdMarker>",
                "</VersionIdMarker><DeleteMarker><IsLatest>true</IsLatest><Key>key&amp;</Key>",
                "<LastModified>2023-11-14T22:13:20.123Z</LastModified><Owner>",
                "<DisplayName>user&lt;&amp;</DisplayName><ID>a&apos;b</ID></Owner>",
                "<VersionId>null</VersionId><UserMetadata></UserMetadata></DeleteMarker><Version>",
                "<ETag>\"weak\"</ETag><IsLatest>false</IsLatest><Key>key&amp;</Key>",
                "<LastModified>2023-11-14T22:13:20.123Z</LastModified><Owner>",
                "<DisplayName>user&lt;&amp;</DisplayName><ID>a&apos;b</ID></Owner><Size>99</Size>",
                "<StorageClass>GLACIER</StorageClass><VersionId>v1</VersionId><UserMetadata>",
                "<键名>a\t\n&#13;&lt;&amp;&gt;&quot;&apos;</键名></UserMetadata>",
                "<UserTags>tag=x&amp;z=y</UserTags><Internal><K>4</K><M>2</M></Internal></Version>",
                "</ListVersionsResult>",
            )
            .as_bytes()
        );
        assert_eq!(
            ListObjectsV2MetadataOutput::default().to_xml().unwrap(),
            concat!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
                "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"></ListBucketResult>",
            )
            .as_bytes()
        );
        assert_eq!(
            ListObjectVersionsMetadataOutput::default().to_xml().unwrap(),
            concat!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
                "<ListVersionsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"></ListVersionsResult>",
            )
            .as_bytes()
        );
    }

    #[test]
    fn metadata_listing_dates_preserve_the_stored_clock_and_four_digit_year() {
        use time::{Date, Month, OffsetDateTime, Time, UtcOffset};
        let render = |date| {
            let object = MetadataObject {
                last_modified: date,
                ..Default::default()
            };
            let mut xml = Writer::new(Vec::new());
            object.write_xml(&mut xml)?;
            Ok::<_, io::Error>(String::from_utf8(xml.into_inner()).unwrap())
        };
        assert_eq!(render(None).unwrap(), "<Key></Key>");
        let local = OffsetDateTime::from_unix_timestamp_nanos(1_700_000_000_123_999_999)
            .unwrap()
            .to_offset(UtcOffset::from_hms(5, 30, 0).unwrap());
        let first = Date::from_calendar_date(0, Month::January, 1)
            .unwrap()
            .midnight()
            .assume_utc();
        let last = Date::from_calendar_date(9999, Month::December, 31)
            .unwrap()
            .with_time(Time::from_hms_nano(23, 59, 59, 999_999_999).unwrap())
            .assume_utc();
        // Captured from the pinned listing timestamp codec, including its literal Z for non-UTC offsets.
        for (date, expected) in [
            (local, "2023-11-15T03:43:20.123Z"),
            (first, "0000-01-01T00:00:00.000Z"),
            (last, "9999-12-31T23:59:59.999Z"),
        ] {
            assert_eq!(render(Some(date)).unwrap(), format!("<Key></Key><LastModified>{expected}</LastModified>"));
        }
        let before = Date::from_calendar_date(-1, Month::January, 1)
            .unwrap()
            .midnight()
            .assume_utc();
        assert_eq!(
            render(Some(before)).unwrap_err().to_string(),
            "time: The year component cannot be formatted into the requested format."
        );
    }

    #[tokio::test]
    async fn execute_list_object_versions_m_returns_internal_error_when_store_uninitialized() {
        let input = VersionsQuery {
            listing: MetadataListQuery {
                bucket: "test-bucket".to_owned(),
                ..Default::default()
            },
            ..Default::default()
        };

        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase
            .list_object_versions_metadata(&owned_envelope(Method::GET), input)
            .await
            .unwrap_err();
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
            ObjectInfo {
                name: "weak".to_owned(),
                etag: Some(r#"W/"a&<b""#.to_owned()),
                ..Default::default()
            },
            ObjectInfo {
                name: "stored-entities".to_owned(),
                etag: Some(r#"x"'<>&amp;"#.to_owned()),
                ..Default::default()
            },
            ObjectInfo {
                name: "no-etag".to_owned(),
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
        assert!(listings.contents[0].e_tag.is_none());
        assert!(listings.contents[1].e_tag.is_some());
        assert!(listings.contents[2].e_tag.is_some());
        assert_eq!(
            listings.contents[3].e_tag.as_ref().map(ETag::opaque_tag),
            Some("1b8a77dff5845270a8d81d86a28f38ea-2")
        );

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
            let ListObjectVersionMetadataEntry::Version(version) = entry else {
                panic!("expected object version");
            };
            assert_eq!(version.e_tag, listings.contents[index].e_tag);
        }
        #[derive(serde::Deserialize)]
        struct Listing {
            #[serde(rename = "Contents", alias = "Version")]
            entries: Vec<Entry>,
        }
        #[derive(serde::Deserialize)]
        struct Entry {
            #[serde(rename = "ETag")]
            e_tag: Option<String>,
        }
        for bytes in [listings.to_xml().unwrap(), versions.to_xml().unwrap()] {
            let xml = std::str::from_utf8(&bytes).expect("UTF-8 listing");
            assert!(!xml.contains(legacy_md5));
            assert!(xml.contains(r#"<ETag>"a&amp;&lt;b"</ETag>"#));
            assert!(xml.contains(r#"<ETag>"x&quot;&apos;&lt;&gt;&amp;amp;"</ETag>"#));
            let listing: Listing = quick_xml::de::from_str(xml).expect("well-formed listing");
            let tags: Vec<_> = listing.entries.iter().map(|entry| entry.e_tag.as_deref()).collect();
            assert_eq!(
                tags,
                [
                    None,
                    Some(r#""0123456789abcdef0123456789abcdef""#),
                    Some(r#""9876543210abcdef9876543210abcdef""#),
                    Some(r#""1b8a77dff5845270a8d81d86a28f38ea-2""#),
                    Some(r#""a&<b""#),
                    Some(r#""x"'<>&amp;""#),
                    None,
                ]
            );
        }
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
        let version_permissions = objects
            .iter()
            .map(|object| {
                (
                    (object.name.clone(), object.version_id),
                    ObjectMetadataPermissions {
                        metadata_allowed: true,
                        tags_allowed: true,
                    },
                )
            })
            .collect::<ObjectVersionMetadataPermissions>();
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
            &version_permissions,
        );
        for (index, entry) in versions.entries.iter().enumerate() {
            let ListObjectVersionMetadataEntry::Version(ObjectMetadataEntry { extension, .. }) = entry else {
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
                ("obj-a".to_string(), Some(Uuid::nil())),
                ObjectMetadataPermissions {
                    metadata_allowed: true,
                    tags_allowed: true,
                },
            ),
            (
                ("obj-b".to_string(), None),
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
            Some(&EncodingType::URL),
            &permissions,
        );

        assert_eq!(output.page.name.as_deref(), Some("demo-bucket"));
        assert_eq!(output.page.prefix.as_deref(), Some("pre"));
        assert_eq!(output.key_marker.as_deref(), Some("start%20marker"));
        assert_eq!(output.next_key_marker.as_deref(), Some("obj-z"));
        assert_eq!(output.next_version_id_marker.as_deref(), Some("null"));
        assert_eq!(output.entries.len(), 2);

        match &output.entries[0] {
            ListObjectVersionMetadataEntry::Version(ObjectMetadataEntry {
                object: version,
                extension,
                ..
            }) => {
                assert_eq!(version.key, "obj-a");
                // A null version's synthesized nil UUID must surface as the
                // literal `null`, never as `00000000-…` (issue #6745).
                assert_eq!(version.version_id.as_deref(), Some("null"));
                assert_eq!(extension.user_tags.as_deref(), Some("env=prod"));
                assert_eq!(extension.internal, Some(ObjectInternalInfo { k: 4, m: 2 }));
                assert_eq!(extension.user_metadata.clone(), Some(vec![("project".to_string(), "alpha".to_string())]));
            }
            other => panic!("expected version entry, got {other:?}"),
        }

        match &output.entries[1] {
            ListObjectVersionMetadataEntry::DeleteMarker(marker, extension) => {
                assert_eq!(marker.key, "obj-b");
                assert_eq!(marker.version_id.as_deref(), Some("null"));
                assert!(extension.user_tags.is_none());
                assert_eq!(extension.user_metadata.clone(), Some(vec![("marker".to_string(), "true".to_string())]));
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
            Some(&EncodingType::URL),
            &HashMap::new(),
        );

        assert_eq!(output.page.name.as_deref(), Some("demo-bucket"));
        assert_eq!(output.page.prefix.as_deref(), Some("logs%20and%20more%2F"));
        assert_eq!(output.page.delimiter.as_deref(), Some("%20"));
        assert_eq!(output.key_marker.as_deref(), Some("marker%20value"));
        assert_eq!(output.version_id_marker.as_deref(), Some(""));
        assert_eq!(output.next_key_marker, None);
        assert_eq!(output.next_version_id_marker, None);

        match &output.entries[0] {
            ListObjectVersionMetadataEntry::Version(ObjectMetadataEntry {
                object: version,
                extension,
                ..
            }) => {
                assert_eq!(version.key, "logs%20and%20more%2Fobject%20one.txt");
                assert!(extension.user_metadata.is_none());
                assert!(extension.user_tags.is_none());
                assert!(extension.internal.is_none());
            }
            other => panic!("expected version entry, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn execute_list_objects_v2m_returns_internal_error_when_store_uninitialized() {
        let input = ObjectsQuery {
            listing: MetadataListQuery {
                bucket: "test-bucket".to_owned(),
                ..Default::default()
            },
            ..Default::default()
        };

        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase
            .list_objects_v2_metadata(&owned_envelope(Method::GET), input)
            .await
            .unwrap_err();
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
            Some(&EncodingType::URL),
            true,
            &permissions,
        );

        assert_eq!(output.page.name.as_deref(), Some("demo-bucket"));
        assert_eq!(output.page.prefix.as_deref(), Some("logs/"));
        assert_eq!(output.continuation_token.as_deref(), Some("start token"));
        assert_eq!(output.start_after.as_deref(), Some("logs/start after"));
        assert_eq!(output.next_continuation_token.as_deref(), Some("bmV4dC10b2tlbg=="));
        assert_eq!(output.key_count, Some(2));
        assert_eq!(output.contents.len(), 1);
        assert_eq!(output.page.common_prefixes.len(), 1);

        let entry = output.contents.first().unwrap();
        assert_eq!(entry.object.key, "logs/obj%20a.txt");
        assert_eq!(entry.extension.user_tags.as_deref(), Some("env=prod"));
        assert_eq!(entry.extension.internal, Some(ObjectInternalInfo { k: 4, m: 2 }));
        assert!(entry.object.owner.is_some());
        assert_eq!(
            entry.extension.user_metadata.clone(),
            Some(vec![("project".to_string(), "alpha".to_string())])
        );

        let prefix = output.page.common_prefixes.first().unwrap();
        assert_eq!(prefix, "logs/archive/");
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
        let xml = String::from_utf8(output.to_xml().unwrap()).expect("metadata output should be UTF-8");

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
        let xml = String::from_utf8(output.to_xml().unwrap()).expect("metadata output should be UTF-8");

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
            Some(&EncodingType::URL),
            false,
            &HashMap::new(),
        );

        assert_eq!(output.page.name.as_deref(), Some("demo-bucket"));
        assert_eq!(output.page.prefix.as_deref(), Some("logs and more/"));
        assert_eq!(output.page.delimiter.as_deref(), Some("/"));
        assert_eq!(output.continuation_token.as_deref(), Some("opaque token"));
        assert_eq!(output.start_after.as_deref(), Some("logs and more/start after"));
        assert_eq!(output.key_count, Some(2));
        assert_eq!(
            output.page.encoding_type.as_ref().map(EncodingType::as_str),
            Some(EncodingType::URL.as_str())
        );

        let entry = output.contents.first().unwrap();
        assert_eq!(entry.object.key, "logs%20and%20more/object%20one.txt");
        assert!(entry.object.owner.is_none());
        assert!(entry.extension.user_metadata.is_none());
        assert!(entry.extension.user_tags.is_none());
        assert!(entry.extension.internal.is_none());

        let prefix = output.page.common_prefixes.first().unwrap();
        assert_eq!(prefix, "logs%20and%20more/archive/");
    }

    #[tokio::test]
    async fn execute_list_objects_v2m_rejects_negative_max_keys() {
        let input = ObjectsQuery {
            listing: MetadataListQuery {
                bucket: "test-bucket".to_owned(),
                max_keys: Some(-1),
                ..Default::default()
            },
            ..Default::default()
        };

        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase
            .list_objects_v2_metadata(&owned_envelope(Method::GET), input)
            .await
            .unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    async fn execute_list_objects_v2m_rejects_invalid_continuation_token_before_store_lookup() {
        let input = ObjectsQuery {
            listing: MetadataListQuery {
                bucket: "test-bucket".to_owned(),
                ..Default::default()
            },
            continuation_token: Some("%%%".to_string()),
            ..Default::default()
        };

        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase
            .list_objects_v2_metadata(&owned_envelope(Method::GET), input)
            .await
            .unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        assert_eq!(err.message(), Some("Invalid continuation token"));
    }

    #[tokio::test]
    async fn execute_list_object_versions_m_rejects_negative_max_keys_before_store_lookup() {
        let input = VersionsQuery {
            listing: MetadataListQuery {
                bucket: "test-bucket".to_owned(),
                max_keys: Some(-1),
                ..Default::default()
            },
            ..Default::default()
        };

        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase
            .list_object_versions_metadata(&owned_envelope(Method::GET), input)
            .await
            .unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn version_listing_noncurrent_entries_are_versioned_reads() {
        use uuid::Uuid;

        let entry = |version_id: Option<Uuid>, is_latest: bool| ObjectInfo {
            name: "report.txt".to_string(),
            version_id,
            is_latest,
            ..Default::default()
        };
        let noncurrent = Uuid::from_u128(2);

        assert_eq!(
            list_object_version_authorized_version(&entry(Some(Uuid::from_u128(1)), true)),
            None,
            "the IsLatest entry matches a plain GET and authorizes as a current-object read"
        );
        assert_eq!(
            list_object_version_authorized_version(&entry(Some(noncurrent), false)),
            Some(noncurrent.to_string()),
            "a noncurrent entry authorizes as a read of that explicit version"
        );
        assert_eq!(
            list_object_version_authorized_version(&entry(Some(Uuid::nil()), false)),
            Some("null".to_string()),
            "a noncurrent null version authorizes with the explicit `null` version selector"
        );
        assert_eq!(
            list_object_version_authorized_version(&entry(None, false)),
            Some("null".to_string()),
            "a noncurrent entry without a version id still authorizes as a versioned read"
        );

        let version_id = list_object_version_authorized_version(&entry(Some(noncurrent), false));
        assert_eq!(
            versioned_read_action(version_id.as_deref()),
            Action::S3Action(S3Action::GetObjectVersionAction)
        );
        assert_eq!(
            versioned_tagging_action(S3Action::GetObjectTaggingAction, version_id.as_deref()),
            Action::S3Action(S3Action::GetObjectVersionTaggingAction)
        );
    }

    #[test]
    fn version_listing_metadata_permissions_are_keyed_by_version() {
        use uuid::Uuid;

        let current = Uuid::from_u128(1);
        let noncurrent = Uuid::from_u128(2);
        let version = |version_id: Uuid, is_latest: bool, note: &str| ObjectInfo {
            bucket: "demo-bucket".to_string(),
            name: "report.txt".to_string(),
            version_id: Some(version_id),
            is_latest,
            user_defined: Arc::new(HashMap::from([("note".to_string(), note.to_string())])),
            user_tags: Arc::new(format!("stage={note}")),
            parity_blocks: 1,
            data_blocks: 2,
            ..Default::default()
        };
        let object_infos = ListObjectVersionsInfo {
            objects: vec![version(current, true, "current"), version(noncurrent, false, "old-secret")],
            ..Default::default()
        };
        let permissions = HashMap::from([(
            ("report.txt".to_string(), Some(current)),
            ObjectMetadataPermissions {
                metadata_allowed: true,
                tags_allowed: true,
            },
        )]);
        let params = ListObjectVersionsParams {
            prefix: String::new(),
            delimiter: None,
            key_marker: None,
            version_id_marker: None,
            max_keys: 1000,
        };

        let output = build_list_object_versions_metadata_output(object_infos, "demo-bucket", &params, None, &permissions);

        assert_eq!(output.entries.len(), 2);
        match &output.entries[0] {
            ListObjectVersionMetadataEntry::Version(ObjectMetadataEntry {
                object: version,
                extension,
                ..
            }) => {
                assert_eq!(version.version_id, Some(current.to_string()));
                assert_eq!(extension.user_metadata.clone(), Some(vec![("note".to_string(), "current".to_string())]));
                assert_eq!(extension.user_tags.as_deref(), Some("stage=current"));
                assert_eq!(extension.internal, Some(ObjectInternalInfo { k: 2, m: 1 }));
            }
            other => panic!("expected version entry, got {other:?}"),
        }
        match &output.entries[1] {
            ListObjectVersionMetadataEntry::Version(ObjectMetadataEntry {
                object: version,
                extension,
                ..
            }) => {
                assert_eq!(version.version_id, Some(noncurrent.to_string()));
                assert!(extension.user_metadata.is_none());
                assert!(extension.user_tags.is_none());
                assert!(extension.internal.is_none());
            }
            other => panic!("expected version entry, got {other:?}"),
        }
    }

    #[test]
    #[serial_test::serial]
    fn version_listing_metadata_permissions_enforce_version_actions_and_conditions() {
        // Real ECStore + IAM futures exceed libtest's 2 MiB default stack in unoptimized builds.
        let handle = std::thread::Builder::new()
            .name("version-listing-authz-test".to_owned())
            .stack_size(8 * 1024 * 1024)
            .spawn(|| {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build test tokio runtime");
                runtime.block_on(Box::pin(run_version_listing_metadata_permissions_enforce_version_actions_and_conditions()));
            })
            .expect("spawn version listing authz test thread");
        handle.join().expect("version listing authz test thread must succeed");
    }

    async fn run_version_listing_metadata_permissions_enforce_version_actions_and_conditions() {
        use crate::app::runtime_sources::ServerContextSlot;
        use crate::app::storage_api::test::contract::object::{ObjectIO as _, ObjectOperations as _};
        use crate::app::storage_api::test::{StorageObjectOptions, StoragePutObjReader};
        use http::{Extensions, HeaderMap, Uri};
        use rustfs_iam::store::{Store as _, UserType, object::ObjectStore};
        use rustfs_madmin::{AccountStatus, AddOrUpdateUserReq};
        use rustfs_policy::policy::Policy;

        const BUCKET: &str = "versioned-meta-listing";

        let temp = tempfile::tempdir().expect("create versioned metadata listing test root");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(temp.path())
            .disk_count(1)
            .build()
            .await;
        env.make_bucket(BUCKET, true).await;

        let put_version = |key: &str, body: &[u8], note: &str, tags: &str| {
            let store = Arc::clone(&env.ecstore);
            let key = key.to_owned();
            let body = body.to_vec();
            let note = note.to_owned();
            let tags = tags.to_owned();
            async move {
                let mut reader = StoragePutObjReader::from_vec(body);
                let opts = StorageObjectOptions {
                    versioned: true,
                    user_defined: HashMap::from([
                        ("x-amz-meta-note".to_owned(), note),
                        (rustfs_utils::http::AMZ_OBJECT_TAGGING.to_owned(), tags),
                    ]),
                    ..Default::default()
                };
                store
                    .put_object(BUCKET, &key, &mut reader, &opts)
                    .await
                    .expect("put versioned test object")
            }
        };

        let old_report = put_version("report.txt", b"v1", "old-secret", "stage=old-secret").await;
        let current_report = put_version("report.txt", b"v2", "current-public", "stage=current").await;
        let deleted_v1 = put_version("deleted.txt", b"gone", "deleted-secret", "stage=old-secret").await;
        env.ecstore
            .delete_object(
                BUCKET,
                "deleted.txt",
                StorageObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("create delete marker");

        let old_report_vid = old_report
            .version_id
            .expect("old report version id must be populated")
            .to_string();
        let current_report_vid = current_report
            .version_id
            .expect("current report version id must be populated")
            .to_string();
        let deleted_v1_vid = deleted_v1
            .version_id
            .expect("deleted object version id must be populated")
            .to_string();

        ObjectStore::new(Arc::clone(&env.ecstore))
            .save_iam_config(
                serde_json::json!({"version": 1}),
                format!("{}/format.json", *rustfs_iam::store::object::IAM_CONFIG_PREFIX),
            )
            .await
            .expect("seed IAM format");
        let iam = rustfs_iam::build_iam_sys(Arc::clone(&env.ecstore))
            .await
            .expect("build test IAM");

        let create_user_with_policy = |access_key: &str, policy_name: &str, policy_json: String| {
            let iam = Arc::clone(&iam);
            let access_key = access_key.to_owned();
            let policy_name = policy_name.to_owned();
            async move {
                iam.create_user(
                    &access_key,
                    &AddOrUpdateUserReq {
                        secret_key: format!("{access_key}-secret-key"),
                        policy: None,
                        status: AccountStatus::Enabled,
                    },
                )
                .await
                .expect("create IAM test user");
                iam.set_policy(&policy_name, Policy::parse_config(policy_json.as_bytes()).expect("parse IAM test policy"))
                    .await
                    .expect("store IAM test policy");
                iam.policy_db_set(&access_key, UserType::Reg, false, &policy_name)
                    .await
                    .expect("attach IAM test policy");
                iam.get_user(&access_key).await.expect("load IAM test user").credentials
            }
        };

        let current_only_cred = create_user_with_policy(
            "current-only-reader",
            "current-only-policy",
            format!(
                r#"{{"Version":"2012-10-17","Statement":[
                    {{"Effect":"Allow","Action":["s3:ListBucket","s3:ListBucketVersions"],"Resource":["arn:aws:s3:::{BUCKET}"]}},
                    {{"Effect":"Allow","Action":["s3:GetObject","s3:GetObjectTagging"],"Resource":["arn:aws:s3:::{BUCKET}/*"]}}
                ]}}"#
            ),
        )
        .await;

        let tag_conditioned_cred = create_user_with_policy(
            "tag-conditioned-reader",
            "tag-conditioned-policy",
            format!(
                r#"{{"Version":"2012-10-17","Statement":[
                    {{"Effect":"Allow","Action":["s3:ListBucket","s3:ListBucketVersions"],"Resource":["arn:aws:s3:::{BUCKET}"]}},
                    {{"Effect":"Allow","Action":["s3:GetObject","s3:GetObjectVersion","s3:GetObjectTagging","s3:GetObjectVersionTagging"],"Resource":["arn:aws:s3:::{BUCKET}/*"],"Condition":{{"StringEquals":{{"s3:ExistingObjectTag/stage":"current"}}}}}}
                ]}}"#
            ),
        )
        .await;

        let versionid_denied_cred = create_user_with_policy(
            "versionid-denied-reader",
            "versionid-denied-policy",
            format!(
                r#"{{"Version":"2012-10-17","Statement":[
                    {{"Effect":"Allow","Action":["s3:ListBucket","s3:ListBucketVersions"],"Resource":["arn:aws:s3:::{BUCKET}"]}},
                    {{"Effect":"Allow","Action":["s3:GetObject","s3:GetObjectVersion","s3:GetObjectTagging","s3:GetObjectVersionTagging"],"Resource":["arn:aws:s3:::{BUCKET}/*"]}},
                    {{"Effect":"Deny","Action":["s3:GetObjectVersion","s3:GetObjectVersionTagging"],"Resource":["arn:aws:s3:::{BUCKET}/*"],"Condition":{{"StringEquals":{{"s3:versionid":"{old_report_vid}"}}}}}}
                ]}}"#
            ),
        )
        .await;

        let full_version_cred = create_user_with_policy(
            "full-version-reader",
            "full-version-policy",
            format!(
                r#"{{"Version":"2012-10-17","Statement":[
                    {{"Effect":"Allow","Action":["s3:ListBucket","s3:ListBucketVersions"],"Resource":["arn:aws:s3:::{BUCKET}"]}},
                    {{"Effect":"Allow","Action":["s3:GetObject","s3:GetObjectVersion","s3:GetObjectTagging","s3:GetObjectVersionTagging"],"Resource":["arn:aws:s3:::{BUCKET}/*"]}}
                ]}}"#
            ),
        )
        .await;

        let context = Arc::new(AppContext::with_default_interfaces(
            Arc::clone(&env.ecstore),
            iam,
            Arc::new(rustfs_kms::KmsServiceManager::new()),
        ));
        let slot = ServerContextSlot::new();
        assert!(slot.install(context), "install server context slot");

        let listed = env
            .ecstore
            .list_object_versions(BUCKET, "", None, None, None, 1000)
            .await
            .expect("list object versions");

        let run_for_cred = |cred: rustfs_credentials::Credentials| {
            let slot = Arc::clone(&slot);
            let listed = listed.clone();
            let params = ListObjectVersionsParams {
                prefix: String::new(),
                delimiter: None,
                key_marker: None,
                version_id_marker: None,
                max_keys: 1000,
            };
            async move {
                let mut extensions = Extensions::new();
                extensions.insert(slot);
                extensions.insert(ReqInfo {
                    cred: Some(cred),
                    bucket: Some(BUCKET.to_owned()),
                    ..Default::default()
                });
                let envelope = RequestEnvelope::from_parts(
                    crate::app::RequestHead {
                        method: Method::GET,
                        uri: Uri::from_static("/versioned-meta-listing?versions&metadata=true"),
                        headers: HeaderMap::new(),
                        credentials: None,
                        region: None,
                        service: None,
                    },
                    extensions,
                );
                let permissions = collect_list_object_versions_metadata_permissions(&envelope, BUCKET, &listed.objects)
                    .await
                    .expect("collect version listing metadata permissions");
                // Delete markers must be skipped and never populate the permission map.
                assert!(
                    !listed
                        .objects
                        .iter()
                        .filter(|obj| obj.delete_marker)
                        .any(|obj| permissions.contains_key(&(obj.name.clone(), obj.version_id))),
                    "delete markers must not require metadata/tag authorization entries"
                );
                build_list_object_versions_metadata_output(listed, BUCKET, &params, None, &permissions)
            }
        };

        let find_version = |output: &ListObjectVersionsMetadataOutput, key: &str, vid: &str| -> ObjectMetadataExtension {
            output
                .entries
                .iter()
                .find_map(|entry| match entry {
                    ListObjectVersionMetadataEntry::Version(v)
                        if v.object.key == key && v.object.version_id.as_deref() == Some(vid) =>
                    {
                        Some(v.extension.clone())
                    }
                    _ => None,
                })
                .unwrap_or_else(|| panic!("missing version entry for {key}@{vid}"))
        };

        // 1. Caller with only s3:GetObject + s3:GetObjectTagging: latest is visible, noncurrent versions are withheld.
        let current_only_out = run_for_cred(current_only_cred).await;
        let current_ext = find_version(&current_only_out, "report.txt", &current_report_vid);
        assert_eq!(current_ext.user_metadata, Some(vec![("note".to_owned(), "current-public".to_owned())]));
        assert_eq!(current_ext.user_tags.as_deref(), Some("stage=current"));
        assert!(current_ext.internal.is_some());

        let old_ext = find_version(&current_only_out, "report.txt", &old_report_vid);
        assert_eq!(old_ext, ObjectMetadataExtension::default());
        let deleted_ext = find_version(&current_only_out, "deleted.txt", &deleted_v1_vid);
        assert_eq!(deleted_ext, ObjectMetadataExtension::default());

        // 2. Caller with s3:ExistingObjectTag/stage == current condition: noncurrent version's own tags are checked.
        let tag_cond_out = run_for_cred(tag_conditioned_cred).await;
        assert_eq!(
            find_version(&tag_cond_out, "report.txt", &current_report_vid).user_metadata,
            Some(vec![("note".to_owned(), "current-public".to_owned())])
        );
        assert_eq!(
            find_version(&tag_cond_out, "report.txt", &old_report_vid),
            ObjectMetadataExtension::default()
        );
        assert_eq!(
            find_version(&tag_cond_out, "deleted.txt", &deleted_v1_vid),
            ObjectMetadataExtension::default()
        );

        // 3. Caller denied specifically on s3:versionid == old_report_vid: only that version is withheld.
        let vid_denied_out = run_for_cred(versionid_denied_cred).await;
        assert_eq!(
            find_version(&vid_denied_out, "report.txt", &current_report_vid).user_metadata,
            Some(vec![("note".to_owned(), "current-public".to_owned())])
        );
        assert_eq!(
            find_version(&vid_denied_out, "report.txt", &old_report_vid),
            ObjectMetadataExtension::default()
        );
        let allowed_deleted_ext = find_version(&vid_denied_out, "deleted.txt", &deleted_v1_vid);
        assert_eq!(
            allowed_deleted_ext.user_metadata,
            Some(vec![("note".to_owned(), "deleted-secret".to_owned())])
        );
        assert_eq!(allowed_deleted_ext.user_tags.as_deref(), Some("stage=old-secret"));
        assert!(allowed_deleted_ext.internal.is_some());

        // 4. Caller with full version read/tagging permissions: both latest and noncurrent metadata are returned.
        let full_out = run_for_cred(full_version_cred).await;
        let full_old_ext = find_version(&full_out, "report.txt", &old_report_vid);
        assert_eq!(full_old_ext.user_metadata, Some(vec![("note".to_owned(), "old-secret".to_owned())]));
        assert_eq!(full_old_ext.user_tags.as_deref(), Some("stage=old-secret"));
        assert!(full_old_ext.internal.is_some());
        assert!(
            full_out.entries.iter().any(|entry| matches!(
                entry,
                ListObjectVersionMetadataEntry::DeleteMarker(marker, ext)
                    if marker.key == "deleted.txt" && *ext == ObjectMetadataExtension::default()
            )),
            "delete marker entry must be present with empty metadata extension"
        );
    }
}
