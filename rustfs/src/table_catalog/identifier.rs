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

use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentifierSegment(String);

impl IdentifierSegment {
    pub const MAX_LEN: usize = 64;

    pub fn parse(value: impl Into<String>) -> Result<Self, CatalogIdentifierError> {
        let value = value.into();
        validate_identifier_segment(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Namespace {
    segments: Vec<IdentifierSegment>,
}

impl Namespace {
    pub const MAX_LEN: usize = 512;

    pub fn parse(value: &str) -> Result<Self, CatalogIdentifierError> {
        if value.len() > Self::MAX_LEN {
            return Err(CatalogIdentifierError::NamespaceTooLong { max: Self::MAX_LEN });
        }
        Self::from_segments(value.split('.').map(str::to_string).collect())
    }

    pub(crate) fn from_segments(values: Vec<String>) -> Result<Self, CatalogIdentifierError> {
        if values.is_empty() {
            return Err(CatalogIdentifierError::Empty);
        }
        let value_len = values
            .iter()
            .try_fold(values.len().saturating_sub(1), |length, value| length.checked_add(value.len()))
            .ok_or(CatalogIdentifierError::NamespaceTooLong { max: Self::MAX_LEN })?;
        if value_len > Self::MAX_LEN {
            return Err(CatalogIdentifierError::NamespaceTooLong { max: Self::MAX_LEN });
        }

        let segments = values
            .into_iter()
            .map(IdentifierSegment::parse)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { segments })
    }

    pub fn segments(&self) -> &[IdentifierSegment] {
        &self.segments
    }

    pub fn storage_id(&self) -> String {
        self.segments
            .iter()
            .map(IdentifierSegment::as_str)
            .collect::<Vec<_>>()
            .join("/")
    }

    pub fn public_name(&self) -> String {
        self.segments
            .iter()
            .map(IdentifierSegment::as_str)
            .collect::<Vec<_>>()
            .join(".")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(
    dead_code,
    reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
)]
pub struct TableIdentifier {
    warehouse: IdentifierSegment,
    namespace: Namespace,
    name: IdentifierSegment,
}

impl TableIdentifier {
    #[allow(
        dead_code,
        reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
    )]
    pub fn new(warehouse: IdentifierSegment, namespace: Namespace, name: IdentifierSegment) -> Self {
        Self {
            warehouse,
            namespace,
            name,
        }
    }

    pub fn warehouse(&self) -> &IdentifierSegment {
        &self.warehouse
    }

    pub fn namespace(&self) -> &Namespace {
        &self.namespace
    }

    pub fn name(&self) -> &IdentifierSegment {
        &self.name
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(
    dead_code,
    reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
)]
pub struct TablePathResolver {
    reserved_prefix: &'static str,
}

impl Default for TablePathResolver {
    fn default() -> Self {
        Self {
            reserved_prefix: TABLE_RESERVED_PREFIX,
        }
    }
}

impl TablePathResolver {
    #[allow(
        dead_code,
        reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
    )]
    pub fn current_pointer_path(&self, table: &TableIdentifier) -> String {
        format!("{}/{}", self.table_root(table), CURRENT_POINTER_FILE)
    }

    #[allow(
        dead_code,
        reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
    )]
    pub fn metadata_dir_path(&self, table: &TableIdentifier) -> String {
        format!("{}/{}", self.table_root(table), METADATA_DIR)
    }

    #[allow(
        dead_code,
        reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
    )]
    pub fn metadata_file_path(&self, table: &TableIdentifier, metadata_file_name: &str) -> String {
        format!("{}/{}", self.metadata_dir_path(table), metadata_file_name)
    }

    fn table_root(&self, table: &TableIdentifier) -> String {
        format!(
            "{}/{}/{}/{}/{}/{}/{}",
            self.reserved_prefix,
            WAREHOUSE_ROOT,
            table.warehouse().as_str(),
            NAMESPACE_ROOT,
            table.namespace().storage_id(),
            TABLE_ROOT,
            table.name().as_str()
        )
    }
}

pub fn is_reserved_table_object_key(object_key: &str) -> bool {
    object_key == TABLE_RESERVED_PREFIX
        || object_key
            .strip_prefix(TABLE_RESERVED_PREFIX)
            .is_some_and(|rest| rest.starts_with('/'))
}

pub(crate) fn default_namespace_root_prefix() -> String {
    format!(
        "{}/{}/{}/{}/",
        TABLE_RESERVED_PREFIX, WAREHOUSE_ROOT, DEFAULT_WAREHOUSE_ID, NAMESPACE_ROOT
    )
}

#[allow(
    dead_code,
    reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
)]
pub(crate) fn default_namespace_marker_path(namespace: &Namespace) -> String {
    format!("{}{}/{}", default_namespace_root_prefix(), namespace.storage_id(), NAMESPACE_MARKER_FILE)
}

pub(crate) fn default_table_root_prefix(namespace: &Namespace) -> String {
    format!("{}{}/{}/", default_namespace_root_prefix(), namespace.storage_id(), TABLE_ROOT)
}

pub(crate) fn default_table_publication_lock_path(namespace: &Namespace, table: &IdentifierSegment) -> String {
    format!("{}{}/publication.lock", default_table_root_prefix(namespace), table.as_str())
}

pub(crate) fn default_table_bucket_publication_lock_path() -> String {
    rustfs_common::table_catalog::TABLE_BUCKET_PUBLICATION_LOCK_PATH.to_string()
}

#[allow(
    dead_code,
    reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
)]
pub(crate) fn default_table_marker_path(namespace: &Namespace, table: &IdentifierSegment) -> String {
    format!("{}{}/{}", default_table_root_prefix(namespace), table.as_str(), TABLE_MARKER_FILE)
}

pub(crate) fn default_table_metadata_dir_path(namespace: &Namespace, table: &IdentifierSegment) -> String {
    format!("{}{}/{}", default_table_root_prefix(namespace), table.as_str(), METADATA_DIR)
}

pub(crate) fn default_table_data_dir_path(namespace: &Namespace, table: &IdentifierSegment) -> String {
    format!("{}{}/{}", default_table_root_prefix(namespace), table.as_str(), DATA_DIR)
}

pub(crate) fn default_table_delete_dir_path(namespace: &Namespace, table: &IdentifierSegment) -> String {
    format!("{}{}/{}", default_table_root_prefix(namespace), table.as_str(), DELETE_DIR)
}

pub(crate) fn default_view_root_prefix(namespace: &Namespace) -> String {
    format!("{}{}/{}/", default_namespace_root_prefix(), namespace.storage_id(), VIEW_ROOT)
}

pub(crate) fn default_view_metadata_dir_path(namespace: &Namespace, view: &IdentifierSegment) -> String {
    format!("{}{}/{}", default_view_root_prefix(namespace), view.as_str(), METADATA_DIR)
}

pub(crate) fn default_view_metadata_file_path(
    namespace: &Namespace,
    view: &IdentifierSegment,
    metadata_file_name: &str,
) -> String {
    format!("{}/{}", default_view_metadata_dir_path(namespace, view), metadata_file_name)
}

pub(crate) fn default_table_metadata_file_path(
    namespace: &Namespace,
    table: &IdentifierSegment,
    metadata_file_name: &str,
) -> String {
    format!("{}/{}", default_table_metadata_dir_path(namespace, table), metadata_file_name)
}

#[allow(
    dead_code,
    reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
)]
pub(crate) fn default_table_current_pointer_path(namespace: &Namespace, table: &IdentifierSegment) -> String {
    format!("{}{}/{}", default_table_root_prefix(namespace), table.as_str(), CURRENT_POINTER_FILE)
}

#[allow(
    dead_code,
    reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
)]
pub(crate) fn default_table_lifecycle_path(namespace: &Namespace, table: &IdentifierSegment) -> String {
    format!("{}{}/{}", default_table_root_prefix(namespace), table.as_str(), LIFECYCLE_FILE)
}

#[allow(
    dead_code,
    reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
)]
pub(crate) fn namespace_name_from_marker_path(object_key: &str) -> Option<String> {
    let prefix = default_namespace_root_prefix();
    let suffix = format!("/{NAMESPACE_MARKER_FILE}");

    object_key
        .strip_prefix(prefix.as_str())
        .and_then(|value| value.strip_suffix(suffix.as_str()))
        .filter(|value| !value.is_empty())
        .map(|value| value.replace('/', "."))
}

#[allow(
    dead_code,
    reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
)]
pub(crate) fn table_name_from_marker_path(namespace: &Namespace, object_key: &str) -> Option<String> {
    let prefix = default_table_root_prefix(namespace);
    let suffix = format!("/{TABLE_MARKER_FILE}");

    object_key
        .strip_prefix(prefix.as_str())
        .and_then(|value| value.strip_suffix(suffix.as_str()))
        .filter(|value| !value.is_empty() && !value.contains('/'))
        .map(ToString::to_string)
}

pub(crate) fn metadata_location_from_metadata_file_path(
    namespace: &Namespace,
    table: &IdentifierSegment,
    object_key: &str,
) -> Option<String> {
    let prefix = format!("{}/", default_table_metadata_dir_path(namespace, table));

    object_key
        .strip_prefix(prefix.as_str())
        .filter(|value| is_valid_table_metadata_file_name(value))
        .map(|_| object_key.to_string())
}

fn table_metadata_dir_from_object_key(object_key: &str) -> Option<String> {
    let namespace_root = default_namespace_root_prefix();
    let relative = object_key.strip_prefix(&namespace_root)?;
    let (namespace_storage_id, table_path) = relative.rsplit_once(&format!("/{TABLE_ROOT}/"))?;
    Namespace::from_segments(namespace_storage_id.split('/').map(str::to_string).collect()).ok()?;
    let (table_name, metadata_file_name) = table_path.split_once(&format!("/{METADATA_DIR}/"))?;
    IdentifierSegment::parse(table_name).ok()?;
    if !is_valid_table_metadata_file_name(metadata_file_name) {
        return None;
    }
    Some(format!("{namespace_root}{namespace_storage_id}/{TABLE_ROOT}/{table_name}/{METADATA_DIR}"))
}

pub(crate) fn table_metadata_dir_path_for_entry(entry: &TableEntry) -> TableCatalogStoreResult<String> {
    let object_key = table_catalog_object_key_from_location(&entry.table_bucket, &entry.metadata_location).ok_or_else(|| {
        TableCatalogStoreError::Invalid("current metadata location must be inside a table metadata directory".to_string())
    })?;
    if let Some(metadata_dir) = table_metadata_dir_from_object_key(&object_key) {
        return Ok(metadata_dir);
    }
    if is_reserved_table_object_key(&object_key) {
        return Err(TableCatalogStoreError::Invalid(
            "current metadata location has an invalid protected table metadata path".to_string(),
        ));
    }
    let (metadata_dir, metadata_file_name) = object_key.rsplit_once('/').ok_or_else(|| {
        TableCatalogStoreError::Invalid("current metadata location must be inside a table metadata directory".to_string())
    })?;
    if metadata_dir
        .strip_suffix(&format!("/{METADATA_DIR}"))
        .is_none_or(str::is_empty)
        || !is_valid_table_metadata_file_name(metadata_file_name)
    {
        return Err(TableCatalogStoreError::Invalid(
            "current metadata location must be inside a table metadata directory".to_string(),
        ));
    }
    Ok(metadata_dir.to_string())
}

pub(crate) fn is_valid_table_metadata_location_for_entry(entry: &TableEntry, metadata_location: &str) -> bool {
    let Ok(metadata_dir) = table_metadata_dir_path_for_entry(entry) else {
        return false;
    };
    let Some(object_key) = table_catalog_object_key_from_location(&entry.table_bucket, metadata_location) else {
        return false;
    };
    object_key
        .strip_prefix(&format!("{metadata_dir}/"))
        .is_some_and(is_valid_table_metadata_file_name)
}

pub(crate) fn table_metadata_file_path_for_entry(
    entry: &TableEntry,
    metadata_file_name: &str,
) -> TableCatalogStoreResult<String> {
    if !is_valid_table_metadata_file_name(metadata_file_name) {
        return Err(TableCatalogStoreError::Invalid("invalid table metadata file name".to_string()));
    }
    Ok(format!("{}/{}", table_metadata_dir_path_for_entry(entry)?, metadata_file_name))
}

pub(crate) fn is_valid_table_metadata_location(
    namespace: &Namespace,
    table: &IdentifierSegment,
    metadata_location: &str,
) -> bool {
    if metadata_location.is_empty() {
        return false;
    }

    let metadata_prefix = format!("{}/", default_table_metadata_dir_path(namespace, table));
    metadata_location
        .strip_prefix(&metadata_prefix)
        .is_some_and(is_valid_table_metadata_file_name)
}

pub(crate) fn is_valid_view_metadata_location(namespace: &Namespace, view: &IdentifierSegment, metadata_location: &str) -> bool {
    if metadata_location.is_empty() {
        return false;
    }

    let metadata_prefix = format!("{}/", default_view_metadata_dir_path(namespace, view));
    metadata_location
        .strip_prefix(&metadata_prefix)
        .is_some_and(is_valid_table_metadata_file_name)
}

pub(crate) fn is_valid_table_metadata_file_name(metadata_file_name: &str) -> bool {
    if metadata_file_name.is_empty()
        || metadata_file_name.len() > TABLE_METADATA_FILE_NAME_MAX_LEN
        || !(metadata_file_name.ends_with(".json") || metadata_file_name.ends_with(".json.gz"))
        || metadata_file_name.contains("..")
        || metadata_file_name.contains('%')
        || metadata_file_name.contains('/')
        || metadata_file_name.contains('\\')
        || metadata_file_name.bytes().any(|byte| byte.is_ascii_control())
    {
        return false;
    }

    let bytes = metadata_file_name.as_bytes();
    if !is_lower_ascii_alnum(bytes[0]) || !is_lower_ascii_alnum(bytes[bytes.len() - 1]) {
        return false;
    }

    bytes
        .iter()
        .all(|byte| is_lower_ascii_alnum(*byte) || matches!(*byte, b'.' | b'_' | b'-'))
}

pub fn validate_object_mutation(table_bucket_enabled: bool, object_key: &str) -> Result<(), TableObjectMutationError> {
    if table_bucket_enabled && is_reserved_table_object_key(object_key) {
        return Err(TableObjectMutationError::ReservedCatalogObject);
    }

    Ok(())
}

pub(crate) async fn validate_bucket_object_mutation(bucket: &str, object_key: &str) -> Result<(), TableObjectMutationError> {
    if !is_reserved_table_object_key(object_key) {
        return Ok(());
    }

    let table_bucket_enabled = get_bucket_metadata(bucket)
        .await
        .map(|metadata| metadata.table_bucket_enabled())
        .unwrap_or(true);

    validate_object_mutation(table_bucket_enabled, object_key)
}

fn validate_identifier_segment(value: &str) -> Result<(), CatalogIdentifierError> {
    if value.is_empty() {
        return Err(CatalogIdentifierError::Empty);
    }

    if value.len() > IdentifierSegment::MAX_LEN {
        return Err(CatalogIdentifierError::TooLong {
            max: IdentifierSegment::MAX_LEN,
        });
    }

    if matches!(value, "." | "..") || value.contains('%') || value.contains('/') || value.contains('\\') {
        return Err(CatalogIdentifierError::Ambiguous);
    }

    let bytes = value.as_bytes();
    if !is_lower_ascii_alnum(bytes[0]) || !is_lower_ascii_alnum(bytes[bytes.len() - 1]) {
        return Err(CatalogIdentifierError::InvalidBoundary);
    }

    if bytes
        .iter()
        .any(|byte| !is_lower_ascii_alnum(*byte) && !matches!(*byte, b'_' | b'-'))
    {
        return Err(CatalogIdentifierError::InvalidCharacter);
    }

    Ok(())
}

fn is_lower_ascii_alnum(value: u8) -> bool {
    value.is_ascii_lowercase() || value.is_ascii_digit()
}
