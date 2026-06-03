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

//! Internal table catalog primitives for the Iceberg REST Catalog framework.
//!
//! This module intentionally does not expose HTTP handlers or mutate existing
//! S3 object behavior. It defines the stable internal boundary that later
//! catalog routes and object guards can share.

#![allow(dead_code)]

use std::fmt;

use rustfs_ecstore::bucket::{
    metadata::{BUCKET_TABLE_CONFIG, BUCKET_TABLE_RESERVED_PREFIX},
    metadata_sys,
};
use serde::{Deserialize, Serialize};

pub(crate) const TABLE_BUCKET_MARKER_CONFIG: &str = BUCKET_TABLE_CONFIG;
pub(crate) const RESERVED_CATALOG_OBJECT_MESSAGE: &str = "Object key is reserved for the table catalog";
pub(crate) const TABLE_BUCKET_CATALOG_TYPE: &str = "iceberg-rest";
pub(crate) const TABLE_BUCKET_CONFIG_VERSION: u16 = 1;
pub(crate) const DEFAULT_WAREHOUSE_ID: &str = "default";
pub(crate) const TABLE_NAMESPACE_MARKER_VERSION: u16 = 1;
pub(crate) const TABLE_RESOURCE_MARKER_VERSION: u16 = 1;
pub(crate) const TABLE_METADATA_POINTER_VERSION: u16 = 1;
pub(crate) const TABLE_METADATA_FILE_NAME_MAX_LEN: usize = 128;
pub const TABLE_RESERVED_PREFIX: &str = BUCKET_TABLE_RESERVED_PREFIX;
const WAREHOUSE_ROOT: &str = "warehouses";
const NAMESPACE_ROOT: &str = "namespaces";
const TABLE_ROOT: &str = "tables";
const NAMESPACE_MARKER_FILE: &str = "namespace.json";
const TABLE_MARKER_FILE: &str = "table.json";
const CURRENT_POINTER_FILE: &str = "current.json";
const LIFECYCLE_FILE: &str = "lifecycle.json";
const METADATA_DIR: &str = "metadata";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogIdentifierError {
    Empty,
    TooLong { max: usize },
    InvalidCharacter,
    InvalidBoundary,
    Ambiguous,
}

impl fmt::Display for CatalogIdentifierError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("catalog identifier segment is empty"),
            Self::TooLong { max } => write!(f, "catalog identifier segment exceeds {max} characters"),
            Self::InvalidCharacter => f.write_str("catalog identifier segment contains invalid characters"),
            Self::InvalidBoundary => {
                f.write_str("catalog identifier segment must start and end with a lowercase letter or digit")
            }
            Self::Ambiguous => f.write_str("catalog identifier segment is ambiguous"),
        }
    }
}

impl std::error::Error for CatalogIdentifierError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableObjectMutationError {
    ReservedCatalogObject,
}

impl fmt::Display for TableObjectMutationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReservedCatalogObject => f.write_str("object key is reserved for the table catalog"),
        }
    }
}

impl std::error::Error for TableObjectMutationError {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct TableBucketMarker {
    pub version: u16,
    pub catalog_type: &'static str,
    pub reserved_prefix: &'static str,
}

impl Default for TableBucketMarker {
    fn default() -> Self {
        Self {
            version: TABLE_BUCKET_CONFIG_VERSION,
            catalog_type: TABLE_BUCKET_CATALOG_TYPE,
            reserved_prefix: TABLE_RESERVED_PREFIX,
        }
    }
}

pub(crate) fn table_bucket_marker_json() -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&TableBucketMarker::default())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct NamespaceMarker {
    pub version: u16,
    pub namespace: String,
}

impl NamespaceMarker {
    pub fn new(namespace: &Namespace) -> Self {
        Self {
            version: TABLE_NAMESPACE_MARKER_VERSION,
            namespace: namespace.public_name(),
        }
    }
}

pub(crate) fn namespace_marker_json(namespace: &Namespace) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&NamespaceMarker::new(namespace))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct TableMarker {
    pub version: u16,
    pub namespace: String,
    pub name: String,
    pub metadata_location: Option<String>,
}

impl TableMarker {
    pub fn new(namespace: &Namespace, table: &IdentifierSegment) -> Self {
        Self {
            version: TABLE_RESOURCE_MARKER_VERSION,
            namespace: namespace.public_name(),
            name: table.as_str().to_string(),
            metadata_location: None,
        }
    }
}

pub(crate) fn table_marker_json(namespace: &Namespace, table: &IdentifierSegment) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&TableMarker::new(namespace, table))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TableMetadataPointer {
    pub version: u16,
    pub metadata_location: String,
}

impl TableMetadataPointer {
    pub fn new(metadata_location: String) -> Self {
        Self {
            version: TABLE_METADATA_POINTER_VERSION,
            metadata_location,
        }
    }
}

pub(crate) fn table_metadata_pointer_json(metadata_location: String) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&TableMetadataPointer::new(metadata_location))
}

pub(crate) fn parse_table_metadata_pointer(data: &[u8]) -> Result<TableMetadataPointer, serde_json::Error> {
    serde_json::from_slice(data)
}

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
    pub fn parse(value: &str) -> Result<Self, CatalogIdentifierError> {
        if value.is_empty() {
            return Err(CatalogIdentifierError::Empty);
        }

        let mut segments = Vec::new();
        for segment in value.split('.') {
            segments.push(IdentifierSegment::parse(segment.to_string())?);
        }

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
pub struct TableIdentifier {
    warehouse: IdentifierSegment,
    namespace: Namespace,
    name: IdentifierSegment,
}

impl TableIdentifier {
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
    pub fn current_pointer_path(&self, table: &TableIdentifier) -> String {
        format!("{}/{}", self.table_root(table), CURRENT_POINTER_FILE)
    }

    pub fn metadata_dir_path(&self, table: &TableIdentifier) -> String {
        format!("{}/{}", self.table_root(table), METADATA_DIR)
    }

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

pub(crate) fn default_namespace_marker_path(namespace: &Namespace) -> String {
    format!("{}{}/{}", default_namespace_root_prefix(), namespace.storage_id(), NAMESPACE_MARKER_FILE)
}

pub(crate) fn default_table_root_prefix(namespace: &Namespace) -> String {
    format!("{}{}/{}/", default_namespace_root_prefix(), namespace.storage_id(), TABLE_ROOT)
}

pub(crate) fn default_table_marker_path(namespace: &Namespace, table: &IdentifierSegment) -> String {
    format!("{}{}/{}", default_table_root_prefix(namespace), table.as_str(), TABLE_MARKER_FILE)
}

pub(crate) fn default_table_metadata_dir_path(namespace: &Namespace, table: &IdentifierSegment) -> String {
    format!("{}{}/{}", default_table_root_prefix(namespace), table.as_str(), METADATA_DIR)
}

pub(crate) fn default_table_metadata_file_path(
    namespace: &Namespace,
    table: &IdentifierSegment,
    metadata_file_name: &str,
) -> String {
    format!("{}/{}", default_table_metadata_dir_path(namespace, table), metadata_file_name)
}

pub(crate) fn default_table_current_pointer_path(namespace: &Namespace, table: &IdentifierSegment) -> String {
    format!("{}{}/{}", default_table_root_prefix(namespace), table.as_str(), CURRENT_POINTER_FILE)
}

pub(crate) fn default_table_lifecycle_path(namespace: &Namespace, table: &IdentifierSegment) -> String {
    format!("{}{}/{}", default_table_root_prefix(namespace), table.as_str(), LIFECYCLE_FILE)
}

pub(crate) fn namespace_name_from_marker_path(object_key: &str) -> Option<String> {
    let prefix = default_namespace_root_prefix();
    let suffix = format!("/{NAMESPACE_MARKER_FILE}");

    object_key
        .strip_prefix(prefix.as_str())
        .and_then(|value| value.strip_suffix(suffix.as_str()))
        .filter(|value| !value.is_empty())
        .map(|value| value.replace('/', "."))
}

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

pub(crate) fn is_valid_table_metadata_file_name(metadata_file_name: &str) -> bool {
    if metadata_file_name.is_empty()
        || metadata_file_name.len() > TABLE_METADATA_FILE_NAME_MAX_LEN
        || !metadata_file_name.ends_with(".json")
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
    let table_bucket_enabled = metadata_sys::get(bucket)
        .await
        .is_ok_and(|metadata| metadata.table_bucket_enabled());

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserved_table_object_key_matches_exact_prefix_and_children_only() {
        assert!(is_reserved_table_object_key(".rustfs-table"));
        assert!(is_reserved_table_object_key(".rustfs-table/"));
        assert!(is_reserved_table_object_key(".rustfs-table/metadata/current.json"));

        assert!(!is_reserved_table_object_key(""));
        assert!(!is_reserved_table_object_key(".rustfs-table-other"));
        assert!(!is_reserved_table_object_key("prefix/.rustfs-table/object"));
        assert!(!is_reserved_table_object_key("user/.rustfs-table"));
    }

    #[test]
    fn object_mutation_guard_only_blocks_reserved_prefix_for_table_buckets() {
        assert!(validate_object_mutation(false, ".rustfs-table/current.json").is_ok());
        assert_eq!(
            validate_object_mutation(true, ".rustfs-table/current.json").unwrap_err(),
            TableObjectMutationError::ReservedCatalogObject
        );
        assert!(validate_object_mutation(true, ".rustfs-table-other/current.json").is_ok());
    }

    #[tokio::test]
    async fn bucket_object_mutation_guard_allows_when_bucket_metadata_is_unavailable() {
        assert!(
            validate_bucket_object_mutation("missing-bucket", ".rustfs-table/current.json")
                .await
                .is_ok()
        );
    }

    #[test]
    fn table_bucket_marker_json_uses_stable_catalog_defaults() {
        let marker = serde_json::to_value(TableBucketMarker::default()).unwrap();

        assert_eq!(marker["version"], TABLE_BUCKET_CONFIG_VERSION);
        assert_eq!(marker["catalog_type"], TABLE_BUCKET_CATALOG_TYPE);
        assert_eq!(marker["reserved_prefix"], TABLE_RESERVED_PREFIX);
        assert!(!table_bucket_marker_json().unwrap().is_empty());
    }

    #[test]
    fn namespace_marker_path_stays_under_default_reserved_boundary() {
        let namespace = Namespace::parse("analytics.daily_events").unwrap();

        assert_eq!(
            default_namespace_marker_path(&namespace),
            ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/namespace.json"
        );
        assert_eq!(namespace.public_name(), "analytics.daily_events");
    }

    #[test]
    fn namespace_marker_path_extracts_public_name() {
        assert_eq!(
            namespace_name_from_marker_path(".rustfs-table/warehouses/default/namespaces/analytics/daily_events/namespace.json"),
            Some("analytics.daily_events".to_string())
        );
        assert_eq!(
            namespace_name_from_marker_path(
                ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/current.json"
            ),
            None
        );
        assert_eq!(
            namespace_name_from_marker_path(".rustfs-table/warehouses/other/namespaces/analytics/daily_events/namespace.json"),
            None
        );
    }

    #[test]
    fn namespace_marker_json_uses_stable_catalog_defaults() {
        let namespace = Namespace::parse("analytics.daily_events").unwrap();
        let marker = serde_json::to_value(NamespaceMarker::new(&namespace)).unwrap();

        assert_eq!(marker["version"], TABLE_NAMESPACE_MARKER_VERSION);
        assert_eq!(marker["namespace"], "analytics.daily_events");
        assert!(!namespace_marker_json(&namespace).unwrap().is_empty());
    }

    #[test]
    fn table_marker_path_stays_under_namespace_reserved_boundary() {
        let namespace = Namespace::parse("analytics.daily_events").unwrap();
        let table = IdentifierSegment::parse("events").unwrap();

        assert_eq!(
            default_table_root_prefix(&namespace),
            ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/"
        );
        assert_eq!(
            default_table_marker_path(&namespace, &table),
            ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/table.json"
        );
    }

    #[test]
    fn table_marker_path_extracts_table_name() {
        let namespace = Namespace::parse("analytics.daily_events").unwrap();

        assert_eq!(
            table_name_from_marker_path(
                &namespace,
                ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/table.json"
            ),
            Some("events".to_string())
        );
        assert_eq!(
            table_name_from_marker_path(
                &namespace,
                ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/metadata/current.json"
            ),
            None
        );
        assert_eq!(
            table_name_from_marker_path(
                &namespace,
                ".rustfs-table/warehouses/default/namespaces/analytics/other/tables/events/table.json"
            ),
            None
        );
    }

    #[test]
    fn table_marker_json_uses_stable_catalog_defaults() {
        let namespace = Namespace::parse("analytics.daily_events").unwrap();
        let table = IdentifierSegment::parse("events").unwrap();
        let marker = serde_json::to_value(TableMarker::new(&namespace, &table)).unwrap();

        assert_eq!(marker["version"], TABLE_RESOURCE_MARKER_VERSION);
        assert_eq!(marker["namespace"], "analytics.daily_events");
        assert_eq!(marker["name"], "events");
        assert!(marker["metadata_location"].is_null());
        assert!(!table_marker_json(&namespace, &table).unwrap().is_empty());
    }

    #[test]
    fn table_current_pointer_path_stays_under_table_boundary() {
        let namespace = Namespace::parse("analytics.daily_events").unwrap();
        let table = IdentifierSegment::parse("events").unwrap();

        assert_eq!(
            default_table_metadata_dir_path(&namespace, &table),
            ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/metadata"
        );
        assert_eq!(
            default_table_current_pointer_path(&namespace, &table),
            ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/current.json"
        );
        assert_eq!(
            default_table_lifecycle_path(&namespace, &table),
            ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/lifecycle.json"
        );
    }

    #[test]
    fn table_metadata_file_path_stays_under_metadata_boundary() {
        let namespace = Namespace::parse("analytics.daily_events").unwrap();
        let table = IdentifierSegment::parse("events").unwrap();
        let table_identifier =
            TableIdentifier::new(IdentifierSegment::parse(DEFAULT_WAREHOUSE_ID).unwrap(), namespace.clone(), table.clone());

        assert_eq!(
            default_table_metadata_file_path(&namespace, &table, "00001.metadata.json"),
            ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/metadata/00001.metadata.json"
        );
        assert_eq!(
            TablePathResolver::default().metadata_file_path(&table_identifier, "00001.metadata.json"),
            ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/metadata/00001.metadata.json"
        );
    }

    #[test]
    fn table_metadata_file_name_validation_rejects_unsafe_names() {
        assert!(is_valid_table_metadata_file_name("00001.metadata.json"));
        assert!(is_valid_table_metadata_file_name("v1-4f2c_metadata.json"));

        assert!(!is_valid_table_metadata_file_name(""));
        assert!(!is_valid_table_metadata_file_name(".metadata.json"));
        assert!(!is_valid_table_metadata_file_name("00001.metadata"));
        assert!(!is_valid_table_metadata_file_name("00001.JSON"));
        assert!(!is_valid_table_metadata_file_name("../current.json"));
        assert!(!is_valid_table_metadata_file_name("nested/00001.json"));
        assert!(!is_valid_table_metadata_file_name("nested%2f00001.json"));
        assert!(!is_valid_table_metadata_file_name("00001\\metadata.json"));
        assert!(!is_valid_table_metadata_file_name("00001\nmetadata.json"));
    }

    #[test]
    fn table_metadata_location_validation_stays_inside_metadata_dir() {
        let namespace = Namespace::parse("analytics.daily_events").unwrap();
        let table = IdentifierSegment::parse("events").unwrap();

        assert!(is_valid_table_metadata_location(
            &namespace,
            &table,
            ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/metadata/00001.metadata.json"
        ));
        assert!(!is_valid_table_metadata_location(&namespace, &table, ""));
        assert!(!is_valid_table_metadata_location(
            &namespace,
            &table,
            ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/current.json"
        ));
        assert!(!is_valid_table_metadata_location(
            &namespace,
            &table,
            ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/other/metadata/00001.json"
        ));
        assert!(!is_valid_table_metadata_location(
            &namespace,
            &table,
            ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/metadata/../current.json"
        ));
        assert!(!is_valid_table_metadata_location(
            &namespace,
            &table,
            ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/metadata/nested/00001.json"
        ));
    }

    #[test]
    fn metadata_location_from_metadata_file_path_extracts_table_metadata_only() {
        let namespace = Namespace::parse("analytics.daily_events").unwrap();
        let table = IdentifierSegment::parse("events").unwrap();

        assert_eq!(
            metadata_location_from_metadata_file_path(
                &namespace,
                &table,
                ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/metadata/00001.metadata.json"
            ),
            Some(
                ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/metadata/00001.metadata.json"
                    .to_string()
            )
        );
        assert_eq!(
            metadata_location_from_metadata_file_path(
                &namespace,
                &table,
                ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/current.json"
            ),
            None
        );
        assert_eq!(
            metadata_location_from_metadata_file_path(
                &namespace,
                &table,
                ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/metadata/nested/00001.metadata.json"
            ),
            None
        );
        assert_eq!(
            metadata_location_from_metadata_file_path(
                &namespace,
                &table,
                ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/other/metadata/00001.metadata.json"
            ),
            None
        );
    }

    #[test]
    fn table_metadata_pointer_json_round_trips() {
        let location =
            ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/metadata/00001.json".to_string();
        let data = table_metadata_pointer_json(location.clone()).unwrap();
        let pointer = parse_table_metadata_pointer(&data).unwrap();

        assert_eq!(pointer.version, TABLE_METADATA_POINTER_VERSION);
        assert_eq!(pointer.metadata_location, location);
    }

    #[test]
    fn object_mutation_entrypoints_call_reserved_prefix_guard() {
        let source = include_str!("app/object_usecase.rs");
        for expected in [
            "validate_object_key(&key, request_method_name)?;\n        validate_table_catalog_object_mutation(&bucket, &key).await?;",
            "validate_object_key(&key, \"COPY (dest)\")?;\n        validate_table_catalog_object_mutation(&bucket, &key).await?;",
            "if let Err(err) = validate_table_catalog_object_mutation(&bucket, &obj_id.key).await",
            "validate_object_key(&key, \"DELETE\")?;\n        validate_table_catalog_object_mutation(&bucket, &key).await?;",
            "validate_table_catalog_object_mutation(&bucket, &object).await?;",
            "validate_object_key(&key, \"PUT\")?;\n        validate_table_catalog_object_mutation(&bucket, &key).await?;",
            "validate_table_catalog_object_mutation(&bucket, &fpath).await?;",
        ] {
            assert!(source.contains(expected), "missing object mutation guard: {expected}");
        }
    }

    #[test]
    fn multipart_mutation_entrypoints_call_reserved_prefix_guard() {
        let source = include_str!("app/multipart_usecase.rs");

        assert_eq!(
            source
                .matches("validate_table_catalog_object_mutation(&bucket, &key).await?;")
                .count(),
            4
        );
    }

    #[test]
    fn object_metadata_mutation_entrypoints_call_reserved_prefix_guard() {
        let source = include_str!("storage/ecfs.rs");

        assert_eq!(
            source
                .matches("validate_table_catalog_object_mutation(&bucket, &object).await?;")
                .count(),
            2
        );
        assert_eq!(
            source
                .matches("validate_table_catalog_object_mutation(&bucket, &key).await?;")
                .count(),
            2
        );
    }

    #[test]
    fn identifier_segment_accepts_conservative_catalog_names() {
        for value in [
            "a",
            "a1",
            "a-b",
            "a_b",
            "abc123",
            "a23456789012345678901234567890123456789012345678901234567890123",
        ] {
            assert_eq!(IdentifierSegment::parse(value).unwrap().as_str(), value);
        }
    }

    #[test]
    fn identifier_segment_rejects_ambiguous_or_unsafe_names() {
        for value in [
            "",
            ".",
            "..",
            "Upper",
            "has.dot",
            "has/slash",
            "has\\slash",
            "has%2fslash",
            "-leading",
            "trailing-",
            "_leading",
            "trailing_",
            "has space",
            "name\nbreak",
        ] {
            assert!(IdentifierSegment::parse(value).is_err(), "value should be rejected: {value:?}");
        }

        let too_long = "a".repeat(IdentifierSegment::MAX_LEN + 1);
        assert!(IdentifierSegment::parse(too_long).is_err());
    }

    #[test]
    fn namespace_uses_dot_syntax_for_public_identity_and_slash_for_storage() {
        let namespace = Namespace::parse("analytics.daily_events").unwrap();

        assert_eq!(namespace.segments().len(), 2);
        assert_eq!(namespace.storage_id(), "analytics/daily_events");
    }

    #[test]
    fn resolver_builds_paths_under_reserved_table_boundary() {
        let table = TableIdentifier::new(
            IdentifierSegment::parse("warehouse1").unwrap(),
            Namespace::parse("analytics.daily").unwrap(),
            IdentifierSegment::parse("events").unwrap(),
        );
        let resolver = TablePathResolver::default();

        assert_eq!(
            resolver.current_pointer_path(&table),
            ".rustfs-table/warehouses/warehouse1/namespaces/analytics/daily/tables/events/current.json"
        );
        assert_eq!(
            resolver.metadata_dir_path(&table),
            ".rustfs-table/warehouses/warehouse1/namespaces/analytics/daily/tables/events/metadata"
        );
    }
}
