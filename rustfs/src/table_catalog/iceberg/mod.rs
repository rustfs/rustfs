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

mod backup;
mod backup_operations;
mod cleanup;
mod cleanup_metadata;
mod commit;
mod graph_proof;
mod maintenance;
mod manifest;
mod metadata;
mod validation;

pub(crate) use commit::*;
pub(crate) use graph_proof::*;
pub(crate) use maintenance::*;
pub(crate) use manifest::*;
pub(crate) use metadata::*;
pub(crate) use validation::*;

pub(crate) const TABLE_METADATA_DIGEST_REQUIREMENT_TYPE: &str = "assert-rustfs-metadata-sha256";
pub(crate) const TABLE_METADATA_FILE_NAME_MAX_LEN: usize = 128;
pub(crate) const TABLE_METADATA_JSON_MAX_SIZE: usize = 50 * 1024 * 1024;
pub(crate) const TABLE_MANIFEST_AVRO_MAX_SIZE: usize = 128 * 1024 * 1024;
pub(crate) const TABLE_MANIFEST_AVRO_MAX_DECODED_SIZE: usize = 128 * 1024 * 1024;
pub(crate) const TABLE_MANIFEST_AVRO_MAX_RECORDS: usize = 1_000_000;
pub(crate) const TABLE_MANIFEST_AVRO_MAX_HEADER_ENTRIES: usize = 1_024;
pub(crate) const TABLE_COMMIT_MAX_MANIFESTS: usize = 10_000;
pub(crate) const TABLE_COMMIT_MAX_MANIFEST_TRAVERSALS: usize = 20_000;
pub(crate) const TABLE_COMMIT_MAX_AVRO_BYTES: usize = 512 * 1024 * 1024;
pub(crate) const TABLE_COMMIT_MAX_FILE_REFERENCES: usize = 1_000_000;
pub(crate) const TABLE_COMMIT_MAX_STATISTICS_OBJECTS: usize = 1_024;
pub(crate) const TABLE_COMMIT_MAX_STATISTICS_BYTES: usize = 512 * 1024 * 1024;
pub(crate) const TABLE_COMPACTION_MAX_STORED_BYTES: usize = 512 * 1024 * 1024;
pub(crate) const TABLE_COMPACTION_MAX_DECODED_BYTES: usize = 512 * 1024 * 1024;
pub(crate) const TABLE_STATISTICS_FILE_MAX_SIZE: usize = 128 * 1024 * 1024;
pub(crate) const TABLE_COMMIT_OBJECT_VALIDATION_CONCURRENCY: usize = 16;
pub(crate) const ICEBERG_MAIN_REF: &str = "main";
pub(crate) const ICEBERG_MIN_SNAPSHOTS_TO_KEEP_PROPERTY: &str = "history.expire.min-snapshots-to-keep";
pub(crate) const ICEBERG_MAX_SNAPSHOT_AGE_MS_PROPERTY: &str = "history.expire.max-snapshot-age-ms";
pub(crate) const ICEBERG_MAX_REF_AGE_MS_PROPERTY: &str = "history.expire.max-ref-age-ms";
pub(crate) const ICEBERG_REF_MIN_SNAPSHOTS_TO_KEEP_FIELD: &str = "min-snapshots-to-keep";
pub(crate) const ICEBERG_REF_MAX_SNAPSHOT_AGE_MS_FIELD: &str = "max-snapshot-age-ms";
pub(crate) const ICEBERG_REF_MAX_REF_AGE_MS_FIELD: &str = "max-ref-age-ms";
