// Copyright 2026 RustFS Team
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

use std::{collections::BTreeSet, num::NonZeroU32, sync::Arc};

use lance::deps::arrow_schema::{DataType, Field, Schema as ArrowSchema};
use lance_core::datatypes::Schema;

use super::LanceManifestError;

pub(crate) use crate::vector::model::VectorDistanceMetric;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VectorIndexManifestSpec {
    pub schema_version: u16,
    pub dimension: NonZeroU32,
    pub distance_metric: VectorDistanceMetric,
    pub non_filterable_metadata_keys: BTreeSet<String>,
    pub encryption_descriptor_sha256: crate::table_catalog::CatalogContentSha256,
}

impl VectorIndexManifestSpec {
    pub(super) fn validate(&self) -> Result<(), LanceManifestError> {
        if self.schema_version != 1 {
            return Err(LanceManifestError::UnsupportedFormat {
                reason: "Index schema version must be 1",
            });
        }
        vector_schema(self.dimension)?;
        Ok(())
    }
}

pub(super) fn vector_schema(dimension: NonZeroU32) -> Result<Schema, LanceManifestError> {
    if dimension.get() > 4096 {
        return Err(LanceManifestError::SchemaMismatch {
            reason: "vector dimension exceeds 4096",
        });
    }
    let dimension = i32::try_from(dimension.get()).map_err(|_| LanceManifestError::SchemaMismatch {
        reason: "vector dimension cannot be represented by Arrow",
    })?;
    let arrow_schema = ArrowSchema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), dimension),
            false,
        ),
        lance_arrow::json::json_field("metadata", true),
    ]);
    Schema::try_from(&arrow_schema).map_err(|source| LanceManifestError::engine("create schema", source))
}

pub(super) fn validate_schema(schema: &Schema, dimension: NonZeroU32) -> Result<(), LanceManifestError> {
    let expected = vector_schema(dimension)?;
    // Primitive fixed-size list item nullability is not preserved by Lance's logical type encoding.
    if schema != &expected {
        return Err(LanceManifestError::SchemaMismatch {
            reason: "fields, field IDs, nullability, vector type, dimension or JSON extension differ",
        });
    }
    schema
        .validate()
        .map_err(|source| LanceManifestError::engine("validate schema", source))?;
    Ok(())
}
