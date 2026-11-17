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

use std::{collections::BTreeSet, num::NonZeroU32};

use serde_json::{Map, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum VectorDistanceMetric {
    Cosine,
    Euclidean,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub(crate) enum VectorInputError {
    #[error("vector dimension {dimension} exceeds 4096")]
    InvalidDimension { dimension: u32 },
    #[error("vector dimension must be {expected}, found {actual}")]
    DimensionMismatch { expected: u32, actual: usize },
    #[error("vector element {position} must be finite")]
    NonFiniteValue { position: usize },
    #[error("cosine vectors must contain a non-zero value")]
    ZeroCosineVector,
    #[error("vector metadata contains {count} keys; at most 50 are supported")]
    TooManyMetadataKeys { count: usize },
    #[error("vector metadata size {size_bytes} exceeds 40960 bytes")]
    MetadataTooLarge { size_bytes: usize },
    #[error("vector filterable metadata size {size_bytes} exceeds 2048 bytes")]
    FilterableMetadataTooLarge { size_bytes: usize },
    #[error("non-filterable metadata keys must contain between 1 and 10 entries")]
    InvalidNonFilterableKeyCount,
    #[error("non-filterable metadata key length must be between 1 and 63 characters")]
    InvalidNonFilterableKeyLength,
    #[error("non-filterable metadata keys must be unique")]
    DuplicateNonFilterableKey,
}

pub(crate) fn validate_vector(data: &[f32], dimension: NonZeroU32, metric: VectorDistanceMetric) -> Result<(), VectorInputError> {
    if dimension.get() > 4096 {
        return Err(VectorInputError::InvalidDimension {
            dimension: dimension.get(),
        });
    }
    let expected = usize::try_from(dimension.get()).map_err(|_| VectorInputError::InvalidDimension {
        dimension: dimension.get(),
    })?;
    if expected != data.len() {
        return Err(VectorInputError::DimensionMismatch {
            expected: dimension.get(),
            actual: data.len(),
        });
    }
    if let Some(position) = data.iter().position(|value| !value.is_finite()) {
        return Err(VectorInputError::NonFiniteValue { position });
    }
    if metric == VectorDistanceMetric::Cosine && data.iter().all(|value| *value == 0.0) {
        return Err(VectorInputError::ZeroCosineVector);
    }
    Ok(())
}

pub(crate) fn validate_metadata_key_count(metadata: &Map<String, Value>) -> Result<(), VectorInputError> {
    if metadata.len() > 50 {
        return Err(VectorInputError::TooManyMetadataKeys { count: metadata.len() });
    }
    Ok(())
}

pub(crate) fn validate_non_filterable_keys(keys: &[String]) -> Result<BTreeSet<String>, VectorInputError> {
    if !(1..=10).contains(&keys.len()) {
        return Err(VectorInputError::InvalidNonFilterableKeyCount);
    }
    let mut unique = BTreeSet::new();
    for key in keys {
        if !(1..=63).contains(&key.chars().count()) {
            return Err(VectorInputError::InvalidNonFilterableKeyLength);
        }
        if !unique.insert(key.clone()) {
            return Err(VectorInputError::DuplicateNonFilterableKey);
        }
    }
    Ok(unique)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn vectors_enforce_dimension_and_finite_values() {
        let dimension = NonZeroU32::new(2).unwrap();
        assert!(validate_vector(&[1.0, 2.0], dimension, VectorDistanceMetric::Euclidean).is_ok());
        for data in [
            vec![],
            vec![1.0],
            vec![1.0, 2.0, 3.0],
            vec![f32::NAN, 1.0],
            vec![1.0, f32::INFINITY],
            vec![f32::NEG_INFINITY, 1.0],
        ] {
            assert!(validate_vector(&data, dimension, VectorDistanceMetric::Euclidean).is_err());
        }
        assert!(validate_vector(&[1.0], NonZeroU32::new(4097).unwrap(), VectorDistanceMetric::Euclidean).is_err());
    }

    #[test]
    fn cosine_rejects_zero_vectors_and_euclidean_accepts_them() {
        let dimension = NonZeroU32::new(2).unwrap();
        assert!(validate_vector(&[0.0, -0.0], dimension, VectorDistanceMetric::Cosine).is_err());
        assert!(validate_vector(&[0.0, -0.0], dimension, VectorDistanceMetric::Euclidean).is_ok());
        assert!(validate_vector(&[0.0, 1.0], dimension, VectorDistanceMetric::Cosine).is_ok());
    }

    #[test]
    fn metadata_key_count_preserves_the_fifty_key_boundary() {
        let mut metadata = (0..50)
            .map(|index| (format!("key{index}"), json!(index)))
            .collect::<Map<_, _>>();
        assert!(validate_metadata_key_count(&metadata).is_ok());
        metadata.insert("extra".to_string(), json!(true));
        assert!(validate_metadata_key_count(&metadata).is_err());
    }

    #[test]
    fn non_filterable_keys_validate_count_uniqueness_and_character_length() {
        assert!(validate_non_filterable_keys(&[]).is_err());
        let mut keys = (0..10).map(|index| format!("key{index}")).collect::<Vec<_>>();
        assert_eq!(validate_non_filterable_keys(&keys).unwrap().len(), 10);
        keys.push("extra".to_string());
        assert!(validate_non_filterable_keys(&keys).is_err());
        assert!(validate_non_filterable_keys(&["duplicate".to_string(), "duplicate".to_string()]).is_err());
        assert!(validate_non_filterable_keys(&[String::new()]).is_err());
        assert!(validate_non_filterable_keys(&["x".repeat(63)]).is_ok());
        assert!(validate_non_filterable_keys(&["x".repeat(64)]).is_err());
        assert!(validate_non_filterable_keys(&["é".repeat(63)]).is_ok());
    }

    #[test]
    fn distance_metric_preserves_the_manifest_encoding() {
        assert_eq!(serde_json::to_string(&VectorDistanceMetric::Cosine).unwrap(), "\"cosine\"");
        assert_eq!(serde_json::to_string(&VectorDistanceMetric::Euclidean).unwrap(), "\"euclidean\"");
        assert!(serde_json::from_str::<VectorDistanceMetric>("\"unknown\"").is_err());
    }
}
