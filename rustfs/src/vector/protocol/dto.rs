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

//! Wire types for botocore S3 Vectors model 053554bf4606d7affaad0d40e9f3367b7a7ff771.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// Each modeled structure enters through deserialize_map. The derived field parser
// retains required fields, duplicate detection, and unknown-member compatibility.
macro_rules! json_object {
    (pub(crate) struct $name:ident {
        $($(#[$attribute:meta])* pub(crate) $field:ident: $ty:ty,)*
    }) => {
        #[derive(Debug, Clone, PartialEq, Serialize)]
        #[serde(rename_all = "camelCase")]
        pub(crate) struct $name {
            $($(#[$attribute])* pub(crate) $field: $ty,)*
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                #[derive(Deserialize)]
                #[serde(rename_all = "camelCase")]
                struct Fields {
                    $($(#[$attribute])* $field: $ty,)*
                }
                let Fields { $($field,)* } = deserialize_object(deserializer)?;
                Ok(Self { $($field,)* })
            }
        }
    };
}

macro_rules! string_enum {
    ($name:ident { $($wire:literal => $variant:ident),+ $(,)? }) => {
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let value = String::deserialize(deserializer)?;
                match value.as_str() {
                    $($wire => Ok(Self::$variant),)+
                    _ => Err(serde::de::Error::unknown_variant(&value, &[$($wire),+])),
                }
            }
        }
    };
}

string_enum!(DataType { "float32" => Float32 });
string_enum!(DistanceMetric { "euclidean" => Euclidean, "cosine" => Cosine });
string_enum!(IndexMode { "CLASSIC" => Classic, "ENHANCED" => Enhanced });
string_enum!(SseType { "AES256" => Aes256, "aws:kms" => AwsKms });

json_object! {
    pub(crate) struct CreateIndexInput {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) vector_bucket_name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) vector_bucket_arn: Option<String>,
        pub(crate) index_name: String,
        pub(crate) data_type: DataType,
        pub(crate) dimension: i32,
        pub(crate) distance_metric: DistanceMetric,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) metadata_configuration: Option<MetadataConfiguration>,
        #[serde(default, skip_serializing_if = "Option::is_none", deserialize_with = "deserialize_present")]
        pub(crate) encryption_configuration: Option<EncryptionConfiguration>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) tags: Option<BTreeMap<String, String>>,
    }
}

json_object! {
    pub(crate) struct CreateIndexOutput {
        pub(crate) index_arn: String,
    }
}

json_object! {
    pub(crate) struct CreateVectorBucketInput {
        pub(crate) vector_bucket_name: String,
        #[serde(default, skip_serializing_if = "Option::is_none", deserialize_with = "deserialize_present")]
        pub(crate) encryption_configuration: Option<EncryptionConfiguration>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) tags: Option<BTreeMap<String, String>>,
    }
}

json_object! {
    pub(crate) struct CreateVectorBucketOutput {
        pub(crate) vector_bucket_arn: String,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub(crate) enum DataType {
    #[serde(rename = "float32")]
    Float32,
}

json_object! {
    pub(crate) struct DeleteIndexInput {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) vector_bucket_name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) index_name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) index_arn: Option<String>,
    }
}

json_object! {
    pub(crate) struct DeleteIndexOutput {}
}

json_object! {
    pub(crate) struct DeleteVectorBucketInput {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) vector_bucket_name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) vector_bucket_arn: Option<String>,
    }
}

json_object! {
    pub(crate) struct DeleteVectorBucketOutput {}
}

json_object! {
    pub(crate) struct DeleteVectorBucketPolicyInput {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) vector_bucket_name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) vector_bucket_arn: Option<String>,
    }
}

json_object! {
    pub(crate) struct DeleteVectorBucketPolicyOutput {}
}

json_object! {
    pub(crate) struct DeleteVectorsInput {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) vector_bucket_name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) index_name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) index_arn: Option<String>,
        pub(crate) keys: Vec<String>,
    }
}

json_object! {
    pub(crate) struct DeleteVectorsOutput {}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub(crate) enum DistanceMetric {
    #[serde(rename = "euclidean")]
    Euclidean,
    #[serde(rename = "cosine")]
    Cosine,
}

json_object! {
    pub(crate) struct EncryptionConfiguration {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) sse_type: Option<SseType>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) kms_key_arn: Option<String>,
    }
}

json_object! {
    pub(crate) struct GetIndexInput {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) vector_bucket_name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) index_name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) index_arn: Option<String>,
    }
}

json_object! {
    pub(crate) struct GetIndexOutput {
        pub(crate) index: Index,
    }
}

json_object! {
    pub(crate) struct GetOutputVector {
        pub(crate) key: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) data: Option<VectorData>,
        #[serde(default, skip_serializing_if = "Option::is_none", deserialize_with = "deserialize_document")]
        pub(crate) metadata: Option<serde_json::Value>,
    }
}

json_object! {
    pub(crate) struct GetVectorBucketInput {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) vector_bucket_name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) vector_bucket_arn: Option<String>,
    }
}

json_object! {
    pub(crate) struct GetVectorBucketOutput {
        pub(crate) vector_bucket: VectorBucket,
    }
}

json_object! {
    pub(crate) struct GetVectorBucketPolicyInput {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) vector_bucket_name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) vector_bucket_arn: Option<String>,
    }
}

json_object! {
    pub(crate) struct GetVectorBucketPolicyOutput {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) policy: Option<String>,
    }
}

json_object! {
    pub(crate) struct GetVectorsInput {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) vector_bucket_name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) index_name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) index_arn: Option<String>,
        pub(crate) keys: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) return_data: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) return_metadata: Option<bool>,
    }
}

json_object! {
    pub(crate) struct GetVectorsOutput {
        pub(crate) vectors: Vec<GetOutputVector>,
    }
}

json_object! {
    pub(crate) struct Index {
        pub(crate) vector_bucket_name: String,
        pub(crate) index_name: String,
        pub(crate) index_arn: String,
        #[serde(serialize_with = "serialize_timestamp")]
        pub(crate) creation_time: f64,
        pub(crate) data_type: DataType,
        pub(crate) dimension: i32,
        pub(crate) distance_metric: DistanceMetric,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) metadata_configuration: Option<MetadataConfiguration>,
        #[serde(default, skip_serializing_if = "Option::is_none", deserialize_with = "deserialize_present")]
        pub(crate) encryption_configuration: Option<EncryptionConfiguration>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) index_mode: Option<IndexMode>,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub(crate) enum IndexMode {
    #[serde(rename = "CLASSIC")]
    Classic,
    #[serde(rename = "ENHANCED")]
    Enhanced,
}

json_object! {
    pub(crate) struct IndexSummary {
        pub(crate) vector_bucket_name: String,
        pub(crate) index_name: String,
        pub(crate) index_arn: String,
        #[serde(serialize_with = "serialize_timestamp")]
        pub(crate) creation_time: f64,
    }
}

json_object! {
    pub(crate) struct ListIndexesInput {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) vector_bucket_name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) vector_bucket_arn: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) max_results: Option<i32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) next_token: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) prefix: Option<String>,
    }
}

json_object! {
    pub(crate) struct ListIndexesOutput {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) next_token: Option<String>,
        pub(crate) indexes: Vec<IndexSummary>,
    }
}

json_object! {
    pub(crate) struct ListOutputVector {
        pub(crate) key: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) data: Option<VectorData>,
        #[serde(default, skip_serializing_if = "Option::is_none", deserialize_with = "deserialize_document")]
        pub(crate) metadata: Option<serde_json::Value>,
    }
}

json_object! {
    pub(crate) struct ListTagsForResourceInput {
        pub(crate) resource_arn: String,
    }
}

json_object! {
    pub(crate) struct ListTagsForResourceOutput {
        pub(crate) tags: BTreeMap<String, String>,
    }
}

json_object! {
    pub(crate) struct ListVectorBucketsInput {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) max_results: Option<i32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) next_token: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) prefix: Option<String>,
    }
}

json_object! {
    pub(crate) struct ListVectorBucketsOutput {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) next_token: Option<String>,
        pub(crate) vector_buckets: Vec<VectorBucketSummary>,
    }
}

json_object! {
    pub(crate) struct ListVectorsInput {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) vector_bucket_name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) index_name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) index_arn: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) max_results: Option<i32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) next_token: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) segment_count: Option<i32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) segment_index: Option<i32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) return_data: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) return_metadata: Option<bool>,
    }
}

json_object! {
    pub(crate) struct ListVectorsOutput {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) next_token: Option<String>,
        pub(crate) vectors: Vec<ListOutputVector>,
    }
}

json_object! {
    pub(crate) struct MetadataConfiguration {
        pub(crate) non_filterable_metadata_keys: Vec<String>,
    }
}

json_object! {
    pub(crate) struct PutInputVector {
        pub(crate) key: String,
        pub(crate) data: VectorData,
        #[serde(default, skip_serializing_if = "Option::is_none", deserialize_with = "deserialize_document")]
        pub(crate) metadata: Option<serde_json::Value>,
    }
}

json_object! {
    pub(crate) struct PutVectorBucketDefaultIndexModeInput {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) vector_bucket_name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) vector_bucket_arn: Option<String>,
        pub(crate) default_index_mode: IndexMode,
    }
}

json_object! {
    pub(crate) struct PutVectorBucketDefaultIndexModeOutput {}
}

json_object! {
    pub(crate) struct PutVectorBucketPolicyInput {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) vector_bucket_name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) vector_bucket_arn: Option<String>,
        pub(crate) policy: String,
    }
}

json_object! {
    pub(crate) struct PutVectorBucketPolicyOutput {}
}

json_object! {
    pub(crate) struct PutVectorsInput {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) vector_bucket_name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) index_name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) index_arn: Option<String>,
        pub(crate) vectors: Vec<PutInputVector>,
    }
}

json_object! {
    pub(crate) struct PutVectorsOutput {}
}

json_object! {
    pub(crate) struct QueryOutputVector {
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            serialize_with = "serialize_optional_float"
        )]
        pub(crate) distance: Option<f32>,
        pub(crate) key: String,
        #[serde(default, skip_serializing_if = "Option::is_none", deserialize_with = "deserialize_document")]
        pub(crate) metadata: Option<serde_json::Value>,
    }
}

json_object! {
    pub(crate) struct QueryVectorsInput {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) vector_bucket_name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) index_name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) index_arn: Option<String>,
        pub(crate) top_k: i32,
        pub(crate) query_vector: VectorData,
        #[serde(default, skip_serializing_if = "Option::is_none", deserialize_with = "deserialize_document")]
        pub(crate) filter: Option<serde_json::Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) query_mode: Option<IndexMode>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) return_metadata: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) return_distance: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) next_token: Option<String>,
    }
}

json_object! {
    pub(crate) struct QueryVectorsOutput {
        pub(crate) vectors: Vec<QueryOutputVector>,
        pub(crate) distance_metric: DistanceMetric,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) next_token: Option<String>,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub(crate) enum SseType {
    #[serde(rename = "AES256")]
    Aes256,
    #[serde(rename = "aws:kms")]
    AwsKms,
}

json_object! {
    pub(crate) struct TagResourceInput {
        pub(crate) resource_arn: String,
        pub(crate) tags: BTreeMap<String, String>,
    }
}

json_object! {
    pub(crate) struct TagResourceOutput {}
}

json_object! {
    pub(crate) struct UntagResourceInput {
        pub(crate) resource_arn: String,
        pub(crate) tag_keys: Vec<String>,
    }
}

json_object! {
    pub(crate) struct UntagResourceOutput {}
}

json_object! {
    pub(crate) struct UpdateIndexModeInput {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) vector_bucket_name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) index_name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) index_arn: Option<String>,
        pub(crate) index_mode: IndexMode,
    }
}

json_object! {
    pub(crate) struct UpdateIndexModeOutput {}
}

json_object! {
    pub(crate) struct ValidationExceptionField {
        pub(crate) path: String,
        pub(crate) message: String,
    }
}

json_object! {
    pub(crate) struct VectorBucket {
        pub(crate) vector_bucket_name: String,
        pub(crate) vector_bucket_arn: String,
        #[serde(serialize_with = "serialize_timestamp")]
        pub(crate) creation_time: f64,
        #[serde(default, skip_serializing_if = "Option::is_none", deserialize_with = "deserialize_present")]
        pub(crate) encryption_configuration: Option<EncryptionConfiguration>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) default_index_mode: Option<IndexMode>,
    }
}

json_object! {
    pub(crate) struct VectorBucketSummary {
        pub(crate) vector_bucket_name: String,
        pub(crate) vector_bucket_arn: String,
        #[serde(serialize_with = "serialize_timestamp")]
        pub(crate) creation_time: f64,
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct VectorData {
    #[serde(serialize_with = "serialize_vector")]
    pub(crate) float32: Vec<f32>,
}

fn deserialize_present<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    // A supplied encryption configuration must never become plaintext by omission.
    T::deserialize(deserializer).map(Some)
}

impl<'de> Deserialize<'de> for VectorData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct VectorDataVisitor;
        impl<'de> serde::de::Visitor<'de> for VectorDataVisitor {
            type Value = VectorData;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a vector union with one non-null float32 member")
            }

            fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
            where
                M: serde::de::MapAccess<'de>,
            {
                let mut float32 = None;
                while let Some(key) = map.next_key::<String>()? {
                    if key == "__type" {
                        map.next_value::<serde::de::IgnoredAny>()?;
                    } else if key == "float32" {
                        let value = map.next_value::<Option<Vec<f32>>>()?;
                        if let Some(value) = value {
                            if float32.is_some() {
                                return Err(serde::de::Error::custom("VectorData must contain exactly one member"));
                            }
                            float32 = Some(value);
                        }
                    } else if map.next_value::<Option<serde::de::IgnoredAny>>()?.is_some() {
                        return Err(serde::de::Error::custom("VectorData contains an unknown member"));
                    }
                }
                float32
                    .map(|float32| VectorData { float32 })
                    .ok_or_else(|| serde::de::Error::custom("VectorData requires a non-null float32 member"))
            }
        }
        deserializer.deserialize_map(VectorDataVisitor)
    }
}

fn serialize_timestamp<S>(value: &f64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    if !value.is_finite() {
        return Err(serde::ser::Error::custom("timestamp must be finite"));
    }
    value.serialize(serializer)
}

fn serialize_optional_float<S>(value: &Option<f32>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match value {
        Some(value) if value.is_nan() => serializer.serialize_str("NaN"),
        Some(value) if *value == f32::INFINITY => serializer.serialize_str("Infinity"),
        Some(value) if *value == f32::NEG_INFINITY => serializer.serialize_str("-Infinity"),
        _ => value.serialize(serializer),
    }
}

fn serialize_vector<S>(value: &[f32], serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    if value.iter().any(|value| !value.is_finite()) {
        return Err(serde::ser::Error::custom("vector values must be finite"));
    }
    value.serialize(serializer)
}

fn deserialize_document<'de, D>(deserializer: D) -> Result<Option<serde_json::Value>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    // Smithy documents preserve an explicit null as a supplied document value.
    serde_json::Value::deserialize(deserializer).map(Some)
}

pub(super) fn deserialize_object<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct ObjectVisitor<T>(std::marker::PhantomData<fn() -> T>);
    impl<'de, T: Deserialize<'de>> serde::de::Visitor<'de> for ObjectVisitor<T> {
        type Value = T;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a JSON object")
        }

        fn visit_map<M: serde::de::MapAccess<'de>>(self, map: M) -> Result<T, M::Error> {
            T::deserialize(serde::de::value::MapAccessDeserializer::new(map))
        }
    }
    deserializer.deserialize_map(ObjectVisitor(std::marker::PhantomData))
}
