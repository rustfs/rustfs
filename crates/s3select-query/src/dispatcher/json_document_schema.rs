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

use datafusion::{
    arrow::{
        datatypes::{DataType, Field, Schema},
        error::ArrowError,
    },
    common::{DataFusionError, Result as DFResult},
};
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use std::{
    collections::HashMap,
    fmt,
    io::{self, Cursor},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

const CANCELLATION_CHECK_ITEMS: usize = 1024;
const READER_BUFFER_BYTES: usize = 64 * 1024;

pub(super) fn infer_schema(
    bytes: &[u8],
    expected_records: usize,
    cancellation: &AtomicBool,
    max_schema_bytes: usize,
) -> DFResult<(Schema, usize, usize)> {
    let mut context = InferenceContext {
        cancellation,
        items_since_check: 0,
    };
    let mut fields = InferredObject::default();
    let mut records = 0;

    for row in bytes.split_inclusive(|byte| *byte == b'\n') {
        if row.is_empty() {
            continue;
        }
        context.check_now()?;
        let reader = io::BufReader::with_capacity(
            READER_BUFFER_BYTES,
            CancellableReader {
                inner: Cursor::new(row),
                cancellation,
            },
        );
        let mut deserializer = serde_json::Deserializer::from_reader(reader);
        let inferred = InferSeed { context: &mut context }
            .deserialize(&mut deserializer)
            .map_err(|error| schema_parse_error(error, cancellation))?;
        deserializer.end().map_err(|error| schema_parse_error(error, cancellation))?;
        let InferredType::Object(record) = inferred else {
            return Err(DataFusionError::Execution(
                "JSON DOCUMENT schema inference expected an object row".to_string(),
            ));
        };
        if record.retained_size() > max_schema_bytes {
            return Err(schema_complexity_error());
        }
        fields.merge(record, &mut context)?;
        if fields.retained_size() > max_schema_bytes {
            return Err(schema_complexity_error());
        }
        records += 1;
    }

    if records != expected_records {
        return Err(DataFusionError::Execution(format!(
            "JSON DOCUMENT schema prefix contained {expected_records} records but inference read {records}"
        )));
    }
    let schema = Schema::new(fields.into_fields(&mut context)?);
    let schema_bytes = std::mem::size_of::<Schema>()
        .checked_add(schema.fields().size())
        .ok_or_else(schema_complexity_error)?;
    if schema_bytes > max_schema_bytes {
        return Err(schema_complexity_error());
    }
    Ok((schema, records, schema_bytes))
}

fn schema_parse_error(error: serde_json::Error, cancellation: &AtomicBool) -> DataFusionError {
    if cancellation.load(Ordering::Acquire) {
        DataFusionError::Execution("JSON DOCUMENT schema inference canceled".to_string())
    } else {
        schema_json_error(format!("JSON DOCUMENT schema inference failed: {error}"))
    }
}

fn schema_json_error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::ArrowError(Box::new(ArrowError::JsonError(message.into())), None)
}

fn schema_complexity_error() -> DataFusionError {
    DataFusionError::ResourcesExhausted("JSON DOCUMENT schema exceeds the inference complexity limit".to_string())
}

struct CancellableReader<'a> {
    inner: Cursor<&'a [u8]>,
    cancellation: &'a AtomicBool,
}

impl io::Read for CancellableReader<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.cancellation.load(Ordering::Acquire) {
            return Err(io::Error::new(io::ErrorKind::Interrupted, "JSON DOCUMENT schema inference canceled"));
        }
        io::Read::read(&mut self.inner, buffer)
    }
}

struct InferenceContext<'a> {
    cancellation: &'a AtomicBool,
    items_since_check: usize,
}

impl InferenceContext<'_> {
    fn checkpoint<E: de::Error>(&mut self) -> Result<(), E> {
        self.items_since_check += 1;
        if self.items_since_check < CANCELLATION_CHECK_ITEMS {
            return Ok(());
        }
        self.items_since_check = 0;
        if self.cancellation.load(Ordering::Acquire) {
            Err(E::custom("JSON DOCUMENT schema inference canceled"))
        } else {
            Ok(())
        }
    }

    fn check_now(&self) -> DFResult<()> {
        if self.cancellation.load(Ordering::Acquire) {
            Err(DataFusionError::Execution("JSON DOCUMENT schema inference canceled".to_string()))
        } else {
            Ok(())
        }
    }

    fn checkpoint_df(&mut self) -> DFResult<()> {
        self.items_since_check += 1;
        if self.items_since_check < CANCELLATION_CHECK_ITEMS {
            return Ok(());
        }
        self.items_since_check = 0;
        self.check_now()
    }
}

struct InferSeed<'a, 'b> {
    context: &'a mut InferenceContext<'b>,
}

impl<'de> DeserializeSeed<'de> for InferSeed<'_, '_> {
    type Value = InferredType;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(InferVisitor { context: self.context })
    }
}

struct InferVisitor<'a, 'b> {
    context: &'a mut InferenceContext<'b>,
}

impl<'de> Visitor<'de> for InferVisitor<'_, '_> {
    type Value = InferredType;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value")
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(InferredType::Any)
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(InferredType::Any)
    }

    fn visit_bool<E>(self, _value: bool) -> Result<Self::Value, E> {
        Ok(InferredType::Scalar(ScalarKinds::BOOLEAN))
    }

    fn visit_i64<E>(self, _value: i64) -> Result<Self::Value, E> {
        Ok(InferredType::Scalar(ScalarKinds::INTEGER))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(InferredType::Scalar(if i64::try_from(value).is_ok() {
            ScalarKinds::INTEGER
        } else {
            ScalarKinds::FLOAT
        }))
    }

    fn visit_f64<E>(self, _value: f64) -> Result<Self::Value, E> {
        Ok(InferredType::Scalar(ScalarKinds::FLOAT))
    }

    fn visit_str<E>(self, _value: &str) -> Result<Self::Value, E> {
        Ok(InferredType::Scalar(ScalarKinds::STRING))
    }

    fn visit_string<E>(self, _value: String) -> Result<Self::Value, E> {
        Ok(InferredType::Scalar(ScalarKinds::STRING))
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut inner = InferredType::Any;
        let mut first = true;
        while let Some(value) = sequence.next_element_seed(InferSeed { context: self.context })? {
            self.context.checkpoint()?;
            if first && matches!(value, InferredType::Any) {
                inner = InferredType::Scalar(ScalarKinds::default());
            } else {
                inner.merge_array_element(value, self.context).map_err(de::Error::custom)?;
            }
            first = false;
        }
        Ok(InferredType::Array(Box::new(inner)))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut object = InferredObject::default();
        while let Some(key) = map.next_key::<String>()? {
            self.context.checkpoint()?;
            let value = map.next_value_seed(InferSeed { context: self.context })?;
            object.insert_record_field(key, value);
        }
        Ok(InferredType::Object(object))
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct ScalarKinds(u8);

impl ScalarKinds {
    const BOOLEAN: Self = Self(1 << 0);
    const INTEGER: Self = Self(1 << 1);
    const FLOAT: Self = Self(1 << 2);
    const STRING: Self = Self(1 << 3);

    fn merge(&mut self, other: Self) {
        self.0 |= other.0;
    }

    fn data_type(self) -> DataType {
        match self.0 {
            value if value == Self::BOOLEAN.0 => DataType::Boolean,
            value if value == Self::INTEGER.0 => DataType::Int64,
            value if value == Self::FLOAT.0 || value == (Self::INTEGER.0 | Self::FLOAT.0) => DataType::Float64,
            value if value == Self::STRING.0 => DataType::Utf8,
            _ => DataType::Utf8,
        }
    }
}

#[derive(Debug)]
enum InferredType {
    Scalar(ScalarKinds),
    Array(Box<InferredType>),
    Object(InferredObject),
    Any,
}

impl InferredType {
    fn merge(&mut self, other: Self, context: &mut InferenceContext<'_>) -> DFResult<()> {
        match (self, other) {
            (Self::Array(current), Self::Array(other)) => current.merge(*other, context),
            (Self::Scalar(current), Self::Scalar(other)) => {
                current.merge(other);
                Ok(())
            }
            (Self::Object(current), Self::Object(other)) => current.merge(other, context),
            (current @ Self::Any, value) => {
                *current = value;
                Ok(())
            }
            (_, Self::Any) => Ok(()),
            (Self::Array(inner), scalar @ Self::Scalar(_)) => inner.merge(scalar, context),
            (current @ Self::Scalar(_), Self::Array(mut inner)) => {
                let scalar = std::mem::replace(current, Self::Any);
                inner.merge(scalar, context)?;
                *current = Self::Array(inner);
                Ok(())
            }
            (current, other) => Err(schema_json_error(format!(
                "incompatible JSON types during schema inference: {} and {}",
                current.kind(),
                other.kind()
            ))),
        }
    }

    fn merge_array_element(&mut self, other: Self, context: &mut InferenceContext<'_>) -> DFResult<()> {
        match (self, other) {
            (current @ Self::Any, value) => {
                *current = value;
                Ok(())
            }
            (_, Self::Any) => Ok(()),
            (Self::Scalar(current), Self::Scalar(other)) => {
                current.merge(other);
                Ok(())
            }
            (Self::Object(current), Self::Object(other)) => current.merge(other, context),
            (Self::Array(current), Self::Array(other)) => current.merge(*other, context),
            (current, other) => Err(schema_json_error(format!(
                "incompatible JSON array elements during schema inference: {} and {}",
                current.kind(),
                other.kind()
            ))),
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::Scalar(_) => "scalar",
            Self::Array(_) => "array",
            Self::Object(_) => "object",
            Self::Any => "null",
        }
    }

    fn into_data_type(self, context: &mut InferenceContext<'_>) -> DFResult<DataType> {
        context.check_now()?;
        match self {
            Self::Scalar(kinds) => Ok(kinds.data_type()),
            Self::Array(inner) => Ok(DataType::List(Arc::new(Field::new_list_field(inner.into_data_type(context)?, true)))),
            Self::Object(object) => Ok(DataType::Struct(object.into_fields(context)?.into())),
            Self::Any => Ok(DataType::Null),
        }
    }

    fn retained_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + match self {
                Self::Array(inner) => inner.retained_size(),
                Self::Object(object) => object.retained_size(),
                Self::Scalar(_) | Self::Any => 0,
            }
    }
}

#[derive(Debug, Default)]
struct InferredObject {
    indexes: HashMap<Arc<str>, usize>,
    fields: Vec<(Arc<str>, InferredType)>,
}

impl InferredObject {
    fn insert_record_field(&mut self, key: String, value: InferredType) {
        if let Some(index) = self.indexes.get(key.as_str()).copied() {
            self.fields[index].1 = value;
            return;
        }
        let key: Arc<str> = Arc::from(key);
        let index = self.fields.len();
        self.indexes.insert(Arc::clone(&key), index);
        self.fields.push((key, value));
    }

    fn merge(&mut self, other: Self, context: &mut InferenceContext<'_>) -> DFResult<()> {
        for (key, value) in other.fields {
            context.checkpoint_df()?;
            if let Some(index) = self.indexes.get(key.as_ref()).copied() {
                self.fields[index].1.merge(value, context)?;
            } else {
                let index = self.fields.len();
                self.indexes.insert(Arc::clone(&key), index);
                self.fields.push((key, value));
            }
        }
        Ok(())
    }

    fn into_fields(self, context: &mut InferenceContext<'_>) -> DFResult<Vec<Field>> {
        self.fields
            .into_iter()
            .map(|(name, inferred)| {
                context.check_now()?;
                Ok(Field::new(name.as_ref(), inferred.into_data_type(context)?, true))
            })
            .collect()
    }

    fn retained_size(&self) -> usize {
        let index_entry = std::mem::size_of::<(Arc<str>, usize)>() + 16;
        let mut bytes = std::mem::size_of::<Self>()
            .saturating_add(self.indexes.capacity().saturating_mul(index_entry))
            .saturating_add(
                self.fields
                    .capacity()
                    .saturating_mul(std::mem::size_of::<(Arc<str>, InferredType)>()),
            );
        for (name, inferred) in &self.fields {
            bytes = bytes.saturating_add(name.len()).saturating_add(inferred.retained_size());
        }
        bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::json::reader::infer_json_schema as arrow_infer_json_schema;

    const TEST_SCHEMA_LIMIT: usize = 4 * 1024 * 1024;

    fn assert_matches_arrow(input: &[u8]) {
        let records = input.iter().filter(|byte| **byte == b'\n').count();
        let (expected, expected_records) = arrow_infer_json_schema(io::BufReader::new(input), Some(records))
            .expect("Arrow should infer the compatibility fixture");
        let (actual, actual_records, _) = infer_schema(input, records, &AtomicBool::new(false), TEST_SCHEMA_LIMIT)
            .expect("streaming inference should accept the compatibility fixture");
        assert_eq!(actual_records, expected_records);
        assert_eq!(actual, expected);
    }

    #[test]
    fn inference_matches_arrow_for_nested_and_coerced_types() {
        assert_matches_arrow(
            br#"{"a":1,"values":[1,2],"nested":{"enabled":true},"nullable":null}
{"a":1.5,"values":3,"nested":{"name":"ok"},"nullable":"set"}
"#,
        );
        assert_matches_arrow(
            br#"{"matrix":[[1,2],[3]],"objects":[{"id":1},{"name":"two"}],"empty":[]}
{"matrix":[[4.5]],"objects":[],"empty":[null]}
"#,
        );
    }

    #[test]
    fn inference_matches_arrow_for_null_first_arrays_and_duplicate_keys() {
        assert_matches_arrow(
            br#"{"values":[null,1,2],"duplicate":1,"duplicate":"last"}
"#,
        );

        let input = br#"{"values":[null,{"id":1}]}
"#;
        let records = 1;
        assert!(arrow_infer_json_schema(io::BufReader::new(input.as_slice()), Some(records)).is_err());
        assert!(infer_schema(input, records, &AtomicBool::new(false), TEST_SCHEMA_LIMIT).is_err());
    }

    #[test]
    fn inference_checkpoints_observe_cancellation() {
        let cancellation = AtomicBool::new(true);
        let mut context = InferenceContext {
            cancellation: &cancellation,
            items_since_check: CANCELLATION_CHECK_ITEMS - 1,
        };
        let error = context
            .checkpoint::<serde_json::Error>()
            .expect_err("the item checkpoint must observe cancellation");
        assert!(error.to_string().contains("schema inference canceled"));
    }

    #[test]
    fn incompatible_types_preserve_arrow_json_error_classification() {
        let error = infer_schema(b"{\"a\":{}}\n{\"a\":1}\n", 2, &AtomicBool::new(false), TEST_SCHEMA_LIMIT)
            .expect_err("object and scalar fields must remain incompatible");

        assert!(matches!(
            error,
            DataFusionError::ArrowError(source, None) if matches!(*source, ArrowError::JsonError(_))
        ));
    }
}
