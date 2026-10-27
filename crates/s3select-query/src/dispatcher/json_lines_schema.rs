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

use datafusion::arrow::{
    datatypes::{DataType, FieldRef, Fields, Schema},
    error::ArrowError,
    json::reader::{ValueIter, infer_json_schema_from_iterator},
};
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use std::{collections::HashMap, fmt, sync::Arc};

/// Infers a JSON LINES schema with Arrow's types and the source's first-seen
/// key order.
///
/// Arrow walks each record as a `serde_json::Map`, which sorts keys unless the
/// process-wide `serde_json/preserve_order` feature is enabled, so `SELECT *`
/// would otherwise reorder fields and CSV output columns.
pub(super) fn infer_schema(bytes: &[u8], max_records: usize) -> Result<Schema, ArrowError> {
    let schema = infer_json_schema_from_iterator(ValueIter::new(bytes, Some(max_records)))?;
    let mut order = KeyOrder::default();
    // Same records as `ValueIter`: one value per line, blank lines skipped.
    // Arrow has already rejected invalid UTF-8 and JSON within this prefix.
    let records = bytes
        .split(|byte| *byte == b'\n')
        .filter_map(|line| std::str::from_utf8(line).ok())
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .take(max_records);
    for record in records {
        let mut deserializer = serde_json::Deserializer::from_str(record);
        (&mut order)
            .deserialize(&mut deserializer)
            .map_err(|error| ArrowError::JsonError(format!("Not valid JSON: {error}")))?;
    }
    Ok(Schema::new_with_metadata(order.reorder(schema.fields()), schema.metadata().clone()))
}

/// First-seen key order of a JSON object, merged across records. Array
/// elements share their parent's node, matching Arrow's `List<Struct>` shape.
#[derive(Default)]
struct KeyOrder {
    positions: HashMap<String, usize>,
    children: Vec<KeyOrder>,
}

impl KeyOrder {
    fn child(&mut self, key: String) -> &mut KeyOrder {
        let next = self.children.len();
        let position = *self.positions.entry(key).or_insert(next);
        if position == next {
            self.children.push(KeyOrder::default());
        }
        &mut self.children[position]
    }

    fn reorder(&self, fields: &Fields) -> Fields {
        let mut fields: Vec<FieldRef> = fields
            .iter()
            .map(|field| match self.positions.get(field.name()) {
                Some(&position) => {
                    let data_type = self.children[position].reorder_type(field.data_type());
                    Arc::new(field.as_ref().clone().with_data_type(data_type))
                }
                None => Arc::clone(field),
            })
            .collect();
        fields.sort_by_key(|field| self.positions.get(field.name()).copied().unwrap_or(usize::MAX));
        fields.into()
    }

    fn reorder_type(&self, data_type: &DataType) -> DataType {
        match data_type {
            DataType::Struct(fields) => DataType::Struct(self.reorder(fields)),
            DataType::List(item) => {
                DataType::List(Arc::new(item.as_ref().clone().with_data_type(self.reorder_type(item.data_type()))))
            }
            other => other.clone(),
        }
    }
}

impl<'de> DeserializeSeed<'de> for &mut KeyOrder {
    type Value = ();

    fn deserialize<D: de::Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for &mut KeyOrder {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a JSON value")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        while let Some(key) = map.next_key::<String>()? {
            map.next_value_seed(self.child(key))?;
        }
        Ok(())
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        while seq.next_element_seed(&mut *self)?.is_some() {}
        Ok(())
    }

    fn visit_bool<E: de::Error>(self, _: bool) -> Result<(), E> {
        Ok(())
    }

    fn visit_i64<E: de::Error>(self, _: i64) -> Result<(), E> {
        Ok(())
    }

    fn visit_u64<E: de::Error>(self, _: u64) -> Result<(), E> {
        Ok(())
    }

    fn visit_f64<E: de::Error>(self, _: f64) -> Result<(), E> {
        Ok(())
    }

    fn visit_str<E: de::Error>(self, _: &str) -> Result<(), E> {
        Ok(())
    }

    fn visit_unit<E: de::Error>(self) -> Result<(), E> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::infer_schema;
    use datafusion::arrow::datatypes::{DataType, Fields};

    fn names(fields: &Fields) -> Vec<&str> {
        fields.iter().map(|field| field.name().as_str()).collect()
    }

    #[test]
    fn keeps_first_seen_key_order_at_every_level() {
        let input = b"{\"z\":1,\"a\":{\"y\":[{\"k2\":1,\"k1\":2}],\"b\":2}}\n\n{\"m\":null,\"a\":{\"c\":3}}\n";
        let schema = infer_schema(input, 1000).expect("schema");

        assert_eq!(names(schema.fields()), ["z", "a", "m"]);
        let DataType::Struct(a) = schema.field(1).data_type() else {
            panic!("a should be a struct");
        };
        assert_eq!(names(a), ["y", "b", "c"]);
        let DataType::List(item) = a[0].data_type() else {
            panic!("y should be a list");
        };
        let DataType::Struct(y) = item.data_type() else {
            panic!("y items should be structs");
        };
        assert_eq!(names(y), ["k2", "k1"]);
    }

    #[test]
    fn ignores_records_after_the_inference_prefix_like_arrow() {
        let schema = infer_schema(b"{\"b\":1,\"a\":2}\nnot json\n", 1).expect("schema");

        assert_eq!(names(schema.fields()), ["b", "a"]);
    }

    #[test]
    fn keeps_arrow_errors_for_invalid_records() {
        assert!(infer_schema(b"{\"a\":1}\n[1]\n", 1000).is_err());
        assert!(infer_schema(b"{\"a\":\n", 1000).is_err());
    }
}
