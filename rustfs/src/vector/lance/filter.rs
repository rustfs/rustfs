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

use std::sync::Arc;

use lance::deps::{
    arrow_array::{BooleanArray, LargeBinaryArray},
    arrow_schema::DataType,
    datafusion::{
        common::DataFusionError,
        logical_expr::{ColumnarValue, Expr, Volatility, create_udf},
        prelude::col,
    },
};
use serde_json::{Map, Value};

use crate::vector::filter::ValidatedFilter;

pub(super) fn filter_expr(filter: ValidatedFilter) -> Expr {
    let function = create_udf(
        "rustfs_vector_metadata_filter",
        vec![DataType::LargeBinary],
        DataType::Boolean,
        Volatility::Immutable,
        Arc::new(move |arguments| {
            let arrays = ColumnarValue::values_to_arrays(arguments)?;
            let [metadata] = arrays.as_slice() else {
                return Err(DataFusionError::Internal("metadata filter requires one column".to_string()));
            };
            let metadata = metadata
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| DataFusionError::Internal("metadata filter requires Lance JSONB values".to_string()))?;
            let empty = Map::new();
            let values = metadata
                .iter()
                .map(|bytes| {
                    let Some(bytes) = bytes else {
                        return Ok(filter.matches(&empty));
                    };
                    let json = lance_arrow::json::decode_json(bytes);
                    let value = serde_json::from_str::<Map<String, Value>>(&json)
                        .map_err(|source| DataFusionError::External(Box::new(source)))?;
                    Ok(filter.matches(&value))
                })
                .collect::<Result<Vec<_>, DataFusionError>>()?;
            Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(values))))
        }),
    );
    function.call(vec![col("metadata")])
}

#[cfg(test)]
mod tests {
    use super::*;
    use lance::deps::{
        arrow_array::{RecordBatch, StringArray},
        arrow_schema::{Field, Schema},
        datafusion::prelude::SessionContext,
    };
    use serde_json::{Value, json};
    use std::collections::BTreeSet;

    async fn selected_keys(filter: Value, metadata: Vec<Value>) -> Vec<String> {
        let keys = StringArray::from_iter_values((0..metadata.len()).map(|index| index.to_string()));
        let json = lance_arrow::json::JsonArray::try_from_iter(metadata.iter().map(|value| Some(value.to_string())))
            .expect("encode JSON fixture");
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            lance_arrow::json::json_field("metadata", true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(keys), Arc::new(json.into_inner())]).expect("fixture batch");
        let filter = ValidatedFilter::parse(&filter, &BTreeSet::new()).expect("validated filter");
        let batches = SessionContext::new()
            .read_batch(batch)
            .expect("read batch")
            .filter(filter_expr(filter))
            .expect("plan controlled expression")
            .collect()
            .await
            .expect("evaluate controlled expression");
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("key array")
                    .iter()
                    .map(|key| key.expect("non-null key").to_string())
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    #[tokio::test]
    async fn controlled_expr_evaluates_documented_filters() {
        let metadata = vec![
            json!({"genre":["documentary","romance"],"rating":4,"year":2020,"flag":true,"path":"/marketing/a"}),
            json!({"genre":"drama","rating":5,"year":2021,"flag":false,"path":"/sales/b"}),
            json!({"genre":"comedy","rating":3,"year":2019,"flag":true,"path":"/support/c"}),
        ];
        for (filter, expected) in [
            (json!({"genre":"documentary"}), vec!["0"]),
            (json!({"genre":{"$eq":"documentary"}}), vec!["0"]),
            (json!({"genre":{"$ne":"drama"}}), vec!["0", "2"]),
            (json!({"year":{"$gt":2019}}), vec!["0", "1"]),
            (json!({"year":{"$gte":2020}}), vec!["0", "1"]),
            (json!({"year":{"$lt":2020}}), vec!["2"]),
            (json!({"year":{"$lte":2020}}), vec!["0", "2"]),
            (json!({"genre":{"$in":["documentary","drama"]}}), vec!["0", "1"]),
            (json!({"genre":{"$nin":["documentary","drama"]}}), vec!["2"]),
            (json!({"rating":{"$exists":true}}), vec!["0", "1", "2"]),
            (json!({"missing":{"$exists":false}}), vec!["0", "1", "2"]),
            (json!({"path":{"$startsWith":"/marketing/"}}), vec!["0"]),
            (json!({"$and":[{"flag":true},{"year":{"$gte":2020}}]}), vec!["0"]),
            (json!({"$or":[{"year":2019},{"genre":"drama"}]}), vec!["1", "2"]),
            (json!({"rating":{"$gte":4,"$lt":5}}), vec!["0"]),
        ] {
            assert_eq!(selected_keys(filter.clone(), metadata.clone()).await, expected, "{filter}");
        }
    }

    #[tokio::test]
    async fn expression_characters_remain_literal_data() {
        let filter = json!({"a.b['x'] OR true --":{"$eq":"' OR true --"}});
        let metadata = vec![
            json!({"a.b['x'] OR true --":"' OR true --"}),
            json!({"a.b['x'] OR true --":"different"}),
            json!({"a":{"b":{"x":"' OR true --"}}}),
        ];
        assert_eq!(selected_keys(filter, metadata).await, ["0"]);
    }

    #[tokio::test]
    async fn primitive_types_and_integer_precision_are_preserved() {
        let metadata = vec![
            json!({"value":1}),
            json!({"value":"1"}),
            json!({"value":true}),
            json!({"value":1.0}),
            json!({"value":9007199254740992_u64}),
            json!({"value":9007199254740993_u64}),
        ];
        assert_eq!(selected_keys(json!({"value":1}), metadata.clone()).await, ["0", "3"]);
        assert_eq!(selected_keys(json!({"value":true}), metadata.clone()).await, ["2"]);
        assert_eq!(selected_keys(json!({"value":"1"}), metadata.clone()).await, ["1"]);
        assert_eq!(selected_keys(json!({"value":9007199254740993_u64}), metadata).await, ["5"]);
    }
}
