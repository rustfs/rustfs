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

use s3s::dto::SelectObjectContentInput;
use std::sync::Arc;

use crate::{SelectInputMetrics, SelectObjectSnapshot};

pub mod analyzer;
pub mod ast;
pub mod dispatcher;
pub mod execution;
pub mod function;
pub mod logical_planner;
pub mod optimizer;
pub mod parser;
pub mod physical_planner;
pub mod scheduler;
pub mod session;

#[derive(Clone)]
pub struct Context {
    // maybe we need transfer some info?
    pub input: Arc<SelectObjectContentInput>,
}

#[derive(Clone)]
pub struct Query {
    context: Context,
    content: String,
    snapshot: Option<Arc<SelectObjectSnapshot>>,
    input_metrics: Arc<SelectInputMetrics>,
}

impl Query {
    #[inline(always)]
    pub fn new(context: Context, content: String) -> Self {
        Self {
            context,
            content,
            snapshot: None,
            input_metrics: Arc::new(SelectInputMetrics::default()),
        }
    }

    #[inline(always)]
    pub fn new_with_snapshot(context: Context, content: String, snapshot: Arc<SelectObjectSnapshot>) -> Self {
        Self {
            context,
            content,
            snapshot: Some(snapshot),
            input_metrics: Arc::new(SelectInputMetrics::default()),
        }
    }

    pub fn context(&self) -> &Context {
        &self.context
    }

    pub fn content(&self) -> &str {
        self.content.as_str()
    }

    pub fn snapshot(&self) -> Option<&Arc<SelectObjectSnapshot>> {
        self.snapshot.as_ref()
    }

    pub fn input_metrics(&self) -> &Arc<SelectInputMetrics> {
        &self.input_metrics
    }

    pub fn for_execution(&self) -> Self {
        Self {
            context: self.context.clone(),
            content: self.content.clone(),
            snapshot: self.snapshot.clone(),
            input_metrics: Arc::new(SelectInputMetrics::default()),
        }
    }
}

#[cfg(test)]
fn test_query() -> Query {
    use s3s::dto::{CSVInput, CSVOutput, ExpressionType, InputSerialization, OutputSerialization, SelectObjectContentRequest};

    let input = SelectObjectContentInput {
        bucket: "bucket".to_string(),
        expected_bucket_owner: None,
        key: "input.csv".to_string(),
        sse_customer_algorithm: None,
        sse_customer_key: None,
        sse_customer_key_md5: None,
        request: SelectObjectContentRequest {
            expression: "SELECT * FROM S3Object".to_string(),
            expression_type: ExpressionType::from_static(ExpressionType::SQL),
            input_serialization: InputSerialization {
                csv: Some(CSVInput::default()),
                ..Default::default()
            },
            output_serialization: OutputSerialization {
                csv: Some(CSVOutput::default()),
                ..Default::default()
            },
            request_progress: None,
            scan_range: None,
        },
    };
    Query::new(Context { input: Arc::new(input) }, "SELECT * FROM S3Object".to_string())
}
