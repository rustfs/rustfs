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

//! SelectObjectContent requests across the legacy S3 edge.
//!
//! Responsible for: converting the s3s request the edge decoded into the
//! request the query engine runs (rustfs/backlog#2746), through the gateway's
//! s3s seam for the serialization shapes. Not responsible for: validating the
//! request; the select use case does that before the engine is built.
//! DELETE BY T4.1 (rustfs/backlog#2783) with the rest of the legacy edge.

use rustfs_gateway_types::compat::ConversionError;
use rustfs_gateway_types::compat::s3s_5761ddfe::generated::shapes::input_serialization::input_serialization_from_s3s;
use rustfs_gateway_types::compat::s3s_5761ddfe::generated::shapes::output_serialization::output_serialization_from_s3s;
use rustfs_s3select_api::request as engine;

/// Converts the decoded request, keeping the SSE-C members as the client sent them.
pub(crate) fn select_input_from_s3s(
    input: s3s::dto::SelectObjectContentInput,
) -> Result<engine::SelectObjectContentInput, ConversionError> {
    let s3s::dto::SelectObjectContentInput {
        bucket,
        expected_bucket_owner,
        key,
        sse_customer_algorithm,
        sse_customer_key,
        sse_customer_key_md5,
        request,
    } = input;
    let s3s::dto::SelectObjectContentRequest {
        expression,
        expression_type,
        input_serialization,
        output_serialization,
        request_progress,
        scan_range,
    } = request;
    Ok(engine::SelectObjectContentInput {
        bucket,
        expected_bucket_owner,
        key,
        sse_customer_algorithm,
        sse_customer_key,
        sse_customer_key_md5,
        request: engine::SelectObjectContentRequest {
            expression,
            expression_type: engine::ExpressionType::custom(expression_type.as_str().to_owned()),
            input_serialization: input_serialization_from_s3s(input_serialization)?,
            output_serialization: output_serialization_from_s3s(output_serialization)?,
            request_progress: request_progress.map(|progress| engine::RequestProgress {
                enabled: progress.enabled,
            }),
            scan_range: scan_range.map(|range| engine::ScanRange {
                start: range.start,
                end: range.end,
            }),
        },
    })
}

#[cfg(test)]
mod tests {
    use super::select_input_from_s3s;
    use s3s::dto::{
        CSVInput, CSVOutput, CompressionType, ExpressionType, FileHeaderInfo, InputSerialization, JSONInput, JSONType,
        OutputSerialization, RequestProgress, ScanRange, SelectObjectContentInput, SelectObjectContentRequest,
    };

    fn input(input_serialization: InputSerialization) -> SelectObjectContentInput {
        SelectObjectContentInput {
            bucket: "bucket".to_owned(),
            expected_bucket_owner: Some("owner".to_owned()),
            key: "key".to_owned(),
            sse_customer_algorithm: Some("AES256".to_owned()),
            sse_customer_key: Some("key-text".to_owned()),
            sse_customer_key_md5: Some("client-md5".to_owned()),
            request: SelectObjectContentRequest {
                expression: "SELECT * FROM S3Object".to_owned(),
                expression_type: ExpressionType::from_static(ExpressionType::SQL),
                input_serialization,
                output_serialization: OutputSerialization {
                    csv: Some(CSVOutput {
                        field_delimiter: Some(";".to_owned()),
                        quote_fields: None,
                        quote_character: Some("'".to_owned()),
                        quote_escape_character: None,
                        record_delimiter: Some("\r\n".to_owned()),
                    }),
                    json: None,
                },
                request_progress: Some(RequestProgress { enabled: Some(true) }),
                scan_range: Some(ScanRange {
                    start: Some(3),
                    end: Some(9),
                }),
            },
        }
    }

    #[test]
    fn keeps_every_member_the_engine_reads() {
        let converted = select_input_from_s3s(input(InputSerialization {
            csv: Some(CSVInput {
                allow_quoted_record_delimiter: Some(true),
                comments: Some("#".to_owned()),
                field_delimiter: Some("|".to_owned()),
                file_header_info: Some(FileHeaderInfo::from_static(FileHeaderInfo::USE)),
                quote_character: Some("\"".to_owned()),
                quote_escape_character: Some("\\".to_owned()),
                record_delimiter: Some("\n".to_owned()),
            }),
            compression_type: Some(CompressionType::from_static(CompressionType::GZIP)),
            json: None,
            parquet: None,
        }))
        .expect("convert");

        assert_eq!((converted.bucket.as_str(), converted.key.as_str()), ("bucket", "key"));
        assert_eq!(converted.expected_bucket_owner.as_deref(), Some("owner"));
        assert_eq!(converted.sse_customer_algorithm.as_deref(), Some("AES256"));
        assert_eq!(converted.sse_customer_key.as_deref(), Some("key-text"));
        assert_eq!(converted.sse_customer_key_md5.as_deref(), Some("client-md5"));
        let request = &converted.request;
        assert_eq!(request.expression, "SELECT * FROM S3Object");
        assert_eq!(request.expression_type.as_str(), "SQL");
        let csv = request.input_serialization.csv.as_ref().expect("csv input");
        assert_eq!(csv.allow_quoted_record_delimiter, Some(true));
        assert_eq!(csv.comments.as_deref(), Some("#"));
        assert_eq!(csv.field_delimiter.as_deref(), Some("|"));
        assert_eq!(csv.file_header_info.as_ref().map(|value| value.as_str()), Some("USE"));
        assert_eq!(csv.quote_character.as_deref(), Some("\""));
        assert_eq!(csv.quote_escape_character.as_deref(), Some("\\"));
        assert_eq!(csv.record_delimiter.as_deref(), Some("\n"));
        assert_eq!(
            request
                .input_serialization
                .compression_type
                .as_ref()
                .map(|value| value.as_str()),
            Some("GZIP")
        );
        let out = request.output_serialization.csv.as_ref().expect("csv output");
        assert_eq!(out.field_delimiter.as_deref(), Some(";"));
        assert_eq!(out.quote_character.as_deref(), Some("'"));
        assert_eq!(out.record_delimiter.as_deref(), Some("\r\n"));
        assert_eq!(request.request_progress.as_ref().and_then(|progress| progress.enabled), Some(true));
        let range = request.scan_range.as_ref().expect("scan range");
        assert_eq!((range.start, range.end), (Some(3), Some(9)));
    }

    #[test]
    fn keeps_the_json_type_and_an_unknown_expression_type() {
        let mut source = input(InputSerialization {
            json: Some(JSONInput {
                type_: Some(JSONType::from_static(JSONType::LINES)),
            }),
            ..Default::default()
        });
        source.request.expression_type = ExpressionType::from("SQL2".to_owned());
        let converted = select_input_from_s3s(source).expect("convert");
        let json = converted.request.input_serialization.json.as_ref().expect("json input");
        assert_eq!(json.r#type.as_ref().map(|value| value.as_str()), Some("LINES"));
        assert_eq!(converted.request.expression_type.as_str(), "SQL2");
    }
}
