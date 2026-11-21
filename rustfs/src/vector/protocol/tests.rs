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

use super::dto::*;
use super::{ErrorCode, Operation, ProtocolError, Request, Response, Route};
use http::{Method, StatusCode};
use serde_json::{Value, json};

const REGION: &str = "us-east-1";
const INDEX_ARN: &str = "arn:aws:s3vectors:us-east-1::bucket/test-bucket/index/index.one";

fn parse(action: &str, body: Value) -> Result<Request, ProtocolError> {
    let route = Route::recognize(&Method::POST, &format!("/{action}")).expect("modeled operation");
    route.parse(None, &serde_json::to_vec(&body).unwrap(), REGION)
}

fn assert_validation(action: &str, body: Value) {
    let error = parse(action, body).expect_err("invalid input must fail before service dispatch");
    assert_eq!(error.code(), ErrorCode::Validation);
    assert_eq!(error.status_code(), StatusCode::BAD_REQUEST);
}

#[test]
fn all_twenty_one_routes_use_model_http_bindings() {
    let post = [
        "CreateIndex",
        "CreateVectorBucket",
        "DeleteIndex",
        "DeleteVectorBucket",
        "DeleteVectorBucketPolicy",
        "DeleteVectors",
        "GetIndex",
        "GetVectorBucket",
        "GetVectorBucketPolicy",
        "GetVectors",
        "ListIndexes",
        "ListVectorBuckets",
        "ListVectors",
        "PutVectorBucketDefaultIndexMode",
        "PutVectorBucketPolicy",
        "PutVectors",
        "QueryVectors",
        "UpdateIndexMode",
    ];
    assert_eq!(Operation::ALL.len(), 21);
    for name in post {
        let path = format!("/{name}");
        assert_eq!(Route::recognize(&Method::POST, &path).unwrap().operation().name(), name);
        assert!(Route::recognize(&Method::GET, &path).is_none());
        assert!(Route::recognize(&Method::POST, &format!("{path}/")).is_none());
    }
    for (method, name) in [
        (Method::POST, "TagResource"),
        (Method::DELETE, "UntagResource"),
        (Method::GET, "ListTagsForResource"),
    ] {
        assert_eq!(
            Route::recognize(&method, "/tags/arn%3Aaws%3As3vectors%3Aus-east-1%3A%3Abucket%2Ftest-bucket")
                .unwrap()
                .operation()
                .name(),
            name
        );
    }
    assert!(Route::recognize(&Method::POST, "/UnknownOperation").is_none());
}

#[test]
fn tags_decode_one_label_and_preserve_repeated_query_values() {
    let path = "/tags/arn%3Aaws%3As3vectors%3Aus-east-1%3A%3Abucket%2Ftest-bucket%2Findex%2Findex.one";
    let query = "tagKeys=a%20b&tagKeys=a%2Fb&tagKeys=a%2Bb";
    let route = Route::recognize(&Method::DELETE, path).unwrap();
    let Request::UntagResource(input) = route.parse(Some(query), b"", REGION).unwrap() else {
        panic!("UntagResource DTO");
    };
    assert_eq!(input.resource_arn, INDEX_ARN);
    assert_eq!(input.tag_keys, ["a b", "a/b", "a+b"]);
    assert_eq!(
        path,
        "/tags/arn%3Aaws%3As3vectors%3Aus-east-1%3A%3Abucket%2Ftest-bucket%2Findex%2Findex.one"
    );
    assert_eq!(query, "tagKeys=a%20b&tagKeys=a%2Fb&tagKeys=a%2Bb");
}

#[test]
fn malformed_and_double_encoded_tags_are_rejected() {
    for suffix in [
        "%",
        "%GG",
        "%FF",
        "arn%253Aaws%253As3vectors%253Aus-east-1%253A%253Abucket%252Ftest-bucket",
    ] {
        let route = Route::recognize(&Method::GET, &format!("/tags/{suffix}")).unwrap();
        assert_eq!(route.parse(None, b"", REGION).unwrap_err().code(), ErrorCode::Validation);
    }
    let route = Route::recognize(
        &Method::DELETE,
        "/tags/arn%3Aaws%3As3vectors%3Aus-east-1%3A%3Abucket%2Ftest-bucket%2Findex%2Findex.one",
    )
    .unwrap();
    assert!(route.parse(Some("tagKeys=%GG"), b"", REGION).is_err());
    assert!(route.parse(None, b"", REGION).is_err());
}

#[test]
fn index_names_accept_periods_and_bucket_names_enforce_their_own_rules() {
    let valid = json!({"vectorBucketName":"test-bucket","indexName":"index.one","dataType":"float32","dimension":3,"distanceMetric":"cosine"});
    assert!(parse("CreateIndex", valid.clone()).is_ok());
    for name in ["ab", "-abc", "abc-", "ABC", "a_b"] {
        let mut body = valid.clone();
        body["indexName"] = json!(name);
        assert_validation("CreateIndex", body);
    }
    assert_validation("CreateVectorBucket", json!({"vectorBucketName":"bucket.one"}));
}

#[test]
fn selectors_require_exactly_one_complete_resource_address() {
    for body in [
        json!({}),
        json!({"indexName":"index.one"}),
        json!({"vectorBucketName":"test-bucket"}),
        json!({"indexArn":INDEX_ARN,"indexName":"index.one"}),
        json!({"indexArn":INDEX_ARN,"vectorBucketName":"test-bucket"}),
    ] {
        assert_validation("GetIndex", body);
    }
    assert!(parse("GetIndex", json!({"indexArn":INDEX_ARN})).is_ok());
    assert_validation(
        "GetVectorBucket",
        json!({"vectorBucketName":"test-bucket","vectorBucketArn":"arn:aws:s3vectors:us-east-1::bucket/test-bucket"}),
    );
}

#[test]
fn vector_arns_bind_region_service_empty_account_and_resource_kind() {
    for arn in [
        "arn:aws:s3vectors:us-east-1:123456789012:bucket/test-bucket/index/index.one",
        "arn:aws:s3vectors:us-west-2::bucket/test-bucket/index/index.one",
        "arn:aws:s3:us-east-1::bucket/test-bucket/index/index.one",
        "arn:aws:s3vectors:us-east-1::bucket/test-bucket",
        "arn:aws:s3vectors:us-east-1::bucket/test-bucket/index/index.one/extra",
    ] {
        assert_validation("GetIndex", json!({"indexArn":arn}));
    }
}

#[test]
fn required_fields_types_enums_and_union_are_checked() {
    let base = json!({"vectorBucketName":"test-bucket","indexName":"index.one","dataType":"float32","dimension":1,"distanceMetric":"cosine"});
    for field in ["indexName", "dataType", "dimension", "distanceMetric"] {
        let mut input = base.clone();
        input.as_object_mut().unwrap().remove(field);
        assert_validation("CreateIndex", input);
    }
    for value in [json!(0), json!(4097), json!("1"), json!(true), Value::Null] {
        let mut input = base.clone();
        input["dimension"] = value;
        assert_validation("CreateIndex", input);
    }
    for data in [
        json!({}),
        json!({"float32":[]}),
        json!({"float32":[1e39]}),
        json!({"float32":["NaN"]}),
        json!({"float32":[1],"float64":[1]}),
    ] {
        assert_validation("QueryVectors", json!({"indexArn":INDEX_ARN,"topK":1,"queryVector":data}));
    }
}

#[test]
fn enhanced_mode_and_query_limits_are_enforced() {
    for top_k in [1, 100, 101, 10000] {
        assert!(parse("QueryVectors", json!({"indexArn":INDEX_ARN,"topK":top_k,"queryVector":{"float32":[1]}})).is_ok());
    }
    for top_k in [0, 10001] {
        assert_validation("QueryVectors", json!({"indexArn":INDEX_ARN,"topK":top_k,"queryVector":{"float32":[1]}}));
    }
    assert_validation(
        "QueryVectors",
        json!({"indexArn":INDEX_ARN,"topK":1,"queryVector":{"float32":[1]},"queryMode":"CLASSIC"}),
    );
    assert_validation("UpdateIndexMode", json!({"indexArn":INDEX_ARN,"indexMode":"CLASSIC"}));
    assert!(parse("UpdateIndexMode", json!({"indexArn":INDEX_ARN,"indexMode":"ENHANCED"})).is_ok());
    assert_validation(
        "PutVectorBucketDefaultIndexMode",
        json!({"vectorBucketName":"test-bucket","defaultIndexMode":"CLASSIC"}),
    );
}

#[test]
fn list_segments_are_paired_and_bounded() {
    for (count, index) in [(1, 0), (16, 15)] {
        assert!(parse("ListVectors", json!({"indexArn":INDEX_ARN,"segmentCount":count,"segmentIndex":index})).is_ok());
    }
    for fields in [
        json!({"segmentCount":2}),
        json!({"segmentIndex":0}),
        json!({"segmentCount":2,"segmentIndex":2}),
        json!({"segmentCount":17,"segmentIndex":0}),
        json!({"segmentCount":1,"segmentIndex":-1}),
    ] {
        let mut body = fields;
        body["indexArn"] = json!(INDEX_ARN);
        assert_validation("ListVectors", body);
    }
}

#[test]
fn key_batch_limits_are_operation_specific() {
    for (name, max) in [("GetVectors", 100), ("DeleteVectors", 500)] {
        for count in [1, max] {
            assert!(parse(name, json!({"indexArn":INDEX_ARN,"keys":vec!["key";count]})).is_ok());
        }
        for count in [0, max + 1] {
            assert_validation(name, json!({"indexArn":INDEX_ARN,"keys":vec!["key";count]}));
        }
    }
    for count in [0, 501] {
        assert_validation(
            "PutVectors",
            json!({"indexArn":INDEX_ARN,"vectors":vec![json!({"key":"key","data":{"float32":[1]}});count]}),
        );
    }
}

#[test]
fn optional_encryption_preserves_omitted_empty_and_explicit_values() {
    for encryption in [
        None,
        Some(json!({})),
        Some(json!({"sseType":"AES256"})),
        Some(json!({"sseType":"aws:kms","kmsKeyArn":"arn:aws:kms:us-east-1:123456789012:key/example"})),
    ] {
        let mut body = json!({"vectorBucketName":"test-bucket"});
        if let Some(value) = encryption.clone() {
            body["encryptionConfiguration"] = value;
        }
        let Request::CreateVectorBucket(input) = parse("CreateVectorBucket", body).unwrap() else {
            panic!("CreateVectorBucket DTO");
        };
        assert_eq!(input.encryption_configuration.is_some(), encryption.is_some());
    }
    for value in [
        json!({"sseType":"none"}),
        json!({"sseType":"AES256","kmsKeyArn":"arn:aws:kms:us-east-1:123456789012:key/example"}),
        json!({"sseType":"aws:kms","kmsKeyArn":"arn:aws:kms:us-east-1::key/example"}),
    ] {
        assert_validation(
            "CreateVectorBucket",
            json!({"vectorBucketName":"test-bucket","encryptionConfiguration":value}),
        );
    }
}

#[test]
fn unencrypted_response_omits_encryption_configuration() {
    let output = GetVectorBucketOutput {
        vector_bucket: VectorBucket {
            vector_bucket_name: "test-bucket".into(),
            vector_bucket_arn: "arn:aws:s3vectors:us-east-1::bucket/test-bucket".into(),
            creation_time: 1.25,
            encryption_configuration: None,
            default_index_mode: Some(IndexMode::Enhanced),
        },
    };
    let bytes = Response::GetVectorBucket(output).body_bytes().unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["vectorBucket"]["creationTime"], json!(1.25));
    assert!(body["vectorBucket"].get("encryptionConfiguration").is_none());
}

#[test]
fn errors_keep_aws_json_shape_and_quota_status() {
    let error = ProtocolError::new(ErrorCode::ServiceQuotaExceeded, "Resource quota exceeded");
    assert_eq!(error.status_code(), StatusCode::PAYMENT_REQUIRED);
    let body: Value = serde_json::from_slice(&error.body_bytes()).unwrap();
    assert_eq!(
        body,
        json!({"__type":"ServiceQuotaExceededException","message":"Resource quota exceeded"})
    );
    let validation = ProtocolError::validation("dimension", "Must be between 1 and 4096");
    let body: Value = serde_json::from_slice(&validation.body_bytes()).unwrap();
    assert_eq!(body["fieldList"][0]["path"], "dimension");
    let response = error.into_response();
    assert_eq!(response.status(), StatusCode::PAYMENT_REQUIRED);
    assert_eq!(response.headers()["x-amzn-errortype"], "ServiceQuotaExceededException");
    assert_eq!(response.headers()["content-type"], "application/json");
}

#[test]
fn empty_operation_output_is_empty_body() {
    assert!(
        Response::UpdateIndexMode(UpdateIndexModeOutput {})
            .body_bytes()
            .unwrap()
            .is_empty()
    );
    assert!(
        Response::DeleteVectors(DeleteVectorsOutput {})
            .body_bytes()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn every_operation_parses_a_valid_input_and_serializes_its_output() {
    macro_rules! operation {
        ($name:ident, $input:expr, $output:expr) => {{
            let input = parse(stringify!($name), $input).expect("valid modeled input");
            assert_eq!(input.operation(), Operation::$name);
            let expected = $output;
            let output = Response::$name(serde_json::from_value(expected.clone()).unwrap());
            let response = output.into_response().expect("modeled response");
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.headers()["content-type"], "application/json");
            if expected == json!({}) {
                assert!(response.body().is_empty());
            } else {
                assert_eq!(serde_json::from_slice::<Value>(response.body()).unwrap(), expected);
            }
        }};
    }
    let bucket_arn = "arn:aws:s3vectors:us-east-1::bucket/test-bucket";
    let bucket = json!({"vectorBucketName":"test-bucket","vectorBucketArn":bucket_arn,"creationTime":1.0});
    let index = json!({"vectorBucketName":"test-bucket","indexName":"index.one","indexArn":INDEX_ARN,"creationTime":1.0,"dataType":"float32","dimension":1,"distanceMetric":"euclidean"});
    operation!(
        CreateVectorBucket,
        json!({"vectorBucketName":"test-bucket"}),
        json!({"vectorBucketArn":bucket_arn})
    );
    operation!(GetVectorBucket, json!({"vectorBucketArn":bucket_arn}), json!({"vectorBucket":bucket}));
    operation!(ListVectorBuckets, json!({}), json!({"vectorBuckets":[]}));
    operation!(DeleteVectorBucket, json!({"vectorBucketName":"test-bucket"}), json!({}));
    operation!(PutVectorBucketPolicy, json!({"vectorBucketArn":bucket_arn,"policy":"{}"}), json!({}));
    operation!(GetVectorBucketPolicy, json!({"vectorBucketName":"test-bucket"}), json!({"policy":"{}"}));
    operation!(DeleteVectorBucketPolicy, json!({"vectorBucketArn":bucket_arn}), json!({}));
    operation!(
        CreateIndex,
        json!({"vectorBucketArn":bucket_arn,"indexName":"index.one","dataType":"float32","dimension":1,"distanceMetric":"euclidean"}),
        json!({"indexArn":INDEX_ARN})
    );
    operation!(
        GetIndex,
        json!({"vectorBucketName":"test-bucket","indexName":"index.one"}),
        json!({"index":index})
    );
    operation!(ListIndexes, json!({"vectorBucketName":"test-bucket"}), json!({"indexes":[]}));
    operation!(DeleteIndex, json!({"indexArn":INDEX_ARN}), json!({}));
    operation!(
        PutVectors,
        json!({"indexArn":INDEX_ARN,"vectors":[{"key":"key","data":{"float32":[1]}}]}),
        json!({})
    );
    operation!(
        GetVectors,
        json!({"indexArn":INDEX_ARN,"keys":["key"]}),
        json!({"vectors":[{"key":"key"}]})
    );
    operation!(DeleteVectors, json!({"indexArn":INDEX_ARN,"keys":["key"]}), json!({}));
    operation!(ListVectors, json!({"indexArn":INDEX_ARN}), json!({"vectors":[]}));
    operation!(
        QueryVectors,
        json!({"indexArn":INDEX_ARN,"topK":1,"queryVector":{"float32":[1]}}),
        json!({"vectors":[],"distanceMetric":"cosine"})
    );
    operation!(
        PutVectorBucketDefaultIndexMode,
        json!({"vectorBucketArn":bucket_arn,"defaultIndexMode":"ENHANCED"}),
        json!({})
    );
    operation!(UpdateIndexMode, json!({"indexArn":INDEX_ARN,"indexMode":"ENHANCED"}), json!({}));
    let path = "/tags/arn%3Aaws%3As3vectors%3Aus-east-1%3A%3Abucket%2Ftest-bucket";
    let tagged = Route::recognize(&Method::POST, path)
        .unwrap()
        .parse(None, br#"{"tags":{"owner":"team"}}"#, REGION)
        .unwrap();
    assert_eq!(tagged.operation(), Operation::TagResource);
    let listed = Route::recognize(&Method::GET, path)
        .unwrap()
        .parse(None, b"", REGION)
        .unwrap();
    assert_eq!(listed.operation(), Operation::ListTagsForResource);
    let untagged = Route::recognize(&Method::DELETE, path)
        .unwrap()
        .parse(Some("tagKeys=owner"), b"", REGION)
        .unwrap();
    assert_eq!(untagged.operation(), Operation::UntagResource);
    assert!(Response::TagResource(TagResourceOutput {}).body_bytes().unwrap().is_empty());
    assert!(
        Response::UntagResource(UntagResourceOutput {})
            .body_bytes()
            .unwrap()
            .is_empty()
    );
    let list = Response::ListTagsForResource(ListTagsForResourceOutput {
        tags: [("owner".into(), "team".into())].into_iter().collect(),
    });
    assert_eq!(
        serde_json::from_slice::<Value>(&list.body_bytes().unwrap()).unwrap(),
        json!({"tags":{"owner":"team"}})
    );
}

#[test]
fn smithy_unknown_members_and_optional_null_preserve_required_validation() {
    let Request::QueryVectors(query) = parse(
        "QueryVectors",
        json!({
            "indexArn":INDEX_ARN,"topK":1,"queryVector":{"float32":[1],"__type":"VectorData","future":null},
            "filter":null,"queryMode":null,"returnMetadata":null,"future":{"any":[null,true]}
        }),
    )
    .unwrap() else {
        panic!("QueryVectors DTO");
    };
    assert_eq!(query.filter, Some(Value::Null));
    assert!(query.query_mode.is_none());
    assert!(query.return_metadata.is_none());
    assert_validation("QueryVectors", json!({"indexArn":INDEX_ARN,"topK":null,"queryVector":{"float32":[1]}}));
    assert_validation("QueryVectors", json!({"indexArn":INDEX_ARN,"topK":1,"queryVector":{"float32":null}}));
    assert_validation(
        "CreateVectorBucket",
        json!({"vectorBucketName":"test-bucket","encryptionConfiguration":null}),
    );
}

#[test]
fn aws_kms_requires_full_key_arn_and_plaintext_is_never_inferred_from_null() {
    for encryption in [
        json!({"sseType":"aws:kms"}),
        json!({"sseType":"aws:kms","kmsKeyArn":null}),
        json!({"kmsKeyArn":"arn:aws:kms:us-east-1:123456789012:key/example"}),
    ] {
        assert_validation(
            "CreateVectorBucket",
            json!({"vectorBucketName":"test-bucket","encryptionConfiguration":encryption}),
        );
    }
    let Request::CreateVectorBucket(input) = parse(
        "CreateVectorBucket",
        json!({"vectorBucketName":"test-bucket","encryptionConfiguration":{"future":true}}),
    )
    .unwrap() else {
        panic!("CreateVectorBucket DTO");
    };
    let encryption = input
        .encryption_configuration
        .expect("explicit configuration remains present");
    assert_eq!(encryption.sse_type.unwrap_or(SseType::Aes256), SseType::Aes256);
}

#[test]
fn body_limit_counts_actual_json_bytes_including_utf8_and_escapes() {
    let route = Route::recognize(&Method::POST, "/CreateVectorBucket").unwrap();
    for base in [
        r#"{"vectorBucketName":"test-bucket","tags":{"team":"vector"}}"#,
        r#"{"vectorBucketName":"test-bucket","tags":{"业务":"值"}}"#,
        r#"{"vectorBucketName":"test-bucket","tags":{"\u4e1a\u52a1":"\u503c"}}"#,
    ] {
        for length in [
            super::MAX_REQUEST_BODY_BYTES - 1,
            super::MAX_REQUEST_BODY_BYTES,
            super::MAX_REQUEST_BODY_BYTES + 1,
        ] {
            let mut body = base.as_bytes().to_vec();
            body.resize(length, b' ');
            let result = route.parse(None, &body, REGION);
            if length <= super::MAX_REQUEST_BODY_BYTES {
                assert!(result.is_ok(), "exact bytes {length}");
            } else {
                let error = result.unwrap_err();
                assert_eq!(error.code(), ErrorCode::Validation);
                assert_eq!(error.status_code(), StatusCode::BAD_REQUEST);
            }
        }
    }
}

#[test]
fn tags_validate_unicode_patterns_lengths_and_counts() {
    assert!(
        parse(
            "CreateVectorBucket",
            json!({"vectorBucketName":"test-bucket","tags":{"业务 空间._:/=+-@":"","数字１２３":"值"}})
        )
        .is_ok()
    );
    for tags in [
        json!({"":"value"}),
        json!({"key\n":"value"}),
        json!({"key":"emoji😀"}),
        json!({"x".repeat(129):"value"}),
        json!({"key":"值".repeat(257)}),
    ] {
        assert_validation("CreateVectorBucket", json!({"vectorBucketName":"test-bucket","tags":tags}));
    }
    for count in [50, 51] {
        let tags: serde_json::Map<String, Value> = (0..count).map(|index| (format!("key{index}"), json!("value"))).collect();
        let result = parse("CreateVectorBucket", json!({"vectorBucketName":"test-bucket","tags":tags}));
        assert_eq!(result.is_ok(), count == 50);
    }
    assert!(
        parse(
            "CreateVectorBucket",
            json!({"vectorBucketName":"test-bucket","tags":{"界".repeat(128):"值".repeat(256)}})
        )
        .is_ok()
    );
}

#[test]
fn pagination_fields_enforce_operation_specific_limits() {
    for (operation, max, token_max) in [
        ("ListVectorBuckets", 500, 512),
        ("ListIndexes", 500, 512),
        ("ListVectors", 1000, 2048),
    ] {
        let mut input = match operation {
            "ListIndexes" => json!({"vectorBucketName":"test-bucket"}),
            "ListVectors" => json!({"indexArn":INDEX_ARN}),
            _ => json!({}),
        };
        for value in [1, max] {
            input["maxResults"] = json!(value);
            assert!(parse(operation, input.clone()).is_ok());
        }
        for value in [0, max + 1] {
            input["maxResults"] = json!(value);
            assert_validation(operation, input.clone());
        }
        input["maxResults"] = json!(1);
        input["nextToken"] = json!("x".repeat(token_max));
        assert!(parse(operation, input.clone()).is_ok());
        for token in [String::new(), "x".repeat(token_max + 1)] {
            input["nextToken"] = json!(token);
            assert_validation(operation, input.clone());
        }
    }
    for token in [String::new(), "x".repeat(4097)] {
        assert_validation(
            "QueryVectors",
            json!({"indexArn":INDEX_ARN,"topK":1,"queryVector":{"float32":[1]},"nextToken":token}),
        );
    }
}

#[test]
fn metadata_configuration_is_bounded_and_unique() {
    let mut input = json!({"vectorBucketName":"test-bucket","indexName":"index.one","dataType":"float32","dimension":4096,"distanceMetric":"euclidean","metadataConfiguration":{"nonFilterableMetadataKeys":["private"]}});
    assert!(parse("CreateIndex", input.clone()).is_ok());
    for keys in [vec![], vec!["same"; 2], vec!["key"; 11], vec![""]] {
        input["metadataConfiguration"]["nonFilterableMetadataKeys"] = json!(keys);
        assert_validation("CreateIndex", input.clone());
    }
}

#[test]
fn json_duplicates_trailing_content_and_wrong_types_are_rejected() {
    let route = Route::recognize(&Method::POST, "/CreateVectorBucket").unwrap();
    for body in [
        br#"{"vectorBucketName":"test-bucket","vectorBucketName":"other-bucket"}"#.as_slice(),
        br#"{"vectorBucketName":"test-bucket"} {}"#,
        br#"{"vectorBucketName":true}"#,
        br#"[]"#,
        b"null",
    ] {
        assert_eq!(route.parse(None, body, REGION).unwrap_err().code(), ErrorCode::Validation);
    }
    let route = Route::recognize(&Method::POST, "/QueryVectors").unwrap();
    let duplicate=br#"{"indexArn":"arn:aws:s3vectors:us-east-1::bucket/test-bucket/index/index.one","topK":1,"queryVector":{"float32":[1],"float32":[2]}}"#;
    assert_eq!(route.parse(None, duplicate, REGION).unwrap_err().code(), ErrorCode::Validation);
}

#[test]
fn all_error_codes_match_the_fixed_model() {
    for (code, name, status) in [
        (ErrorCode::AccessDenied, "AccessDeniedException", 403),
        (ErrorCode::Conflict, "ConflictException", 409),
        (ErrorCode::InternalServer, "InternalServerException", 500),
        (ErrorCode::KmsDisabled, "KmsDisabledException", 400),
        (ErrorCode::KmsInvalidKeyUsage, "KmsInvalidKeyUsageException", 400),
        (ErrorCode::KmsInvalidState, "KmsInvalidStateException", 400),
        (ErrorCode::KmsNotFound, "KmsNotFoundException", 400),
        (ErrorCode::NotFound, "NotFoundException", 404),
        (ErrorCode::RequestTimeout, "RequestTimeoutException", 408),
        (ErrorCode::ServiceQuotaExceeded, "ServiceQuotaExceededException", 402),
        (ErrorCode::ServiceUnavailable, "ServiceUnavailableException", 503),
        (ErrorCode::TooManyRequests, "TooManyRequestsException", 429),
        (ErrorCode::Validation, "ValidationException", 400),
    ] {
        let response = ProtocolError::new(code, "message").into_response();
        assert_eq!(response.status().as_u16(), status);
        assert_eq!(response.headers()["x-amzn-errortype"], name);
        assert_eq!(
            serde_json::from_slice::<Value>(response.body()).unwrap(),
            json!({"__type":name,"message":"message"})
        );
    }
}

#[test]
fn response_never_serializes_nonfinite_values_as_null() {
    for (distance, expected) in [
        (f32::INFINITY, "Infinity"),
        (f32::NEG_INFINITY, "-Infinity"),
        (f32::NAN, "NaN"),
    ] {
        let response = Response::QueryVectors(QueryVectorsOutput {
            vectors: vec![QueryOutputVector {
                key: "key".into(),
                distance: Some(distance),
                metadata: None,
            }],
            distance_metric: DistanceMetric::Euclidean,
            next_token: None,
        })
        .into_response()
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["content-type"], "application/json");
        assert_eq!(
            serde_json::from_slice::<Value>(response.body()).unwrap(),
            json!({"vectors":[{"key":"key","distance":expected}],"distanceMetric":"euclidean"})
        );
    }
    let response = Response::GetVectors(GetVectorsOutput {
        vectors: vec![GetOutputVector {
            key: "key".into(),
            data: Some(VectorData { float32: vec![f32::NAN] }),
            metadata: None,
        }],
    });
    assert_eq!(response.body_bytes().unwrap_err().code(), ErrorCode::InternalServer);
}

#[test]
fn tags_route_and_body_bindings_cannot_override_the_signed_resource() {
    assert!(Route::recognize(&Method::POST, "/tags/bucket/object").is_none());
    assert!(Route::recognize(&Method::POST, "/tags/").is_none());
    assert!(Route::recognize(&Method::POST, "//CreateIndex").is_none());
    assert!(Route::recognize(&Method::POST, "/%43reateIndex").is_none());
    let path = "/tags/arn%3Aaws%3As3vectors%3Aus-east-1%3A%3Abucket%2Ftest-bucket";
    let Request::TagResource(input) = Route::recognize(&Method::POST, path)
        .unwrap()
        .parse(None, br#"{"resourceArn":"untrusted body","tags":{}}"#, REGION)
        .unwrap()
    else {
        panic!("TagResource DTO");
    };
    assert_eq!(input.resource_arn, "arn:aws:s3vectors:us-east-1::bucket/test-bucket");
    for method in [Method::GET, Method::DELETE] {
        let error = Route::recognize(&method, path)
            .unwrap()
            .parse(Some("tagKeys=key"), b"{}", REGION)
            .unwrap_err();
        assert_eq!(error.code(), ErrorCode::Validation);
    }
}

#[test]
fn documents_distinguish_omitted_fields_from_explicit_null() {
    let Request::QueryVectors(query) =
        parse("QueryVectors", json!({"indexArn":INDEX_ARN,"topK":1,"queryVector":{"float32":[1]}})).unwrap()
    else {
        panic!("QueryVectors DTO");
    };
    assert!(query.filter.is_none());
    let Request::PutVectors(put) = parse("PutVectors", json!({"indexArn":INDEX_ARN,"vectors":[{"key":"null","data":{"float32":[1]},"metadata":null},{"key":"absent","data":{"float32":[1]}}]})).unwrap() else {panic!("PutVectors DTO");};
    assert_eq!(put.vectors[0].metadata, Some(Value::Null));
    assert!(put.vectors[1].metadata.is_none());
}

#[test]
fn query_plus_and_percent_plus_match_the_signature_query_interpretation() {
    let route = Route::recognize(&Method::DELETE, "/tags/arn%3Aaws%3As3vectors%3Aus-east-1%3A%3Abucket%2Ftest-bucket").unwrap();
    let Request::UntagResource(input) = route
        .parse(Some("tagKeys=a+b&tagKeys=a%2Bb&tag%4Beys=encoded"), b"", REGION)
        .unwrap()
    else {
        panic!("UntagResource DTO");
    };
    assert_eq!(input.tag_keys, ["a b", "a+b", "encoded"]);
}

#[test]
fn structure_arrays_are_rejected_at_root_and_nested_paths() {
    for (action, body) in [
        ("ListVectorBuckets", json!([])),
        ("CreateVectorBucket", json!(["test-bucket"])),
        (
            "CreateVectorBucket",
            json!({"vectorBucketName":"test-bucket","encryptionConfiguration":[]}),
        ),
        (
            "CreateIndex",
            json!({"vectorBucketName":"test-bucket","indexName":"index.one","dataType":"float32","dimension":1,"distanceMetric":"cosine","metadataConfiguration":[["private"]]}),
        ),
        ("PutVectors", json!({"indexArn":INDEX_ARN,"vectors":[["key",{"float32":[1]}]]})),
        ("PutVectors", json!({"indexArn":INDEX_ARN,"vectors":[{"key":"key","data":[[1]]}]})),
    ] {
        assert_validation(action, body);
    }
    let tags = Route::recognize(&Method::POST, "/tags/arn%3Aaws%3As3vectors%3Aus-east-1%3A%3Abucket%2Ftest-bucket").unwrap();
    assert_eq!(
        tags.parse(None, br#"[{"team":"vectors"}]"#, REGION).unwrap_err().code(),
        ErrorCode::Validation
    );
}

#[test]
fn string_enums_require_json_strings() {
    let base = json!({"vectorBucketName":"test-bucket","indexName":"index.one","dataType":"float32","dimension":1,"distanceMetric":"cosine"});
    for (field, variant) in [("dataType", "float32"), ("distanceMetric", "cosine")] {
        for value in [json!({variant:null}), json!([variant]), json!(true), json!(1)] {
            let mut body = base.clone();
            body[field] = value;
            assert_validation("CreateIndex", body);
        }
    }
    assert_validation("UpdateIndexMode", json!({"indexArn":INDEX_ARN,"indexMode":{"ENHANCED":null}}));
    assert_validation(
        "PutVectorBucketDefaultIndexMode",
        json!({"vectorBucketName":"test-bucket","defaultIndexMode":{"ENHANCED":null}}),
    );
    assert_validation(
        "QueryVectors",
        json!({"indexArn":INDEX_ARN,"topK":1,"queryVector":{"float32":[1]},"queryMode":{"ENHANCED":null}}),
    );
    for value in [json!({"AES256":null}), json!(["AES256"]), json!(false), json!(0)] {
        assert_validation(
            "CreateVectorBucket",
            json!({"vectorBucketName":"test-bucket","encryptionConfiguration":{"sseType":value}}),
        );
    }
}

#[test]
fn empty_request_recognizes_only_json_whitespace() {
    let route = Route::recognize(&Method::POST, "/ListVectorBuckets").unwrap();
    for body in [b"".as_slice(), b" ", b"\t", b"\n", b"\r", b" \t\n\r "] {
        assert!(route.parse(None, body, REGION).is_ok());
    }
    for byte in 0..=0x20u8 {
        if matches!(byte, b' ' | b'\t' | b'\n' | b'\r') {
            continue;
        }
        assert_eq!(
            route.parse(None, &[byte], REGION).unwrap_err().code(),
            ErrorCode::Validation,
            "control byte {byte}"
        );
    }
    for body in [b" \x0c\r".as_slice(), b"\x0b{}", b"{}\x0c"] {
        assert_eq!(route.parse(None, body, REGION).unwrap_err().code(), ErrorCode::Validation);
    }
}

#[test]
fn structure_restrictions_preserve_document_arrays_and_unknown_fields() {
    let Request::PutVectors(input) = parse(
        "PutVectors",
        json!({
            "indexArn":INDEX_ARN,"future":[null,{}],"vectors":[
                {"key":"array","data":{"float32":[1]},"metadata":[1,"value",null],"future":[true]},
                {"key":"null","data":{"float32":[1]},"metadata":null}
            ]
        }),
    )
    .unwrap() else {
        panic!("PutVectors DTO");
    };
    assert_eq!(input.vectors[0].metadata, Some(json!([1, "value", null])));
    assert_eq!(input.vectors[1].metadata, Some(Value::Null));
    let Request::QueryVectors(input) = parse(
        "QueryVectors",
        json!({"indexArn":INDEX_ARN,"topK":1,"queryVector":{"float32":[1]},"filter":[null]}),
    )
    .unwrap() else {
        panic!("QueryVectors DTO");
    };
    assert_eq!(input.filter, Some(json!([null])));
}

#[test]
fn every_modeled_structure_rejects_sequence_deserialization() {
    macro_rules! object_only {
        ($($ty:ty),+ $(,)?) => {$(
            assert!(serde_json::from_str::<$ty>("[]").is_err(), stringify!($ty));
            assert!(serde_json::from_str::<$ty>("null").is_err(), stringify!($ty));
        )+};
    }
    object_only!(
        CreateIndexInput,
        CreateIndexOutput,
        CreateVectorBucketInput,
        CreateVectorBucketOutput,
        DeleteIndexInput,
        DeleteIndexOutput,
        DeleteVectorBucketInput,
        DeleteVectorBucketOutput,
        DeleteVectorBucketPolicyInput,
        DeleteVectorBucketPolicyOutput,
        DeleteVectorsInput,
        DeleteVectorsOutput,
        EncryptionConfiguration,
        GetIndexInput,
        GetIndexOutput,
        GetOutputVector,
        GetVectorBucketInput,
        GetVectorBucketOutput,
        GetVectorBucketPolicyInput,
        GetVectorBucketPolicyOutput,
        GetVectorsInput,
        GetVectorsOutput,
        Index,
        IndexSummary,
        ListIndexesInput,
        ListIndexesOutput,
        ListOutputVector,
        ListTagsForResourceInput,
        ListTagsForResourceOutput,
        ListVectorBucketsInput,
        ListVectorBucketsOutput,
        ListVectorsInput,
        ListVectorsOutput,
        MetadataConfiguration,
        PutInputVector,
        PutVectorBucketDefaultIndexModeInput,
        PutVectorBucketDefaultIndexModeOutput,
        PutVectorBucketPolicyInput,
        PutVectorBucketPolicyOutput,
        PutVectorsInput,
        PutVectorsOutput,
        QueryOutputVector,
        QueryVectorsInput,
        QueryVectorsOutput,
        TagResourceInput,
        TagResourceOutput,
        UntagResourceInput,
        UntagResourceOutput,
        UpdateIndexModeInput,
        UpdateIndexModeOutput,
        ValidationExceptionField,
        VectorBucket,
        VectorBucketSummary,
        VectorData,
    );
}

#[test]
fn query_response_preserves_finite_and_absent_distances() {
    for (distance, expected) in [
        (Some(1.25), json!({"key":"key","distance":1.25})),
        (Some(0.0), json!({"key":"key","distance":0.0})),
        (None, json!({"key":"key"})),
    ] {
        let response = Response::QueryVectors(QueryVectorsOutput {
            vectors: vec![QueryOutputVector {
                key: "key".into(),
                distance,
                metadata: None,
            }],
            distance_metric: DistanceMetric::Euclidean,
            next_token: None,
        })
        .into_response()
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["content-type"], "application/json");
        assert_eq!(
            serde_json::from_slice::<Value>(response.body()).unwrap(),
            json!({"vectors":[expected],"distanceMetric":"euclidean"})
        );
    }
}
