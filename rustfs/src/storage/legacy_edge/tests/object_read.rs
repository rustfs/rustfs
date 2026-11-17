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

//! Tests of the `object_read` legacy edge (batch C) and of the seam helper it is the first user of.

use super::build_request;
use crate::app::Answered;
use crate::storage::ecfs::*;
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use rustfs_gateway_types::compat::s3s_5761ddfe::generated::ops::head_bucket;

fn head_bucket_request() -> S3Request<HeadBucketInput> {
    let input = HeadBucketInput::builder().bucket("bucket".to_owned()).build().unwrap();
    build_request(input, Method::HEAD)
}

#[tokio::test]
async fn an_answered_status_and_headers_reach_the_legacy_response_unchanged() {
    let mut headers = HeaderMap::new();
    headers.append("x-amz-tagging-count", HeaderValue::from_static("2"));
    headers.append("x-amz-meta-repeated", HeaderValue::from_static("one"));
    headers.append("x-amz-meta-repeated", HeaderValue::from_static("two"));
    let answered = headers.clone();

    let response = through_gateway_seam_answered(
        head_bucket_request(),
        head_bucket::input_from_s3s,
        head_bucket::output_to_s3s,
        |_, _| async move {
            Ok(Answered {
                output: Default::default(),
                status: Some(StatusCode::PARTIAL_CONTENT),
                headers: answered,
            })
        },
    )
    .await
    .unwrap();

    assert_eq!(response.status, Some(StatusCode::PARTIAL_CONTENT));
    assert_eq!(response.headers, headers);
}

#[tokio::test]
async fn a_bare_output_answers_the_declared_status_and_no_header() {
    let response = through_gateway_seam(
        head_bucket_request(),
        head_bucket::input_from_s3s,
        head_bucket::output_to_s3s,
        |_, _| async { Ok(Default::default()) },
    )
    .await
    .unwrap();

    assert_eq!(response.status, None);
    assert!(response.headers.is_empty());
}
