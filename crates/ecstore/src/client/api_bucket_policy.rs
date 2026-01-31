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
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_assignments)]
#![allow(unused_must_use)]
#![allow(clippy::all)]

use http::{HeaderMap, StatusCode};
use http_body_util::BodyExt;
use hyper::body::Body;
use hyper::body::Bytes;
use std::collections::HashMap;

use crate::client::{
    api_error_response::http_resp_to_error_response,
    transition_api::{ReaderImpl, RequestMetadata, TransitionClient},
};
use rustfs_utils::hash::EMPTY_STRING_SHA256_HASH;

impl TransitionClient {
    pub async fn set_bucket_policy(&self, bucket_name: &str, policy: &str) -> Result<(), std::io::Error> {
        if policy == "" {
            return self.remove_bucket_policy(bucket_name).await;
        }

        self.put_bucket_policy(bucket_name, policy).await
    }

    pub async fn put_bucket_policy(&self, bucket_name: &str, policy: &str) -> Result<(), std::io::Error> {
        let mut url_values = HashMap::new();
        url_values.insert("policy".to_string(), "".to_string());

        let mut req_metadata = RequestMetadata {
            bucket_name: bucket_name.to_string(),
            query_values: url_values,
            content_body: ReaderImpl::Body(Bytes::from(policy.as_bytes().to_vec())),
            content_length: policy.len() as i64,
            object_name: "".to_string(),
            custom_header: HeaderMap::new(),
            content_md5_base64: "".to_string(),
            content_sha256_hex: "".to_string(),
            stream_sha256: false,
            trailer: HeaderMap::new(),
            pre_sign_url: Default::default(),
            add_crc: Default::default(),
            extra_pre_sign_header: Default::default(),
            bucket_location: Default::default(),
            expires: Default::default(),
        };

        let resp = self.execute_method(http::Method::PUT, &mut req_metadata).await?;
        //defer closeResponse(resp)

        let resp_status = resp.status();
        let h = resp.headers().clone();

        //if resp != nil {
        if resp_status != StatusCode::NO_CONTENT && resp.status() != StatusCode::OK {
            return Err(std::io::Error::other(http_resp_to_error_response(
                resp_status,
                &h,
                vec![],
                bucket_name,
                "",
            )));
        }
        //}
        Ok(())
    }

    pub async fn remove_bucket_policy(&self, bucket_name: &str) -> Result<(), std::io::Error> {
        let mut url_values = HashMap::new();
        url_values.insert("policy".to_string(), "".to_string());

        let resp = self
            .execute_method(
                http::Method::DELETE,
                &mut RequestMetadata {
                    bucket_name: bucket_name.to_string(),
                    query_values: url_values,
                    content_sha256_hex: EMPTY_STRING_SHA256_HASH.to_string(),
                    object_name: "".to_string(),
                    custom_header: HeaderMap::new(),
                    content_body: ReaderImpl::Body(Bytes::new()),
                    content_length: 0,
                    content_md5_base64: "".to_string(),
                    stream_sha256: false,
                    trailer: HeaderMap::new(),
                    pre_sign_url: Default::default(),
                    add_crc: Default::default(),
                    extra_pre_sign_header: Default::default(),
                    bucket_location: Default::default(),
                    expires: Default::default(),
                },
            )
            .await?;
        //defer closeResponse(resp)

        let resp_status = resp.status();
        let h = resp.headers().clone();

        if resp_status != StatusCode::NO_CONTENT {
            return Err(std::io::Error::other(http_resp_to_error_response(
                resp_status,
                &h,
                vec![],
                bucket_name,
                "",
            )));
        }

        Ok(())
    }

    pub async fn get_bucket_policy(&self, bucket_name: &str) -> Result<String, std::io::Error> {
        let bucket_policy = self.get_bucket_policy_inner(bucket_name).await?;
        Ok(bucket_policy)
    }

    pub async fn get_bucket_policy_inner(&self, bucket_name: &str) -> Result<String, std::io::Error> {
        let mut url_values = HashMap::new();
        url_values.insert("policy".to_string(), "".to_string());

        let resp = self
            .execute_method(
                http::Method::GET,
                &mut RequestMetadata {
                    bucket_name: bucket_name.to_string(),
                    query_values: url_values,
                    content_sha256_hex: EMPTY_STRING_SHA256_HASH.to_string(),
                    object_name: "".to_string(),
                    custom_header: HeaderMap::new(),
                    content_body: ReaderImpl::Body(Bytes::new()),
                    content_length: 0,
                    content_md5_base64: "".to_string(),
                    stream_sha256: false,
                    trailer: HeaderMap::new(),
                    pre_sign_url: Default::default(),
                    add_crc: Default::default(),
                    extra_pre_sign_header: Default::default(),
                    bucket_location: Default::default(),
                    expires: Default::default(),
                },
            )
            .await?;

        let mut body_vec = Vec::new();
        let mut body = resp.into_body();
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
            if let Some(data) = frame.data_ref() {
                body_vec.extend_from_slice(data);
            }
        }
        let policy = String::from_utf8_lossy(&body_vec).to_string();
        Ok(policy)
    }
}
