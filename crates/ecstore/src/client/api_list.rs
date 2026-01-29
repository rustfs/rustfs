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

use crate::client::{
    api_error_response::http_resp_to_error_response,
    api_s3_datatypes::{
        ListBucketResult, ListBucketV2Result, ListMultipartUploadsResult, ListObjectPartsResult, ListVersionsResult, ObjectPart,
    },
    credentials,
    transition_api::{ReaderImpl, RequestMetadata, TransitionClient},
};
use crate::store_api::BucketInfo;
use http::{HeaderMap, StatusCode};
use http_body_util::BodyExt;
use hyper::body::Body;
use hyper::body::Bytes;
use rustfs_config::MAX_S3_CLIENT_RESPONSE_SIZE;
use rustfs_utils::hash::EMPTY_STRING_SHA256_HASH;
use std::collections::HashMap;

impl TransitionClient {
    pub fn list_buckets(&self) -> Result<Vec<BucketInfo>, std::io::Error> {
        todo!();
    }

    pub async fn list_objects_v2_query(
        &self,
        bucket_name: &str,
        object_prefix: &str,
        continuation_token: &str,
        fetch_owner: bool,
        metadata: bool,
        delimiter: &str,
        start_after: &str,
        max_keys: i64,
        headers: HeaderMap,
    ) -> Result<ListBucketV2Result, std::io::Error> {
        let mut url_values = HashMap::new();

        url_values.insert("list-type".to_string(), "2".to_string());
        if metadata {
            url_values.insert("metadata".to_string(), "true".to_string());
        }
        if start_after != "" {
            url_values.insert("start-after".to_string(), start_after.to_string());
        }
        url_values.insert("encoding-type".to_string(), "url".to_string());
        url_values.insert("prefix".to_string(), object_prefix.to_string());
        url_values.insert("delimiter".to_string(), delimiter.to_string());

        if continuation_token != "" {
            url_values.insert("continuation-token".to_string(), continuation_token.to_string());
        }

        if fetch_owner {
            url_values.insert("fetch-owner".to_string(), "true".to_string());
        }

        if max_keys > 0 {
            url_values.insert("max-keys".to_string(), max_keys.to_string());
        }

        let mut resp = self
            .execute_method(
                http::Method::GET,
                &mut RequestMetadata {
                    bucket_name: bucket_name.to_string(),
                    object_name: "".to_string(),
                    query_values: url_values,
                    content_sha256_hex: EMPTY_STRING_SHA256_HASH.to_string(),
                    custom_header: headers,
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

        let resp_status = resp.status();
        let h = resp.headers().clone();

        if resp.status() != StatusCode::OK {
            return Err(std::io::Error::other(http_resp_to_error_response(
                resp_status,
                &h,
                vec![],
                bucket_name,
                "",
            )));
        }

        //let mut list_bucket_result = ListBucketV2Result::default();
        let mut body_vec = Vec::new();
        let mut body = resp.into_body();
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
            if let Some(data) = frame.data_ref() {
                body_vec.extend_from_slice(data);
            }
        }
        let mut list_bucket_result = match quick_xml::de::from_str::<ListBucketV2Result>(&String::from_utf8(body_vec).unwrap()) {
            Ok(result) => result,
            Err(err) => {
                return Err(std::io::Error::other(err.to_string()));
            }
        };
        //println!("list_bucket_result: {:?}", list_bucket_result);

        if list_bucket_result.is_truncated && list_bucket_result.next_continuation_token == "" {
            return Err(std::io::Error::other(credentials::ErrorResponse {
                sts_error: credentials::STSError {
                    r#type: "".to_string(),
                    code: "NotImplemented".to_string(),
                    message: "Truncated response should have continuation token set".to_string(),
                },
                request_id: "".to_string(),
            }));
        }

        for (i, obj) in list_bucket_result.contents.iter_mut().enumerate() {
            obj.name = decode_s3_name(&obj.name, &list_bucket_result.encoding_type)?;
            //list_bucket_result.contents[i].mod_time = list_bucket_result.contents[i].mod_time.Truncate(time.Millisecond);
        }

        for (i, obj) in list_bucket_result.common_prefixes.iter_mut().enumerate() {
            obj.prefix = decode_s3_name(&obj.prefix, &list_bucket_result.encoding_type)?;
        }

        Ok(list_bucket_result)
    }

    pub fn list_object_versions_query(
        &self,
        bucket_name: &str,
        opts: &ListObjectsOptions,
        key_marker: &str,
        version_id_marker: &str,
        delimiter: &str,
    ) -> Result<ListVersionsResult, std::io::Error> {
        /*if err := s3utils.CheckValidBucketName(bucketName); err != nil {
          return ListVersionsResult{}, err
        }
        if err := s3utils.CheckValidObjectNamePrefix(opts.Prefix); err != nil {
          return ListVersionsResult{}, err
        }
        urlValues := make(url.Values)

        urlValues.Set("versions", "")

        urlValues.Set("prefix", opts.Prefix)

        urlValues.Set("delimiter", delimiter)

        if keyMarker != "" {
          urlValues.Set("key-marker", keyMarker)
        }

        if opts.max_keys > 0 {
          urlValues.Set("max-keys", fmt.Sprintf("%d", opts.max_keys))
        }

        if versionIDMarker != "" {
          urlValues.Set("version-id-marker", versionIDMarker)
        }

        if opts.WithMetadata {
          urlValues.Set("metadata", "true")
        }

        urlValues.Set("encoding-type", "url")

        let resp = self.executeMethod(http::Method::GET, &mut RequestMetadata{
            bucketName:       bucketName,
            queryValues:      urlValues,
            contentSHA256Hex: emptySHA256Hex,
            customHeader:     opts.headers,
        }).await?;
        defer closeResponse(resp)
        if err != nil {
          return ListVersionsResult{}, err
        }
        if resp != nil {
          if resp.StatusCode != http.StatusOK {
            return ListVersionsResult{}, httpRespToErrorResponse(resp, bucketName, "")
          }
        }

        listObjectVersionsOutput := ListVersionsResult{}
        err = xml_decoder(resp.Body, &listObjectVersionsOutput)
        if err != nil {
          return ListVersionsResult{}, err
        }

        for i, obj := range listObjectVersionsOutput.Versions {
          listObjectVersionsOutput.Versions[i].Key, err = decode_s3_name(obj.Key, listObjectVersionsOutput.EncodingType)
          if err != nil {
            return listObjectVersionsOutput, err
          }
        }

        for i, obj := range listObjectVersionsOutput.CommonPrefixes {
          listObjectVersionsOutput.CommonPrefixes[i].Prefix, err = decode_s3_name(obj.Prefix, listObjectVersionsOutput.EncodingType)
          if err != nil {
            return listObjectVersionsOutput, err
          }
        }

        if listObjectVersionsOutput.NextKeyMarker != "" {
          listObjectVersionsOutput.NextKeyMarker, err = decode_s3_name(listObjectVersionsOutput.NextKeyMarker, listObjectVersionsOutput.EncodingType)
          if err != nil {
            return listObjectVersionsOutput, err
          }
        }

        Ok(listObjectVersionsOutput)*/
        todo!();
    }

    pub fn list_objects_query(
        &self,
        bucket_name: &str,
        object_prefix: &str,
        object_marker: &str,
        delimiter: &str,
        max_keys: i64,
        headers: HeaderMap,
    ) -> Result<ListBucketResult, std::io::Error> {
        todo!();
    }

    pub fn list_multipart_uploads_query(
        &self,
        bucket_name: &str,
        key_marker: &str,
        upload_id_marker: &str,
        prefix: &str,
        delimiter: &str,
        max_uploads: i64,
    ) -> Result<ListMultipartUploadsResult, std::io::Error> {
        todo!();
    }

    pub fn list_object_parts(
        &self,
        bucket_name: &str,
        object_name: &str,
        upload_id: &str,
    ) -> Result<HashMap<i64, ObjectPart>, std::io::Error> {
        todo!();
    }

    pub fn find_upload_ids(&self, bucket_name: &str, object_name: &str) -> Result<Vec<String>, std::io::Error> {
        todo!();
    }

    pub async fn list_object_parts_query(
        &self,
        bucket_name: &str,
        object_name: &str,
        upload_id: &str,
        part_number_marker: i64,
        max_parts: i64,
    ) -> Result<ListObjectPartsResult, std::io::Error> {
        todo!();
    }
}

#[allow(dead_code)]
pub struct ListObjectsOptions {
    reverse_versions: bool,
    with_versions: bool,
    with_metadata: bool,
    prefix: String,
    recursive: bool,
    max_keys: i64,
    start_after: String,
    use_v1: bool,
    headers: HeaderMap,
}

impl ListObjectsOptions {
    pub fn set(&mut self, key: &str, value: &str) {
        todo!();
    }
}

fn decode_s3_name(name: &str, encoding_type: &str) -> Result<String, std::io::Error> {
    match encoding_type {
        "url" => {
            //return url::QueryUnescape(name);
            return Ok(name.to_string());
        }
        _ => {
            return Ok(name.to_string());
        }
    }
}
