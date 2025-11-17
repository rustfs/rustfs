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

use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use rustfs_utils::HashAlgorithm;
use s3s::S3ErrorCode;
use s3s::dto::ReplicationStatus;
use s3s::header::X_AMZ_BYPASS_GOVERNANCE_RETENTION;
use std::fmt::Display;
use std::{collections::HashMap, sync::Arc};
use time::OffsetDateTime;
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::client::utils::base64_encode;
use crate::client::{
    api_error_response::{ErrorResponse, http_resp_to_error_response, to_error_response},
    transition_api::{ReaderImpl, RequestMetadata, TransitionClient},
};
use crate::{
    disk::DiskAPI,
    store_api::{GetObjectReader, ObjectInfo, StorageAPI},
};
use rustfs_utils::hash::EMPTY_STRING_SHA256_HASH;

pub struct RemoveBucketOptions {
    _forced_elete: bool,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct AdvancedRemoveOptions {
    pub replication_delete_marker: bool,
    pub replication_status: ReplicationStatus,
    pub replication_mtime: Option<OffsetDateTime>,
    pub replication_request: bool,
    pub replication_validity_check: bool,
}

impl Default for AdvancedRemoveOptions {
    fn default() -> Self {
        Self {
            replication_delete_marker: false,
            replication_status: ReplicationStatus::from_static(ReplicationStatus::PENDING),
            replication_mtime: None,
            replication_request: false,
            replication_validity_check: false,
        }
    }
}

#[derive(Debug, Default)]
pub struct RemoveObjectOptions {
    pub force_delete: bool,
    pub governance_bypass: bool,
    pub version_id: String,
    pub internal: AdvancedRemoveOptions,
}

impl TransitionClient {
    pub async fn remove_bucket_with_options(&self, bucket_name: &str, opts: &RemoveBucketOptions) -> Result<(), std::io::Error> {
        let headers = HeaderMap::new();

        let resp = self
            .execute_method(
                Method::DELETE,
                &mut RequestMetadata {
                    bucket_name: bucket_name.to_string(),
                    content_sha256_hex: EMPTY_STRING_SHA256_HASH.to_string(),
                    custom_header: headers,
                    object_name: "".to_string(),
                    query_values: Default::default(),
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

        {
            let mut bucket_loc_cache = self.bucket_loc_cache.lock().unwrap();
            bucket_loc_cache.delete(bucket_name);
        }
        Ok(())
    }

    pub async fn remove_bucket(&self, bucket_name: &str) -> Result<(), std::io::Error> {
        let resp = self
            .execute_method(
                http::Method::DELETE,
                &mut RequestMetadata {
                    bucket_name: bucket_name.to_string(),
                    content_sha256_hex: EMPTY_STRING_SHA256_HASH.to_string(),
                    custom_header: Default::default(),
                    object_name: "".to_string(),
                    query_values: Default::default(),
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

        {
            let mut bucket_loc_cache = self.bucket_loc_cache.lock().unwrap();
            bucket_loc_cache.delete(bucket_name);
        }

        Ok(())
    }

    pub async fn remove_object(&self, bucket_name: &str, object_name: &str, opts: RemoveObjectOptions) -> Option<std::io::Error> {
        self.remove_object_inner(bucket_name, object_name, opts).await.err()
    }

    pub async fn remove_object_inner(
        &self,
        bucket_name: &str,
        object_name: &str,
        opts: RemoveObjectOptions,
    ) -> Result<RemoveObjectResult, std::io::Error> {
        let mut url_values = HashMap::new();

        if opts.version_id != "" {
            url_values.insert("versionId".to_string(), opts.version_id.clone());
        }

        let mut headers = HeaderMap::new();

        if opts.governance_bypass {
            headers.insert(X_AMZ_BYPASS_GOVERNANCE_RETENTION, "true".parse().expect("err")); //amzBypassGovernance
        }

        let resp = self
            .execute_method(
                http::Method::DELETE,
                &mut RequestMetadata {
                    bucket_name: bucket_name.to_string(),
                    object_name: object_name.to_string(),
                    content_sha256_hex: EMPTY_STRING_SHA256_HASH.to_string(),
                    query_values: url_values,
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

        Ok(RemoveObjectResult {
            object_name: object_name.to_string(),
            object_version_id: opts.version_id,
            delete_marker: resp.headers().get("x-amz-delete-marker").expect("err") == "true",
            delete_marker_version_id: resp
                .headers()
                .get("x-amz-version-id")
                .expect("err")
                .to_str()
                .expect("err")
                .to_string(),
            ..Default::default()
        })
    }

    pub async fn remove_objects_with_result(
        self: Arc<Self>,
        bucket_name: &str,
        objects_rx: Receiver<ObjectInfo>,
        opts: RemoveObjectsOptions,
    ) -> Receiver<RemoveObjectResult> {
        let (result_tx, result_rx) = mpsc::channel(1);

        let self_clone = Arc::clone(&self);
        let bucket_name_owned = bucket_name.to_string();

        tokio::spawn(async move {
            self_clone
                .remove_objects_inner(&bucket_name_owned, objects_rx, &result_tx, opts)
                .await;
        });
        result_rx
    }

    pub async fn remove_objects(
        self: Arc<Self>,
        bucket_name: &str,
        objects_rx: Receiver<ObjectInfo>,
        opts: RemoveObjectsOptions,
    ) -> Receiver<RemoveObjectError> {
        let (error_tx, error_rx) = mpsc::channel(1);

        let self_clone = Arc::clone(&self);
        let bucket_name_owned = bucket_name.to_string();

        let (result_tx, mut result_rx) = mpsc::channel(1);
        tokio::spawn(async move {
            self_clone
                .remove_objects_inner(&bucket_name_owned, objects_rx, &result_tx, opts)
                .await;
        });
        tokio::spawn(async move {
            while let Some(res) = result_rx.recv().await {
                if res.err.is_none() {
                    continue;
                }
                error_tx
                    .send(RemoveObjectError {
                        object_name: res.object_name,
                        version_id: res.object_version_id,
                        err: res.err,
                        ..Default::default()
                    })
                    .await;
            }
        });

        error_rx
    }

    pub async fn remove_objects_inner(
        &self,
        bucket_name: &str,
        mut objects_rx: Receiver<ObjectInfo>,
        result_tx: &Sender<RemoveObjectResult>,
        opts: RemoveObjectsOptions,
    ) -> Result<(), std::io::Error> {
        let max_entries = 1000;
        let mut finish = false;
        let mut url_values = HashMap::new();
        url_values.insert("delete".to_string(), "".to_string());

        loop {
            if finish {
                break;
            }
            let mut count = 0;
            let mut batch = Vec::<ObjectInfo>::new();

            while let Some(object) = objects_rx.recv().await {
                if has_invalid_xml_char(&object.name) {
                    let remove_result = self
                        .remove_object_inner(
                            bucket_name,
                            &object.name,
                            RemoveObjectOptions {
                                version_id: object.version_id.expect("err").to_string(),
                                governance_bypass: opts.governance_bypass,
                                ..Default::default()
                            },
                        )
                        .await?;
                    let remove_result_clone = remove_result.clone();
                    if !remove_result.err.is_none() {
                        match to_error_response(&remove_result.err.expect("err")).code {
                            S3ErrorCode::InvalidArgument | S3ErrorCode::NoSuchVersion => {
                                continue;
                            }
                            _ => (),
                        }
                        result_tx.send(remove_result_clone.clone()).await;
                    }

                    result_tx.send(remove_result_clone).await;
                    continue;
                }

                batch.push(object);
                count += 1;
                if count >= max_entries {
                    break;
                }
            }
            if count == 0 {
                break;
            }
            if count < max_entries {
                finish = true;
            }

            let mut headers = HeaderMap::new();
            if opts.governance_bypass {
                headers.insert(X_AMZ_BYPASS_GOVERNANCE_RETENTION, "true".parse().expect("err"));
            }

            let remove_bytes = generate_remove_multi_objects_request(&batch);
            let resp = self
                .execute_method(
                    http::Method::POST,
                    &mut RequestMetadata {
                        bucket_name: bucket_name.to_string(),
                        query_values: url_values.clone(),
                        content_body: ReaderImpl::Body(Bytes::from(remove_bytes.clone())),
                        content_length: remove_bytes.len() as i64,
                        content_md5_base64: base64_encode(&HashAlgorithm::Md5.hash_encode(&remove_bytes).as_ref()),
                        content_sha256_hex: base64_encode(&HashAlgorithm::SHA256.hash_encode(&remove_bytes).as_ref()),
                        custom_header: headers,
                        object_name: "".to_string(),
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

            let body_bytes: Vec<u8> = resp.body().bytes().expect("err").to_vec();
            process_remove_multi_objects_response(ReaderImpl::Body(Bytes::from(body_bytes)), result_tx.clone());
        }
        Ok(())
    }

    pub async fn remove_incomplete_upload(&self, bucket_name: &str, object_name: &str) -> Result<(), std::io::Error> {
        let upload_ids = self.find_upload_ids(bucket_name, object_name)?;
        for upload_id in upload_ids {
            self.abort_multipart_upload(bucket_name, object_name, &upload_id).await?;
        }

        Ok(())
    }

    pub async fn abort_multipart_upload(
        &self,
        bucket_name: &str,
        object_name: &str,
        upload_id: &str,
    ) -> Result<(), std::io::Error> {
        let mut url_values = HashMap::new();
        url_values.insert("uploadId".to_string(), upload_id.to_string());

        let resp = self
            .execute_method(
                http::Method::DELETE,
                &mut RequestMetadata {
                    bucket_name: bucket_name.to_string(),
                    object_name: object_name.to_string(),
                    query_values: url_values,
                    content_sha256_hex: EMPTY_STRING_SHA256_HASH.to_string(),
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
        //if resp.is_some() {
        if resp.status() != StatusCode::NO_CONTENT {
            let error_response: ErrorResponse;
            match resp.status() {
                StatusCode::NOT_FOUND => {
                    error_response = ErrorResponse {
                        code: S3ErrorCode::NoSuchUpload,
                        message: "The specified multipart upload does not exist.".to_string(),
                        bucket_name: bucket_name.to_string(),
                        key: object_name.to_string(),
                        request_id: resp
                            .headers()
                            .get("x-amz-request-id")
                            .expect("err")
                            .to_str()
                            .expect("err")
                            .to_string(),
                        host_id: resp
                            .headers()
                            .get("x-amz-id-2")
                            .expect("err")
                            .to_str()
                            .expect("err")
                            .to_string(),
                        region: resp
                            .headers()
                            .get("x-amz-bucket-region")
                            .expect("err")
                            .to_str()
                            .expect("err")
                            .to_string(),
                        ..Default::default()
                    };
                }
                _ => {
                    return Err(std::io::Error::other(http_resp_to_error_response(
                        &resp,
                        vec![],
                        bucket_name,
                        object_name,
                    )));
                }
            }
            return Err(std::io::Error::other(error_response));
        }
        //}
        Ok(())
    }
}

#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct RemoveObjectError {
    object_name: String,
    #[allow(dead_code)]
    version_id: String,
    err: Option<std::io::Error>,
}

impl Display for RemoveObjectError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if self.err.is_none() {
            return write!(f, "unexpected remove object error result");
        }
        write!(f, "{}", self.err.as_ref().expect("err").to_string())
    }
}

#[derive(Debug, Default)]
pub struct RemoveObjectResult {
    pub object_name: String,
    pub object_version_id: String,
    pub delete_marker: bool,
    pub delete_marker_version_id: String,
    pub err: Option<std::io::Error>,
}

impl Clone for RemoveObjectResult {
    fn clone(&self) -> Self {
        Self {
            object_name: self.object_name.clone(),
            object_version_id: self.object_version_id.clone(),
            delete_marker: self.delete_marker,
            delete_marker_version_id: self.delete_marker_version_id.clone(),
            err: None, //err
        }
    }
}

pub struct RemoveObjectsOptions {
    pub governance_bypass: bool,
}

pub fn generate_remove_multi_objects_request(objects: &[ObjectInfo]) -> Vec<u8> {
    todo!();
}

pub fn process_remove_multi_objects_response(body: ReaderImpl, result_tx: Sender<RemoveObjectResult>) {
    todo!();
}

fn has_invalid_xml_char(str: &str) -> bool {
    false
}
