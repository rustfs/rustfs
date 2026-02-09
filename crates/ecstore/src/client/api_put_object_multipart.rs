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
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_assignments)]
#![allow(unused_must_use)]
#![allow(clippy::all)]

use http::{HeaderMap, HeaderName, StatusCode};
use hyper::body::Bytes;
use s3s::S3ErrorCode;
use std::collections::HashMap;
use time::OffsetDateTime;
use tracing::warn;
use uuid::Uuid;

use crate::client::checksum::ChecksumMode;
use crate::client::utils::base64_encode;
use crate::client::{
    api_error_response::{
        err_entity_too_large, err_entity_too_small, err_invalid_argument, http_resp_to_error_response, to_error_response,
    },
    api_put_object::PutObjectOptions,
    api_put_object_common::optimal_part_info,
    api_s3_datatypes::{
        CompleteMultipartUpload, CompleteMultipartUploadResult, CompletePart, InitiateMultipartUploadResult, ObjectPart,
    },
    constants::{ISO8601_DATEFORMAT, MAX_PART_SIZE, MAX_SINGLE_PUT_OBJECT_SIZE},
    transition_api::{ReaderImpl, RequestMetadata, TransitionClient, UploadInfo},
};
use rustfs_utils::path::trim_etag;
use s3s::header::{X_AMZ_EXPIRATION, X_AMZ_VERSION_ID};

impl TransitionClient {
    pub async fn put_object_multipart(
        &self,
        bucket_name: &str,
        object_name: &str,
        mut reader: ReaderImpl,
        size: i64,
        opts: &PutObjectOptions,
    ) -> Result<UploadInfo, std::io::Error> {
        let info = self
            .put_object_multipart_no_stream(bucket_name, object_name, &mut reader, opts)
            .await;
        if let Err(err) = &info {
            let err_resp = to_error_response(err);
            if err_resp.code == S3ErrorCode::AccessDenied && err_resp.message.contains("Access Denied") {
                if size > MAX_SINGLE_PUT_OBJECT_SIZE {
                    return Err(std::io::Error::other(err_entity_too_large(
                        size,
                        MAX_SINGLE_PUT_OBJECT_SIZE,
                        bucket_name,
                        object_name,
                    )));
                }
                return self.put_object_gcs(bucket_name, object_name, reader, size, opts).await;
            }
        }
        Ok(info?)
    }

    pub async fn put_object_multipart_no_stream(
        &self,
        bucket_name: &str,
        object_name: &str,
        reader: &mut ReaderImpl,
        opts: &PutObjectOptions,
    ) -> Result<UploadInfo, std::io::Error> {
        let mut total_uploaded_size: i64 = 0;
        let mut compl_multipart_upload = CompleteMultipartUpload::default();

        let ret = optimal_part_info(-1, opts.part_size)?;
        let (total_parts_count, part_size, _) = ret;

        let (mut hash_algos, mut hash_sums) = self.hash_materials(opts.send_content_md5, !opts.disable_content_sha256);
        let upload_id = self.new_upload_id(bucket_name, object_name, opts).await?;
        let mut opts = opts.clone();
        opts.user_metadata.remove("X-Amz-Checksum-Algorithm");

        let mut part_number = 1;
        let mut parts_info = HashMap::<i64, ObjectPart>::new();
        let mut buf = Vec::<u8>::with_capacity(part_size as usize);
        let mut custom_header = HeaderMap::new();
        while part_number <= total_parts_count {
            match reader {
                ReaderImpl::Body(content_body) => {
                    buf = content_body.to_vec();
                }
                ReaderImpl::ObjectBody(content_body) => {
                    buf = content_body.read_all().await?;
                }
            }
            let length = buf.len();

            for (k, v) in hash_algos.iter_mut() {
                let hash = v.hash_encode(&buf[..length]);
                hash_sums.insert(k.to_string(), hash.as_ref().to_vec());
            }

            //let rd = newHook(bytes.NewReader(buf[..length]), opts.progress);
            let rd = Bytes::from(buf.clone());

            let md5_base64: String;
            let sha256_hex: String;

            //if hash_sums["md5"] != nil {
            md5_base64 = base64_encode(&hash_sums["md5"]);
            //}
            //if hash_sums["sha256"] != nil {
            sha256_hex = hex_simd::encode_to_string(hash_sums["sha256"].clone(), hex_simd::AsciiCase::Lower);
            //}
            if hash_sums.len() == 0 {
                let mut crc = opts.auto_checksum.hasher()?;
                crc.update(&buf[..length]);
                let csum = crc.finalize();

                if let Ok(header_name) = HeaderName::from_bytes(opts.auto_checksum.key().as_bytes()) {
                    custom_header.insert(header_name, base64_encode(csum.as_ref()).parse().expect("err"));
                } else {
                    warn!("Invalid header name: {}", opts.auto_checksum.key());
                }
            }

            let mut p = UploadPartParams {
                bucket_name: bucket_name.to_string(),
                object_name: object_name.to_string(),
                upload_id: upload_id.clone(),
                reader: ReaderImpl::Body(rd),
                part_number,
                md5_base64,
                sha256_hex,
                size: length as i64,
                //sse: opts.server_side_encryption,
                stream_sha256: !opts.disable_content_sha256,
                custom_header: custom_header.clone(),
                trailer: HeaderMap::new(),
            };
            let obj_part = self.upload_part(&mut p).await?;

            parts_info.insert(part_number, obj_part);
            total_uploaded_size += length as i64;
            part_number += 1;
        }

        let mut all_parts = Vec::<ObjectPart>::with_capacity(parts_info.len());
        for i in 1..part_number {
            let part = parts_info[&i].clone();
            all_parts.push(part.clone());
            compl_multipart_upload.parts.push(CompletePart {
                etag: part.etag,
                part_num: part.part_num,
                checksum_crc32: part.checksum_crc32,
                checksum_crc32c: part.checksum_crc32c,
                checksum_sha1: part.checksum_sha1,
                checksum_sha256: part.checksum_sha256,
                checksum_crc64nvme: part.checksum_crc64nvme,
                ..Default::default()
            });
        }

        compl_multipart_upload.parts.sort();
        let opts = PutObjectOptions {
            //server_side_encryption: opts.server_side_encryption,
            auto_checksum: opts.auto_checksum,
            ..Default::default()
        };
        //apply_auto_checksum(&mut opts, all_parts);

        let mut upload_info = self
            .complete_multipart_upload(bucket_name, object_name, &upload_id, compl_multipart_upload, &opts)
            .await?;

        upload_info.size = total_uploaded_size;
        Ok(upload_info)
    }

    pub async fn initiate_multipart_upload(
        &self,
        bucket_name: &str,
        object_name: &str,
        opts: &PutObjectOptions,
    ) -> Result<InitiateMultipartUploadResult, std::io::Error> {
        let mut url_values = HashMap::new();
        url_values.insert("uploads".to_string(), "".to_string());

        if opts.internal.source_version_id != "" {
            if !opts.internal.source_version_id.is_empty() {
                if let Err(err) = Uuid::parse_str(&opts.internal.source_version_id) {
                    return Err(std::io::Error::other(err_invalid_argument(&err.to_string())));
                }
            }
            url_values.insert("versionId".to_string(), opts.internal.source_version_id.clone());
        }

        let custom_header = opts.header();

        let mut req_metadata = RequestMetadata {
            bucket_name: bucket_name.to_string(),
            object_name: object_name.to_string(),
            query_values: url_values,
            custom_header,
            content_body: ReaderImpl::Body(Bytes::new()),
            content_length: 0,
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

        let resp = self.execute_method(http::Method::POST, &mut req_metadata).await?;

        let resp_status = resp.status();
        let h = resp.headers().clone();

        //if resp.is_none() {
        if resp.status() != StatusCode::OK {
            return Err(std::io::Error::other(http_resp_to_error_response(
                resp_status,
                &h,
                vec![],
                bucket_name,
                object_name,
            )));
        }
        //}
        let initiate_multipart_upload_result = InitiateMultipartUploadResult::default();
        Ok(initiate_multipart_upload_result)
    }

    pub async fn upload_part(&self, p: &mut UploadPartParams) -> Result<ObjectPart, std::io::Error> {
        if p.size > MAX_PART_SIZE {
            return Err(std::io::Error::other(err_entity_too_large(
                p.size,
                MAX_PART_SIZE,
                &p.bucket_name,
                &p.object_name,
            )));
        }
        if p.size <= -1 {
            return Err(std::io::Error::other(err_entity_too_small(p.size, &p.bucket_name, &p.object_name)));
        }
        if p.part_number <= 0 {
            return Err(std::io::Error::other(err_invalid_argument(
                "Part number cannot be negative or equal to zero.",
            )));
        }
        if p.upload_id == "" {
            return Err(std::io::Error::other(err_invalid_argument("UploadID cannot be empty.")));
        }

        let mut url_values = HashMap::new();
        url_values.insert("partNumber".to_string(), p.part_number.to_string());
        url_values.insert("uploadId".to_string(), p.upload_id.clone());

        let buf = match &mut p.reader {
            ReaderImpl::Body(content_body) => content_body.to_vec(),
            ReaderImpl::ObjectBody(content_body) => content_body.read_all().await?,
        };
        let mut req_metadata = RequestMetadata {
            bucket_name: p.bucket_name.clone(),
            object_name: p.object_name.clone(),
            query_values: url_values,
            custom_header: p.custom_header.clone(),
            content_body: ReaderImpl::Body(Bytes::from(buf)),
            content_length: p.size,
            content_md5_base64: p.md5_base64.clone(),
            content_sha256_hex: p.sha256_hex.clone(),
            stream_sha256: p.stream_sha256,
            trailer: p.trailer.clone(),
            pre_sign_url: Default::default(),
            add_crc: Default::default(),
            extra_pre_sign_header: Default::default(),
            bucket_location: Default::default(),
            expires: Default::default(),
        };

        let resp = self.execute_method(http::Method::PUT, &mut req_metadata).await?;

        let resp_status = resp.status();
        let h = resp.headers().clone();

        if resp.status() != StatusCode::OK {
            return Err(std::io::Error::other(http_resp_to_error_response(
                resp_status,
                &h,
                vec![],
                &p.bucket_name.clone(),
                &p.object_name,
            )));
        }
        //}
        let h = resp.headers();
        let mut obj_part = ObjectPart {
            checksum_crc32: if let Some(h_checksum_crc32) = h.get(ChecksumMode::ChecksumCRC32.key()) {
                h_checksum_crc32.to_str().expect("err").to_string()
            } else {
                "".to_string()
            },
            checksum_crc32c: if let Some(h_checksum_crc32c) = h.get(ChecksumMode::ChecksumCRC32C.key()) {
                h_checksum_crc32c.to_str().expect("err").to_string()
            } else {
                "".to_string()
            },
            checksum_sha1: if let Some(h_checksum_sha1) = h.get(ChecksumMode::ChecksumSHA1.key()) {
                h_checksum_sha1.to_str().expect("err").to_string()
            } else {
                "".to_string()
            },
            checksum_sha256: if let Some(h_checksum_sha256) = h.get(ChecksumMode::ChecksumSHA256.key()) {
                h_checksum_sha256.to_str().expect("err").to_string()
            } else {
                "".to_string()
            },
            checksum_crc64nvme: if let Some(h_checksum_crc64nvme) = h.get(ChecksumMode::ChecksumCRC64NVME.key()) {
                h_checksum_crc64nvme.to_str().expect("err").to_string()
            } else {
                "".to_string()
            },
            ..Default::default()
        };
        obj_part.size = p.size;
        obj_part.part_num = p.part_number;
        obj_part.etag = if let Some(h_etag) = h.get("ETag") {
            h_etag.to_str().expect("err").trim_matches('"').to_string()
        } else {
            "".to_string()
        };
        Ok(obj_part)
    }

    pub async fn complete_multipart_upload(
        &self,
        bucket_name: &str,
        object_name: &str,
        upload_id: &str,
        complete: CompleteMultipartUpload,
        opts: &PutObjectOptions,
    ) -> Result<UploadInfo, std::io::Error> {
        let mut url_values = HashMap::new();
        url_values.insert("uploadId".to_string(), upload_id.to_string());
        let complete_multipart_upload_bytes = complete.marshal_msg()?.as_bytes().to_vec();

        let headers = opts.header();

        let complete_multipart_upload_buffer = Bytes::from(complete_multipart_upload_bytes);
        let mut req_metadata = RequestMetadata {
            bucket_name: bucket_name.to_string(),
            object_name: object_name.to_string(),
            query_values: url_values,
            custom_header: headers,
            content_body: ReaderImpl::Body(complete_multipart_upload_buffer),
            content_length: 100,                //complete_multipart_upload_bytes.len(),
            content_sha256_hex: "".to_string(), //hex_simd::encode_to_string(complete_multipart_upload_bytes, hex_simd::AsciiCase::Lower),
            content_md5_base64: "".to_string(),
            stream_sha256: Default::default(),
            trailer: Default::default(),
            pre_sign_url: Default::default(),
            add_crc: Default::default(),
            extra_pre_sign_header: Default::default(),
            bucket_location: Default::default(),
            expires: Default::default(),
        };

        let resp = self.execute_method(http::Method::POST, &mut req_metadata).await?;

        let h = resp.headers().clone();

        let complete_multipart_upload_result: CompleteMultipartUploadResult = CompleteMultipartUploadResult::default();

        let (exp_time, rule_id) = if let Some(h_x_amz_expiration) = resp.headers().get(X_AMZ_EXPIRATION) {
            (
                OffsetDateTime::parse(h_x_amz_expiration.to_str().unwrap(), ISO8601_DATEFORMAT).unwrap(),
                "".to_string(),
            )
        } else {
            (OffsetDateTime::now_utc(), "".to_string())
        };

        Ok(UploadInfo {
            bucket: complete_multipart_upload_result.bucket,
            key: complete_multipart_upload_result.key,
            etag: trim_etag(&complete_multipart_upload_result.etag),
            version_id: if let Some(h_x_amz_version_id) = h.get(X_AMZ_VERSION_ID) {
                h_x_amz_version_id.to_str().expect("err").to_string()
            } else {
                "".to_string()
            },
            location: complete_multipart_upload_result.location,
            expiration: exp_time,
            expiration_rule_id: rule_id,
            checksum_sha256: complete_multipart_upload_result.checksum_sha256,
            checksum_sha1: complete_multipart_upload_result.checksum_sha1,
            checksum_crc32: complete_multipart_upload_result.checksum_crc32,
            checksum_crc32c: complete_multipart_upload_result.checksum_crc32c,
            checksum_crc64nvme: complete_multipart_upload_result.checksum_crc64nvme,
            ..Default::default()
        })
    }
}

pub struct UploadPartParams {
    pub bucket_name: String,
    pub object_name: String,
    pub upload_id: String,
    pub reader: ReaderImpl,
    pub part_number: i64,
    pub md5_base64: String,
    pub sha256_hex: String,
    pub size: i64,
    //pub sse: encrypt.ServerSide,
    pub stream_sha256: bool,
    pub custom_header: HeaderMap,
    pub trailer: HeaderMap,
}
