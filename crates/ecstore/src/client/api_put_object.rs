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
use http::{HeaderMap, HeaderName, HeaderValue};
use std::{collections::HashMap, sync::Arc};
use time::{Duration, OffsetDateTime, macros::format_description};
use tracing::{error, info, warn};

use s3s::dto::{ObjectLockLegalHoldStatus, ObjectLockRetentionMode, ReplicationStatus};
use s3s::header::{
    X_AMZ_OBJECT_LOCK_LEGAL_HOLD, X_AMZ_OBJECT_LOCK_MODE, X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE, X_AMZ_REPLICATION_STATUS,
    X_AMZ_STORAGE_CLASS, X_AMZ_WEBSITE_REDIRECT_LOCATION,
};
//use crate::disk::{BufferReader, Reader};
use crate::client::checksum::ChecksumMode;
use crate::client::utils::base64_encode;
use crate::client::{
    api_error_response::{err_entity_too_large, err_invalid_argument},
    api_put_object_common::optimal_part_info,
    api_put_object_multipart::UploadPartParams,
    api_s3_datatypes::{CompleteMultipartUpload, CompletePart, ObjectPart},
    constants::{ISO8601_DATEFORMAT, MAX_MULTIPART_PUT_OBJECT_SIZE, MIN_PART_SIZE, TOTAL_WORKERS},
    credentials::SignatureType,
    transition_api::{ReaderImpl, TransitionClient, UploadInfo},
    utils::{is_amz_header, is_minio_header, is_rustfs_header, is_standard_header, is_storageclass_header},
};

#[derive(Debug, Clone)]
pub struct AdvancedPutOptions {
    pub source_version_id: String,
    pub source_etag: String,
    pub replication_status: ReplicationStatus,
    pub source_mtime: OffsetDateTime,
    pub replication_request: bool,
    pub retention_timestamp: OffsetDateTime,
    pub tagging_timestamp: OffsetDateTime,
    pub legalhold_timestamp: OffsetDateTime,
    pub replication_validity_check: bool,
}

impl Default for AdvancedPutOptions {
    fn default() -> Self {
        Self {
            source_version_id: "".to_string(),
            source_etag: "".to_string(),
            replication_status: ReplicationStatus::from_static(ReplicationStatus::PENDING),
            source_mtime: OffsetDateTime::now_utc(),
            replication_request: false,
            retention_timestamp: OffsetDateTime::now_utc(),
            tagging_timestamp: OffsetDateTime::now_utc(),
            legalhold_timestamp: OffsetDateTime::now_utc(),
            replication_validity_check: false,
        }
    }
}

#[derive(Clone)]
pub struct PutObjectOptions {
    pub user_metadata: HashMap<String, String>,
    pub user_tags: HashMap<String, String>,
    //pub progress: ReaderImpl,
    pub content_type: String,
    pub content_encoding: String,
    pub content_disposition: String,
    pub content_language: String,
    pub cache_control: String,
    pub expires: OffsetDateTime,
    pub mode: ObjectLockRetentionMode,
    pub retain_until_date: OffsetDateTime,
    //pub server_side_encryption: encrypt::ServerSide,
    pub num_threads: u64,
    pub storage_class: String,
    pub website_redirect_location: String,
    pub part_size: u64,
    pub legalhold: ObjectLockLegalHoldStatus,
    pub send_content_md5: bool,
    pub disable_content_sha256: bool,
    pub disable_multipart: bool,
    pub auto_checksum: ChecksumMode,
    pub checksum: ChecksumMode,
    pub concurrent_stream_parts: bool,
    pub internal: AdvancedPutOptions,
    pub custom_header: HeaderMap,
}

impl Default for PutObjectOptions {
    fn default() -> Self {
        Self {
            user_metadata: HashMap::new(),
            user_tags: HashMap::new(),
            //progress: ReaderImpl::Body(Bytes::new()),
            content_type: "".to_string(),
            content_encoding: "".to_string(),
            content_disposition: "".to_string(),
            content_language: "".to_string(),
            cache_control: "".to_string(),
            expires: OffsetDateTime::UNIX_EPOCH,
            mode: ObjectLockRetentionMode::from_static(""),
            retain_until_date: OffsetDateTime::UNIX_EPOCH,
            //server_side_encryption: encrypt.ServerSide::default(),
            num_threads: 0,
            storage_class: "".to_string(),
            website_redirect_location: "".to_string(),
            part_size: 0,
            legalhold: ObjectLockLegalHoldStatus::from_static(ObjectLockLegalHoldStatus::OFF),
            send_content_md5: false,
            disable_content_sha256: false,
            disable_multipart: false,
            auto_checksum: ChecksumMode::ChecksumNone,
            checksum: ChecksumMode::ChecksumNone,
            concurrent_stream_parts: false,
            internal: AdvancedPutOptions::default(),
            custom_header: HeaderMap::new(),
        }
    }
}

#[allow(dead_code)]
impl PutObjectOptions {
    fn set_match_etag(&mut self, etag: &str) {
        if etag == "*" {
            self.custom_header
                .insert("If-Match", HeaderValue::from_str("*").expect("err"));
        } else {
            self.custom_header
                .insert("If-Match", HeaderValue::from_str(&format!("\"{}\"", etag)).expect("err"));
        }
    }

    fn set_match_etag_except(&mut self, etag: &str) {
        if etag == "*" {
            self.custom_header
                .insert("If-None-Match", HeaderValue::from_str("*").expect("err"));
        } else {
            self.custom_header
                .insert("If-None-Match", HeaderValue::from_str(&format!("\"{etag}\"")).expect("err"));
        }
    }

    pub fn header(&self) -> HeaderMap {
        let mut header = HeaderMap::new();

        let mut content_type = self.content_type.clone();
        if content_type == "" {
            content_type = "application/octet-stream".to_string();
        }
        header.insert("Content-Type", HeaderValue::from_str(&content_type).expect("err"));

        if self.content_encoding != "" {
            header.insert("Content-Encoding", HeaderValue::from_str(&self.content_encoding).expect("err"));
        }
        if self.content_disposition != "" {
            header.insert("Content-Disposition", HeaderValue::from_str(&self.content_disposition).expect("err"));
        }
        if self.content_language != "" {
            header.insert("Content-Language", HeaderValue::from_str(&self.content_language).expect("err"));
        }
        if self.cache_control != "" {
            header.insert("Cache-Control", HeaderValue::from_str(&self.cache_control).expect("err"));
        }

        if self.expires.unix_timestamp() != 0 {
            header.insert(
                "Expires",
                HeaderValue::from_str(&self.expires.format(ISO8601_DATEFORMAT).unwrap()).expect("err"),
            ); //rustfs invalid header
        }

        if self.mode.as_str() != "" {
            header.insert(X_AMZ_OBJECT_LOCK_MODE, HeaderValue::from_str(self.mode.as_str()).expect("err"));
        }

        if self.retain_until_date.unix_timestamp() != 0 {
            header.insert(
                X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE,
                HeaderValue::from_str(&self.retain_until_date.format(ISO8601_DATEFORMAT).unwrap()).expect("err"),
            );
        }

        if self.legalhold.as_str() != "" {
            header.insert(X_AMZ_OBJECT_LOCK_LEGAL_HOLD, HeaderValue::from_str(self.legalhold.as_str()).expect("err"));
        }

        if self.storage_class != "" {
            header.insert(X_AMZ_STORAGE_CLASS, HeaderValue::from_str(&self.storage_class).expect("err"));
        }

        if self.website_redirect_location != "" {
            header.insert(
                X_AMZ_WEBSITE_REDIRECT_LOCATION,
                HeaderValue::from_str(&self.website_redirect_location).expect("err"),
            );
        }

        if !self.internal.replication_status.as_str().is_empty() {
            header.insert(
                X_AMZ_REPLICATION_STATUS,
                HeaderValue::from_str(self.internal.replication_status.as_str()).expect("err"),
            );
        }

        for (k, v) in &self.user_metadata {
            if is_amz_header(k) || is_standard_header(k) || is_storageclass_header(k) || is_rustfs_header(k) || is_minio_header(k)
            {
                if let Ok(header_name) = HeaderName::from_bytes(k.as_bytes()) {
                    header.insert(header_name, HeaderValue::from_str(&v).unwrap());
                }
            } else if let Ok(header_name) = HeaderName::from_bytes(format!("x-amz-meta-{}", k).as_bytes()) {
                header.insert(header_name, HeaderValue::from_str(&v).unwrap());
            }
        }

        for (k, v) in self.custom_header.iter() {
            header.insert(k.clone(), v.clone());
        }

        header
    }

    fn validate(&self, c: TransitionClient) -> Result<(), std::io::Error> {
        //if self.checksum.is_set() {
        /*if !self.trailing_header_support {
            return Err(Error::from(err_invalid_argument("Checksum requires Client with TrailingHeaders enabled")));
        }*/
        /*else if self.override_signer_type == SignatureType::SignatureV2 {
            return Err(Error::from(err_invalid_argument("Checksum cannot be used with v2 signatures")));
        }*/
        //}

        Ok(())
    }
}

impl TransitionClient {
    pub async fn put_object(
        self: Arc<Self>,
        bucket_name: &str,
        object_name: &str,
        reader: ReaderImpl,
        object_size: i64,
        opts: &PutObjectOptions,
    ) -> Result<UploadInfo, std::io::Error> {
        if object_size < 0 && opts.disable_multipart {
            return Err(std::io::Error::other("object size must be provided with disable multipart upload"));
        }

        self.put_object_common(bucket_name, object_name, reader, object_size, opts)
            .await
    }

    pub async fn put_object_common(
        self: Arc<Self>,
        bucket_name: &str,
        object_name: &str,
        reader: ReaderImpl,
        size: i64,
        opts: &PutObjectOptions,
    ) -> Result<UploadInfo, std::io::Error> {
        if size > MAX_MULTIPART_PUT_OBJECT_SIZE {
            return Err(std::io::Error::other(err_entity_too_large(
                size,
                MAX_MULTIPART_PUT_OBJECT_SIZE,
                bucket_name,
                object_name,
            )));
        }
        let mut opts = opts.clone();
        opts.auto_checksum.set_default(ChecksumMode::ChecksumCRC32C);

        let mut part_size = opts.part_size as i64;
        if opts.part_size == 0 {
            part_size = MIN_PART_SIZE;
        }

        if SignatureType::SignatureV2 == self.override_signer_type {
            if size >= 0 && size < part_size || opts.disable_multipart {
                return self.put_object_gcs(bucket_name, object_name, reader, size, &opts).await;
            }
            return self.put_object_multipart(bucket_name, object_name, reader, size, &opts).await;
        }

        if size < 0 {
            if opts.disable_multipart {
                return Err(std::io::Error::other("no length provided and multipart disabled"));
            }
            if opts.concurrent_stream_parts && opts.num_threads > 1 {
                return self
                    .put_object_multipart_stream_parallel(bucket_name, object_name, reader, &opts)
                    .await;
            }
            return self
                .put_object_multipart_stream_no_length(bucket_name, object_name, reader, &opts)
                .await;
        }

        if size <= part_size || opts.disable_multipart {
            return self.put_object_gcs(bucket_name, object_name, reader, size, &opts).await;
        }

        self.put_object_multipart_stream(bucket_name, object_name, reader, size, &opts)
            .await
    }

    pub async fn put_object_multipart_stream_no_length(
        &self,
        bucket_name: &str,
        object_name: &str,
        mut reader: ReaderImpl,
        opts: &PutObjectOptions,
    ) -> Result<UploadInfo, std::io::Error> {
        let mut total_uploaded_size: i64 = 0;

        let mut compl_multipart_upload = CompleteMultipartUpload::default();

        let (total_parts_count, part_size, _) = optimal_part_info(-1, opts.part_size)?;

        let mut opts = opts.clone();

        if opts.checksum.is_set() {
            opts.send_content_md5 = false;
            opts.auto_checksum = opts.checksum.clone();
        }
        if !opts.send_content_md5 {
            //add_auto_checksum_headers(&mut opts);
        }

        let upload_id = self.new_upload_id(bucket_name, object_name, &opts).await?;
        opts.user_metadata.remove("X-Amz-Checksum-Algorithm");

        let mut part_number = 1;
        let mut parts_info = HashMap::<i64, ObjectPart>::new();
        let mut buf = Vec::<u8>::with_capacity(part_size as usize);

        let mut custom_header = HeaderMap::new();

        while part_number <= total_parts_count {
            buf = match &mut reader {
                ReaderImpl::Body(content_body) => content_body.to_vec(),
                ReaderImpl::ObjectBody(content_body) => content_body.read_all().await?,
            };
            let length = buf.len();

            let mut md5_base64: String = "".to_string();
            if opts.send_content_md5 {
                let mut md5_hasher = self.md5_hasher.lock().unwrap();
                let hash = md5_hasher.as_mut().expect("err");
                let hash = hash.hash_encode(&buf[..length]);
                md5_base64 = base64_encode(hash.as_ref());
            } else {
                let mut crc = opts.auto_checksum.hasher()?;
                crc.update(&buf[..length]);
                let csum = crc.finalize();

                if let Ok(header_name) = HeaderName::from_bytes(opts.auto_checksum.key().as_bytes()) {
                    custom_header.insert(header_name, base64_encode(csum.as_ref()).parse().expect("err"));
                } else {
                    warn!("Invalid header name: {}", opts.auto_checksum.key());
                }
            }

            //let rd = newHook(bytes.NewReader(buf[..length]), opts.progress);
            let rd = ReaderImpl::Body(Bytes::from(buf));

            let mut p = UploadPartParams {
                bucket_name: bucket_name.to_string(),
                object_name: object_name.to_string(),
                upload_id: upload_id.clone(),
                reader: rd,
                part_number,
                md5_base64,
                size: length as i64,
                //sse: opts.server_side_encryption,
                stream_sha256: !opts.disable_content_sha256,
                custom_header: custom_header.clone(),
                sha256_hex: Default::default(),
                trailer: Default::default(),
            };
            let obj_part = self.upload_part(&mut p).await?;

            parts_info.entry(part_number).or_insert(obj_part);
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
}
