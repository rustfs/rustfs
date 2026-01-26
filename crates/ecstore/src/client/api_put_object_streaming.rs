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
use futures::future::join_all;
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use std::sync::RwLock;
use std::{collections::HashMap, sync::Arc};
use time::{OffsetDateTime, format_description};
use tokio::{select, sync::mpsc};
use tokio_util::sync::CancellationToken;
use tracing::warn;
use uuid::Uuid;

use crate::client::checksum::{ChecksumMode, add_auto_checksum_headers, apply_auto_checksum};
use crate::client::{
    api_error_response::{err_invalid_argument, err_unexpected_eof, http_resp_to_error_response},
    api_put_object::PutObjectOptions,
    api_put_object_common::{is_object, optimal_part_info},
    api_put_object_multipart::UploadPartParams,
    api_s3_datatypes::{CompleteMultipartUpload, CompletePart, ObjectPart},
    constants::ISO8601_DATEFORMAT,
    transition_api::{ReaderImpl, RequestMetadata, TransitionClient, UploadInfo},
};

use crate::client::utils::base64_encode;
use rustfs_utils::path::trim_etag;
use s3s::header::{X_AMZ_EXPIRATION, X_AMZ_VERSION_ID};

pub struct UploadedPartRes {
    pub error: std::io::Error,
    pub part_num: i64,
    pub size: i64,
    pub part: ObjectPart,
}

pub struct UploadPartReq {
    pub part_num: i64,
    pub part: ObjectPart,
}

impl TransitionClient {
    pub async fn put_object_multipart_stream(
        self: Arc<Self>,
        bucket_name: &str,
        object_name: &str,
        reader: ReaderImpl,
        size: i64,
        opts: &PutObjectOptions,
    ) -> Result<UploadInfo, std::io::Error> {
        let info: UploadInfo;
        if opts.concurrent_stream_parts && opts.num_threads > 1 {
            info = self
                .put_object_multipart_stream_parallel(bucket_name, object_name, reader, opts)
                .await?;
        } else if !is_object(&reader) && !opts.send_content_md5 {
            info = self
                .put_object_multipart_stream_from_readat(bucket_name, object_name, reader, size, opts)
                .await?;
        } else {
            info = self
                .put_object_multipart_stream_optional_checksum(bucket_name, object_name, reader, size, opts)
                .await?;
        }

        Ok(info)
    }

    pub async fn put_object_multipart_stream_from_readat(
        &self,
        bucket_name: &str,
        object_name: &str,
        reader: ReaderImpl,
        size: i64,
        opts: &PutObjectOptions,
    ) -> Result<UploadInfo, std::io::Error> {
        let ret = optimal_part_info(size, opts.part_size)?;
        let (total_parts_count, part_size, lastpart_size) = ret;
        let mut opts = opts.clone();
        if opts.checksum.is_set() {
            opts.auto_checksum = opts.checksum.clone();
        }
        let with_checksum = self.trailing_header_support;
        let upload_id = self.new_upload_id(bucket_name, object_name, &opts).await?;
        opts.user_metadata.remove("X-Amz-Checksum-Algorithm");

        todo!();
    }

    pub async fn put_object_multipart_stream_optional_checksum(
        &self,
        bucket_name: &str,
        object_name: &str,
        mut reader: ReaderImpl,
        size: i64,
        opts: &PutObjectOptions,
    ) -> Result<UploadInfo, std::io::Error> {
        let mut opts = opts.clone();
        if opts.checksum.is_set() {
            opts.auto_checksum = opts.checksum.clone();
            opts.send_content_md5 = false;
        }

        if !opts.send_content_md5 {
            add_auto_checksum_headers(&mut opts);
        }

        let ret = optimal_part_info(size, opts.part_size)?;
        let (total_parts_count, mut part_size, lastpart_size) = ret;
        let upload_id = self.new_upload_id(bucket_name, object_name, &opts).await?;
        opts.user_metadata.remove("X-Amz-Checksum-Algorithm");

        let mut custom_header = opts.header().clone();

        let mut total_uploaded_size: i64 = 0;

        let mut parts_info = HashMap::<i64, ObjectPart>::new();
        let mut buf = Vec::<u8>::with_capacity(part_size as usize);

        let mut md5_base64: String = "".to_string();
        for part_number in 1..=total_parts_count {
            if part_number == total_parts_count {
                part_size = lastpart_size;
            }

            match &mut reader {
                ReaderImpl::Body(content_body) => {
                    buf = content_body.to_vec();
                }
                ReaderImpl::ObjectBody(content_body) => {
                    buf = content_body.read_all().await?;
                }
            }
            let length = buf.len();

            if opts.send_content_md5 {
                let mut md5_hasher = self.md5_hasher.lock().unwrap();
                let md5_hash = md5_hasher.as_mut().expect("err");
                let hash = md5_hash.hash_encode(&buf[..length]);
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

            let hooked = ReaderImpl::Body(Bytes::from(buf)); //newHook(BufferReader::new(buf), opts.progress);
            let mut p = UploadPartParams {
                bucket_name: bucket_name.to_string(),
                object_name: object_name.to_string(),
                upload_id: upload_id.clone(),
                reader: hooked,
                part_number,
                md5_base64: md5_base64.clone(),
                size: part_size,
                //sse: opts.server_side_encryption,
                stream_sha256: !opts.disable_content_sha256,
                custom_header: custom_header.clone(),
                sha256_hex: "".to_string(),
                trailer: HeaderMap::new(),
            };
            let obj_part = self.upload_part(&mut p).await?;

            parts_info.entry(part_number).or_insert(obj_part);

            total_uploaded_size += part_size as i64;
        }

        if size > 0 && total_uploaded_size != size {
            return Err(std::io::Error::other(err_unexpected_eof(
                total_uploaded_size,
                size,
                bucket_name,
                object_name,
            )));
        }

        let mut compl_multipart_upload = CompleteMultipartUpload::default();

        let mut all_parts = Vec::<ObjectPart>::with_capacity(parts_info.len());
        let part_number = total_parts_count;
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
            });
        }

        compl_multipart_upload.parts.sort();

        let mut opts = PutObjectOptions {
            //server_side_encryption: opts.server_side_encryption,
            auto_checksum: opts.auto_checksum,
            ..Default::default()
        };
        apply_auto_checksum(&mut opts, &mut all_parts);
        let mut upload_info = self
            .complete_multipart_upload(bucket_name, object_name, &upload_id, compl_multipart_upload, &opts)
            .await?;

        upload_info.size = total_uploaded_size;
        Ok(upload_info)
    }

    pub async fn put_object_multipart_stream_parallel(
        self: Arc<Self>,
        bucket_name: &str,
        object_name: &str,
        mut reader: ReaderImpl, /*GetObjectReader*/
        opts: &PutObjectOptions,
    ) -> Result<UploadInfo, std::io::Error> {
        let mut opts = opts.clone();
        if opts.checksum.is_set() {
            opts.send_content_md5 = false;
            opts.auto_checksum = opts.checksum.clone();
        }
        if !opts.send_content_md5 {
            add_auto_checksum_headers(&mut opts);
        }

        let ret = optimal_part_info(-1, opts.part_size)?;
        let (total_parts_count, part_size, _) = ret;

        let upload_id = self.new_upload_id(bucket_name, object_name, &opts).await?;
        opts.user_metadata.remove("X-Amz-Checksum-Algorithm");

        let mut total_uploaded_size: i64 = 0;
        let parts_info = Arc::new(RwLock::new(HashMap::<i64, ObjectPart>::new()));

        let n_buffers = opts.num_threads;
        let (bufs_tx, mut bufs_rx) = mpsc::channel(n_buffers as usize);
        //let all = Vec::<u8>::with_capacity(n_buffers as usize * part_size as usize);
        for i in 0..n_buffers {
            //bufs_tx.send(&all[i * part_size..i * part_size + part_size]);
            bufs_tx.send(Vec::<u8>::with_capacity(part_size as usize));
        }

        let mut futures = Vec::with_capacity(total_parts_count as usize);
        let (err_tx, mut err_rx) = mpsc::channel(opts.num_threads as usize);
        let cancel_token = CancellationToken::new();

        //reader = newHook(reader, opts.progress);

        for part_number in 1..=total_parts_count {
            let mut buf = Vec::<u8>::new();
            select! {
                buf = bufs_rx.recv() => {}
                err = err_rx.recv() => {
                    //cancel_token.cancel();
                    //wg.Wait()
                    return Err(err.expect("err"));
                }
                else => (),
            }

            if buf.len() != part_size as usize {
                return Err(std::io::Error::other(format!(
                    "read buffer < {} than expected partSize: {}",
                    buf.len(),
                    part_size
                )));
            }

            match &mut reader {
                ReaderImpl::Body(content_body) => {
                    buf = content_body.to_vec();
                }
                ReaderImpl::ObjectBody(content_body) => {
                    buf = content_body.read_all().await?;
                }
            }
            let length = buf.len();

            let mut custom_header = HeaderMap::new();
            if !opts.send_content_md5 {
                let mut crc = opts.auto_checksum.hasher()?;
                crc.update(&buf[..length]);
                let csum = crc.finalize();

                if let Ok(header_name) = HeaderName::from_bytes(opts.auto_checksum.key().as_bytes()) {
                    custom_header.insert(header_name, base64_encode(csum.as_ref()).parse().expect("err"));
                } else {
                    warn!("Invalid header name: {}", opts.auto_checksum.key());
                }
            }

            let clone_bufs_tx = bufs_tx.clone();
            let clone_parts_info = parts_info.clone();
            let clone_upload_id = upload_id.clone();
            let clone_self = self.clone();
            futures.push(async move {
                let mut md5_base64: String = "".to_string();

                if opts.send_content_md5 {
                    let mut md5_hasher = clone_self.md5_hasher.lock().unwrap();
                    let md5_hash = md5_hasher.as_mut().expect("err");
                    let hash = md5_hash.hash_encode(&buf[..length]);
                    md5_base64 = base64_encode(hash.as_ref());
                }

                //defer wg.Done()
                let mut p = UploadPartParams {
                    bucket_name: bucket_name.to_string(),
                    object_name: object_name.to_string(),
                    upload_id: clone_upload_id,
                    reader: ReaderImpl::Body(Bytes::from(buf.clone())),
                    part_number,
                    md5_base64,
                    size: length as i64,
                    //sse:           opts.server_side_encryption,
                    stream_sha256: !opts.disable_content_sha256,
                    custom_header,
                    sha256_hex: "".to_string(),
                    trailer: HeaderMap::new(),
                };
                let obj_part = clone_self.upload_part(&mut p).await.expect("err");

                let mut clone_parts_info = clone_parts_info.write().unwrap();
                clone_parts_info.entry(part_number).or_insert(obj_part);

                clone_bufs_tx.send(buf);
            });

            total_uploaded_size += length as i64;
        }

        let results = join_all(futures).await;

        select! {
            err = err_rx.recv() => {
                return Err(err.expect("err"));
            }
            else => (),
        }

        let mut compl_multipart_upload = CompleteMultipartUpload::default();

        let part_number: i64 = total_parts_count;
        let mut all_parts = Vec::<ObjectPart>::with_capacity(parts_info.read().unwrap().len());
        for i in 1..part_number {
            let part = parts_info.read().unwrap()[&i].clone();

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

        let mut opts = PutObjectOptions {
            //server_side_encryption: opts.server_side_encryption,
            auto_checksum: opts.auto_checksum,
            ..Default::default()
        };
        apply_auto_checksum(&mut opts, &mut all_parts);

        let mut upload_info = self
            .complete_multipart_upload(bucket_name, object_name, &upload_id, compl_multipart_upload, &opts)
            .await?;

        upload_info.size = total_uploaded_size;
        Ok(upload_info)
    }

    pub async fn put_object_gcs(
        &self,
        bucket_name: &str,
        object_name: &str,
        reader: ReaderImpl,
        size: i64,
        opts: &PutObjectOptions,
    ) -> Result<UploadInfo, std::io::Error> {
        let mut opts = opts.clone();
        if opts.checksum.is_set() {
            opts.send_content_md5 = false;
        }

        let md5_base64: String = "".to_string();
        let progress_reader = reader; //newHook(reader, opts.progress);

        self.put_object_do(bucket_name, object_name, progress_reader, &md5_base64, "", size, &opts)
            .await
    }

    pub async fn put_object_do(
        &self,
        bucket_name: &str,
        object_name: &str,
        reader: ReaderImpl,
        md5_base64: &str,
        sha256_hex: &str,
        size: i64,
        opts: &PutObjectOptions,
    ) -> Result<UploadInfo, std::io::Error> {
        let custom_header = opts.header();

        let mut req_metadata = RequestMetadata {
            bucket_name: bucket_name.to_string(),
            object_name: object_name.to_string(),
            custom_header,
            content_body: reader,
            content_length: size,
            content_md5_base64: md5_base64.to_string(),
            content_sha256_hex: sha256_hex.to_string(),
            stream_sha256: !opts.disable_content_sha256,
            add_crc: Default::default(),
            bucket_location: Default::default(),
            pre_sign_url: Default::default(),
            query_values: Default::default(),
            extra_pre_sign_header: Default::default(),
            expires: Default::default(),
            trailer: Default::default(),
        };
        let mut add_crc = false; //self.trailing_header_support && md5_base64 == "" && !s3utils.IsGoogleEndpoint(self.endpoint_url) && (opts.disable_content_sha256 || self.secure);
        let mut opts = opts.clone();
        if opts.checksum.is_set() {
            req_metadata.add_crc = opts.checksum;
        } else if add_crc {
            for (k, _) in opts.user_metadata {
                if k.to_lowercase().starts_with("x-amz-checksum-") {
                    add_crc = false;
                }
            }
            if add_crc {
                opts.auto_checksum.set_default(ChecksumMode::ChecksumCRC32C);
                req_metadata.add_crc = opts.auto_checksum;
            }
        }

        if opts.internal.source_version_id != "" {
            if !opts.internal.source_version_id.is_empty() {
                if let Err(err) = Uuid::parse_str(&opts.internal.source_version_id) {
                    return Err(std::io::Error::other(err_invalid_argument(&err.to_string())));
                }
            }
            let mut url_values = HashMap::new();
            url_values.insert("versionId".to_string(), opts.internal.source_version_id);
            req_metadata.query_values = url_values;
        }

        let resp = self.execute_method(http::Method::PUT, &mut req_metadata).await?;

        let resp_status = resp.status();
        let h = resp.headers().clone();

        if resp.status() != StatusCode::OK {
            return Err(std::io::Error::other(http_resp_to_error_response(
                resp_status,
                &h,
                vec![],
                bucket_name,
                object_name,
            )));
        }

        let (exp_time, rule_id) = if let Some(h_x_amz_expiration) = resp.headers().get(X_AMZ_EXPIRATION) {
            (
                OffsetDateTime::parse(h_x_amz_expiration.to_str().unwrap(), ISO8601_DATEFORMAT).unwrap(),
                "".to_string(),
            )
        } else {
            (OffsetDateTime::now_utc(), "".to_string())
        };
        let h = resp.headers();
        Ok(UploadInfo {
            bucket: bucket_name.to_string(),
            key: object_name.to_string(),
            etag: trim_etag(h.get("ETag").expect("err").to_str().expect("err")),
            version_id: if let Some(h_x_amz_version_id) = h.get(X_AMZ_VERSION_ID) {
                h_x_amz_version_id.to_str().expect("err").to_string()
            } else {
                "".to_string()
            },
            size,
            expiration: exp_time,
            expiration_rule_id: rule_id,
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
        })
    }
}
