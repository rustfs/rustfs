#![allow(clippy::map_entry)]
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

use futures_util::ready;
use http::HeaderMap;
use std::io::{Cursor, Error as IoError, ErrorKind as IoErrorKind, Read};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::BufReader;
use tokio_util::io::StreamReader;

use crate::client::{
    api_error_response::err_invalid_argument,
    api_get_options::GetObjectOptions,
    transition_api::{ObjectInfo, ReadCloser, ReaderImpl, RequestMetadata, TransitionClient, to_object_info},
};
use futures_util::StreamExt;
use http_body_util::BodyExt;
use hyper::body::Body;
use hyper::body::Bytes;
use rustfs_utils::hash::EMPTY_STRING_SHA256_HASH;
use tokio_util::io::ReaderStream;

impl TransitionClient {
    pub fn get_object(&self, bucket_name: &str, object_name: &str, opts: &GetObjectOptions) -> Result<Object, std::io::Error> {
        todo!();
    }

    pub async fn get_object_inner(
        &self,
        bucket_name: &str,
        object_name: &str,
        opts: &GetObjectOptions,
    ) -> Result<(ObjectInfo, HeaderMap, ReadCloser), std::io::Error> {
        let resp = self
            .execute_method(
                http::Method::GET,
                &mut RequestMetadata {
                    bucket_name: bucket_name.to_string(),
                    object_name: object_name.to_string(),
                    query_values: opts.to_query_values(),
                    custom_header: opts.header(),
                    content_sha256_hex: EMPTY_STRING_SHA256_HASH.to_string(),
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

        let object_stat = to_object_info(bucket_name, object_name, resp.headers())?;

        let h = resp.headers().clone();

        let mut body_vec = Vec::new();
        let mut body = resp.into_body();
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
            if let Some(data) = frame.data_ref() {
                body_vec.extend_from_slice(data);
            }
        }
        Ok((object_stat, h, BufReader::new(Cursor::new(body_vec))))
    }
}

#[derive(Default)]
#[allow(dead_code)]
pub struct GetRequest {
    pub buffer: Vec<u8>,
    pub offset: i64,
    pub did_offset_change: bool,
    pub been_read: bool,
    pub is_read_at: bool,
    pub is_read_op: bool,
    pub is_first_req: bool,
    pub setting_object_info: bool,
}

#[allow(dead_code)]
pub struct GetResponse {
    pub size: i64,
    //pub error:       error,
    pub did_read: bool,
    pub object_info: ObjectInfo,
}

#[derive(Default)]
pub struct Object {
    //pub reqch:      chan<- getRequest,
    //pub resch:      <-chan getResponse,
    //pub cancel:     context.CancelFunc,
    pub curr_offset: i64,
    pub object_info: ObjectInfo,
    pub seek_data: bool,
    pub is_closed: bool,
    pub is_started: bool,
    //pub prev_err: error,
    pub been_read: bool,
    pub object_info_set: bool,
}

impl Object {
    pub fn new() -> Object {
        Self { ..Default::default() }
    }

    fn do_get_request(&self, request: &GetRequest) -> Result<GetResponse, std::io::Error> {
        todo!()
    }

    fn set_offset(&mut self, bytes_read: i64) -> Result<(), std::io::Error> {
        self.curr_offset += bytes_read;

        Ok(())
    }

    fn read(&mut self, b: &[u8]) -> Result<i64, std::io::Error> {
        let mut read_req = GetRequest {
            is_read_op: true,
            been_read: self.been_read,
            buffer: b.to_vec(),
            ..Default::default()
        };

        if !self.is_started {
            read_req.is_first_req = true;
        }

        read_req.did_offset_change = self.seek_data;
        read_req.offset = self.curr_offset;

        let response = self.do_get_request(&read_req)?;

        let bytes_read = response.size;

        let oerr = self.set_offset(bytes_read);

        Ok(response.size)
    }

    fn stat(&self) -> Result<ObjectInfo, std::io::Error> {
        if !self.is_started || !self.object_info_set {
            let _ = self.do_get_request(&GetRequest {
                is_first_req: !self.is_started,
                setting_object_info: !self.object_info_set,
                ..Default::default()
            })?;
        }

        Ok(self.object_info.clone())
    }

    fn read_at(&mut self, b: &[u8], offset: i64) -> Result<i64, std::io::Error> {
        self.curr_offset = offset;

        let mut read_at_req = GetRequest {
            is_read_op: true,
            is_read_at: true,
            did_offset_change: true,
            been_read: self.been_read,
            offset,
            buffer: b.to_vec(),
            ..Default::default()
        };

        if !self.is_started {
            read_at_req.is_first_req = true;
        }

        let response = self.do_get_request(&read_at_req)?;
        let bytes_read = response.size;
        if !self.object_info_set {
            self.curr_offset += bytes_read;
        } else {
            let oerr = self.set_offset(bytes_read);
        }
        Ok(response.size)
    }

    fn seek(&mut self, offset: i64, whence: i64) -> Result<i64, std::io::Error> {
        if !self.is_started || !self.object_info_set {
            let seek_req = GetRequest {
                is_read_op: false,
                offset,
                is_first_req: true,
                ..Default::default()
            };
            let _ = self.do_get_request(&seek_req);
        }

        let mut new_offset = self.curr_offset;

        match whence {
            0 => {
                new_offset = offset;
            }
            1 => {
                new_offset += offset;
            }
            2 => {
                new_offset = self.object_info.size as i64 + offset as i64;
            }
            _ => {
                return Err(std::io::Error::other(err_invalid_argument(&format!("Invalid whence {}", whence))));
            }
        }

        self.seek_data = (new_offset != self.curr_offset) || self.seek_data;
        self.curr_offset = new_offset;

        Ok(self.curr_offset)
    }

    fn close(&mut self) -> Result<(), std::io::Error> {
        self.is_closed = true;
        Ok(())
    }
}
