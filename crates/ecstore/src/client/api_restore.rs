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
    api_error_response::{err_invalid_argument, http_resp_to_error_response},
    api_get_object_acl::AccessControlList,
    api_get_options::GetObjectOptions,
    transition_api::{ObjectInfo, ReadCloser, ReaderImpl, RequestMetadata, TransitionClient, to_object_info},
};
use http::HeaderMap;
use http_body_util::BodyExt;
use hyper::body::Body;
use hyper::body::Bytes;
use s3s::dto::RestoreRequest;
use std::collections::HashMap;
use std::io::Cursor;
use tokio::io::BufReader;

const TIER_STANDARD: &str = "Standard";
const TIER_BULK: &str = "Bulk";
const TIER_EXPEDITED: &str = "Expedited";

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Encryption {
    pub encryption_type: String,
    pub kms_context: String,
    pub kms_key_id: String,
}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct MetadataEntry {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Default, serde::Serialize)]
pub struct S3 {
    pub access_control_list: AccessControlList,
    pub bucket_name: String,
    pub prefix: String,
    pub canned_acl: String,
    pub encryption: Encryption,
    pub storage_class: String,
    //tagging:            Tags,
    pub user_metadata: MetadataEntry,
}

impl TransitionClient {
    pub async fn restore_object(
        &self,
        bucket_name: &str,
        object_name: &str,
        version_id: &str,
        restore_req: &RestoreRequest,
    ) -> Result<(), std::io::Error> {
        /*let restore_request = match quick_xml::se::to_string(restore_req) {
            Ok(buf) => buf,
            Err(e) => {
                return Err(std::io::Error::other(e));
            }
        };*/
        let restore_request = "".to_string();
        let restore_request_bytes = restore_request.as_bytes().to_vec();

        let mut url_values = HashMap::new();
        url_values.insert("restore".to_string(), "".to_string());
        if version_id != "" {
            url_values.insert("versionId".to_string(), version_id.to_string());
        }

        let restore_request_buffer = Bytes::from(restore_request_bytes.clone());
        let resp = self
            .execute_method(
                http::Method::HEAD,
                &mut RequestMetadata {
                    bucket_name: bucket_name.to_string(),
                    object_name: object_name.to_string(),
                    query_values: url_values,
                    custom_header: HeaderMap::new(),
                    content_sha256_hex: "".to_string(), //sum_sha256_hex(&restore_request_bytes),
                    content_md5_base64: "".to_string(), //sum_md5_base64(&restore_request_bytes),
                    content_body: ReaderImpl::Body(restore_request_buffer),
                    content_length: restore_request_bytes.len() as i64,
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

        let mut body_vec = Vec::new();
        let mut body = resp.into_body();
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
            if let Some(data) = frame.data_ref() {
                body_vec.extend_from_slice(data);
            }
        }
        if resp_status != http::StatusCode::ACCEPTED && resp_status != http::StatusCode::OK {
            return Err(std::io::Error::other(http_resp_to_error_response(
                resp_status,
                &h,
                body_vec,
                bucket_name,
                "",
            )));
        }
        Ok(())
    }
}
