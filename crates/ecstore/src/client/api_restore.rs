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
use http::HeaderMap;
use std::collections::HashMap;
use std::io::Cursor;
use tokio::io::BufReader;

use crate::client::{
    api_error_response::{err_invalid_argument, http_resp_to_error_response},
    api_get_object_acl::AccessControlList,
    api_get_options::GetObjectOptions,
    transition_api::{ObjectInfo, ReadCloser, ReaderImpl, RequestMetadata, TransitionClient, to_object_info},
};

const TIER_STANDARD: &str = "Standard";
const TIER_BULK: &str = "Bulk";
const TIER_EXPEDITED: &str = "Expedited";

#[derive(Debug, Default, serde::Serialize)]
pub struct GlacierJobParameters {
    pub tier: String,
}

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

#[derive(Debug, Default, serde::Serialize)]
pub struct SelectParameters {
    pub expression_type: String,
    pub expression: String,
    //input_serialization:  SelectObjectInputSerialization,
    //output_serialization: SelectObjectOutputSerialization,
}

#[derive(Debug, Default, serde::Serialize)]
pub struct OutputLocation(pub S3);

#[derive(Debug, Default, serde::Serialize)]
pub struct RestoreRequest {
    pub restore_type: String,
    pub tier: String,
    pub days: i64,
    pub glacier_job_parameters: GlacierJobParameters,
    pub description: String,
    pub select_parameters: SelectParameters,
    pub output_location: OutputLocation,
}

impl RestoreRequest {
    fn set_days(&mut self, v: i64) {
        self.days = v;
    }

    fn set_glacier_job_parameters(&mut self, v: GlacierJobParameters) {
        self.glacier_job_parameters = v;
    }

    fn set_type(&mut self, v: &str) {
        self.restore_type = v.to_string();
    }

    fn set_tier(&mut self, v: &str) {
        self.tier = v.to_string();
    }

    fn set_description(&mut self, v: &str) {
        self.description = v.to_string();
    }

    fn set_select_parameters(&mut self, v: SelectParameters) {
        self.select_parameters = v;
    }

    fn set_output_location(&mut self, v: OutputLocation) {
        self.output_location = v;
    }
}

impl TransitionClient {
    pub async fn restore_object(
        &self,
        bucket_name: &str,
        object_name: &str,
        version_id: &str,
        restore_req: &RestoreRequest,
    ) -> Result<(), std::io::Error> {
        let restore_request = match serde_xml_rs::to_string(restore_req) {
            Ok(buf) => buf,
            Err(e) => {
                return Err(std::io::Error::other(e));
            }
        };
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

        let b = resp.body().bytes().expect("err").to_vec();
        if resp.status() != http::StatusCode::ACCEPTED && resp.status() != http::StatusCode::OK {
            return Err(std::io::Error::other(http_resp_to_error_response(&resp, b, bucket_name, "")));
        }
        Ok(())
    }
}
