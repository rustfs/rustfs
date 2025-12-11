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
    api_get_options::GetObjectOptions,
    transition_api::{ObjectInfo, ReaderImpl, RequestMetadata, TransitionClient},
};
use bytes::Bytes;
use http::{HeaderMap, HeaderValue};
use rustfs_config::MAX_S3_CLIENT_RESPONSE_SIZE;
use rustfs_utils::EMPTY_STRING_SHA256_HASH;
use s3s::dto::Owner;
use std::collections::HashMap;

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Grantee {
    pub id: String,
    pub display_name: String,
    pub uri: String,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Grant {
    pub grantee: Grantee,
    pub permission: String,
}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct AccessControlList {
    pub grant: Vec<Grant>,
    pub permission: String,
}

#[derive(Debug, Default, serde::Deserialize)]
pub struct AccessControlPolicy {
    #[serde(skip)]
    owner: Owner,
    pub access_control_list: AccessControlList,
}

impl TransitionClient {
    pub async fn get_object_acl(&self, bucket_name: &str, object_name: &str) -> Result<ObjectInfo, std::io::Error> {
        let mut url_values = HashMap::new();
        url_values.insert("acl".to_string(), "".to_string());
        let mut resp = self
            .execute_method(
                http::Method::GET,
                &mut RequestMetadata {
                    bucket_name: bucket_name.to_string(),
                    object_name: object_name.to_string(),
                    query_values: url_values,
                    custom_header: HeaderMap::new(),
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

        if resp.status() != http::StatusCode::OK {
            let b = resp.body().bytes().expect("err").to_vec();
            return Err(std::io::Error::other(http_resp_to_error_response(&resp, b, bucket_name, object_name)));
        }

        let b = resp
            .body_mut()
            .store_all_limited(MAX_S3_CLIENT_RESPONSE_SIZE)
            .await
            .unwrap()
            .to_vec();
        let mut res = match quick_xml::de::from_str::<AccessControlPolicy>(&String::from_utf8(b).unwrap()) {
            Ok(result) => result,
            Err(err) => {
                return Err(std::io::Error::other(err.to_string()));
            }
        };

        let mut obj_info = self
            .stat_object(bucket_name, object_name, &GetObjectOptions::default())
            .await?;

        obj_info.owner.display_name = res.owner.display_name.clone();
        obj_info.owner.id = res.owner.id.clone();

        //obj_info.grant.extend(res.access_control_list.grant);

        let canned_acl = get_canned_acl(&res);
        if canned_acl != "" {
            obj_info
                .metadata
                .insert("X-Amz-Acl", HeaderValue::from_str(&canned_acl).unwrap());
            return Ok(obj_info);
        }

        let grant_acl = get_amz_grant_acl(&res);
        /*for (k, v) in grant_acl {
            obj_info.metadata.insert(HeaderName::from_bytes(k.as_bytes()).unwrap(), HeaderValue::from_str(&v.to_string()).unwrap());
        }*/

        Ok(obj_info)
    }
}

fn get_canned_acl(ac_policy: &AccessControlPolicy) -> String {
    let grants = ac_policy.access_control_list.grant.clone();

    if grants.len() == 1 {
        if grants[0].grantee.uri == "" && grants[0].permission == "FULL_CONTROL" {
            return "private".to_string();
        }
    } else if grants.len() == 2 {
        for g in grants {
            if g.grantee.uri == "http://acs.amazonaws.com/groups/global/AuthenticatedUsers" && &g.permission == "READ" {
                return "authenticated-read".to_string();
            }
            if g.grantee.uri == "http://acs.amazonaws.com/groups/global/AllUsers" && &g.permission == "READ" {
                return "public-read".to_string();
            }
            if g.permission == "READ" && g.grantee.id == ac_policy.owner.id.clone().unwrap() {
                return "bucket-owner-read".to_string();
            }
        }
    } else if grants.len() == 3 {
        for g in grants {
            if g.grantee.uri == "http://acs.amazonaws.com/groups/global/AllUsers" && g.permission == "WRITE" {
                return "public-read-write".to_string();
            }
        }
    }
    "".to_string()
}

pub fn get_amz_grant_acl(ac_policy: &AccessControlPolicy) -> HashMap<String, Vec<String>> {
    let grants = ac_policy.access_control_list.grant.clone();
    let mut res = HashMap::<String, Vec<String>>::new();

    for g in grants {
        let mut id = "id=".to_string();
        id.push_str(&g.grantee.id);
        let permission: &str = &g.permission;
        match permission {
            "READ" => {
                res.entry("X-Amz-Grant-Read".to_string()).or_insert(vec![]).push(id);
            }
            "WRITE" => {
                res.entry("X-Amz-Grant-Write".to_string()).or_insert(vec![]).push(id);
            }
            "READ_ACP" => {
                res.entry("X-Amz-Grant-Read-Acp".to_string()).or_insert(vec![]).push(id);
            }
            "WRITE_ACP" => {
                res.entry("X-Amz-Grant-Write-Acp".to_string()).or_insert(vec![]).push(id);
            }
            "FULL_CONTROL" => {
                res.entry("X-Amz-Grant-Full-Control".to_string()).or_insert(vec![]).push(id);
            }
            _ => (),
        }
    }
    res
}
