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

use http::Request;
use hyper::StatusCode;
use hyper::body::Incoming;
use std::{collections::HashMap, sync::Arc};
use tracing::warn;
use tracing::{debug, error, info};

use crate::client::{
    api_error_response::{http_resp_to_error_response, to_error_response},
    transition_api::{CreateBucketConfiguration, LocationConstraint, TransitionClient},
};
use rustfs_utils::hash::EMPTY_STRING_SHA256_HASH;
use s3s::Body;
use s3s::S3ErrorCode;

use super::constants::UNSIGNED_PAYLOAD;
use super::credentials::SignatureType;

#[derive(Debug, Clone)]
pub struct BucketLocationCache {
    items: HashMap<String, String>,
}

impl BucketLocationCache {
    pub fn new() -> BucketLocationCache {
        BucketLocationCache { items: HashMap::new() }
    }

    pub fn get(&self, bucket_name: &str) -> Option<String> {
        self.items.get(bucket_name).map(|s| s.clone())
    }

    pub fn set(&mut self, bucket_name: &str, location: &str) {
        self.items.insert(bucket_name.to_string(), location.to_string());
    }

    pub fn delete(&mut self, bucket_name: &str) {
        self.items.remove(bucket_name);
    }
}

impl TransitionClient {
    pub async fn get_bucket_location(&self, bucket_name: &str) -> Result<String, std::io::Error> {
        Ok(self.get_bucket_location_inner(bucket_name).await?)
    }

    async fn get_bucket_location_inner(&self, bucket_name: &str) -> Result<String, std::io::Error> {
        if self.region != "" {
            return Ok(self.region.clone());
        }

        let mut location;
        {
            let mut bucket_loc_cache = self.bucket_loc_cache.lock().unwrap();
            let ret = bucket_loc_cache.get(bucket_name);
            if let Some(location) = ret {
                return Ok(location);
            }
            //location = ret?;
        }

        let req = self.get_bucket_location_request(bucket_name)?;

        let mut resp = self.doit(req).await?;
        location = process_bucket_location_response(resp, bucket_name, &self.tier_type).await?;
        {
            let mut bucket_loc_cache = self.bucket_loc_cache.lock().unwrap();
            bucket_loc_cache.set(bucket_name, &location);
        }
        Ok(location)
    }

    fn get_bucket_location_request(&self, bucket_name: &str) -> Result<http::Request<Body>, std::io::Error> {
        let mut url_values = HashMap::new();
        url_values.insert("location".to_string(), "".to_string());

        let mut target_url = self.endpoint_url.clone();
        let scheme = self.endpoint_url.scheme();
        let h = target_url.host().expect("host is none.");
        let default_port = if scheme == "https" { 443 } else { 80 };
        let p = target_url.port().unwrap_or(default_port);

        let is_virtual_style = self.is_virtual_host_style_request(&target_url, bucket_name);

        let mut url_str: String = "".to_string();

        if is_virtual_style {
            url_str = scheme.to_string();
            url_str.push_str("://");
            url_str.push_str(bucket_name);
            url_str.push_str(".");
            url_str.push_str(target_url.host_str().expect("err"));
            url_str.push_str("/?location");
        } else {
            let mut path = bucket_name.to_string();
            path.push_str("/");
            target_url.set_path(&path);
            {
                let mut q = target_url.query_pairs_mut();
                for (k, v) in url_values {
                    q.append_pair(&k, &urlencoding::encode(&v));
                }
            }
            url_str = target_url.to_string();
        }

        let Ok(mut req) = Request::builder().method(http::Method::GET).uri(url_str).body(Body::empty()) else {
            return Err(std::io::Error::other("create request error"));
        };

        self.set_user_agent(&mut req);

        let value;
        {
            let mut creds_provider = self.creds_provider.lock().unwrap();
            value = match creds_provider.get_with_context(Some(self.cred_context())) {
                Ok(v) => v,
                Err(err) => {
                    return Err(std::io::Error::other(err));
                }
            };
        }

        let mut signer_type = value.signer_type.clone();
        let mut access_key_id = value.access_key_id;
        let mut secret_access_key = value.secret_access_key;
        let mut session_token = value.session_token;

        if self.override_signer_type != SignatureType::SignatureDefault {
            signer_type = self.override_signer_type.clone();
        }

        if value.signer_type == SignatureType::SignatureAnonymous {
            signer_type = SignatureType::SignatureAnonymous
        }

        if signer_type == SignatureType::SignatureAnonymous {
            return Ok(req);
        }

        if signer_type == SignatureType::SignatureV2 {
            let req = rustfs_signer::sign_v2(req, 0, &access_key_id, &secret_access_key, is_virtual_style);
            return Ok(req);
        }

        let mut content_sha256 = EMPTY_STRING_SHA256_HASH.to_string();
        if self.secure {
            content_sha256 = UNSIGNED_PAYLOAD.to_string();
        }

        req.headers_mut()
            .insert("X-Amz-Content-Sha256", content_sha256.parse().unwrap());
        let req = rustfs_signer::sign_v4(req, 0, &access_key_id, &secret_access_key, &session_token, "us-east-1");
        Ok(req)
    }
}

async fn process_bucket_location_response(
    mut resp: http::Response<Body>,
    bucket_name: &str,
    tier_type: &str,
) -> Result<String, std::io::Error> {
    //if resp != nil {
    if resp.status() != StatusCode::OK {
        let err_resp = http_resp_to_error_response(&resp, vec![], bucket_name, "");
        match err_resp.code {
                S3ErrorCode::NotImplemented => {
                    match err_resp.server.as_str() {
                        "AmazonSnowball" => {
                            return Ok("snowball".to_string());
                        }
                        "cloudflare" => {
                            return Ok("us-east-1".to_string());
                        }
                        _ => {
                            return Err(std::io::Error::other(err_resp));
                        }
                    }
                }
                S3ErrorCode::AuthorizationHeaderMalformed |
                //S3ErrorCode::InvalidRegion |
                S3ErrorCode::AccessDenied => {
                    if err_resp.region == "" {
                      return Ok("us-east-1".to_string());
                    }
                    return Ok(err_resp.region);
                }
                _ => {
                    return Err(std::io::Error::other(err_resp));
                }
            }
    }
    //}

    let b = resp.body_mut().store_all_unlimited().await.unwrap().to_vec();
    let mut location = "".to_string();
    if tier_type == "huaweicloud" {
        let d = quick_xml::de::from_str::<CreateBucketConfiguration>(&String::from_utf8(b).unwrap()).unwrap();
        location = d.location_constraint;
    } else {
        if let Ok(LocationConstraint { field }) = quick_xml::de::from_str::<LocationConstraint>(&String::from_utf8(b).unwrap()) {
            location = field;
        }
    }
    //debug!("location: {}", location);

    if location == "" {
        location = "us-east-1".to_string();
    }

    if location == "EU" {
        location = "eu-west-1".to_string();
    }

    Ok(location)
}
