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

use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;

use crate::client::transition_api::{ReadCloser, ReaderImpl};
use crate::tier::{
    tier_config::TierS3,
    warm_backend::{WarmBackend, WarmBackendGetOpts},
};

pub struct WarmBackendS3 {
    pub client: Arc<Client>,
    pub bucket: String,
    pub prefix: String,
    pub storage_class: String,
}

impl WarmBackendS3 {
    pub async fn new(conf: &TierS3, tier: &str) -> Result<Self, std::io::Error> {
        let u = match Url::parse(&conf.endpoint) {
            Ok(u) => u,
            Err(err) => {
                return Err(std::io::Error::other(err.to_string()));
            }
        };

        if conf.aws_role_web_identity_token_file == "" && conf.aws_role_arn != ""
            || conf.aws_role_web_identity_token_file != "" && conf.aws_role_arn == ""
        {
            return Err(std::io::Error::other("both the token file and the role ARN are required"));
        } else if conf.access_key == "" && conf.secret_key != "" || conf.access_key != "" && conf.secret_key == "" {
            return Err(std::io::Error::other("both the access and secret keys are required"));
        } else if conf.aws_role
            && (conf.aws_role_web_identity_token_file != ""
                || conf.aws_role_arn != ""
                || conf.access_key != ""
                || conf.secret_key != "")
        {
            return Err(std::io::Error::other(
                "AWS Role cannot be activated with static credentials or the web identity token file",
            ));
        } else if conf.bucket == "" {
            return Err(std::io::Error::other("no bucket name was provided"));
        }

        let creds;
        if conf.access_key != "" && conf.secret_key != "" {
            creds = Credentials::new(
                conf.access_key.clone(), // access_key_id
                conf.secret_key.clone(), // secret_access_key
                None,                    // session_token (optional)
                None,
                "Static",
            );
        } else {
            return Err(std::io::Error::other("insufficient parameters for S3 backend authentication"));
        }
        let region_provider = RegionProviderChain::default_provider().or_else(Region::new(conf.region.clone()));
        #[allow(deprecated)]
        let config = aws_config::from_env()
            .endpoint_url(conf.endpoint.clone())
            .region(region_provider)
            .credentials_provider(creds)
            .load()
            .await;
        let client = Client::new(&config);
        let client = Arc::new(client);
        Ok(Self {
            client,
            bucket: conf.bucket.clone(),
            prefix: conf.prefix.clone().trim_matches('/').to_string(),
            storage_class: conf.storage_class.clone(),
        })
    }

    pub fn get_dest(&self, object: &str) -> String {
        let mut dest_obj = object.to_string();
        if self.prefix != "" {
            dest_obj = format!("{}/{}", &self.prefix, object);
        }
        return dest_obj;
    }
}

#[async_trait::async_trait]
impl WarmBackend for WarmBackendS3 {
    async fn put_with_meta(
        &self,
        object: &str,
        r: ReaderImpl,
        length: i64,
        meta: HashMap<String, String>,
    ) -> Result<String, std::io::Error> {
        let client = self.client.clone();
        let Ok(res) = client
            .put_object()
            .bucket(&self.bucket)
            .key(&self.get_dest(object))
            .body(match r {
                ReaderImpl::Body(content_body) => ByteStream::from(content_body.to_vec()),
                ReaderImpl::ObjectBody(mut content_body) => ByteStream::from(content_body.read_all().await?),
            })
            .send()
            .await
        else {
            return Err(std::io::Error::other("put_object error"));
        };

        Ok(res.version_id().unwrap_or("").to_string())
    }

    async fn put(&self, object: &str, r: ReaderImpl, length: i64) -> Result<String, std::io::Error> {
        self.put_with_meta(object, r, length, HashMap::new()).await
    }

    async fn get(&self, object: &str, rv: &str, opts: WarmBackendGetOpts) -> Result<ReadCloser, std::io::Error> {
        let client = self.client.clone();
        let Ok(res) = client
            .get_object()
            .bucket(&self.bucket)
            .key(&self.get_dest(object))
            .send()
            .await
        else {
            return Err(std::io::Error::other("get_object error"));
        };

        Ok(ReadCloser::new(std::io::Cursor::new(
            res.body.collect().await.map(|data| data.into_bytes().to_vec())?,
        )))
    }

    async fn remove(&self, object: &str, rv: &str) -> Result<(), std::io::Error> {
        let client = self.client.clone();
        if let Err(_) = client
            .delete_object()
            .bucket(&self.bucket)
            .key(&self.get_dest(object))
            .send()
            .await
        {
            return Err(std::io::Error::other("delete_object error"));
        }

        Ok(())
    }

    async fn in_use(&self) -> Result<bool, std::io::Error> {
        let client = self.client.clone();
        let Ok(res) = client
            .list_objects_v2()
            .bucket(&self.bucket)
            //.max_keys(10)
            //.into_paginator()
            .send()
            .await
        else {
            return Err(std::io::Error::other("list_objects_v2 error"));
        };

        Ok(res.common_prefixes.unwrap().len() > 0 || res.contents.unwrap().len() > 0)
    }
}
